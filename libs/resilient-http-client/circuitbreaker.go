package resilient

import (
	"sync/atomic"
	"time"
)

// CBState is the circuit-breaker state, encoded as a small integer so it can be
// exported directly as a Prometheus gauge.
type CBState uint32

const (
	// CBClosed is normal operation — all requests are allowed through.
	CBClosed CBState = iota
	// CBOpen rejects requests until the half-open timeout elapses.
	CBOpen
	// CBHalfOpen admits a single probe; success closes, failure re-opens.
	CBHalfOpen
)

// String renders the state for logs and tests.
func (s CBState) String() string {
	switch s {
	case CBClosed:
		return "closed"
	case CBOpen:
		return "open"
	case CBHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitBreaker is a lock-free sliding-window circuit breaker.
//
// State machine:
//
//	         failures/window >= threshold
//	┌────────┐ AND requests >= minRequests ┌──────┐
//	│ Closed │ ───────────────────────────►│ Open │
//	└────────┘                              └──────┘
//	     ▲   probe succeeds                     │ halfOpenTimeout elapsed
//	     │                                      ▼
//	     │                                ┌──────────┐
//	     └────────────────────────────────│ HalfOpen │
//	         probe fails → back to Open    └──────────┘
//
// Every transition uses an atomic compare-and-swap, so the breaker can be shared
// across goroutines with no mutex. The sliding window is two atomic counters plus
// an atomic window-start timestamp rotated by a single CAS winner; brief
// double-counting at a rotation boundary is acceptable for a breaker.
type CircuitBreaker struct {
	state atomic.Uint32 // CBState

	windowRequests atomic.Uint32
	windowFailures atomic.Uint32
	windowStartMS  atomic.Int64 // unix millis
	openedAtMS     atomic.Int64 // unix millis, 0 = never opened

	failureThreshold float64
	minRequests      uint32
	windowMS         int64
	halfOpenMS       int64
}

// NewCircuitBreaker constructs a breaker. A failureThreshold outside (0,1] is
// clamped to a usable value.
func NewCircuitBreaker(failureThreshold float64, minRequests uint32, window, halfOpenTimeout time.Duration) *CircuitBreaker {
	if failureThreshold <= 0 || failureThreshold > 1 {
		failureThreshold = defCBThreshold
	}
	cb := &CircuitBreaker{
		failureThreshold: failureThreshold,
		minRequests:      minRequests,
		windowMS:         window.Milliseconds(),
		halfOpenMS:       halfOpenTimeout.Milliseconds(),
	}
	cb.windowStartMS.Store(nowMS())
	return cb
}

// State returns the current state with a single atomic load.
func (cb *CircuitBreaker) State() CBState { return CBState(cb.state.Load()) }

// Allow reports whether a caller may proceed.
//
//   - Closed / HalfOpen: allowed.
//   - Open: rejected, unless halfOpenTimeout has elapsed — in which case exactly
//     one caller wins the CAS into HalfOpen and is admitted as the probe.
func (cb *CircuitBreaker) Allow() bool {
	switch cb.State() {
	case CBClosed, CBHalfOpen:
		return true
	case CBOpen:
		openedAt := cb.openedAtMS.Load()
		if openedAt > 0 && nowMS()-openedAt >= cb.halfOpenMS {
			// Only the CAS winner gets the probe.
			return cb.state.CompareAndSwap(uint32(CBOpen), uint32(CBHalfOpen))
		}
		return false
	default:
		return false
	}
}

// RecordSuccess records a successful response. A success in HalfOpen closes the
// circuit and starts a fresh window.
func (cb *CircuitBreaker) RecordSuccess() {
	switch cb.State() {
	case CBHalfOpen:
		cb.state.Store(uint32(CBClosed))
		cb.resetWindow()
	case CBClosed:
		cb.maybeRotateWindow()
		cb.windowRequests.Add(1)
	case CBOpen:
		// no-op
	}
}

// RecordFailure records a failed response. It may trip Closed→Open (ratio breach)
// or HalfOpen→Open (failed probe).
func (cb *CircuitBreaker) RecordFailure() {
	switch cb.State() {
	case CBHalfOpen:
		cb.open()
	case CBClosed:
		cb.maybeRotateWindow()
		requests := cb.windowRequests.Add(1)
		failures := cb.windowFailures.Add(1)
		if requests >= cb.minRequests {
			if float64(failures)/float64(requests) >= cb.failureThreshold {
				cb.open()
			}
		}
	case CBOpen:
		// no-op
	}
}

func (cb *CircuitBreaker) open() {
	cb.state.Store(uint32(CBOpen))
	cb.openedAtMS.Store(nowMS())
}

func (cb *CircuitBreaker) resetWindow() {
	cb.windowStartMS.Store(nowMS())
	cb.windowRequests.Store(0)
	cb.windowFailures.Store(0)
}

// maybeRotateWindow zeroes the counters when the window has elapsed. A CAS on
// the start timestamp ensures only one goroutine performs the reset.
func (cb *CircuitBreaker) maybeRotateWindow() {
	start := cb.windowStartMS.Load()
	now := nowMS()
	if now-start >= cb.windowMS {
		if cb.windowStartMS.CompareAndSwap(start, now) {
			cb.windowRequests.Store(0)
			cb.windowFailures.Store(0)
		}
	}
}

func nowMS() int64 { return time.Now().UnixMilli() }
