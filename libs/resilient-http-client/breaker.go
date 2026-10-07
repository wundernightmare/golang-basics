package resilient

import (
	"sync"
	"sync/atomic"
	"time"
)

// BreakerState is a circuit breaker's state; its value is the
// circuit_breaker_state gauge.
type BreakerState int

// The breaker states.
const (
	// StateClosed lets every request through and counts failures.
	StateClosed BreakerState = iota
	// StateOpen rejects every request until the open timeout elapses.
	StateOpen
	// StateHalfOpen admits a fixed number of probes; they decide whether the
	// breaker closes (all succeed) or re-opens (any fails).
	StateHalfOpen
)

// String returns the state's label value.
func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// outcome is what a finished request tells the breaker.
type outcome int

const (
	// outcomeSuccess: the dependency answered (2xx–4xx, a refused redirect).
	outcomeSuccess outcome = iota
	// outcomeFailure: 5xx, attempt timeout, connection error.
	outcomeFailure
	// outcomeIgnored: says nothing about the dependency — the caller
	// cancelled, or a local limit rejected the request before it was sent.
	outcomeIgnored
)

// ticket is what [breaker.allow] hands out and [breaker.record] takes back:
// the generation the request was admitted in, and whether it holds a probe
// slot. Results from an older generation are ignored.
type ticket struct {
	gen   uint64
	probe bool
}

// snapshot is one immutable breaker state. Every transition replaces the
// whole snapshot with a compare-and-swap, so the state, its generation, the
// probe accounting and the open time always change together.
type snapshot struct {
	state     BreakerState
	gen       uint64
	probes    int // half-open: slots handed out and not released
	successes int // half-open: probes that succeeded
	openedAt  time.Time
}

// breaker is a circuit breaker. A nil *breaker admits everything.
type breaker struct {
	cur atomic.Pointer[snapshot]

	mu     sync.Mutex // guards win / winGen
	win    window     // closed state: a = requests, b = failures
	winGen uint64     // the generation win counts for

	ratio       float64
	minRequests int
	openTimeout time.Duration
	maxProbes   int
	now         func() time.Time
	onChange    func(from, to BreakerState)
}

func newBreaker(cfg *TargetConfig, now func() time.Time, onChange func(from, to BreakerState)) *breaker {
	if cfg.BreakerFailureRatio <= 0 {
		return nil
	}
	b := &breaker{
		win:         newWindow(cfg.BreakerWindow),
		ratio:       cfg.BreakerFailureRatio,
		minRequests: cfg.BreakerMinRequests,
		openTimeout: cfg.BreakerOpenTimeout,
		maxProbes:   cfg.BreakerHalfOpenProbes,
		now:         now,
		onChange:    onChange,
	}
	b.cur.Store(&snapshot{state: StateClosed})
	return b
}

// state returns the current state.
func (b *breaker) state() BreakerState {
	if b == nil {
		return StateClosed
	}
	return b.cur.Load().state
}

// allow admits a request, returning its ticket, or reports false when the
// breaker rejects it.
func (b *breaker) allow() (ticket, bool) {
	if b == nil {
		return ticket{}, true
	}
	for {
		s := b.cur.Load()
		switch s.state {
		case StateClosed:
			return ticket{gen: s.gen}, true
		case StateOpen:
			if b.now().Sub(s.openedAt) < b.openTimeout {
				return ticket{}, false
			}
			// The CAS winner moves to half-open and takes the first probe.
			next := &snapshot{state: StateHalfOpen, gen: s.gen + 1, probes: 1}
			if b.swap(s, next) {
				return ticket{gen: next.gen, probe: true}, true
			}
		case StateHalfOpen:
			if s.probes >= b.maxProbes {
				return ticket{}, false
			}
			next := *s
			next.probes++
			if b.cur.CompareAndSwap(s, &next) {
				return ticket{gen: s.gen, probe: true}, true
			}
		}
	}
}

// record reports how the request admitted with t ended.
func (b *breaker) record(t ticket, o outcome) {
	if b == nil {
		return
	}
	if t.probe {
		b.recordProbe(t, o)
		return
	}
	if o == outcomeIgnored {
		return
	}
	s := b.cur.Load()
	if s.state != StateClosed || s.gen != t.gen {
		return // admitted in an earlier generation: its result is stale
	}
	failed := 0
	if o == outcomeFailure {
		failed = 1
	}
	now := b.now()
	b.mu.Lock()
	if b.winGen != s.gen {
		b.win.reset()
		b.winGen = s.gen
	}
	b.win.add(now, 1, failed)
	requests, failures := b.win.sum(now)
	b.mu.Unlock()

	if failed == 1 && requests >= b.minRequests && float64(failures) >= b.ratio*float64(requests) {
		b.swap(s, &snapshot{state: StateOpen, gen: s.gen + 1, openedAt: now})
	}
}

func (b *breaker) recordProbe(t ticket, o outcome) {
	for {
		s := b.cur.Load()
		if s.state != StateHalfOpen || s.gen != t.gen {
			return
		}
		var next *snapshot
		switch o {
		case outcomeIgnored: // release the slot for another probe
			n := *s
			n.probes--
			next = &n
		case outcomeFailure:
			next = &snapshot{state: StateOpen, gen: s.gen + 1, openedAt: b.now()}
		case outcomeSuccess:
			if s.successes+1 >= b.maxProbes {
				next = &snapshot{state: StateClosed, gen: s.gen + 1}
			} else {
				n := *s
				n.successes++
				next = &n
			}
		}
		if next.state != s.state {
			if b.swap(s, next) {
				return
			}
		} else if b.cur.CompareAndSwap(s, next) {
			return
		}
	}
}

// swap is a state transition: a CAS from s to next that notifies onChange
// when it wins.
func (b *breaker) swap(s, next *snapshot) bool {
	if !b.cur.CompareAndSwap(s, next) {
		return false
	}
	if b.onChange != nil {
		b.onChange(s.state, next.state)
	}
	return true
}
