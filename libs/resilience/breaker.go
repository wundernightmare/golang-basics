package resilience

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// State is a circuit breaker's state; its value is what the
// *_circuit_breaker_state gauges report.
type State int

// The breaker states.
const (
	// StateClosed lets every request through and counts failures.
	StateClosed State = iota
	// StateOpen rejects every request until the open timeout elapses.
	StateOpen
	// StateHalfOpen admits a fixed number of probes; they decide whether the
	// breaker closes (all succeed) or re-opens (any fails).
	StateHalfOpen
)

// String returns the state's label value.
func (s State) String() string {
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

// Outcome is what a finished request tells the breaker.
type Outcome int

const (
	// OutcomeSuccess is: the dependency answered — whatever it said that is not
	// its own failure (HTTP 2xx–4xx, a gRPC status such as NotFound).
	OutcomeSuccess Outcome = iota
	// OutcomeFailure is: the dependency failed or did not answer in time (HTTP
	// 5xx, gRPC Unavailable, a timeout, a connection error, the caller's
	// deadline passing mid-request).
	OutcomeFailure
	// OutcomeIgnored is: the request says nothing about the dependency — the caller
	// cancelled, or a local limit rejected the request before it was sent.
	// It releases a half-open probe slot.
	OutcomeIgnored
)

// Ticket is what [Breaker.Allow] hands out and [Breaker.Record] takes back:
// the generation the request was admitted in, and whether it holds a probe
// slot. Results from an older generation are ignored.
type Ticket struct {
	gen   uint64
	probe bool
}

// snapshot is one immutable breaker state. Every transition replaces the
// whole snapshot with a compare-and-swap, so the state, its generation, the
// probe accounting and the open time always change together.
type snapshot struct {
	state     State
	gen       uint64
	probes    int // half-open: slots handed out and not released
	successes int // half-open: probes that succeeded
	openedAt  time.Time
}

// BreakerConfig is a circuit breaker's policy. Its yaml keys are the ones
// every client's target config uses, so a config can inline it.
type BreakerConfig struct {
	// FailureRatio trips the breaker when failures / requests in the window
	// reach it, in (0, 1]; 0 = no breaker.
	FailureRatio float64 `yaml:"breaker_failure_ratio"`
	// MinRequests is the least requests in the window before the ratio is
	// evaluated (≥ 1 with a breaker).
	MinRequests int `yaml:"breaker_min_requests"`
	// Window is the sliding window the ratio is measured over (> 0 with a
	// breaker).
	Window time.Duration `yaml:"breaker_window"`
	// OpenTimeout is how long the breaker stays open before admitting probes
	// (> 0 with a breaker).
	OpenTimeout time.Duration `yaml:"breaker_open_timeout"`
	// HalfOpenProbes is how many probes half-open admits; all must succeed
	// to close it, any failure re-opens it (≥ 1 with a breaker).
	HalfOpenProbes int `yaml:"breaker_half_open_probes"`
}

// DefaultBreakerConfig trips at half the requests failing, of at least 20 in
// 10s, stays open 30s and then admits one probe.
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureRatio:   0.5,
		MinRequests:    20,
		Window:         10 * time.Second,
		OpenTimeout:    30 * time.Second,
		HalfOpenProbes: 1,
	}
}

// Validate reports every problem in c, by its yaml key; nil when there is
// none. With FailureRatio 0 (no breaker) the other fields are free.
func (c BreakerConfig) Validate() []error {
	if c.FailureRatio < 0 || c.FailureRatio > 1 {
		return []error{errors.New("breaker_failure_ratio must be in [0, 1]")}
	}

	if c.FailureRatio == 0 {
		return nil
	}

	var errs []error

	if c.MinRequests < 1 {
		errs = append(errs, errors.New("breaker_min_requests must be ≥ 1"))
	}

	if c.Window <= 0 || c.OpenTimeout <= 0 {
		errs = append(errs, errors.New("breaker_window and breaker_open_timeout must be > 0"))
	}

	if c.HalfOpenProbes < 1 {
		errs = append(errs, errors.New("breaker_half_open_probes must be ≥ 1"))
	}

	return errs
}

// Breaker is a circuit breaker: closed / open / half-open, lock-free on the
// admission path. A nil *Breaker admits everything, so "no breaker" needs no
// branch at the call site.
//
//	grant, ok := b.Allow()
//	if !ok { /* reject: the dependency is down */ }
//	// … send …
//	b.Record(grant, resilience.OutcomeFailure) // exactly once per granted ticket
type Breaker struct {
	cur atomic.Pointer[snapshot]

	mu     sync.Mutex // guards win / winGen
	win    window     // closed state: a = requests, b = failures
	winGen uint64     // the generation win counts for

	ratio       float64
	minRequests int
	openTimeout time.Duration
	maxProbes   int
	now         func() time.Time
	onChange    func(from, to State)
}

// NewBreaker builds a breaker from cfg, which must pass
// [BreakerConfig.Validate]; nil (admit everything) when cfg.FailureRatio is
// 0. now is the clock (time.Now); onChange, if not nil, is called after every
// state transition — concurrently, possibly out of order.
func NewBreaker(cfg BreakerConfig, now func() time.Time, onChange func(from, to State)) *Breaker {
	if cfg.FailureRatio <= 0 {
		return nil
	}

	b := &Breaker{
		win:         newWindow(cfg.Window),
		ratio:       cfg.FailureRatio,
		minRequests: cfg.MinRequests,
		openTimeout: cfg.OpenTimeout,
		maxProbes:   cfg.HalfOpenProbes,
		now:         now,
		onChange:    onChange,
	}
	b.cur.Store(&snapshot{state: StateClosed})

	return b
}

// State returns the current state.
func (b *Breaker) State() State {
	if b == nil {
		return StateClosed
	}

	return b.cur.Load().state
}

// Allow admits a request, returning its ticket, or reports false when the
// breaker rejects it.
func (b *Breaker) Allow() (Ticket, bool) {
	if b == nil {
		return Ticket{}, true
	}

	for {
		s := b.cur.Load()
		switch s.state {
		case StateClosed:
			return Ticket{gen: s.gen}, true
		case StateOpen:
			if b.now().Sub(s.openedAt) < b.openTimeout {
				return Ticket{}, false
			}
			// The CAS winner moves to half-open and takes the first probe.
			next := &snapshot{state: StateHalfOpen, gen: s.gen + 1, probes: 1}
			if b.swap(s, next) {
				return Ticket{gen: next.gen, probe: true}, true
			}
		case StateHalfOpen:
			if s.probes >= b.maxProbes {
				return Ticket{}, false
			}

			next := *s

			next.probes++
			if b.cur.CompareAndSwap(s, &next) {
				return Ticket{gen: s.gen, probe: true}, true
			}
		}
	}
}

// Record reports how the request admitted with t ended. Every granted ticket
// must be recorded exactly once: an unrecorded probe keeps its half-open slot.
func (b *Breaker) Record(t Ticket, result Outcome) {
	if b == nil {
		return
	}

	if t.probe {
		b.recordProbe(t, result)
		return
	}

	if result == OutcomeIgnored {
		return
	}

	s := b.cur.Load()
	if s.state != StateClosed || s.gen != t.gen {
		return // admitted in an earlier generation: its result is stale
	}

	failed := 0
	if result == OutcomeFailure {
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

func (b *Breaker) recordProbe(t Ticket, o Outcome) {
	for {
		s := b.cur.Load()
		if s.state != StateHalfOpen || s.gen != t.gen {
			return
		}

		var next *snapshot

		switch o {
		case OutcomeIgnored: // release the slot for another probe
			n := *s
			n.probes--
			next = &n
		case OutcomeFailure:
			next = &snapshot{state: StateOpen, gen: s.gen + 1, openedAt: b.now()}
		case OutcomeSuccess:
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
func (b *Breaker) swap(s, next *snapshot) bool {
	if !b.cur.CompareAndSwap(s, next) {
		return false
	}

	if b.onChange != nil {
		b.onChange(s.state, next.state)
	}

	return true
}
