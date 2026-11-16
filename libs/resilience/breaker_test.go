package resilience

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type transition struct{ from, to State }

func testBreaker(t *testing.T, mut func(*BreakerConfig)) (*Breaker, *fakeClock, func() []transition) {
	t.Helper()

	tc := DefaultBreakerConfig()
	tc.MinRequests = 4
	tc.FailureRatio = 0.5
	tc.Window = 10 * time.Second

	tc.OpenTimeout = 5 * time.Second
	if mut != nil {
		mut(&tc)
	}

	clk := newFakeClock()

	var (
		mu   sync.Mutex
		seen []transition
	)

	b := NewBreaker(tc, clk.now, func(from, to State) {
		mu.Lock()

		seen = append(seen, transition{from, to})
		mu.Unlock()
	})
	require.NotNil(t, b)

	return b, clk, func() []transition {
		mu.Lock()
		defer mu.Unlock()

		return append([]transition(nil), seen...)
	}
}

// admit is b.Allow() that must succeed.
func admit(t *testing.T, b *Breaker) Ticket {
	t.Helper()

	tk, ok := b.Allow()
	require.True(t, ok, "breaker rejected in state %s", b.State())

	return tk
}

func run(t *testing.T, b *Breaker, o Outcome, n int) {
	t.Helper()

	for range n {
		b.Record(admit(t, b), o)
	}
}

// trip opens b with minRequests failures.
func trip(t *testing.T, b *Breaker) {
	t.Helper()
	run(t, b, OutcomeFailure, b.minRequests)
	require.Equal(t, StateOpen, b.State())
}

func TestBreaker_Disabled(t *testing.T) {
	t.Parallel()

	tc := DefaultBreakerConfig()
	tc.FailureRatio = 0
	b := NewBreaker(tc, time.Now, nil)
	require.Nil(t, b)
	tk, ok := b.Allow()
	assert.True(t, ok)
	b.Record(tk, OutcomeFailure) // no panic
	assert.Equal(t, StateClosed, b.State())
	b.Mirror(func(s State) { assert.Equal(t, StateClosed, s) })

	ran := false

	require.NoError(t, b.Do(func() Outcome { ran = true; return OutcomeFailure }))
	assert.True(t, ran, "no breaker: the request runs")

	tc.FailureRatio = -0.5
	assert.Nil(t, NewBreaker(tc, time.Now, nil), "a ratio below 0 is no breaker either")
}

func TestBreaker_DoRecordsWhatTheRequestReturns(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, nil)
	failing := func() Outcome { return OutcomeFailure }

	for range b.minRequests {
		require.NoError(t, b.Do(failing))
	}

	require.Equal(t, StateOpen, b.State())
	require.ErrorIs(t, b.Do(func() Outcome { panic("a rejected request must not run") }), ErrOpen)

	clk.advance(5 * time.Second)
	require.NoError(t, b.Do(func() Outcome { return OutcomeIgnored }))
	assert.Equal(t, StateHalfOpen, b.State(), "an ignored probe gives its slot back")
	require.NoError(t, b.Do(func() Outcome { return OutcomeSuccess }))
	assert.Equal(t, StateClosed, b.State())
}

// A probe that panics is a failed probe: the breaker re-opens and probes
// again after the open timeout, instead of waiting for a result for good.
func TestBreaker_DoRecordsAPanickingProbeAsAFailure(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, nil)
	trip(t, b)
	clk.advance(5 * time.Second)

	assert.PanicsWithValue(t, "round tripper blew up", func() {
		_ = b.Do(func() Outcome { panic("round tripper blew up") })
	})
	assert.Equal(t, StateOpen, b.State())

	clk.advance(5 * time.Second)
	require.NoError(t, b.Do(func() Outcome { return OutcomeSuccess }), "the next probe is admitted")
	assert.Equal(t, StateClosed, b.State())
}

// Transitions notify concurrently: a writer that read the state before a
// transition must not overwrite the one that read it after.
func TestBreaker_MirrorEndsOnTheLatestState(t *testing.T) {
	t.Parallel()

	b, _, _ := testBreaker(t, nil)

	var (
		mirrored        State // written only inside Mirror
		wg              sync.WaitGroup
		reading, resume = make(chan struct{}), make(chan struct{})
	)

	wg.Go(func() {
		b.Mirror(func(s State) {
			close(reading)
			<-resume

			mirrored = s
		})
	})

	<-reading
	trip(t, b)
	wg.Go(func() { b.Mirror(func(s State) { mirrored = s }) })
	close(resume)
	wg.Wait()

	assert.Equal(t, StateOpen, mirrored)
}

// Results recorded from many goroutines while closed are all counted.
func TestBreaker_ConcurrentRecordsAreAllCounted(t *testing.T) {
	t.Parallel()

	const goroutines, each = 8, 2000

	b, clk, _ := testBreaker(t, nil)

	var wg sync.WaitGroup

	for range goroutines {
		wg.Go(func() {
			for range each {
				if tk, ok := b.Allow(); ok {
					b.Record(tk, OutcomeSuccess)
				}
			}
		})
	}

	wg.Wait()

	requests, failures := b.win.sum(clk.now())
	assert.Equal(t, [2]int{goroutines * each, 0}, [2]int{requests, failures})
}

func TestBreaker_TripsOnRatioAfterMinRequests(t *testing.T) {
	t.Parallel()

	b, _, seen := testBreaker(t, nil)

	run(t, b, OutcomeFailure, 3)
	assert.Equal(t, StateClosed, b.State(), "3 < min_requests: not evaluated yet")

	run(t, b, OutcomeFailure, 1)
	assert.Equal(t, StateOpen, b.State(), "4/4 failures ≥ 0.5")
	assert.Equal(t, []transition{{StateClosed, StateOpen}}, seen())

	_, ok := b.Allow()
	assert.False(t, ok, "open rejects")
}

func TestBreaker_RatioIsInclusiveAndSuccessesDilute(t *testing.T) {
	t.Parallel()

	b, _, _ := testBreaker(t, nil)
	run(t, b, OutcomeSuccess, 2)
	run(t, b, OutcomeFailure, 1)
	assert.Equal(t, StateClosed, b.State(), "1/3 below min_requests")
	run(t, b, OutcomeFailure, 1)
	assert.Equal(t, StateOpen, b.State(), "2/4 = 0.5 trips (≥)")

	b, _, _ = testBreaker(t, nil)
	run(t, b, OutcomeSuccess, 3)
	run(t, b, OutcomeFailure, 2)
	assert.Equal(t, StateClosed, b.State(), "2/5 < 0.5")
}

func TestBreaker_SuccessNeverTrips(t *testing.T) {
	t.Parallel()

	b, _, _ := testBreaker(t, func(tc *BreakerConfig) { tc.MinRequests = 1; tc.FailureRatio = 1 })
	run(t, b, OutcomeFailure, 0)
	b.Record(admit(t, b), OutcomeFailure)
	assert.Equal(t, StateOpen, b.State(), "ratio 1 trips when every request failed")

	b, _, _ = testBreaker(t, func(tc *BreakerConfig) { tc.MinRequests = 1; tc.FailureRatio = 0.01 })
	run(t, b, OutcomeSuccess, 50)
	assert.Equal(t, StateClosed, b.State(), "only a failure evaluates the ratio")
}

func TestBreaker_IgnoredOutcomesAreNotCounted(t *testing.T) {
	t.Parallel()

	b, _, _ := testBreaker(t, nil)
	run(t, b, OutcomeIgnored, 100)
	run(t, b, OutcomeFailure, 3)
	assert.Equal(t, StateClosed, b.State(), "ignored requests add neither to requests nor failures")
}

func TestBreaker_WindowSlides(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, nil)
	run(t, b, OutcomeFailure, 3)
	clk.advance(11 * time.Second)
	run(t, b, OutcomeFailure, 3)
	assert.Equal(t, StateClosed, b.State(), "the first failures left the window")
	run(t, b, OutcomeFailure, 1)
	assert.Equal(t, StateOpen, b.State())
}

func TestBreaker_OpenTimeoutThenProbe(t *testing.T) {
	t.Parallel()

	b, clk, seen := testBreaker(t, nil)
	trip(t, b)

	clk.advance(5*time.Second - time.Nanosecond)

	_, ok := b.Allow()
	require.False(t, ok, "still open just before the timeout")

	clk.advance(time.Nanosecond)

	probe := admit(t, b)
	assert.True(t, probe.probe)
	assert.Equal(t, StateHalfOpen, b.State())
	_, ok = b.Allow()
	assert.False(t, ok, "one probe only")

	b.Record(probe, OutcomeSuccess)
	assert.Equal(t, StateClosed, b.State())
	assert.Equal(t, []transition{{StateClosed, StateOpen}, {StateOpen, StateHalfOpen}, {StateHalfOpen, StateClosed}}, seen())

	run(t, b, OutcomeFailure, 3)
	assert.Equal(t, StateClosed, b.State(), "closing starts a fresh window")
}

func TestBreaker_FailedProbeReopens(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, nil)
	trip(t, b)
	clk.advance(5 * time.Second)

	probe := admit(t, b)

	clk.advance(time.Second)
	b.Record(probe, OutcomeFailure)
	require.Equal(t, StateOpen, b.State())

	clk.advance(4*time.Second + 999*time.Millisecond)

	_, ok := b.Allow()
	assert.False(t, ok, "the open timeout restarts at the re-open")
	clk.advance(time.Millisecond)

	_, ok = b.Allow()
	assert.True(t, ok)
}

func TestBreaker_ProbeLocalRejectionReleasesSlot(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, nil)
	trip(t, b)
	clk.advance(5 * time.Second)

	probe := admit(t, b)
	b.Record(probe, OutcomeIgnored)
	assert.Equal(t, StateHalfOpen, b.State())

	again := admit(t, b)
	assert.True(t, again.probe, "the released slot goes to the next request")

	_, ok := b.Allow()
	assert.False(t, ok)
	b.Record(again, OutcomeSuccess)
	assert.Equal(t, StateClosed, b.State())
}

func TestBreaker_StaleResultsDoNotDecide(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, nil)
	old := admit(t, b) // admitted while closed
	trip(t, b)
	clk.advance(5 * time.Second)

	probe := admit(t, b)

	b.Record(old, OutcomeFailure)
	assert.Equal(t, StateHalfOpen, b.State(), "a closed-era failure does not re-open half-open")
	b.Record(old, OutcomeSuccess)
	assert.Equal(t, StateHalfOpen, b.State(), "nor does a closed-era success close it")

	b.Record(probe, OutcomeSuccess)
	require.Equal(t, StateClosed, b.State())

	b.Record(probe, OutcomeFailure)
	assert.Equal(t, StateClosed, b.State(), "a probe result after the half-open ended is stale")

	for range b.minRequests {
		b.Record(old, OutcomeFailure)
	}

	assert.Equal(t, StateClosed, b.State(), "a ticket of an earlier closed period does not count in this one")
	run(t, b, OutcomeFailure, 3)
	assert.Equal(t, StateClosed, b.State(), "and it was not counted in the new window either")
}

func TestBreaker_MultipleProbesMustAllSucceed(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, func(tc *BreakerConfig) { tc.HalfOpenProbes = 3 })
	trip(t, b)
	clk.advance(5 * time.Second)

	probe1, probe2, probe3 := admit(t, b), admit(t, b), admit(t, b)
	_, ok := b.Allow()
	require.False(t, ok, "exactly 3 probes")

	b.Record(probe1, OutcomeSuccess)
	b.Record(probe2, OutcomeSuccess)
	assert.Equal(t, StateHalfOpen, b.State(), "2 of 3")
	_, ok = b.Allow()
	assert.False(t, ok, "a finished probe does not free its slot")
	b.Record(probe3, OutcomeSuccess)
	assert.Equal(t, StateClosed, b.State())

	trip(t, b)
	clk.advance(5 * time.Second)

	probe1, probe2 = admit(t, b), admit(t, b)
	b.Record(probe1, OutcomeSuccess)
	b.Record(probe2, OutcomeFailure)
	assert.Equal(t, StateOpen, b.State(), "any failed probe re-opens")
}

func TestBreaker_ConcurrentHalfOpenAdmitsExactlyTheProbes(t *testing.T) {
	t.Parallel()

	for _, probes := range []int{1, 3} {
		b, clk, seen := testBreaker(t, func(tc *BreakerConfig) { tc.HalfOpenProbes = probes })
		trip(t, b)
		clk.advance(5 * time.Second)

		const n = 64

		var admitted atomic.Int32

		tickets := make(chan Ticket, n)

		var start, wg sync.WaitGroup
		start.Add(1)

		for range n {
			wg.Go(func() {
				start.Wait()

				if tk, ok := b.Allow(); ok {
					admitted.Add(1)

					tickets <- tk
				}
			})
		}

		start.Done()
		wg.Wait()
		close(tickets)

		assert.Equal(t, int32(probes), admitted.Load(), "probes=%d", probes)

		for tk := range tickets {
			assert.True(t, tk.probe)
			b.Record(tk, OutcomeSuccess)
		}

		assert.Equal(t, StateClosed, b.State())
		assert.Len(t, seen(), 3, "closed→open→half-open→closed, one transition each")
	}
}

func TestState_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "closed", StateClosed.String())
	assert.Equal(t, "open", StateOpen.String())
	assert.Equal(t, "half_open", StateHalfOpen.String())
	assert.Equal(t, "unknown", State(9).String())
}

// The generation is what makes a late result stale, so every transition must
// start a new one — never return to a number an earlier Ticket may carry.
func TestBreaker_EveryTransitionStartsANewGeneration(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, nil)
	gen := func() uint64 { return b.cur.Load().gen }

	require.Equal(t, uint64(0), gen())
	trip(t, b)
	require.Equal(t, uint64(1), gen(), "closed → open")

	clk.advance(5 * time.Second)

	probe := admit(t, b)
	require.Equal(t, uint64(2), gen(), "open → half-open")
	assert.Equal(t, uint64(2), probe.gen)

	b.Record(probe, OutcomeFailure)
	require.Equal(t, uint64(3), gen(), "half-open → open")

	clk.advance(5 * time.Second)

	probe = admit(t, b)
	require.Equal(t, uint64(4), gen())
	b.Record(probe, OutcomeSuccess)
	require.Equal(t, StateClosed, b.State())
	require.Equal(t, uint64(5), gen(), "half-open → closed")
}

// A probe admitted in one half-open period must not decide the next one.
func TestBreaker_ProbeOfAnEarlierHalfOpenDoesNotDecide(t *testing.T) {
	t.Parallel()

	b, clk, _ := testBreaker(t, func(tc *BreakerConfig) { tc.HalfOpenProbes = 2 })
	trip(t, b)
	clk.advance(5 * time.Second)

	slow, failing := admit(t, b), admit(t, b)
	b.Record(failing, OutcomeFailure)
	require.Equal(t, StateOpen, b.State())

	clk.advance(5 * time.Second)

	fresh := admit(t, b)
	require.Equal(t, StateHalfOpen, b.State())

	b.Record(slow, OutcomeSuccess)
	b.Record(fresh, OutcomeSuccess)
	assert.Equal(t, StateHalfOpen, b.State(), "one fresh success of two: the earlier period's probe did not count")

	b.Record(slow, OutcomeFailure)
	assert.Equal(t, StateHalfOpen, b.State(), "nor can it re-open the breaker")

	b.Record(admit(t, b), OutcomeSuccess)
	assert.Equal(t, StateClosed, b.State())
}

func TestBreakerConfig_Validate(t *testing.T) {
	t.Parallel()

	require.NoError(t, DefaultBreakerConfig().Validate())
	require.NoError(t, BreakerConfig{}.Validate(), "no breaker: nothing to validate")
	require.NoError(t, BreakerConfig{FailureRatio: 1, MinRequests: 1, Window: 1, OpenTimeout: 1, HalfOpenProbes: 1}.Validate(),
		"the bounds are inclusive")

	for _, tc := range []struct {
		name string
		mut  func(*BreakerConfig)
		msg  string
	}{
		{"ratio low", func(c *BreakerConfig) { c.FailureRatio = -0.1 }, "failure_ratio"},
		{"ratio high", func(c *BreakerConfig) { c.FailureRatio = 1.01 }, "failure_ratio"},
		{"ratio NaN", func(c *BreakerConfig) { c.FailureRatio = math.NaN() }, "failure_ratio"},
		{"min requests", func(c *BreakerConfig) { c.MinRequests = 0 }, "min_requests"},
		{"window", func(c *BreakerConfig) { c.Window = 0 }, "window"},
		{"open timeout", func(c *BreakerConfig) { c.OpenTimeout = 0 }, "window"},
		{"probes", func(c *BreakerConfig) { c.HalfOpenProbes = 0 }, "half_open_probes"},
	} {
		cfg := DefaultBreakerConfig()
		tc.mut(&cfg)

		err := cfg.Validate()
		if assert.Error(t, err, tc.name) {
			assert.Contains(t, err.Error(), tc.msg, tc.name)
			assert.NotContains(t, err.Error(), "\n", "%s: one problem", tc.name)
		}
	}

	all := BreakerConfig{FailureRatio: 0.5}.Validate()
	for _, key := range []string{"min_requests", "window", "half_open_probes"} {
		assert.ErrorContains(t, all, key, "every problem is reported")
	}
}

// A Record that read the state, was descheduled while the breaker opened,
// recovered and started counting again, and only then reached the window,
// must leave the new period's counts alone.
func TestBreaker_LateRecordKeepsTheNewWindow(t *testing.T) {
	t.Parallel()

	var (
		b     *Breaker
		clk   = newFakeClock()
		cycle func()
	)

	b = NewBreaker(BreakerConfig{FailureRatio: 0.6, MinRequests: 5, Window: 10 * time.Second, OpenTimeout: 5 * time.Second, HalfOpenProbes: 1},
		func() time.Time {
			if run := cycle; run != nil {
				cycle = nil

				run()
			}

			return clk.now()
		}, nil)

	late := admit(t, b)
	// Runs inside the late Record's clock read: after it loaded the state,
	// before it takes the window's lock.
	cycle = func() {
		trip(t, b)
		clk.advance(5 * time.Second)
		b.Record(admit(t, b), OutcomeSuccess)
		require.Equal(t, StateClosed, b.State())
		run(t, b, OutcomeFailure, 4)
	}

	b.Record(late, OutcomeSuccess)
	require.Equal(t, StateClosed, b.State())

	run(t, b, OutcomeFailure, 1)
	assert.Equal(t, StateOpen, b.State(), "5 failures of 5 in the new period")
}

func BenchmarkBreaker_AllowRecord(b *testing.B) {
	br := NewBreaker(DefaultBreakerConfig(), time.Now, nil)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if tk, ok := br.Allow(); ok {
				br.Record(tk, OutcomeSuccess)
			}
		}
	})
}
