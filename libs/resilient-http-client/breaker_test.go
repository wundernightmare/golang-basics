package resilient

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a settable clock for the breaker and the retry budget.
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

type transition struct{ from, to BreakerState }

func testBreaker(t *testing.T, mut func(*TargetConfig)) (*breaker, *fakeClock, func() []transition) {
	t.Helper()
	tc := DefaultTarget("t")
	tc.BreakerMinRequests = 4
	tc.BreakerFailureRatio = 0.5
	tc.BreakerWindow = 10 * time.Second
	tc.BreakerOpenTimeout = 5 * time.Second
	if mut != nil {
		mut(&tc)
	}
	clk := newFakeClock()
	var mu sync.Mutex
	var seen []transition
	b := newBreaker(&tc, clk.now, func(from, to BreakerState) {
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

// admit is allow() that must succeed.
func admit(t *testing.T, b *breaker) ticket {
	t.Helper()
	tk, ok := b.allow()
	require.True(t, ok, "breaker rejected in state %s", b.state())
	return tk
}

func run(t *testing.T, b *breaker, o outcome, n int) {
	t.Helper()
	for range n {
		b.record(admit(t, b), o)
	}
}

// trip opens b with minRequests failures.
func trip(t *testing.T, b *breaker) {
	t.Helper()
	run(t, b, outcomeFailure, b.minRequests)
	require.Equal(t, StateOpen, b.state())
}

func TestBreaker_Disabled(t *testing.T) {
	tc := DefaultTarget("t")
	tc.BreakerFailureRatio = 0
	b := newBreaker(&tc, time.Now, nil)
	require.Nil(t, b)
	tk, ok := b.allow()
	assert.True(t, ok)
	b.record(tk, outcomeFailure) // no panic
	assert.Equal(t, StateClosed, b.state())
}

func TestBreaker_TripsOnRatioAfterMinRequests(t *testing.T) {
	b, _, seen := testBreaker(t, nil)

	run(t, b, outcomeFailure, 3)
	assert.Equal(t, StateClosed, b.state(), "3 < min_requests: not evaluated yet")

	run(t, b, outcomeFailure, 1)
	assert.Equal(t, StateOpen, b.state(), "4/4 failures ≥ 0.5")
	assert.Equal(t, []transition{{StateClosed, StateOpen}}, seen())

	_, ok := b.allow()
	assert.False(t, ok, "open rejects")
}

func TestBreaker_RatioIsInclusiveAndSuccessesDilute(t *testing.T) {
	b, _, _ := testBreaker(t, nil)
	run(t, b, outcomeSuccess, 2)
	run(t, b, outcomeFailure, 1)
	assert.Equal(t, StateClosed, b.state(), "1/3 below min_requests")
	run(t, b, outcomeFailure, 1)
	assert.Equal(t, StateOpen, b.state(), "2/4 = 0.5 trips (≥)")

	b, _, _ = testBreaker(t, nil)
	run(t, b, outcomeSuccess, 3)
	run(t, b, outcomeFailure, 2)
	assert.Equal(t, StateClosed, b.state(), "2/5 < 0.5")
}

func TestBreaker_SuccessNeverTrips(t *testing.T) {
	b, _, _ := testBreaker(t, func(tc *TargetConfig) { tc.BreakerMinRequests = 1; tc.BreakerFailureRatio = 1 })
	run(t, b, outcomeFailure, 0)
	b.record(admit(t, b), outcomeFailure)
	assert.Equal(t, StateOpen, b.state(), "ratio 1 trips when every request failed")

	b, _, _ = testBreaker(t, func(tc *TargetConfig) { tc.BreakerMinRequests = 1; tc.BreakerFailureRatio = 0.01 })
	run(t, b, outcomeSuccess, 50)
	assert.Equal(t, StateClosed, b.state(), "only a failure evaluates the ratio")
}

func TestBreaker_IgnoredOutcomesAreNotCounted(t *testing.T) {
	b, _, _ := testBreaker(t, nil)
	run(t, b, outcomeIgnored, 100)
	run(t, b, outcomeFailure, 3)
	assert.Equal(t, StateClosed, b.state(), "ignored requests add neither to requests nor failures")
}

func TestBreaker_WindowSlides(t *testing.T) {
	b, clk, _ := testBreaker(t, nil)
	run(t, b, outcomeFailure, 3)
	clk.advance(11 * time.Second)
	run(t, b, outcomeFailure, 3)
	assert.Equal(t, StateClosed, b.state(), "the first failures left the window")
	run(t, b, outcomeFailure, 1)
	assert.Equal(t, StateOpen, b.state())
}

func TestBreaker_OpenTimeoutThenProbe(t *testing.T) {
	b, clk, seen := testBreaker(t, nil)
	trip(t, b)

	clk.advance(5*time.Second - time.Nanosecond)
	_, ok := b.allow()
	require.False(t, ok, "still open just before the timeout")

	clk.advance(time.Nanosecond)
	probe := admit(t, b)
	assert.True(t, probe.probe)
	assert.Equal(t, StateHalfOpen, b.state())
	_, ok = b.allow()
	assert.False(t, ok, "one probe only")

	b.record(probe, outcomeSuccess)
	assert.Equal(t, StateClosed, b.state())
	assert.Equal(t, []transition{{StateClosed, StateOpen}, {StateOpen, StateHalfOpen}, {StateHalfOpen, StateClosed}}, seen())

	run(t, b, outcomeFailure, 3)
	assert.Equal(t, StateClosed, b.state(), "closing starts a fresh window")
}

func TestBreaker_FailedProbeReopens(t *testing.T) {
	b, clk, _ := testBreaker(t, nil)
	trip(t, b)
	clk.advance(5 * time.Second)
	probe := admit(t, b)
	clk.advance(time.Second)
	b.record(probe, outcomeFailure)
	require.Equal(t, StateOpen, b.state())

	clk.advance(4*time.Second + 999*time.Millisecond)
	_, ok := b.allow()
	assert.False(t, ok, "the open timeout restarts at the re-open")
	clk.advance(time.Millisecond)
	_, ok = b.allow()
	assert.True(t, ok)
}

func TestBreaker_ProbeLocalRejectionReleasesSlot(t *testing.T) {
	b, clk, _ := testBreaker(t, nil)
	trip(t, b)
	clk.advance(5 * time.Second)

	probe := admit(t, b)
	b.record(probe, outcomeIgnored)
	assert.Equal(t, StateHalfOpen, b.state())

	again := admit(t, b)
	assert.True(t, again.probe, "the released slot goes to the next request")
	_, ok := b.allow()
	assert.False(t, ok)
	b.record(again, outcomeSuccess)
	assert.Equal(t, StateClosed, b.state())
}

func TestBreaker_StaleResultsDoNotDecide(t *testing.T) {
	b, clk, _ := testBreaker(t, nil)
	old := admit(t, b) // admitted while closed
	trip(t, b)
	clk.advance(5 * time.Second)
	probe := admit(t, b)

	b.record(old, outcomeFailure)
	assert.Equal(t, StateHalfOpen, b.state(), "a closed-era failure does not re-open half-open")
	b.record(old, outcomeSuccess)
	assert.Equal(t, StateHalfOpen, b.state(), "nor does a closed-era success close it")

	b.record(probe, outcomeSuccess)
	require.Equal(t, StateClosed, b.state())

	b.record(probe, outcomeFailure)
	assert.Equal(t, StateClosed, b.state(), "a probe result after the half-open ended is stale")
	run(t, b, outcomeFailure, 3)
	assert.Equal(t, StateClosed, b.state(), "and it was not counted in the new window either")
}

func TestBreaker_MultipleProbesMustAllSucceed(t *testing.T) {
	b, clk, _ := testBreaker(t, func(tc *TargetConfig) { tc.BreakerHalfOpenProbes = 3 })
	trip(t, b)
	clk.advance(5 * time.Second)

	p1, p2, p3 := admit(t, b), admit(t, b), admit(t, b)
	_, ok := b.allow()
	require.False(t, ok, "exactly 3 probes")

	b.record(p1, outcomeSuccess)
	b.record(p2, outcomeSuccess)
	assert.Equal(t, StateHalfOpen, b.state(), "2 of 3")
	_, ok = b.allow()
	assert.False(t, ok, "a finished probe does not free its slot")
	b.record(p3, outcomeSuccess)
	assert.Equal(t, StateClosed, b.state())

	trip(t, b)
	clk.advance(5 * time.Second)
	p1, p2 = admit(t, b), admit(t, b)
	b.record(p1, outcomeSuccess)
	b.record(p2, outcomeFailure)
	assert.Equal(t, StateOpen, b.state(), "any failed probe re-opens")
}

func TestBreaker_ConcurrentHalfOpenAdmitsExactlyTheProbes(t *testing.T) {
	for _, probes := range []int{1, 3} {
		b, clk, seen := testBreaker(t, func(tc *TargetConfig) { tc.BreakerHalfOpenProbes = probes })
		trip(t, b)
		clk.advance(5 * time.Second)

		const n = 64
		var admitted atomic.Int32
		tickets := make(chan ticket, n)
		var start, wg sync.WaitGroup
		start.Add(1)
		for range n {
			wg.Go(func() {
				start.Wait()
				if tk, ok := b.allow(); ok {
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
			b.record(tk, outcomeSuccess)
		}
		assert.Equal(t, StateClosed, b.state())
		assert.Len(t, seen(), 3, "closed→open→half-open→closed, one transition each")
	}
}

func TestBreakerState_String(t *testing.T) {
	assert.Equal(t, "closed", StateClosed.String())
	assert.Equal(t, "open", StateOpen.String())
	assert.Equal(t, "half_open", StateHalfOpen.String())
	assert.Equal(t, "unknown", BreakerState(9).String())
}
