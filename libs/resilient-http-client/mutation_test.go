package resilient

// Tests written against surviving mutants. `just mutate` (gremlins) mutates
// the pure-logic files of this module — backoff.go, circuitbreaker.go,
// adaptive.go — and reports every mutant the suite lets live. Each test here
// pins down a boundary or a branch a mutant had shown to be unobserved; the
// comments name the mutant so the intent survives a refactor.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- backoff ----------------------------------------------------------------

// ARITHMETIC_BASE on `rand.Int64N(int64(ceiling) + 1)`: with `- 1` the ceiling
// itself could never be drawn. The delay is inclusive of the ceiling.
func TestFullJitter_CeilingIsInclusive(t *testing.T) {
	const ceiling = 2 * time.Nanosecond // base 1ns, attempt 1 → 2ns
	seen := map[time.Duration]bool{}
	for range 500 {
		seen[FullJitter(1, time.Nanosecond, time.Second)] = true
	}
	assert.True(t, seen[ceiling], "the ceiling must be a possible outcome")
	assert.True(t, seen[0], "zero must be a possible outcome")
	for d := range seen {
		assert.LessOrEqual(t, d, ceiling)
	}
}

// CONDITIONALS_BOUNDARY on `ceiling < 1`: a one-nanosecond ceiling is still a
// real ceiling, not "no delay".
func TestFullJitter_OneNanosecondCapIsDrawable(t *testing.T) {
	seen := map[time.Duration]bool{}
	for range 500 {
		seen[FullJitter(3, time.Second, time.Nanosecond)] = true
	}
	assert.True(t, seen[time.Nanosecond])
	assert.Len(t, seen, 2, "only 0ns and 1ns are possible")
}

func TestFullJitter_NonPositiveBaseOrCapIsImmediate(t *testing.T) {
	assert.Equal(t, time.Duration(0), FullJitter(3, 0, time.Second))
	assert.Equal(t, time.Duration(0), FullJitter(3, -time.Second, time.Second))
	assert.Equal(t, time.Duration(0), FullJitter(3, time.Second, -time.Second))
}

// --- circuit breaker --------------------------------------------------------

// pinClock replaces the breaker clock for the test and returns a func that
// advances it.
func pinClock(t *testing.T, start int64) func(ms int64) {
	t.Helper()
	now := start
	prev := nowMS
	nowMS = func() int64 { return now }
	t.Cleanup(func() { nowMS = prev })
	return func(ms int64) { now += ms }
}

// CONDITIONALS_BOUNDARY / NEGATION on the threshold validation: exactly 1.0
// is valid, 0 and anything above 1 fall back to the default, and a valid value
// is kept as is.
func TestCB_ThresholdValidation(t *testing.T) {
	assert.Equal(t, defCBThreshold, NewCircuitBreaker(0, 1, time.Second, time.Second).failureThreshold)
	assert.Equal(t, defCBThreshold, NewCircuitBreaker(-0.1, 1, time.Second, time.Second).failureThreshold)
	assert.Equal(t, defCBThreshold, NewCircuitBreaker(1.0001, 1, time.Second, time.Second).failureThreshold)
	assert.Equal(t, 1.0, NewCircuitBreaker(1, 1, time.Second, time.Second).failureThreshold)
	assert.Equal(t, 0.25, NewCircuitBreaker(0.25, 1, time.Second, time.Second).failureThreshold)
}

// CONDITIONALS_BOUNDARY on `nowMS()-openedAt >= halfOpenMS`: the probe is
// admitted exactly when the timeout elapses, not one millisecond later.
func TestCB_HalfOpenExactlyAtTimeout(t *testing.T) {
	advance := pinClock(t, 1_000)
	cb := NewCircuitBreaker(0.5, 2, 10*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure() // → Open at t=1000
	require.Equal(t, CBOpen, cb.State())

	advance(49)
	assert.False(t, cb.Allow(), "49ms: still open")
	advance(1)
	assert.True(t, cb.Allow(), "50ms: exactly the timeout → probe admitted")
	assert.Equal(t, CBHalfOpen, cb.State())
}

// CONDITIONALS_BOUNDARY on `openedAt > 0`: an Open breaker that never recorded
// an opening time (openedAt == 0) must not admit probes.
func TestCB_OpenWithoutOpenedAtNeverAdmits(t *testing.T) {
	pinClock(t, 1_000_000)
	cb := NewCircuitBreaker(0.5, 2, 10*time.Second, time.Millisecond)
	cb.setState(CBOpen) // openedAtMS stays 0
	assert.False(t, cb.Allow())
	assert.Equal(t, CBOpen, cb.State())
}

// CONDITIONALS_BOUNDARY on `now-start >= windowMS`: the window rotates exactly
// when it elapses. Four failures below minRequests, then the fifth request
// either lands in the same window (→ 5 requests, 80% failures, Open) or in a
// fresh one (→ Closed).
func TestCB_WindowRotatesExactlyAtWindowLength(t *testing.T) {
	advance := pinClock(t, 5_000)
	cb := NewCircuitBreaker(0.5, 5, 100*time.Millisecond, time.Minute)
	for range 4 {
		cb.RecordFailure()
	}
	advance(99)
	cb.RecordFailure() // same window: 5 requests, 100% failures → Open
	assert.Equal(t, CBOpen, cb.State())

	advance = pinClock(t, 5_000)
	cb = NewCircuitBreaker(0.5, 5, 100*time.Millisecond, time.Minute)
	for range 4 {
		cb.RecordFailure()
	}
	advance(100)
	cb.RecordFailure() // window rotated: 1 request → Closed
	assert.Equal(t, CBClosed, cb.State())
}

// --- adaptive limiter -------------------------------------------------------

// CONDITIONALS_BOUNDARY / NEGATION on `inFlight < limit` in OnSuccess: a
// success that only brings the limit back up to the current in-flight count
// gives no headroom, so no waiter may be admitted.
func TestAdaptive_OnSuccessAdmitsOnlyWithHeadroom(t *testing.T) {
	l := NewAdaptiveLimiter(2, 1, 5)
	require.NoError(t, l.Acquire(context.Background()))
	require.NoError(t, l.Acquire(context.Background())) // in-flight 2 == limit 2

	admitted := make(chan struct{})
	go func() {
		_ = l.Acquire(context.Background())
		close(admitted)
	}()
	time.Sleep(10 * time.Millisecond) // let the waiter queue

	l.OnFailure() // limit 1, in-flight 2 (surplus)
	l.OnSuccess() // limit 2 == in-flight 2: no headroom
	select {
	case <-admitted:
		t.Fatal("waiter admitted without headroom")
	case <-time.After(30 * time.Millisecond):
	}

	l.OnSuccess() // limit 3 > in-flight 2: headroom → admit
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("waiter not admitted once headroom appeared")
	}
	assert.Equal(t, 3, l.CurrentLimit())
}

// CONDITIONALS_NEGATION on `w == ch` in removeWaiter: cancelling one waiter
// must remove *that* waiter, not the first one in the queue.
func TestAdaptive_CancelRemovesTheRightWaiter(t *testing.T) {
	l := NewAdaptiveLimiter(1, 1, 4)
	require.NoError(t, l.Acquire(context.Background())) // saturate

	first := make(chan struct{})
	go func() {
		_ = l.Acquire(context.Background())
		close(first)
	}()
	time.Sleep(10 * time.Millisecond) // first is queued ahead

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, l.Acquire(ctx), context.DeadlineExceeded) // second waiter gives up

	l.Release() // must go to `first`, the only remaining waiter
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("the surviving waiter was not admitted: the wrong waiter was removed on cancel")
	}
}

func TestAdaptive_ClampsMinAndMax(t *testing.T) {
	l := NewAdaptiveLimiter(3, 0, 0)
	assert.Equal(t, 1, l.CurrentLimit(), "min floored at 1, max floored at min, initial clamped")
	l = NewAdaptiveLimiter(-5, 2, 10)
	assert.Equal(t, 2, l.CurrentLimit())
}

// INCREMENT_DECREMENT on `l.limit++`: a success raises the limit by exactly one.
func TestAdaptive_OnSuccessRaisesLimitByOne(t *testing.T) {
	l := NewAdaptiveLimiter(2, 1, 5)
	l.OnSuccess()
	assert.Equal(t, 3, l.CurrentLimit())
	l.OnSuccess()
	assert.Equal(t, 4, l.CurrentLimit())
}

// INCREMENT_DECREMENT on `l.inFlight++` when OnSuccess admits a waiter: the
// admitted waiter must count as in-flight, so the limit still gates the next
// caller.
func TestAdaptive_WaiterAdmittedBySuccessCountsAsInFlight(t *testing.T) {
	l := NewAdaptiveLimiter(1, 1, 2)
	require.NoError(t, l.Acquire(context.Background()))

	admitted := make(chan struct{})
	go func() {
		_ = l.Acquire(context.Background())
		close(admitted)
	}()
	time.Sleep(10 * time.Millisecond)
	l.OnSuccess() // limit 2 → the waiter is admitted; in-flight must now be 2
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("waiter not admitted")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, l.Acquire(ctx), context.DeadlineExceeded, "limit 2 with 2 in flight must block")
}
