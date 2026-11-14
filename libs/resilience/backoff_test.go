package resilience

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestBackoffDoublesFromTheFirstAttemptAndIsCapped(t *testing.T) {
	t.Parallel()

	const initial, maxDelay = 100 * time.Millisecond, time.Second

	for attempt, ms := range map[int]int64{-3: 100, 0: 100, 1: 100, 2: 200, 3: 400, 4: 800, 5: 1000, 6: 1000, 1000: 1000} {
		ceiling := time.Duration(ms) * time.Millisecond

		for range 50 {
			got := Backoff(attempt, initial, maxDelay)
			assert.GreaterOrEqual(t, got, ceiling/2, "attempt %d", attempt)
			assert.LessOrEqual(t, got, ceiling, "attempt %d", attempt)
		}
	}
}

// For any schedule and any attempt: the delay is never above the cap, never
// below half of what the schedule says, never negative — a huge attempt
// number does not overflow into one — and a schedule too small to halve is
// returned as it is.
func TestBackoffStaysWithinItsSchedule(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		initial := time.Duration(rapid.Int64Range(0, int64(time.Hour)).Draw(t, "initial"))
		maxDelay := time.Duration(rapid.Int64Range(int64(initial), int64(24*time.Hour)).Draw(t, "max"))
		attempt := rapid.IntRange(-10, 10_000).Draw(t, "attempt")

		scheduled := initial
		for i := 1; i < attempt && scheduled < maxDelay; i++ {
			scheduled *= 2
		}

		scheduled = min(scheduled, maxDelay)

		got := Backoff(attempt, initial, maxDelay)
		if got < scheduled/2 || got > scheduled || got > maxDelay || got < 0 {
			t.Fatalf("Backoff(%d, %s, %s) = %s, the schedule says %s", attempt, initial, maxDelay, got, scheduled)
		}
	})
}

// The jitter is there — a fleet that fails together must not retry together —
// and a delay too small to halve is returned whole rather than as zero.
func TestBackoffIsJitteredAndNeverLosesATinyDelay(t *testing.T) {
	t.Parallel()

	seen := map[time.Duration]struct{}{}
	for range 200 {
		seen[Backoff(3, time.Second, time.Minute)] = struct{}{}
	}

	assert.Greater(t, len(seen), 100, "200 draws from a two-second range give many different delays")

	assert.Equal(t, time.Nanosecond, Backoff(1, time.Nanosecond, time.Nanosecond), "one nanosecond has no half: it is not rounded to nothing")
	assert.Zero(t, Backoff(5, 0, time.Minute), "no delay configured is no delay")
}
