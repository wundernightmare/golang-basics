package resilience

import (
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before retry number attempt (1-based; anything
// below 1 counts as 1): initial·2^(attempt-1), capped at maxDelay, with
// "equal jitter" — half of that fixed, half random — so a fleet retrying the
// same outage does not retry in lockstep, while every delay is still at
// least half of what the schedule says. The result is in [d/2, d] and never
// above maxDelay, however large attempt is.
//
// It is the schedule of whatever retries on its own clock — a consumer
// redelivering a message, a relay after a failed pass. A client that must
// spread a thundering herd as widely as possible (the HTTP client, after a
// 503) uses full jitter instead; that is its own.
func Backoff(attempt int, initial, maxDelay time.Duration) time.Duration {
	delay := initial
	for i := 1; i < attempt && delay < maxDelay; i++ {
		delay *= 2
	}

	delay = min(delay, maxDelay)

	half := delay / 2
	if half <= 0 {
		return delay
	}

	return half + rand.N(half+1) //nolint:gosec // jitter, not a secret
}
