package resilience

import (
	// nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used -- retry jitter does not need cryptographic randomness
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before retry number attempt (1-based; anything
// below 1 counts as 1): initial·2^(attempt-1), capped at maxDelay, with
// "equal jitter" — half of that fixed, half random — so a fleet retrying the
// same outage does not retry in lockstep, while every delay is still at
// least half of what the schedule says. The result is in [d/2, d], never
// above maxDelay and never negative, however large attempt and maxDelay
// are; a negative initial or maxDelay counts as 0.
//
// It is the schedule of whatever retries on its own clock — a consumer
// redelivering a message, a relay after a failed pass. A client that must
// spread a thundering herd as widely as possible (the HTTP client, after a
// 503) uses full jitter instead; that is its own.
func Backoff(attempt int, initial, maxDelay time.Duration) time.Duration {
	delay := max(initial, 0)
	// 0 doubles to 0: stop rather than count to attempt. The step is
	// min(2·delay, maxDelay) written so that it cannot overflow.
	for i := 1; i < attempt && delay > 0 && delay < maxDelay; i++ {
		delay += min(delay, maxDelay-delay)
	}

	delay = max(min(delay, maxDelay), 0)

	half := delay / 2
	if half <= 0 {
		return delay
	}

	return half + rand.N(half+1) //nolint:gosec // G404: jitter, predictability is harmless
}

// Jitter returns ttl spread uniformly over ±10%, so keys filled together (a
// warm-up, a burst of misses after a deploy) do not all expire together and
// stampede the source of truth at once. A ttl of 0 or below, or one too
// small to have a tenth (under 10ns), is returned as it is.
func Jitter(ttl time.Duration) time.Duration {
	spread := int64(ttl) / 10
	if spread <= 0 {
		return ttl
	}

	return ttl + time.Duration(rand.Int64N(2*spread+1)-spread) //nolint:gosec // G404: jitter, predictability is harmless
}
