package resilient

import (
	"math/rand/v2"
	"time"
)

// FullJitter returns a full-jitter exponential-backoff delay for the given
// retry attempt, following the AWS "Exponential Backoff And Jitter" algorithm:
//
//	delay = random(0, min(cap, base * 2^attempt))
//
// attempt is 0-indexed: attempt 0 (the first try) is always zero delay. base is
// the base delay and capDelay is the ceiling. The randomness uses math/rand/v2,
// whose top-level source is safe for concurrent use.
//
// The 2^attempt term is computed in float64 to avoid integer overflow at large
// attempt counts; the result is clamped to capDelay long before that matters.
func FullJitter(attempt int, base, capDelay time.Duration) time.Duration {
	if attempt <= 0 || capDelay <= 0 || base <= 0 {
		return 0
	}

	ceiling := float64(base) * float64(int64(1)<<min(attempt, 62))
	if c := float64(capDelay); ceiling > c {
		ceiling = c
	}
	if ceiling <= 0 {
		return 0
	}
	// rand.Int64N requires n > 0; ceiling >= 1ns here.
	//nolint:gosec // jitter is a backoff smoother, not a security primitive — a fast PRNG is correct here.
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}
