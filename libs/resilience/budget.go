package resilience

import (
	"sync"
	"time"
)

// RetryBudget bounds retries to minRetries + ratio × requests over a sliding
// window, so an outage of the dependency cannot multiply the traffic sent to
// it by the attempt count. A nil budget allows every retry.
//
// A retry is granted while the retries already in the window are below that
// bound — not while one more would still fit — so the bound is in effect
// rounded up: with a ratio above 0, one request in the window is enough for
// one retry, and a caller that sends a single request per window can retry
// every one of them.
type RetryBudget struct {
	mu sync.Mutex
	w  window // a = requests, b = retries

	ratio      float64
	minRetries int
	now        func() time.Time
}

// NewRetryBudget builds a budget of minRetries + ratio × requests per
// window; nil (no budget) when window is 0 or negative. now is the clock
// (time.Now), must not be nil and is read once here: the window counts the
// time elapsed since. A ratio that is not a number grants no
// retries at all.
func NewRetryBudget(ratio float64, minRetries int, window time.Duration, now func() time.Time) *RetryBudget {
	if window <= 0 {
		return nil
	}

	return &RetryBudget{
		w:          newWindow(window, now()),
		ratio:      ratio,
		minRetries: minRetries,
		now:        now,
	}
}

// Request counts one logical request (not its retries).
func (b *RetryBudget) Request() {
	if b == nil {
		return
	}

	b.mu.Lock()
	b.w.add(b.now(), 1, 0)
	b.mu.Unlock()
}

// TryRetry spends one retry if the budget has one left.
func (b *RetryBudget) TryRetry() bool {
	if b == nil {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()

	requests, retries := b.w.sum(now)
	// "Not below the bound" rather than "at or above it": a NaN ratio makes
	// every comparison false, and must refuse rather than grant.
	if !(float64(retries) < float64(b.minRetries)+b.ratio*float64(requests)) {
		return false
	}

	b.w.add(now, 0, 1)

	return true
}
