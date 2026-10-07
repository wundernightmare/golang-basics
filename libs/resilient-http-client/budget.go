package resilient

import (
	"sync"
	"time"
)

// retryBudget bounds retries to minRetries + ratio × requests over a sliding
// window, so an outage of the dependency cannot multiply the traffic sent to
// it by the attempt count. A nil budget allows every retry.
type retryBudget struct {
	mu         sync.Mutex
	w          window // a = requests, b = retries
	ratio      float64
	minRetries int
	now        func() time.Time
}

func newRetryBudget(cfg *TargetConfig, now func() time.Time) *retryBudget {
	if cfg.RetryBudgetWindow <= 0 {
		return nil
	}
	return &retryBudget{
		w:          newWindow(cfg.RetryBudgetWindow),
		ratio:      cfg.RetryBudgetRatio,
		minRetries: cfg.RetryBudgetMinRetries,
		now:        now,
	}
}

// request counts one logical request (not its retries).
func (b *retryBudget) request() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.w.add(b.now(), 1, 0)
	b.mu.Unlock()
}

// tryRetry spends one retry if the budget has one left.
func (b *retryBudget) tryRetry() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	requests, retries := b.w.sum(now)
	if float64(retries) >= float64(b.minRetries)+b.ratio*float64(requests) {
		return false
	}
	b.w.add(now, 0, 1)
	return true
}
