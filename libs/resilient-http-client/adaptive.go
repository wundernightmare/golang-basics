package resilient

import (
	"context"
	"sync"
)

// AdaptiveLimiter is an AIMD (Additive-Increase / Multiplicative-Decrease)
// concurrency limiter: the maximum number of in-flight requests grows by one on
// every success and is halved on every failure, clamped to [min, max].
//
//   - Additive increase — [AdaptiveLimiter.OnSuccess] raises the limit by 1.
//   - Multiplicative decrease — [AdaptiveLimiter.OnFailure] halves it.
//
// [AdaptiveLimiter.Acquire] blocks (respecting the caller's context) until a
// slot is free, then [AdaptiveLimiter.Release] returns it. Slots are handed
// directly to the oldest waiter in FIFO order, so there is no thundering herd.
// After a decrease the surplus drains lazily: releases below the new limit are
// dropped rather than handed on, until in-flight catches down to the limit.
//
// A single mutex guards the small amount of shared state; it is held only for
// O(1) bookkeeping (plus an O(n) waiter removal on context cancellation, which
// is the cold path). Use [NewAdaptiveLimiter] to construct one.
type AdaptiveLimiter struct {
	mu       sync.Mutex
	limit    int
	inFlight int
	waiters  []chan struct{}
	min      int
	max      int
}

// NewAdaptiveLimiter creates a limiter starting at initial, bounded by
// [min, max]. min is floored at 1; initial is clamped into range.
func NewAdaptiveLimiter(initial, minLimit, maxLimit int) *AdaptiveLimiter {
	if minLimit < 1 {
		minLimit = 1
	}
	if maxLimit < minLimit {
		maxLimit = minLimit
	}
	if initial < minLimit {
		initial = minLimit
	}
	if initial > maxLimit {
		initial = maxLimit
	}
	return &AdaptiveLimiter{limit: initial, min: minLimit, max: maxLimit}
}

// Acquire reserves one concurrency slot, blocking until one is free or ctx is
// done. On success it returns nil and the caller must later call
// [AdaptiveLimiter.Release] exactly once. On cancellation it returns ctx.Err()
// and no slot is held.
func (l *AdaptiveLimiter) Acquire(ctx context.Context) error {
	l.mu.Lock()
	if l.inFlight < l.limit {
		l.inFlight++
		l.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	l.waiters = append(l.waiters, ch)
	l.mu.Unlock()

	select {
	case <-ch:
		// A releaser handed us its slot (in-flight already accounts for us).
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		if l.removeWaiter(ch) {
			// Still queued — we never received a slot.
			l.mu.Unlock()
			return ctx.Err()
		}
		// Raced with a hand-off: we own a slot after all. Give it back.
		l.mu.Unlock()
		l.Release()
		return ctx.Err()
	}
}

// Release returns a slot previously taken by [AdaptiveLimiter.Acquire]. If the
// limiter is currently over its limit (just shrunk) the slot is dropped to drain
// the surplus; otherwise it is handed to the next waiter, or in-flight is
// decremented if none are waiting.
func (l *AdaptiveLimiter) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inFlight > l.limit {
		l.inFlight--
		return
	}
	if len(l.waiters) > 0 {
		ch := l.waiters[0]
		l.waiters[0] = nil
		l.waiters = l.waiters[1:]
		close(ch) // hand the slot on; in-flight is unchanged
		return
	}
	l.inFlight--
}

// OnSuccess applies the additive increase (+1, capped at max). If the new
// headroom lets a waiter through, one is admitted immediately.
func (l *AdaptiveLimiter) OnSuccess() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limit >= l.max {
		return
	}
	l.limit++
	if l.inFlight < l.limit && len(l.waiters) > 0 {
		ch := l.waiters[0]
		l.waiters[0] = nil
		l.waiters = l.waiters[1:]
		l.inFlight++
		close(ch)
	}
}

// OnFailure applies the multiplicative decrease (÷2, floored at min). The
// surplus in-flight requests drain lazily via [AdaptiveLimiter.Release].
func (l *AdaptiveLimiter) OnFailure() {
	l.mu.Lock()
	defer l.mu.Unlock()
	nl := l.limit / 2
	if nl < l.min {
		nl = l.min
	}
	l.limit = nl
}

// CurrentLimit reports the current AIMD limit (for metrics / diagnostics).
func (l *AdaptiveLimiter) CurrentLimit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

// removeWaiter drops ch from the queue, returning true if it was present. The
// caller must hold l.mu.
func (l *AdaptiveLimiter) removeWaiter(ch chan struct{}) bool {
	for i, w := range l.waiters {
		if w == ch {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			return true
		}
	}
	return false
}
