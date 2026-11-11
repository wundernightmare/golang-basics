package tiercache

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// breaker keeps a failing L2 from costing every miss a full command timeout:
// after threshold consecutive failures it is open for cooldown and L2 is
// skipped; then calls are let through again, and a single failure re-opens
// it. It is deliberately this small — L2 is optional, so there is nothing to
// protect but latency, and a few extra probes after the cooldown are harmless.
type breaker struct {
	threshold int64
	cooldown  time.Duration
	failures  atomic.Int64
	openUntil atomic.Int64 // Unix nanoseconds
}

// allow reports whether L2 may be called now.
func (b *breaker) allow(now time.Time) bool { return now.UnixNano() >= b.openUntil.Load() }

// record takes the outcome of an L2 call and reports whether it opened the
// breaker. A caller that went away says nothing about L2.
func (b *breaker) record(now time.Time, err error) (opened bool) {
	if err == nil {
		b.failures.Store(0)

		return false
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	if b.failures.Add(1) < b.threshold {
		return false
	}

	// One failure short of the threshold: the first failed probe after the
	// cooldown opens it again.
	b.failures.Store(b.threshold - 1)
	b.openUntil.Store(now.Add(b.cooldown).UnixNano())

	return true
}
