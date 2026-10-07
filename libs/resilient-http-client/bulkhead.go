package resilient

import (
	"context"
	"time"
)

// bulkhead is a fixed concurrency limit: a semaphore with a bounded wait. A
// nil *bulkhead admits everything.
type bulkhead struct {
	slots chan struct{}
	wait  time.Duration
}

func newBulkhead(cfg *TargetConfig) *bulkhead {
	if cfg.MaxConcurrent <= 0 {
		return nil
	}
	return &bulkhead{slots: make(chan struct{}, cfg.MaxConcurrent), wait: cfg.MaxConcurrentWait}
}

// acquire takes a slot, waiting at most b.wait and never past ctx. It returns
// false when no slot came free in time; the caller tells a full bulkhead from
// a cancelled ctx by ctx.Err().
func (b *bulkhead) acquire(ctx context.Context) bool {
	if b == nil {
		return true
	}
	select {
	case b.slots <- struct{}{}:
		return true
	default:
	}
	if b.wait <= 0 || ctx.Err() != nil {
		return false
	}
	t := time.NewTimer(b.wait)
	defer t.Stop()
	select {
	case b.slots <- struct{}{}:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// release frees a slot taken by acquire.
func (b *bulkhead) release() {
	if b != nil {
		<-b.slots
	}
}
