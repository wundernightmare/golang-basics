package httpx

import (
	"context"
	"hash/fnv"
	"log/slog"
	"sync/atomic"
	"time"
)

// samplingHandler drops repetitive debug/info records the way zap's sampler
// does: per (level, message) and per tick it passes the first `initial`
// records, then every `thereafter`-th. Warn and error always pass. Counters
// live in a fixed table indexed by a hash of the message, so memory is bounded
// and the hot path allocates nothing; two messages sharing a slot share a
// budget, which is an acceptable imprecision for a rate limiter.
//
// The point is cost control: an access log at 20k req/s or a tight loop
// logging every item would otherwise spend more CPU on JSON encoding than on
// the work itself. What survives is enough to see the shape of traffic;
// metrics carry the exact counts.
type samplingHandler struct {
	slog.Handler
	initial    uint64
	thereafter uint64
	tick       int64 // nanoseconds
	counters   *[samplingSlots]sampleCounter
	dropped    *[2]atomic.Uint64 // 0=debug 1=info
}

const samplingSlots = 4096

type sampleCounter struct {
	resetAt atomic.Int64
	n       atomic.Uint64
}

func newSamplingHandler(next slog.Handler, initial, thereafter int, tick time.Duration) *samplingHandler {
	if thereafter <= 0 {
		thereafter = 100
	}
	if tick <= 0 {
		tick = time.Second
	}
	return &samplingHandler{
		Handler:    next,
		initial:    uint64(initial),    //nolint:gosec // validated > 0 by the caller
		thereafter: uint64(thereafter), //nolint:gosec // validated > 0 above
		tick:       int64(tick),
		counters:   new([samplingSlots]sampleCounter),
		dropped:    new([2]atomic.Uint64),
	}
}

func (h *samplingHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		return h.Handler.Handle(ctx, r)
	}
	c := &h.counters[h.slot(r)]
	now := r.Time.UnixNano()
	if now == 0 {
		now = time.Now().UnixNano()
	}
	if resetAt := c.resetAt.Load(); now >= resetAt {
		// New window: whoever wins the CAS zeroes the count. A loser simply
		// counts into the fresh window, which is the intended behaviour.
		if c.resetAt.CompareAndSwap(resetAt, now+h.tick) {
			c.n.Store(0)
		}
	}
	n := c.n.Add(1)
	if n <= h.initial || (n-h.initial)%h.thereafter == 0 {
		return h.Handler.Handle(ctx, r)
	}
	if r.Level >= slog.LevelInfo {
		h.dropped[1].Add(1)
	} else {
		h.dropped[0].Add(1)
	}
	return nil
}

func (h *samplingHandler) slot(r slog.Record) uint64 {
	f := fnv.New64a()
	_, _ = f.Write([]byte(r.Message))
	return (f.Sum64() ^ uint64(r.Level+128)) % samplingSlots //nolint:gosec // level offset keeps it non-negative
}

// Dropped returns the number of debug and info records dropped so far.
func (h *samplingHandler) Dropped() (debug, info uint64) {
	return h.dropped[0].Load(), h.dropped[1].Load()
}

func (h *samplingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	c.Handler = h.Handler.WithAttrs(attrs)
	return &c
}

func (h *samplingHandler) WithGroup(name string) slog.Handler {
	c := *h
	c.Handler = h.Handler.WithGroup(name)
	return &c
}

// logDropped finds the sampling handler behind log (if any) and returns its
// counters for the log_dropped_total metric.
func logDropped(log *slog.Logger) func() (debug, info uint64) {
	if sh, ok := log.Handler().(*samplingHandler); ok {
		return sh.Dropped
	}
	return nil
}
