package valkey

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// warnInterval is how often one kind of cache-failure warning is logged.
// During an outage every request fails the same way; the line says that it
// is happening and cache_command_errors_total says how often. A constant:
// there is no configuration key for it.
const warnInterval = 10 * time.Second

// warnGate lets one warning through per warnInterval and counts the rest.
type warnGate struct {
	mu      sync.Mutex
	next    time.Time
	dropped int
}

// pass reports whether a warning at now is logged, and how many were left
// out since the last one that was.
func (g *warnGate) pass(now time.Time) (dropped int, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if now.Before(g.next) {
		g.dropped++

		return 0, false
	}

	dropped, g.dropped = g.dropped, 0
	g.next = now.Add(warnInterval)

	return dropped, true
}

func (g *warnGate) log(ctx context.Context, log *slog.Logger, msg string, args ...any) {
	dropped, ok := g.pass(time.Now())
	if !ok {
		return
	}

	if dropped > 0 {
		args = append(args, "suppressed", dropped)
	}

	log.WarnContext(ctx, msg, args...)
}
