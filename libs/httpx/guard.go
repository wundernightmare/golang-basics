package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
)

// ErrPanic is in the chain of the error [Server.Guard] returns for a task
// that panicked.
var ErrPanic = errors.New("panic")

// Guard wraps a background task — a worker loop, a relay, a consumer — so
// that a panic in it does not take the process down mid-flight with nothing
// but a stack on stderr. The panic is recovered, logged at error level with
// its stack, counted in background_panics_total{task}, and returned as an
// error wrapping [ErrPanic]:
//
//	g, ctx := errgroup.WithContext(ctx)
//	g.Go(srv.Guard("outbox-relay", ctx, relay.Run))
//	g.Go(func() error { return srv.Run(ctx) })
//
// The error is the point: the errgroup cancels the others, the server drains
// its requests, deferred cleanup runs and the process exits non-zero, to be
// restarted by its supervisor — an orderly stop instead of a crash. A task
// must not be resumed after a panic: its state is whatever the panic left.
//
// name is a metric label: a constant per call site, never derived from input.
func (s *Server) Guard(name string, ctx context.Context, task func(context.Context) error) func() error {
	return func() (err error) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			s.Metrics.backgroundPanics.WithLabelValues(name).Inc()
			s.log.LogAttrs(ctx, slog.LevelError, "panic in background task",
				slog.String("task", name), slog.Any("panic", recovered), slog.String("stack", string(debug.Stack())))

			err = fmt.Errorf("httpx: background task %q: %w: %v", name, ErrPanic, recovered)
		}()

		return task(ctx)
	}
}
