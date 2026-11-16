package httpx

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// Worker is a background task that runs next to the server: a relay, a
// janitor, a batch writer. Name is its background_panics_total{task} label —
// a constant, never derived from input.
type Worker struct {
	Name string
	Run  func(ctx context.Context) error
}

// RunWith runs the server and its workers until ctx is cancelled or one of
// them fails, and returns the first error.
//
// It is [Server.Run] plus the shutdown order a service with background work
// needs: **the server drains first, the workers stop after**. When ctx is
// cancelled the workers keep running — a request still in flight may be
// waiting for what one of them does (a batch being written, an outbox row
// being relayed) — and their context is cancelled only once Run has
// returned, i.e. after HTTP_SHUTDOWN_DELAY and the drain. Each worker then
// finishes what it holds and returns; whatever the caller closes afterwards
// (a producer, a pool) is no longer in use.
//
// A worker that returns an error stops the server, which drains as above. So
// does a worker that panics: every worker runs under [Server.Guard]. A worker
// that returns nil ends alone — the rest keep running. Once the server has
// stopped and the workers were told to, a worker's context.Canceled is the
// answer to that and not a failure: `return ctx.Err()` ends a clean shutdown
// cleanly.
//
// The workers get HTTP_SHUTDOWN_TIMEOUT, counted from the moment they are
// told to stop, to return. Past it RunWith returns an error that names those
// still running and leaves them behind: a worker that ignores its context
// would otherwise hold the process until the supervisor kills it, with the
// caller's deferred cleanup never run. What the caller closes next may then
// still be in use by such a worker.
//
// A worker that must stop *with* the signal rather than after the drain (a
// consumer loop: nothing in flight depends on it) does not belong here; run
// it next to [Server.Run] with the same context instead.
func (s *Server) RunWith(ctx context.Context, workers ...Worker) error {
	// serving ends when ctx does, or when a worker fails.
	serving, stopServing := context.WithCancel(ctx)
	defer stopServing()

	// working outlives ctx: it ends when the server has stopped.
	working, stopWorking := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWorking()

	var (
		mu      sync.Mutex // guards first and running: a worker abandoned below may still report
		first   error
		running = make(map[string]int, len(workers)) // by name: two workers may share one
		stopped = make(chan struct{})
		wg      sync.WaitGroup
	)

	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()

		if err != nil && first == nil {
			first = err

			stopServing()
		}
	}

	for _, worker := range workers {
		running[worker.Name]++

		wg.Go(func() {
			err := s.Guard(worker.Name, working, worker.Run)()

			mu.Lock()
			if running[worker.Name]--; running[worker.Name] == 0 {
				delete(running, worker.Name)
			}
			mu.Unlock()

			if errors.Is(err, context.Canceled) && working.Err() != nil {
				return
			}

			fail(err)
		})
	}

	go func() {
		wg.Wait()
		close(stopped)
	}()

	fail(s.Run(serving))
	stopWorking()

	budget := time.NewTimer(s.cfg.ShutdownTimeout)
	defer budget.Stop()

	select {
	case <-stopped:
	case <-budget.C:
		mu.Lock()
		defer mu.Unlock()

		return errors.Join(first, fmt.Errorf("httpx: workers %q still running %s after they were told to stop",
			slices.Sorted(maps.Keys(running)), s.cfg.ShutdownTimeout))
	}

	mu.Lock()
	defer mu.Unlock()

	return first
}
