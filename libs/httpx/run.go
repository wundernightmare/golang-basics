package httpx

import (
	"context"
	"sync"
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
// that returns nil ends alone — the rest keep running.
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
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)

	fail := func(err error) {
		if err == nil {
			return
		}

		once.Do(func() {
			first = err

			stopServing()
		})
	}

	for _, worker := range workers {
		wg.Go(func() { fail(s.Guard(worker.Name, working, worker.Run)()) })
	}

	fail(s.Run(serving))
	stopWorking()
	wg.Wait()

	return first
}
