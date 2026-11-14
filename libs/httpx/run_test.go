package httpx_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// oneShot is a client that leaves no connection behind. The default transport
// keeps connections alive and may open one it never uses; a server that is
// shutting down waits for such a connection (net/http gives a new one five
// seconds), which is longer than the admin listener's drain budget — and
// these tests assert that shutdown is clean.
var oneShot = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 2 * time.Second}

func readyz(adm string) int {
	resp, err := oneShot.Get(adm + "/readyz")
	if err != nil {
		return 0
	}

	_ = resp.Body.Close()

	return resp.StatusCode
}

func servingAddr(t *testing.T, srv *httpx.Server) string {
	t.Helper()

	require.Eventually(t, func() bool { return srv.AdminListenAddr() != "" }, 2*time.Second, 5*time.Millisecond)

	adm := "http://" + srv.AdminListenAddr()

	require.Eventually(t, func() bool { return readyz(adm) == http.StatusOK }, 2*time.Second, 5*time.Millisecond)

	return adm
}

// The order RunWith exists for: on the signal the server drains while the
// worker is still running, and the worker is told to stop only afterwards.
func TestRunWith_WorkersOutliveTheSignalUntilTheServerHasDrained(t *testing.T) {
	t.Parallel()

	srv, logs := loggedServer(t, httpx.Config{
		AdminAddr: "127.0.0.1:0", ShutdownDelay: 300 * time.Millisecond, ShutdownTimeout: 2 * time.Second,
	})

	var (
		stoppedAt  atomic.Int64 // when the worker saw its context end
		sawDrained atomic.Bool  // whether the server had already stopped by then
	)

	worker := httpx.Worker{Name: "writer", Run: func(ctx context.Context) error {
		<-ctx.Done()
		stoppedAt.Store(time.Now().UnixNano())
		sawDrained.Store(logs.Find(t, map[string]any{"msg": "servers stopped cleanly"}) != nil)

		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- srv.RunWith(ctx, worker) }()

	servingAddr(t, srv)

	signalled := time.Now()

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("RunWith did not return")
	}

	assert.True(t, sawDrained.Load(), "the worker's context ended after the server had stopped")
	assert.GreaterOrEqual(t, time.Unix(0, stoppedAt.Load()).Sub(signalled), 300*time.Millisecond,
		"the worker kept running through the shutdown delay")
}

var errWorker = errors.New("worker failed")

// A failing worker — by an error or by a panic — stops the service in order:
// the server drains, the other workers are stopped, and the failure is what
// RunWith returns.
func TestRunWith_AFailingWorkerStopsTheService(t *testing.T) {
	t.Parallel()

	for name, failing := range map[string]func(context.Context) error{
		"error": func(context.Context) error { return errWorker },
		"panic": func(context.Context) error { panic("boom") },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv, logs := loggedServer(t, httpx.Config{AdminAddr: "127.0.0.1:0", ShutdownTimeout: 2 * time.Second})

			var (
				release      = make(chan struct{})
				otherStopped atomic.Bool
			)

			done := make(chan error, 1)

			go func() {
				done <- srv.RunWith(context.Background(),
					httpx.Worker{Name: "failing", Run: func(ctx context.Context) error {
						<-release

						return failing(ctx)
					}},
					httpx.Worker{Name: "other", Run: func(ctx context.Context) error {
						<-ctx.Done()
						otherStopped.Store(true)

						return nil
					}})
			}()

			servingAddr(t, srv)
			close(release)

			select {
			case err := <-done:
				if name == "panic" {
					require.ErrorIs(t, err, httpx.ErrPanic)
					assert.InDelta(t, 1, testx.Metric(t, srv.Metrics.Registry, "background_panics_total", map[string]string{"task": "failing"}), 0)
				} else {
					require.ErrorIs(t, err, errWorker)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("RunWith did not return")
			}

			assert.True(t, otherStopped.Load(), "the other worker was stopped")
			assert.NotNil(t, logs.Find(t, map[string]any{"msg": "servers stopped cleanly"}), "the server drained, it was not torn down")
		})
	}
}

// A worker that finishes on its own (nil) ends alone: the service goes on.
func TestRunWith_AWorkerThatFinishesLeavesTheRestRunning(t *testing.T) {
	t.Parallel()

	srv, _ := loggedServer(t, httpx.Config{AdminAddr: "127.0.0.1:0", ShutdownTimeout: 2 * time.Second})
	finished := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- srv.RunWith(ctx, httpx.Worker{Name: "one-shot", Run: func(context.Context) error {
			close(finished)

			return nil
		}})
	}()

	adm := servingAddr(t, srv)

	<-finished
	assert.Equal(t, http.StatusOK, readyz(adm), "still serving after the worker returned")

	select {
	case err := <-done:
		t.Fatalf("RunWith returned (%v) while the server should be running", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	require.NoError(t, <-done)
}

// A server that cannot start is the error, and the workers are stopped.
func TestRunWith_ServerFailureStopsTheWorkers(t *testing.T) {
	t.Parallel()

	first, _ := loggedServer(t, httpx.Config{AdminAddr: "127.0.0.1:0", ShutdownTimeout: time.Second})

	go func() { _ = first.Run(t.Context()) }()

	servingAddr(t, first)

	second, _ := loggedServer(t, httpx.Config{AdminAddr: first.AdminListenAddr(), ShutdownTimeout: time.Second})

	var stopped atomic.Bool

	err := second.RunWith(context.Background(), httpx.Worker{Name: "w", Run: func(ctx context.Context) error {
		<-ctx.Done()
		stopped.Store(true)

		return nil
	}})
	require.Error(t, err, "the port is taken")
	assert.True(t, stopped.Load())
}
