package httpx

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// running starts Run on a server whose listeners the test can reach, and
// waits until it is ready. The API listener is the first one bound.
func running(t *testing.T, srv *Server) (apiLn net.Listener, stop context.CancelFunc, done <-chan error) {
	t.Helper()

	bound := make(chan net.Listener, 2)
	srv.listen = func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err == nil {
			bound <- ln
		}

		return ln, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)

	go func() { result <- srv.Run(ctx) }()

	require.Eventually(t, srv.Health.ready.Load, 5*time.Second, time.Millisecond)

	return <-bound, cancel, result
}

// A listener that stops accepting ends the service: readiness goes first,
// and Run returns the listener's error — without sitting out the shutdown
// delay when the failure comes during it.
func TestRun_AFailedListenerEndsRun(t *testing.T) {
	t.Parallel()

	for name, shuttingDown := range map[string]bool{"while serving": false, "during the shutdown delay": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv, err := NewServer(Config{
				Addr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0", ShutdownDelay: time.Minute, ShutdownTimeout: time.Second,
			}, nil)
			require.NoError(t, err)

			apiLn, stop, done := running(t, srv)
			defer stop()

			if shuttingDown {
				stop()
				require.Eventually(t, func() bool { return !srv.Health.ready.Load() }, 5*time.Second, time.Millisecond)
			}

			require.NoError(t, apiLn.Close())

			select {
			case err := <-done:
				require.ErrorContains(t, err, "http server")
				assert.False(t, srv.Health.ready.Load())
			case <-time.After(10 * time.Second):
				t.Fatal("Run outlives its listener")
			}
		})
	}
}

// The forced close of the API ends the API's requests only: what the admin
// listener is serving at that moment (a profile, a scrape) runs to its end.
func TestRun_TheForcedCloseLeavesAdminRequestsAlone(t *testing.T) {
	t.Parallel()

	srv, err := NewServer(Config{Addr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0", ShutdownTimeout: 50 * time.Millisecond}, nil)
	require.NoError(t, err)

	var (
		started   = make(chan struct{}, 2)
		forced    = make(chan struct{})
		cancelled = make(chan bool, 1)
	)

	srv.Mux().HandleFunc("GET /stuck", func(_ http.ResponseWriter, r *http.Request) {
		started <- struct{}{}

		<-r.Context().Done()
		close(forced)
	})
	srv.admin = http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		started <- struct{}{}

		<-forced

		cancelled <- r.Context().Err() != nil
	})

	_, stop, done := running(t, srv)
	defer stop()

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	for _, url := range []string{"http://" + srv.ListenAddr() + "/stuck", "http://" + srv.AdminListenAddr() + "/profile"} {
		go func() {
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
			if resp, err := client.Do(req); err == nil {
				_ = resp.Body.Close()
			}
		}()
	}

	<-started
	<-started
	stop()

	require.ErrorContains(t, <-done, "drain")
	assert.False(t, <-cancelled)
}

// A timer that fired and lost the lock to a newer Set must not undo it.
func TestLogLevel_ARevertOfAnOlderChangeIsIgnored(t *testing.T) {
	t.Parallel()

	level := newLogLevel(slog.LevelInfo)
	defer level.Reset()

	level.Set(slog.LevelDebug, time.Hour)
	older := level.gen
	level.Set(slog.LevelWarn, time.Hour)

	level.revert(older)

	got, expires := level.Level()
	assert.Equal(t, slog.LevelWarn, got)
	assert.False(t, expires.IsZero(), "the newer change still has its own term")

	level.revert(level.gen)

	got, expires = level.Level()
	assert.Equal(t, slog.LevelInfo, got)
	assert.True(t, expires.IsZero())
}
