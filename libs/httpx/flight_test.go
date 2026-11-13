package httpx

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowUnderTrace is the handler whose slowness the flight recorder is there
// to explain; its name is looked for in the captured trace.
func slowUnderTrace(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("slow") != "" {
		time.Sleep(150 * time.Millisecond)
	}

	w.WriteHeader(http.StatusNoContent)
}

// A real server with the flight recorder on. A fast request captures nothing.
// A request over DIAG_FLIGHT_SLOW_REQUEST ships a snapshot of the execution
// trace to the dump sink — an object the Go trace tool reads, and in which
// the slow handler itself can be found. A second slow request inside the
// cooldown ships nothing more.
//
//nolint:paralleltest // the runtime allows one flight recorder per process
func TestFlightRecorder_SlowRequestShipsAReadableTrace(t *testing.T) {
	sink := &memSink{}

	srv, err := NewServer(Config{
		Service: "svc", Addr: "127.0.0.1:0",
		DiagFlightRecorder: true, DiagFlightSlowRequest: 100 * time.Millisecond, DiagFlightCooldown: time.Hour,
		DiagCheckInterval: 10 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	srv.SetDumpSink(sink)
	srv.Mux().HandleFunc("GET /work", slowUnderTrace)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- srv.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})

	require.Eventually(t, func() bool { return srv.ListenAddr() != "" }, 10*time.Second, 5*time.Millisecond)

	call := func(query string) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+srv.ListenAddr()+"/work"+query, http.NoBody)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	}

	for range 5 {
		call("")
	}

	time.Sleep(50 * time.Millisecond) // five ticks of the watcher: it had its chance
	require.Empty(t, sink.keys(), "fast requests capture nothing")

	call("?slow=1")

	require.Eventually(t, func() bool { return len(sink.keys()) == 1 }, 10*time.Second, 10*time.Millisecond,
		"the slow request shipped a snapshot")

	key, body := sink.find(t, "-flight.trace")
	assert.Contains(t, key, "diagnostics/svc/dt=", "under the same layout as every other artifact")
	assert.InDelta(t, 1, testutil.ToFloat64(srv.diag.dumps.WithLabelValues(dumpKindFlight, dumpUploaded)), 0)

	// The object is a Go execution trace: the toolchain's own reader parses
	// it, and the handler that slept is in it.
	path := filepath.Join(t.TempDir(), "flight.trace")
	require.NoError(t, os.WriteFile(path, body, 0o600))

	parsed, err := exec.CommandContext(t.Context(), "go", "tool", "trace", "-d=parsed", path).CombinedOutput()
	require.NoError(t, err, "go tool trace rejected the snapshot: %s", firstLines(parsed, 5))
	assert.Contains(t, string(parsed), "slowUnderTrace", "the trace shows what the process was doing: the slow handler is in a stack")

	call("?slow=1")
	time.Sleep(100 * time.Millisecond)
	assert.Len(t, sink.keys(), 1, "inside DIAG_FLIGHT_COOLDOWN a second anomaly ships nothing")

	assert.True(t, srv.CaptureFlightTrace("asked by the service"), "a request for a snapshot is taken")
}

func firstLines(out []byte, n int) string {
	lines := strings.SplitN(string(out), "\n", n+1)
	if len(lines) > n {
		lines = lines[:n]
	}

	return strings.Join(lines, "\n")
}

// Off by default: no recorder, no trigger, and asking for a snapshot says so.
func TestFlightRecorder_OffByDefault(t *testing.T) {
	t.Parallel()

	srv, err := NewServer(Config{Service: "svc", Addr: "127.0.0.1:0"}, nil)
	require.NoError(t, err)
	assert.False(t, srv.CaptureFlightTrace("nobody is recording"))

	_, err = NewServer(Config{Service: "svc", Addr: "127.0.0.1:0", DiagFlightRecorder: true, DiagFlightCooldown: -time.Second}, nil)
	require.ErrorContains(t, err, "DIAG_FLIGHT_")
}
