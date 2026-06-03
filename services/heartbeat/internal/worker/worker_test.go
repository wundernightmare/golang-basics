package worker_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/heartbeat/internal/worker"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(noopWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestWorker_BeatsThenStopsOnCancel(t *testing.T) {
	reg := prometheus.NewRegistry()
	w := worker.New(20*time.Millisecond, quietLogger(), reg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Let a few ticks happen.
	time.Sleep(120 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err) // graceful cancel is not an error
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancel")
	}

	assert.GreaterOrEqual(t, gatherBeats(t, reg), 2.0, "expected at least a couple of beats")
}

// gatherBeats reads the heartbeat_beats_total value back through the registry,
// exactly as the /metrics endpoint would — the Worker keeps the counter private.
func gatherBeats(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "heartbeat_beats_total" {
			require.NotEmpty(t, mf.GetMetric())
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatal("heartbeat_beats_total not found in registry")
	return 0
}
