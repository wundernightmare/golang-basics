package worker_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/services/heartbeat/internal/worker"
)

func TestWorker_BeatsThenStopsOnCancel(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: buf})
	w := worker.New(20*time.Millisecond, log, reg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for a couple of ticks to be counted, then stop.
	require.Eventually(t, func() bool { return testx.Metric(t, reg, "heartbeat_beats_total", nil) >= 2 },
		2*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "a graceful cancel is not an error")
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancel")
	}

	beats := testx.Metric(t, reg, "heartbeat_beats_total", nil)
	assert.GreaterOrEqual(t, beats, 2.0)
	stop := buf.Find(t, map[string]any{"msg": "heartbeat worker stopping"})
	require.NotNil(t, stop)
	assert.Equal(t, beats, stop["total_beats"], "the final log line and the counter agree")
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "heartbeat", "count": 1.0}))
	testx.LintMetrics(t, reg)
}

func TestWorker_CounterOnTheServerRegistry(t *testing.T) {
	t.Parallel()
	srv, err := httpx.NewServer(httpx.Config{Service: "heartbeat", AdminAddr: "127.0.0.1:0"}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	worker.New(time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)), srv.Metrics.Registry)
	assert.Equal(t, 0.0, testx.Metric(t, srv.Metrics.Registry, "heartbeat_beats_total", nil),
		"exported (at zero) on the admin /metrics from the start")
}
