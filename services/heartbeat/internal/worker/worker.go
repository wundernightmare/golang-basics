// Package worker implements the heartbeat loop: a ticker that emits a
// structured log line and bumps a Prometheus counter on every tick. It is the
// "background worker" shape of this monorepo (the analogue of a Kafka-consumer
// crate in the Rust sibling repo, minus the broker), and it still reuses the
// shared libs/httpx server for its /healthz, /readyz and /metrics surface.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Worker periodically "beats" until its context is cancelled.
type Worker struct {
	interval time.Duration
	log      *slog.Logger
	beats    prometheus.Counter
}

// New builds a Worker ticking every interval and registers its
// heartbeat_beats_total counter on reg (typically the server's metrics
// registry, so the count is exported on /metrics).
func New(interval time.Duration, log *slog.Logger, reg prometheus.Registerer) *Worker {
	beats := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "heartbeat_beats_total",
		Help: "Total number of heartbeats emitted since process start.",
	})
	reg.MustRegister(beats)
	return &Worker{interval: interval, log: log, beats: beats}
}

// Run ticks until ctx is cancelled, then returns nil (a cancelled context is
// the normal graceful-shutdown signal, not an error).
func (w *Worker) Run(ctx context.Context) error {
	t := time.NewTicker(w.interval)
	defer t.Stop()

	w.log.Info("heartbeat worker started", "interval", w.interval)
	var count uint64
	for {
		select {
		case <-ctx.Done():
			w.log.Info("heartbeat worker stopping", "total_beats", count)
			return nil
		case <-t.C:
			count++
			w.beats.Inc()
			w.log.Info("heartbeat", "count", count)
		}
	}
}
