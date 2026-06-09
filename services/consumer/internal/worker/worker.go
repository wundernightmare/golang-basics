// Package worker implements the consumer loop: it drains the tasks.events topic
// via the shared libs/kafka consumer and, for each task.created event, bumps a
// Prometheus counter and logs a structured line. It is the "broker-fed worker"
// shape of this monorepo — the counterpart to services/tasks, which produces
// these events, and a real-broker version of services/heartbeat's ticker.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// taskCreatedEvent is the consumer's local copy of the event contract published
// by services/tasks. It is intentionally duplicated rather than imported: the
// two services are decoupled and only share the JSON shape on the wire.
type taskCreatedEvent struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
}

// Consumer is the lib dependency (satisfied by *libs/kafka.Consumer); the
// interface keeps the worker unit-testable without a broker.
type Consumer interface {
	Run(ctx context.Context, handler kafka.Handler) error
}

// Worker consumes task events until its context is cancelled.
type Worker struct {
	consumer Consumer
	log      *slog.Logger
	consumed prometheus.Counter
	skipped  prometheus.Counter
}

// New builds a Worker over consumer and registers its metrics on reg (typically
// the server's registry, so they appear on /metrics):
//
//	consumer_tasks_consumed_total  events handled successfully
//	consumer_tasks_skipped_total   events dropped as undecodable
func New(consumer Consumer, log *slog.Logger, reg prometheus.Registerer) *Worker {
	consumed := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_tasks_consumed_total",
		Help: "Total number of task.created events consumed successfully.",
	})
	skipped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_tasks_skipped_total",
		Help: "Total number of events skipped because they could not be decoded.",
	})
	reg.MustRegister(consumed, skipped)
	return &Worker{consumer: consumer, log: log, consumed: consumed, skipped: skipped}
}

// Run drains the topic until ctx is cancelled (graceful shutdown → nil) or the
// consumer errors.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("consumer worker started")
	return w.consumer.Run(ctx, w.handle)
}

// handle decodes one event and records it. An undecodable message is counted as
// skipped and acknowledged (returning nil) rather than failing the loop — a
// single poison record must not wedge the consumer. A real pipeline would route
// it to a dead-letter topic.
func (w *Worker) handle(_ context.Context, msg kafka.Message) error {
	var evt taskCreatedEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		w.skipped.Inc()
		w.log.Warn("skipping undecodable event", "partition", msg.Partition, "offset", msg.Offset, "err", err)
		return nil
	}
	w.consumed.Inc()
	w.log.Info("task.created consumed",
		"id", evt.ID, "title", evt.Title, "partition", msg.Partition, "offset", msg.Offset)
	return nil
}
