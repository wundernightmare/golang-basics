// Package worker implements the consumer loop: it drains the tasks.events topic
// via the shared libs/kafka consumer and applies each task event
// (task.created / task.updated / task.deleted). It is the "broker-fed worker"
// shape of this monorepo — the counterpart to services/tasks, which produces
// these events through its outbox, and a real-broker version of
// services/heartbeat's ticker.
//
// It is also the worked example of an idempotent at-least-once consumer:
// libs/kafka may deliver a record twice (a crash between processing and
// commit, a rebalance, the outbox relay re-publishing), so the worker
// remembers the event ids it has applied and skips a repeat.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// Event types the worker applies.
const (
	TaskCreated = "task.created"
	TaskUpdated = "task.updated"
	TaskDeleted = "task.deleted"
)

// DefaultDedupeSize is how many applied event ids are remembered when
// Options.DedupeSize is zero.
const DefaultDedupeSize = 10000

// Consumer is the lib dependency (satisfied by *libs/kafka.Consumer); the
// interface keeps the worker unit-testable without a broker.
type Consumer interface {
	Run(ctx context.Context, handler kafka.Handler) error
}

// Options tunes a [Worker].
type Options struct {
	// DedupeSize bounds the in-memory set of applied event ids (an LRU).
	// Default [DefaultDedupeSize].
	DedupeSize int
	// Inject, when set, runs before an event is applied; a non-nil error
	// is returned as the handler's (a transient failure unless wrapped with
	// kafka.Permanent). A test hook for exercising retries and the DLQ.
	Inject func(ctx context.Context, eventType string, msg kafka.Message) error
}

// Worker consumes task events until its context is cancelled.
type Worker struct {
	consumer Consumer
	log      *slog.Logger
	inject   func(context.Context, string, kafka.Message) error
	seen     *lru

	consumed   prometheus.Counter
	byType     *prometheus.CounterVec
	duplicates prometheus.Counter
	rejected   *prometheus.CounterVec
}

// New builds a Worker over consumer and registers its metrics on reg (typically
// the server's registry, so they appear on /metrics):
//
//	consumer_tasks_consumed_total                 events applied (every type)
//	consumer_events_total{event_type}             events applied, by type
//	consumer_duplicates_total                     redeliveries skipped by event id
//	consumer_events_rejected_total{reason}        events rejected as permanent failures
//	                                              (undecodable|unknown_type); they go to the DLQ
func New(consumer Consumer, log *slog.Logger, reg prometheus.Registerer, opts Options) *Worker {
	if opts.DedupeSize <= 0 {
		opts.DedupeSize = DefaultDedupeSize
	}
	w := &Worker{
		consumer: consumer,
		log:      log,
		inject:   opts.Inject,
		seen:     newLRU(opts.DedupeSize),
		consumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "consumer_tasks_consumed_total",
			Help: "Task events applied (every type).",
		}),
		byType: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_events_total",
			Help: "Task events applied, by event type.",
		}, []string{"event_type"}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "consumer_duplicates_total",
			Help: "Redelivered events skipped because their event id was already applied.",
		}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_events_rejected_total",
			Help: "Events rejected as permanent failures (dead-lettered), by reason (undecodable|unknown_type).",
		}, []string{"reason"}),
	}
	reg.MustRegister(w.consumed, w.byType, w.duplicates, w.rejected)
	return w
}

// Run drains the topic until ctx is cancelled (graceful shutdown → nil) or the
// consumer errors.
func (w *Worker) Run(ctx context.Context) error {
	w.log.InfoContext(ctx, "consumer worker started")
	return w.consumer.Run(ctx, w.Handle)
}

// taskEvent is the part of a task event the worker reads: ids and the type.
// It deliberately has no title or other user content, so none can reach a
// log line.
type taskEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	ID        string `json:"id"`
	TaskID    string `json:"task_id"`
	Task      *struct {
		ID string `json:"id"`
	} `json:"task"`
}

func (e taskEvent) taskID() string {
	switch {
	case e.ID != "":
		return e.ID
	case e.TaskID != "":
		return e.TaskID
	case e.Task != nil:
		return e.Task.ID
	}
	return ""
}

// Handle applies one event; it is the [kafka.Handler] the worker runs.
//
// Dispatch is on the event_type header, falling back to an "event_type"
// field in the payload, and to task.created for a payload with neither (the
// pre-outbox producer sent only that event, without headers). An
// undecodable payload or an unknown type is a [kafka.Permanent] error: no
// retry can fix it, so it goes straight to the dead-letter topic. An event
// id already applied is acknowledged without being applied again.
//
// ctx carries the record's process span (continued from the producer's trace
// via the record headers) and the event id as request_id, so the *Context
// log calls stamp both.
func (w *Worker) Handle(ctx context.Context, msg kafka.Message) error {
	var evt taskEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		w.rejected.WithLabelValues("undecodable").Inc()
		return kafka.Permanent(fmt.Errorf("decode task event (%d bytes): %w", len(msg.Value), err))
	}
	eventType := evt.EventType
	if h, ok := msg.Header(kafka.HeaderEventType); ok {
		eventType = string(h)
	}
	if eventType == "" {
		eventType = TaskCreated
	}
	switch eventType {
	case TaskCreated, TaskUpdated, TaskDeleted:
	default:
		w.rejected.WithLabelValues("unknown_type").Inc()
		return kafka.Permanent(fmt.Errorf("unknown event type %q", truncate(eventType, 64)))
	}
	eventID := evt.EventID
	if h, ok := msg.Header(kafka.HeaderEventID); ok && len(h) > 0 {
		eventID = string(h)
	}

	if eventID != "" && w.seen.contains(eventID) {
		w.duplicates.Inc()
		w.log.InfoContext(ctx, "duplicate event skipped",
			"event_type", eventType, "event_id", eventID, "partition", msg.Partition, "offset", msg.Offset)
		return nil
	}
	if w.inject != nil {
		if err := w.inject(ctx, eventType, msg); err != nil {
			return err
		}
	}

	// Applying the event is where a real worker would call a downstream
	// system (index, notify, project); this one counts it. Remember the id
	// only once it is applied, so a failed attempt is retried, not skipped.
	if eventID != "" {
		w.seen.add(eventID)
	}
	w.consumed.Inc()
	w.byType.WithLabelValues(eventType).Inc()
	w.log.InfoContext(ctx, "task event applied",
		"event_type", eventType, "event_id", eventID, "task_id", evt.taskID(),
		"bytes", len(msg.Value), "partition", msg.Partition, "offset", msg.Offset, "attempt", msg.Attempt)
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
