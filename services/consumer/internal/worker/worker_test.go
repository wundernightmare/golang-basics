package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/services/consumer/internal/worker"
)

func newWorker(t testing.TB, opts worker.Options) (*worker.Worker, *prometheus.Registry, *testx.LogBuffer) {
	t.Helper()
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Service: "consumer", Level: "info", Format: "json", Writer: buf})
	reg := prometheus.NewRegistry()
	return worker.New(nil, log, reg, opts), reg, buf
}

// msg builds a record the way the tasks outbox publishes one: JSON payload
// with the event id, headers with the id and the type.
func msg(t testing.TB, eventType, eventID string, payload map[string]any) kafka.Message {
	t.Helper()
	if payload == nil {
		payload = map[string]any{"event_id": eventID, "id": "task-1", "title": "a private title"}
	}
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	m := kafka.Message{Topic: "tasks.events", Value: b, Attempt: 1}
	if eventID != "" {
		m.Headers = append(m.Headers, kafka.Header{Key: kafka.HeaderEventID, Value: []byte(eventID)})
	}
	if eventType != "" {
		m.Headers = append(m.Headers, kafka.Header{Key: kafka.HeaderEventType, Value: []byte(eventType)})
	}
	return m
}

func TestHandle_DispatchesOnEventType(t *testing.T) {
	w, reg, _ := newWorker(t, worker.Options{})
	ctx := t.Context()

	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskCreated, "e1", nil)))
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskUpdated, "e2", nil)))
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskDeleted, "e3", nil)))
	// No header: the payload's event_type decides.
	require.NoError(t, w.Handle(ctx, msg(t, "", "", map[string]any{"event_id": "e4", "event_type": "task.deleted", "task_id": "t"})))
	// Neither: the pre-outbox producer's task.created.
	require.NoError(t, w.Handle(ctx, msg(t, "", "", map[string]any{"id": "t", "title": "x", "created_at": "2026-01-01T00:00:00Z"})))
	// The header wins over the payload.
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskUpdated, "e6", map[string]any{"event_id": "e6", "event_type": "task.deleted"})))

	assert.Equal(t, 2.0, testx.Metric(t, reg, "consumer_events_total", map[string]string{"event_type": worker.TaskCreated}))
	assert.Equal(t, 2.0, testx.Metric(t, reg, "consumer_events_total", map[string]string{"event_type": worker.TaskUpdated}))
	assert.Equal(t, 2.0, testx.Metric(t, reg, "consumer_events_total", map[string]string{"event_type": worker.TaskDeleted}))
	assert.Equal(t, 6.0, testx.Metric(t, reg, "consumer_tasks_consumed_total", nil))
	testx.LintMetrics(t, reg)
}

func TestHandle_PermanentFailures(t *testing.T) {
	w, reg, _ := newWorker(t, worker.Options{})
	cases := map[string]struct {
		msg    kafka.Message
		reason string
	}{
		"undecodable":   {msg: kafka.Message{Value: []byte("not json")}, reason: "undecodable"},
		"not an object": {msg: kafka.Message{Value: []byte(`[1,2]`)}, reason: "undecodable"},
		"unknown type":  {msg: msg(t, "task.archived", "e1", nil), reason: "unknown_type"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			before := testx.Metric(t, reg, "consumer_events_rejected_total", map[string]string{"reason": tc.reason})
			err := w.Handle(t.Context(), tc.msg)
			require.Error(t, err)
			assert.True(t, kafka.IsPermanent(err), "no retry can fix it: straight to the DLQ")
			assert.Equal(t, max(before, 0)+1, testx.Metric(t, reg, "consumer_events_rejected_total", map[string]string{"reason": tc.reason}))
		})
	}
	assert.Equal(t, 0.0, testx.Metric(t, reg, "consumer_tasks_consumed_total", nil), "nothing was applied")
}

func TestHandle_DedupesByEventID(t *testing.T) {
	w, reg, _ := newWorker(t, worker.Options{DedupeSize: 2})
	ctx := t.Context()

	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskCreated, "e1", nil)))
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskCreated, "e1", nil)), "a duplicate is acknowledged")
	// Same id carried only in the payload.
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskCreated, "", map[string]any{"event_id": "e1", "id": "t"})))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "consumer_tasks_consumed_total", nil))
	assert.Equal(t, 2.0, testx.Metric(t, reg, "consumer_duplicates_total", nil))

	// Bounded: two newer ids push e1 out, so it is applied again.
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskUpdated, "e2", nil)))
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskUpdated, "e3", nil)))
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskCreated, "e1", nil)))
	assert.Equal(t, 4.0, testx.Metric(t, reg, "consumer_tasks_consumed_total", nil))
	assert.Equal(t, 2.0, testx.Metric(t, reg, "consumer_duplicates_total", nil))

	// No event id at all: nothing to dedupe on, applied every time.
	legacy := msg(t, "", "", map[string]any{"id": "t"})
	require.NoError(t, w.Handle(ctx, legacy))
	require.NoError(t, w.Handle(ctx, legacy))
	assert.Equal(t, 6.0, testx.Metric(t, reg, "consumer_tasks_consumed_total", nil))
}

func TestHandle_TransientFailureIsRetriedNotDeduped(t *testing.T) {
	calls := 0
	w, reg, _ := newWorker(t, worker.Options{Inject: func(context.Context, string, kafka.Message) error {
		calls++
		if calls == 1 {
			return errors.New("downstream unavailable")
		}
		return nil
	}})
	m := msg(t, worker.TaskCreated, "e1", nil)

	err := w.Handle(t.Context(), m)
	require.Error(t, err)
	assert.False(t, kafka.IsPermanent(err), "a transient failure is retried by the consumer")
	m.Attempt = 2
	require.NoError(t, w.Handle(t.Context(), m), "the retry applies it: a failed attempt did not mark the id as seen")
	assert.Equal(t, 1.0, testx.Metric(t, reg, "consumer_tasks_consumed_total", nil))
	assert.Equal(t, 0.0, testx.Metric(t, reg, "consumer_duplicates_total", nil))
}

func TestHandle_LogsIDsNeverContent(t *testing.T) {
	w, _, buf := newWorker(t, worker.Options{})
	ctx := httpx.WithRequestID(t.Context(), "e1")
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskCreated, "e1", nil)))
	require.NoError(t, w.Handle(ctx, msg(t, worker.TaskCreated, "e1", nil)))

	assert.NotContains(t, buf.String(), "a private title", "user content never reaches a log line")
	line := buf.Find(t, map[string]any{"msg": "task event applied"})
	require.NotNil(t, line)
	assert.Equal(t, "e1", line["event_id"])
	assert.Equal(t, "e1", line["request_id"])
	assert.Equal(t, "task-1", line["task_id"])
	assert.Equal(t, worker.TaskCreated, line["event_type"])
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "duplicate event skipped", "event_id": "e1"}))
}

type fakeConsumer struct{ msgs []kafka.Message }

func (f *fakeConsumer) Run(ctx context.Context, h kafka.Handler) error {
	for _, m := range f.msgs {
		if err := h(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func TestRun_DrivesTheHandler(t *testing.T) {
	fc := &fakeConsumer{msgs: []kafka.Message{msg(t, worker.TaskCreated, "e1", nil), msg(t, worker.TaskDeleted, "e2", nil)}}
	reg := prometheus.NewRegistry()
	w := worker.New(fc, slog.New(slog.DiscardHandler), reg, worker.Options{})
	require.NoError(t, w.Run(t.Context()))
	assert.Equal(t, 2.0, testx.Metric(t, reg, "consumer_tasks_consumed_total", nil))
}

// Handle parses bytes off the wire: whatever arrives, it applies, skips or
// returns an error — it never panics.
func FuzzHandle(f *testing.F) {
	f.Add([]byte(`{"event_id":"e1","id":"t","title":"x"}`), "task.created", "e1")
	f.Add([]byte(`{"event_type":"task.deleted","task":{"id":"t"}}`), "", "")
	f.Add([]byte(`not json`), "task.updated", "")
	f.Add([]byte(`{"task":null}`), "\x00", "\xff")
	w := worker.New(nil, slog.New(slog.DiscardHandler), prometheus.NewRegistry(), worker.Options{DedupeSize: 16})
	f.Fuzz(func(t *testing.T, value []byte, eventType, eventID string) {
		m := kafka.Message{Value: value, Headers: []kafka.Header{
			{Key: kafka.HeaderEventType, Value: []byte(eventType)},
			{Key: kafka.HeaderEventID, Value: []byte(eventID)},
		}}
		_ = w.Handle(context.Background(), m)
	})
}
