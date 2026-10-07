package worker_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/services/consumer/internal/worker"
)

func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

// Over the real broker: task events published the way the tasks outbox does
// (headers + event_id) are applied once each, a redelivered event id is
// skipped, an unknown type is dead-lettered without stopping the worker, and
// each applied event's log line carries the producing request's trace.
func TestIntegration_ConsumesTaskEvents(t *testing.T) {
	brokers := containers.Kafka(t)
	testx.Recorder(t) // before the clients: kotel captures the provider then
	topic := testx.Unique("tasks.events")
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Service: "consumer", Level: "info", Format: "json", Writer: buf})

	// The test broker creates topics on demand; a deployment pre-provisions
	// them (auto-creation is off by default).
	prod, err := kafka.NewProducer(t.Context(), kafka.Config{
		Brokers: brokers, Topic: topic, ClientID: testx.Unique("prod"),
		AllowAutoTopicCreation: true, PublishTimeout: 15 * time.Second,
	}, log)
	require.NoError(t, err)
	t.Cleanup(prod.Close)

	traces := map[string]string{} // event id → trace id of the request that produced it
	publish := func(eventType, eventID string) {
		pctx, span := otel.Tracer("test").Start(t.Context(), "POST /tasks")
		defer span.End()
		payload, err := json.Marshal(map[string]any{"event_id": eventID, "id": "task-" + eventID, "title": "private"})
		require.NoError(t, err)
		require.NoError(t, prod.Publish(pctx, topic, []byte("task-"+eventID), payload,
			kafka.Header{Key: kafka.HeaderEventID, Value: []byte(eventID)},
			kafka.Header{Key: kafka.HeaderEventType, Value: []byte(eventType)},
			kafka.Header{Key: kafka.HeaderContentType, Value: []byte("application/json")}))
		if _, ok := traces[eventID]; !ok {
			traces[eventID] = span.SpanContext().TraceID().String()
		}
	}
	publish(worker.TaskCreated, "e1")
	publish(worker.TaskCreated, "e1") // the relay re-published it
	publish("task.archived", "e2")    // a type this worker does not know
	publish(worker.TaskDeleted, "e3")

	cons, err := kafka.NewConsumer(t.Context(), kafka.Config{
		Brokers: brokers, Topics: []string{topic}, Group: testx.Unique("group"), ClientID: testx.Unique("cons"),
		AllowAutoTopicCreation: true, PublishTimeout: 15 * time.Second, LagInterval: -1,
	}, log)
	require.NoError(t, err)
	reg := prometheus.NewRegistry()
	reg.MustRegister(cons.Collectors()...)
	w := worker.New(cons, log, reg, worker.Options{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(cancel)

	require.Eventually(t, func() bool {
		return testx.Metric(t, reg, "consumer_tasks_consumed_total", nil) == 2 &&
			testx.Metric(t, reg, "consumer_duplicates_total", nil) == 1 &&
			testx.Metric(t, reg, "kafka_consume_total", map[string]string{"topic": topic, "result": "dead_lettered"}) == 1
	}, 60*time.Second, 100*time.Millisecond)
	assert.Equal(t, 1.0, testx.Metric(t, reg, "consumer_events_total", map[string]string{"event_type": worker.TaskCreated}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "consumer_events_total", map[string]string{"event_type": worker.TaskDeleted}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "kafka_consumer_dlq_total", map[string]string{"topic": topic + ".dlq"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "consumer_events_rejected_total", map[string]string{"reason": "unknown_type"}))

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "shutdown is clean")
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not stop")
	}

	applied := map[string]string{}
	for _, m := range buf.Lines(t) {
		if m["msg"] != "task event applied" {
			continue
		}
		assert.Equal(t, "consumer", m["service"])
		assert.Equal(t, m["event_id"], m["request_id"], "the event id correlates the logs")
		id, _ := m["event_id"].(string)
		tid, _ := m["trace_id"].(string)
		applied[id] = tid
	}
	assert.Equal(t, map[string]string{"e1": traces["e1"], "e3": traces["e3"]}, applied,
		"each applied event is logged once, under the trace of the request that produced it")
	assert.NotContains(t, buf.String(), `"private"`, "no payload content in the logs")
}
