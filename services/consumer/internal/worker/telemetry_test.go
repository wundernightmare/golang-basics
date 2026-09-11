package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/services/consumer/internal/worker"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// The worker's own log line for a consumed event carries the trace id of the
// request that produced it — through a real broker — and its counters plus
// the lib's lag gauge agree with what was consumed.
func TestWorkerLogLinesCarryTheProducersTrace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}
	ctx := context.Background()
	const topic = "tasks.events.consumer-telemetry"

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		if _, ok := os.LookupEnv("CI"); ok {
			require.NoError(t, err)
		}
		t.Skipf("docker unavailable (kafka): %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	installRecorder(t) // before the clients are built: kotel captures the provider then
	buf := &syncBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Service: "consumer", Level: "info", Format: "json", Writer: buf})

	prod, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: brokers, Topic: topic, ClientID: "consumer-telemetry-prod", DialTimeout: 10 * time.Second,
	}, log)
	require.NoError(t, err)
	defer prod.Close()

	// One event per parent trace, so each log line can be matched to its trace.
	traces := map[string]string{} // event id → trace id
	for _, id := range []string{"a", "b"} {
		pctx, span := otel.Tracer("test").Start(ctx, "POST /tasks")
		payload, _ := json.Marshal(map[string]any{"id": id, "title": "t-" + id, "created_at": time.Now().UTC()})
		require.NoError(t, prod.Publish(pctx, topic, []byte(id), payload))
		span.End()
		traces[id] = span.SpanContext().TraceID().String()
	}

	cons, err := kafka.NewConsumer(ctx, kafka.Config{
		Brokers: brokers, Topics: []string{topic}, Group: "consumer-telemetry-group",
		ClientID: "consumer-telemetry", DialTimeout: 10 * time.Second, LagInterval: 300 * time.Millisecond,
	}, log)
	require.NoError(t, err)
	defer cons.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(cons.Collectors()...)
	w := worker.New(cons, log, reg)

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go func() { _ = w.Run(runCtx) }()

	require.Eventually(t, func() bool {
		return metricValue(t, reg, "consumer_tasks_consumed_total", nil) == 2 &&
			metricValue(t, reg, "kafka_consumer_group_lag", map[string]string{"topic": topic, "partition": "0"}) == 0
	}, 25*time.Second, 250*time.Millisecond, "both events consumed and committed")
	cancel()

	require.Equal(t, 2.0, metricValue(t, reg, "kafka_consumer_records_total", map[string]string{"topic": topic}))

	consumed := map[string]string{} // event id → trace id seen in the log
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m), line)
		if m["msg"] != "task.created consumed" {
			continue
		}
		require.Equal(t, "consumer", m["service"])
		id, _ := m["id"].(string)
		tid, _ := m["trace_id"].(string)
		consumed[id] = tid
	}
	require.Equal(t, traces, consumed, "each consumed-event log line carries the producing request's trace id")
}
