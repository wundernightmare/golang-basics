package worker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ozontech/testo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/services/consumer/internal/worker"
)

func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }

// fakeConsumer drives the worker's handler with a fixed set of messages, no
// broker required.
type fakeConsumer struct{ msgs []kafka.Message }

func (f *fakeConsumer) Run(ctx context.Context, h kafka.Handler) error {
	for _, m := range f.msgs {
		if err := h(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// Unit: the handler's decode / count / skip logic, driven directly.
func TestWorkerCountsConsumedAndSkipped(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		valid, _ := json.Marshal(map[string]any{"id": "1", "title": "a"})
		fc := &fakeConsumer{msgs: []kafka.Message{
			{Value: valid},
			{Value: []byte("not json")},
			{Value: valid},
		}}
		reg := prometheus.NewRegistry()
		w := worker.New(fc, slog.New(slog.DiscardHandler), reg)

		require.NoError(t, w.Run(context.Background()))

		require.Equal(t, float64(2), testx.Metric(t, reg, "consumer_tasks_consumed_total", nil))
		require.Equal(t, float64(1), testx.Metric(t, reg, "consumer_tasks_skipped_total", nil))
	}, "consumer", "unit")
}

type Suite struct{ testo.Suite[testx.T] }

func TestConsumerWorker(t *testing.T) {
	testo.RunSuite(t, new(Suite), testx.Options("consumer", "integration")...)
}

// Integration: over the real broker, the worker consumes what tasks produced,
// its log line for each event names the trace of the request that produced
// it, and its counters plus the lib's lag gauge agree with what was consumed.
func (Suite) TestDrainsEventsWithTheirTrace(t testx.T) {
	t.Title("the worker drains task.created events and keeps their trace")
	topic := testx.Unique("tasks.events")
	ctx := context.Background()

	testx.Recorder(t) // before the clients: kotel captures the provider then
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Service: "consumer", Level: "info", Format: "json", Writer: buf})

	prod, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: testx.Kafka(t), Topic: topic, ClientID: testx.Unique("prod"), DialTimeout: 10 * time.Second,
	}, log)
	require.NoError(t, err)
	t.Cleanup(prod.Close)

	// One event per parent trace, so each log line can be matched to its trace.
	traces := map[string]string{} // event id → trace id
	testx.Step(t, "tasks publishes two events, each under its own request span", func(t testx.T) {
		for _, id := range []string{"a", "b"} {
			pctx, span := otel.Tracer("test").Start(ctx, "POST /tasks")
			payload, _ := json.Marshal(map[string]any{"id": id, "title": "t-" + id, "created_at": time.Now().UTC()})
			require.NoError(t, prod.Publish(pctx, topic, []byte(id), payload))
			span.End()
			traces[id] = span.SpanContext().TraceID().String()
		}
	})

	cons, err := kafka.NewConsumer(ctx, kafka.Config{
		Brokers: testx.Kafka(t), Topics: []string{topic}, Group: testx.Unique("group"),
		ClientID: testx.Unique("cons"), DialTimeout: 10 * time.Second, LagInterval: 300 * time.Millisecond,
	}, log)
	require.NoError(t, err)
	t.Cleanup(cons.Close)
	reg := prometheus.NewRegistry()
	reg.MustRegister(cons.Collectors()...)
	w := worker.New(cons, log, reg)

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go func() { _ = w.Run(runCtx) }()

	testx.Step(t, "both events are consumed and committed (lag back to 0)", func(t testx.T) {
		require.Eventually(t, func() bool {
			return testx.Metric(t, reg, "consumer_tasks_consumed_total", nil) == 2 &&
				testx.Metric(t, reg, "kafka_consumer_group_lag", map[string]string{"topic": topic, "partition": "0"}) == 0
		}, 25*time.Second, 250*time.Millisecond)
		require.Equal(t, 2.0, testx.Metric(t, reg, "kafka_consumer_records_total", map[string]string{"topic": topic}))
	})
	cancel()

	testx.Step(t, "each consumed-event log line carries the producing request's trace id", func(t testx.T) {
		consumed := map[string]string{}
		for _, m := range buf.Lines(t) {
			if m["msg"] != "task.created consumed" {
				continue
			}
			require.Equal(t, "consumer", m["service"])
			id, _ := m["id"].(string)
			tid, _ := m["trace_id"].(string)
			consumed[id] = tid
		}
		require.Equal(t, traces, consumed)
	})
}
