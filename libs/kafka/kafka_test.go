package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/testo"
	allure "github.com/ozontech/testo-allure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// One Kafka (KRaft) per test binary (testx); every test owns a topic and a
// consumer group by name.
func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }

func TestLoadConfigDefaults(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		cfg, err := kafka.LoadConfig("TASKS_")
		require.NoError(t, err)
		require.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
		require.Equal(t, "tasks.events", cfg.Topic)
		require.Equal(t, []string{"tasks.events"}, cfg.Topics, "Topics falls back to the single Topic")

		t.Setenv("TASKS_KAFKA_BROKERS", "a:9092,b:9092")
		t.Setenv("TASKS_KAFKA_TOPICS", "x,y")
		cfg, err = kafka.LoadConfig("TASKS_")
		require.NoError(t, err)
		require.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)
		require.Equal(t, []string{"x", "y"}, cfg.Topics)
	}, "kafka", "unit")
}

type Suite struct{ testo.Suite[testx.T] }

func TestKafka(t *testing.T) {
	testo.RunSuite(t, new(Suite), testx.Options("kafka", "integration", testx.Meta{
		Epic: "golang-basics", Feature: "kafka client", Owner: "@team-platform", // sample TestOps values
	})...)
}

func producer(t testx.T, topic string) *kafka.Producer {
	t.Helper()
	p, err := kafka.NewProducer(context.Background(), kafka.Config{
		Brokers: testx.Kafka(t), Topic: topic, ClientID: testx.Unique("prod"), DialTimeout: 10 * time.Second,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

func consumer(t testx.T, topic string, lag time.Duration) *kafka.Consumer {
	t.Helper()
	c, err := kafka.NewConsumer(context.Background(), kafka.Config{
		Brokers: testx.Kafka(t), Topics: []string{topic}, Group: testx.Unique("group"),
		ClientID: testx.Unique("cons"), DialTimeout: 10 * time.Second, LagInterval: lag,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// One trip through the broker proves delivery, ordering, the trace continuing
// from producer to consumer, and the counters + lag describing exactly what
// happened. Real broker, real SDK, in-memory exporter.
func (Suite) TestProduceConsume(t testx.T) {
	testx.Case(t, "GB-301", "produce and consume") // sample TestOps id — replace with your project\'s
	t.Title("records round-trip the broker with their trace and are accounted for")
	t.Severity(allure.SeverityCritical)

	sr := testx.Recorder(t) // before the clients: kotel captures the provider then
	topic := testx.Unique("tasks.events")
	prod := producer(t, topic)
	cons := consumer(t, topic, 300*time.Millisecond)
	reg := prometheus.NewRegistry()
	reg.MustRegister(prod.Collectors()...)
	reg.MustRegister(cons.Collectors()...)

	const n = 5
	var parentTrace trace.TraceID
	testx.Step(t, "publish 5 keyed records under one parent span", func(t testx.T) {
		pctx, parent := otel.Tracer("test").Start(context.Background(), "create-task")
		for i := range n {
			require.NoError(t, prod.Publish(pctx, topic, fmt.Appendf(nil, "key-%d", i), fmt.Appendf(nil, "value-%d", i)))
		}
		parent.End()
		parentTrace = parent.SpanContext().TraceID()
		require.Equal(t, float64(n), testx.Metric(t, reg, "kafka_producer_records_total", map[string]string{"topic": topic, "result": "ok"}))
	})

	var (
		mu    sync.Mutex
		got   = map[string]string{}
		spans = map[string]trace.SpanContext{}
	)
	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = cons.Run(runCtx, func(hctx context.Context, msg kafka.Message) error {
			mu.Lock()
			got[string(msg.Key)] = string(msg.Value)
			spans[string(msg.Key)] = trace.SpanContextFromContext(hctx)
			full := len(got) == n
			mu.Unlock()
			if full {
				select {
				case <-done:
				default:
					close(done)
				}
			}
			return nil
		})
	}()

	testx.Step(t, "every record arrives with its key and value", func(t testx.T) {
		select {
		case <-done:
		case <-runCtx.Done():
			t.Fatal("timed out waiting for all records")
		}
		mu.Lock()
		defer mu.Unlock()
		for i := range n {
			require.Equal(t, fmt.Sprintf("value-%d", i), got[fmt.Sprintf("key-%d", i)])
		}
	})

	testx.Step(t, "the handler runs inside the producer's trace", func(t testx.T) {
		mu.Lock()
		defer mu.Unlock()
		for k, sc := range spans {
			require.True(t, sc.IsValid(), k)
			require.Equal(t, parentTrace, sc.TraceID(), "%s: consumer continues the producer's trace", k)
		}
	})

	testx.Step(t, "group lag drops to zero once the records are committed", func(t testx.T) {
		require.Eventually(t, func() bool {
			return testx.Metric(t, reg, "kafka_consumer_group_lag", map[string]string{"topic": topic, "partition": "0"}) == 0
		}, 20*time.Second, 100*time.Millisecond)
	})
	cancel()

	testx.Step(t, "counters are exact and lint-clean", func(t testx.T) {
		require.Equal(t, float64(n), testx.Metric(t, reg, "kafka_consumer_records_total", map[string]string{"topic": topic}))
		require.Equal(t, float64(n), testx.Metric(t, reg, "kafka_consumer_handle_duration_seconds", map[string]string{"topic": topic}))
		require.Equal(t, -1.0, testx.Metric(t, reg, "kafka_consumer_handler_errors_total", map[string]string{"topic": topic}), "no error series")
		testx.LintMetrics(t, reg)
	})

	testx.Step(t, "publish, receive and process spans share the trace", func(t testx.T) {
		kinds := map[string]int{}
		for _, s := range sr.Ended() {
			if s.SpanContext().TraceID() != parentTrace {
				continue
			}
			switch {
			case strings.HasSuffix(s.Name(), " publish"):
				kinds["publish"]++
				require.Equal(t, trace.SpanKindProducer, s.SpanKind())
			case strings.HasSuffix(s.Name(), " receive"):
				kinds["receive"]++
			case strings.HasSuffix(s.Name(), " process"):
				kinds["process"]++
				require.Equal(t, trace.SpanKindConsumer, s.SpanKind())
			}
		}
		require.Equal(t, map[string]int{"publish": n, "receive": n, "process": n}, kinds)
	})
}

func (Suite) TestConsumerStopsOnContextCancel(t testx.T) {
	testx.Case(t, "GB-302", "graceful shutdown") // sample TestOps id — replace with your project\'s
	t.Title("a cancelled context is a clean shutdown, not an error")
	cons := consumer(t, testx.Unique("tasks.events.idle"), 0)

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- cons.Run(runCtx, func(context.Context, kafka.Message) error { return nil }) }()

	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop on context cancel")
	}
}
