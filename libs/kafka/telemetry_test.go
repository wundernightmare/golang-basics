package kafka_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// A record carries its trace through the broker: the consumer's handler runs
// in a span whose trace id is the producer's, the produce/receive/process
// spans all land in that one trace, and the counters + lag describe exactly
// what happened. Real broker (testcontainers), real SDK, in-memory exporter.
func TestTraceAndMetricsFollowARecordThroughTheBroker(t *testing.T) {
	brokers := startKafka(t)
	sr := installRecorder(t)
	ctx := context.Background()
	const topic = "tasks.events.telemetry"

	prod, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: brokers, Topic: topic, ClientID: "telemetry-prod", DialTimeout: 10 * time.Second,
	}, nil)
	require.NoError(t, err)
	defer prod.Close()

	cons, err := kafka.NewConsumer(ctx, kafka.Config{
		Brokers: brokers, Topics: []string{topic}, Group: "telemetry-group",
		ClientID: "telemetry-cons", DialTimeout: 10 * time.Second, LagInterval: 300 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	defer cons.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(prod.Collectors()...)
	reg.MustRegister(cons.Collectors()...)

	// Publish under a parent span, as an HTTP handler would.
	pctx, parent := otel.Tracer("test").Start(ctx, "create-task")
	require.NoError(t, prod.Publish(pctx, topic, []byte("k"), []byte(`{"id":"1"}`)))
	parent.End()
	parentTrace := parent.SpanContext().TraceID()

	// Consume: capture the span context the handler is given.
	got := make(chan trace.SpanContext, 1)
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go func() {
		_ = cons.Run(runCtx, func(hctx context.Context, _ kafka.Message) error {
			select {
			case got <- trace.SpanContextFromContext(hctx):
			default:
			}
			return nil
		})
	}()

	var handlerSC trace.SpanContext
	select {
	case handlerSC = <-got:
	case <-runCtx.Done():
		t.Fatal("record was not consumed in time")
	}
	require.True(t, handlerSC.IsValid(), "handler context carries a span")
	require.Equal(t, parentTrace, handlerSC.TraceID(), "consumer continues the producer's trace")

	// Lag reaches zero once the record is committed; the counters are exact.
	require.Eventually(t, func() bool {
		return metricValue(t, reg, "kafka_consumer_group_lag", map[string]string{"topic": topic, "partition": "0"}) == 0
	}, 20*time.Second, 100*time.Millisecond, "group lag must drop to 0 after commit")
	cancel()

	require.Equal(t, 1.0, metricValue(t, reg, "kafka_producer_records_total", map[string]string{"topic": topic, "result": "ok"}))
	require.Equal(t, 1.0, metricValue(t, reg, "kafka_producer_publish_duration_seconds", map[string]string{"topic": topic}))
	require.Equal(t, 1.0, metricValue(t, reg, "kafka_consumer_records_total", map[string]string{"topic": topic}))
	require.Equal(t, 1.0, metricValue(t, reg, "kafka_consumer_handle_duration_seconds", map[string]string{"topic": topic}))
	require.Equal(t, -1.0, metricValue(t, reg, "kafka_consumer_handler_errors_total", map[string]string{"topic": topic}), "no error series")
	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	require.Empty(t, problems)

	// The produce / receive / process spans are all in the parent's trace.
	kinds := map[string]bool{}
	for _, s := range sr.Ended() {
		if s.SpanContext().TraceID() != parentTrace {
			continue
		}
		switch {
		case strings.HasSuffix(s.Name(), " publish"):
			kinds["publish"] = true
			require.Equal(t, trace.SpanKindProducer, s.SpanKind())
		case strings.HasSuffix(s.Name(), " receive"):
			kinds["receive"] = true
		case strings.HasSuffix(s.Name(), " process"):
			kinds["process"] = true
			require.Equal(t, trace.SpanKindConsumer, s.SpanKind())
			require.Equal(t, handlerSC.SpanID(), s.SpanContext().SpanID(), "the handler ran inside the process span")
		}
	}
	require.Equal(t, map[string]bool{"publish": true, "receive": true, "process": true}, kinds)
}
