// Package kafkatest is the conformance suite of libs/kafka: the behaviour
// every provider (libs/kafka-rdkafka, libs/kafka-franz) must show against a
// real broker — delivery with headers and trace context, retries,
// Permanent, the dead-letter topic, commit-after-handle across restarts,
// graceful drain, rebalances without loss, the error taxonomy and the
// client metrics. A provider's test calls [Run] with a [Harness] built from
// its own admin client; the expectations are the same for both.
//
// Test-only code: import it from _test.go files.
package kafkatest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// Harness is what the suite needs from a provider's test: a broker and two
// admin operations the libs/kafka API does not offer.
type Harness struct {
	// Brokers returns the seed addresses of a test broker.
	Brokers func(t testing.TB) []string
	// CreateTopic creates the topics with the given partition count and
	// waits until every partition has a leader that serves it.
	CreateTopic func(t testing.TB, partitions int32, names ...string)
	// Committed returns the group's committed offsets for topic, by
	// partition.
	Committed func(t testing.TB, group, topic string) map[int32]int64
}

// Run runs the conformance suite against the linked provider.
func Run(t *testing.T, h Harness) {
	t.Helper()
	t.Run("ProduceConsume_HeadersTraceAndContext", func(t *testing.T) { produceConsume(t, h) })
	t.Run("Consumer_RetriesThenSucceeds", func(t *testing.T) { retriesThenSucceeds(t, h) })
	t.Run("Consumer_ExhaustedAndPermanentGoToDLQ", func(t *testing.T) { deadLetter(t, h) })
	t.Run("Consumer_DLQDisabledStopsOnPoison", func(t *testing.T) { dlqDisabled(t, h) })
	t.Run("Consumer_GracefulShutdownCommitsProcessed", func(t *testing.T) { gracefulShutdown(t, h) })
	t.Run("Consumer_RebalanceNoLossBoundedDuplicates", func(t *testing.T) { rebalance(t, h) })
	t.Run("Consumer_StopsOnContextCancel", func(t *testing.T) { stopsOnCancel(t, h) })
	t.Run("Producer_ErrorTaxonomy", func(t *testing.T) { errorTaxonomy(t, h) })
}

// ── helpers ──────────────────────────────────────────────────────────────────

func newProducer(t testing.TB, h Harness, topic string) *kafka.Producer {
	t.Helper()
	p, err := kafka.NewProducer(t.Context(), kafka.Config{
		Brokers: h.Brokers(t), Topic: topic, ClientID: testx.Unique("prod"),
	}, nil)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

// consumerConfig is a fast-retrying consumer on topic with its own group.
func consumerConfig(t testing.TB, h Harness, topic string) kafka.Config {
	t.Helper()
	return kafka.Config{
		Brokers: h.Brokers(t), Topics: []string{topic}, Group: testx.Unique("group"),
		ClientID: testx.Unique("cons"), StatisticsInterval: -1,
		RetryBackoff: 10 * time.Millisecond, RetryBackoffMax: 50 * time.Millisecond,
		DrainTimeout: 5 * time.Second, SessionTimeoutMs: 10_000,
	}
}

type running struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when Run returns
	err    error         // Run's result, valid once done is closed
	reg    *prometheus.Registry
}

// wait returns Run's result, failing the test if it does not return in time.
func (r *running) wait(t testing.TB) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not stop")
		return nil
	}
}

// start builds a consumer from cfg and runs it with h in the background;
// cleanup cancels it and waits for Run to return.
func start(t testing.TB, cfg kafka.Config, h kafka.Handler) *running {
	t.Helper()
	c, err := kafka.NewConsumer(t.Context(), cfg, nil)
	require.NoError(t, err)
	reg := prometheus.NewRegistry()
	reg.MustRegister(c.Collectors()...)
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan struct{}), reg: reg}
	go func() {
		defer close(r.done)
		r.err = c.Run(ctx, h)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(30 * time.Second):
		}
	})
	return r
}

// seen collects what a handler received, safe for concurrent use.
type seen struct {
	mu   sync.Mutex
	msgs []kafka.Message
	ctxs []context.Context
}

func (s *seen) add(ctx context.Context, m kafka.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, m)
	s.ctxs = append(s.ctxs, ctx)
}

func (s *seen) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

func (s *seen) snapshot() []kafka.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.msgs)
}

func (s *seen) offsets() map[int64]int {
	out := map[int64]int{}
	for _, m := range s.snapshot() {
		out[m.Offset]++
	}
	return out
}

// readAll reads n records from topic with a fresh group from the start.
func readAll(t testing.TB, h Harness, topic string, n int) []kafka.Message {
	t.Helper()
	var got seen
	r := start(t, consumerConfig(t, h, topic), func(ctx context.Context, m kafka.Message) error {
		got.add(ctx, m)
		return nil
	})
	require.Eventually(t, func() bool { return got.len() >= n }, 30*time.Second, 50*time.Millisecond,
		"read %d records from %s", n, topic)
	r.cancel()
	require.NoError(t, r.wait(t))
	return got.snapshot()
}

func header(m kafka.Message, key string) string {
	v, _ := m.Header(key)
	return string(v)
}

func sendN(t testing.TB, p *kafka.Producer, topic string, n int) {
	t.Helper()
	for i := range n {
		require.NoError(t, p.Send(t.Context(), kafka.Message{Topic: topic, Key: fmt.Appendf(nil, "k-%d", i), Value: fmt.Appendf(nil, "v-%d", i),
			Headers: []kafka.Header{{Key: kafka.HeaderEventID, Value: fmt.Appendf(nil, "e-%d", i)}}}))
	}
}

// ── tests ────────────────────────────────────────────────────────────────────

// One trip through the broker proves delivery, headers, both trace paths
// (the caller's span, and a traceparent header replayed by an outbox relay),
// the per-record context, the counters and the client gauges.
func produceConsume(t *testing.T, h Harness) {
	t.Helper()
	sr := testx.Recorder(t)
	topic := testx.Unique("roundtrip")
	h.CreateTopic(t, 1, topic)
	pcfg := kafka.Config{Brokers: h.Brokers(t), Topic: topic, ClientID: testx.Unique("prod"), StatisticsInterval: 200}
	prod, err := kafka.NewProducer(t.Context(), pcfg, nil)
	require.NoError(t, err)
	t.Cleanup(prod.Close)
	preg := prometheus.NewRegistry()
	preg.MustRegister(prod.Collectors()...)

	const n = 3
	pctx, parent := otel.Tracer("test").Start(t.Context(), "create-task")
	for i := range n {
		require.NoError(t, prod.Send(pctx, kafka.Message{Key: fmt.Appendf(nil, "k-%d", i), Value: fmt.Appendf(nil, "v-%d", i),
			Headers: []kafka.Header{
				{Key: kafka.HeaderEventID, Value: fmt.Appendf(nil, "evt-%d", i)},
				{Key: kafka.HeaderEventType, Value: []byte("task.created")},
			}}))
	}
	parent.End()
	parentTrace := parent.SpanContext().TraceID()

	// The outbox path: the event carries the context of the request that
	// wrote it; the relay publishes later under its own span.
	_, stored := otel.Tracer("test").Start(context.Background(), "POST /tasks (earlier)")
	stored.End()
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(trace.ContextWithSpan(context.Background(), stored), carrier)
	relayCtx, relay := otel.Tracer("test").Start(t.Context(), "outbox relay")
	require.NoError(t, prod.Send(relayCtx, kafka.Message{Key: []byte("k-outbox"), Value: []byte("v"), Headers: []kafka.Header{
		{Key: kafka.HeaderEventID, Value: []byte("evt-outbox")},
		{Key: kafka.HeaderTraceparent, Value: []byte(carrier.Get("traceparent"))},
	}}))
	relay.End()
	assert.Equal(t, float64(n+1), testx.Metric(t, preg, "kafka_producer_records_total", map[string]string{"client": "producer", "topic": topic, "result": "ok"}))
	assert.Equal(t, 1.0, testx.Metric(t, preg, "kafka_client_info", map[string]string{"client": "producer", "provider": kafka.Provider()}))
	require.Eventually(t, func() bool {
		return testx.Metric(t, preg, "kafka_producer_queue_max_bytes", nil) == float64(64<<20) &&
			testx.Metric(t, preg, "kafka_client_brokers_up", map[string]string{"client": "producer"}) >= 1
	}, 20*time.Second, 100*time.Millisecond, "the producer's client gauges report its bound and its brokers")

	var got seen
	cfg := consumerConfig(t, h, topic)
	cfg.StatisticsInterval = 200
	r := start(t, cfg, func(ctx context.Context, m kafka.Message) error {
		got.add(ctx, m)
		return nil
	})
	require.Eventually(t, func() bool { return got.len() == n+1 }, 30*time.Second, 50*time.Millisecond)

	got.mu.Lock()
	for i, m := range got.msgs {
		ctx := got.ctxs[i]
		eventID, ok := m.Header(kafka.HeaderEventID)
		require.True(t, ok)
		assert.Equal(t, string(eventID), httpx.RequestIDFromContext(ctx), "the event id is the request id")
		_, hasDeadline := ctx.Deadline()
		assert.True(t, hasDeadline, "every attempt has a deadline")
		assert.Equal(t, 1, m.Attempt)
		sc := trace.SpanContextFromContext(ctx)
		require.True(t, sc.IsValid())
		if string(m.Key) == "k-outbox" {
			assert.Equal(t, stored.SpanContext().TraceID(), sc.TraceID(), "the stored traceparent wins over the relay's span")
			continue
		}
		assert.Equal(t, fmt.Sprintf("v-%d", i), string(m.Value), "in order within the partition")
		assert.Equal(t, parentTrace, sc.TraceID(), "the consumer continues the producer's trace")
		assert.Equal(t, "task.created", header(m, kafka.HeaderEventType))
	}
	got.mu.Unlock()

	require.Eventually(t, func() bool {
		return testx.Metric(t, r.reg, "kafka_client_brokers_up", map[string]string{"client": "consumer"}) >= 1
	}, 30*time.Second, 100*time.Millisecond, "the consumer's client gauges report its brokers")
	assert.Equal(t, -1.0, testx.Metric(t, r.reg, "kafka_consumer_lag", nil), "group lag is the broker's to export")
	assert.Equal(t, 1.0, testx.Metric(t, r.reg, "kafka_consumer_assigned_partitions", nil))

	r.cancel()
	require.NoError(t, r.wait(t))
	assert.Equal(t, float64(n+1), testx.Metric(t, r.reg, "kafka_consumer_records_total", map[string]string{"topic": topic, "result": "ok"}))
	assert.Equal(t, float64(n+1), testx.Metric(t, r.reg, "kafka_consumer_handler_duration_seconds", map[string]string{"topic": topic}))
	assert.Equal(t, -1.0, testx.Metric(t, r.reg, "kafka_consumer_records_total", map[string]string{"topic": topic, "result": "failed"}), "no failure series")
	assert.Equal(t, 0.0, testx.Metric(t, r.reg, "kafka_consumer_commit_errors_total", nil))
	testx.LintMetrics(t, r.reg)
	testx.LintMetrics(t, preg)

	kinds := map[string]int{}
	for _, s := range sr.Ended() {
		if s.SpanContext().TraceID() != parentTrace {
			continue
		}
		switch {
		case strings.HasPrefix(s.Name(), "publish "):
			kinds["publish"]++
			assert.Equal(t, trace.SpanKindProducer, s.SpanKind())
		case strings.HasPrefix(s.Name(), "process "):
			kinds["process"]++
			assert.Equal(t, trace.SpanKindConsumer, s.SpanKind())
		}
	}
	assert.Equal(t, map[string]int{"publish": n, "process": n}, kinds)
}

func retriesThenSucceeds(t *testing.T, h Harness) {
	t.Helper()
	topic := testx.Unique("retry")
	h.CreateTopic(t, 1, topic)
	sendN(t, newProducer(t, h, topic), topic, 3)

	var got seen
	r := start(t, consumerConfig(t, h, topic), func(ctx context.Context, m kafka.Message) error {
		got.add(ctx, m)
		if string(m.Key) == "k-1" && m.Attempt < 3 {
			return errors.New("downstream unavailable")
		}
		return nil
	})
	require.Eventually(t, func() bool {
		return testx.Metric(t, r.reg, "kafka_consumer_records_total", map[string]string{"topic": topic, "result": "ok"}) == 2 &&
			testx.Metric(t, r.reg, "kafka_consumer_records_total", map[string]string{"topic": topic, "result": "retried"}) == 1
	}, 30*time.Second, 50*time.Millisecond)

	var attempts []int
	for _, m := range got.snapshot() {
		if string(m.Key) == "k-1" {
			attempts = append(attempts, m.Attempt)
		}
	}
	assert.Equal(t, []int{1, 2, 3}, attempts)
	assert.Equal(t, 5.0, testx.Metric(t, r.reg, "kafka_consumer_handler_duration_seconds", map[string]string{"topic": topic}), "one observation per attempt: 1 + 3 + 1")
	assert.Equal(t, -1.0, testx.Metric(t, r.reg, "kafka_producer_records_total", map[string]string{"client": "consumer"}), "nothing dead-lettered")
}

func deadLetter(t *testing.T, h Harness) {
	t.Helper()
	topic := testx.Unique("dlq")
	dlq := topic + ".dlq"
	h.CreateTopic(t, 1, topic, dlq)
	sendN(t, newProducer(t, h, topic), topic, 4)

	cfg := consumerConfig(t, h, topic)
	cfg.MaxRetries = 2
	r := start(t, cfg, func(_ context.Context, m kafka.Message) error {
		switch string(m.Key) {
		case "k-1":
			return kafka.Permanent(errors.New("undecodable payload"))
		case "k-2":
			return errors.New("still broken")
		}
		return nil
	})

	recs := readAll(t, h, dlq, 2)
	require.Len(t, recs, 2)
	byKey := map[string]kafka.Message{}
	for _, rec := range recs {
		byKey[string(rec.Key)] = rec
	}
	perm, exhausted := byKey["k-1"], byKey["k-2"]
	assert.Equal(t, "v-1", string(perm.Value), "the value is parked unchanged")
	assert.Equal(t, "e-1", header(perm, kafka.HeaderEventID), "original headers are kept")
	assert.Equal(t, "permanent", header(perm, kafka.HeaderDLQReason))
	assert.Equal(t, "undecodable payload", header(perm, kafka.HeaderDLQError))
	assert.Equal(t, topic, header(perm, kafka.HeaderDLQSourceTopic))
	assert.Equal(t, "0", header(perm, kafka.HeaderDLQSourcePartition))
	assert.Equal(t, "1", header(perm, kafka.HeaderDLQSourceOffset))
	assert.Equal(t, "1", header(perm, kafka.HeaderDLQAttempts), "a permanent error is not retried")
	assert.Equal(t, "retries_exhausted", header(exhausted, kafka.HeaderDLQReason))
	assert.Equal(t, "3", header(exhausted, kafka.HeaderDLQAttempts), "first attempt + MaxRetries")
	assert.Equal(t, "2", header(exhausted, kafka.HeaderDLQSourceOffset))

	require.Eventually(t, func() bool { return h.Committed(t, cfg.Group, topic)[0] == 4 }, 20*time.Second, 100*time.Millisecond,
		"everything is committed (offset + 1), the parked records included")
	assert.Equal(t, 2.0, testx.Metric(t, r.reg, "kafka_consumer_records_total", map[string]string{"topic": topic, "result": "dead_lettered"}))
	assert.Equal(t, 2.0, testx.Metric(t, r.reg, "kafka_consumer_records_total", map[string]string{"topic": topic, "result": "ok"}))
	assert.Equal(t, 2.0, testx.Metric(t, r.reg, "kafka_producer_records_total", map[string]string{"client": "consumer", "topic": dlq, "result": "ok"}),
		"dead-letter publishes are counted as sends of the consumer's producer")

	select {
	case <-r.done:
		t.Fatalf("the consumer keeps running past parked records, but Run returned %v", r.err)
	default:
	}
}

func dlqDisabled(t *testing.T, h Harness) {
	t.Helper()
	topic := testx.Unique("poison")
	h.CreateTopic(t, 1, topic)
	sendN(t, newProducer(t, h, topic), topic, 3)

	cfg := consumerConfig(t, h, topic)
	cfg.DeadLetterTopic = kafka.DeadLetterDisabled
	cfg.MaxRetries = 1
	var got seen
	r := start(t, cfg, func(ctx context.Context, m kafka.Message) error {
		got.add(ctx, m)
		if string(m.Key) == "k-1" {
			return errors.New("poison")
		}
		return nil
	})

	err := r.wait(t)
	require.ErrorContains(t, err, "dead-letter topic is disabled")
	require.ErrorContains(t, err, "poison")
	for _, m := range got.snapshot() {
		assert.NotEqual(t, "k-2", string(m.Key), "nothing past the poison record is processed")
	}
	assert.Equal(t, int64(1), h.Committed(t, cfg.Group, topic)[0], "the record before the poison is committed, the poison is not")
	assert.Equal(t, 1.0, testx.Metric(t, r.reg, "kafka_consumer_records_total", map[string]string{"topic": topic, "result": "failed"}))
}

// A shutdown mid-batch commits exactly what was processed: a restarted
// member of the same group resumes after it, with no redelivery.
func gracefulShutdown(t *testing.T, h Harness) {
	t.Helper()
	t.Run("drain deadline cuts the batch", func(t *testing.T) {
		topic := testx.Unique("drain-cut")
		h.CreateTopic(t, 1, topic)
		sendN(t, newProducer(t, h, topic), topic, 10)

		cfg := consumerConfig(t, h, topic)
		cfg.DrainTimeout = 300 * time.Millisecond
		var first seen
		reached := make(chan struct{})
		r := start(t, cfg, func(ctx context.Context, m kafka.Message) error {
			if m.Offset == 5 {
				close(reached)
				<-ctx.Done() // a handler that only stops when told to
				return ctx.Err()
			}
			first.add(ctx, m)
			return nil
		})
		<-reached
		r.cancel()
		require.NoError(t, r.wait(t), "running out of drain budget is still a clean shutdown")
		assert.Equal(t, map[int64]int{0: 1, 1: 1, 2: 1, 3: 1, 4: 1}, first.offsets())
		assert.Equal(t, int64(5), h.Committed(t, cfg.Group, topic)[0])

		var second seen
		start(t, cfg, func(ctx context.Context, m kafka.Message) error { second.add(ctx, m); return nil })
		require.Eventually(t, func() bool { return second.len() >= 5 }, 30*time.Second, 50*time.Millisecond)
		assert.Equal(t, map[int64]int{5: 1, 6: 1, 7: 1, 8: 1, 9: 1}, second.offsets(), "no redelivery of processed records")
	})

	t.Run("drain finishes the batch", func(t *testing.T) {
		topic := testx.Unique("drain-finish")
		h.CreateTopic(t, 1, topic)
		sendN(t, newProducer(t, h, topic), topic, 10)

		cfg := consumerConfig(t, h, topic)
		var first seen
		reached := make(chan struct{})
		release := make(chan struct{})
		r := start(t, cfg, func(ctx context.Context, m kafka.Message) error {
			if m.Offset == 2 {
				close(reached)
				<-release // shutdown arrives while this record is in flight
			}
			first.add(ctx, m)
			return nil
		})
		<-reached
		r.cancel()
		close(release)
		require.NoError(t, r.wait(t))
		done := first.offsets()
		require.Contains(t, done, int64(2), "the in-flight record finished")
		last := slices.Max(slices.Collect(maps.Keys(done)))
		assert.Len(t, done, int(last)+1, "a contiguous prefix was processed")
		assert.Equal(t, last+1, h.Committed(t, cfg.Group, topic)[0], "and all of it committed")

		if last < 9 {
			var second seen
			start(t, cfg, func(ctx context.Context, m kafka.Message) error { second.add(ctx, m); return nil })
			require.Eventually(t, func() bool { return second.len() >= int(9-last) }, 30*time.Second, 50*time.Millisecond)
			for off := range second.offsets() {
				assert.Greater(t, off, last, "no redelivery of processed records")
			}
		}
	})
}

// Two members of one group share the partitions without losing a record,
// with bounded duplicates across the rebalances (a member joining, then one
// leaving mid-stream), each reporting its own client gauges.
func rebalance(t *testing.T, h Harness) {
	t.Helper()
	topic := testx.Unique("rebalance")
	h.CreateTopic(t, 4, topic)
	prod := newProducer(t, h, topic)

	var got seen
	handler := func(ctx context.Context, m kafka.Message) error {
		got.add(ctx, m)
		time.Sleep(2 * time.Millisecond)
		return nil
	}
	cfg := consumerConfig(t, h, topic)
	cfg.MaxPollRecords = 10
	cfg.StatisticsInterval = 200
	cfgB := cfg
	cfgB.ClientID = testx.Unique("cons-b")

	sendN(t, prod, topic, 100)
	a := start(t, cfg, handler)
	require.Eventually(t, func() bool { return got.len() >= 100 }, 30*time.Second, 50*time.Millisecond)

	// B joins while records keep arriving.
	publishing := make(chan struct{})
	go func() {
		defer close(publishing)
		for i := 100; i < 200; i++ {
			if err := prod.Send(context.Background(), kafka.Message{Topic: topic, Key: fmt.Appendf(nil, "k-%d", i), Value: fmt.Appendf(nil, "v-%d", i)}); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	b := start(t, cfgB, handler)
	assigned := func(r *running) float64 {
		return testx.Metric(t, r.reg, "kafka_consumer_assigned_partitions", nil)
	}
	require.Eventually(t, func() bool { return assigned(a) > 0 && assigned(b) > 0 && assigned(a)+assigned(b) == 4 },
		60*time.Second, 50*time.Millisecond, "the group settles with both members")
	<-publishing
	require.Eventually(t, func() bool {
		up := map[string]string{"client": "consumer"}
		return testx.Metric(t, a.reg, "kafka_client_brokers_up", up) >= 1 && testx.Metric(t, b.reg, "kafka_client_brokers_up", up) >= 1
	}, 30*time.Second, 100*time.Millisecond, "both members report their client gauges")

	// A leaves mid-stream; B takes everything over.
	publishing = make(chan struct{})
	go func() {
		defer close(publishing)
		for i := 200; i < 300; i++ {
			if err := prod.Send(context.Background(), kafka.Message{Topic: topic, Key: fmt.Appendf(nil, "k-%d", i), Value: fmt.Appendf(nil, "v-%d", i)}); err != nil {
				return
			}
		}
	}()
	require.Eventually(t, func() bool { return got.len() >= 220 }, 30*time.Second, 20*time.Millisecond)
	a.cancel()
	require.NoError(t, a.wait(t))
	require.Eventually(t, func() bool { return assigned(b) == 4 }, 60*time.Second, 50*time.Millisecond)
	<-publishing

	keys := func() map[string]int {
		out := map[string]int{}
		for _, m := range got.snapshot() {
			out[string(m.Key)]++
		}
		return out
	}
	require.Eventually(t, func() bool { return len(keys()) == 300 }, 30*time.Second, 50*time.Millisecond, "no record is lost")
	dups := got.len() - 300
	t.Logf("deliveries=%d unique=300 duplicates=%d", got.len(), dups)
	assert.LessOrEqual(t, dups, 2*cfg.MaxPollRecords, "duplicates are bounded by the in-flight batch")
	assert.Positive(t, testx.Metric(t, b.reg, "kafka_consumer_rebalances_total", map[string]string{"event": "assigned"}))
}

func stopsOnCancel(t *testing.T, h Harness) {
	t.Helper()
	topic := testx.Unique("idle")
	h.CreateTopic(t, 1, topic)
	r := start(t, consumerConfig(t, h, topic), func(context.Context, kafka.Message) error { return nil })
	require.Eventually(t, func() bool {
		return testx.Metric(t, r.reg, "kafka_consumer_assigned_partitions", nil) == 1
	}, 30*time.Second, 50*time.Millisecond, "joined the group")
	r.cancel()
	require.NoError(t, r.wait(t), "a cancelled context is a clean shutdown, not an error")
}

// A missing topic is the deployment's problem (retryable); a record over
// KAFKA_MESSAGE_MAX_BYTES is the record's (rejected for good).
func errorTaxonomy(t *testing.T, h Harness) {
	t.Helper()
	brokers := h.Brokers(t)
	missing := testx.Unique("missing")
	p, err := kafka.NewProducer(t.Context(), kafka.Config{Brokers: brokers, ClientID: testx.Unique("prod"), PublishTimeout: 10 * time.Second}, nil)
	require.NoError(t, err)
	defer p.Close()
	reg := prometheus.NewRegistry()
	reg.MustRegister(p.Collectors()...)

	err = p.Send(t.Context(), kafka.Message{Topic: missing, Value: []byte("x")})
	require.Error(t, err)
	var se *kafka.SendError
	require.ErrorAs(t, err, &se)
	assert.Contains(t, []string{kafka.KindNotProvisioned, kafka.KindUnavailable}, se.Kind,
		"a topic that is not there fails as the path's problem: %v", err)
	assert.False(t, kafka.Rejected(err))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "kafka_producer_records_total", map[string]string{"topic": missing, "result": se.Kind}))

	auto, err := kafka.NewProducer(t.Context(), kafka.Config{Brokers: brokers, ClientID: testx.Unique("prod"), AllowAutoTopicCreation: true}, nil)
	require.NoError(t, err)
	defer auto.Close()
	require.NoError(t, auto.Send(t.Context(), kafka.Message{Topic: missing, Value: []byte("x")}), "the broker creates it when asked")

	err = auto.Send(t.Context(), kafka.Message{Topic: missing, Value: make([]byte, 2<<20)})
	require.Error(t, err)
	assert.True(t, kafka.Rejected(err), "a record over KAFKA_MESSAGE_MAX_BYTES is rejected for good: %v", err)
}

// SASLSSL produces and consumes one record through a SASL_SSL listener
// with a custom CA — the examples/kafka-sasl-ssl broker. It runs when
// KAFKA_SASL_SSL_BROKERS is set (`just kafka-sasl-ssl-up` prints the
// variables), and skips otherwise.
//
//	KAFKA_SASL_SSL_BROKERS        localhost:29094
//	KAFKA_SASL_SSL_CA_LOCATION    examples/kafka-sasl-ssl/certs/ca.crt
//	KAFKA_SASL_SSL_USERNAME       app
//	KAFKA_SASL_SSL_PASSWORD_FILE  examples/kafka-sasl-ssl/certs/app.password
//	KAFKA_SASL_SSL_TOPIC          a topic the user may write and read (default "tasks.events")
func SASLSSL(t *testing.T) {
	t.Helper()
	brokers := os.Getenv("KAFKA_SASL_SSL_BROKERS")
	if brokers == "" {
		t.Skip("KAFKA_SASL_SSL_BROKERS not set: run `just kafka-sasl-ssl-up` (examples/kafka-sasl-ssl)")
	}
	topic := os.Getenv("KAFKA_SASL_SSL_TOPIC")
	if topic == "" {
		topic = "tasks.events"
	}
	cfg := kafka.Config{
		Brokers:          strings.Split(brokers, ","),
		Topic:            topic,
		Group:            testx.Unique("sasl-ssl"),
		ClientID:         testx.Unique("sasl-ssl"),
		SecurityProtocol: "sasl_ssl",
		SASLMechanism:    "SCRAM-SHA-512",
		SASLUsername:     os.Getenv("KAFKA_SASL_SSL_USERNAME"),
		SASLPasswordFile: os.Getenv("KAFKA_SASL_SSL_PASSWORD_FILE"),
		SSLCALocation:    os.Getenv("KAFKA_SASL_SSL_CA_LOCATION"),
		AutoOffsetReset:  "latest",
		// The example broker creates topics on demand (the ACLs allow it).
		AllowAutoTopicCreation: true,
		RetryBackoff:           10 * time.Millisecond, RetryBackoffMax: 50 * time.Millisecond,
	}
	p, err := kafka.NewProducer(t.Context(), cfg, nil)
	require.NoError(t, err, "connect over SASL_SSL with the custom CA")
	defer p.Close()

	want := testx.Unique("sasl-ssl-record")
	got := make(chan string, 16)
	c, err := kafka.NewConsumer(t.Context(), cfg, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, func(_ context.Context, m kafka.Message) error {
			got <- string(m.Value)
			return nil
		})
	}()
	defer func() { cancel(); <-done }()

	// The consumer starts at the end: keep sending until it has joined and
	// sees one.
	deadline := time.After(60 * time.Second)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		require.NoError(t, p.Send(t.Context(), kafka.Message{Value: []byte(want)}))
		select {
		case v := <-got:
			if v == want {
				return
			}
		case <-tick.C:
		case <-deadline:
			t.Fatal("no record came back through the SASL_SSL listener")
		}
	}
}
