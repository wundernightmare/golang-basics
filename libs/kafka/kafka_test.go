package kafka_test

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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// One Kafka (KRaft) per test binary; every test owns its topics and consumer
// groups by name (testx.Unique).
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

// ── helpers ──────────────────────────────────────────────────────────────────

func createTopic(t testing.TB, partitions int32, names ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(containers.Kafka(t)...))
	require.NoError(t, err)
	defer cl.Close()
	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopics(t.Context(), partitions, 1, nil, names...)
	require.NoError(t, err)
	require.NoError(t, resp.Error())
	// Created is not yet led: the metadata names a leader before the leader
	// serves the partition (NOT_LEADER_FOR_PARTITION, which franz-go then
	// retries only after its 5s metadata min-age). Wait until every
	// partition answers a ListOffsets, which the leader itself serves.
	require.Eventually(t, func() bool {
		offs, err := adm.ListEndOffsets(t.Context(), names...)
		if err != nil {
			return false
		}
		n := 0
		ok := true
		offs.Each(func(o kadm.ListedOffset) {
			n++
			ok = ok && o.Err == nil
		})
		return ok && n == len(names)*int(partitions)
	}, 30*time.Second, 50*time.Millisecond, "topics %v get leaders", names)
}

func newProducer(t testing.TB, topic string) *kafka.Producer {
	t.Helper()
	p, err := kafka.NewProducer(t.Context(), kafka.Config{
		Brokers: containers.Kafka(t), Topic: topic, ClientID: testx.Unique("prod"),
	}, nil)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

// consumerConfig is a fast-retrying consumer on topic with its own group.
func consumerConfig(t testing.TB, topic string) kafka.Config {
	t.Helper()
	return kafka.Config{
		Brokers: containers.Kafka(t), Topics: []string{topic}, Group: testx.Unique("group"),
		ClientID: testx.Unique("cons"), LagInterval: -1,
		RetryBackoff: 10 * time.Millisecond, RetryBackoffMax: 50 * time.Millisecond,
		DrainTimeout: 5 * time.Second,
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

// committed returns the group's committed offsets for topic, by partition.
func committed(t testing.TB, group, topic string) map[int32]int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(containers.Kafka(t)...))
	require.NoError(t, err)
	defer cl.Close()
	resp, err := kadm.NewClient(cl).FetchOffsets(t.Context(), group)
	require.NoError(t, err)
	out := map[int32]int64{}
	resp.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.Err == nil {
			out[o.Partition] = o.At
		}
	})
	return out
}

// readAll reads n records from topic (all partitions, from the start) with a
// group-less client.
func readAll(t testing.TB, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(containers.Kafka(t)...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "timed out with %d/%d records", len(out), n)
		out = append(out, fs.Records()...)
	}
	return out
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func publishN(t testing.TB, p *kafka.Producer, topic string, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		require.NoError(t, p.Publish(t.Context(), topic, fmt.Appendf(nil, "k-%d", i), fmt.Appendf(nil, "v-%d", i),
			kafka.Header{Key: kafka.HeaderEventID, Value: fmt.Appendf(nil, "e-%d", i)}))
	}
}

// ── tests ────────────────────────────────────────────────────────────────────

// One trip through the broker proves delivery, headers, both trace paths
// (the caller's span, and a traceparent header replayed by an outbox relay),
// the per-record context, and the counters describing exactly what happened.
func TestProduceConsume_HeadersTraceAndContext(t *testing.T) {
	sr := testx.Recorder(t) // before the clients: kotel captures the provider then
	topic := testx.Unique("roundtrip")
	createTopic(t, 1, topic)
	prod := newProducer(t, topic)
	preg := prometheus.NewRegistry()
	preg.MustRegister(prod.Collectors()...)

	const n = 3
	pctx, parent := otel.Tracer("test").Start(t.Context(), "create-task")
	for i := range n {
		require.NoError(t, prod.Publish(pctx, topic, fmt.Appendf(nil, "k-%d", i), fmt.Appendf(nil, "v-%d", i),
			kafka.Header{Key: kafka.HeaderEventID, Value: fmt.Appendf(nil, "evt-%d", i)},
			kafka.Header{Key: kafka.HeaderEventType, Value: []byte("task.created")}))
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
	require.NoError(t, prod.Publish(relayCtx, "", []byte("k-outbox"), []byte("v"),
		kafka.Header{Key: kafka.HeaderEventID, Value: []byte("evt-outbox")},
		kafka.Header{Key: kafka.HeaderTraceparent, Value: []byte(carrier.Get("traceparent"))}))
	relay.End()
	assert.Equal(t, float64(n+1), testx.Metric(t, preg, "kafka_publish_total", map[string]string{"topic": topic, "result": "ok"}))

	var got seen
	cfg := consumerConfig(t, topic)
	cfg.LagInterval = 200 * time.Millisecond
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
		et, _ := m.Header(kafka.HeaderEventType)
		assert.Equal(t, "task.created", string(et))
	}
	got.mu.Unlock()

	require.Eventually(t, func() bool {
		return testx.Metric(t, r.reg, "kafka_consumer_group_lag", map[string]string{"topic": topic, "partition": "0"}) == 0
	}, 20*time.Second, 100*time.Millisecond, "lag drops to zero once the records are committed")
	assert.Positive(t, testx.Metric(t, r.reg, "kafka_consumer_lag_last_success_timestamp_seconds", nil))
	assert.Equal(t, 1.0, testx.Metric(t, r.reg, "kafka_consumer_assigned_partitions", map[string]string{"topic": topic}))

	r.cancel()
	require.NoError(t, r.wait(t))
	assert.Equal(t, float64(n+1), testx.Metric(t, r.reg, "kafka_consume_total", map[string]string{"topic": topic, "result": "ok"}))
	assert.Equal(t, float64(n+1), testx.Metric(t, r.reg, "kafka_consumer_handler_duration_seconds", map[string]string{"topic": topic}))
	assert.Equal(t, -1.0, testx.Metric(t, r.reg, "kafka_consume_total", map[string]string{"topic": topic, "result": "failed"}), "no failure series")
	assert.Equal(t, 0.0, testx.Metric(t, r.reg, "kafka_consumer_commit_errors_total", nil))
	testx.LintMetrics(t, r.reg)
	testx.LintMetrics(t, preg)

	kinds := map[string]int{}
	for _, s := range sr.Ended() {
		if s.SpanContext().TraceID() != parentTrace {
			continue
		}
		switch {
		case strings.HasSuffix(s.Name(), " publish"):
			kinds["publish"]++
			assert.Equal(t, trace.SpanKindProducer, s.SpanKind())
		case strings.HasSuffix(s.Name(), " receive"):
			kinds["receive"]++
		case strings.HasSuffix(s.Name(), " process"):
			kinds["process"]++
			assert.Equal(t, trace.SpanKindConsumer, s.SpanKind())
		}
	}
	assert.Equal(t, map[string]int{"publish": n, "receive": n, "process": n}, kinds)
}

func TestConsumer_RetriesThenSucceeds(t *testing.T) {
	topic := testx.Unique("retry")
	createTopic(t, 1, topic)
	prod := newProducer(t, topic)
	publishN(t, prod, topic, 0, 3)

	var got seen
	r := start(t, consumerConfig(t, topic), func(ctx context.Context, m kafka.Message) error {
		got.add(ctx, m)
		if string(m.Key) == "k-1" && m.Attempt < 3 {
			return errors.New("downstream unavailable")
		}
		return nil
	})
	require.Eventually(t, func() bool {
		return testx.Metric(t, r.reg, "kafka_consume_total", map[string]string{"topic": topic, "result": "ok"}) == 2 &&
			testx.Metric(t, r.reg, "kafka_consume_total", map[string]string{"topic": topic, "result": "retried"}) == 1
	}, 30*time.Second, 50*time.Millisecond)

	var attempts []int
	for _, m := range got.snapshot() {
		if string(m.Key) == "k-1" {
			attempts = append(attempts, m.Attempt)
		}
	}
	assert.Equal(t, []int{1, 2, 3}, attempts)
	assert.Equal(t, 5.0, testx.Metric(t, r.reg, "kafka_consumer_handler_duration_seconds", map[string]string{"topic": topic}), "one observation per attempt: 1 + 3 + 1")
	assert.Equal(t, -1.0, testx.Metric(t, r.reg, "kafka_consumer_dlq_total", nil))
}

func TestConsumer_ExhaustedAndPermanentGoToDLQ(t *testing.T) {
	topic := testx.Unique("dlq")
	dlq := topic + ".dlq"
	createTopic(t, 1, topic, dlq)
	prod := newProducer(t, topic)
	publishN(t, prod, topic, 0, 4)

	cfg := consumerConfig(t, topic)
	cfg.MaxRetries = 2
	var got seen
	r := start(t, cfg, func(ctx context.Context, m kafka.Message) error {
		got.add(ctx, m)
		switch string(m.Key) {
		case "k-1":
			return kafka.Permanent(errors.New("undecodable payload"))
		case "k-2":
			return errors.New("still broken")
		}
		return nil
	})

	recs := readAll(t, dlq, 2)
	require.Len(t, recs, 2)
	byKey := map[string]*kgo.Record{}
	for _, rec := range recs {
		byKey[string(rec.Key)] = rec
	}
	perm, exhausted := byKey["k-1"], byKey["k-2"]
	require.NotNil(t, perm)
	require.NotNil(t, exhausted)
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

	require.Eventually(t, func() bool { return committed(t, cfg.Group, topic)[0] == 4 }, 20*time.Second, 100*time.Millisecond,
		"everything is committed, the parked records included")
	assert.Equal(t, 2.0, testx.Metric(t, r.reg, "kafka_consume_total", map[string]string{"topic": topic, "result": "dead_lettered"}))
	assert.Equal(t, 2.0, testx.Metric(t, r.reg, "kafka_consume_total", map[string]string{"topic": topic, "result": "ok"}))
	assert.Equal(t, 2.0, testx.Metric(t, r.reg, "kafka_consumer_dlq_total", map[string]string{"topic": dlq}))

	select {
	case <-r.done:
		t.Fatalf("the consumer keeps running past parked records, but Run returned %v", r.err)
	default:
	}
}

func TestConsumer_DLQDisabledStopsOnPoison(t *testing.T) {
	topic := testx.Unique("poison")
	createTopic(t, 1, topic)
	prod := newProducer(t, topic)
	publishN(t, prod, topic, 0, 3)

	cfg := consumerConfig(t, topic)
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
	assert.Equal(t, int64(1), committed(t, cfg.Group, topic)[0], "the record before the poison is committed, the poison is not")
	assert.Equal(t, 1.0, testx.Metric(t, r.reg, "kafka_consume_total", map[string]string{"topic": topic, "result": "failed"}))
}

// A shutdown mid-batch commits exactly what was processed: a restarted
// member of the same group resumes after it, with no redelivery.
func TestConsumer_GracefulShutdownCommitsProcessed(t *testing.T) {
	t.Run("drain deadline cuts the batch", func(t *testing.T) {
		topic := testx.Unique("drain-cut")
		createTopic(t, 1, topic)
		publishN(t, newProducer(t, topic), topic, 0, 10)

		cfg := consumerConfig(t, topic)
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
		assert.Equal(t, int64(5), committed(t, cfg.Group, topic)[0])

		var second seen
		start(t, cfg, func(ctx context.Context, m kafka.Message) error { second.add(ctx, m); return nil })
		require.Eventually(t, func() bool { return second.len() >= 5 }, 30*time.Second, 50*time.Millisecond)
		assert.Equal(t, map[int64]int{5: 1, 6: 1, 7: 1, 8: 1, 9: 1}, second.offsets(), "no redelivery of processed records")
	})

	t.Run("drain finishes the batch", func(t *testing.T) {
		topic := testx.Unique("drain-finish")
		createTopic(t, 1, topic)
		publishN(t, newProducer(t, topic), topic, 0, 10)

		cfg := consumerConfig(t, topic)
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
		assert.Equal(t, last+1, committed(t, cfg.Group, topic)[0], "and all of it committed")

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
// leaving mid-stream), and each reports lag only for what it owns.
func TestConsumer_RebalanceNoLossBoundedDuplicates(t *testing.T) {
	topic := testx.Unique("rebalance")
	createTopic(t, 4, topic)
	prod := newProducer(t, topic)

	var got seen
	handler := func(ctx context.Context, m kafka.Message) error {
		got.add(ctx, m)
		time.Sleep(2 * time.Millisecond)
		return nil
	}
	cfg := consumerConfig(t, topic)
	cfg.MaxPollRecords = 10
	cfg.LagInterval = 200 * time.Millisecond
	cfgB := cfg
	cfgB.ClientID = testx.Unique("cons-b")

	publishN(t, prod, topic, 0, 100)
	a := start(t, cfg, handler)
	require.Eventually(t, func() bool { return got.len() >= 100 }, 30*time.Second, 50*time.Millisecond)

	// B joins while records keep arriving.
	publishing := make(chan struct{})
	go func() {
		defer close(publishing)
		for i := 100; i < 200; i++ {
			if err := prod.Publish(context.Background(), topic, fmt.Appendf(nil, "k-%d", i), fmt.Appendf(nil, "v-%d", i)); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	b := start(t, cfgB, handler)
	assigned := func(r *running) float64 {
		return testx.Metric(t, r.reg, "kafka_consumer_assigned_partitions", map[string]string{"topic": topic})
	}
	require.Eventually(t, func() bool { return assigned(a) > 0 && assigned(b) > 0 && assigned(a)+assigned(b) == 4 },
		30*time.Second, 50*time.Millisecond, "the group settles with both members")
	<-publishing

	// Lag: each member reports its own partitions, together all of them once.
	lagPartitions := func(r *running) map[string]bool {
		out := map[string]bool{}
		mfs, err := r.reg.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() != "kafka_consumer_group_lag" {
				continue
			}
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "partition" {
						out[l.GetValue()] = true
					}
				}
			}
		}
		return out
	}
	require.Eventually(t, func() bool {
		pa, pb := lagPartitions(a), lagPartitions(b)
		return len(pa) == int(assigned(a)) && len(pb) == int(assigned(b)) && len(pa)+len(pb) == 4
	}, 20*time.Second, 100*time.Millisecond)
	pa, pb := lagPartitions(a), lagPartitions(b)
	for p := range pa {
		assert.False(t, pb[p], "partition %s reported by both members", p)
	}

	// A leaves mid-stream; B takes everything over.
	publishing = make(chan struct{})
	go func() {
		defer close(publishing)
		for i := 200; i < 300; i++ {
			if err := prod.Publish(context.Background(), topic, fmt.Appendf(nil, "k-%d", i), fmt.Appendf(nil, "v-%d", i)); err != nil {
				return
			}
		}
	}()
	require.Eventually(t, func() bool { return got.len() >= 220 }, 30*time.Second, 20*time.Millisecond)
	a.cancel()
	require.NoError(t, a.wait(t))
	require.Eventually(t, func() bool { return assigned(b) == 4 }, 30*time.Second, 50*time.Millisecond)
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

func TestConsumer_StopsOnContextCancel(t *testing.T) {
	topic := testx.Unique("idle")
	createTopic(t, 1, topic)
	r := start(t, consumerConfig(t, topic), func(context.Context, kafka.Message) error { return nil })
	require.Eventually(t, func() bool {
		return testx.Metric(t, r.reg, "kafka_consumer_assigned_partitions", map[string]string{"topic": topic}) == 1
	}, 30*time.Second, 50*time.Millisecond, "joined the group")
	r.cancel()
	require.NoError(t, r.wait(t), "a cancelled context is a clean shutdown, not an error")
}

func TestProducer_TopicAutoCreationIsOptIn(t *testing.T) {
	brokers := containers.Kafka(t)
	missing := testx.Unique("missing")
	p, err := kafka.NewProducer(t.Context(), kafka.Config{Brokers: brokers, ClientID: testx.Unique("prod"), PublishTimeout: 15 * time.Second}, nil)
	require.NoError(t, err)
	defer p.Close()
	reg := prometheus.NewRegistry()
	reg.MustRegister(p.Collectors()...)

	err = p.Publish(t.Context(), missing, nil, []byte("x"))
	require.Error(t, err)
	assert.Equal(t, 1.0, testx.Metric(t, reg, "kafka_publish_total", map[string]string{"topic": missing, "result": "unknown_topic"}))

	auto, err := kafka.NewProducer(t.Context(), kafka.Config{Brokers: brokers, ClientID: testx.Unique("prod"), AllowAutoTopicCreation: true}, nil)
	require.NoError(t, err)
	defer auto.Close()
	require.NoError(t, auto.Publish(t.Context(), missing, nil, []byte("x")), "the broker creates it when asked")
}
