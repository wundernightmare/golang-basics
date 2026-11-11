package pipeline_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/pipeline"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/testsupport"
)

type fixture struct {
	pipe           *pipeline.Pipeline
	store          *testsupport.Store
	pub            *testsupport.Publisher
	kafkaBreaker   *resilience.Breaker
	archiveBreaker *resilience.Breaker
	reg            *prometheus.Registry
	stop           func()
}

type options struct {
	noKafka bool
	cfg     func(*pipeline.Config)
	archive func(*archive.WriterConfig)
}

func breaker(t *testing.T) *resilience.Breaker {
	t.Helper()

	brk := resilience.NewBreaker(resilience.BreakerConfig{
		FailureRatio: 0.5, MinRequests: 2, Window: time.Minute, OpenTimeout: 50 * time.Millisecond, HalfOpenProbes: 1,
	}, time.Now, nil)

	return brk
}

func start(t *testing.T, opts options) *fixture {
	t.Helper()

	fix := &fixture{
		store: testsupport.NewStore(), pub: &testsupport.Publisher{},
		kafkaBreaker: breaker(t), archiveBreaker: breaker(t),
		reg: prometheus.NewRegistry(),
	}

	// Kafka answers in normal operation, so the archive lingers "forever":
	// a test that sees an archive answer knows it was asked to hurry.
	writerCfg := archive.WriterConfig{
		BatchBytes: 1 << 20, Linger: time.Hour, UrgentLinger: 20 * time.Millisecond,
		MaxConcurrentFlushes: 2, Compression: shard.LevelFastest,
	}
	if opts.archive != nil {
		opts.archive(&writerCfg)
	}

	writer, err := archive.NewWriter(writerCfg, fix.store, archive.NewLayout(""), "w", fix.archiveBreaker, nil)
	require.NoError(t, err)

	cfg := pipeline.Config{Topic: "events", MaxInFlightBytes: 1 << 20, KafkaAckGrace: time.Hour, KafkaMaxInFlight: 100}
	if opts.cfg != nil {
		opts.cfg(&cfg)
	}

	var pub pipeline.Publisher
	if !opts.noKafka {
		pub = fix.pub
	}

	fix.pipe, err = pipeline.New(cfg, pub, writer, fix.kafkaBreaker, nil)
	require.NoError(t, err)
	fix.reg.MustRegister(fix.pipe.Collectors()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = writer.Run(ctx)
	}()

	var once sync.Once

	fix.stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(fix.stop)

	return fix
}

func events(ids ...string) []pipeline.Event {
	out := make([]pipeline.Event, len(ids))
	for i, id := range ids {
		out[i] = pipeline.Event{ID: id, Type: "t.est", Raw: []byte(`{"id":"` + id + `","type":"t.est","payload":{}}`)}
	}

	return out
}

func (fix *fixture) outcome(t *testing.T, name string) float64 {
	t.Helper()
	return testx.Metric(t, fix.reg, "ingest_batches_total", map[string]string{"outcome": name})
}

func (fix *fixture) inFlight(t *testing.T) float64 {
	t.Helper()
	return testx.Metric(t, fix.reg, "ingest_in_flight_bytes", nil)
}

// settled waits until both sinks have let go of every accepted byte.
func (fix *fixture) settled(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool { return fix.inFlight(t) == 0 },
		5*time.Second, 5*time.Millisecond, "every accepted byte is given back once both sinks settle")
}

func unavailable() error { return kafka.NewSendError(kafka.KindUnavailable, 0, testsupport.ErrDown) }

func TestIngest_KafkaAnswersAndTheArchiveStillGetsItsCopy(t *testing.T) {
	t.Parallel()

	fix := start(t, options{})

	started := time.Now()

	require.NoError(t, fix.pipe.Ingest(context.Background(), events("a", "b")))
	assert.Less(t, time.Since(started), time.Second, "the archive's hour-long linger was not waited on")
	assert.InDelta(t, 1.0, fix.outcome(t, "kafka"), 0)

	sent := fix.pub.Sent()
	require.Len(t, sent, 2)

	byID := map[string]kafka.Message{}
	for _, msg := range sent {
		byID[string(msg.Key)] = msg
	}

	msg := byID["a"]
	assert.Equal(t, "events", msg.Topic)
	assert.JSONEq(t, `{"id":"a","type":"t.est","payload":{}}`, string(msg.Value), "the value is the event as sent")

	id, _ := msg.Header(kafka.HeaderEventID)
	assert.Equal(t, "a", string(id))

	eventType, _ := msg.Header(kafka.HeaderEventType)
	assert.Equal(t, "t.est", string(eventType))

	assert.Positive(t, fix.inFlight(t), "the archive copy is still in memory")

	fix.stop() // shutdown flushes the lingering batch
	fix.settled(t)

	ids, err := fix.store.IDs()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, ids, "the second copy was written after the answer")
}

func TestIngest_KafkaFailureFallsBackToTheArchive(t *testing.T) {
	t.Parallel()

	fix := start(t, options{})
	fix.pub.FailWith(unavailable())

	require.NoError(t, fix.pipe.Ingest(context.Background(), events("a")), "the archive was expedited when Kafka failed")
	assert.InDelta(t, 1.0, fix.outcome(t, "archive"), 0)
	fix.settled(t)

	ids, err := fix.store.IDs()
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, ids)
}

func TestIngest_OpenKafkaCircuitSkipsKafkaAndClosesAgain(t *testing.T) {
	t.Parallel()

	fix := start(t, options{})
	fix.pub.FailWith(unavailable())

	for _, id := range []string{"a", "b"} {
		require.NoError(t, fix.pipe.Ingest(context.Background(), events(id)))
	}

	require.Eventually(t, func() bool { return fix.kafkaBreaker.State() == resilience.StateOpen }, time.Second, time.Millisecond)

	release := fix.pub.Block() // a send now would hang: an open circuit must not try
	require.NoError(t, fix.pipe.Ingest(context.Background(), events("c", "d")))
	release()
	assert.InDelta(t, 2.0, testx.Metric(t, fix.reg, "ingest_kafka_skipped_events_total", map[string]string{"reason": "circuit_open"}), 0)
	assert.InDelta(t, 3.0, fix.outcome(t, "archive"), 0)

	// Kafka is back: after the open timeout one request probes it.
	fix.pub.FailWith(nil)
	time.Sleep(60 * time.Millisecond)
	require.NoError(t, fix.pipe.Ingest(context.Background(), events("e")))
	require.Eventually(t, func() bool { return fix.kafkaBreaker.State() == resilience.StateClosed }, time.Second, time.Millisecond)
	fix.stop() // "e" was answered by Kafka: its archive copy lingers until shutdown
	fix.settled(t)
}

func TestIngest_SlowKafkaIsNotWaitedOnPastTheGrace(t *testing.T) {
	t.Parallel()

	fix := start(t, options{cfg: func(c *pipeline.Config) { c.KafkaAckGrace = 30 * time.Millisecond }})
	release := fix.pub.Block()

	started := time.Now()

	require.NoError(t, fix.pipe.Ingest(context.Background(), events("a")))
	assert.Less(t, time.Since(started), 2*time.Second)
	assert.InDelta(t, 1.0, fix.outcome(t, "archive"), 0, "slow is not failed: the archive answered meanwhile")
	assert.Equal(t, resilience.StateClosed, fix.kafkaBreaker.State())

	release()
	fix.settled(t)
	assert.Len(t, fix.pub.Sent(), 1, "Kafka still got its copy after the answer")
}

func TestIngest_NoKafkaConfigured(t *testing.T) {
	t.Parallel()

	fix := start(t, options{noKafka: true})
	require.NoError(t, fix.pipe.Ingest(context.Background(), events("a")))
	assert.InDelta(t, 1.0, fix.outcome(t, "archive"), 0)
	fix.settled(t)

	fix.store.Fail(true)
	require.ErrorIs(t, fix.pipe.Ingest(context.Background(), events("b")), pipeline.ErrNoSink)
	fix.settled(t)
}

func TestIngest_ArchiveDownKafkaAnswers(t *testing.T) {
	t.Parallel()

	fix := start(t, options{archive: func(c *archive.WriterConfig) { c.Linger = 20 * time.Millisecond }})
	fix.store.Fail(true)

	for _, id := range []string{"a", "b"} {
		require.NoError(t, fix.pipe.Ingest(context.Background(), events(id)))
		fix.settled(t)
	}

	require.Eventually(t, func() bool { return fix.archiveBreaker.State() == resilience.StateOpen }, time.Second, time.Millisecond)

	require.NoError(t, fix.pipe.Ingest(context.Background(), events("c")), "an open archive circuit does not stop Kafka from answering")
	assert.InDelta(t, 3.0, fix.outcome(t, "kafka"), 0)
	fix.settled(t)
}

func TestIngest_BothSinksDown(t *testing.T) {
	t.Parallel()

	fix := start(t, options{})
	fix.store.Fail(true)
	fix.pub.FailWith(unavailable())

	ready := fix.pipe.ReadyCheck(func() bool { return fix.archiveBreaker.State() == resilience.StateOpen })
	require.NoError(t, ready(context.Background()))

	for range 2 {
		err := fix.pipe.Ingest(context.Background(), events("a"))
		require.ErrorIs(t, err, pipeline.ErrNoSink)
		require.ErrorIs(t, err, testsupport.ErrDown, "the causes are kept for the log")
	}

	fix.settled(t)

	// Both circuits open now: refused at once, nothing held, not ready.
	puts := fix.store.Puts()
	started := time.Now()

	require.ErrorIs(t, fix.pipe.Ingest(context.Background(), events("b")), pipeline.ErrNoSink)
	assert.Less(t, time.Since(started), 15*time.Millisecond, "open circuits fail fast")
	assert.Equal(t, puts, fix.store.Puts())
	assert.InDelta(t, 3.0, fix.outcome(t, "no_sink"), 0)
	require.Error(t, ready(context.Background()))
	fix.settled(t)
}

func TestIngest_RejectedRecordsDoNotOpenTheCircuit(t *testing.T) {
	t.Parallel()

	fix := start(t, options{})
	fix.pub.FailWith(kafka.NewSendError(kafka.KindRejected, 0, errors.New("message too large")))

	for _, id := range []string{"a", "b", "c", "d"} {
		require.NoError(t, fix.pipe.Ingest(context.Background(), events(id)), "the archive takes what Kafka refuses")
	}

	fix.settled(t)
	assert.Equal(t, resilience.StateClosed, fix.kafkaBreaker.State(), "a refused record says nothing about the brokers")
}

func TestIngest_OverloadedPastTheInFlightBound(t *testing.T) {
	t.Parallel()

	one := int64(len(events("a")[0].Raw))
	fix := start(t, options{cfg: func(c *pipeline.Config) { c.MaxInFlightBytes = one + one/2 }})

	releaseKafka := fix.pub.Block()
	defer releaseKafka()

	releaseStore := fix.store.Block()
	defer releaseStore()

	held := make(chan error, 1)

	go func() { held <- fix.pipe.Ingest(context.Background(), events("a")) }()

	require.Eventually(t, func() bool { return fix.inFlight(t) > 0 }, time.Second, time.Millisecond)

	require.ErrorIs(t, fix.pipe.Ingest(context.Background(), events("b")), pipeline.ErrOverloaded)
	assert.InDelta(t, 1.0, fix.outcome(t, "overloaded"), 0)
	assert.InDelta(t, float64(one+one/2), testx.Metric(t, fix.reg, "ingest_in_flight_max_bytes", nil), 0)

	releaseKafka()
	require.NoError(t, <-held)
	releaseStore()
	fix.stop()
	fix.settled(t)
}

func TestIngest_KafkaBulkheadFull(t *testing.T) {
	t.Parallel()

	fix := start(t, options{cfg: func(c *pipeline.Config) {
		c.KafkaMaxInFlight = 2
		c.KafkaAckGrace = 10 * time.Millisecond
	}})
	release := fix.pub.Block()

	// Kafka hangs; past the grace the archive answers.
	require.NoError(t, fix.pipe.Ingest(context.Background(), events("a", "b")))

	// The two slots are held by a and b; c cannot be sent and goes urgent.
	require.NoError(t, fix.pipe.Ingest(context.Background(), events("c")))
	assert.InDelta(t, 1.0, testx.Metric(t, fix.reg, "ingest_kafka_skipped_events_total", map[string]string{"reason": "busy"}), 0)

	release()
	fix.settled(t)
	assert.Len(t, fix.pub.Sent(), 2)
}

func TestIngest_CallerGivesUp(t *testing.T) {
	t.Parallel()

	fix := start(t, options{})
	release := fix.pub.Block()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, fix.pipe.Ingest(ctx, events("a")), context.DeadlineExceeded)
	assert.InDelta(t, 1.0, fix.outcome(t, "canceled"), 0)

	// The sinks were not told to stop: the event is stored all the same,
	// which is why a retry must be safe.
	release()
	fix.stop()
	fix.settled(t)
	assert.Len(t, fix.pub.Sent(), 1)

	ids, err := fix.store.IDs()
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, ids)
}

func TestDrain_WaitsForSendsWithinItsBudget(t *testing.T) {
	t.Parallel()

	fix := start(t, options{cfg: func(c *pipeline.Config) { c.KafkaAckGrace = 10 * time.Millisecond }})
	release := fix.pub.Block()

	require.NoError(t, fix.pipe.Ingest(context.Background(), events("a")))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	assert.Positive(t, fix.pipe.Drain(ctx), "a hung send is reported, not waited on forever")

	release()
	assert.Zero(t, fix.pipe.Drain(context.Background()))
}

func TestNew_RejectsBadConfig(t *testing.T) {
	t.Parallel()

	good := pipeline.Config{MaxInFlightBytes: 1, KafkaAckGrace: time.Second, KafkaMaxInFlight: 1}

	_, err := pipeline.New(good, &testsupport.Publisher{}, nil, nil, nil)
	require.ErrorContains(t, err, "needs a breaker")

	for _, mutate := range []func(*pipeline.Config){
		func(c *pipeline.Config) { c.MaxInFlightBytes = 0 },
		func(c *pipeline.Config) { c.KafkaAckGrace = 0 },
		func(c *pipeline.Config) { c.KafkaMaxInFlight = 0 },
	} {
		cfg := good
		mutate(&cfg)

		_, err := pipeline.New(cfg, nil, nil, nil, nil)
		require.Error(t, err)
	}
}

func TestMetricsAreLintClean(t *testing.T) {
	t.Parallel()

	fix := start(t, options{noKafka: true})
	require.NoError(t, fix.pipe.Ingest(context.Background(), events("a")))
	testx.LintMetrics(t, fix.reg)
}
