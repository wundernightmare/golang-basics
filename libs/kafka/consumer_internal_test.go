package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The provider-neutral loop against a scripted client: the paths a real
// broker rarely takes (lost partitions, a dead-letter topic that refuses,
// constructors failing) — kafkatest covers the rest against both providers.

// fake is the linked provider of this test binary; each test sets what its
// next client does.
var fake = &fakeDriver{}

func TestMain(m *testing.M) {
	Register(fake)
	os.Exit(m.Run())
}

type fakeDriver struct {
	mu       sync.Mutex
	consumer *fakeConsumer
	producer *fakeProducer
	newErr   error
}

func (*fakeDriver) Name() string            { return "fake" }
func (*fakeDriver) Validate(Config) []error { return nil }

func (d *fakeDriver) NewProducer(Config, *slog.Logger) (ProducerClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.newErr != nil {
		return nil, d.newErr
	}
	return d.producer, nil
}

func (d *fakeDriver) NewConsumer(_ Config, _ *slog.Logger, rb Rebalance) (ConsumerClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.newErr != nil {
		return nil, d.newErr
	}
	d.consumer.rb = rb
	return d.consumer, nil
}

// use installs the clients the next constructor returns.
func use(t *testing.T, c *fakeConsumer, p *fakeProducer, newErr error) {
	t.Helper()
	fake.mu.Lock()
	fake.consumer, fake.producer, fake.newErr = c, p, newErr
	fake.mu.Unlock()
}

type fakeProducer struct {
	mu      sync.Mutex
	sendErr error
	pingErr error
	sent    []Message
	closed  bool
}

func (p *fakeProducer) Send(_ context.Context, m Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sendErr != nil {
		return p.sendErr
	}
	p.sent = append(p.sent, m)
	return nil
}
func (p *fakeProducer) Ping(context.Context) error { return p.pingErr }
func (*fakeProducer) Stats() ClientStats           { return ClientStats{} }
func (p *fakeProducer) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

// fakeConsumer hands out its batches once each, then blocks until ctx ends.
type fakeConsumer struct {
	rb      Rebalance
	batches [][]Message
	dlq     *fakeProducer
	pingErr error

	mu        sync.Mutex
	commits   [][]Offset
	commitErr error // every Commit fails with it while set
	pollErr   error // every Poll returns it while set
	closed    bool
}

func (c *fakeConsumer) Poll(ctx context.Context, _ int) ([]Message, error) {
	c.mu.Lock()
	if c.pollErr != nil {
		err := c.pollErr
		c.mu.Unlock()
		return nil, err
	}
	if len(c.batches) > 0 {
		b := c.batches[0]
		c.batches = c.batches[1:]
		c.mu.Unlock()
		return b, nil
	}
	c.mu.Unlock()
	<-ctx.Done()
	return nil, nil
}

func (c *fakeConsumer) Commit(_ context.Context, offsets []Offset) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.commitErr != nil {
		return c.commitErr
	}
	c.commits = append(c.commits, offsets)
	return nil
}

func (c *fakeConsumer) failCommits(err error)      { c.mu.Lock(); c.commitErr = err; c.mu.Unlock() }
func (*fakeConsumer) AllowRebalance()              {}
func (c *fakeConsumer) DeadLetter() ProducerClient { return c.dlq }
func (c *fakeConsumer) Ping(context.Context) error { return c.pingErr }
func (*fakeConsumer) Stats() ClientStats           { return ClientStats{} }
func (c *fakeConsumer) Close()                     { c.mu.Lock(); c.closed = true; c.mu.Unlock() }
func (c *fakeConsumer) committed() [][]Offset      { c.mu.Lock(); defer c.mu.Unlock(); return c.commits }

func testConfig() Config {
	return Config{Brokers: []string{"fake:9092"}, Topics: []string{"t"}, Group: "g",
		MaxRetries: 1, RetryBackoff: time.Millisecond, RetryBackoffMax: time.Millisecond,
		HandlerTimeout: time.Second, PublishTimeout: time.Second, DrainTimeout: time.Second}
}

func TestNewConsumer_Errors(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()
	cfg.Brokers = nil
	_, err := NewConsumer(ctx, cfg, nil)
	require.ErrorContains(t, err, "KAFKA_BROKERS must be set")

	use(t, nil, nil, errors.New("boom"))
	_, err = NewConsumer(ctx, testConfig(), nil)
	require.ErrorContains(t, err, "new fake consumer: boom")

	fc := &fakeConsumer{pingErr: errors.New("no route")}
	use(t, fc, nil, nil)
	_, err = NewConsumer(ctx, testConfig(), nil)
	require.ErrorContains(t, err, "brokers unreachable")
	assert.True(t, fc.closed, "a consumer that cannot reach the brokers is closed")
}

func TestNewProducer_Errors(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()
	cfg.CompressionType = "brotli"
	_, err := NewProducer(ctx, cfg, nil)
	require.ErrorContains(t, err, "unknown codec")

	use(t, nil, nil, errors.New("boom"))
	_, err = NewProducer(ctx, testConfig(), nil)
	require.ErrorContains(t, err, "new fake producer: boom")

	fp := &fakeProducer{pingErr: errors.New("no route")}
	use(t, nil, fp, nil)
	_, err = NewProducer(ctx, testConfig(), nil)
	require.ErrorContains(t, err, "brokers unreachable")
	assert.True(t, fp.closed)
}

// A lost partition's processed-but-uncommitted offsets are dropped, not
// committed: the partition already belongs to someone else.
func TestConsumer_LostDropsPendingRevokedCommitsOnlyItsOwn(t *testing.T) {
	fc := &fakeConsumer{}
	use(t, fc, nil, nil)
	c, err := NewConsumer(context.Background(), testConfig(), nil)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	p0, p1, p2 := TopicPartition{"t", 0}, TopicPartition{"t", 1}, TopicPartition{"t", 2}
	fc.rb.Assigned(ctx, []TopicPartition{p0, p1, p2})
	c.markDone(Message{Topic: "t", Partition: 0, Offset: 4})
	c.markDone(Message{Topic: "t", Partition: 1, Offset: 7})
	c.markDone(Message{Topic: "t", Partition: 2, Offset: 9})

	fc.rb.Lost(ctx, []TopicPartition{p0})
	fc.rb.Revoked(ctx, []TopicPartition{p1})

	require.Len(t, fc.committed(), 1)
	assert.Equal(t, []Offset{{TopicPartition: p1, Next: 8}}, fc.committed()[0], "only the revoked partition, at last processed + 1")
	assert.False(t, c.owns(Message{Topic: "t", Partition: 0}))
	assert.False(t, c.owns(Message{Topic: "t", Partition: 1}))
	assert.True(t, c.owns(Message{Topic: "t", Partition: 2}))
	assert.Equal(t, map[TopicPartition]int64{p2: 9}, c.pending)
}

// A revoke whose commit fails still forgets the revoked partitions' offsets:
// the next commit must never include a partition another member now reads —
// the broker would accept it and move that member's position back.
func TestConsumer_FailedRevokeCommitIsNeverRetriedForAMovedPartition(t *testing.T) {
	fc := &fakeConsumer{}
	use(t, fc, nil, nil)
	c, err := NewConsumer(context.Background(), testConfig(), nil)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	p0, p1 := TopicPartition{"t", 0}, TopicPartition{"t", 1}
	fc.rb.Assigned(ctx, []TopicPartition{p0, p1})
	c.markDone(Message{Topic: "t", Partition: 0, Offset: 4})
	c.markDone(Message{Topic: "t", Partition: 1, Offset: 7})

	fc.failCommits(errors.New("coordinator not available"))
	fc.rb.Revoked(ctx, []TopicPartition{p0})
	assert.Empty(t, fc.committed(), "the revoke's commit failed")

	fc.failCommits(nil)
	c.commitPending(ctx)
	require.Len(t, fc.committed(), 1)
	assert.Equal(t, []Offset{{TopicPartition: p1, Next: 8}}, fc.committed()[0], "p0 moved: its offset is not committed later")
	assert.Empty(t, c.pending)
}

// A record finished after its partition was revoked (or lost) is not
// committed: the partition is someone else's now.
func TestConsumer_NeverCommitsAPartitionItDoesNotOwn(t *testing.T) {
	fc := &fakeConsumer{}
	use(t, fc, nil, nil)
	c, err := NewConsumer(context.Background(), testConfig(), nil)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	p0, p1 := TopicPartition{"t", 0}, TopicPartition{"t", 1}
	fc.rb.Assigned(ctx, []TopicPartition{p0, p1})
	fc.rb.Revoked(ctx, []TopicPartition{p0})
	c.markDone(Message{Topic: "t", Partition: 0, Offset: 4}) // in flight across the revoke
	c.markDone(Message{Topic: "t", Partition: 1, Offset: 7})

	c.commitPending(ctx)
	require.Len(t, fc.committed(), 1)
	assert.Equal(t, []Offset{{TopicPartition: p1, Next: 8}}, fc.committed()[0])
	assert.Empty(t, c.pending, "the stray offset is dropped, not kept for a later commit")
}

// A client that failed for good ends Run with its error; a fetch error that
// the client retries does not.
func TestConsumer_RunStopsWhenTheClientFails(t *testing.T) {
	fc := &fakeConsumer{pollErr: fmt.Errorf("%w: FENCED_INSTANCE_ID", ErrClientFailed)}
	use(t, fc, nil, nil)
	c, err := NewConsumer(context.Background(), testConfig(), nil)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background(), func(context.Context, Message) error { return nil }) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrClientFailed)
		require.ErrorContains(t, err, "FENCED_INSTANCE_ID")
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept polling a failed client")
	}
}

// A record that fails and cannot be dead-lettered stops the loop with both
// errors and stays uncommitted, after the dead-letter publish was retried.
func TestConsumer_DeadLetterRefusedStopsTheLoop(t *testing.T) {
	dlq := &fakeProducer{sendErr: NewSendError(KindUnavailable, 0, errors.New("dlq down"))}
	fc := &fakeConsumer{dlq: dlq, batches: [][]Message{{{Topic: "t", Partition: 0, Offset: 3, Value: []byte("x")}}}}
	use(t, fc, nil, nil)
	c, err := NewConsumer(context.Background(), testConfig(), nil)
	require.NoError(t, err)
	fc.rb.Assigned(context.Background(), []TopicPartition{{"t", 0}})

	err = c.Run(context.Background(), func(context.Context, Message) error { return errors.New("bad record") })
	require.Error(t, err)
	require.ErrorContains(t, err, `could not be dead-lettered to "t.dlq"`)
	require.ErrorContains(t, err, "bad record")
	require.ErrorContains(t, err, "dlq down")
	assert.Empty(t, fc.committed(), "the record stays for redelivery")
}

// A dead-lettered record carries the dlq_* headers (replacing stale ones
// from an earlier park) and a bounded error text, and is committed.
func TestConsumer_DeadLetterHeaders(t *testing.T) {
	dlq := &fakeProducer{}
	long := make([]byte, 2*maxDLQErrorBytes)
	for i := range long {
		long[i] = 'e'
	}
	rec := Message{Topic: "t", Partition: 2, Offset: 11, Key: []byte("k"), Value: []byte("v"),
		Headers: []Header{{Key: "keep", Value: []byte("1")}, {Key: HeaderDLQReason, Value: []byte("stale")}}}
	fc := &fakeConsumer{dlq: dlq, batches: [][]Message{{rec}}}
	use(t, fc, nil, nil)
	cfg := testConfig()
	cfg.MaxRetries = -1 // no retries: straight to the DLQ
	c, err := NewConsumer(context.Background(), cfg, nil)
	require.NoError(t, err)
	fc.rb.Assigned(context.Background(), []TopicPartition{{"t", 2}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, func(context.Context, Message) error { return Permanent(errors.New(string(long))) })
	}()
	require.Eventually(t, func() bool { return len(fc.committed()) == 1 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)

	require.Len(t, dlq.sent, 1)
	m := dlq.sent[0]
	assert.Equal(t, "t.dlq", m.Topic)
	reason, _ := m.Header(HeaderDLQReason)
	assert.Equal(t, "permanent", string(reason))
	errText, _ := m.Header(HeaderDLQError)
	assert.Len(t, errText, maxDLQErrorBytes)
	keep, _ := m.Header("keep")
	assert.Equal(t, "1", string(keep))
	n := 0
	for _, h := range m.Headers {
		if h.Key == HeaderDLQReason {
			n++
		}
	}
	assert.Equal(t, 1, n, "the stale reason header is replaced, not duplicated")
	assert.Equal(t, []Offset{{TopicPartition: TopicPartition{"t", 2}, Next: 12}}, fc.committed()[0])
}

// With the DLQ disabled a failing record stops the loop instead.
func TestConsumer_DeadLetterDisabled(t *testing.T) {
	fc := &fakeConsumer{batches: [][]Message{{{Topic: "t", Partition: 0, Offset: 0}}}}
	use(t, fc, nil, nil)
	cfg := testConfig()
	cfg.DeadLetterTopic = DeadLetterDisabled
	c, err := NewConsumer(context.Background(), cfg, nil)
	require.NoError(t, err)
	fc.rb.Assigned(context.Background(), []TopicPartition{{"t", 0}})

	err = c.Run(context.Background(), func(context.Context, Message) error { return errors.New("bad") })
	require.ErrorContains(t, err, "dead-letter topic is disabled")
	assert.Empty(t, fc.committed())
}

// Every rejection Validate makes, one field at a time.
func TestConfig_ValidateRejects(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"KAFKA_BROKERS: empty broker":        func(c *Config) { c.Brokers = []string{" "} },
		"KAFKA_CONSUMER_AUTO_OFFSET_RESET":   func(c *Config) { c.AutoOffsetReset = "middle" },
		"KAFKA_CONSUMER_ASSIGNMENT_STRATEGY": func(c *Config) { c.AssignmentStrategy = "sticky" },
		"must be positive":                   func(c *Config) { c.SessionTimeoutMs = -1 },
		"is below KAFKA_CONSUMER_SESSION":    func(c *Config) { c.SessionTimeoutMs, c.MaxPollIntervalMs = 60000, 30000 },
		"KAFKA_PRODUCER_LINGER_MS":           func(c *Config) { c.LingerMs = -1 },
		"KAFKA_DIAL_TIMEOUT must be":         func(c *Config) { c.DialTimeout = -time.Second },
		"is below KAFKA_CONSUMER_RETRY":      func(c *Config) { c.RetryBackoff, c.RetryBackoffMax = time.Second, time.Millisecond },
		"KAFKA_CONSUMER_MAX_POLL_RECORDS":    func(c *Config) { c.MaxPollRecords = -1 },
		"surrounding spaces":                 func(c *Config) { c.DeadLetterTopic = " {topic}.dlq" },
	} {
		cfg := testConfig()
		mutate(&cfg)
		assert.ErrorContains(t, cfg.Validate(), name, name)
	}
}

func TestProperties_UnmarshalText(t *testing.T) {
	var p Properties
	require.NoError(t, p.UnmarshalText([]byte(`{"debug":"broker","batch.size":262144,"check.crcs":true}`)))
	assert.Equal(t, Properties{"debug": "broker", "batch.size": "262144", "check.crcs": "true"}, p)
	require.NoError(t, p.UnmarshalText([]byte("  ")))
	assert.Nil(t, p, "empty = unset")
	require.ErrorContains(t, p.UnmarshalText([]byte(`{"a":[1]}`)), `property "a"`)
	require.ErrorContains(t, p.UnmarshalText([]byte(`[]`)), "JSON object")
}

// Send wraps a client error that is not a *SendError, and the producer warns
// about SASL PLAIN over plaintext once at boot.
func TestProducer_WrapsForeignErrorsAndWarnsPlainSASL(t *testing.T) {
	fp := &fakeProducer{sendErr: errors.New("socket closed")}
	use(t, nil, fp, nil)
	cfg := testConfig()
	cfg.SecurityProtocol, cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword = "sasl_plaintext", "PLAIN", "u", "p"
	var logs safeBuffer
	p, err := NewProducer(context.Background(), cfg, slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)
	defer p.Close()
	assert.Contains(t, logs.String(), "clear text")

	err = p.Send(context.Background(), Message{Value: []byte("x")})
	var se *SendError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, KindUnknown, se.Kind)
	require.ErrorContains(t, err, "socket closed")
}

type safeBuffer struct {
	mu sync.Mutex
	b  []byte
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b = append(s.b, p...)
	return len(p), nil
}

func (s *safeBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return string(s.b) }

// One provider per binary: a second registration is a link error, said loudly.
func TestRegister_SecondProviderPanics(t *testing.T) {
	assert.Equal(t, "fake", Provider())
	assert.PanicsWithValue(t,
		`kafka: provider "fake" registered while "fake" already is: link exactly one of libs/kafka-rdkafka, libs/kafka-franz`,
		func() { Register(&fakeDriver{}) })
}
