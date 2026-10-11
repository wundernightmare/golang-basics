// Package rdkafka is the librdkafka provider of libs/kafka, through
// confluent-kafka-go — the same engine as nodejs-basics'
// @confluentinc/kafka-javascript. It needs cgo; the bundled librdkafka is
// linked statically (the image builds a fully static musl binary, so the
// runtime stays distroless/static). Import it for its side effect to link it:
//
//	import _ "github.com/tracehubmmp/golang-basics/libs/kafka-rdkafka"
//
// Every libs/kafka key is written as a librdkafka property (config.go);
// any other property can be passed through KAFKA_*_EXTRA_PROPERTIES.
// Client metrics come from librdkafka's statistics (statistics.interval.ms),
// its log lines go to slog. See libs/kafka's README for when to pick it over
// libs/kafka-franz.
package rdkafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

func init() { kafka.Register(Driver{}) }

// Driver is the librdkafka [kafka.Driver].
type Driver struct{}

// Name implements kafka.Driver.
func (Driver) Name() string { return "rdkafka" }

// Validate implements kafka.Driver.
func (Driver) Validate(cfg kafka.Config) []error { return validate(cfg) }

// queueFullRetry is how often Send retries enqueueing while the local send
// queue is full.
const queueFullRetry = 20 * time.Millisecond

// flushOnClose bounds how long Close waits for queued records.
const flushOnClose = 5 * time.Second

// sendErrorKinds maps librdkafka codes onto the libs/kafka kinds (the same
// codes as Node's KAFKA_SEND_ERRORS).
var sendErrorKinds = map[ckafka.ErrorCode]string{
	ckafka.ErrQueueFull: kafka.KindQueueFull,

	ckafka.ErrMsgTimedOut:                  kafka.KindUnavailable,
	ckafka.ErrTimedOut:                     kafka.KindUnavailable,
	ckafka.ErrRequestTimedOut:              kafka.KindUnavailable,
	ckafka.ErrTransport:                    kafka.KindUnavailable,
	ckafka.ErrAllBrokersDown:               kafka.KindUnavailable,
	ckafka.ErrLeaderNotAvailable:           kafka.KindUnavailable,
	ckafka.ErrNotLeaderForPartition:        kafka.KindUnavailable,
	ckafka.ErrNetworkException:             kafka.KindUnavailable,
	ckafka.ErrNotEnoughReplicas:            kafka.KindUnavailable,
	ckafka.ErrNotEnoughReplicasAfterAppend: kafka.KindUnavailable,
	ckafka.ErrKafkaStorageError:            kafka.KindUnavailable,
	ckafka.ErrThrottlingQuotaExceeded:      kafka.KindUnavailable,

	ckafka.ErrUnknownTopicOrPart:         kafka.KindNotProvisioned,
	ckafka.ErrUnknownTopic:               kafka.KindNotProvisioned,
	ckafka.ErrUnknownPartition:           kafka.KindNotProvisioned,
	ckafka.ErrTopicAuthorizationFailed:   kafka.KindNotProvisioned,
	ckafka.ErrClusterAuthorizationFailed: kafka.KindNotProvisioned,
	ckafka.ErrAuthentication:             kafka.KindNotProvisioned,
	ckafka.ErrSaslAuthenticationFailed:   kafka.KindNotProvisioned,

	ckafka.ErrFatal: kafka.KindFatal,

	ckafka.ErrMsgSizeTooLarge:    kafka.KindRejected,
	ckafka.ErrRecordListTooLarge: kafka.KindRejected,
	ckafka.ErrInvalidMsg:         kafka.KindRejected,
	ckafka.ErrInvalidRecord:      kafka.KindRejected,
	ckafka.ErrPolicyViolation:    kafka.KindRejected,
	ckafka.ErrBadMsg:             kafka.KindRejected,
	ckafka.ErrTopicException:     kafka.KindRejected,
}

// sendError maps a produce error onto the registry. A fatal error wins over
// its code (the client is gone, whatever the cause).
func sendError(err error) error {
	if err == nil {
		return nil
	}
	kind, code := kafka.KindUnknown, 0
	var kerr ckafka.Error
	if errors.As(err, &kerr) {
		code = int(kerr.Code())
		if k, ok := sendErrorKinds[kerr.Code()]; ok {
			kind = k
		}
		if kerr.IsFatal() {
			kind = kafka.KindFatal
		}
	}
	return kafka.NewSendError(kind, code, err)
}

// NewProducer implements kafka.Driver.
func (Driver) NewProducer(cfg kafka.Config, log *slog.Logger) (kafka.ProducerClient, error) {
	return newProducer(cfg, log, "producer")
}

func newProducer(cfg kafka.Config, log *slog.Logger, role string) (*producer, error) {
	props, err := producerProperties(cfg)
	if err != nil {
		return nil, err
	}
	p := &producer{cfg: cfg, props: props, log: log, role: role, stats: &statsHolder{}}
	cl, err := p.newClient()
	if err != nil {
		return nil, err
	}
	p.cl = cl
	return p, nil
}

// producer is a librdkafka producer. A fatal error (lost idempotence state)
// replaces the client underneath; librdkafka reconnects on its own
// otherwise.
//
// Every call into the client holds mu for reading and finds the client in
// p.cl under it; replacing or closing the client takes mu for writing. So a
// client is never closed (rd_kafka_destroy) while a call is still inside it,
// and nothing is created after Close.
type producer struct {
	cfg   kafka.Config
	props ckafka.ConfigMap
	log   *slog.Logger
	role  string
	stats *statsHolder

	mu     sync.RWMutex
	cl     *ckafka.Producer
	closed bool
	wg     sync.WaitGroup // event and log pumps, replacements under way
}

// testHookNewClient, when set (tests), sees every client the producer
// creates.
var testHookNewClient func(*ckafka.Producer)

// newClient creates a librdkafka producer and starts draining its events
// (statistics, errors) and logs. It runs before the producer is shared or
// with mu held for writing and closed false — never after Close began.
func (p *producer) newClient() (*ckafka.Producer, error) {
	props := ckafka.ConfigMap{}
	for k, v := range p.props {
		props[k] = v
	}
	cl, err := ckafka.NewProducer(&props)
	if err != nil {
		return nil, err
	}
	if testHookNewClient != nil {
		testHookNewClient(cl)
	}
	p.wg.Add(2)
	go func() { defer p.wg.Done(); pumpLogs(cl.Logs(), p.log, p.role) }()
	go func() { defer p.wg.Done(); p.pumpEvents(cl) }()
	return cl, nil
}

// pumpEvents drains one client's events channel until the client closes.
// Delivery reports go to each Send's own channel; what arrives here is
// statistics and client-level errors.
func (p *producer) pumpEvents(cl *ckafka.Producer) {
	for ev := range cl.Events() {
		switch e := ev.(type) {
		case *ckafka.Stats:
			p.stats.update(e.String())
		case ckafka.Error:
			if e.IsFatal() {
				p.log.Error("kafka producer failed fatally; replacing the client", "kafka_client", p.role, "err", e)
				p.startReplace(cl)
			}
			// Non-fatal errors are librdkafka's to retry; its log line
			// (folded) already says what happened.
		}
	}
}

// startReplace replaces failed in the background, unless the producer is
// closing. The replacement is counted in wg, so Close waits for it.
func (p *producer) startReplace(failed *ckafka.Producer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.wg.Add(1)
	go p.replace(failed)
}

// replace swaps a fatally failed client for a new one, then closes the old
// one. Records queued on the old one fail with a retryable error. A second
// fatal error of the same client finds it already replaced and does
// nothing; a replacement that lost the race with Close does nothing either.
func (p *producer) replace(failed *ckafka.Producer) {
	defer p.wg.Done()
	p.mu.Lock()
	if p.closed || p.cl != failed {
		p.mu.Unlock()
		return
	}
	cl, err := p.newClient()
	if err != nil {
		p.mu.Unlock()
		p.log.Error("kafka producer could not be replaced; readiness stays failed", "err", err)
		return
	}
	p.cl = cl
	p.mu.Unlock()
	failed.Close() // no call is inside it: each one took the read lock and found the new client
}

// errProducerClosed is a Send or Ping after Close.
var errProducerClosed = ckafka.NewError(ckafka.ErrState, "producer closed", false)

// Send implements kafka.ProducerClient: enqueue (waiting out a full local
// queue within ctx), then await the delivery report. librdkafka fails the
// record itself at message.timeout.ms (KAFKA_PUBLISH_TIMEOUT).
func (p *producer) Send(ctx context.Context, msg kafka.Message) error {
	topic := msg.Topic
	hs := make([]ckafka.Header, len(msg.Headers))
	for i, h := range msg.Headers {
		hs[i] = ckafka.Header{Key: h.Key, Value: h.Value}
	}
	m := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Key:            msg.Key, Value: msg.Value, Headers: hs,
	}
	delivery := make(chan ckafka.Event, 1)
	var err error
	for {
		err = p.produce(m, delivery)
		var kerr ckafka.Error
		if !errors.As(err, &kerr) || kerr.Code() != ckafka.ErrQueueFull {
			break
		}
		t := time.NewTimer(queueFullRetry) // back-pressure: wait for room within the budget
		select {
		case <-t.C:
			continue
		case <-ctx.Done():
			t.Stop()
		}
		break
	}
	if err != nil {
		return sendError(err)
	}
	select {
	case ev := <-delivery:
		if r, ok := ev.(*ckafka.Message); ok {
			return sendError(r.TopicPartition.Error)
		}
		return nil
	case <-ctx.Done():
		return sendError(ctx.Err())
	}
}

// produce enqueues m on the current client, under the read lock.
func (p *producer) produce(m *ckafka.Message, delivery chan ckafka.Event) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errProducerClosed
	}
	return p.cl.Produce(m, delivery)
}

// Ping implements kafka.ProducerClient.
func (p *producer) Ping(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errProducerClosed
	}
	return ping(ctx, p.cl)
}

// Stats implements kafka.ProducerClient.
func (p *producer) Stats() kafka.ClientStats { return p.stats.clientStats(p.cfg) }

// Close implements kafka.ProducerClient: no call is inside the client once
// it holds the write lock, and none starts after; then it flushes what is
// queued, closes the client and waits for its pumps and for a replacement
// that was under way.
func (p *producer) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	cl := p.cl
	p.mu.Unlock()
	if left := cl.Flush(int(flushOnClose.Milliseconds())); left > 0 {
		p.log.Warn("kafka producer closed with records still queued; they were not delivered", "kafka_client", p.role, "records", left)
	}
	cl.Close()
	p.wg.Wait()
}

// NewConsumer implements kafka.Driver.
func (Driver) NewConsumer(cfg kafka.Config, log *slog.Logger, rb kafka.Rebalance) (kafka.ConsumerClient, error) {
	props, err := consumerProperties(cfg)
	if err != nil {
		return nil, err
	}
	cl, err := ckafka.NewConsumer(&props)
	if err != nil {
		return nil, err
	}
	c := &consumer{cl: cl, cfg: cfg, log: log, rb: rb, stats: &statsHolder{}}
	c.logsDone = make(chan struct{})
	go func() { defer close(c.logsDone); pumpLogs(cl.Logs(), log, "consumer") }()
	if err := cl.SubscribeTopics(cfg.Topics, c.rebalance); err != nil {
		_ = cl.Close()
		<-c.logsDone
		return nil, err
	}
	return c, nil
}

// consumer is a librdkafka group member. Group changes are only delivered
// from within Poll or Close (the rebalance callback), so the loop's batch is
// never interrupted by one; what was polled for a partition revoked during
// the same Poll is dropped.
type consumer struct {
	cl    *ckafka.Consumer
	cfg   kafka.Config
	log   *slog.Logger
	rb    kafka.Rebalance
	stats *statsHolder

	revoked  map[kafka.TopicPartition]struct{} // during the current Poll
	dlqOnce  sync.Once
	dlq      *producer
	dlqErr   error
	logsDone chan struct{}
	failed   atomic.Pointer[error] // the fatal error that ended the client

	// Calls into the client from outside the loop (Ping from readiness,
	// Commit, whose wait ctx may end before librdkafka returns) are counted
	// in calls, so Close destroys the client only once none is inside it.
	mu      sync.Mutex
	closing bool // Close has begun: no new Ping; a Commit runs to the end
	closed  bool // the client is gone (or abandoned): nothing may call it
	calls   sync.WaitGroup
}

// closeWait bounds how long Close waits for a call still inside librdkafka
// (a commit waiting on an unreachable coordinator) before giving up on the
// client: destroying it under a running call would crash the process, so
// past this the handle is left to the exiting process instead.
var closeWait = 30 * time.Second // a variable for tests

func toPartitions(tps []ckafka.TopicPartition) []kafka.TopicPartition {
	out := make([]kafka.TopicPartition, 0, len(tps))
	for _, tp := range tps {
		if tp.Topic != nil {
			out = append(out, kafka.TopicPartition{Topic: *tp.Topic, Partition: tp.Partition})
		}
	}
	return out
}

// rebalance is the librdkafka rebalance callback. It returns without
// assigning: confluent-kafka-go then (incrementally, for
// cooperative-sticky) assigns or unassigns itself.
func (c *consumer) rebalance(cl *ckafka.Consumer, ev ckafka.Event) error {
	ctx := context.Background()
	switch e := ev.(type) {
	case ckafka.AssignedPartitions:
		c.rb.Assigned(ctx, toPartitions(e.Partitions))
	case ckafka.RevokedPartitions:
		parts := toPartitions(e.Partitions)
		if cl.AssignmentLost() {
			c.rb.Lost(ctx, parts)
		} else {
			c.rb.Revoked(ctx, parts)
		}
		if c.revoked != nil {
			for _, p := range parts {
				c.revoked[p] = struct{}{}
			}
		}
	}
	return nil
}

// Poll implements kafka.ConsumerClient: wait (in short slices, to notice
// ctx) for a first record, then take whatever is already queued up to max.
func (c *consumer) Poll(ctx context.Context, max int) ([]kafka.Message, error) {
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	if closing {
		return nil, kafka.ErrClientClosed
	}
	c.revoked = map[kafka.TopicPartition]struct{}{}
	defer func() { c.revoked = nil }()
	var out []kafka.Message
	var errs []error
	timeoutMs := 100
	for len(out) < max && ctx.Err() == nil {
		ev := c.cl.Poll(timeoutMs)
		if ev == nil {
			if len(out) > 0 {
				break
			}
			continue
		}
		switch e := ev.(type) {
		case *ckafka.Message:
			if e.TopicPartition.Error != nil {
				errs = append(errs, e.TopicPartition.Error)
				continue
			}
			out = append(out, toMessage(e))
			timeoutMs = 0
		case *ckafka.Stats:
			c.stats.update(e.String())
		case ckafka.Error:
			if e.IsFatal() {
				// The client is finished (e.g. FENCED_INSTANCE_ID): what
				// was polled is left for the group's next owner.
				err := fmt.Errorf("%w: %w", kafka.ErrClientFailed, e)
				c.failed.Store(&err)
				return nil, err
			}
			errs = append(errs, e)
		}
		if len(c.revoked) > 0 {
			kept := out[:0]
			for _, m := range out {
				if _, gone := c.revoked[kafka.TopicPartition{Topic: m.Topic, Partition: m.Partition}]; !gone {
					kept = append(kept, m)
				}
			}
			out = kept
			clear(c.revoked)
		}
	}
	return out, errors.Join(errs...)
}

func toMessage(e *ckafka.Message) kafka.Message {
	var hs []kafka.Header
	if len(e.Headers) > 0 {
		hs = make([]kafka.Header, len(e.Headers))
		for i, h := range e.Headers {
			hs[i] = kafka.Header{Key: h.Key, Value: h.Value}
		}
	}
	m := kafka.Message{Key: e.Key, Value: e.Value, Partition: e.TopicPartition.Partition,
		Offset: int64(e.TopicPartition.Offset), Headers: hs, Timestamp: e.Timestamp}
	if e.TopicPartition.Topic != nil {
		m.Topic = *e.TopicPartition.Topic
	}
	return m
}

// Commit implements kafka.ConsumerClient. librdkafka's commit takes no
// context; ctx bounds only the wait for it — the call itself is counted until
// it returns, so Close never destroys the client under it. During Close (the
// revoke callback commits what was processed) it waits for the call: the
// client is destroyed right after the callback.
func (c *consumer) Commit(ctx context.Context, offsets []kafka.Offset) error {
	tps := make([]ckafka.TopicPartition, len(offsets))
	for i, o := range offsets {
		topic := o.Topic
		tps[i] = ckafka.TopicPartition{Topic: &topic, Partition: o.Partition, Offset: ckafka.Offset(o.Next)}
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return kafka.ErrClientClosed
	}
	closing := c.closing
	c.calls.Add(1)
	c.mu.Unlock()
	if closing {
		defer c.calls.Done()
		return commitOffsets(c.cl, tps)
	}
	done := make(chan error, 1)
	go func() {
		defer c.calls.Done()
		done <- commitOffsets(c.cl, tps)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func commitOffsets(cl *ckafka.Consumer, tps []ckafka.TopicPartition) error {
	res, err := cl.CommitOffsets(tps)
	for _, tp := range res {
		if tp.Error != nil {
			err = errors.Join(err, tp.Error)
		}
	}
	return err
}

// AllowRebalance implements kafka.ConsumerClient: librdkafka rebalances
// only within Poll, so there is nothing to release.
func (c *consumer) AllowRebalance() {}

// DeadLetter implements kafka.ConsumerClient: a librdkafka consumer cannot
// produce, so the dead-letter topic gets its own producer, created on first
// use with the same settings.
func (c *consumer) DeadLetter() kafka.ProducerClient {
	c.dlqOnce.Do(func() { c.dlq, c.dlqErr = newProducer(c.cfg, c.log, "dlq_producer") })
	if c.dlqErr != nil {
		return failedProducer{c.dlqErr}
	}
	return c.dlq
}

// Ping implements kafka.ConsumerClient: the fatal error once the client has
// failed (readiness must not stay green on a dead client), else a metadata
// request.
func (c *consumer) Ping(ctx context.Context) error {
	if err := c.failed.Load(); err != nil {
		return *err
	}
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return kafka.ErrClientClosed
	}
	c.calls.Add(1)
	c.mu.Unlock()
	defer c.calls.Done()
	return ping(ctx, c.cl)
}

// Stats implements kafka.ConsumerClient.
func (c *consumer) Stats() kafka.ClientStats { return c.stats.clientStats(c.cfg) }

// Close implements kafka.ConsumerClient: no new Ping starts, the calls
// already inside the client finish (within closeWait), then leaving the group
// revokes the partitions through the rebalance callback (committing their
// processed offsets) before the client is destroyed.
func (c *consumer) Close() {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return
	}
	c.closing = true
	c.mu.Unlock()
	if !waitTimeout(&c.calls, closeWait) {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.log.Error("kafka consumer: a call into librdkafka did not return; leaving the client to the exiting process",
			"waited", closeWait.String())
		return
	}
	if err := c.cl.Close(); err != nil {
		c.log.Warn("kafka consumer close", "err", err)
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	<-c.logsDone
	if c.dlq != nil {
		c.dlq.Close()
	}
}

// waitTimeout waits for wg up to d and reports whether it got to zero.
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// failedProducer is the dead-letter producer that could not be built.
type failedProducer struct{ err error }

func (f failedProducer) Send(context.Context, kafka.Message) error {
	return kafka.NewSendError(kafka.KindUnknown, 0, f.err)
}
func (f failedProducer) Ping(context.Context) error { return f.err }
func (failedProducer) Stats() kafka.ClientStats     { return kafka.ClientStats{} }
func (failedProducer) Close()                       {}
