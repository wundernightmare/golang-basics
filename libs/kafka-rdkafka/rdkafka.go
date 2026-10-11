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
	p.cl.Store(cl)
	return p, nil
}

// producer is a librdkafka producer. A fatal error (lost idempotence state)
// replaces the client underneath; librdkafka reconnects on its own
// otherwise.
type producer struct {
	cfg   kafka.Config
	props ckafka.ConfigMap
	log   *slog.Logger
	role  string
	stats *statsHolder

	cl        atomic.Pointer[ckafka.Producer]
	replacing atomic.Bool
	wg        sync.WaitGroup // event and log pumps
	closeOnce sync.Once
}

// newClient creates a librdkafka producer and starts draining its events
// (statistics, errors) and logs.
func (p *producer) newClient() (*ckafka.Producer, error) {
	props := ckafka.ConfigMap{}
	for k, v := range p.props {
		props[k] = v
	}
	cl, err := ckafka.NewProducer(&props)
	if err != nil {
		return nil, err
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
				go p.replace(cl)
			}
			// Non-fatal errors are librdkafka's to retry; its log line
			// (folded) already says what happened.
		}
	}
}

// replace swaps a fatally failed client for a new one. Records queued on the
// old one fail with a retryable error.
func (p *producer) replace(failed *ckafka.Producer) {
	if p.cl.Load() != failed || !p.replacing.CompareAndSwap(false, true) {
		return
	}
	defer p.replacing.Store(false)
	cl, err := p.newClient()
	if err != nil {
		p.log.Error("kafka producer could not be replaced; readiness stays failed", "err", err)
		return
	}
	if !p.cl.CompareAndSwap(failed, cl) {
		cl.Close()
		return
	}
	failed.Close()
}

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
		err = p.cl.Load().Produce(m, delivery)
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

// Ping implements kafka.ProducerClient.
func (p *producer) Ping(ctx context.Context) error { return ping(ctx, p.cl.Load()) }

// Stats implements kafka.ProducerClient.
func (p *producer) Stats() kafka.ClientStats { return p.stats.clientStats(p.cfg) }

// Close implements kafka.ProducerClient.
func (p *producer) Close() {
	p.closeOnce.Do(func() {
		cl := p.cl.Load()
		if left := cl.Flush(int(flushOnClose.Milliseconds())); left > 0 {
			p.log.Warn("kafka producer closed with records still queued; they were not delivered", "kafka_client", p.role, "records", left)
		}
		cl.Close()
		p.wg.Wait()
	})
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
	closed   atomic.Bool
	logsDone chan struct{}
}

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
	if c.closed.Load() {
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
				return out, e
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
// context; ctx bounds only the wait for it.
func (c *consumer) Commit(ctx context.Context, offsets []kafka.Offset) error {
	tps := make([]ckafka.TopicPartition, len(offsets))
	for i, o := range offsets {
		topic := o.Topic
		tps[i] = ckafka.TopicPartition{Topic: &topic, Partition: o.Partition, Offset: ckafka.Offset(o.Next)}
	}
	done := make(chan error, 1)
	go func() {
		res, err := c.cl.CommitOffsets(tps)
		for _, tp := range res {
			if tp.Error != nil {
				err = errors.Join(err, tp.Error)
			}
		}
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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

// Ping implements kafka.ConsumerClient.
func (c *consumer) Ping(ctx context.Context) error { return ping(ctx, c.cl) }

// Stats implements kafka.ConsumerClient.
func (c *consumer) Stats() kafka.ClientStats { return c.stats.clientStats(c.cfg) }

// Close implements kafka.ConsumerClient: leaving the group revokes the
// partitions through the rebalance callback (committing their processed
// offsets) before the client is destroyed.
func (c *consumer) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	if err := c.cl.Close(); err != nil {
		c.log.Warn("kafka consumer close", "err", err)
	}
	<-c.logsDone
	if c.dlq != nil {
		c.dlq.Close()
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
