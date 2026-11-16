// Package franz is the franz-go provider of libs/kafka: pure Go, no cgo, so
// a service built with it stays a CGO_ENABLED=0 binary on distroless/static.
// Import it for its side effect to link it:
//
//	import _ "github.com/wundernightmare/golang-basics/libs/kafka-franz"
//
// Every libs/kafka key is mapped onto a typed franz-go option (config.go);
// librdkafka properties (KAFKA_*_EXTRA_PROPERTIES) have no franz-go
// equivalent and are a config error here. Client metrics come from franz-go
// hooks (connections, request errors), reported while
// KAFKA_STATISTICS_INTERVAL_MS is positive (negative = off). See libs/kafka's
// README for when to pick it over libs/kafka-rdkafka.
package franz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/wundernightmare/golang-basics/libs/kafka"
)

func init() { kafka.Register(Driver{}) } //nolint:gochecknoinits // a provider registers itself on import (database/sql style); services pick one via internal/kafkaprovider

// Driver is the franz-go [kafka.Driver].
type Driver struct{}

// Name implements kafka.Driver.
func (Driver) Name() string { return "franz" }

// Validate implements kafka.Driver.
func (Driver) Validate(cfg kafka.Config) []error { return validate(cfg) }

// queueFullRetry is how often Send retries buffering while the producer
// buffer is full.
const queueFullRetry = 20 * time.Millisecond

// flushOnClose bounds how long Close waits for buffered records.
const flushOnClose = 5 * time.Second

// NewProducer implements kafka.Driver.
func (Driver) NewProducer(cfg kafka.Config, _ *slog.Logger) (kafka.ProducerClient, error) {
	st := newStats(cfg)

	opts, err := commonOpts(cfg, st)
	if err != nil {
		return nil, err
	}

	cl, err := kgo.NewClient(append(opts, producerOpts(cfg)...)...)
	if err != nil {
		return nil, err
	}

	st.cl = cl

	return &producer{cl: cl, stats: st}, nil
}

type producer struct {
	cl    *kgo.Client
	stats *stats
}

// Send implements kafka.ProducerClient: TryProduce (never block on a full
// buffer inside the client) retried while the buffer is full, then the
// promise awaited.
//
// The record is produced with a context that ctx cannot cancel. franz-go
// fails every buffered record of a partition when the context of the batch's
// first record ends, so a caller that gave up would fail the sends of other
// callers. ctx bounds this call's wait instead; the record itself is bounded
// by RecordDeliveryTimeout (KAFKA_PUBLISH_TIMEOUT).
func (p *producer) Send(ctx context.Context, msg kafka.Message) error {
	rec := &kgo.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value}
	for _, h := range msg.Headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}

	for {
		done := make(chan error, 1)

		p.cl.TryProduce(context.WithoutCancel(ctx), rec, func(_ *kgo.Record, err error) { done <- err })

		var err error
		select {
		case err = <-done:
		case <-ctx.Done():
			err = ctx.Err()
		}

		if errors.Is(err, kgo.ErrMaxBuffered) {
			t := time.NewTimer(queueFullRetry)
			select {
			case <-t.C:
				continue // back-pressure: wait for room within the budget
			case <-ctx.Done():
				t.Stop()
			}
		}

		return sendError(err)
	}
}

func (p *producer) Ping(ctx context.Context) error { return ping(ctx, p.cl) }

func (p *producer) Stats() kafka.ClientStats { return p.stats.producer() }

func (p *producer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), flushOnClose)
	defer cancel()

	_ = p.cl.Flush(ctx) // what does not make it fails with the client closing
	p.cl.Close()
}

// ping is Ping that returns when ctx does: franz-go's Ping keeps waiting for
// a broker that accepted the connection but stopped answering (a frozen
// process) until its own request timeout, well past a readiness deadline.
// The in-flight request is left to finish in the background.
func ping(ctx context.Context, cl *kgo.Client) error {
	done := make(chan error, 1)
	go func() { done <- cl.Ping(ctx) }()

	select {
	case err := <-done:
		return closedError(err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// closedError makes franz-go's "client closed" the libs/kafka one as well
// (errors.Is with [kafka.ErrClientClosed]).
func closedError(err error) error {
	if errors.Is(err, kgo.ErrClientClosed) {
		return fmt.Errorf("%w: %w", kafka.ErrClientClosed, err)
	}

	return err
}

// sendError maps a produce error onto the registry: a broker's answer by its
// protocol code (the table is libs/kafka's, shared with the librdkafka
// provider), franz-go's own sentinels by what they say.
func sendError(err error) error {
	if err == nil {
		return nil
	}

	err = closedError(err)
	kind, code := kafka.KindUnknown, 0

	var ke *kerr.Error
	switch {
	case errors.As(err, &ke):
		kind, code = kafka.BrokerSendErrorKind(ke.Code), int(ke.Code)
	case errors.Is(err, kgo.ErrMaxBuffered):
		kind = kafka.KindQueueFull
	case errors.Is(err, kgo.ErrRecordTimeout), errors.Is(err, kgo.ErrRecordRetries), errors.Is(err, kgo.ErrClientClosed):
		kind = kafka.KindUnavailable
	}

	// A net.Error carries no Kafka code: a connection that failed or timed
	// out is the brokers being unavailable, as librdkafka's _TRANSPORT is.
	var ne net.Error
	if kind == kafka.KindUnknown && errors.As(err, &ne) {
		kind = kafka.KindUnavailable
	}

	return kafka.NewSendError(kind, code, err)
}

// NewConsumer implements kafka.Driver.
func (Driver) NewConsumer(cfg kafka.Config, log *slog.Logger, rb kafka.Rebalance) (kafka.ConsumerClient, error) {
	stats := newStats(cfg)
	cons := &consumer{cfg: cfg, log: log, rb: rb, stats: stats}

	opts, err := consumerClientOpts(cfg, stats) // the dead-letter topic is produced through this client
	if err != nil {
		return nil, err
	}

	opts = append(opts,
		kgo.OnPartitionsAssigned(cons.onAssigned),
		kgo.OnPartitionsRevoked(cons.onRevoked),
		kgo.OnPartitionsLost(cons.onLost),
	)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	cons.cl = cl
	stats.cl = cl

	return cons, nil
}

type consumer struct {
	cl    *kgo.Client
	cfg   kafka.Config
	log   *slog.Logger
	rb    kafka.Rebalance
	stats *stats

	closed atomic.Bool
}

// Poll implements kafka.ConsumerClient. No record of a revoked partition is
// in the batch: with BlockRebalanceOnPoll (config.go) franz-go runs no
// rebalance callback from the moment PollRecords returns until
// AllowRebalance.
func (c *consumer) Poll(ctx context.Context, maxRecords int) ([]kafka.Message, error) {
	fetches := c.cl.PollRecords(ctx, maxRecords)
	if fetches.IsClientClosed() {
		return nil, kafka.ErrClientClosed
	}

	var errs []error

	fetches.EachError(func(topic string, partition int32, err error) {
		// A poll cut short by ctx or by Close comes back as a fetch error:
		// that is the loop stopping, not a broker failing.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kgo.ErrClientClosed) {
			return
		}

		errs = append(errs, fmt.Errorf("%s/%d: %w", topic, partition, err))
	})
	records := fetches.Records()

	out := make([]kafka.Message, len(records))
	for i, r := range records {
		var hs []kafka.Header
		if len(r.Headers) > 0 {
			hs = make([]kafka.Header, len(r.Headers))
			for j, h := range r.Headers {
				hs[j] = kafka.Header{Key: h.Key, Value: h.Value}
			}
		}

		out[i] = kafka.Message{
			Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value,
			Headers: hs, Timestamp: r.Timestamp,
		}
	}

	return out, errors.Join(errs...)
}

// Commit implements kafka.ConsumerClient. The leader epoch is left unknown
// (-1): the committed offset is what the next owner resumes from.
func (c *consumer) Commit(ctx context.Context, offsets []kafka.Offset) error {
	m := map[string]map[int32]kgo.EpochOffset{}
	for _, o := range offsets {
		if m[o.Topic] == nil {
			m[o.Topic] = map[int32]kgo.EpochOffset{}
		}

		m[o.Topic][o.Partition] = kgo.EpochOffset{Epoch: -1, Offset: o.Next}
	}

	var cerr error

	c.cl.CommitOffsetsSync(ctx, m, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			cerr = err
			return
		}

		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if e := kerr.ErrorForCode(p.ErrorCode); e != nil {
					cerr = errors.Join(cerr, fmt.Errorf("%s/%d: %w", t.Topic, p.Partition, e))
				}
			}
		}
	})

	return cerr
}

// AllowRebalance implements kafka.ConsumerClient: it releases the group
// change BlockRebalanceOnPoll has held back since Poll returned.
func (c *consumer) AllowRebalance() { c.cl.AllowRebalance() }

// DeadLetter implements kafka.ConsumerClient: the same client produces.
func (c *consumer) DeadLetter() kafka.ProducerClient { return &producer{cl: c.cl, stats: c.stats} }

func (c *consumer) Ping(ctx context.Context) error { return ping(ctx, c.cl) }

func (c *consumer) Stats() kafka.ClientStats { return c.stats.consumer() }

// Close implements kafka.ConsumerClient: leave the group, committing what
// was processed on the partitions held (the revoke callback).
func (c *consumer) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}

	c.cl.CloseAllowingRebalance()
}

func toPartitions(m map[string][]int32) []kafka.TopicPartition {
	var out []kafka.TopicPartition

	for topic, ps := range m {
		for _, p := range ps {
			out = append(out, kafka.TopicPartition{Topic: topic, Partition: p})
		}
	}

	return out
}

// The group callbacks hand franz-go's report to the loop's. franz-go runs
// them on its own goroutine, not the loop's.
func (c *consumer) onAssigned(ctx context.Context, _ *kgo.Client, m map[string][]int32) {
	c.rb.Assigned(ctx, toPartitions(m))
}

func (c *consumer) onRevoked(ctx context.Context, _ *kgo.Client, m map[string][]int32) {
	c.rb.Revoked(ctx, toPartitions(m))
}

func (c *consumer) onLost(ctx context.Context, _ *kgo.Client, m map[string][]int32) {
	c.rb.Lost(ctx, toPartitions(m))
}
