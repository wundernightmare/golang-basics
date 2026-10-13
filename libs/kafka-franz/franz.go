// Package franz is the franz-go provider of libs/kafka: pure Go, no cgo, so
// a service built with it stays a CGO_ENABLED=0 binary on distroless/static.
// Import it for its side effect to link it:
//
//	import _ "github.com/tracehubmmp/golang-basics/libs/kafka-franz"
//
// Every libs/kafka key is mapped onto a typed franz-go option (config.go);
// librdkafka properties (KAFKA_*_EXTRA_PROPERTIES) have no franz-go
// equivalent and are a config error here. Client metrics come from franz-go
// hooks (connections, request errors), reported while
// KAFKA_STATISTICS_INTERVAL_MS is not 0; consumer-group lag is the broker's
// to export. See libs/kafka's README for when to pick it over
// libs/kafka-rdkafka.
package franz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

func init() { kafka.Register(Driver{}) }

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

// producer is a franz-go client used for producing.
type producer struct {
	cl    *kgo.Client
	stats *stats
}

// Send implements kafka.ProducerClient: TryProduce (never block on a full
// buffer inside the client) retried while the buffer is full, then the
// promise awaited.
func (p *producer) Send(ctx context.Context, msg kafka.Message) error {
	rec := &kgo.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value}
	for _, h := range msg.Headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	for {
		done := make(chan error, 1)
		p.cl.TryProduce(ctx, rec, func(_ *kgo.Record, err error) { done <- err })
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

// Ping implements kafka.ProducerClient.
func (p *producer) Ping(ctx context.Context) error { return ping(ctx, p.cl) }

// Stats implements kafka.ProducerClient.
func (p *producer) Stats() kafka.ClientStats { return p.stats.producer() }

// Close implements kafka.ProducerClient.
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
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendErrorKinds maps Kafka protocol errors onto the libs/kafka kinds (the
// same codes as the librdkafka provider and Node's KAFKA_SEND_ERRORS).
var sendErrorKinds = map[*kerr.Error]string{
	kerr.RequestTimedOut:              kafka.KindUnavailable,
	kerr.LeaderNotAvailable:           kafka.KindUnavailable,
	kerr.NotLeaderForPartition:        kafka.KindUnavailable,
	kerr.NetworkException:             kafka.KindUnavailable,
	kerr.NotEnoughReplicas:            kafka.KindUnavailable,
	kerr.NotEnoughReplicasAfterAppend: kafka.KindUnavailable,
	kerr.KafkaStorageError:            kafka.KindUnavailable,
	kerr.ThrottlingQuotaExceeded:      kafka.KindUnavailable,

	kerr.UnknownTopicOrPartition:            kafka.KindNotProvisioned,
	kerr.TopicAuthorizationFailed:           kafka.KindNotProvisioned,
	kerr.ClusterAuthorizationFailed:         kafka.KindNotProvisioned,
	kerr.SaslAuthenticationFailed:           kafka.KindNotProvisioned,
	kerr.IllegalSaslState:                   kafka.KindNotProvisioned,
	kerr.UnsupportedSaslMechanism:           kafka.KindNotProvisioned,
	kerr.TransactionalIDAuthorizationFailed: kafka.KindNotProvisioned,

	kerr.MessageTooLarge:       kafka.KindRejected,
	kerr.RecordListTooLarge:    kafka.KindRejected,
	kerr.InvalidRecord:         kafka.KindRejected,
	kerr.CorruptMessage:        kafka.KindRejected,
	kerr.PolicyViolation:       kafka.KindRejected,
	kerr.InvalidTopicException: kafka.KindRejected,
}

func sendError(err error) error {
	if err == nil {
		return nil
	}
	kind, code := kafka.KindUnknown, 0
	var ke *kerr.Error
	switch {
	case errors.As(err, &ke):
		code = int(ke.Code)
		if k, ok := sendErrorKinds[ke]; ok {
			kind = k
		}
	case errors.Is(err, kgo.ErrMaxBuffered):
		kind = kafka.KindQueueFull
	case errors.Is(err, kgo.ErrRecordTimeout), errors.Is(err, kgo.ErrRecordRetries), errors.Is(err, kgo.ErrClientClosed):
		kind = kafka.KindUnavailable
	}
	var ne net.Error
	if kind == kafka.KindUnknown && errors.As(err, &ne) {
		kind = kafka.KindUnavailable
	}
	return kafka.NewSendError(kind, code, err)
}

// NewConsumer implements kafka.Driver.
func (Driver) NewConsumer(cfg kafka.Config, log *slog.Logger, rb kafka.Rebalance) (kafka.ConsumerClient, error) {
	st := newStats(cfg)
	c := &consumer{cfg: cfg, log: log, rb: rb, stats: st}
	opts, err := commonOpts(cfg, st)
	if err != nil {
		return nil, err
	}
	opts = append(opts, producerOpts(cfg)...) // the dead-letter topic is produced through this client
	opts = append(opts, consumerOpts(cfg)...)
	opts = append(opts,
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	st.cl = cl
	return c, nil
}

// consumer is a franz-go consumer-group member.
type consumer struct {
	cl    *kgo.Client
	cfg   kafka.Config
	log   *slog.Logger
	rb    kafka.Rebalance
	stats *stats

	mu    sync.Mutex
	owned map[kafka.TopicPartition]struct{}

	closed atomic.Bool
}

// Poll implements kafka.ConsumerClient.
func (c *consumer) Poll(ctx context.Context, max int) ([]kafka.Message, error) {
	fetches := c.cl.PollRecords(ctx, max)
	if fetches.IsClientClosed() {
		return nil, kafka.ErrClientClosed
	}
	var errs []error
	fetches.EachError(func(topic string, partition int32, err error) {
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
		out[i] = kafka.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value,
			Headers: hs, Timestamp: r.Timestamp}
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

// AllowRebalance implements kafka.ConsumerClient.
func (c *consumer) AllowRebalance() { c.cl.AllowRebalance() }

// DeadLetter implements kafka.ConsumerClient: the same client produces.
func (c *consumer) DeadLetter() kafka.ProducerClient { return &producer{cl: c.cl, stats: c.stats} }

// Ping implements kafka.ConsumerClient.
func (c *consumer) Ping(ctx context.Context) error { return ping(ctx, c.cl) }

// Stats implements kafka.ConsumerClient.
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

func (c *consumer) onAssigned(ctx context.Context, _ *kgo.Client, m map[string][]int32) {
	parts := toPartitions(m)
	c.mu.Lock()
	if c.owned == nil {
		c.owned = map[kafka.TopicPartition]struct{}{}
	}
	for _, p := range parts {
		c.owned[p] = struct{}{}
	}
	c.mu.Unlock()
	c.rb.Assigned(ctx, parts)
}

func (c *consumer) onRevoked(ctx context.Context, _ *kgo.Client, m map[string][]int32) {
	parts := toPartitions(m)
	c.rb.Revoked(ctx, parts)
	c.forget(parts)
}

func (c *consumer) onLost(ctx context.Context, _ *kgo.Client, m map[string][]int32) {
	parts := toPartitions(m)
	c.rb.Lost(ctx, parts)
	c.forget(parts)
}

func (c *consumer) forget(parts []kafka.TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range parts {
		delete(c.owned, p)
	}
}
