package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is one consumed record, handed to a [Handler]. It exposes the fields a
// service handler usually needs without leaking the franz-go record type.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition int32
	Offset    int64
}

// Handler processes a single [Message]. Returning nil lets the consumer commit
// the record's offset; returning an error stops the poll loop with that error
// and leaves the record uncommitted, so it (and everything after it in the
// batch) is redelivered — at-least-once delivery.
type Handler func(ctx context.Context, msg Message) error

// Consumer drains a consumer group with a poll loop. It is the "background
// worker" shape of this monorepo, fed by a broker instead of a ticker.
type Consumer struct {
	cl  *kgo.Client
	cfg Config
	log *slog.Logger
}

// NewConsumer builds a consumer-group client subscribed to cfg.Topics and
// verifies broker connectivity with a ping. Auto-commit is disabled so offsets
// advance only past records a [Handler] has accepted. The caller owns it and
// must call [Consumer.Close].
func NewConsumer(ctx context.Context, cfg Config, log *slog.Logger) (*Consumer, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		// A brand-new group (no committed offsets yet) starts from the oldest
		// retained record rather than the newest, so a freshly-deployed worker
		// does not silently skip a backlog. Once the group has committed, those
		// offsets win and this only applies to new partitions.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka: consumer ping: %w", err)
	}

	log.Info("kafka consumer ready", "brokers", cfg.brokersString(), "group", cfg.Group, "topics", cfg.Topics)
	return &Consumer{cl: cl, cfg: cfg, log: log}, nil
}

// Run polls and dispatches records to handler until ctx is cancelled (a clean
// shutdown, returning nil) or handler returns an error (returning that error).
// Within each fetched batch it commits the longest prefix of records the
// handler accepted, so a mid-batch failure does not skip unprocessed records.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	c.log.Info("kafka consumer loop started", "group", c.cfg.Group, "topics", c.cfg.Topics)
	for {
		select {
		case <-ctx.Done():
			c.log.Info("kafka consumer loop stopping")
			return nil
		default:
		}

		fetches := c.cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		// A context cancellation surfaces as a fetch error; treat it as graceful
		// shutdown rather than logging it as a failure.
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Error("kafka fetch error", "topic", topic, "partition", partition, "err", err)
		})

		accepted := make([]*kgo.Record, 0, fetches.NumRecords())
		var handlerErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if handlerErr != nil {
				return // stop dispatching once a handler has failed
			}
			msg := Message{Topic: r.Topic, Key: r.Key, Value: r.Value, Partition: r.Partition, Offset: r.Offset}
			if err := handler(ctx, msg); err != nil {
				handlerErr = err
				return
			}
			accepted = append(accepted, r)
		})

		if len(accepted) > 0 {
			if err := c.cl.CommitRecords(ctx, accepted...); err != nil && ctx.Err() == nil {
				return fmt.Errorf("kafka: commit offsets: %w", err)
			}
		}
		if handlerErr != nil && !errors.Is(handlerErr, context.Canceled) {
			return fmt.Errorf("kafka: handler failed: %w", handlerErr)
		}
	}
}

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that pings the
// brokers with a short timeout.
func (c *Consumer) ReadyCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := c.cl.Ping(ctx); err != nil {
			return fmt.Errorf("kafka brokers unreachable: %w", err)
		}
		return nil
	}
}

// Close leaves the consumer group and shuts the client down.
func (c *Consumer) Close() { c.cl.Close() }
