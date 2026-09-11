package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/codes"
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
//
// ctx carries the record's "process" span, continued from the trace the
// producer put in the record headers: log with the *Context methods and pass
// ctx to downstream calls so the whole hop is one trace.
type Handler func(ctx context.Context, msg Message) error

// Consumer drains a consumer group with a poll loop. It is the "background
// worker" shape of this monorepo, fed by a broker instead of a ticker.
// Register [Consumer.Collectors] for records / errors / handler latency and
// the group's lag, which the consumer polls from the broker every
// Config.LagInterval.
type Consumer struct {
	cl      *kgo.Client
	cfg     Config
	log     *slog.Logger
	tracer  *kotel.Tracer
	metrics *consumerMetrics
}

// NewConsumer builds a consumer-group client subscribed to cfg.Topics and
// verifies broker connectivity with a ping. Auto-commit is disabled so offsets
// advance only past records a [Handler] has accepted. The caller owns it and
// must call [Consumer.Close].
func NewConsumer(ctx context.Context, cfg Config, log *slog.Logger) (*Consumer, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	tracer := kotel.NewTracer()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(tracer)).Hooks()...),
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

	log.InfoContext(ctx, "kafka consumer ready", "brokers", cfg.brokersString(), "group", cfg.Group, "topics", cfg.Topics)
	return &Consumer{cl: cl, cfg: cfg, log: log, tracer: tracer, metrics: newConsumerMetrics()}, nil
}

// Run polls and dispatches records to handler until ctx is cancelled (a clean
// shutdown, returning nil) or handler returns an error (returning that error).
// Within each fetched batch it commits the longest prefix of records the
// handler accepted, so a mid-batch failure does not skip unprocessed records.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	c.log.InfoContext(ctx, "kafka consumer loop started", "group", c.cfg.Group, "topics", c.cfg.Topics)
	if c.cfg.LagInterval > 0 {
		go c.pollLag(ctx)
	}
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
			c.metrics.fetchErrs.WithLabelValues(topic).Inc()
			c.log.ErrorContext(ctx, "kafka fetch error", "topic", topic, "partition", partition, "err", err)
		})

		accepted := make([]*kgo.Record, 0, fetches.NumRecords())
		var handlerErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if handlerErr != nil {
				return // stop dispatching once a handler has failed
			}
			if err := c.dispatch(ctx, r, handler); err != nil {
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

// dispatch runs handler for one record inside a "process" span that continues
// the trace from the record headers (the kotel receive hook already parsed
// them into r.Context), records the outcome on the span and in the metrics.
func (c *Consumer) dispatch(ctx context.Context, r *kgo.Record, handler Handler) error {
	rctx, span := c.tracer.WithProcessSpan(r)
	defer span.End()
	// A cancelled poll context must still stop the handler: derive from the
	// span context but watch the loop context.
	rctx, cancel := context.WithCancel(rctx)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	msg := Message{Topic: r.Topic, Key: r.Key, Value: r.Value, Partition: r.Partition, Offset: r.Offset}
	start := time.Now()
	err := handler(rctx, msg)
	c.metrics.duration.WithLabelValues(r.Topic).Observe(time.Since(start).Seconds())
	c.metrics.records.WithLabelValues(r.Topic).Inc()
	if err != nil {
		c.metrics.handlerErrs.WithLabelValues(r.Topic).Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// pollLag refreshes kafka_consumer_group_lag from the broker's committed
// offsets every LagInterval until ctx is cancelled. One admin round-trip per
// interval; failures are logged at debug and retried next tick.
func (c *Consumer) pollLag(ctx context.Context) {
	adm := kadm.NewClient(c.cl)
	t := time.NewTicker(c.cfg.LagInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		lagCtx, cancel := context.WithTimeout(ctx, c.cfg.LagInterval)
		lags, err := adm.Lag(lagCtx, c.cfg.Group)
		cancel()
		if err != nil {
			c.log.DebugContext(ctx, "kafka lag poll failed", "group", c.cfg.Group, "err", err)
			continue
		}
		c.metrics.lag.Reset() // partitions come and go on rebalance
		for _, l := range lags[c.cfg.Group].Lag.Sorted() {
			if l.Lag < 0 {
				continue // commit or list-offset error for this partition; see l.Err
			}
			c.metrics.lag.WithLabelValues(l.Topic, strconv.Itoa(int(l.Partition))).Set(float64(l.Lag))
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
