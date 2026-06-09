package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes records to Kafka synchronously (one network round-trip per
// Publish, waiting for the broker ack). It is safe for concurrent use;
// construct one with [NewProducer] and share it.
type Producer struct {
	cl  *kgo.Client
	cfg Config
	log *slog.Logger
}

// NewProducer builds a producer from cfg and verifies broker connectivity with
// a ping (so a misconfigured broker fails fast at boot). Records default to
// cfg.Topic when [Producer.Publish] is given an empty topic. The caller owns it
// and must call [Producer.Close].
func NewProducer(ctx context.Context, cfg Config, log *slog.Logger) (*Producer, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ProducerLinger(0), // synchronous shape: don't batch-wait
		// Ask the broker to create the topic on first publish when it is
		// missing (the broker still decides, via auto.create.topics.enable).
		// Convenient for a demo/dev broker; production pre-provisions topics.
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka: producer ping: %w", err)
	}

	log.Info("kafka producer ready", "brokers", cfg.brokersString(), "default_topic", cfg.Topic)
	return &Producer{cl: cl, cfg: cfg, log: log}, nil
}

// Publish sends one record and blocks until the broker acknowledges it. An
// empty topic falls back to the configured default Topic. key may be nil; it
// determines partitioning (records with the same key keep their relative order).
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte) error {
	if topic == "" {
		topic = p.cfg.Topic
	}
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	if err := p.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish to %q: %w", topic, err)
	}
	p.log.Debug("kafka record published", "topic", topic, "bytes", len(value))
	return nil
}

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that pings the
// brokers with a short timeout.
func (p *Producer) ReadyCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := p.cl.Ping(ctx); err != nil {
			return fmt.Errorf("kafka brokers unreachable: %w", err)
		}
		return nil
	}
}

// Close flushes any buffered records and shuts the client down.
func (p *Producer) Close() { p.cl.Close() }
