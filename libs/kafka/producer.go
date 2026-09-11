package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
)

// Producer publishes records to Kafka synchronously (one network round-trip per
// Publish, waiting for the broker ack). It is safe for concurrent use;
// construct one with [NewProducer] and share it.
//
// Each Publish is a producer span (child of the span in ctx) and the trace
// context is written into the record headers, so the consumer on the other
// side continues the same trace. Register [Producer.Collectors] for metrics.
type Producer struct {
	cl      *kgo.Client
	cfg     Config
	log     *slog.Logger
	metrics *producerMetrics
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
		kgo.WithHooks(tracingHooks()...),
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

	log.InfoContext(ctx, "kafka producer ready", "brokers", cfg.brokersString(), "default_topic", cfg.Topic)
	return &Producer{cl: cl, cfg: cfg, log: log, metrics: newProducerMetrics()}, nil
}

// tracingHooks returns the franz-go hooks that create produce/receive spans and
// carry the W3C trace context in record headers. The propagator and tracer
// provider are the globals otelx.Init installs; with tracing disabled they
// are no-ops.
func tracingHooks() []kgo.Hook {
	return kotel.NewKotel(kotel.WithTracer(kotel.NewTracer())).Hooks()
}

// Publish sends one record and blocks until the broker acknowledges it. An
// empty topic falls back to the configured default Topic. key may be nil; it
// determines partitioning (records with the same key keep their relative order).
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte) error {
	if topic == "" {
		topic = p.cfg.Topic
	}
	// Context on the record is what the kotel hook reads to parent the
	// produce span and inject the trace headers.
	rec := &kgo.Record{Topic: topic, Key: key, Value: value, Context: ctx}
	start := time.Now()
	err := p.cl.ProduceSync(ctx, rec).FirstErr()
	p.metrics.duration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	if err != nil {
		p.metrics.records.WithLabelValues(topic, "error").Inc()
		return fmt.Errorf("kafka: publish to %q: %w", topic, err)
	}
	p.metrics.records.WithLabelValues(topic, "ok").Inc()
	p.log.DebugContext(ctx, "kafka record published", "topic", topic, "bytes", len(value))
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
