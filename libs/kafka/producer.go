package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

// Producer publishes records to Kafka synchronously (one network round-trip per
// Publish, waiting for every in-sync replica to acknowledge). The idempotent
// producer is on (franz-go's default), so a retried produce does not
// duplicate a record within a partition. It is safe for concurrent use;
// construct one with [NewProducer] and share it.
//
// Each Publish is a producer span and the W3C trace context travels in the
// record headers, so the consumer on the other side continues the same trace.
// Register [Producer.Collectors] for metrics.
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
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: producer config: %w", err)
	}
	opts, err := cfg.commonOpts()
	if err != nil {
		return nil, err
	}
	opts = append(opts, cfg.producerOpts()...)
	opts = append(opts, kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(kotel.NewTracer(kotel.ClientID(cfg.ClientID)))).Hooks()...))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka: producer ping: %w", err)
	}
	warnPlaintextSASL(ctx, cfg, log)

	log.InfoContext(ctx, "kafka producer ready", "brokers", cfg.brokersString(), "default_topic", cfg.Topic,
		"tls", cfg.TLSEnabled, "sasl", cfg.SASLMechanism, "auto_topic_creation", cfg.AllowAutoTopicCreation)
	return &Producer{cl: cl, cfg: cfg, log: log, metrics: newProducerMetrics()}, nil
}

// Publish sends one record and blocks until the broker acknowledges it or
// PublishTimeout passes. An empty topic falls back to the configured default
// Topic. key may be nil; it determines partitioning (records with the same
// key keep their relative order).
//
// Trace context: when headers carry a traceparent (an outbox relay replaying
// the context stored with the event), the publish span continues that trace;
// otherwise it is a child of the span in ctx. Either way the record leaves
// with a traceparent for the consumer.
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte, headers ...Header) error {
	if topic == "" {
		topic = p.cfg.Topic
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()

	start := time.Now()
	err := produce(ctx, p.cl, newRecord(ctx, topic, key, value, headers))
	result := publishResult(err)
	p.metrics.duration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	p.metrics.records.WithLabelValues(topic, result).Inc()
	if err != nil {
		return fmt.Errorf("kafka: publish to %q (%s): %w", topic, result, err)
	}
	p.log.DebugContext(ctx, "kafka record published", "topic", topic, "bytes", len(value))
	return nil
}

// newRecord builds the record, with the context the kotel produce hook reads
// to parent the publish span and inject the trace headers.
func newRecord(ctx context.Context, topic string, key, value []byte, headers []Header) *kgo.Record {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	if len(headers) > 0 {
		rec.Headers = make([]kgo.RecordHeader, len(headers))
		for i, h := range headers {
			rec.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
		}
		if _, ok := headerValue(rec.Headers, HeaderTraceparent); ok {
			// The stored context wins over the caller's: extract it as the
			// remote parent of the publish span.
			ctx = otel.GetTextMapPropagator().Extract(ctx, kotel.NewRecordCarrier(rec))
		}
	}
	rec.Context = ctx
	return rec
}

// produce is the synchronous produce both the [Producer] and the consumer's
// dead-letter publishing use.
func produce(ctx context.Context, cl *kgo.Client, rec *kgo.Record) error {
	return cl.ProduceSync(ctx, rec).FirstErr()
}

// Publish results: a bounded set, so the metric label cannot explode.
const (
	resultOK           = "ok"
	resultTimeout      = "timeout"
	resultTooLarge     = "too_large"
	resultAuth         = "auth"
	resultUnknownTopic = "unknown_topic"
	resultOther        = "other"
)

// publishResult classifies a produce error into the bounded result label.
func publishResult(err error) string {
	switch {
	case err == nil:
		return resultOK
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kgo.ErrRecordTimeout), errors.Is(err, kerr.RequestTimedOut):
		return resultTimeout
	case errors.Is(err, kerr.MessageTooLarge), errors.Is(err, kerr.RecordListTooLarge):
		return resultTooLarge
	case errors.Is(err, kerr.SaslAuthenticationFailed), errors.Is(err, kerr.TopicAuthorizationFailed),
		errors.Is(err, kerr.ClusterAuthorizationFailed), errors.Is(err, kerr.TransactionalIDAuthorizationFailed),
		errors.Is(err, kerr.IllegalSaslState), errors.Is(err, kerr.UnsupportedSaslMechanism):
		return resultAuth
	case errors.Is(err, kerr.UnknownTopicOrPartition):
		return resultUnknownTopic
	default:
		return resultOther
	}
}

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that pings the
// brokers with a short timeout.
func (p *Producer) ReadyCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := pingBounded(ctx, p.cl); err != nil {
			return fmt.Errorf("kafka brokers unreachable: %w", err)
		}
		return nil
	}
}

// Close flushes any buffered records and shuts the client down.
func (p *Producer) Close() { p.cl.Close() }

// pingBounded is Ping that returns when ctx does: franz-go's Ping keeps
// waiting for a broker that accepted the connection but stopped answering
// (a frozen process) until its own request timeout, well past a readiness
// deadline. The in-flight request is left to finish in the background.
func pingBounded(ctx context.Context, cl *kgo.Client) error {
	done := make(chan error, 1)
	go func() { done <- cl.Ping(ctx) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// warnPlaintextSASL logs the one insecure combination Validate allows: a
// PLAIN password sent over an unencrypted connection.
func warnPlaintextSASL(ctx context.Context, cfg Config, log *slog.Logger) {
	if cfg.SASLMechanism == "plain" && !cfg.TLSEnabled {
		log.WarnContext(ctx, "kafka SASL PLAIN without TLS sends the password in clear text; set KAFKA_TLS_ENABLED")
	}
}
