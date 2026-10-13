package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName names this library's tracer.
const instrumentationName = "github.com/tracehubmmp/golang-basics/libs/kafka"

func tracer() trace.Tracer { return otel.Tracer(instrumentationName) }

// Producer publishes records synchronously: [Producer.Send] returns once
// every in-sync replica acknowledged the record (acks=all, idempotent: a
// retried send never duplicates or reorders a record within a partition).
// It is safe for concurrent use — concurrent sends share batches
// (KAFKA_PRODUCER_LINGER_MS) and their compression; construct one with
// [NewProducer] and share it.
//
// Each Send is a producer span and the W3C trace context travels in the
// record headers, so the consumer on the other side continues the same
// trace. Register [Producer.Collectors] for metrics.
type Producer struct {
	cl      ProducerClient
	cfg     Config
	log     *slog.Logger
	metrics *producerMetrics
}

// NewProducer builds a producer from cfg with the linked provider (see
// [Driver]) and verifies broker connectivity (so a misconfigured broker or
// credential fails fast at boot). The caller owns it and must call
// [Producer.Close].
func NewProducer(ctx context.Context, cfg Config, log *slog.Logger) (*Producer, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	d, err := mustDriver()
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: producer config: %w", err)
	}
	cl, err := d.NewProducer(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("kafka: new %s producer: %w", d.Name(), err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka: producer: brokers unreachable: %w", err)
	}
	warnPlaintextSASL(ctx, cfg, log)
	log.InfoContext(ctx, "kafka producer ready", "provider", d.Name(), "brokers", cfg.brokersString(),
		"default_topic", cfg.Topic, "security_protocol", cfg.SecurityProtocol, "sasl", cfg.SASLMechanism,
		"compression", cfg.CompressionType, "queue_max_kbytes", cfg.ProducerQueueMaxKBytes,
		"auto_topic_creation", cfg.AllowAutoTopicCreation)
	return &Producer{cl: cl, cfg: cfg, log: log, metrics: newProducerMetrics(cl.Stats, d.Name())}, nil
}

// Send publishes msg and blocks until the brokers acknowledge it or
// KAFKA_PUBLISH_TIMEOUT passes (or ctx ends first). An empty msg.Topic falls
// back to the configured default Topic; msg.Key may be nil and determines
// the partition (records with the same key keep their relative order).
// Every error is a *[SendError] (see [Rejected]).
//
// Trace context: when msg.Headers carry a traceparent (an outbox relay
// replaying the context stored with the event), the publish span continues
// that trace; otherwise it is a child of the span in ctx. Either way the
// record leaves with a traceparent for the consumer.
func (p *Producer) Send(ctx context.Context, msg Message) error {
	if msg.Topic == "" {
		msg.Topic = p.cfg.Topic
	}
	start := time.Now()
	err := send(ctx, p.cl, p.cfg.PublishTimeout, msg)
	p.metrics.send.observe(msg.Topic, start, err)
	if err != nil {
		return fmt.Errorf("kafka: send to %q: %w", msg.Topic, err)
	}
	p.log.DebugContext(ctx, "kafka record sent", "topic", msg.Topic, "bytes", len(msg.Value))
	return nil
}

// send is the traced, bounded send both the [Producer] and the consumer's
// dead-letter publishing use. The wait is bounded by budget plus a second
// of slack: the client itself fails the record at the budget.
func send(ctx context.Context, cl ProducerClient, budget time.Duration, msg Message) error {
	msg.Headers = append([]Header(nil), msg.Headers...) // the caller's slice is not ours to grow
	parent := ctx
	if _, ok := headerValue(msg.Headers, HeaderTraceparent); ok {
		// The stored context wins over the caller's: it is the remote parent.
		parent = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{&msg.Headers})
	}
	ctx, span := tracer().Start(parent, "publish "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.type", "send"),
			attribute.String("messaging.operation.name", "publish"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.Int("messaging.message.body.size", len(msg.Value)),
		))
	defer span.End()
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{&msg.Headers})

	ctx, cancel := context.WithTimeout(ctx, budget+time.Second)
	defer cancel()
	err := cl.Send(ctx, msg)
	if err != nil {
		var se *SendError
		if !errors.As(err, &se) {
			se = NewSendError(KindUnknown, 0, err)
			err = se
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, se.Kind)
		span.SetAttributes(attribute.String("error.type", se.Kind))
	}
	return err
}

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that asks
// the brokers for metadata with a short timeout.
func (p *Producer) ReadyCheck() func(ctx context.Context) error {
	return readyCheck(p.cl)
}

func readyCheck(cl interface{ Ping(context.Context) error }) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := cl.Ping(ctx); err != nil {
			return fmt.Errorf("kafka brokers unreachable: %w", err)
		}
		return nil
	}
}

// Close flushes queued records (briefly) and shuts the client down.
func (p *Producer) Close() { p.cl.Close() }

// warnPlaintextSASL logs the one insecure combination Validate allows: a
// password sent over an unencrypted connection.
func warnPlaintextSASL(ctx context.Context, cfg Config, log *slog.Logger) {
	if cfg.SecurityProtocol == "sasl_plaintext" && cfg.SASLMechanism == "PLAIN" {
		log.WarnContext(ctx, "kafka SASL PLAIN over sasl_plaintext sends the password in clear text; use sasl_ssl")
	}
}

// headerCarrier adapts record headers to the OTel propagator, so the W3C
// trace context travels in them (traceparent / tracestate).
type headerCarrier struct{ hs *[]Header }

func (c headerCarrier) Get(key string) string {
	v, _ := headerValue(*c.hs, key)
	return v
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range *c.hs {
		if h.Key == key {
			(*c.hs)[i].Value = []byte(value)
			return
		}
	}
	*c.hs = append(*c.hs, Header{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, len(*c.hs))
	for i, h := range *c.hs {
		keys[i] = h.Key
	}
	return keys
}

var _ propagation.TextMapCarrier = headerCarrier{}
