// Package pipeline is the write path of the ingest service: every accepted
// event is offered to two sinks — Kafka and the S3 archive — and the request
// is answered as soon as either has made the whole batch durable.
//
// The rule that gives the service its guarantees:
//
//	An acknowledgement is sent only after a sink confirmed. Until then the
//	events exist in this process's memory alone, and the client still holds
//	them.
//
// So there is nothing to lose that was promised: a pod killed at any moment
// (OOM, eviction, node loss, a rollout) takes with it only requests that were
// never answered, which the client sends again. What a dying pod can lose is
// the *second* copy of an event the first sink already holds — the archive
// copy of a batch still lingering, or a Kafka record still in the client —
// and that is counted, not hidden.
//
// The two sinks behave differently and are driven differently:
//
//   - Kafka acknowledges a record in milliseconds; each event is one send
//     (libs/kafka batches them underneath). While Kafka is healthy it answers
//     every request, and the archive batch can linger for seconds, growing
//     into few large objects.
//   - The archive acknowledges a batch (archive.Writer: one object per
//     flush). A request waits for it only when Kafka does not answer — its
//     circuit is open, it failed, or it is slower than KafkaAckGrace — and
//     then the batch is flushed within UrgentLinger instead.
//
// Each sink has a circuit breaker so a sink that is down is skipped at once
// rather than waited on, and what the process holds unconfirmed is bounded
// (MaxInFlightBytes): past it requests are refused with [ErrOverloaded]
// instead of the pod growing until the kernel kills it.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
)

// Event is one validated event of a request.
type Event struct {
	ID   string
	Type string // "" when the event has none
	// Raw is the event's JSON object as the client sent it: what both sinks
	// store.
	Raw []byte
}

// Publisher is the Kafka side (libs/kafka.Producer).
type Publisher interface {
	Send(ctx context.Context, msg kafka.Message) error
}

// Archive is the S3 side (archive.Writer).
type Archive interface {
	Add(records []shard.Record, urgent bool, settled func()) (*archive.Ticket, error)
}

// Errors [Pipeline.Ingest] returns.
var (
	// ErrOverloaded: the process already holds MaxInFlightBytes of
	// unconfirmed events. The client should retry shortly (429).
	ErrOverloaded = errors.New("pipeline: too much unconfirmed data in flight")
	// ErrNoSink: neither sink made the batch durable. The client should
	// retry (503). The error wraps what each sink said.
	ErrNoSink = errors.New("pipeline: no sink accepted the batch")
)

// Reasons Kafka is not offered an event (the `reason` label of
// ingest_kafka_skipped_events_total).
var (
	errKafkaDisabled = errors.New("kafka: not configured")
	errKafkaOpen     = errors.New("kafka: unavailable (circuit open)")
	errKafkaBusy     = errors.New("kafka: too many sends in flight")
)

// Config tunes a [Pipeline].
type Config struct {
	// Topic the events are published to ("" = the producer's default).
	Topic string
	// MaxInFlightBytes bounds the event bytes accepted but not yet settled
	// by both sinks.
	MaxInFlightBytes int64
	// KafkaAckGrace is how long a request waits for Kafka alone before it
	// also asks the archive to hurry.
	KafkaAckGrace time.Duration
	// KafkaMaxInFlight bounds the sends in flight: while Kafka hangs each
	// one holds an event until the publish timeout.
	KafkaMaxInFlight int
}

// Pipeline fans events out to the sinks. It is safe for concurrent use.
type Pipeline struct {
	cfg     Config
	pub     Publisher // nil: Kafka is not configured
	archive Archive
	breaker *resilience.Breaker // Kafka's; nil when pub is nil
	log     *slog.Logger
	metrics *metrics
	now     func() time.Time

	inFlight   atomic.Int64
	kafkaSlots chan struct{}
	sends      sync.WaitGroup
}

// New builds a pipeline. pub may be nil (no Kafka: every request waits for
// the archive), and then breaker is unused.
func New(cfg Config, pub Publisher, arch Archive, breaker *resilience.Breaker, log *slog.Logger) (*Pipeline, error) {
	if cfg.MaxInFlightBytes < 1 || cfg.KafkaAckGrace <= 0 || cfg.KafkaMaxInFlight < 1 {
		return nil, fmt.Errorf("pipeline: config: in-flight bytes, kafka ack grace and kafka in-flight must be positive: %+v", cfg)
	}

	if pub != nil && breaker == nil {
		return nil, errors.New("pipeline: a publisher needs a breaker")
	}

	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	p := &Pipeline{
		cfg: cfg, pub: pub, archive: arch, breaker: breaker, log: log, now: time.Now,
		kafkaSlots: make(chan struct{}, cfg.KafkaMaxInFlight),
	}
	p.metrics = newMetrics(p)

	return p, nil
}

// request is the bookkeeping of one Ingest call that outlives it: the sinks
// settle after the client has its answer, and only then are the bytes given
// back to the in-flight budget.
type request struct {
	pipeline *Pipeline
	size     int64
	pending  atomic.Int32
}

func (r *request) settle() {
	if r.pending.Add(-1) == 0 {
		r.pipeline.inFlight.Add(-r.size)
	}
}

// Ingest offers events to both sinks and returns nil as soon as one of them
// has made all of them durable. On [ErrNoSink] or a ctx error some events may
// nevertheless have been stored: a retry can duplicate, never lose.
//
// The sinks keep working after Ingest returns — the second copy is still
// written — detached from ctx.
func (p *Pipeline) Ingest(ctx context.Context, events []Event) error {
	var size int64
	for i := range events {
		size += int64(len(events[i].Raw))
	}

	if !p.reserve(size) {
		p.metrics.requests.WithLabelValues(outcomeOverloaded).Inc()
		return ErrOverloaded
	}

	req := &request{pipeline: p, size: size}
	req.pending.Store(2) // one per sink; each settles exactly once

	// Kafka first: whether it takes the events decides how urgent the
	// archive batch is.
	kafkaDone, kafkaErr := p.publish(ctx, req, events)

	now := p.now()
	records := make([]shard.Record, len(events))

	for i := range events {
		records[i] = shard.Record{ID: events[i].ID, Type: events[i].Type, ReceivedAt: now, Event: events[i].Raw}
	}

	ticket, archiveErr := p.archive.Add(records, kafkaErr != nil, req.settle)
	if archiveErr != nil {
		req.settle()
	}

	return p.await(ctx, kafkaDone, kafkaErr, ticket, archiveErr)
}

// await waits for the first sink to confirm, or for both to fail.
func (p *Pipeline) await(ctx context.Context, kafkaDone <-chan error, kafkaErr error, ticket *archive.Ticket, archiveErr error) error {
	var archiveDone <-chan error
	if ticket != nil {
		archiveDone = ticket.Done()
	}

	if kafkaErr != nil {
		kafkaDone = nil
	}

	grace := time.NewTimer(p.cfg.KafkaAckGrace)
	defer grace.Stop()

	for {
		if kafkaDone == nil && archiveDone == nil {
			p.metrics.requests.WithLabelValues(outcomeNoSink).Inc()
			return fmt.Errorf("%w: %w; %w", ErrNoSink, kafkaErr, archiveErr)
		}

		select {
		case err := <-kafkaDone:
			if err == nil {
				p.metrics.requests.WithLabelValues(outcomeKafka).Inc()
				return nil
			}

			kafkaErr, kafkaDone = err, nil

			if ticket != nil {
				ticket.Expedite()
			}
		case err := <-archiveDone:
			if err == nil {
				p.metrics.requests.WithLabelValues(outcomeArchive).Inc()
				return nil
			}

			archiveErr, archiveDone = err, nil
		case <-grace.C:
			// Kafka is slow, not (yet) failed: stop betting on it alone.
			if ticket != nil && archiveDone != nil {
				ticket.Expedite()
			}
		case <-ctx.Done():
			p.metrics.requests.WithLabelValues(outcomeCanceled).Inc()
			return ctx.Err()
		}
	}
}

// reserve takes size bytes of the in-flight budget, or reports false.
func (p *Pipeline) reserve(size int64) bool {
	for {
		cur := p.inFlight.Load()
		if cur+size > p.cfg.MaxInFlightBytes {
			return false
		}

		if p.inFlight.CompareAndSwap(cur, cur+size) {
			return true
		}
	}
}

// publish sends every event to Kafka, each in its own goroutine (a send
// blocks until the brokers acknowledge; the client batches concurrent sends),
// and returns a channel that yields once: nil when all were acknowledged,
// the first error otherwise. When Kafka is not offered the events at all it
// returns the reason instead, and has already settled the request's Kafka
// half.
func (p *Pipeline) publish(ctx context.Context, req *request, events []Event) (<-chan error, error) {
	skip := func(reason string, err error) (<-chan error, error) {
		p.metrics.kafkaSkipped.WithLabelValues(reason).Add(float64(len(events)))
		req.settle()

		return nil, err
	}

	if p.pub == nil {
		req.settle()
		return nil, errKafkaDisabled
	}

	admission, ok := p.breaker.Allow()
	if !ok {
		return skip(reasonCircuitOpen, errKafkaOpen)
	}

	if !p.takeSlots(len(events)) {
		p.breaker.Record(admission, resilience.OutcomeIgnored) // says nothing new about the brokers
		return skip(reasonBusy, errKafkaBusy)
	}

	// The sends outlive the request: once the archive has answered the
	// client, Kafka still gets its copy. Keep the trace, drop the deadline —
	// libs/kafka bounds every send by KAFKA_PUBLISH_TIMEOUT.
	sendCtx := context.WithoutCancel(ctx)
	done := make(chan error, 1)
	results := make(chan error, len(events))

	for i := range events {
		event := &events[i]

		p.sends.Go(func() {
			results <- p.pub.Send(sendCtx, message(p.cfg.Topic, event))
		})
	}

	p.sends.Go(func() {
		var (
			first    error
			rejected int
		)

		for range events {
			err := <-results
			if err == nil {
				continue
			}

			if kafka.Rejected(err) {
				rejected++
			}

			if first == nil {
				first = err
			}
		}

		p.releaseSlots(len(events))

		switch {
		case first == nil:
			p.breaker.Record(admission, resilience.OutcomeSuccess)
		case rejected > 0:
			// The brokers answered: these records are refused for what they
			// are (too large for the topic), which no outage explains and no
			// retry fixes. The archive still takes them.
			p.breaker.Record(admission, resilience.OutcomeIgnored)
			p.log.WarnContext(sendCtx, "kafka rejected events: they are in the archive only — check KAFKA_MESSAGE_MAX_BYTES against MAX_EVENT_BYTES",
				"rejected", rejected, "events", len(events), "err", first)
		default:
			p.breaker.Record(admission, resilience.OutcomeFailure)
		}

		done <- first

		req.settle()
	})

	return done, nil
}

func message(topic string, event *Event) kafka.Message {
	headers := []kafka.Header{
		{Key: kafka.HeaderEventID, Value: []byte(event.ID)},
		{Key: kafka.HeaderContentType, Value: []byte("application/json")},
	}
	if event.Type != "" {
		headers = append(headers, kafka.Header{Key: kafka.HeaderEventType, Value: []byte(event.Type)})
	}

	return kafka.Message{Topic: topic, Key: []byte(event.ID), Value: event.Raw, Headers: headers}
}

// takeSlots takes n send slots or none.
func (p *Pipeline) takeSlots(n int) bool {
	for taken := range n {
		select {
		case p.kafkaSlots <- struct{}{}:
		default:
			p.releaseSlots(taken)
			return false
		}
	}

	return true
}

func (p *Pipeline) releaseSlots(n int) {
	for range n {
		<-p.kafkaSlots
	}
}

// Drain waits until every Kafka send in flight has ended, or ctx does. Call
// it after the HTTP server has drained and before the producer is closed.
// It reports how many event bytes were still unsettled when it returned.
func (p *Pipeline) Drain(ctx context.Context) (unsettledBytes int64) {
	finished := make(chan struct{})

	go func() {
		p.sends.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-ctx.Done():
	}

	return p.inFlight.Load()
}

// ReadyCheck is the service's critical readiness check: it fails only when
// no sink can take an event — Kafka's circuit is open (or there is no Kafka)
// and archiveOpen reports the archive's is too. One sink down is a degraded
// service, not an unready one.
func (p *Pipeline) ReadyCheck(archiveOpen func() bool) func(context.Context) error {
	return func(context.Context) error {
		kafkaDown := p.pub == nil || p.breaker.State() == resilience.StateOpen
		if kafkaDown && archiveOpen() {
			return errors.New("no sink available: the kafka and archive circuits are both open")
		}

		return nil
	}
}
