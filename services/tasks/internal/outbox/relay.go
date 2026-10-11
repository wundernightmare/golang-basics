package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used -- poll-backoff jitter is not security-sensitive; a CSPRNG (crypto/rand) is unnecessary here.
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	pgxlib "github.com/tracehubmmp/golang-basics/libs/pgx"
)

// Publisher sends one record (satisfied by libs/kafka.Producer).
type Publisher interface {
	Send(ctx context.Context, msg kafka.Message) error
}

// Config tunes a [Relay]. Zero fields take the defaults.
type Config struct {
	Topic        string        // Kafka topic every event goes to
	PollInterval time.Duration // idle poll period (default 1s); Wake cuts it short
	BatchSize    int           // rows per batch (default 100)
	MaxBackoff   time.Duration // cap on the retry delay after a failed batch (default 30s)
	MaxAttempts  int           // rejections after which a row is dead: kept, no longer sent (default 10)
}

func (c *Config) withDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
}

// Relay publishes pending outbox rows to Kafka. Construct it with [NewRelay],
// start [Relay.Run] beside the HTTP server, and call [Relay.Wake] after a
// commit that enqueued an event so it is relayed at once rather than on the
// next poll.
type Relay struct {
	pool    *pgxpool.Pool
	pub     Publisher
	cfg     Config
	log     *slog.Logger
	wake    chan struct{}
	tracer  trace.Tracer
	metrics relayMetrics
}

type relayMetrics struct {
	pending   prometheus.Gauge
	dead      prometheus.Gauge
	lag       prometheus.Gauge
	published *prometheus.CounterVec
}

// NewRelay builds a relay over pool (the service's database, schema migrated)
// publishing through pub.
func NewRelay(pool *pgxpool.Pool, pub Publisher, cfg Config, log *slog.Logger) *Relay {
	cfg.withDefaults()
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Relay{
		pool: pool, pub: pub, cfg: cfg, log: log,
		wake:   make(chan struct{}, 1),
		tracer: otel.Tracer("github.com/tracehubmmp/golang-basics/services/tasks/internal/outbox"),
		metrics: relayMetrics{
			pending: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "outbox_pending",
				Help: "Outbox events not yet published to Kafka (as of the relay's last pass).",
			}),
			dead: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "outbox_dead",
				Help: "Outbox events the broker rejected OUTBOX_MAX_ATTEMPTS times; kept for an operator, no longer sent.",
			}),
			lag: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "outbox_relay_lag_seconds",
				Help: "Age of the oldest unpublished outbox event in seconds (0 when the outbox is empty).",
			}),
			published: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "outbox_published_total",
				Help: "Outbox events the relay tried to publish, by result (ok, rejected, error).",
			}, []string{"result"}),
		},
	}
}

// Collectors returns the relay's metrics for the service's registry:
//
//	outbox_pending                   backlog size (rows still being sent)
//	outbox_dead                      rows given up on (rejected OUTBOX_MAX_ATTEMPTS times)
//	outbox_relay_lag_seconds         age of the oldest pending event
//	outbox_published_total{result}   publish attempts, ok | rejected | error
//
// Alert on the lag, not on the count: a steady backlog that moves is fine, an
// event older than a minute means the broker (or the relay) is stuck. Any
// dead row needs a person: fix and re-arm it (UPDATE outbox SET attempts = 0)
// or delete it.
func (r *Relay) Collectors() []prometheus.Collector {
	return []prometheus.Collector{r.metrics.pending, r.metrics.dead, r.metrics.lag, r.metrics.published}
}

// Wake asks the relay to run a pass now. It never blocks.
func (r *Relay) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run relays until ctx is cancelled, then returns nil. After a failed batch
// it backs off exponentially (with jitter, capped at MaxBackoff) and ignores
// Wake until the delay is over — a broker outage is not hammered by every
// request. A full batch is followed immediately by the next one.
func (r *Relay) Run(ctx context.Context) error {
	r.log.InfoContext(ctx, "outbox relay started", "topic", r.cfg.Topic,
		"poll_interval", r.cfg.PollInterval.String(), "batch_size", r.cfg.BatchSize)
	failures := 0
	for {
		n, err := r.RelayOnce(ctx)
		r.refreshGauges(ctx)
		var wait time.Duration
		switch {
		case err != nil:
			failures++
			wait = backoff(failures, r.cfg.PollInterval, r.cfg.MaxBackoff)
			r.log.WarnContext(ctx, "outbox relay pass failed, backing off",
				"err", err, "failures", failures, "retry_in", wait.String())
		case n == r.cfg.BatchSize:
			failures = 0
			wait = 0 // more is waiting
		default:
			failures = 0
			wait = r.cfg.PollInterval
		}
		if wait == 0 {
			select {
			case <-ctx.Done():
				r.log.InfoContext(ctx, "outbox relay stopped")
				return nil
			default:
				continue
			}
		}
		wake := r.wake
		if err != nil {
			wake = nil // back off for real
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			r.log.InfoContext(ctx, "outbox relay stopped")
			return nil
		case <-t.C:
		case <-wake:
			t.Stop()
		}
	}
}

// backoff is interval·2^(n-1) capped at maxDelay, with "equal jitter".
func backoff(n int, interval, maxDelay time.Duration) time.Duration {
	d := interval
	for i := 1; i < n && d < maxDelay; i++ {
		d *= 2
	}
	d = min(d, maxDelay)
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half+1) //nolint:gosec // jitter, not a secret
}

// relayLockSQL makes one relay (across all replicas) the active one for a
// batch; the others skip the pass. Keyed by schema so isolated schemas in one
// database do not contend.
const relayLockSQL = `SELECT pg_try_advisory_xact_lock(hashtextextended('tasks/outbox.relay:' || coalesce(current_schema(), ''), 0))`

type pending struct {
	id          int64
	eventID     string
	aggregateID string
	eventType   string
	payload     []byte
	headers     map[string]string
}

// batchBudget bounds one pass so shutdown (and a hung broker) cannot hold it
// open indefinitely; the per-record publish timeout is libs/kafka's.
const batchBudget = 30 * time.Second

// RelayOnce publishes one batch of pending events and deletes the rows the
// broker acknowledged, returning how many it published. A failed publish is
// judged by [kafka.Rejected]:
//
//   - an outage (broker down, timeout, topic not provisioned, auth): the
//     batch stops there so events keep their order, the row records
//     last_error, and the error is returned (the caller backs off). Never
//     counted: no outage can make a row dead.
//   - a rejection (too large, malformed — the record itself is the problem):
//     attempts + 1 and last_error, and the batch goes on without it. After
//     MaxAttempts the row is dead: left in the table, skipped by every pass.
//     A rejected row gives up its order relative to later events of its task.
//
// Another replica holding the relay lock is not an error: the pass publishes
// nothing.
//
// The pass runs detached from ctx's cancellation (bounded by its own budget)
// so a shutdown lets it finish and delete what it published instead of
// leaving acknowledged rows to be published twice; it stops early, between
// records, once ctx is done.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchBudget)
	defer cancel()
	bctx = pgxlib.WithOperation(bctx, "outbox.relay")

	tx, err := r.pool.Begin(bctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(bctx)) }() // no-op after commit

	var leader bool
	if err := tx.QueryRow(bctx, relayLockSQL).Scan(&leader); err != nil {
		return 0, fmt.Errorf("outbox: relay lock: %w", err)
	}
	if !leader {
		return 0, nil
	}

	rows, err := tx.Query(bctx,
		`SELECT id, event_id::text, aggregate_id, event_type, payload, headers
		   FROM outbox WHERE attempts < $2 ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, r.cfg.BatchSize, r.cfg.MaxAttempts)
	if err != nil {
		return 0, fmt.Errorf("outbox: select batch: %w", err)
	}
	batch, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (pending, error) {
		var x pending
		var headers []byte
		err := r.Scan(&x.id, &x.eventID, &x.aggregateID, &x.eventType, &x.payload, &headers)
		if err == nil && len(headers) > 0 {
			err = json.Unmarshal(headers, &x.headers)
		}
		return x, err
	})
	if err != nil {
		return 0, fmt.Errorf("outbox: read batch: %w", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}

	sent := make([]int64, 0, len(batch))
	var pubErr error
	for _, x := range batch {
		if ctx.Err() != nil {
			break // shutting down: commit what is done
		}
		if err := r.publish(bctx, x); err != nil {
			if kafka.Rejected(err) {
				r.metrics.published.WithLabelValues("rejected").Inc()
				var attempts int
				if uerr := tx.QueryRow(bctx, `UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1 RETURNING attempts`,
					x.id, truncate(err.Error(), 1000)).Scan(&attempts); uerr != nil {
					pubErr = fmt.Errorf("outbox: record rejection: %w", uerr)
					break
				}
				r.log.WarnContext(ctx, "outbox event rejected by the broker", "event_id", x.eventID,
					"event_type", x.eventType, "attempts", attempts, "max_attempts", r.cfg.MaxAttempts,
					"dead", attempts >= r.cfg.MaxAttempts, "err", err)
				continue // the record is the problem, not the broker: the rest can go
			}
			r.metrics.published.WithLabelValues("error").Inc()
			pubErr = fmt.Errorf("outbox: publish %s %s (event %s): %w", x.eventType, x.aggregateID, x.eventID, err)
			if _, uerr := tx.Exec(bctx, `UPDATE outbox SET last_error = $2 WHERE id = $1`,
				x.id, truncate(err.Error(), 1000)); uerr != nil {
				pubErr = errors.Join(pubErr, fmt.Errorf("outbox: record failure: %w", uerr))
			}
			break
		}
		r.metrics.published.WithLabelValues("ok").Inc()
		sent = append(sent, x.id)
	}

	if len(sent) > 0 {
		if _, err := tx.Exec(bctx, `DELETE FROM outbox WHERE id = ANY($1)`, sent); err != nil {
			return 0, errors.Join(pubErr, fmt.Errorf("outbox: delete published: %w", err))
		}
	}
	if err := tx.Commit(bctx); err != nil {
		return 0, errors.Join(pubErr, fmt.Errorf("outbox: commit: %w", err))
	}
	return len(sent), pubErr
}

// publish sends one row, continuing the trace of the request that wrote it.
func (r *Relay) publish(ctx context.Context, x pending) error {
	parent := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(x.headers))
	ctx, span := r.tracer.Start(parent, "outbox relay "+x.eventType,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("messaging.message.id", x.eventID),
			attribute.String("messaging.destination.name", r.cfg.Topic),
			attribute.String("tasks.event_type", x.eventType),
		))
	defer span.End()

	headers := []kafka.Header{
		{Key: kafka.HeaderEventID, Value: []byte(x.eventID)},
		{Key: kafka.HeaderEventType, Value: []byte(x.eventType)},
		{Key: kafka.HeaderContentType, Value: []byte("application/json")},
	}
	// The trace context stored at insert time. With tracing on, the producer
	// replaces traceparent with its own span — a child in the same trace.
	for _, k := range []string{kafka.HeaderTraceparent, "tracestate"} {
		if v := x.headers[k]; v != "" {
			headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
		}
	}
	if err := r.pub.Send(ctx, kafka.Message{Topic: r.cfg.Topic, Key: []byte(x.aggregateID), Value: x.payload, Headers: headers}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		return err
	}
	return nil
}

// statsSQL counts pending and dead rows and the age of the oldest pending one.
const statsSQL = `SELECT count(*) FILTER (WHERE attempts < $1),
       count(*) FILTER (WHERE attempts >= $1),
       coalesce(extract(epoch FROM now() - min(created_at) FILTER (WHERE attempts < $1)), 0)::float8
  FROM outbox`

// refreshGauges updates outbox_pending, outbox_dead and outbox_relay_lag_seconds.
func (r *Relay) refreshGauges(ctx context.Context) {
	qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var n, dead int64
	var lag float64
	err := r.pool.QueryRow(pgxlib.WithOperation(qctx, "outbox.stats"), statsSQL, r.cfg.MaxAttempts).Scan(&n, &dead, &lag)
	if err != nil {
		r.log.DebugContext(ctx, "outbox stats unavailable", "err", err)
		return
	}
	r.metrics.pending.Set(float64(n))
	r.metrics.dead.Set(float64(dead))
	r.metrics.lag.Set(max(lag, 0))
}

// Stats returns the backlog size (dead rows aside) and the age of its oldest
// event, for tests and tooling.
func (r *Relay) Stats(ctx context.Context) (pending int64, oldest time.Duration, err error) {
	var dead int64
	var lag float64
	err = r.pool.QueryRow(ctx, statsSQL, r.cfg.MaxAttempts).Scan(&pending, &dead, &lag)
	return pending, time.Duration(lag * float64(time.Second)), err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") // never cut a rune in half: TEXT must be valid UTF-8
}
