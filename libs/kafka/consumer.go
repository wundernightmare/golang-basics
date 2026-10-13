package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// errDraining reports that the drain budget ran out (or the loop was told to
// stop) before a record finished: it is left uncommitted for redelivery and
// is not the record's fault.
var errDraining = errors.New("kafka: consumer draining")

// maxDLQErrorBytes bounds the dlq_error header: an error string can embed
// anything, and a header is not the place for a stack dump.
const maxDLQErrorBytes = 1024

// fetchWarnEvery rate-limits the "fetch error" log: a broker outage would
// otherwise log one per poll.
const fetchWarnEvery = time.Minute

// Consumer drains a consumer group with a poll / process / commit loop. It
// is the "background worker" shape of this monorepo, fed by a broker instead
// of a ticker. The loop — commit after handling, retries, [Permanent],
// dead-lettering, drain — is the same whichever provider is linked (see
// [Driver]); the provider only polls, commits and reports group changes.
// Register [Consumer.Collectors] for throughput, retries, dead-lettering,
// commits and rebalances (the group's lag is the broker's to export).
//
// A Consumer is single-use: [Consumer.Run] closes it when it returns.
type Consumer struct {
	cl      ConsumerClient
	cfg     Config
	log     *slog.Logger
	metrics *consumerMetrics

	mu      sync.Mutex
	pending map[TopicPartition]int64 // processed, not yet committed: the latest offset per partition
	owned   map[TopicPartition]struct{}

	running       atomic.Bool
	closeOnce     sync.Once
	lastFetchWarn atomic.Int64 // unix nanos of the last fetch-error log
}

// NewConsumer builds a consumer-group member subscribed to cfg.Topics with
// the linked provider and verifies broker connectivity. Automatic commits
// are off: offsets advance only past records a [Handler] has accepted or
// that were parked on the dead-letter topic. The caller owns it and must
// call [Consumer.Close] if it never calls [Consumer.Run].
func NewConsumer(ctx context.Context, cfg Config, log *slog.Logger) (*Consumer, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	d, err := mustDriver()
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: consumer config: %w", err)
	}
	if len(cfg.Topics) == 0 || cfg.Group == "" {
		return nil, errors.New("kafka: consumer needs KAFKA_TOPICS (or KAFKA_TOPIC) and KAFKA_GROUP")
	}
	c := &Consumer{
		cfg:     cfg,
		log:     log,
		pending: map[TopicPartition]int64{},
		owned:   map[TopicPartition]struct{}{},
	}
	cl, err := d.NewConsumer(cfg, log, Rebalance{Assigned: c.onAssigned, Revoked: c.onRevoked, Lost: c.onLost})
	if err != nil {
		return nil, fmt.Errorf("kafka: new %s consumer: %w", d.Name(), err)
	}
	c.cl = cl
	c.metrics = newConsumerMetrics(cl.Stats, d.Name(), cfg.Topics)

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka: consumer: brokers unreachable: %w", err)
	}
	warnPlaintextSASL(ctx, cfg, log)

	dlq := cfg.DeadLetterTopic
	if cfg.deadLetterTopic("x") == "" {
		dlq = "disabled"
	}
	log.InfoContext(ctx, "kafka consumer ready", "provider", d.Name(), "brokers", cfg.brokersString(),
		"group", cfg.Group, "topics", cfg.Topics, "assignment_strategy", cfg.AssignmentStrategy,
		"max_retries", cfg.maxRetries(), "dead_letter_topic", dlq, "queue_max_kbytes", cfg.ConsumerQueueMaxKBytes,
		"security_protocol", cfg.SecurityProtocol, "sasl", cfg.SASLMechanism)
	return c, nil
}

// Run polls and dispatches records to handler until ctx is cancelled (a clean
// shutdown, returning nil) or a record can be neither handled nor parked
// (returning that error). It closes the consumer before returning.
//
// Per record: the handler runs with a per-attempt deadline
// (HandlerTimeout); an error is retried MaxRetries times with jittered
// exponential backoff unless it is [Permanent]. A record that still fails is
// published to the dead-letter topic with dlq_* headers and committed, so
// one poison record costs its retries and nothing else. With the DLQ
// disabled ("none") it is not committed and Run returns the error instead:
// the process stops and the record is redelivered on restart — the "poison
// must be visible" mode, for streams where skipping is never acceptable.
//
// Batches: up to MaxPollRecords records are processed in partition order,
// then everything processed is committed in one request (offset + 1 per
// partition), then the group may rebalance. On shutdown the in-flight batch
// gets DrainTimeout to finish; what finished is committed (with a context
// detached from ctx), what did not is redelivered to the next owner.
// Delivery is at-least-once: a handler must tolerate seeing a record twice
// (see services/consumer's dedupe for the pattern).
//
// Retries happen inside the loop, so one record's retries plus the rest of
// its batch must fit in KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS, or the member
// is considered stuck and its partitions move (the records are then
// redelivered to the new owner).
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("kafka: Consumer.Run called twice")
	}
	defer c.Close()
	c.log.InfoContext(ctx, "kafka consumer loop started", "group", c.cfg.Group, "topics", c.cfg.Topics)

	// Handlers run under procCtx, which outlives ctx by DrainTimeout so the
	// in-flight batch can finish; its values (logger fields, etc.) are ctx's.
	procCtx, cancelProc := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelProc()
	var drain atomic.Pointer[time.Timer]
	stopDrain := context.AfterFunc(ctx, func() { drain.Store(time.AfterFunc(c.cfg.DrainTimeout, cancelProc)) })
	defer func() {
		stopDrain()
		if t := drain.Load(); t != nil {
			t.Stop()
		}
	}()

	for {
		if ctx.Err() != nil {
			c.log.InfoContext(ctx, "kafka consumer loop stopping")
			return nil
		}
		records, err := c.cl.Poll(ctx, c.cfg.MaxPollRecords)
		if errors.Is(err, ErrClientClosed) {
			return nil
		}
		if errors.Is(err, ErrClientFailed) {
			// Nothing of this poll is processed: the records go to the
			// group's next owner from the last commit.
			c.log.ErrorContext(ctx, "kafka consumer client failed; stopping the loop", "err", err)
			return err
		}
		if err != nil && ctx.Err() == nil {
			c.fetchError(ctx, err)
		}
		if ctx.Err() != nil {
			// Shutdown arrived with the poll: nothing of this batch has
			// started, so leave all of it for the next owner.
			c.cl.AllowRebalance()
			c.log.InfoContext(ctx, "kafka consumer loop stopping")
			return nil
		}

		err = c.processBatch(procCtx, records, handler)
		c.commitPending(ctx)
		c.cl.AllowRebalance()
		switch {
		case errors.Is(err, errDraining):
			c.log.WarnContext(ctx, "kafka consumer drain budget exhausted; unfinished records will be redelivered",
				"drain_timeout", c.cfg.DrainTimeout)
			return nil
		case err != nil:
			c.log.ErrorContext(ctx, "kafka consumer stopping on a record it could not handle or park", "err", err)
			return err
		}
	}
}

// fetchError counts a fetch error and logs it at most once per
// fetchWarnEvery. Warn, not error: the client keeps retrying on its own
// (a broker restarting, a subscribed topic not created yet), and
// kafka_consumer_fetch_errors_total is what an alert watches.
func (c *Consumer) fetchError(ctx context.Context, err error) {
	c.metrics.fetchErrs.Inc()
	now := time.Now().UnixNano()
	if last := c.lastFetchWarn.Load(); now-last >= int64(fetchWarnEvery) && c.lastFetchWarn.CompareAndSwap(last, now) {
		c.log.WarnContext(ctx, "kafka fetch error; the client retries (further ones within a minute are counted, not logged)", "err", err)
	}
}

// processBatch handles records in order, marking each finished one for
// commit. It stops at the first record that neither succeeded nor was
// parked, so what is marked is always a per-partition prefix.
func (c *Consumer) processBatch(ctx context.Context, records []Message, handler Handler) error {
	for _, r := range records {
		if ctx.Err() != nil {
			return errDraining
		}
		if !c.owns(r) {
			continue // its partition moved during the poll: the new owner gets it
		}
		if err := c.process(ctx, r, handler); err != nil {
			return err
		}
		c.markDone(r)
	}
	return nil
}

func (c *Consumer) owns(r Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.owned[TopicPartition{r.Topic, r.Partition}]
	return ok
}

// process runs one record through the handler with retries, then the DLQ.
// nil means "done, commit it".
func (c *Consumer) process(ctx context.Context, r Message, handler Handler) error {
	reqID := requestID(r)
	// logCtx: the record's trace (continued from its headers) and request
	// id, for the loop's own log lines about this record.
	logCtx := httpx.WithRequestID(recordContext(context.WithoutCancel(ctx), r), reqID)
	maxRetries := c.cfg.maxRetries()
	var err error
	attempt := 1
	for ; ; attempt++ {
		err = c.invoke(ctx, r, handler, attempt, reqID)
		if err == nil {
			result := consumeOK
			if attempt > 1 {
				result = consumeRetried
			}
			c.metrics.records.WithLabelValues(c.metrics.topic(r.Topic), result).Inc()
			return nil
		}
		if ctx.Err() != nil {
			return errDraining
		}
		if IsPermanent(err) || attempt > maxRetries {
			break
		}
		wait := backoff(attempt-1, c.cfg.RetryBackoff, c.cfg.RetryBackoffMax)
		c.log.WarnContext(logCtx, "kafka handler failed; retrying",
			"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "attempt", attempt, "backoff", wait, "err", err)
		if !sleep(ctx, wait) {
			return errDraining
		}
	}

	reason := "retries_exhausted"
	if IsPermanent(err) {
		reason = "permanent"
	}
	dlqTopic := c.cfg.deadLetterTopic(r.Topic)
	if dlqTopic == "" {
		c.metrics.records.WithLabelValues(c.metrics.topic(r.Topic), consumeFailed).Inc()
		return fmt.Errorf("kafka: record %s/%d@%d failed after %d attempt(s) (%s) and the dead-letter topic is disabled: %w",
			r.Topic, r.Partition, r.Offset, attempt, reason, err)
	}
	if dlqErr := c.deadLetter(ctx, logCtx, r, dlqTopic, reason, err, attempt); dlqErr != nil {
		if errors.Is(dlqErr, errDraining) {
			return errDraining
		}
		c.metrics.records.WithLabelValues(c.metrics.topic(r.Topic), consumeFailed).Inc()
		return fmt.Errorf("kafka: record %s/%d@%d failed and could not be dead-lettered to %q: %w",
			r.Topic, r.Partition, r.Offset, dlqTopic, errors.Join(err, dlqErr))
	}
	c.metrics.records.WithLabelValues(c.metrics.topic(r.Topic), consumeDeadLettered).Inc()
	return nil
}

// recordContext is ctx with the trace context the record's headers carry as
// the remote parent.
func recordContext(ctx context.Context, r Message) context.Context {
	hs := r.Headers
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{&hs})
}

// invoke runs one handler attempt inside a process span continued from the
// record's trace, with the request id and the per-attempt deadline, and
// turns a panic into an error (retried like any other).
func (c *Consumer) invoke(ctx context.Context, r Message, handler Handler, attempt int, reqID string) (err error) {
	// The span's parent comes from the record, its cancellation from the
	// loop (the drain deadline): ctx's values are kept, its span replaced.
	rctx, span := tracer().Start(recordContext(ctx, r), "process "+r.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.operation.name", "process"),
			attribute.String("messaging.destination.name", r.Topic),
			attribute.String("messaging.consumer.group.name", c.cfg.Group),
			attribute.Int("messaging.destination.partition.id", int(r.Partition)),
			attribute.Int64("messaging.kafka.offset", r.Offset),
			attribute.Int("messaging.kafka.attempt", attempt),
		))
	defer span.End()
	rctx, cancel := context.WithTimeout(rctx, c.cfg.HandlerTimeout)
	defer cancel()
	rctx = httpx.WithRequestID(rctx, reqID)

	start := time.Now()
	defer func() {
		if p := recover(); p != nil {
			c.log.ErrorContext(rctx, "kafka handler panicked", "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			err = fmt.Errorf("kafka: handler panic: %v", p)
		}
		c.metrics.duration.WithLabelValues(c.metrics.topic(r.Topic)).Observe(time.Since(start).Seconds())
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}()
	r.Attempt = attempt
	return handler(rctx, r)
}

// requestID is the record's event_id header when it is a usable correlation
// id (see httpx.ValidRequestID), a fresh id otherwise.
func requestID(r Message) string {
	if id, ok := headerValue(r.Headers, HeaderEventID); ok && httpx.ValidRequestID(id) {
		return id
	}
	return httpx.NewRequestID()
}

// deadLetter publishes r to topic with its original headers plus the dlq_*
// ones, retrying the publish like a handler. The publish span continues the
// record's trace.
func (c *Consumer) deadLetter(ctx, logCtx context.Context, r Message, topic, reason string, cause error, attempts int) error {
	errText := cause.Error()
	if len(errText) > maxDLQErrorBytes {
		errText = errText[:maxDLQErrorBytes]
	}
	headers := make([]Header, 0, len(r.Headers)+6)
	for _, h := range r.Headers {
		switch h.Key {
		case HeaderDLQReason, HeaderDLQError, HeaderDLQSourceTopic, HeaderDLQSourcePartition, HeaderDLQSourceOffset, HeaderDLQAttempts:
			continue // a record re-parked from a DLQ replay gets fresh ones
		}
		headers = append(headers, h)
	}
	headers = append(headers,
		Header{Key: HeaderDLQReason, Value: []byte(reason)},
		Header{Key: HeaderDLQError, Value: []byte(errText)},
		Header{Key: HeaderDLQSourceTopic, Value: []byte(r.Topic)},
		Header{Key: HeaderDLQSourcePartition, Value: []byte(strconv.Itoa(int(r.Partition)))},
		Header{Key: HeaderDLQSourceOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
		Header{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
	)

	msg := Message{Topic: topic, Key: r.Key, Value: r.Value, Headers: headers}
	for n := 0; ; n++ {
		// The record's traceparent travels along: the dead-letter publish
		// continues the record's trace.
		start := time.Now()
		err := send(ctx, c.cl.DeadLetter(), c.cfg.PublishTimeout, msg)
		c.metrics.dlq.observe(topic, start, err)
		if err == nil {
			c.log.WarnContext(logCtx, "kafka record dead-lettered",
				"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "dead_letter_topic", topic,
				"reason", reason, "attempts", attempts, "err", cause)
			return nil
		}
		if ctx.Err() != nil {
			return errDraining
		}
		if n >= c.cfg.maxRetries() {
			return err
		}
		wait := backoff(n, c.cfg.RetryBackoff, c.cfg.RetryBackoffMax)
		c.log.WarnContext(logCtx, "kafka dead-letter publish failed; retrying", "dead_letter_topic", topic, "backoff", wait, "err", err)
		if !sleep(ctx, wait) {
			return errDraining
		}
	}
}

// markDone records r as processed; the next commit covers it.
func (c *Consumer) markDone(r Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending[TopicPartition{r.Topic, r.Partition}] = r.Offset
}

// commitPending commits every processed record, with a context detached from
// ctx (a shutdown must still commit what it finished) and bounded by
// DrainTimeout. A failure is counted and logged and the records stay
// pending: the next commit (or the revoke callback) retries them, and if the
// process dies first they are redelivered — at-least-once either way.
func (c *Consumer) commitPending(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commitLocked(ctx, nil)
}

// commitLocked commits the pending offsets of the given partitions (all of
// them when only is nil) and forgets the ones committed. The committed
// position is the next offset to read: last processed + 1. Only partitions
// this member owns are committed: an offset left over for one that moved
// (a record finished after its revoke) is dropped — committing it would
// rewind the new owner's position, which the broker accepts. c.mu must be
// held.
func (c *Consumer) commitLocked(ctx context.Context, only []TopicPartition) {
	var offsets []Offset
	for tp, off := range c.pending {
		if _, ok := c.owned[tp]; !ok {
			delete(c.pending, tp)
			continue
		}
		if only == nil || containsPartition(only, tp) {
			offsets = append(offsets, Offset{TopicPartition: tp, Next: off + 1})
		}
	}
	if len(offsets) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.DrainTimeout)
	defer cancel()
	if err := c.cl.Commit(cctx, offsets); err != nil {
		c.metrics.commitErrs.Inc()
		c.log.ErrorContext(ctx, "kafka offset commit failed; records stay pending", "partitions", len(offsets), "err", err)
		return
	}
	for _, o := range offsets {
		if c.pending[o.TopicPartition] == o.Next-1 {
			delete(c.pending, o.TopicPartition)
		}
	}
}

func containsPartition(ps []TopicPartition, p TopicPartition) bool {
	for _, q := range ps {
		if q == p {
			return true
		}
	}
	return false
}

// onAssigned tracks the partitions this member now owns.
func (c *Consumer) onAssigned(ctx context.Context, parts []TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range parts {
		c.owned[p] = struct{}{}
	}
	c.metrics.assigned.Set(float64(len(c.owned)))
	if len(parts) > 0 {
		c.metrics.rebalances.WithLabelValues("assigned").Inc()
		c.log.InfoContext(ctx, "kafka partitions assigned", "partitions", partitionList(parts))
	}
}

// onRevoked commits what was processed on the revoked partitions before they
// move (normally nothing: the loop commits before it allows a rebalance; it
// matters when that commit failed, and on Close), then stops owning them.
// Their offsets are forgotten whether or not that commit went through: this
// is the last chance to commit them, and a later commit of a partition now
// read by another member would move its position back. Records the failed
// commit did not cover are redelivered to the new owner (at-least-once).
func (c *Consumer) onRevoked(ctx context.Context, parts []TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commitLocked(ctx, parts)
	for _, p := range parts {
		delete(c.pending, p)
	}
	c.forgetLocked(parts)
	if len(parts) > 0 {
		c.metrics.rebalances.WithLabelValues("revoked").Inc()
		c.log.InfoContext(ctx, "kafka partitions revoked", "partitions", partitionList(parts))
	}
}

// onLost drops the lost partitions: they already belong to someone else, so
// committing would at best fail and at worst rewind the new owner.
func (c *Consumer) onLost(ctx context.Context, parts []TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dropped := 0
	for _, p := range parts {
		if _, ok := c.pending[p]; ok {
			delete(c.pending, p)
			dropped++
		}
	}
	c.forgetLocked(parts)
	if len(parts) > 0 {
		c.metrics.rebalances.WithLabelValues("lost").Inc()
		c.log.WarnContext(ctx, "kafka partitions lost; uncommitted records will be redelivered to the new owner",
			"partitions", partitionList(parts), "uncommitted_partitions", dropped)
	}
}

// forgetLocked removes partitions from the owned set. c.mu must be held.
func (c *Consumer) forgetLocked(gone []TopicPartition) {
	for _, p := range gone {
		delete(c.owned, p)
	}
	c.metrics.assigned.Set(float64(len(c.owned)))
}

func partitionList(parts []TopicPartition) []string {
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = p.Topic + "/" + strconv.Itoa(int(p.Partition))
	}
	return out
}

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that asks
// the brokers for metadata with a short timeout.
func (c *Consumer) ReadyCheck() func(ctx context.Context) error { return readyCheck(c.cl) }

// Close commits anything still pending (via the revoke callback), leaves the
// consumer group and shuts the client down. It is idempotent; [Consumer.Run]
// calls it on return.
func (c *Consumer) Close() { c.closeOnce.Do(c.cl.Close) }
