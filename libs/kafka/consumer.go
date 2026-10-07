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

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// errDraining reports that the drain budget ran out (or the loop was told to
// stop) before a record finished: it is left uncommitted for redelivery and
// is not the record's fault.
var errDraining = errors.New("kafka: consumer draining")

// maxDLQErrorBytes bounds the dlq_error header: an error string can embed
// anything, and a header is not the place for a stack dump.
const maxDLQErrorBytes = 1024

// lagWarnEvery rate-limits the "lag poll failed" warning: a broker outage
// would otherwise log one per LagInterval per replica.
const lagWarnEvery = time.Minute

// Consumer drains a consumer group with a poll / process / commit loop. It
// is the "background worker" shape of this monorepo, fed by a broker instead
// of a ticker. Register [Consumer.Collectors] for throughput, retries,
// dead-lettering, commits, rebalances and the group's lag.
//
// A Consumer is single-use: [Consumer.Run] closes it when it returns.
type Consumer struct {
	cl      *kgo.Client
	adm     *kadm.Client
	cfg     Config
	log     *slog.Logger
	tracer  *kotel.Tracer
	metrics *consumerMetrics

	mu      sync.Mutex
	pending map[string]map[int32]*kgo.Record // processed, not yet committed: the latest per partition
	owned   map[string]map[int32]struct{}    // partitions assigned to this member

	running     atomic.Bool
	closeOnce   sync.Once
	lastLagWarn atomic.Int64 // unix nanos of the last lag-poll warning
}

// NewConsumer builds a consumer-group client subscribed to cfg.Topics and
// verifies broker connectivity with a ping. Auto-commit is off: offsets
// advance only past records a [Handler] has accepted or that were parked on
// the dead-letter topic. The caller owns it and must call [Consumer.Close]
// if it never calls [Consumer.Run].
func NewConsumer(ctx context.Context, cfg Config, log *slog.Logger) (*Consumer, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
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
		metrics: newConsumerMetrics(),
		pending: map[string]map[int32]*kgo.Record{},
		owned:   map[string]map[int32]struct{}{},
	}
	c.tracer = kotel.NewTracer(kotel.ClientID(cfg.ClientID), kotel.ConsumerGroup(cfg.Group))

	opts, err := cfg.commonOpts()
	if err != nil {
		return nil, err
	}
	opts = append(opts, cfg.producerOpts()...) // the dead-letter topic is produced through this client
	opts = append(opts,
		kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(c.tracer)).Hooks()...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		// Rebalances wait until the loop has processed and committed the
		// batch it polled (AllowRebalance), so a partition is never handed
		// to another member with processed-but-uncommitted records on it.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
		// A brand-new group (no committed offsets yet) starts from the oldest
		// retained record rather than the newest, so a freshly-deployed worker
		// does not silently skip a backlog. Once the group has committed, those
		// offsets win and this only applies to new partitions.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka: consumer ping: %w", err)
	}
	warnPlaintextSASL(ctx, cfg, log)
	c.cl = cl
	c.adm = kadm.NewClient(cl)

	dlq := cfg.DeadLetterTopic
	if cfg.deadLetterTopic("x") == "" {
		dlq = "disabled"
	}
	log.InfoContext(ctx, "kafka consumer ready", "brokers", cfg.brokersString(), "group", cfg.Group, "topics", cfg.Topics,
		"max_retries", cfg.maxRetries(), "dead_letter_topic", dlq, "tls", cfg.TLSEnabled, "sasl", cfg.SASLMechanism)
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
// then everything processed is committed in one request, then the group may
// rebalance. On shutdown the in-flight batch gets DrainTimeout to finish;
// what finished is committed (with a context detached from ctx), what did
// not is redelivered to the next owner. Delivery is at-least-once: a
// handler must tolerate seeing a record twice (see services/consumer's
// dedupe for the pattern).
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("kafka: Consumer.Run called twice")
	}
	defer c.Close()
	c.log.InfoContext(ctx, "kafka consumer loop started", "group", c.cfg.Group, "topics", c.cfg.Topics)

	lagCtx, stopLag := context.WithCancel(ctx)
	lagDone := make(chan struct{})
	go func() {
		defer close(lagDone)
		if c.cfg.LagInterval > 0 {
			c.pollLag(lagCtx)
		}
	}()
	defer func() { stopLag(); <-lagDone }()

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
		fetches := c.cl.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() {
			return nil
		}
		c.fetchErrors(ctx, fetches)
		if ctx.Err() != nil {
			// Shutdown arrived with the poll: nothing of this batch has
			// started, so leave all of it for the next owner.
			c.cl.AllowRebalance()
			c.log.InfoContext(ctx, "kafka consumer loop stopping")
			return nil
		}

		err := c.processBatch(procCtx, fetches.Records(), handler)
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

// fetchErrors counts and logs the fetch errors in fetches, skipping the ones
// that only mean "shutting down".
func (c *Consumer) fetchErrors(ctx context.Context, fetches kgo.Fetches) {
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kgo.ErrClientClosed) {
			return
		}
		c.metrics.fetchErrs.WithLabelValues(topic).Inc()
		c.log.ErrorContext(ctx, "kafka fetch error", "topic", topic, "partition", partition, "err", err)
	})
}

// processBatch handles records in order, marking each finished one for
// commit. It stops at the first record that neither succeeded nor was
// parked, so what is marked is always a per-partition prefix.
func (c *Consumer) processBatch(ctx context.Context, records []*kgo.Record, handler Handler) error {
	for _, r := range records {
		if ctx.Err() != nil {
			return errDraining
		}
		if err := c.process(ctx, r, handler); err != nil {
			return err
		}
		c.markDone(r)
	}
	return nil
}

// process runs one record through the handler with retries, then the DLQ.
// nil means "done, commit it".
func (c *Consumer) process(ctx context.Context, r *kgo.Record, handler Handler) error {
	reqID := requestID(r)
	// logCtx: the record's trace (continued from its headers) and request
	// id, for the loop's own log lines about this record.
	logCtx := r.Context
	if logCtx == nil {
		logCtx = context.WithoutCancel(ctx)
	}
	logCtx = httpx.WithRequestID(logCtx, reqID)
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
			c.metrics.records.WithLabelValues(r.Topic, result).Inc()
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
		c.metrics.records.WithLabelValues(r.Topic, consumeFailed).Inc()
		return fmt.Errorf("kafka: record %s/%d@%d failed after %d attempt(s) (%s) and the dead-letter topic is disabled: %w",
			r.Topic, r.Partition, r.Offset, attempt, reason, err)
	}
	if dlqErr := c.deadLetter(ctx, logCtx, r, dlqTopic, reason, err, attempt); dlqErr != nil {
		if errors.Is(dlqErr, errDraining) {
			return errDraining
		}
		c.metrics.records.WithLabelValues(r.Topic, consumeFailed).Inc()
		return fmt.Errorf("kafka: record %s/%d@%d failed and could not be dead-lettered to %q: %w",
			r.Topic, r.Partition, r.Offset, dlqTopic, errors.Join(err, dlqErr))
	}
	c.metrics.records.WithLabelValues(r.Topic, consumeDeadLettered).Inc()
	return nil
}

// invoke runs one handler attempt inside a process span continued from the
// record's trace, with the request id and the per-attempt deadline, and
// turns a panic into an error (retried like any other).
func (c *Consumer) invoke(ctx context.Context, r *kgo.Record, handler Handler, attempt int, reqID string) (err error) {
	rctx, span := c.tracer.WithProcessSpan(r)
	defer span.End()
	span.SetAttributes(attribute.Int("messaging.kafka.attempt", attempt))
	// The span context descends from the record, not from ctx: tie the
	// loop's cancellation (the drain deadline) to it explicitly.
	rctx, cancel := context.WithTimeout(rctx, c.cfg.HandlerTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	rctx = httpx.WithRequestID(rctx, reqID)

	start := time.Now()
	defer func() {
		if p := recover(); p != nil {
			c.log.ErrorContext(rctx, "kafka handler panicked", "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			err = fmt.Errorf("kafka: handler panic: %v", p)
		}
		c.metrics.duration.WithLabelValues(r.Topic).Observe(time.Since(start).Seconds())
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}()
	return handler(rctx, toMessage(r, attempt))
}

// requestID is the record's event_id header when it is a usable correlation
// id (see httpx.ValidRequestID), a fresh id otherwise.
func requestID(r *kgo.Record) string {
	if id, ok := headerValue(r.Headers, HeaderEventID); ok && httpx.ValidRequestID(id) {
		return id
	}
	return httpx.NewRequestID()
}

// deadLetter publishes r to topic with its original headers plus the dlq_*
// ones, retrying the publish like a handler. The publish span continues the
// record's trace.
func (c *Consumer) deadLetter(ctx, logCtx context.Context, r *kgo.Record, topic, reason string, cause error, attempts int) error {
	errText := cause.Error()
	if len(errText) > maxDLQErrorBytes {
		errText = errText[:maxDLQErrorBytes]
	}
	headers := make([]kgo.RecordHeader, 0, len(r.Headers)+6)
	for _, h := range r.Headers {
		switch h.Key {
		case HeaderDLQReason, HeaderDLQError, HeaderDLQSourceTopic, HeaderDLQSourcePartition, HeaderDLQSourceOffset, HeaderDLQAttempts:
			continue // a record re-parked from a DLQ replay gets fresh ones
		}
		headers = append(headers, h)
	}
	headers = append(headers,
		kgo.RecordHeader{Key: HeaderDLQReason, Value: []byte(reason)},
		kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(errText)},
		kgo.RecordHeader{Key: HeaderDLQSourceTopic, Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: HeaderDLQSourcePartition, Value: []byte(strconv.Itoa(int(r.Partition)))},
		kgo.RecordHeader{Key: HeaderDLQSourceOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
	)

	for n := 0; ; n++ {
		pctx, cancel := context.WithTimeout(ctx, c.cfg.PublishTimeout)
		// A fresh record per attempt: franz-go owns a record while producing it.
		rec := &kgo.Record{Topic: topic, Key: r.Key, Value: r.Value, Headers: append([]kgo.RecordHeader(nil), headers...), Context: logCtx}
		err := produce(pctx, c.cl, rec)
		cancel()
		if err == nil {
			c.metrics.dlq.WithLabelValues(topic).Inc()
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
func (c *Consumer) markDone(r *kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := c.pending[r.Topic]
	if parts == nil {
		parts = map[int32]*kgo.Record{}
		c.pending[r.Topic] = parts
	}
	parts[r.Partition] = r
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

// commitLocked commits the pending records of the given partitions (all of
// them when only is nil) and forgets the ones committed. c.mu must be held.
func (c *Consumer) commitLocked(ctx context.Context, only map[string][]int32) {
	var recs []*kgo.Record
	for topic, parts := range c.pending {
		for p, r := range parts {
			if only == nil || containsPartition(only[topic], p) {
				recs = append(recs, r)
			}
		}
	}
	if len(recs) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.DrainTimeout)
	defer cancel()
	if err := c.cl.CommitRecords(cctx, recs...); err != nil {
		c.metrics.commitErrs.Inc()
		c.log.ErrorContext(ctx, "kafka offset commit failed; records stay pending", "records", len(recs), "err", err)
		return
	}
	for _, r := range recs {
		if cur := c.pending[r.Topic][r.Partition]; cur == r {
			delete(c.pending[r.Topic], r.Partition)
		}
	}
}

func containsPartition(ps []int32, p int32) bool {
	for _, q := range ps {
		if q == p {
			return true
		}
	}
	return false
}

// onAssigned tracks the partitions this member now owns.
func (c *Consumer) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, ps := range assigned {
		if c.owned[topic] == nil {
			c.owned[topic] = map[int32]struct{}{}
		}
		for _, p := range ps {
			c.owned[topic][p] = struct{}{}
		}
	}
	c.updateAssignedLocked()
	if len(assigned) > 0 {
		c.metrics.rebalances.WithLabelValues("assigned").Inc()
		c.log.InfoContext(ctx, "kafka partitions assigned", "partitions", assigned)
	}
}

// onRevoked commits what was processed on the revoked partitions before they
// move (with BlockRebalanceOnPoll this is normally nothing: the loop commits
// before it allows a rebalance; it matters when that commit failed, and on
// Close), then stops reporting them.
func (c *Consumer) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commitLocked(ctx, revoked)
	c.forgetLocked(revoked)
	if len(revoked) > 0 {
		c.metrics.rebalances.WithLabelValues("revoked").Inc()
		c.log.InfoContext(ctx, "kafka partitions revoked", "partitions", revoked)
	}
}

// onLost drops the lost partitions: they already belong to someone else, so
// committing would at best fail and at worst rewind the new owner.
func (c *Consumer) onLost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dropped := 0
	for topic, ps := range lost {
		for _, p := range ps {
			if _, ok := c.pending[topic][p]; ok {
				delete(c.pending[topic], p)
				dropped++
			}
		}
	}
	c.forgetLocked(lost)
	if len(lost) > 0 {
		c.metrics.rebalances.WithLabelValues("lost").Inc()
		c.log.WarnContext(ctx, "kafka partitions lost; uncommitted records will be redelivered to the new owner",
			"partitions", lost, "uncommitted_partitions", dropped)
	}
}

// forgetLocked removes partitions from the owned set, the gauge and the lag
// snapshot. c.mu must be held.
func (c *Consumer) forgetLocked(gone map[string][]int32) {
	for topic, ps := range gone {
		for _, p := range ps {
			delete(c.owned[topic], p)
		}
	}
	c.updateAssignedLocked()
	c.metrics.lag.drop(gone)
}

func (c *Consumer) updateAssignedLocked() {
	for topic, ps := range c.owned {
		c.metrics.assigned.WithLabelValues(topic).Set(float64(len(ps)))
	}
}

// pollLag refreshes the lag snapshot every LagInterval until ctx is done.
func (c *Consumer) pollLag(ctx context.Context) {
	t := time.NewTicker(c.cfg.LagInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.refreshLag(ctx)
	}
}

// refreshLag computes the lag of the partitions this member owns (end offset
// minus the group's committed offset, from the broker) and swaps the
// snapshot in whole. A failed poll keeps the previous snapshot, bumps the
// error counter and warns (at most once per lagWarnEvery).
func (c *Consumer) refreshLag(ctx context.Context) {
	lctx, cancel := context.WithTimeout(ctx, c.cfg.LagInterval)
	defer cancel()
	lags, err := c.adm.Lag(lctx, c.cfg.Group)
	if err == nil {
		if gl, ok := lags[c.cfg.Group]; ok {
			err = gl.Error()
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		c.metrics.lag.pollErrors.Inc()
		now := time.Now()
		if last := c.lastLagWarn.Load(); now.UnixNano()-last >= int64(lagWarnEvery) && c.lastLagWarn.CompareAndSwap(last, now.UnixNano()) {
			c.log.WarnContext(ctx, "kafka lag poll failed (further failures within a minute are not logged)", "group", c.cfg.Group, "err", err)
		}
		return
	}
	gl := lags[c.cfg.Group].Lag

	c.mu.Lock()
	defer c.mu.Unlock()
	var points []lagPoint
	for topic, ps := range c.owned {
		for p := range ps {
			l, ok := gl.Lookup(topic, p)
			if !ok || l.Err != nil || l.Lag < 0 {
				continue
			}
			points = append(points, lagPoint{topic: topic, partition: p, lag: l.Lag})
		}
	}
	c.metrics.lag.set(points)
	c.metrics.lag.lastSuccess.Store(time.Now().UnixNano())
}

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that pings the
// brokers with a short timeout.
func (c *Consumer) ReadyCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := pingBounded(ctx, c.cl); err != nil {
			return fmt.Errorf("kafka brokers unreachable: %w", err)
		}
		return nil
	}
}

// Close commits anything still pending (via the revoke callback), leaves the
// consumer group and shuts the client down. It is idempotent; [Consumer.Run]
// calls it on return.
func (c *Consumer) Close() {
	c.closeOnce.Do(c.cl.CloseAllowingRebalance)
}
