package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
)

// Store is the object store the writer puts batches into (libs/s3.Client).
type Store interface {
	Put(ctx context.Context, key string, body []byte, contentType string) error
}

// Errors [Writer.Add] and a [Ticket] report.
var (
	// ErrUnavailable: the store's circuit breaker is open — recent batches
	// failed, so this one is refused at once instead of held in memory.
	ErrUnavailable = errors.New("archive: store unavailable (circuit open)")
	// ErrClosed: the writer has shut down.
	ErrClosed = errors.New("archive: writer closed")
)

// WriterConfig tunes a [Writer].
type WriterConfig struct {
	// BatchBytes flushes a batch once its uncompressed size reaches it.
	BatchBytes int
	// Linger is how long a batch may wait for more events when nobody is
	// waiting on it — the events are already acknowledged by Kafka, and the
	// only cost of waiting is fewer, larger objects.
	Linger time.Duration
	// UrgentLinger is the wait once a request depends on the batch for its
	// answer: it is the latency a client pays while Kafka is away.
	UrgentLinger time.Duration
	// MaxConcurrentFlushes bounds the puts in flight; a batch that is due
	// while all are busy keeps growing until one finishes.
	MaxConcurrentFlushes int
	// Compression is the zstd level name (see shard.ParseLevel).
	Compression string
}

// Writer collects events into batches and stores each as one object. It is
// group commit: many requests share one put, and each learns through its
// [Ticket] whether the object holding its events was stored.
//
// A batch is flushed when it reaches BatchBytes or its deadline — Linger
// after it was opened, pulled in to UrgentLinger from now as soon as a
// request needs the answer. Each batch takes one admission from the breaker:
// while the store is failing, new batches are refused immediately rather
// than buffered for a put that will not succeed.
type Writer struct {
	cfg      WriterConfig
	store    Store
	layout   Layout
	writerID string
	breaker  *resilience.Breaker
	comp     *shard.Compressor
	log      *slog.Logger
	metrics  *writerMetrics
	now      func() time.Time

	mu      sync.Mutex
	cur     *batch
	seq     uint64
	closed  bool
	wake    chan struct{}
	slots   chan struct{}
	flushes sync.WaitGroup
}

type batch struct {
	buf       []byte
	events    int
	tickets   []*Ticket
	deadline  time.Time
	admission resilience.Ticket
}

// Ticket is one request's stake in a batch.
type Ticket struct {
	writer  *Writer
	batch   *batch
	done    chan error
	settled func()
}

// Done yields the outcome of the batch: nil once the object is stored, the
// error otherwise. It yields exactly once.
func (t *Ticket) Done() <-chan error { return t.done }

// Expedite says the request now depends on this batch: its flush is pulled in
// to UrgentLinger from now, if it was not already that close.
func (t *Ticket) Expedite() {
	w := t.writer

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.cur != t.batch {
		return // already being flushed
	}

	if urgent := w.now().Add(w.cfg.UrgentLinger); urgent.Before(t.batch.deadline) {
		t.batch.deadline = urgent

		w.nudge()
	}
}

// NewWriter builds a writer. Run it with [Writer.Run].
func NewWriter(cfg WriterConfig, store Store, layout Layout, writerID string, breaker *resilience.Breaker, log *slog.Logger) (*Writer, error) {
	comp, err := shard.NewCompressor(cfg.Compression)
	if err != nil {
		return nil, err
	}

	if cfg.BatchBytes < 1 || cfg.Linger <= 0 || cfg.UrgentLinger <= 0 || cfg.MaxConcurrentFlushes < 1 {
		return nil, fmt.Errorf("archive: writer config: batch bytes, lingers and concurrent flushes must be positive: %+v", cfg)
	}

	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	return &Writer{
		cfg: cfg, store: store, layout: layout, writerID: writerID, breaker: breaker, comp: comp, log: log,
		metrics: newWriterMetrics(), now: time.Now,
		wake:  make(chan struct{}, 1),
		slots: make(chan struct{}, cfg.MaxConcurrentFlushes),
	}, nil
}

// Add appends records to the open batch (opening one if needed) and returns
// the request's ticket. urgent says the request already depends on the
// archive for its answer. settled is called exactly once, after the batch's
// outcome is known — when Add fails it is not called. The records' bytes are
// copied: the caller may reuse them.
func (w *Writer) Add(records []shard.Record, urgent bool, settled func()) (*Ticket, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrClosed
	}

	now := w.now()

	if w.cur == nil {
		admission, ok := w.breaker.Allow()
		if !ok {
			w.metrics.events.WithLabelValues(resultRefused).Add(float64(len(records)))
			return nil, ErrUnavailable
		}

		w.cur = &batch{
			buf:       shard.AppendHeader(make([]byte, 0, min(w.cfg.BatchBytes, 1<<20))),
			deadline:  now.Add(w.cfg.Linger),
			admission: admission,
		}
	}

	cur := w.cur
	mark := len(cur.buf)

	for i := range records {
		var err error

		cur.buf, err = shard.AppendRecord(cur.buf, records[i])
		if err != nil {
			cur.buf = cur.buf[:mark] // the request is refused as a whole

			w.dropIfEmpty()

			return nil, err
		}
	}

	ticket := &Ticket{writer: w, batch: cur, done: make(chan error, 1), settled: settled}
	cur.tickets = append(cur.tickets, ticket)
	cur.events += len(records)

	if urgent {
		if deadline := now.Add(w.cfg.UrgentLinger); deadline.Before(cur.deadline) {
			cur.deadline = deadline
		}
	}

	w.nudge()

	return ticket, nil
}

// dropIfEmpty gives back the admission of a batch whose only request was
// refused. Caller holds mu.
func (w *Writer) dropIfEmpty() {
	if w.cur != nil && len(w.cur.tickets) == 0 {
		w.breaker.Record(w.cur.admission, resilience.OutcomeIgnored)
		w.cur = nil
	}
}

// nudge wakes the flush loop to look at the batch again.
func (w *Writer) nudge() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run flushes batches as they fall due until ctx is cancelled, then flushes
// what is left and waits for every put in flight. It returns only when no
// ticket is outstanding: each put is bounded by the store's own deadline.
func (w *Writer) Run(ctx context.Context) error {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		wait := w.flushDue(false)

		timer.Reset(wait)

		select {
		case <-ctx.Done():
			w.mu.Lock()
			w.closed = true
			w.mu.Unlock()

			w.flushDue(true)
			w.flushes.Wait()

			return nil
		case <-timer.C:
		case <-w.wake:
		}
	}
}

// flushDue starts a flush of the open batch when it is due (or force is set)
// and a flush slot is free, and returns how long to sleep before looking
// again. When every slot is busy the batch stays open: a finishing flush
// nudges the loop.
func (w *Writer) flushDue(force bool) time.Duration {
	const idle = time.Hour

	w.mu.Lock()

	cur := w.cur
	if cur == nil {
		w.mu.Unlock()
		return idle
	}

	if !force && len(cur.buf) < w.cfg.BatchBytes {
		if wait := cur.deadline.Sub(w.now()); wait > 0 {
			w.mu.Unlock()
			return wait
		}
	}

	if force {
		w.mu.Unlock()

		w.slots <- struct{}{} // shutdown waits for a slot: nothing is left behind

		w.mu.Lock()

		cur = w.cur
		if cur == nil { // flushed by the loop while this waited
			w.mu.Unlock()
			<-w.slots

			return idle
		}
	} else {
		select {
		case w.slots <- struct{}{}:
		default:
			w.mu.Unlock()
			return idle
		}
	}

	w.cur = nil
	w.seq++
	seq := w.seq
	w.mu.Unlock()

	w.flushes.Go(func() {
		defer func() {
			<-w.slots
			w.nudge()
		}()

		w.flush(cur, seq)
	})

	return 0 // look again at once: adds may have opened the next batch meanwhile
}

// flush stores one batch and settles its tickets. It runs detached from any
// request: the events of a batch belong to many requests, and to none once
// Kafka has acknowledged them.
func (w *Writer) flush(b *batch, seq uint64) {
	ctx := context.Background()
	key := w.layout.RawKey(w.now(), w.writerID, seq)
	body := w.comp.Compress(b.buf)

	err := w.store.Put(ctx, key, body, shard.ContentType)
	w.breaker.Record(b.admission, breakerOutcome(err == nil))

	if err != nil {
		w.metrics.batches.WithLabelValues(resultFailed).Inc()
		w.metrics.events.WithLabelValues(resultFailed).Add(float64(b.events))
		w.log.WarnContext(ctx, "archive batch not stored: its events have no archive copy from this attempt",
			"key", key, "events", b.events, "requests", len(b.tickets), "bytes", len(b.buf), "err", err)
	} else {
		w.metrics.batches.WithLabelValues(resultOK).Inc()
		w.metrics.events.WithLabelValues(resultOK).Add(float64(b.events))
		w.metrics.storedBytes.Add(float64(len(body)))
		w.log.DebugContext(ctx, "archive batch stored", "key", key, "events", b.events, "bytes", len(b.buf), "stored_bytes", len(body))
	}

	for _, ticket := range b.tickets {
		ticket.done <- err

		ticket.settled()
	}
}

// Result label values.
const (
	resultOK      = "ok"
	resultFailed  = "failed"
	resultRefused = "refused"
)

type writerMetrics struct {
	batches     *prometheus.CounterVec
	events      *prometheus.CounterVec
	storedBytes prometheus.Counter
}

func newWriterMetrics() *writerMetrics {
	return &writerMetrics{
		batches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_archive_batches_total",
			Help: "Archive batches flushed to the object store, by result (ok, failed).",
		}, []string{"result"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_archive_events_total",
			Help: "Events offered to the archive, by result: ok (stored), failed (their batch's put failed), refused (the store's circuit was open).",
		}, []string{"result"}),
		storedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ingest_archive_stored_bytes_total",
			Help: "Compressed bytes of the archive objects stored.",
		}),
	}
}

// Collectors returns the writer's Prometheus collectors:
//
//	ingest_archive_batches_total{result="ok|failed"}
//	ingest_archive_events_total{result="ok|failed|refused"}
//	ingest_archive_stored_bytes_total
//
// failed + refused events have no archive copy from this process (they are
// in Kafka, or their request was answered 503 and will be sent again).
func (w *Writer) Collectors() []prometheus.Collector {
	return []prometheus.Collector{w.metrics.batches, w.metrics.events, w.metrics.storedBytes}
}

// breakerOutcome maps "the store took the object" onto the breaker's terms.
func breakerOutcome(ok bool) resilience.Outcome {
	if ok {
		return resilience.OutcomeSuccess
	}

	return resilience.OutcomeFailure
}
