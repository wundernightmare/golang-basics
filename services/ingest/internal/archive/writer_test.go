package archive_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/testsupport"
)

type fixture struct {
	writer  *archive.Writer
	store   *testsupport.Store
	breaker *resilience.Breaker
	reg     *prometheus.Registry
	stop    func() // cancels Run and waits for it
}

func baseConfig() archive.WriterConfig {
	return archive.WriterConfig{
		BatchBytes: 1 << 20, Linger: time.Hour, UrgentLinger: 20 * time.Millisecond,
		MaxConcurrentFlushes: 2, Compression: shard.LevelFastest,
	}
}

func start(t *testing.T, cfg archive.WriterConfig) *fixture {
	t.Helper()

	breaker := resilience.NewBreaker(resilience.BreakerConfig{
		FailureRatio: 0.5, MinRequests: 2, Window: time.Minute, OpenTimeout: 50 * time.Millisecond, HalfOpenProbes: 1,
	}, time.Now, nil)

	store := testsupport.NewStore()
	writer, err := archive.NewWriter(cfg, store, archive.NewLayout("t"), "w1", breaker, nil)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	reg.MustRegister(writer.Collectors()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = writer.Run(ctx)
	}()

	var once sync.Once

	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)

	return &fixture{writer: writer, store: store, breaker: breaker, reg: reg, stop: stop}
}

func records(ids ...string) []shard.Record {
	out := make([]shard.Record, len(ids))
	for i, id := range ids {
		out[i] = shard.Record{ID: id, ReceivedAt: time.Now(), Event: []byte(`{"id":"` + id + `","payload":{}}`)}
	}

	return out
}

func wait(t *testing.T, ticket *archive.Ticket) error {
	t.Helper()

	select {
	case err := <-ticket.Done():
		return err
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the ticket was never settled")
		return nil
	}
}

func noop() {}

func TestWriter_UrgentBatchFlushesWithinTheUrgentLinger(t *testing.T) {
	t.Parallel()

	fix := start(t, baseConfig())

	settled := make(chan struct{})
	first, err := fix.writer.Add(records("a1", "a2"), true, func() { close(settled) })
	require.NoError(t, err)

	second, err := fix.writer.Add(records("b1"), false, noop)
	require.NoError(t, err)

	require.NoError(t, wait(t, first))
	require.NoError(t, wait(t, second), "requests share the batch: group commit")
	<-settled

	ids, err := fix.store.IDs()
	require.NoError(t, err)
	assert.Equal(t, []string{"a1", "a2", "b1"}, ids)
	assert.Len(t, fix.store.Keys(), 1, "one object for both requests")
	assert.True(t, strings.HasPrefix(fix.store.Keys()[0], "t/raw/dt="))
	assert.True(t, strings.HasSuffix(fix.store.Keys()[0], "-w1-00000001.evt.zst"))

	assert.InDelta(t, 1.0, testx.Metric(t, fix.reg, "ingest_archive_batches_total", map[string]string{"result": "ok"}), 0)
	assert.InDelta(t, 3.0, testx.Metric(t, fix.reg, "ingest_archive_events_total", map[string]string{"result": "ok"}), 0)
	assert.Positive(t, testx.Metric(t, fix.reg, "ingest_archive_stored_bytes_total", nil))
}

func TestWriter_LingeringBatchWaitsUntilExpedited(t *testing.T) {
	t.Parallel()

	fix := start(t, baseConfig())

	ticket, err := fix.writer.Add(records("a"), false, noop)
	require.NoError(t, err)

	select {
	case err := <-ticket.Done():
		require.FailNow(t, "a batch nobody waits on lingers", "settled with: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	ticket.Expedite()
	require.NoError(t, wait(t, ticket))
	ticket.Expedite() // after the flush: a no-op
}

func TestWriter_SizeTriggerFlushesWithoutWaiting(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()
	cfg.BatchBytes = 256
	cfg.UrgentLinger = time.Hour
	fix := start(t, cfg)

	big := []shard.Record{{ID: "big", ReceivedAt: time.Now(), Event: make([]byte, 300)}}
	ticket, err := fix.writer.Add(big, false, noop)
	require.NoError(t, err)
	require.NoError(t, wait(t, ticket), "a full batch is flushed at once")
}

func TestWriter_FailedPutFailsEveryTicketAndOpensTheCircuit(t *testing.T) {
	t.Parallel()

	fix := start(t, baseConfig())
	fix.store.Fail(true)

	for range 2 {
		ticket, err := fix.writer.Add(records("x"), true, noop)
		require.NoError(t, err)
		require.ErrorIs(t, wait(t, ticket), testsupport.ErrDown)
	}

	assert.Equal(t, resilience.StateOpen, fix.breaker.State())

	puts := fix.store.Puts()
	_, err := fix.writer.Add(records("y", "z"), true, func() { assert.Fail(t, "settled must not be called when Add fails") })
	require.ErrorIs(t, err, archive.ErrUnavailable, "open: refused at once, nothing buffered")
	assert.Equal(t, puts, fix.store.Puts())
	assert.InDelta(t, 2.0, testx.Metric(t, fix.reg, "ingest_archive_events_total", map[string]string{"result": "refused"}), 0)
	assert.InDelta(t, 2.0, testx.Metric(t, fix.reg, "ingest_archive_events_total", map[string]string{"result": "failed"}), 0)

	// The store recovers: after the open timeout one batch probes and closes it.
	fix.store.Fail(false)
	time.Sleep(60 * time.Millisecond)

	ticket, err := fix.writer.Add(records("probe"), true, noop)
	require.NoError(t, err)
	require.NoError(t, wait(t, ticket))
	assert.Equal(t, resilience.StateClosed, fix.breaker.State())
}

func TestWriter_RefusedRecordLeavesTheBatchIntact(t *testing.T) {
	t.Parallel()

	fix := start(t, baseConfig())

	good, err := fix.writer.Add(records("good"), false, noop)
	require.NoError(t, err)

	_, err = fix.writer.Add([]shard.Record{{ID: "ok"}, {ID: ""}}, true, noop)
	require.Error(t, err, "a record the format cannot carry refuses the whole request")

	good.Expedite()
	require.NoError(t, wait(t, good))

	ids, err := fix.store.IDs()
	require.NoError(t, err)
	assert.Equal(t, []string{"good"}, ids, "nothing of the refused request was stored")

	// Alone in a batch, the refused request gives the breaker admission back.
	_, err = fix.writer.Add([]shard.Record{{ID: ""}}, true, noop)
	require.Error(t, err)

	next, err := fix.writer.Add(records("next"), true, noop)
	require.NoError(t, err)
	require.NoError(t, wait(t, next))
}

func TestWriter_BusyFlushSlotsLetTheBatchGrow(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()
	cfg.MaxConcurrentFlushes = 1
	fix := start(t, cfg)

	release := fix.store.Block()
	defer release()

	first, err := fix.writer.Add(records("a"), true, noop)
	require.NoError(t, err)

	time.Sleep(60 * time.Millisecond) // "a" is in its put, holding the only slot

	second, err := fix.writer.Add(records("b"), true, noop)
	require.NoError(t, err)

	third, err := fix.writer.Add(records("c"), true, noop)
	require.NoError(t, err)

	time.Sleep(60 * time.Millisecond) // due, but no slot: b and c wait together

	release()
	require.NoError(t, wait(t, first))
	require.NoError(t, wait(t, second))
	require.NoError(t, wait(t, third))
	assert.Len(t, fix.store.Keys(), 2, "b and c went out as one object once the slot freed")
}

func TestWriter_ShutdownFlushesWhatIsLeft(t *testing.T) {
	t.Parallel()

	fix := start(t, baseConfig())

	var settled sync.WaitGroup

	settled.Add(1)

	ticket, err := fix.writer.Add(records("last"), false, settled.Done) // lingering for an hour
	require.NoError(t, err)

	fix.stop()
	settled.Wait()
	require.NoError(t, wait(t, ticket), "Run does not return before the last batch is stored")

	ids, err := fix.store.IDs()
	require.NoError(t, err)
	assert.Equal(t, []string{"last"}, ids)

	_, err = fix.writer.Add(records("late"), true, noop)
	require.ErrorIs(t, err, archive.ErrClosed)
}

func TestNewWriter_RejectsBadConfig(t *testing.T) {
	t.Parallel()

	breaker := resilience.NewBreaker(resilience.BreakerConfig{FailureRatio: 1, MinRequests: 1, Window: time.Second, OpenTimeout: time.Second, HalfOpenProbes: 1}, time.Now, nil)

	for name, mutate := range map[string]func(*archive.WriterConfig){
		"compression": func(c *archive.WriterConfig) { c.Compression = "zip" },
		"batch bytes": func(c *archive.WriterConfig) { c.BatchBytes = 0 },
		"linger":      func(c *archive.WriterConfig) { c.Linger = 0 },
		"urgent":      func(c *archive.WriterConfig) { c.UrgentLinger = 0 },
		"flushes":     func(c *archive.WriterConfig) { c.MaxConcurrentFlushes = 0 },
	} {
		cfg := baseConfig()
		mutate(&cfg)

		_, err := archive.NewWriter(cfg, testsupport.NewStore(), archive.NewLayout(""), "w", breaker, nil)
		require.Error(t, err, name)
	}
}

func TestWriterMetricsAreLintClean(t *testing.T) {
	t.Parallel()

	fix := start(t, baseConfig())
	ticket, err := fix.writer.Add(records("m"), true, noop)
	require.NoError(t, err)
	require.NoError(t, wait(t, ticket))
	testx.LintMetrics(t, fix.reg)
}
