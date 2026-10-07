package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/outbox"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/testsupport"
)

func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

type record struct {
	topic   string
	key     string
	value   string
	headers map[string]string
}

// fakePublisher records what it is asked to publish; failOn makes the
// publish of that key fail.
type fakePublisher struct {
	mu      sync.Mutex
	records []record
	failOn  map[string]error
	delay   time.Duration
}

func (f *fakePublisher) Publish(_ context.Context, topic string, key, value []byte, headers ...kafka.Header) error {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOn[string(key)]; err != nil {
		return err
	}
	h := map[string]string{}
	for _, x := range headers {
		h[x.Key] = string(x.Value)
	}
	f.records = append(f.records, record{topic: topic, key: string(key), value: string(value), headers: h})
	return nil
}

func (f *fakePublisher) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.records))
	for i, r := range f.records {
		out[i] = r.key
	}
	return out
}

func enqueue(t testing.TB, ctx context.Context, db *pgx.DB, keys ...string) []string {
	t.Helper()
	tx, err := db.Pool().Begin(ctx)
	require.NoError(t, err)
	ids := make([]string, len(keys))
	for i, k := range keys {
		ids[i] = outbox.NewID()
		require.NoError(t, outbox.Enqueue(ctx, tx, outbox.Event{
			ID: ids[i], AggregateID: k, Type: outbox.TypeTaskCreated, Payload: []byte(`{"id":"` + k + `"}`),
		}))
	}
	require.NoError(t, tx.Commit(ctx))
	return ids
}

func pending(t testing.TB, r *outbox.Relay) int64 {
	t.Helper()
	n, _, err := r.Stats(context.Background())
	require.NoError(t, err)
	return n
}

func TestRelayOnce_PublishesInOrderWithHeadersAndDeletes(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	testx.Recorder(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "POST /tasks")
	ids := enqueue(t, ctx, db, "t1", "t2", "t3")
	span.End()

	pub := &fakePublisher{}
	r := outbox.NewRelay(db.Pool(), pub, outbox.Config{Topic: "tasks.events"}, nil)
	n, err := r.RelayOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []string{"t1", "t2", "t3"}, pub.keys(), "insertion order")
	assert.Equal(t, int64(0), pending(t, r), "published rows are deleted")

	rec := pub.records[0]
	assert.Equal(t, "tasks.events", rec.topic)
	assert.JSONEq(t, `{"id":"t1"}`, rec.value)
	assert.Equal(t, ids[0], rec.headers[kafka.HeaderEventID])
	assert.Equal(t, outbox.TypeTaskCreated, rec.headers[kafka.HeaderEventType])
	assert.Equal(t, "application/json", rec.headers[kafka.HeaderContentType])
	assert.Contains(t, rec.headers[kafka.HeaderTraceparent], span.SpanContext().TraceID().String(),
		"the trace context captured at insert time travels with the record")

	n, err = r.RelayOnce(context.Background())
	require.NoError(t, err)
	assert.Zero(t, n, "nothing left")
}

func TestRelayOnce_StopsAtTheFirstFailureAndKeepsTheRest(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	enqueue(t, context.Background(), db, "t1", "t2", "t3")
	pub := &fakePublisher{failOn: map[string]error{"t2": errors.New("broker: not enough replicas")}}
	r := outbox.NewRelay(db.Pool(), pub, outbox.Config{Topic: "tasks.events"}, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(r.Collectors()...)

	n, err := r.RelayOnce(context.Background())
	require.ErrorContains(t, err, "not enough replicas")
	assert.Equal(t, 1, n)
	assert.Equal(t, []string{"t1"}, pub.keys(), "t3 is not published before t2: order is kept")
	assert.Equal(t, int64(2), pending(t, r))

	var attempts int
	var lastErr string
	require.NoError(t, db.Pool().QueryRow(context.Background(),
		`SELECT attempts, last_error FROM outbox WHERE aggregate_id = 't2'`).Scan(&attempts, &lastErr))
	assert.Equal(t, 1, attempts)
	assert.Contains(t, lastErr, "not enough replicas")

	pub.failOn = nil
	n, err = r.RelayOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []string{"t1", "t2", "t3"}, pub.keys())
	assert.Equal(t, 3.0, testx.Metric(t, reg, "outbox_published_total", map[string]string{"result": "ok"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "outbox_published_total", map[string]string{"result": "error"}))
	testx.LintMetrics(t, reg)
}

// Replicas relaying together publish every event exactly once, in order: one
// relay holds the batch lock, the others skip the pass.
func TestRelay_ConcurrentRelaysDoNotDuplicateOrReorder(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	keys := make([]string, 60)
	for i := range keys {
		keys[i] = fmt.Sprintf("t%02d", i)
	}
	enqueue(t, context.Background(), db, keys...)

	pub := &fakePublisher{delay: time.Millisecond}
	relays := make([]*outbox.Relay, 4)
	for i := range relays {
		relays[i] = outbox.NewRelay(db.Pool(), pub, outbox.Config{Topic: "x", BatchSize: 7}, nil)
	}
	// Every relay keeps passing until the outbox is empty; the ones that do
	// not hold the lock pass without publishing.
	var wg sync.WaitGroup
	deadline := time.Now().Add(20 * time.Second)
	for _, r := range relays {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				if _, err := r.RelayOnce(context.Background()); err != nil {
					t.Error(err)
					return
				}
				n, _, err := r.Stats(context.Background())
				if err != nil || n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()
	assert.Equal(t, keys, pub.keys(), "each event once, in insertion order")
	assert.Equal(t, int64(0), pending(t, relays[0]))
}

func TestRun_WakeRelaysPromptlyAndStopsOnCancel(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	pub := &fakePublisher{}
	r := outbox.NewRelay(db.Pool(), pub, outbox.Config{Topic: "x", PollInterval: time.Hour}, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(r.Collectors()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(100 * time.Millisecond) // the first (empty) pass, then a one-hour wait
	enqueue(t, context.Background(), db, "woken")
	r.Wake()
	require.Eventually(t, func() bool { return len(pub.keys()) == 1 }, 5*time.Second, 20*time.Millisecond,
		"Wake cuts the poll interval short")
	assert.Equal(t, 0.0, testx.Metric(t, reg, "outbox_pending", nil))

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// With the broker failing, the backlog and its age are visible on /metrics,
// and the relay backs off instead of hammering.
func TestRun_BacklogMetricsAndBackoff(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	var calls sync.Map
	count := 0
	var mu sync.Mutex
	pub := publisherFunc(func(_ context.Context, _ string, key, _ []byte, _ ...kafka.Header) error {
		mu.Lock()
		count++
		mu.Unlock()
		calls.Store(string(key), true)
		return errors.New("broker down")
	})
	r := outbox.NewRelay(db.Pool(), pub, outbox.Config{Topic: "x", PollInterval: 50 * time.Millisecond, MaxBackoff: 400 * time.Millisecond}, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(r.Collectors()...)
	enqueue(t, context.Background(), db, "a", "b")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()
	time.Sleep(1500 * time.Millisecond)
	for range 20 {
		r.Wake() // ignored while backing off
	}
	time.Sleep(100 * time.Millisecond)
	cancel()

	mu.Lock()
	attempts := count
	mu.Unlock()
	assert.Less(t, attempts, 15, "backed off (50ms→400ms), not polled every 50ms nor on every Wake")
	assert.GreaterOrEqual(t, attempts, 3)
	_, triedB := calls.Load("b")
	assert.False(t, triedB, "b waits behind a")
	assert.Equal(t, 2.0, testx.Metric(t, reg, "outbox_pending", nil))
	assert.Greater(t, testx.Metric(t, reg, "outbox_relay_lag_seconds", nil), 1.0)
}

type publisherFunc func(ctx context.Context, topic string, key, value []byte, headers ...kafka.Header) error

func (f publisherFunc) Publish(ctx context.Context, topic string, key, value []byte, headers ...kafka.Header) error {
	return f(ctx, topic, key, value, headers...)
}
