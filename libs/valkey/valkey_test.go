package valkey_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// One Valkey per test binary; tests own their keys by prefix.
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := valkey.LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, "localhost:6379", cfg.Addr)
	assert.Equal(t, 0, cfg.DB)
	assert.Equal(t, 500*time.Millisecond, cfg.OpTimeout)

	t.Setenv("TASKS_VALKEY_ADDR", "cache:6380")
	t.Setenv("TASKS_VALKEY_DB", "3")
	cfg, err = valkey.LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, "cache:6380", cfg.Addr)
	assert.Equal(t, 3, cfg.DB)
}

func sharedCache(t testing.TB) *valkey.Cache {
	t.Helper()
	c, err := valkey.New(valkey.Config{URL: containers.Valkey(t), DialTimeout: 5 * time.Second}, nil)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestCacheGetSetDel(t *testing.T) {
	c := sharedCache(t)
	ctx := context.Background()
	k := testx.Unique("k") + ":"

	require.NoError(t, c.ReadyCheck()(ctx))

	_, ok, err := c.Get(ctx, k+"absent")
	require.NoError(t, err)
	assert.False(t, ok, "miss on an absent key")

	require.NoError(t, c.Set(ctx, k+"greeting", "pong", time.Minute))
	got, ok, err := c.Get(ctx, k+"greeting")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "pong", got)

	require.NoError(t, c.Set(ctx, k+"ephemeral", "x", 300*time.Millisecond))
	assert.Eventually(t, func() bool {
		_, ok, _ := c.Get(ctx, k+"ephemeral")
		return !ok
	}, 3*time.Second, 50*time.Millisecond, "the TTL is honoured")

	require.NoError(t, c.Del(ctx, k+"greeting"))
	_, ok, err = c.Get(ctx, k+"greeting")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestSetNXAndTombstone(t *testing.T) {
	c := sharedCache(t)
	ctx := context.Background()
	k := testx.Unique("nx")

	stored, err := c.SetNX(ctx, k, "first", time.Minute)
	require.NoError(t, err)
	assert.True(t, stored)
	stored, err = c.SetNX(ctx, k, "second", time.Minute)
	require.NoError(t, err)
	assert.False(t, stored, "an existing value is not overwritten")

	require.NoError(t, c.Tombstone(ctx, k, 400*time.Millisecond))
	_, ok, err := c.Get(ctx, k)
	require.NoError(t, err)
	assert.False(t, ok, "a tombstone reads as a miss")
	stored, err = c.SetNX(ctx, k, "stale", time.Minute)
	require.NoError(t, err)
	assert.False(t, stored, "a fill cannot overwrite a tombstone")

	assert.Eventually(t, func() bool {
		stored, err := c.SetNX(ctx, k, "fresh", time.Minute)
		return err == nil && stored
	}, 3*time.Second, 50*time.Millisecond, "the tombstone expires and fills resume")
	v, ok, err := c.Get(ctx, k)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "fresh", v)

	require.Error(t, c.Tombstone(ctx, k, 0))
	_, err = c.SetNX(ctx, k, "x", 0)
	require.Error(t, err)
}

type widget struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestAsideLoadsOnceThenServesFromCache(t *testing.T) {
	c := sharedCache(t)
	ctx := context.Background()
	key := testx.Unique("widget") + ":7"

	calls := 0
	load := func(context.Context) (widget, error) {
		calls++
		return widget{ID: 7, Name: "gadget"}, nil
	}

	w, hit, err := valkey.Aside(ctx, c, key, time.Minute, load)
	require.NoError(t, err)
	assert.False(t, hit)
	assert.Equal(t, widget{ID: 7, Name: "gadget"}, w)
	assert.Equal(t, 1, calls)

	w, hit, err = valkey.Aside(ctx, c, key, time.Minute, load)
	require.NoError(t, err)
	assert.True(t, hit)
	assert.Equal(t, widget{ID: 7, Name: "gadget"}, w)
	assert.Equal(t, 1, calls, "loader must not be called on a cache hit")

	ttl, err := c.Client().Do(ctx, c.Client().B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	assert.InDelta(t, 60_000, ttl, 6_500, "TTL within ±10%% of the requested one")
}

func TestAsideLoadErrorIsReturnedAndNotCached(t *testing.T) {
	c := sharedCache(t)
	ctx := context.Background()
	key := testx.Unique("widget")
	boom := errors.New("db down")

	_, _, err := valkey.Aside(ctx, c, key, time.Minute, func(context.Context) (widget, error) { return widget{}, boom })
	require.ErrorIs(t, err, boom)
	_, ok, err := c.Get(ctx, key)
	require.NoError(t, err)
	assert.False(t, ok)
}

// Concurrent misses on one key share a single load.
func TestAsideSingleflight(t *testing.T) {
	c := sharedCache(t)
	ctx := context.Background()
	key := testx.Unique("hot")

	var calls atomic.Int32
	release := make(chan struct{})
	load := func(context.Context) (widget, error) {
		calls.Add(1)
		<-release
		return widget{ID: 1, Name: "hot"}, nil
	}

	const n = 20
	var wg sync.WaitGroup
	results := make([]widget, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { results[i], _, errs[i] = valkey.Aside(ctx, c, key, time.Minute, load) })
	}
	time.Sleep(200 * time.Millisecond) // let every caller miss and join the flight
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load(), "one load for %d concurrent misses", n)
	for i := range n {
		require.NoError(t, errs[i])
		assert.Equal(t, widget{ID: 1, Name: "hot"}, results[i])
	}
}

// A waiter whose context ends stops waiting; the shared load is not
// cancelled by the first caller going away.
func TestAsideWaiterHonoursItsOwnContext(t *testing.T) {
	c := sharedCache(t)
	key := testx.Unique("slow")
	release := make(chan struct{})
	var loadErr atomic.Value
	load := func(ctx context.Context) (widget, error) {
		<-release
		if err := ctx.Err(); err != nil {
			loadErr.Store(err)
		}
		return widget{ID: 2}, nil
	}

	impatient, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := valkey.Aside(impatient, c, key, time.Minute, load)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	patient := make(chan widget, 1)
	go func() {
		w, _, _ := valkey.Aside(context.Background(), c, key, time.Minute, load)
		patient <- w
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled, "the cancelled caller returns at once")
	close(release)
	assert.Equal(t, widget{ID: 2}, <-patient, "the other waiter still gets the value")
	assert.Nil(t, loadErr.Load(), "the shared load ran detached from the first caller's cancellation")
}

// The delete-safety scheme end to end: a reader that loaded before the write
// cannot put the stale value back after the writer's tombstone.
func TestAsideCannotResurrectAfterTombstone(t *testing.T) {
	c := sharedCache(t)
	ctx := context.Background()
	key := testx.Unique("task")

	loaded := make(chan struct{})
	proceed := make(chan struct{})
	stale := make(chan widget, 1)
	go func() {
		w, _, _ := valkey.Aside(ctx, c, key, time.Minute, func(context.Context) (widget, error) {
			close(loaded) // read the old row …
			<-proceed     // … and stall until the writer has committed and invalidated
			return widget{ID: 3, Name: "old"}, nil
		})
		stale <- w
	}()
	<-loaded
	require.NoError(t, c.Tombstone(ctx, key, 2*time.Second)) // the writer, after commit
	close(proceed)
	assert.Equal(t, "old", (<-stale).Name, "the racing reader answers with what it read")

	w, hit, err := valkey.Aside(ctx, c, key, time.Minute, func(context.Context) (widget, error) {
		return widget{ID: 3, Name: "new"}, nil
	})
	require.NoError(t, err)
	assert.False(t, hit, "the stale write-back was refused: the next read goes to the source")
	assert.Equal(t, "new", w.Name)
}

// Commands are client spans in the caller's trace; the lookup counters and
// command metrics match what the cache actually did.
func TestCommandsAreTracedAndMeasured(t *testing.T) {
	sr := testx.Recorder(t) // before the client: valkeyotel captures the provider then
	c := sharedCache(t)
	ctx := context.Background()
	key := testx.Unique("telemetry")
	reg := prometheus.NewRegistry()
	reg.MustRegister(c.Collectors()...)

	cctx, parent := otel.Tracer("test").Start(ctx, "handler")
	_, ok, err := c.Get(cctx, key)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, c.Set(cctx, key, "v", time.Minute))
	v, ok, err := c.Get(cctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v", v)
	require.NoError(t, c.Del(cctx, key))
	parent.End()

	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "miss"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "hit"}))
	assert.Equal(t, -1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "error"}))
	assert.Equal(t, 2.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"op": "get"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"op": "set"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"op": "del"}))
	assert.Equal(t, -1.0, testx.Metric(t, reg, "cache_command_errors_total", nil))
	testx.LintMetrics(t, reg)

	names := map[string]int{}
	for _, s := range sr.Ended() {
		if s.SpanContext().TraceID() == parent.SpanContext().TraceID() && s.SpanKind() == trace.SpanKindClient {
			names[strings.ToUpper(strings.Fields(s.Name())[0])]++
		}
	}
	assert.Equal(t, 2, names["GET"], "two GET spans in the handler's trace")
	assert.Equal(t, 1, names["SET"])
}

// An unreachable cache: every command fails fast within OpTimeout and is
// counted as an error by op.
func TestUnreachableCacheFailsFastAndCounts(t *testing.T) {
	proxy := containers.Proxied(t, "valkey")
	c, err := valkey.New(valkey.Config{URL: "valkey://" + proxy.Addr, DialTimeout: 5 * time.Second, OpTimeout: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	reg := prometheus.NewRegistry()
	reg.MustRegister(c.Collectors()...)
	proxy.Down()

	ctx := context.Background()
	start := time.Now()
	_, _, err = c.Get(ctx, "k")
	require.Error(t, err)
	require.Error(t, c.Set(ctx, "k", "v", time.Minute))
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_errors_total", map[string]string{"op": "get"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_errors_total", map[string]string{"op": "set"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "error"}))

	// Aside degrades to the loader.
	w, hit, err := valkey.Aside(ctx, c, "k", time.Minute, func(context.Context) (widget, error) { return widget{ID: 9}, nil })
	require.NoError(t, err)
	assert.False(t, hit)
	assert.Equal(t, 9, w.ID)
}
