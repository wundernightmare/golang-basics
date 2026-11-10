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
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// One Valkey per test binary; tests own their keys by prefix.
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

// The process-environment path of LoadConfig (LoadConfigFrom with nil);
// everything else reads a map through LoadConfigFrom.
//
// Serial on purpose: exercises the process-environment path (t.Setenv cannot run in parallel).
func TestLoadConfig_ReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv("TASKS_VALKEY_ADDR", "cache.from-env:6380")

	cfg, err := valkey.LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, "cache.from-env:6380", cfg.Addr, "LoadConfig reads the process environment")
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := valkey.LoadConfigFrom("TASKS_", map[string]string{})
	require.NoError(t, err)
	assert.Empty(t, cfg.Addr, "no default address: unset means no Valkey")
	assert.False(t, cfg.Configured())
	assert.Equal(t, 0, cfg.DB)
	assert.Equal(t, 500*time.Millisecond, cfg.OpTimeout)
	_, err = valkey.New(cfg, nil)
	require.ErrorContains(t, err, "VALKEY_URL or VALKEY_ADDR must be set")

	cfg, err = valkey.LoadConfigFrom("TASKS_", map[string]string{
		"TASKS_VALKEY_ADDR": "cache:6380",
		"TASKS_VALKEY_DB":   "3",
	})
	require.NoError(t, err)
	assert.Equal(t, "cache:6380", cfg.Addr)
	assert.Equal(t, 3, cfg.DB)
	assert.True(t, cfg.Configured())
}

func sharedCache(t testing.TB, opts ...valkey.Option) *valkey.Cache {
	t.Helper()
	c, err := valkey.New(valkey.Config{URL: containers.Valkey(t), DialTimeout: 5 * time.Second}, nil, opts...)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func TestCacheGetSetDel(t *testing.T) {
	t.Parallel()

	cache := sharedCache(t)
	ctx := context.Background()
	k := testx.Unique("k") + ":"

	require.NoError(t, cache.ReadyCheck()(ctx))

	_, ok, err := cache.Get(ctx, k+"absent")
	require.NoError(t, err)
	assert.False(t, ok, "miss on an absent key")

	require.NoError(t, cache.Set(ctx, k+"greeting", "pong", time.Minute))
	got, ok, err := cache.Get(ctx, k+"greeting")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "pong", got)

	require.NoError(t, cache.Set(ctx, k+"ephemeral", "x", 300*time.Millisecond))
	assert.Eventually(t, func() bool {
		_, ok, _ := cache.Get(ctx, k+"ephemeral")
		return !ok
	}, 3*time.Second, 50*time.Millisecond, "the TTL is honoured")

	require.NoError(t, cache.Del(ctx, k+"greeting"))
	_, ok, err = cache.Get(ctx, k+"greeting")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestSetNXAndTombstone(t *testing.T) {
	t.Parallel()

	cache := sharedCache(t)
	ctx := context.Background()
	k := testx.Unique("nx")

	stored, err := cache.SetNX(ctx, k, "first", time.Minute)
	require.NoError(t, err)
	assert.True(t, stored)
	stored, err = cache.SetNX(ctx, k, "second", time.Minute)
	require.NoError(t, err)
	assert.False(t, stored, "an existing value is not overwritten")

	require.NoError(t, cache.Tombstone(ctx, k, 400*time.Millisecond))
	_, ok, err := cache.Get(ctx, k)
	require.NoError(t, err)
	assert.False(t, ok, "a tombstone reads as a miss")

	stored, err = cache.SetNX(ctx, k, "stale", time.Minute)
	require.NoError(t, err)
	assert.False(t, stored, "a fill cannot overwrite a tombstone")

	assert.Eventually(t, func() bool {
		stored, err := cache.SetNX(ctx, k, "fresh", time.Minute)
		return err == nil && stored
	}, 3*time.Second, 50*time.Millisecond, "the tombstone expires and fills resume")

	v, ok, err := cache.Get(ctx, k)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "fresh", v)

	require.Error(t, cache.Tombstone(ctx, k, 0))
	_, err = cache.SetNX(ctx, k, "x", 0)
	require.Error(t, err)
}

type widget struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestAsideLoadsOnceThenServesFromCache(t *testing.T) {
	t.Parallel()

	cache := sharedCache(t)
	ctx := context.Background()
	key := testx.Unique("widget") + ":7"

	calls := 0
	load := func(context.Context) (widget, error) {
		calls++
		return widget{ID: 7, Name: "gadget"}, nil
	}

	w, hit, err := valkey.Aside(ctx, cache, key, time.Minute, load)
	require.NoError(t, err)
	assert.False(t, hit)
	assert.Equal(t, widget{ID: 7, Name: "gadget"}, w)
	assert.Equal(t, 1, calls)

	w, hit, err = valkey.Aside(ctx, cache, key, time.Minute, load)
	require.NoError(t, err)
	assert.True(t, hit)
	assert.Equal(t, widget{ID: 7, Name: "gadget"}, w)
	assert.Equal(t, 1, calls, "loader must not be called on a cache hit")

	ttl, err := cache.Client().Do(ctx, cache.Client().B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	assert.InDelta(t, 60_000, ttl, 6_500, "TTL within ±10%% of the requested one")
}

func TestAsideLoadErrorIsReturnedAndNotCached(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	cache := sharedCache(t)
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
		wg.Go(func() { results[i], _, errs[i] = valkey.Aside(ctx, cache, key, time.Minute, load) })
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
	t.Parallel()

	cache := sharedCache(t)
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
		_, _, err := valkey.Aside(impatient, cache, key, time.Minute, load)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)

	patient := make(chan widget, 1)

	go func() {
		w, _, _ := valkey.Aside(context.Background(), cache, key, time.Minute, load)
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
	t.Parallel()

	cache := sharedCache(t)
	ctx := context.Background()
	key := testx.Unique("task")

	loaded := make(chan struct{})
	proceed := make(chan struct{})
	stale := make(chan widget, 1)

	go func() {
		w, _, _ := valkey.Aside(ctx, cache, key, time.Minute, func(context.Context) (widget, error) {
			close(loaded) // read the old row …
			<-proceed     // … and stall until the writer has committed and invalidated

			return widget{ID: 3, Name: "old"}, nil
		})
		stale <- w
	}()

	<-loaded
	require.NoError(t, cache.Tombstone(ctx, key, 2*time.Second)) // the writer, after commit
	close(proceed)
	assert.Equal(t, "old", (<-stale).Name, "the racing reader answers with what it read")

	w, hit, err := valkey.Aside(ctx, cache, key, time.Minute, func(context.Context) (widget, error) {
		return widget{ID: 3, Name: "new"}, nil
	})
	require.NoError(t, err)
	assert.False(t, hit, "the stale write-back was refused: the next read goes to the source")
	assert.Equal(t, "new", w.Name)
}

// Commands are client spans in the caller's trace; the lookup counters and
// command metrics match what the cache actually did.
func TestCommandsAreTracedAndMeasured(t *testing.T) {
	t.Parallel()

	recorder := testx.Recorder(t)
	c := sharedCache(t, valkey.WithTracerProvider(recorder.Provider))
	ctx := context.Background()
	key := testx.Unique("telemetry")
	reg := prometheus.NewRegistry()
	reg.MustRegister(c.Collectors()...)

	cctx, parent := recorder.Tracer().Start(ctx, "handler")
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
	assert.Equal(t, 2.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"operation": "get"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"operation": "set"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"operation": "del"}))
	assert.Equal(t, -1.0, testx.Metric(t, reg, "cache_command_errors_total", nil))
	testx.LintMetrics(t, reg)

	names := map[string]int{}

	for _, s := range recorder.Ended() {
		if s.SpanContext().TraceID() == parent.SpanContext().TraceID() && s.SpanKind() == trace.SpanKindClient {
			names[strings.ToUpper(strings.Fields(s.Name())[0])]++
		}
	}

	assert.Equal(t, 2, names["GET"], "two GET spans in the handler's trace")
	assert.Equal(t, 1, names["SET"])
}

// An unreachable cache: every command fails fast within OpTimeout and is
// counted as an error by op.
//
//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxies (one per dependency); Down/Latency here would hit every proxied test
func TestUnreachableCacheFailsFastAndCounts(t *testing.T) {
	proxy := containers.Proxied(t, "valkey")
	cache, err := valkey.New(valkey.Config{URL: "valkey://" + proxy.Addr, DialTimeout: 5 * time.Second, OpTimeout: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	reg := prometheus.NewRegistry()
	reg.MustRegister(cache.Collectors()...)
	proxy.Down()

	ctx := context.Background()
	start := time.Now()
	_, _, err = cache.Get(ctx, "k")
	require.Error(t, err)
	require.Error(t, cache.Set(ctx, "k", "v", time.Minute))
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_errors_total", map[string]string{"operation": "get"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_errors_total", map[string]string{"operation": "set"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "error"}))

	// Aside degrades to the loader.
	w, hit, err := valkey.Aside(ctx, cache, "k", time.Minute, func(context.Context) (widget, error) { return widget{ID: 9}, nil })
	require.NoError(t, err)
	assert.False(t, hit)
	assert.Equal(t, 9, w.ID)
}

func TestReplaceIsCompareAndSet(t *testing.T) {
	t.Parallel()

	cache := sharedCache(t)
	ctx := context.Background()
	k := testx.Unique("cas")

	stored, err := cache.Replace(ctx, k, "old", "new", time.Minute)
	require.NoError(t, err)
	assert.False(t, stored, "an absent key is not created")

	require.NoError(t, cache.Set(ctx, k, "old", time.Minute))
	stored, err = cache.Replace(ctx, k, "other", "new", time.Minute)
	require.NoError(t, err)
	assert.False(t, stored, "a different value is left alone")

	stored, err = cache.Replace(ctx, k, "old", "new", 300*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, stored)

	v, ok, err := cache.Get(ctx, k)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "new", v)

	require.NoError(t, cache.Tombstone(ctx, k, time.Minute))
	stored, err = cache.Replace(ctx, k, "new", "stale", time.Minute)
	require.NoError(t, err)
	assert.False(t, stored, "a refresh cannot overwrite a tombstone")

	_, err = cache.Replace(ctx, k, "a", "b", 0)
	require.Error(t, err)

	k2 := testx.Unique("cas-ttl")
	require.NoError(t, cache.Set(ctx, k2, "old", time.Minute))
	stored, err = cache.Replace(ctx, k2, "old", "new", 300*time.Millisecond)
	require.NoError(t, err)
	require.True(t, stored)
	assert.Eventually(t, func() bool {
		_, ok, _ := cache.Get(ctx, k2)
		return !ok
	}, 3*time.Second, 50*time.Millisecond, "the replacement carries its own TTL")
}

func TestPublishSubscribe(t *testing.T) {
	t.Parallel()

	publisher, subscriber := sharedCache(t), sharedCache(t)
	channel := testx.Unique("events")

	ctx, cancel := context.WithCancel(context.Background())
	subscribed := make(chan struct{}, 1)
	messages := make(chan string, 8)
	done := make(chan error, 1)

	go func() {
		done <- subscriber.Subscribe(ctx, channel,
			func(message string) { messages <- message },
			func() { subscribed <- struct{}{} })
	}()

	select {
	case <-subscribed:
	case <-time.After(5 * time.Second):
		require.Fail(t, "onSubscribed was not called")
	}

	require.NoError(t, publisher.Publish(ctx, channel, "binary \x00\xff payload"))
	require.NoError(t, publisher.Publish(ctx, testx.Unique("elsewhere"), "not for us"))
	require.NoError(t, publisher.Publish(ctx, channel, "second"))

	for _, want := range []string{"binary \x00\xff payload", "second"} {
		select {
		case got := <-messages:
			assert.Equal(t, want, got)
		case <-time.After(5 * time.Second):
			require.Fail(t, "message not delivered", want)
		}
	}

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		require.Fail(t, "Subscribe did not return when its context was cancelled")
	}
}

// A subscription that lost its connection resubscribes by itself and says
// so: onSubscribed is the only sign that messages may have been missed.
//
//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxy
func TestSubscribeSignalsAReconnect(t *testing.T) {
	proxy := containers.Proxied(t, "valkey")
	subscriber, err := valkey.New(valkey.Config{URL: "valkey://" + proxy.Addr, DialTimeout: 5 * time.Second, OpTimeout: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(subscriber.Close)

	publisher := sharedCache(t)
	channel := testx.Unique("events")

	ctx, cancel := context.WithCancel(context.Background())
	subscribed := make(chan struct{}, 8)
	messages := make(chan string, 8)
	done := make(chan error, 1)

	go func() {
		done <- subscriber.Subscribe(ctx, channel,
			func(message string) { messages <- message },
			func() { subscribed <- struct{}{} })
	}()

	waitSubscribed := func(what string) {
		select {
		case <-subscribed:
		case <-time.After(15 * time.Second):
			require.Fail(t, what)
		}
	}
	waitSubscribed("the first subscription")

	proxy.Down()
	proxy.Up()
	waitSubscribed("the subscription after the reconnect")

	require.NoError(t, publisher.Publish(ctx, channel, "delivered"))

	select {
	case got := <-messages:
		assert.Equal(t, "delivered", got, "messages flow again without a new Subscribe call")
	case <-time.After(5 * time.Second):
		require.Fail(t, "no message after the reconnect")
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
