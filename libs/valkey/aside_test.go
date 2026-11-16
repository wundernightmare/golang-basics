package valkey_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/valkey"
)

type widget struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func loadWidget(w widget) func(context.Context) (widget, error) {
	return func(context.Context) (widget, error) { return w, nil }
}

// warnings returns the WARN lines with that message.
func warnings(t *testing.T, logs interface {
	Lines(tb testing.TB) []map[string]any
}, msg string,
) []map[string]any {
	t.Helper()

	var out []map[string]any

	for _, line := range logs.Lines(t) {
		if line["level"] == "WARN" && line["msg"] == msg {
			out = append(out, line)
		}
	}

	return out
}

// A panic in the loader belongs to the request that brought it: singleflight
// would re-raise it on a goroutine of its own, where no recover of the
// caller's reaches it and the process dies.
func TestAsideLoaderPanicIsAnErrorNotACrash(t *testing.T) {
	t.Parallel()

	cache, server, _ := fakeCache(t, valkey.Config{})

	_, hit, err := valkey.Aside(t.Context(), cache, "k", time.Minute, func(context.Context) (widget, error) {
		panic("a bug in the loader")
	})
	require.ErrorIs(t, err, valkey.ErrLoadPanic)
	require.ErrorContains(t, err, "a bug in the loader")
	assert.False(t, hit)

	_, cached := server.get("k")
	assert.False(t, cached, "nothing is cached")

	w, _, err := valkey.Aside(t.Context(), cache, "k", time.Minute, loadWidget(widget{ID: 1}))
	require.NoError(t, err, "the key is not poisoned: the next load runs")
	assert.Equal(t, 1, w.ID)
}

// An entry that no longer decodes into T (the format changed, the key did
// not) is a miss — and is replaced, so it is a miss once, not for its TTL.
func TestAsideReplacesACorruptEntry(t *testing.T) {
	t.Parallel()

	cache, server, logs := fakeCache(t, valkey.Config{})
	server.put("k", `"legacy-format"`)

	w, hit, err := valkey.Aside(t.Context(), cache, "k", time.Minute, loadWidget(widget{ID: 7}))
	require.NoError(t, err)
	assert.False(t, hit)
	assert.Equal(t, 7, w.ID)
	assert.Len(t, warnings(t, logs, "valkey: discarding corrupt cache entry"), 1)

	raw, _ := server.get("k")
	assert.JSONEq(t, `{"id":7,"name":""}`, raw, "the entry was overwritten")
	assert.Empty(t, warnings(t, logs, "valkey: cache write failed"), "a fill that went through is not a failure")

	w, hit, err = valkey.Aside(t.Context(), cache, "k", time.Minute, loadWidget(widget{ID: 8}))
	require.NoError(t, err)
	assert.True(t, hit)
	assert.Equal(t, 7, w.ID)
}

// A server that accepts commands and never answers: the caller's context has
// no deadline, so OpTimeout is the only bound — and Aside pays it once, on
// the lookup, not a second time on a fill into the same silence.
func TestSilentServerCostsAsideOneOpTimeout(t *testing.T) {
	t.Parallel()

	const opTimeout = 300 * time.Millisecond

	cache, server, logs := fakeCache(t, valkey.Config{OpTimeout: opTimeout})
	server.goSilent(t)

	for i := range 2 {
		start := time.Now()
		w, hit, err := valkey.Aside(context.Background(), cache, "k", time.Minute, loadWidget(widget{ID: i}))
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.False(t, hit)
		assert.Equal(t, i, w.ID)
		assert.GreaterOrEqual(t, elapsed, opTimeout)
		assert.Less(t, elapsed, 2*opTimeout, "one OpTimeout, not one per command")
	}

	assert.Len(t, warnings(t, logs, "valkey: lookup failed, loading from source"), 1,
		"an outage is one line per interval, not one per request")
	assert.Empty(t, warnings(t, logs, "valkey: cache write failed"))
}

// The default of a hand-built Config, and the error a caller sees past it.
func TestOpTimeoutDefaultBoundsACommand(t *testing.T) {
	t.Parallel()

	cache, server, _ := fakeCache(t, valkey.Config{})
	server.goSilent(t)

	start := time.Now()
	_, _, err := cache.Get(context.Background(), "k")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.InDelta(t, 500*time.Millisecond, time.Since(start), float64(250*time.Millisecond))
}

// A caller that is already gone is not a cache failure: no warning about
// the cache, and no load that nobody waits for.
func TestAsideCancelledCallerIsNotACacheFailure(t *testing.T) {
	t.Parallel()

	cache, _, logs := fakeCache(t, valkey.Config{})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	loaded := false
	_, _, err := valkey.Aside(ctx, cache, "k", time.Minute, func(context.Context) (widget, error) {
		loaded = true

		return widget{}, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, loaded)

	for _, line := range logs.Lines(t) {
		assert.NotEqual(t, "WARN", line["level"], "%v", line)
	}
}

// One key read as two types at once is a bug of the caller's, answered with
// an error to the caller that joined, not with a panic.
func TestAsideOneKeyAsTwoTypesIsAnError(t *testing.T) {
	t.Parallel()

	cache, _, _ := fakeCache(t, valkey.Config{})
	loading, release := make(chan struct{}), make(chan struct{})

	first := make(chan error, 1)

	go func() {
		_, _, err := valkey.Aside(context.Background(), cache, "k", time.Minute, func(context.Context) (widget, error) {
			close(loading)
			<-release

			return widget{ID: 1}, nil
		})
		first <- err
	}()

	<-loading

	second := make(chan error, 1)

	go func() {
		_, _, err := valkey.Aside(context.Background(), cache, "k", time.Minute, func(context.Context) (string, error) {
			return "", nil
		})
		second <- err
	}()

	time.Sleep(100 * time.Millisecond) // let the second caller miss and join the flight
	close(release)

	require.NoError(t, <-first)
	require.ErrorContains(t, <-second, "valkey_test.widget, not string")
}

// The shared load carries the deadline of the caller that started it. A
// caller that joined with more time is not failed by that deadline: it
// loads again under its own.
func TestAsideJoinerOutlivesTheLeadersDeadline(t *testing.T) {
	t.Parallel()

	cache, _, _ := fakeCache(t, valkey.Config{})

	var calls atomic.Int32

	loading := make(chan struct{})
	load := func(ctx context.Context) (widget, error) {
		if calls.Add(1) > 1 {
			return widget{ID: 2}, nil
		}

		close(loading)
		<-ctx.Done() // the first caller's deadline, kept by the detached load

		return widget{}, ctx.Err()
	}

	short, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	leader := make(chan error, 1)

	go func() {
		_, _, err := valkey.Aside(short, cache, "k", time.Minute, load)
		leader <- err
	}()

	<-loading

	w, _, err := valkey.Aside(context.Background(), cache, "k", time.Minute, load)
	require.NoError(t, err)
	assert.Equal(t, 2, w.ID)
	require.ErrorIs(t, <-leader, context.DeadlineExceeded)
}

// Only a joined load is tried again: a timeout the caller's own load ran
// into (a query deadline inside the loader) is that caller's answer.
func TestAsideReturnsItsOwnLoadsTimeout(t *testing.T) {
	t.Parallel()

	cache, _, _ := fakeCache(t, valkey.Config{})
	calls := 0

	_, _, err := valkey.Aside(context.Background(), cache, "k", time.Minute, func(context.Context) (widget, error) {
		calls++

		return widget{}, context.DeadlineExceeded
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, calls)
}

// Keys filled together must not expire together.
func TestAsideSpreadsTheTTL(t *testing.T) {
	t.Parallel()

	cache, server, logs := fakeCache(t, valkey.Config{})
	seen := map[time.Duration]bool{}

	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		_, _, err := valkey.Aside(t.Context(), cache, key, time.Minute, loadWidget(widget{}))
		require.NoError(t, err)

		ttl := server.ttl(key)
		assert.InDelta(t, time.Minute, ttl, float64(6*time.Second))

		seen[ttl] = true
	}

	assert.Greater(t, len(seen), 1, "eight fills, more than one TTL")
	assert.Empty(t, warnings(t, logs, "valkey: cache write failed"), "a fill that went through is not a failure")
}
