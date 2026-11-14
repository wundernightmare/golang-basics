package valkey_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// The connection settings against a real server. What the server sees is
// read from CLIENT LIST through a second, unnamed client: the client under
// test carries a unique name (the URL's client_name), so its connections can
// be told from every other test's.

// namedCache connects to base (a valkey:// URL without a query) under a
// unique client name and returns the cache with that name.
func namedCache(t *testing.T, base string, cfg valkey.Config) (*valkey.Cache, string) {
	t.Helper()

	name := testx.Unique("pool")
	cfg.URL = base + "?client_name=" + name
	cache, err := valkey.New(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	return cache, name
}

// connections returns the server-side ids of the connections named name.
func connections(t *testing.T, observer *valkey.Cache, name string) []string {
	t.Helper()

	client := observer.Client()
	list, err := client.Do(context.Background(), client.B().ClientList().Build()).ToString()
	require.NoError(t, err)

	var ids []string

	// RESP3 returns the list as a verbatim string: "txt:" before the first line.
	for line := range strings.SplitSeq(strings.TrimPrefix(list, "txt:"), "\n") {
		if !strings.Contains(line, " name="+name+" ") {
			continue
		}

		for field := range strings.FieldsSeq(line) {
			if id, ok := strings.CutPrefix(field, "id="); ok {
				ids = append(ids, id)
			}
		}
	}

	return ids
}

// PipelineConns is the whole footprint of ordinary commands: a burst of
// concurrent commands is pipelined over those connections, not given one
// each.
func TestPipelineConnsIsTheFootprintOfOrdinaryCommands(t *testing.T) {
	t.Parallel()

	observer := sharedCache(t)
	cache, name := namedCache(t, containers.Valkey(t), valkey.Config{PipelineConns: 2, PoolSize: 8})
	ctx := context.Background()
	key := testx.Unique("burst")

	var (
		wg   sync.WaitGroup
		peak int
		mu   sync.Mutex
	)

	for range 200 {
		wg.Go(func() {
			for range 20 {
				_, _, err := cache.Get(ctx, key)
				assert.NoError(t, err)
			}
		})
	}

	for range 20 {
		n := len(connections(t, observer, name))

		mu.Lock()
		peak = max(peak, n)
		mu.Unlock()
	}

	wg.Wait()
	assert.Equal(t, 2, peak, "200 concurrent callers share the 2 pipelined connections")
	assert.Len(t, connections(t, observer, name), 2, "and they stay open afterwards")
}

// PoolSize bounds the dedicated connections of blocking commands: callers
// beyond it wait for a connection instead of opening one, and a waiting
// caller leaves when its own context ends without loosening the bound.
func TestPoolSizeBoundsBlockingCommands(t *testing.T) {
	t.Parallel()

	const (
		poolSize = 2
		callers  = 6
		block    = 400 * time.Millisecond
	)

	observer := sharedCache(t)
	cache, name := namedCache(t, containers.Valkey(t), valkey.Config{PipelineConns: 1, PoolSize: poolSize})
	client := cache.Client()
	ctx := context.Background()
	key := testx.Unique("empty-list")

	var wg sync.WaitGroup

	start := time.Now()

	for range callers {
		wg.Go(func() {
			// An empty list: BLPOP holds its connection for the whole timeout.
			err := client.Do(ctx, client.B().Blpop().Key(key).Timeout(block.Seconds()).Build()).Error()
			assert.True(t, valkeygo.IsValkeyNil(err), "BLPOP on an empty list ends with a nil reply, got %v", err)
		})
	}

	// All six are queued or blocked now. Callers with a deadline, and callers
	// whose context is already over, do not wait out the queue — and, unlike
	// in the library's own pool, do not loosen the bound as they leave
	// (TestUpstreamPoolLimitLeaksOnACancelledWaiter).
	time.Sleep(block / 4)

	over, cancelOver := context.WithCancel(ctx)
	cancelOver()

	var impatient sync.WaitGroup

	for range 10 {
		impatient.Go(func() {
			short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()

			waited := time.Now()

			err := client.Do(short, client.B().Blpop().Key(key).Timeout(block.Seconds()).Build()).Error()
			if !errors.Is(err, context.DeadlineExceeded) { // not require: this is not the test goroutine
				t.Errorf("a queued caller ends with its deadline, got %v", err)
			}

			assert.Less(t, time.Since(waited), block/2, "left the queue at its own deadline")

			err = client.Do(over, client.B().Blpop().Key(key).Timeout(block.Seconds()).Build()).Error()
			if !errors.Is(err, context.Canceled) {
				t.Errorf("a caller whose context is over ends with it, got %v", err)
			}
		})
	}

	impatient.Wait()

	// Meanwhile the server holds the one pipelined connection and no more
	// than the pool's two.
	peak := 0
	for time.Since(start) < 2*block {
		peak = max(peak, len(connections(t, observer, name)))

		time.Sleep(20 * time.Millisecond)
	}

	wg.Wait()

	assert.Equal(t, 1+poolSize, peak, "1 pipelined + 2 dedicated connections, for 6 blocked callers")
	// Six callers, two at a time, 400 ms each: three rounds.
	assert.GreaterOrEqual(t, time.Since(start), callers/poolSize*block-50*time.Millisecond,
		"the callers beyond the pool waited for a connection")
}

// ConnLifetime replaces the pipelined connection as it ages, and a caller
// never sees the swap.
func TestConnLifetimeRecyclesWithoutErrors(t *testing.T) {
	t.Parallel()

	observer := sharedCache(t)
	cache, name := namedCache(t, containers.Valkey(t), valkey.Config{PipelineConns: 1, ConnLifetime: 250 * time.Millisecond})
	ctx := context.Background()
	key := testx.Unique("lifetime")
	seen := map[string]bool{}

	for start := time.Now(); time.Since(start) < 1500*time.Millisecond; {
		require.NoError(t, cache.Set(ctx, key, "v", time.Minute))

		v, ok, err := cache.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "v", v)

		ids := connections(t, observer, name)
		assert.LessOrEqual(t, len(ids), 2, "the old connection is closed, not leaked (two only during a swap)")

		for _, id := range ids {
			seen[id] = true
		}
	}

	assert.GreaterOrEqual(t, len(seen), 3, "1.5 s at a 250 ms lifetime: the connection was replaced several times")
}

// ConnWriteTimeout is how long the client keeps a connection whose peer went
// silent. The proxy swallows everything without closing anything — a dropped
// route — so only the client's own deadline can notice.
//
//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxies (one per dependency)
func TestConnWriteTimeoutDropsASilentConnection(t *testing.T) {
	proxy := containers.Proxied(t, "valkey")
	observer := sharedCache(t)
	base := "valkey://" + proxy.Addr

	short, shortName := namedCache(t, base, valkey.Config{PipelineConns: 1, OpTimeout: 200 * time.Millisecond, ConnWriteTimeout: 500 * time.Millisecond})
	_, longName := namedCache(t, base, valkey.Config{PipelineConns: 1, OpTimeout: 200 * time.Millisecond, ConnWriteTimeout: time.Minute})

	require.Len(t, connections(t, observer, shortName), 1)
	require.Len(t, connections(t, observer, longName), 1)

	proxy.Toxic("timeout", "upstream", map[string]any{"timeout": 0})
	proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0})

	// The caller is bounded by OpTimeout either way.
	started := time.Now()
	_, _, err := short.Get(context.Background(), "k")
	require.Error(t, err)
	assert.Less(t, time.Since(started), time.Second)

	// The client with the short timeout gives the connection up; the server
	// sees it close. The one with a minute keeps talking into the void.
	assert.Eventually(t, func() bool { return len(connections(t, observer, shortName)) == 0 },
		5*time.Second, 50*time.Millisecond, "the silent connection was dropped after conn_write_timeout")
	assert.Len(t, connections(t, observer, longName), 1, "the control: nothing but the timeout closes it")

	proxy.Reset()

	// And the cache is usable again without a restart.
	assert.Eventually(t, func() bool { return short.Set(context.Background(), testx.Unique("back"), "v", time.Minute) == nil },
		5*time.Second, 50*time.Millisecond)
}

// Everything that takes a dedicated connection is behind the same bound;
// everything that does not is never held up by it.
func TestPoolSizeCoversEveryDedicatedPath(t *testing.T) {
	t.Parallel()

	observer := sharedCache(t)
	cache, name := namedCache(t, containers.Valkey(t), valkey.Config{PipelineConns: 1, PoolSize: 1})
	client := cache.Client()
	ctx := context.Background()
	key := testx.Unique("paths")

	// The only dedicated connection is taken.
	held, giveBack := client.Dedicate()
	require.NoError(t, held.Do(ctx, held.B().Set().Key(key).Value("v").Build()).Error())

	impatient := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, 50*time.Millisecond) }
	blpop := func() valkeygo.Completed { return client.B().Blpop().Key(testx.Unique("empty")).Timeout(0.2).Build() }
	get := func() valkeygo.Completed { return client.B().Get().Key(key).Build() }

	short, cancel := impatient()
	require.ErrorIs(t, client.Do(short, blpop()).Error(), context.DeadlineExceeded, "a blocking command waits for the connection")
	cancel()

	short, cancel = impatient()
	for _, res := range client.DoMulti(short, get(), blpop()) {
		require.ErrorIs(t, res.Error(), context.DeadlineExceeded, "a pipeline with a blocking command waits, as a whole")
	}

	cancel()

	long := make(valkeygo.Commands, 0, valkeygo.DefaultBlockingPipeline)
	for range valkeygo.DefaultBlockingPipeline {
		long = append(long, get())
	}

	short, cancel = impatient()
	for _, res := range client.DoMulti(short, long...) {
		require.ErrorIs(t, res.Error(), context.DeadlineExceeded, "a pipeline long enough for a dedicated connection waits")
	}

	cancel()

	// Pipelined traffic does not queue behind the pool.
	short, cancel = impatient()
	v, err := client.Do(short, get()).ToString()
	require.NoError(t, err)
	assert.Equal(t, "v", v)

	for _, res := range client.DoMulti(short, get(), get()) {
		require.NoError(t, res.Error())
	}

	cancel()
	assert.Len(t, connections(t, observer, name), 2, "1 pipelined + the 1 dedicated, whatever was attempted")

	// Given back, the connection serves each path in turn.
	giveBack()
	require.True(t, valkeygo.IsValkeyNil(client.Do(ctx, blpop()).Error()))
	require.NoError(t, client.Dedicated(func(c valkeygo.DedicatedClient) error {
		return c.Do(ctx, c.B().Get().Key(key).Build()).Error()
	}))

	results := client.DoMulti(ctx, long...)
	require.Len(t, results, len(long))
	require.NoError(t, results[len(results)-1].Error())
	assert.Len(t, connections(t, observer, name), 2)
}

// Why this package enforces the bound itself: the library's pool does not
// hold under the overload it exists for. One caller whose context ends while
// it waits for a connection raises the limit by one for good. This test
// talks to the library directly and asserts the defect; when an upgrade
// makes it fail, bounded.go has nothing left to guard and can be removed.
func TestUpstreamPoolLimitLeaksOnACancelledWaiter(t *testing.T) {
	t.Parallel()

	const (
		poolSize = 2
		block    = 400 * time.Millisecond
	)

	observer := sharedCache(t)
	name := testx.Unique("upstream")

	opt, err := valkeygo.ParseURL(containers.Valkey(t) + "?client_name=" + name)
	require.NoError(t, err)

	opt.PipelineMultiplex = -1 // one pipelined connection
	opt.BlockingPoolSize = poolSize

	client, err := valkeygo.NewClient(opt)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx := context.Background()
	key := testx.Unique("empty-list")
	blpop := func(ctx context.Context) error {
		return client.Do(ctx, client.B().Blpop().Key(key).Timeout(block.Seconds()).Build()).Error()
	}

	var wg sync.WaitGroup
	for range 3 * poolSize {
		wg.Go(func() { _ = blpop(ctx) })
	}

	time.Sleep(block / 4) // the pool is full, the rest are waiting

	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, blpop(short), context.DeadlineExceeded)

	peak := 0
	for start := time.Now(); time.Since(start) < 2*block; time.Sleep(20 * time.Millisecond) {
		peak = max(peak, len(connections(t, observer, name)))
	}

	wg.Wait()
	assert.Greater(t, peak, 1+poolSize,
		"valkey-go no longer leaks its pool limit on a cancelled waiter: remove bounded.go and this test")
}

// Reads keep failing and succeeding while connections are reset and replies
// cut short — and nothing in the client races. With valkey-go's own retry on
// (≤ 1.0.79) this test fails under -race within seconds: a retried command is
// recycled while the writer of the connection that failed still reads it.
// The assertion is the race detector's; CI runs this layer with -race.
//
//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxy
func TestReadsUnderConnectionFaultsDoNotRace(t *testing.T) {
	proxy := containers.Proxied(t, "valkey")
	cache, _ := namedCache(t, "valkey://"+proxy.Addr, valkey.Config{OpTimeout: 200 * time.Millisecond})
	key := testx.Unique("faults")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		wg       sync.WaitGroup
		answered atomic.Int64
	)

	for range 256 {
		wg.Go(func() {
			for ctx.Err() == nil {
				if _, _, err := cache.Get(context.Background(), key); err == nil {
					answered.Add(1)
				}
			}
		})
	}

	for ctx.Err() == nil {
		proxy.Toxic("reset_peer", "downstream", map[string]any{"timeout": 0})
		time.Sleep(5 * time.Millisecond)
		proxy.Reset()
		time.Sleep(15 * time.Millisecond)
		proxy.Toxic("limit_data", "downstream", map[string]any{"bytes": 700})
		time.Sleep(15 * time.Millisecond)
		proxy.Reset()
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()
	proxy.Reset()

	assert.Positive(t, answered.Load(), "between the faults reads go through")
	assert.Eventually(t, func() bool { _, _, err := cache.Get(context.Background(), key); return err == nil },
		5*time.Second, 50*time.Millisecond, "and the client recovers once they stop")
}
