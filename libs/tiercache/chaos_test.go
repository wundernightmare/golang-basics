package tiercache_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/tiercache"
)

// The chaos layer: Valkey refuses, hangs, slows down or cuts replies short,
// and the cache must not notice beyond its metrics — every read answers,
// correctly, within a bound — and must pick L2 up again afterwards.
//
// All of it is serial: the Toxiproxy proxy and the container are shared.

// readBound is how long a read may take while Valkey is misbehaving: two L2
// calls at OpTimeout (200ms) each, the loader, and slack for a loaded CI box.
const readBound = 2 * time.Second

// answers reads every key through rig and checks value and latency.
func answers(t *testing.T, rig *harness, keys []tiercache.Key, payload int) {
	t.Helper()

	for _, key := range keys {
		start := time.Now()
		got, err := rig.cache.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, record(key, 1, payload), got)
		require.Less(t, time.Since(start), readBound)
	}
}

func seeded(src *source, prefix string, n int) []tiercache.Key {
	keys := make([]tiercache.Key, n)
	for i := range keys {
		keys[i] = tiercache.KeyOf(fmt.Sprint(prefix, i))
		src.set(keys[i], 1)
	}

	return keys
}

// recovers checks that L2 is in use again: a value loaded now reaches a
// process that can only get it from Valkey.
func recovers(t *testing.T, rig *harness, src *source, directURL string) {
	t.Helper()

	other := newHarness(t, valkeyAt(t, directURL), newSource(), realTime(rig.name, time.Minute, time.Minute))
	attempt := 0

	require.Eventually(t, func() bool {
		attempt++

		key := tiercache.KeyOf(fmt.Sprint("recovery", attempt))
		src.set(key, 1)

		if _, err := rig.cache.Get(context.Background(), key); err != nil {
			return false
		}

		got, err := other.cache.Get(context.Background(), key)

		return err == nil && got == record(key, 1, src.payload)
	}, 15*time.Second, 100*time.Millisecond, "L2 is used again once it is back")
}

//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxy
func TestChaosValkeyDown(t *testing.T) {
	proxy := containers.Proxied(t, "valkey")
	src := newSource()
	rig := newHarness(t, valkeyAt(t, "valkey://"+proxy.Addr), src, realTime("", time.Minute, time.Minute))
	rig.subscribed(t)

	warm := seeded(src, "warm", 20)
	answers(t, rig, warm, 0)

	resets := rig.metric(t, "tiercache_invalidations_total", map[string]string{"scope": "all"})

	proxy.Down()

	loads := src.loads.Load()

	answers(t, rig, warm, 0)
	assert.Equal(t, loads, src.loads.Load(), "warm keys are served from L1")

	// Enough failures to outweigh the successful calls still in the
	// breaker's window from the warm-up.
	answers(t, rig, seeded(src, "cold", 60), 0)
	assert.Positive(t, rig.metric(t, "tiercache_l2_bypassed_total", nil), "the breaker spared the remaining reads the wait")
	require.Error(t, rig.cache.Invalidate(context.Background(), warm[0]))

	proxy.Up()
	recovers(t, rig, src, containers.Valkey(t))
	assert.Eventually(t, func() bool {
		return rig.metric(t, "tiercache_invalidations_total", map[string]string{"scope": "all"}) > resets
	}, 15*time.Second, 100*time.Millisecond, "the subscription came back and L1 started over")
}

//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxy
func TestChaosValkeyMisbehaves(t *testing.T) {
	// Values of 5 kB, so a reply can be cut in the middle.
	const payload = 5000

	for name, toxic := range map[string]func(*containers.Proxy){
		"slow replies":          func(p *containers.Proxy) { p.Latency(time.Second) },
		"black hole":            func(p *containers.Proxy) { p.Toxic("timeout", "downstream", map[string]any{"timeout": 0}) },
		"requests never arrive": func(p *containers.Proxy) { p.Toxic("timeout", "upstream", map[string]any{"timeout": 0}) },
		"connection reset":      func(p *containers.Proxy) { p.Toxic("reset_peer", "downstream", map[string]any{"timeout": 0}) },
		"reply cut mid-value":   func(p *containers.Proxy) { p.Toxic("limit_data", "downstream", map[string]any{"bytes": 1500}) },
		"reply in delayed drops": func(p *containers.Proxy) {
			p.Toxic("slicer", "downstream", map[string]any{"average_size": 64, "size_variation": 32, "delay": 20_000})
		},
	} {
		t.Run(name, func(t *testing.T) { //nolint:paralleltest // serial: (same)
			proxy := containers.Proxied(t, "valkey")
			src := newSource()
			src.payload = payload

			rig := newHarness(t, valkeyAt(t, "valkey://"+proxy.Addr), src, realTime("", time.Minute, time.Minute))
			rig.subscribed(t)

			// Values already in Valkey that this process has not seen: reading
			// them is what puts a large reply on the damaged connection.
			inValkey := seeded(src, "shared", 10)
			answers(t, newHarness(t, valkeyAt(t, containers.Valkey(t)), src, realTime(rig.name, time.Minute, time.Minute)), inValkey, payload)

			warm := seeded(src, "warm", 10)
			answers(t, rig, warm, payload)

			toxic(proxy)

			answers(t, rig, inValkey, payload)
			answers(t, rig, seeded(src, "cold", 10), payload)
			answers(t, rig, warm, payload)

			proxy.Reset()
			recovers(t, rig, src, containers.Valkey(t))
		})
	}
}

// A server that accepts connections and answers nothing — a stall, a long
// fork, a saturated event loop. CLIENT PAUSE is that state on demand, and it
// ends by itself; containers.Pause cannot produce it here, because in the
// Valkey image the server is PID 1, which `kill -STOP -1` does not reach.
//
//nolint:paralleltest // serial: stalls the shared Valkey for every client
func TestChaosValkeyFrozen(t *testing.T) {
	const frozenFor = 5 * time.Second

	src := newSource()
	rig := newHarness(t, valkeyAt(t, containers.Valkey(t)), src, realTime("", time.Minute, time.Minute))
	rig.subscribed(t)

	// Few warm keys: their successful L2 calls are in the breaker's window,
	// and it opens only once the failures outweigh them.
	warm := seeded(src, "warm", 2)
	answers(t, rig, warm, 0)

	admin := valkeyAt(t, containers.Valkey(t)).Client()
	require.NoError(t, admin.Do(context.Background(), admin.B().ClientPause().Timeout(frozenFor.Milliseconds()).All().Build()).Error())

	frozenAt := time.Now()

	answers(t, rig, warm, 0)
	answers(t, rig, seeded(src, "cold", 10), 0)
	require.Less(t, time.Since(frozenAt), frozenFor, "all of that happened while Valkey was not answering")
	assert.Positive(t, rig.metric(t, "tiercache_l2_bypassed_total", nil))

	recovers(t, rig, src, containers.Valkey(t))
}

// The soak: two processes read and invalidate a small keyspace as fast as
// they can while Valkey flaps between healthy, down, slow and resetting.
// Throughout: no read fails, no read returns another key's value or a version
// that was never committed. Afterwards: both converge on the latest versions.
//
//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxy
func TestChaosSoak(t *testing.T) {
	const (
		keyCount = 200
		duration = 6 * time.Second
		hard     = 2 * time.Second
	)

	proxy := containers.Proxied(t, "valkey")
	src := newSource()
	keys := seeded(src, "soak", keyCount)
	committed := make([]atomic.Int64, keyCount)

	for i := range committed {
		committed[i].Store(1)
	}

	first := newHarness(t, valkeyAt(t, "valkey://"+proxy.Addr), src, realTime("", 200*time.Millisecond, hard))
	second := newHarness(t, valkeyAt(t, "valkey://"+proxy.Addr), src, realTime(first.name, 200*time.Millisecond, hard))
	processes := []*harness{first, second}

	first.subscribed(t)
	second.subscribed(t)

	ctx, stop := context.WithTimeout(context.Background(), duration)
	defer stop()

	var (
		wg     sync.WaitGroup
		reads  atomic.Int64
		writes atomic.Int64
	)

	for worker := range 16 {
		wg.Go(func() {
			rig := processes[worker%len(processes)]

			for ctx.Err() == nil {
				i := rand.IntN(keyCount)

				// A read may start before a commit and finish after it, so the
				// bound it is checked against is taken after the read.
				got, err := rig.cache.Get(context.Background(), keys[i])
				latest := committed[i].Load()

				if err != nil {
					t.Errorf("a read failed: %v", err)

					return
				}

				var version int64

				if _, scanErr := fmt.Sscanf(got.CRMID, "v%d", &version); scanErr != nil || got != record(keys[i], int(version), 0) {
					t.Errorf("not an intact value of this key: %+v", got)

					return
				}

				if version > latest {
					t.Errorf("version %d was never committed (latest %d)", version, latest)

					return
				}

				reads.Add(1)
			}
		})
	}

	// One writer per process, each owning its half of the keyspace, so the
	// versions of a key are committed in order.
	for worker, rig := range processes {
		wg.Go(func() {
			for ctx.Err() == nil {
				i := rand.IntN(keyCount/2)*2 + worker

				// The bound goes up before the value is published: a reader
				// that loads the new value and then reads the bound must
				// never find the bound still behind it.
				next := committed[i].Load() + 1
				committed[i].Store(next)
				src.set(keys[i], int(next))

				_ = rig.cache.Invalidate(context.Background(), keys[i]) // fails while Valkey is away: that is the test

				writes.Add(1)

				time.Sleep(2 * time.Millisecond)
			}
		})
	}

	wg.Go(func() {
		faults := []func(){
			proxy.Down,
			func() { proxy.Latency(400 * time.Millisecond) },
			func() { proxy.Toxic("reset_peer", "downstream", map[string]any{"timeout": 0}) },
			func() { proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0}) },
		}

		for step := 0; ctx.Err() == nil; step++ {
			faults[step%len(faults)]()
			time.Sleep(400 * time.Millisecond)
			proxy.Reset()
			time.Sleep(300 * time.Millisecond)
		}
	})

	wg.Wait()
	proxy.Reset()

	t.Logf("soak: %d reads, %d writes", reads.Load(), writes.Load())
	assert.Greater(t, reads.Load(), int64(10_000))
	assert.Greater(t, writes.Load(), int64(100))

	// An invalidation that could not be published is bounded by HardTTL.
	require.Eventually(t, func() bool {
		for _, rig := range processes {
			for i, key := range keys {
				got, err := rig.cache.Get(context.Background(), key)
				if err != nil || got.CRMID != fmt.Sprintf("v%d", committed[i].Load()) {
					return false
				}
			}
		}

		return true
	}, 5*hard, 100*time.Millisecond, "both processes converge on the latest versions")
}
