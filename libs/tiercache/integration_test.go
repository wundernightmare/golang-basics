package tiercache_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/tiercache"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// The integration layer: the same cache against a real Valkey. It owns what
// the fake cannot prove — that libs/valkey's commands mean what the fake
// says (PX, SET NX, SET IFEQ, tombstones, Pub/Sub) and that values survive
// the wire at every size. The read rules themselves are the unit layer's.

func valkeyAt(t *testing.T, url string) *valkey.Cache {
	t.Helper()

	remote, err := valkey.New(valkey.Config{URL: url, DialTimeout: 5 * time.Second, OpTimeout: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(remote.Close)

	return remote
}

// realTime is the TTLs of a test that runs on the wall clock.
func realTime(name string, soft, hard time.Duration) func(*tiercache.Config) {
	return func(cfg *tiercache.Config) {
		cfg.SoftTTL, cfg.HardTTL = soft, hard
		cfg.TombstoneTTL = 300 * time.Millisecond
		cfg.BreakerCooldown = 300 * time.Millisecond

		if name != "" {
			cfg.Name = name
		}
	}
}

// subscribed waits until the harness's invalidation subscription is live.
func (rig *harness) subscribed(t *testing.T) {
	t.Helper()

	require.Eventually(t, func() bool {
		return rig.metric(t, "tiercache_invalidations_total", map[string]string{"scope": "all"}) >= 1
	}, 5*time.Second, 10*time.Millisecond, "the invalidation subscription is established")
}

func TestValkeySharesAValueBetweenProcessesWithItsTTL(t *testing.T) {
	t.Parallel()

	remote := valkeyAt(t, containers.Valkey(t))
	src, key := newSource(), tiercache.KeyOf("a")
	src.set(key, 1)

	first := newHarness(t, remote, src, realTime("", time.Minute, time.Minute))
	second := newHarness(t, valkeyAt(t, containers.Valkey(t)), newSource(), realTime(first.name, time.Minute, time.Minute))
	first.subscribed(t)
	second.subscribed(t)

	require.Equal(t, "v1", first.version(t, key))
	assert.Equal(t, "v1", second.version(t, key), "the second process has no source: this came from Valkey")
	assert.InDelta(t, 1, second.reads(t, "l2"), 0)

	client := remote.Client()
	ttl, err := client.Do(t.Context(), client.B().Pttl().Key("tc:"+first.name+":"+string(key[:])).Build()).AsInt64()
	require.NoError(t, err)
	assert.Greater(t, ttl, (50 * time.Second).Milliseconds(), "the key expires with the value: HardTTL ±10%")
	assert.LessOrEqual(t, ttl, (66 * time.Second).Milliseconds())
}

func TestValkeyInvalidationReachesTheOtherProcess(t *testing.T) {
	t.Parallel()

	src, key := newSource(), tiercache.KeyOf("a")
	src.set(key, 1)

	writer := newHarness(t, valkeyAt(t, containers.Valkey(t)), src, realTime("", time.Minute, time.Minute))
	reader := newHarness(t, valkeyAt(t, containers.Valkey(t)), src, realTime(writer.name, time.Minute, time.Minute))
	writer.subscribed(t)
	reader.subscribed(t)

	require.Equal(t, "v1", writer.version(t, key))
	require.Equal(t, "v1", reader.version(t, key))

	src.set(key, 2)
	require.NoError(t, writer.cache.Invalidate(t.Context(), key))
	assert.Equal(t, "v2", writer.version(t, key))

	assert.Eventually(t, func() bool { return reader.version(t, key) == "v2" }, 5*time.Second, 10*time.Millisecond,
		"the reader's L1 entry is dropped by the published invalidation, and the tombstone hides the one in Valkey")

	// Once the tombstone is gone the new value is cached again, in both levels.
	require.Eventually(t, func() bool {
		loads := src.loads.Load()

		return reader.version(t, key) == "v2" && src.loads.Load() == loads
	}, 5*time.Second, 50*time.Millisecond)
}

func TestValkeyRefreshReplacesTheSharedValue(t *testing.T) {
	t.Parallel()

	src, key := newSource(), tiercache.KeyOf("a")
	src.set(key, 1)

	rig := newHarness(t, valkeyAt(t, containers.Valkey(t)), src, realTime("", 300*time.Millisecond, time.Minute))
	rig.subscribed(t)
	require.Equal(t, "v1", rig.version(t, key))

	src.set(key, 2)
	time.Sleep(400 * time.Millisecond)
	assert.Equal(t, "v1", rig.version(t, key), "stale: served, refreshed behind")
	require.Eventually(t, func() bool { return rig.version(t, key) == "v2" }, 5*time.Second, 10*time.Millisecond)

	other := newHarness(t, valkeyAt(t, containers.Valkey(t)), newSource(), realTime(rig.name, time.Minute, time.Minute))
	assert.Equal(t, "v2", other.version(t, key), "SET IFEQ replaced the value in Valkey")
}

func TestValkeyTombstoneRefusesAFillThatRacedTheWrite(t *testing.T) {
	t.Parallel()

	src, key := newSource(), tiercache.KeyOf("a")
	src.set(key, 1)

	rig := newHarness(t, valkeyAt(t, containers.Valkey(t)), src, realTime("", time.Minute, time.Minute))
	rig.subscribed(t)

	release := src.hold()
	racing := make(chan string, 1)

	go func() {
		version, _ := rig.read(key)
		racing <- version
	}()

	require.Eventually(t, func() bool { return src.inFlight.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	src.set(key, 2)
	require.NoError(t, rig.cache.Invalidate(t.Context(), key))
	release()

	assert.Equal(t, "v1", <-racing)
	assert.Equal(t, "v2", rig.version(t, key))

	other := newHarness(t, valkeyAt(t, containers.Valkey(t)), newSource(), realTime(rig.name, time.Minute, time.Minute))
	require.Eventually(t, func() bool {
		// The first read after the tombstone has expired fills Valkey again.
		if version, err := rig.read(key); err != nil || version != "v2" {
			return false
		}

		got, err := other.cache.Get(context.Background(), key)

		return err == nil && got.CRMID == "v2"
	}, 5*time.Second, 50*time.Millisecond, "Valkey ends up with the new value, never the old one")
}

func TestValkeyCarriesValuesOfEverySize(t *testing.T) {
	t.Parallel()

	// 65 000 is just under L1's per-entry limit, 66 000 just over it.
	for _, size := range []int{0, 1, 1000, 65_000, 66_000, 1 << 20, 4 << 20} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()

			src, key := newSource(), tiercache.KeyOf("a")
			src.set(key, 1)
			src.payload = size

			rig := newHarness(t, valkeyAt(t, containers.Valkey(t)), src, realTime("", time.Minute, time.Minute))
			other := newHarness(t, valkeyAt(t, containers.Valkey(t)), newSource(), realTime(rig.name, time.Minute, time.Minute))
			want := record(key, 1, size)

			for _, reader := range []*harness{rig, rig, other, other} {
				got, err := reader.cache.Get(t.Context(), key)
				require.NoError(t, err)
				require.Equal(t, want, got)
			}

			assert.Equal(t, int64(1), src.loads.Load())
		})
	}
}
