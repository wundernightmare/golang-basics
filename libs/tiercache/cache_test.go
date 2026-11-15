package tiercache_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/tiercache"
	"github.com/tracehubmmp/golang-basics/libs/tiercache/credential"
)

// One Valkey per test binary for the integration and chaos suites; the leak
// check behind it covers the unit tests too.
func TestMain(m *testing.M) {
	fullPropertyChecks()
	os.Exit(containers.Main(m))
}

// The unit tests run in synctest bubbles: time is virtual, so TTLs of
// minutes cost nothing, and synctest.Wait() is "every goroutine the cache
// started has gone as far as it can" — no sleeps, no polling.

func sameName(name string) func(*tiercache.Config) {
	return func(cfg *tiercache.Config) { cfg.Name = name }
}

func TestFreshValueIsServedFromL1Alone(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, remote, src, nil)

		synctest.Wait()

		assert.Equal(t, "v1", rig.version(t, key))
		assert.Equal(t, int64(1), src.loads.Load())
		assert.Equal(t, int64(1), remote.writes.Load(), "the load was written to L2")

		time.Sleep(5 * time.Second)

		for range 10 {
			assert.Equal(t, "v1", rig.version(t, key))
		}

		assert.Equal(t, int64(1), src.loads.Load(), "no further loads")
		assert.Equal(t, int64(1), remote.gets.Load(), "no further L2 lookups")
		assert.InDelta(t, 1, rig.reads(t, "load"), 0)
		assert.InDelta(t, 10, rig.reads(t, "fresh"), 0)
	})
}

func TestStaleValueIsServedAndRefreshedBehind(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, remote, src, nil)

		synctest.Wait()
		require.Equal(t, "v1", rig.version(t, key))

		src.set(key, 2)
		time.Sleep(20 * time.Second)

		release := src.hold()

		var wg sync.WaitGroup
		for range 100 {
			wg.Go(func() {
				if version, err := rig.read(key); err != nil || version != "v1" {
					t.Errorf("want the stale value without waiting for the refresh, got %q, %v", version, err)
				}
			})
		}

		wg.Wait()
		synctest.Wait()
		assert.Equal(t, int64(2), src.loads.Load(), "a hundred stale reads are one refresh")
		assert.InDelta(t, 100, rig.reads(t, "stale"), 0)

		release()
		synctest.Wait()
		assert.Equal(t, "v2", rig.version(t, key))
		assert.InDelta(t, 1, rig.metric(t, "tiercache_refreshes_total", map[string]string{"result": "ok"}), 0)

		// The refresh replaced the value in L2 as well: a second process
		// (whose source has nothing) gets it from there.
		other := newHarness(t, remote, newSource(), sameName(rig.name))

		synctest.Wait()
		assert.Equal(t, "v2", other.version(t, key))
		assert.InDelta(t, 1, other.reads(t, "l2"), 0)
	})
}

func TestExpiredValueIsNeverServed(t *testing.T) {
	t.Parallel()

	for name, remote := range map[string]tiercache.Remote{"with L2": newFakeRemote(), "L1 only": nil} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				src, key := newSource(), tiercache.KeyOf("a")
				src.set(key, 1)

				rig := newHarness(t, remote, src, nil)

				synctest.Wait()
				require.Equal(t, "v1", rig.version(t, key))

				src.set(key, 2)
				time.Sleep(70 * time.Second)
				assert.Equal(t, "v2", rig.version(t, key), "past HardTTL the read waits for the source")

				src.fail(errors.New("database down"))
				time.Sleep(70 * time.Second)

				_, err := rig.cache.Get(t.Context(), key)
				require.ErrorContains(t, err, "database down", "no StaleIfError: an expired value is not an answer")
				assert.InDelta(t, 1, rig.reads(t, "error"), 0)
			})
		})
	}
}

func TestStaleIfErrorServesAnExpiredValueForItsGraceOnly(t *testing.T) {
	t.Parallel()

	for name, remote := range map[string]tiercache.Remote{"with L2": newFakeRemote(), "L1 only": nil} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				src, key := newSource(), tiercache.KeyOf("a")
				src.set(key, 1)

				rig := newHarness(t, remote, src, func(cfg *tiercache.Config) { cfg.StaleIfError = time.Minute })

				synctest.Wait()
				require.Equal(t, "v1", rig.version(t, key))

				src.fail(errors.New("database down"))
				time.Sleep(100 * time.Second)
				assert.Equal(t, "v1", rig.version(t, key), "expired, within the grace, and the source is failing")
				assert.InDelta(t, 1, rig.reads(t, "stale_error"), 0)

				time.Sleep(100 * time.Second)

				_, err := rig.cache.Get(t.Context(), key)
				require.ErrorContains(t, err, "database down", "past the grace")

				src.fail(nil)
				src.set(key, 2)
				assert.Equal(t, "v2", rig.version(t, key), "the source is back")
			})
		})
	}
}

// A second process whose L1 is cold finds the value in L2 — also when its L1
// lost it, and also when only its grace keeps it there.
func TestStaleIfErrorFallsBackToL2(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		grace := func(cfg *tiercache.Config) { cfg.StaleIfError = time.Minute }
		first := newHarness(t, remote, src, grace)

		synctest.Wait()
		require.Equal(t, "v1", first.version(t, key))

		failing := newSource()
		failing.fail(errors.New("database down"))

		second := newHarness(t, remote, failing, func(cfg *tiercache.Config) { grace(cfg); cfg.Name = first.name })

		synctest.Wait()
		time.Sleep(100 * time.Second)
		assert.Equal(t, "v1", second.version(t, key))
		assert.InDelta(t, 1, second.reads(t, "stale_error"), 0)
	})
}

func TestUnreachableL2NeverFailsARead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src := newFakeRemote(), newSource()
		remote.fail(errRemoteDown)

		rig := newHarness(t, remote, src, nil)

		synctest.Wait()

		keys := make([]string, 20)
		for i := range keys {
			keys[i] = tiercache.KeyOf(fmt.Sprint("k", i))
			src.set(keys[i], 1)
			assert.Equal(t, "v1", rig.version(t, keys[i]))
		}

		assert.Less(t, remote.gets.Load(), int64(10), "the breaker stopped asking")
		assert.Positive(t, rig.metric(t, "tiercache_l2_bypassed_total", nil))

		for _, key := range keys {
			assert.Equal(t, "v1", rig.version(t, key))
		}

		assert.Equal(t, int64(20), src.loads.Load(), "what was loaded is served from L1")

		// Stale-while-revalidate keeps working on the loader alone.
		src.set(keys[0], 2)
		time.Sleep(20 * time.Second)
		assert.Equal(t, "v1", rig.version(t, keys[0]))
		synctest.Wait()
		assert.Equal(t, "v2", rig.version(t, keys[0]))

		require.Error(t, rig.cache.Invalidate(t.Context(), keys[1]), "the writer is told nobody else heard")
		src.set(keys[1], 2)
		assert.Equal(t, "v2", rig.version(t, keys[1]), "…but this process dropped it")

		// L2 comes back: it is used again, and L1 starts over, because the
		// invalidations published meanwhile are unknown.
		remote.fail(nil)

		gets := remote.gets.Load()
		resets := rig.metric(t, "tiercache_invalidations_total", map[string]string{"scope": "all"})

		time.Sleep(6 * time.Second)
		synctest.Wait()
		assert.Greater(t, rig.metric(t, "tiercache_invalidations_total", map[string]string{"scope": "all"}), resets)
		assert.Equal(t, "v1", rig.version(t, keys[2]))
		assert.Greater(t, remote.gets.Load(), gets)
		assert.Positive(t, remote.writes.Load())
	})
}

func TestInvalidateReachesEveryProcess(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		writer := newHarness(t, remote, src, nil)
		reader := newHarness(t, remote, src, sameName(writer.name))

		synctest.Wait()
		require.Equal(t, "v1", writer.version(t, key))
		require.Equal(t, "v1", reader.version(t, key))

		fromL2 := reader.reads(t, "l2")

		src.set(key, 2)
		require.NoError(t, writer.cache.Invalidate(t.Context(), key))
		synctest.Wait()

		assert.Equal(t, "v2", writer.version(t, key))
		assert.Equal(t, "v2", reader.version(t, key))
		assert.InDelta(t, fromL2, reader.reads(t, "l2"), 0, "the tombstone hides the old value in L2")

		// While the tombstone lasts nothing is cached; afterwards it is.
		time.Sleep(6 * time.Second)
		require.Equal(t, "v2", reader.version(t, key))

		loads := src.loads.Load()

		assert.Equal(t, "v2", reader.version(t, key))
		assert.Equal(t, "v2", writer.version(t, key))
		assert.Equal(t, loads, src.loads.Load())
	})
}

// The resurrection race: a reader holds the old row across the writer's
// commit and invalidation. It may return the old row once — it asked before
// the commit — but must not leave it in any level.
func TestLoadRacingAnInvalidationIsNotCached(t *testing.T) {
	t.Parallel()

	for name, remote := range map[string]tiercache.Remote{"with L2": newFakeRemote(), "L1 only": nil} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				src, key := newSource(), tiercache.KeyOf("a")
				src.set(key, 1)

				rig := newHarness(t, remote, src, nil)

				synctest.Wait()

				release := src.hold()
				racing := make(chan string, 1)

				go func() {
					version, _ := rig.read(key)
					racing <- version
				}()

				synctest.Wait() // the reader has read v1 and is stalled

				src.set(key, 2)
				require.NoError(t, rig.cache.Invalidate(t.Context(), key))
				release()

				assert.Equal(t, "v1", <-racing)
				assert.Equal(t, "v2", rig.version(t, key), "the old row did not stay in L1")

				time.Sleep(6 * time.Second) // past the tombstone
				assert.Equal(t, "v2", rig.version(t, key), "nor in L2")
			})
		})
	}
}

// Read your own write: a read that starts after Invalidate has returned
// never joins a lookup that started before it — that lookup may hold the row
// the write replaced. The reader that was already waiting still gets it (it
// asked before the write); the next one asks again.
func TestAReadAfterAnInvalidationDoesNotJoinAnOlderLookup(t *testing.T) {
	t.Parallel()

	// How the lookup that holds the old row gets started, once the source
	// stalls: by a miss, or behind a stale read.
	starts := map[string]struct {
		prime func(t *testing.T, rig *harness, key string)
		start func(t *testing.T, rig *harness, key string)
	}{
		"a miss": {
			prime: func(*testing.T, *harness, string) {},
			start: func(_ *testing.T, rig *harness, key string) {
				go func() { _, _ = rig.read(key) }()
			},
		},
		"a background refresh": {
			prime: func(t *testing.T, rig *harness, key string) {
				t.Helper()
				require.Equal(t, "v1", rig.version(t, key))
				time.Sleep(20 * time.Second) // stale: the next read refreshes behind
			},
			start: func(t *testing.T, rig *harness, key string) {
				t.Helper()
				require.Equal(t, "v1", rig.version(t, key))
			},
		},
	}

	// What tells this process that the row has changed.
	signals := map[string]func(t *testing.T, rig, other *harness, remote *fakeRemote, key string){
		"its own Invalidate": func(t *testing.T, rig, _ *harness, _ *fakeRemote, key string) {
			t.Helper()
			require.NoError(t, rig.cache.Invalidate(t.Context(), key))
		},
		"another process's Invalidate": func(t *testing.T, _, other *harness, _ *fakeRemote, key string) {
			t.Helper()
			require.NoError(t, other.cache.Invalidate(t.Context(), key))
		},
		// The write's tombstone is in L2, its message was lost: all this
		// process learns is that its subscription came back.
		"a reconnected subscription": func(t *testing.T, rig, _ *harness, remote *fakeRemote, key string) {
			t.Helper()
			require.NoError(t, remote.Tombstone(t.Context(), "tc:"+rig.name+":"+key, 5*time.Second))
			remote.reconnect()
		},
	}

	for startName, start := range starts {
		for signalName, signal := range signals {
			t.Run(startName+", then "+signalName, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					remote, src, key := newFakeRemote(), newSource(), "tenant:42"
					src.set(key, 1)

					rig := newHarness(t, remote, src, nil)
					other := newHarness(t, remote, src, sameName(rig.name))

					synctest.Wait()
					start.prime(t, rig, key)

					release := src.hold()

					start.start(t, rig, key)
					synctest.Wait() // a lookup has read v1 and is stalled

					src.set(key, 2)
					signal(t, rig, other, remote, key)
					synctest.Wait()

					after := make(chan string, 1)

					go func() {
						version, _ := rig.read(key)
						after <- version
					}()

					synctest.Wait()
					release() // every stalled load goes on

					assert.Equal(t, "v2", <-after, "a read that began after the write")
					synctest.Wait()
					assert.Equal(t, "v2", rig.version(t, key), "and the old row stayed nowhere")
				})
			})
		}
	}
}

// The same race one level down: the old value is read from L2, and the
// invalidation lands before it is copied into L1.
func TestL2HitRacingAnInvalidationIsNotCached(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		writer := newHarness(t, remote, src, nil)
		reader := newHarness(t, remote, src, sameName(writer.name))

		synctest.Wait()
		require.Equal(t, "v1", writer.version(t, key))

		var once sync.Once

		remote.mu.Lock()
		remote.beforeGetReturns = func() {
			once.Do(func() {
				src.set(key, 2)
				assert.NoError(t, writer.cache.Invalidate(context.Background(), key))
			})
		}
		remote.mu.Unlock()

		assert.Equal(t, "v1", reader.version(t, key), "read from L2 before the invalidation")
		assert.Equal(t, "v2", reader.version(t, key), "and not kept in L1 after it")
	})
}

func TestReconnectedSubscriptionEmptiesL1(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, remote, src, nil)

		synctest.Wait()
		require.Equal(t, "v1", rig.version(t, key))
		require.Equal(t, "v1", rig.version(t, key))
		require.InDelta(t, 1, rig.reads(t, "fresh"), 0)

		remote.reconnect()

		assert.Equal(t, "v1", rig.version(t, key))
		assert.InDelta(t, 1, rig.reads(t, "l2"), 0, "L1 was emptied: the value came from L2 again")
		assert.InDelta(t, 2, rig.metric(t, "tiercache_invalidations_total", map[string]string{"scope": "all"}), 0)
	})
}

func TestWithoutInvalidationThereIsNoSubscription(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		off := func(cfg *tiercache.Config) { cfg.DisableInvalidation = true }
		writer := newHarness(t, remote, src, off)
		reader := newHarness(t, remote, src, func(cfg *tiercache.Config) { off(cfg); cfg.Name = writer.name })

		synctest.Wait()
		require.Equal(t, "v1", reader.version(t, key))

		src.set(key, 2)
		require.NoError(t, writer.cache.Invalidate(t.Context(), key))
		synctest.Wait()

		assert.Empty(t, remote.subs)
		assert.Equal(t, "v2", writer.version(t, key))
		assert.Equal(t, "v1", reader.version(t, key), "other processes keep theirs until the TTL")
	})
}

func TestNotFoundIsNotCachedAndADeletedRowStopsBeingServed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")

		rig := newHarness(t, remote, src, nil)

		synctest.Wait()

		_, err := rig.cache.Get(t.Context(), key)
		require.ErrorIs(t, err, tiercache.ErrNotFound)
		assert.InDelta(t, 1, rig.reads(t, "not_found"), 0)

		src.set(key, 1)
		require.Equal(t, "v1", rig.version(t, key), "a miss is not remembered")

		// The row is deleted without an Invalidate: the refresh notices.
		src.mu.Lock()
		delete(src.versions, key)
		src.mu.Unlock()

		time.Sleep(20 * time.Second)
		assert.Equal(t, "v1", rig.version(t, key), "stale, served once more")
		synctest.Wait()

		_, err = rig.cache.Get(t.Context(), key)
		require.ErrorIs(t, err, tiercache.ErrNotFound, "dropped from L1 and from L2")
	})
}

func TestCorruptL2EntriesAreReplaced(t *testing.T) {
	t.Parallel()

	header := string([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0x3f, 0, 0, 0, 0, 0, 0, 0}) // version 1, soft 1, hard far away

	for name, raw := range map[string]string{
		"not an envelope":          "{\"id\":1,\"name\":\"left by another writer\"}",
		"a future format version":  "\x02" + header[1:] + "payload",
		"a payload that is not it": header + "\x0a\x05a",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
				src.set(key, 1)

				rig := newHarness(t, remote, src, nil)

				synctest.Wait()
				remote.put(rig.name, key, raw)

				assert.Equal(t, "v1", rig.version(t, key))
				assert.InDelta(t, 1, rig.reads(t, "load"), 0)

				other := newHarness(t, remote, newSource(), sameName(rig.name))

				synctest.Wait()
				assert.Equal(t, "v1", other.version(t, key), "L2 now holds a good entry")
			})
		})
	}
}

func TestEntryTooLargeForL1IsServedFromL2(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)
		src.payload = 70_000

		rig := newHarness(t, remote, src, nil)

		synctest.Wait()

		for range 3 {
			got, err := rig.cache.Get(t.Context(), key)
			require.NoError(t, err)
			assert.Equal(t, record(key, 1, 70_000), got)
		}

		assert.InDelta(t, 1, rig.reads(t, "load"), 0)
		assert.InDelta(t, 2, rig.reads(t, "l2"), 0)
		assert.InDelta(t, 3, rig.metric(t, "tiercache_l1_oversize_total", nil), 0)
	})
}

func TestBackgroundRefreshesAreBounded(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		src := newSource()
		rig := newHarness(t, nil, src, func(cfg *tiercache.Config) { cfg.RefreshConcurrency = 2 })

		keys := make([]string, 10)
		for i := range keys {
			keys[i] = tiercache.KeyOf(fmt.Sprint("k", i))
			src.set(keys[i], 1)
			require.Equal(t, "v1", rig.version(t, keys[i]))
		}

		time.Sleep(20 * time.Second)

		release := src.hold()

		for _, key := range keys {
			assert.Equal(t, "v1", rig.version(t, key))
		}

		synctest.Wait()
		assert.Equal(t, int64(2), src.inFlight.Load())
		assert.InDelta(t, 8, rig.metric(t, "tiercache_refreshes_total", map[string]string{"result": "dropped"}), 0)

		release()
		synctest.Wait()
		assert.Equal(t, int64(2), src.peak.Load())
	})
}

func TestFailedRefreshKeepsTheStaleValue(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		src, key := newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, newFakeRemote(), src, nil)

		synctest.Wait()
		require.Equal(t, "v1", rig.version(t, key))

		src.fail(errors.New("database down"))
		time.Sleep(20 * time.Second)

		for range 3 {
			assert.Equal(t, "v1", rig.version(t, key))
			synctest.Wait()
		}

		assert.InDelta(t, 3, rig.metric(t, "tiercache_refreshes_total", map[string]string{"result": "error"}), 0)
	})
}

func TestEachCallerHonoursItsOwnContext(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		src, key := newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, nil, src, nil)
		release := src.hold()

		impatient, cancel := context.WithCancel(t.Context())
		first, second := make(chan error, 1), make(chan error, 1)

		go func() { _, err := rig.cache.Get(impatient, key); first <- err }()

		synctest.Wait()

		go func() { _, err := rig.cache.Get(t.Context(), key); second <- err }()

		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-first, context.Canceled)

		release()
		require.NoError(t, <-second, "the shared load outlived the caller that started it")
		assert.Equal(t, int64(1), src.loads.Load())
	})
}

func TestCallerDeadlineBoundsTheLoad(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		src, key := newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, nil, src, nil)
		t.Cleanup(src.hold())

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		start := time.Now()
		_, err := rig.cache.Get(ctx, key)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, time.Second, time.Since(start))

		synctest.Wait()
		assert.Zero(t, src.inFlight.Load(), "the load was cancelled with it")
	})
}

func TestCloseStopsARunningRefresh(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		src, key := newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, newFakeRemote(), src, nil)

		synctest.Wait()
		require.Equal(t, "v1", rig.version(t, key))

		time.Sleep(20 * time.Second)
		t.Cleanup(src.hold())
		require.Equal(t, "v1", rig.version(t, key))
		synctest.Wait()
		require.Equal(t, int64(1), src.inFlight.Load())

		rig.cache.Close() // returns: the refresh is cancelled, not waited out for RefreshTimeout
		assert.Zero(t, src.inFlight.Load())
	})
}

func TestNewRejectsWhatItCannotRunWith(t *testing.T) {
	t.Parallel()

	load := newSource().load
	good := tiercache.Config{Name: "c", HardTTL: time.Minute}

	_, err := tiercache.New(tiercache.Config{HardTTL: time.Minute}, credential.Codec{}, load, nil, nil)
	require.ErrorContains(t, err, "Name")

	_, err = tiercache.New[credential.Credential](good, nil, load, nil, nil)
	require.ErrorContains(t, err, "codec and loader")

	_, err = tiercache.New(good, credential.Codec{}, nil, nil, nil)
	require.ErrorContains(t, err, "codec and loader")
}

// The budget of the hot path: a fresh read allocates what Decode returns —
// three strings for a credential — and nothing of the cache's own.
//
//nolint:paralleltest // serial: testing.AllocsPerRun refuses to run in a parallel test
func TestFreshReadAllocatesOnlyTheDecodedValue(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes sync.Pool behaviour; allocation budgets are measured without it")
	}

	src, key := newSource(), tiercache.KeyOf("a")
	src.set(key, 1)

	rig := newHarness(t, nil, src, nil)
	require.Equal(t, "v1", rig.version(t, key))

	ctx := context.Background()
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := rig.cache.Get(ctx, key); err != nil {
			t.Fatal(err)
		}
	})
	assert.InDelta(t, 3, allocs, 0, "one per string field")

	counters, err := tiercache.New(tiercache.Config{Name: "allocs", HardTTL: time.Minute}, fixedCodec{},
		func(context.Context, string) (uint64, error) { return 42, nil }, nil, nil)
	require.NoError(t, err)
	t.Cleanup(counters.Close)

	_, err = counters.Get(ctx, key)
	require.NoError(t, err)

	allocs = testing.AllocsPerRun(1000, func() {
		if v, err := counters.Get(ctx, key); err != nil || v != 42 {
			t.Fatal(v, err)
		}
	})
	assert.Zero(t, allocs, "a value without pointers: nothing at all")

	// A key is lent to L1, not copied: the longest one costs no more.
	long := strings.Repeat("k", tiercache.MaxKeyLen)

	_, err = counters.Get(ctx, long)
	require.NoError(t, err)

	allocs = testing.AllocsPerRun(1000, func() {
		if v, err := counters.Get(ctx, long); err != nil || v != 42 {
			t.Fatal(v, err)
		}
	})
	assert.Zero(t, allocs, "whatever the length of the key")
}

// A key is not hashed on the way: the loader is asked for it as it is, L2
// holds it under "tc:<name>:<key>", and the other processes are told it.
func TestAKeyTravelsAsItIs(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		for _, key := range []string{"42", "tenant:7/user:Ёж", "0190f3a2-7b1c-7e7e-9c1d-2f6a5b3c4d5e", strings.Repeat("k", tiercache.MaxKeyLen), tiercache.KeyOf("secret")} {
			remote, src := newFakeRemote(), newSource()
			src.set(key, 1)

			writer := newHarness(t, remote, src, nil)
			reader := newHarness(t, remote, src, sameName(writer.name))

			synctest.Wait()
			require.Equal(t, "v1", writer.version(t, key), "the loader was asked for the key itself")

			_, inL2, err := remote.Get(t.Context(), "tc:"+writer.name+":"+key)
			require.NoError(t, err)
			require.True(t, inL2)

			require.Equal(t, "v1", reader.version(t, key))
			require.InDelta(t, 1, reader.reads(t, "l2"), 0)
			require.Equal(t, "v1", reader.version(t, key))
			require.InDelta(t, 1, reader.reads(t, "fresh"), 0, "and it is in L1 under that key")

			src.set(key, 2)
			require.NoError(t, writer.cache.Invalidate(t.Context(), key))
			synctest.Wait()

			assert.Equal(t, "v2", reader.version(t, key))
			assert.InDelta(t, 1, reader.metric(t, "tiercache_invalidations_total", map[string]string{"scope": "key"}), 0)
		}
	})
}

// KeyOf is the SHA-256 and nothing else: what a cache wrote under it before
// keys were strings is still found.
func TestKeyOfIsTheSHA256(t *testing.T) {
	t.Parallel()

	sum := sha256.Sum256([]byte("an API key"))
	assert.Equal(t, string(sum[:]), tiercache.KeyOf("an API key"))
	assert.Len(t, tiercache.KeyOf(""), 32)
	assert.Len(t, tiercache.KeyOf(strings.Repeat("k", 10_000)), 32)
}

// An empty key and one over MaxKeyLen are refused before anything is asked:
// not the loader, not L2, not the channel.
func TestAKeyOutOfBoundsIsRefused(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src := newFakeRemote(), newSource()
		writer := newHarness(t, remote, src, nil)
		reader := newHarness(t, remote, src, sameName(writer.name))

		synctest.Wait()

		for _, key := range []string{"", strings.Repeat("k", tiercache.MaxKeyLen+1)} {
			src.set(key, 1)

			_, err := writer.cache.Get(t.Context(), key)
			require.ErrorIs(t, err, tiercache.ErrKey)
			require.ErrorIs(t, writer.cache.Invalidate(t.Context(), key), tiercache.ErrKey)

			// Nor is one taken from the channel: it carries keys only.
			require.NoError(t, remote.Publish(t.Context(), "tc:"+writer.name+":inv", key))
		}

		synctest.Wait()

		assert.Zero(t, src.loads.Load())
		assert.InDelta(t, 2, writer.reads(t, "error"), 0)

		remote.mu.Lock()
		assert.Empty(t, remote.data)
		remote.mu.Unlock()

		for _, rig := range []*harness{writer, reader} {
			assert.Zero(t, rig.metric(t, "tiercache_invalidations_total", map[string]string{"scope": "key"}))
		}

		// The longest key is a key.
		longest := strings.Repeat("k", tiercache.MaxKeyLen)
		src.set(longest, 1)
		assert.Equal(t, "v1", writer.version(t, longest))
		require.NoError(t, writer.cache.Invalidate(t.Context(), longest))
	})
}

// fixedCodec stores a uint64: a codec whose Decode allocates nothing.
type fixedCodec struct{}

func (fixedCodec) Append(dst []byte, v uint64) ([]byte, error) {
	return append(dst, byte(v), byte(v>>8), byte(v>>16), byte(v>>24), byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56)), nil
}

func (fixedCodec) Decode(src []byte) (uint64, error) {
	if len(src) != 8 {
		return 0, errors.New("fixedCodec: want 8 bytes")
	}

	var v uint64
	for i, b := range src {
		v |= uint64(b) << (8 * i)
	}

	return v, nil
}

// Writing several times its capacity into L1 leaves it at its capacity, and
// the most recent entries readable.
func TestL1StaysWithinItsCapacity(t *testing.T) {
	t.Parallel()

	const capacity = 32 << 20

	src := newSource()
	src.payload = 1000

	rig := newHarness(t, nil, src, func(cfg *tiercache.Config) { cfg.MaxBytes = capacity })

	const entries = 100_000 // ≈ 110 MiB of envelopes

	keys := make([]string, entries)
	for i := range keys {
		keys[i] = tiercache.KeyOf(fmt.Sprint("k", i))
		src.set(keys[i], 1)

		_, err := rig.cache.Get(t.Context(), keys[i])
		require.NoError(t, err)
	}

	held := rig.metric(t, "tiercache_l1_bytes", nil)
	assert.LessOrEqual(t, held, float64(capacity))
	assert.Greater(t, held, float64(capacity)*0.9, "and it is actually used")

	loads := src.loads.Load()

	for _, key := range keys[entries-1000:] {
		require.Equal(t, record(key, 1, 1000).KeyEncrypted, func() string {
			got, err := rig.cache.Get(t.Context(), key)
			require.NoError(t, err)

			return got.KeyEncrypted
		}())
	}

	assert.Less(t, src.loads.Load()-loads, int64(50), "the newest entries are still in L1")
}

func BenchmarkGetFresh(b *testing.B) {
	src, key := newSource(), tiercache.KeyOf("a")
	src.set(key, 1)

	cache, err := tiercache.New(tiercache.Config{Name: "bench", HardTTL: time.Hour}, credential.Codec{}, src.load, nil, nil)
	require.NoError(b, err)
	b.Cleanup(cache.Close)

	ctx := context.Background()
	_, err = cache.Get(ctx, key)
	require.NoError(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := cache.Get(ctx, key); err != nil {
				b.Fatal(err)
			}
		}
	})
}
