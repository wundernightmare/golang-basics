package tiercache_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/wundernightmare/golang-basics/libs/tiercache"
	"github.com/wundernightmare/golang-basics/libs/tiercache/offheap"
	"github.com/wundernightmare/golang-basics/libs/tiercache/onheap"
)

func TestMain(m *testing.M) {
	fullPropertyChecks()
	goleak.VerifyTestMain(m)
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

	// A header this process could have written: only what follows it is wrong.
	header := func() string { return written(t, "a", 1, time.Now())[:17] }

	for name, raw := range map[string]func() string{
		"not an envelope":          func() string { return "{\"id\":1,\"name\":\"left by another writer\"}" },
		"a future format version":  func() string { return "\x02" + header()[1:] + "payload" },
		"a payload that is not it": func() string { return header() + "\x0a\x05a" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
				src.set(key, 1)

				rig := newHarness(t, remote, src, nil)

				synctest.Wait()
				remote.put(rig.name, key, raw())

				assert.Equal(t, "v1", rig.version(t, key))
				assert.InDelta(t, 1, rig.reads(t, "load"), 0)

				other := newHarness(t, remote, newSource(), sameName(rig.name))

				synctest.Wait()
				assert.Equal(t, "v1", other.version(t, key), "L2 now holds a good entry")
			})
		})
	}
}

// written is the L2 entry a process writes for key at version when its clock
// reads at: the envelope of envelope.go, built by hand because the writer
// here is another process with a clock of its own.
func written(t *testing.T, key string, version int, at time.Time) string {
	t.Helper()

	env := []byte{1}
	env = binary.BigEndian.AppendUint64(env, uint64(at.Add(softTTL).UnixMilli()))
	env = binary.BigEndian.AppendUint64(env, uint64(at.Add(hardTTL).UnixMilli()))

	env, err := rowCodec{}.Append(env, record(key, version, 0))
	require.NoError(t, err)

	return string(env)
}

// The deadlines in an L2 entry are the writer's clock, read against the
// reader's. A writer whose clock runs ahead would otherwise hand every other
// process a value that looks fresh for as long as the clocks differ — and
// their L1 would keep it that long, past the entry's TTL in L2. An entry
// whose deadline is further away than this process could have set is not
// trusted: the source answers and the entry is replaced.
func TestAnEntryWrittenByAnotherClockIsHeldToThisOne(t *testing.T) {
	t.Parallel()

	const longest = hardTTL + hardTTL/10 // the jitter's upper end

	for name, tc := range map[string]struct {
		writerAhead time.Duration
		want        string
		loads       float64
	}{
		"the same clock":                   {writerAhead: 0, want: "v1", loads: 0},
		"ahead by all the jitter allows":   {writerAhead: longest - hardTTL, want: "v1", loads: 0},
		"ahead by a millisecond more":      {writerAhead: longest - hardTTL + time.Millisecond, want: "v2", loads: 1},
		"an hour ahead":                    {writerAhead: time.Hour, want: "v2", loads: 1},
		"an hour behind: expired already":  {writerAhead: -time.Hour, want: "v2", loads: 1},
		"behind, within the soft deadline": {writerAhead: -softTTL / 2, want: "v1", loads: 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				remote, src, key := newFakeRemote(), newSource(), "a"
				src.set(key, 2)

				rig := newHarness(t, remote, src, nil)

				synctest.Wait()
				remote.put(rig.name, key, written(t, key, 1, time.Now().Add(tc.writerAhead)))

				assert.Equal(t, tc.want, rig.version(t, key))
				assert.InDelta(t, tc.loads, rig.reads(t, "load"), 0)

				// Whatever was served is what L1 now holds, for no longer
				// than this process's own HardTTL.
				assert.Equal(t, tc.want, rig.version(t, key))
				assert.InDelta(t, 1, rig.reads(t, "fresh"), 0)

				time.Sleep(longest)
				assert.Equal(t, "v2", rig.version(t, key), "past HardTTL nothing older than the source is served")
			})
		})
	}
}

// An hour-ahead entry is replaced in L2 too: the next process finds one it
// can trust and does not load.
func TestAnEntryFromAClockAheadIsReplacedInL2(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), "a"
		src.set(key, 2)

		rig := newHarness(t, remote, src, nil)

		synctest.Wait()
		remote.put(rig.name, key, written(t, key, 1, time.Now().Add(time.Hour)))
		require.Equal(t, "v2", rig.version(t, key))

		other := newHarness(t, remote, newSource(), sameName(rig.name))

		synctest.Wait()
		assert.Equal(t, "v2", other.version(t, key))
		assert.InDelta(t, 1, other.reads(t, "l2"), 0)
	})
}

// With the source down, an entry from a clock ahead is not a fallback
// either: its age is unknown.
func TestAnEntryFromAClockAheadIsNoStaleFallback(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), "a"
		src.fail(errors.New("source down"))

		rig := newHarness(t, remote, src, func(cfg *tiercache.Config) { cfg.StaleIfError = time.Hour })

		synctest.Wait()
		remote.put(rig.name, key, written(t, key, 1, time.Now().Add(time.Hour)))

		_, err := rig.cache.Get(t.Context(), key)
		require.ErrorContains(t, err, "source down")
	})
}

// A value that outgrows L1 takes its smaller predecessor out of it: the
// reads that follow are served from L2, not the older value from L1 until
// its deadline.
func TestAValueThatOutgrowsL1LeavesIt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), "tenant:42"
		src.set(key, 1)

		rig := newHarness(t, remote, src, nil)

		synctest.Wait()
		require.Equal(t, "v1", rig.version(t, key))
		require.Equal(t, "v1", rig.version(t, key))
		require.InDelta(t, 1, rig.reads(t, "fresh"), 0, "the small value is in L1")

		// The row grows past the entry limit; the stale read refreshes it.
		src.set(key, 2)
		src.payload = 70_000

		time.Sleep(20 * time.Second)
		require.Equal(t, "v1", rig.version(t, key))
		synctest.Wait()

		require.InDelta(t, 1, rig.metric(t, "tiercache_l1_oversize_total", nil), 0)

		fromL2 := rig.reads(t, "l2")

		assert.Equal(t, "v2", rig.version(t, key), "not v1 from L1, stale for the rest of its hard TTL")
		assert.InDelta(t, fromL2+1, rig.reads(t, "l2"), 0)
	})
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

func TestNewRejectsWhatItCannotRunWith(t *testing.T) {
	t.Parallel()

	load := newSource().load
	good := tiercache.Config{Name: "c", HardTTL: time.Minute}

	_, err := tiercache.New(tiercache.Config{HardTTL: time.Minute}, offheap.New, rowCodec{}, load, nil, nil)
	require.ErrorContains(t, err, "Name")

	_, err = tiercache.New[row](good, offheap.New, nil, load, nil, nil)
	require.ErrorContains(t, err, "store, codec and loader")

	_, err = tiercache.New(good, offheap.New, rowCodec{}, nil, nil, nil)
	require.ErrorContains(t, err, "store, codec and loader")

	_, err = tiercache.New(good, nil, rowCodec{}, load, nil, nil)
	require.ErrorContains(t, err, "store, codec and loader")

	_, err = tiercache.New(good, func(int) tiercache.Store { return nil }, rowCodec{}, load, nil, nil)
	require.ErrorContains(t, err, "returned no store")
}

// The factory is handed the capacity of the configuration — the default
// when it names none — and its store is the cache's L1.
func TestTheStoreIsBuiltWithTheConfiguredCapacity(t *testing.T) {
	t.Parallel()

	for configured, want := range map[int]int{0: tiercache.DefaultMaxBytes, -1: tiercache.DefaultMaxBytes, 4 << 20: 4 << 20} {
		asked := 0

		cache, err := tiercache.New(tiercache.Config{Name: "c", HardTTL: time.Minute, MaxBytes: configured},
			func(maxBytes int) tiercache.Store {
				asked = maxBytes

				return onheap.New(maxBytes)
			}, rowCodec{}, newSource().load, nil, nil)
		require.NoError(t, err)
		cache.Close()

		assert.Equal(t, want, asked, "configured: %d", configured)
	}

	assert.Equal(t, 32<<20, tiercache.DefaultMaxBytes)
}

// Every test builds its caches on the off-heap store unless it says
// otherwise. This runs them all once more on the heap store — the same
// binary, with the suite's store set — so a rule of the cache that holds on
// one store and not on the other fails here. (What a store itself owes is
// storetest's, run by each provider.)
//
//nolint:paralleltest // serial: a second suite beside this one would have both wait on each other's CPU
func TestTheSuiteHoldsOnTheHeapStore(t *testing.T) {
	if onTheHeap() {
		t.Skip("this is the second run")
	}

	args := []string{"-test.count=1", "-test.shuffle=on"}
	if testing.Short() {
		args = append(args, "-test.short")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()

	child := exec.CommandContext(ctx, os.Args[0], args...)

	child.Env = append(os.Environ(), suiteStoreEnv+"=onheap")

	out, err := child.CombinedOutput()
	require.NoError(t, err, "on the heap store:\n%s", out)
}

// The budget of the hot path: a fresh read allocates what Decode returns —
// three strings for a row — and nothing of the cache's own.
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

	counters, err := tiercache.New(tiercache.Config{Name: "allocs", HardTTL: time.Minute, MaxBytes: suiteBytes()}, suiteStore(), fixedCodec{},
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

// The limit is in bytes, and this is what it means in characters: the
// numbers the README gives.
func TestWhatFitsInAKey(t *testing.T) {
	t.Parallel()

	fits := map[string]string{
		"a UUID, 36 bytes":      "0190f3a2-7b1c-7e7e-9c1d-2f6a5b3c4d5e",
		"512 Latin letters":     strings.Repeat("a", 512),
		"256 Cyrillic letters":  strings.Repeat("ж", 256),
		"170 CJK characters":    strings.Repeat("\u6f22", 170),
		"128 emoji":             strings.Repeat("😀", 128),
		"a SHA-256 from KeyOf":  tiercache.KeyOf("secret"),
		"one byte":              "k",
		"bytes that are no UTF": "\xff\x00\xfe",
	}
	tooLong := map[string]string{
		"513 Latin letters":    strings.Repeat("a", 513),
		"257 Cyrillic letters": strings.Repeat("ж", 257),
		"171 CJK characters":   strings.Repeat("\u6f22", 171),
		"129 emoji":            strings.Repeat("😀", 129),
	}

	assert.Len(t, fits["a UUID, 36 bytes"], 36)

	src := newSource()
	rig := newHarness(t, nil, src, nil)

	for name, key := range fits {
		src.set(key, 1)
		assert.Equal(t, "v1", rig.version(t, key), name)
		require.NoError(t, rig.cache.Invalidate(t.Context(), key), name)
	}

	for name, key := range tooLong {
		src.set(key, 1)

		_, err := rig.cache.Get(t.Context(), key)
		require.ErrorIs(t, err, tiercache.ErrKey, name)
		require.ErrorIs(t, rig.cache.Invalidate(t.Context(), key), tiercache.ErrKey, name)
	}
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

	cache, err := tiercache.New(tiercache.Config{Name: "bench", HardTTL: time.Hour, MaxBytes: suiteBytes()}, suiteStore(), rowCodec{}, src.load, nil, nil)
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

// Read your own write, in the window the write itself opens: Invalidate has
// dropped the key from L1 and its tombstone is still on the way to L2, where
// a reader of this process finds the old value. Nothing but Invalidate can
// take it out of L1 again when no message of its own comes back.
func TestAReadDuringInvalidateLeavesNoOldValueBehind(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		tune           func(*tiercache.Config)
		publishFailure error
	}{
		"invalidation disabled": {tune: func(cfg *tiercache.Config) { cfg.DisableInvalidation = true }},
		"the publish fails":     {publishFailure: errRemoteDown},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
				src.set(key, 1)

				rig := newHarness(t, remote, src, tc.tune)

				synctest.Wait()
				require.Equal(t, "v1", rig.version(t, key))

				src.set(key, 2)

				remote.mu.Lock()
				remote.publishFailure = tc.publishFailure
				remote.beforeTombstone = func() {
					version, err := rig.read(key)
					require.NoError(t, err)
					assert.Equal(t, "v1", version, "L2 still holds the old value: this reader asked before Invalidate returned")
				}
				remote.mu.Unlock()

				err := rig.cache.Invalidate(t.Context(), key)
				if tc.publishFailure != nil {
					require.ErrorIs(t, err, tc.publishFailure)
				} else {
					require.NoError(t, err)
				}

				assert.Equal(t, "v2", rig.version(t, key))
			})
		})
	}
}

// A fill that L2 refused is not kept in L1 either: the tombstone of a write
// this process heard nothing else of (its message was lost) is the only
// thing that says the loaded row is the old one.
func TestAFillRefusedByL2IsNotKeptInL1(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, remote, src, func(cfg *tiercache.Config) { cfg.DisableInvalidation = true })
		release := src.hold()
		racing := make(chan string, 1)

		go func() {
			version, _ := rig.read(key)
			racing <- version
		}()

		synctest.Wait() // the reader has read v1 and is stalled

		src.set(key, 2)
		require.NoError(t, remote.Tombstone(t.Context(), "tc:"+rig.name+":"+key, 5*time.Second))
		release()

		assert.Equal(t, "v1", <-racing)
		assert.Equal(t, "v2", rig.version(t, key))
	})
}

// A panic in the code a lookup calls is that lookup's failure: every caller
// gets an error, and the process — which no recover of theirs could have
// saved, the lookup runs in a goroutine of the cache — goes on.
func TestAPanicInALookupIsAnErrorNotACrash(t *testing.T) {
	t.Parallel()

	for name, breakIt := range map[string]func(*fakeRemote, *source){
		"in the loader": func(_ *fakeRemote, src *source) { src.bug = "loader bug" },
		"in the remote": func(remote *fakeRemote, _ *source) { remote.beforeGetReturns = func() { panic("remote bug") } },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				remote, src, key := newFakeRemote(), newSource(), tiercache.KeyOf("a")
				src.set(key, 1)

				rig := newHarness(t, remote, src, func(cfg *tiercache.Config) { cfg.DisableInvalidation = true })
				require.Equal(t, "v1", rig.version(t, key))

				// Past the soft deadline: served, and refreshed behind — by a
				// lookup that panics after the read has returned.
				time.Sleep(20 * time.Second)
				breakIt(remote, src)
				require.Equal(t, "v1", rig.version(t, key))
				synctest.Wait()
				assert.InDelta(t, 1, rig.metric(t, "tiercache_refreshes_total", map[string]string{"result": "error"}), 0)

				// Past the hard one the read waits for the lookup itself.
				time.Sleep(70 * time.Second)

				_, err := rig.cache.Get(t.Context(), key)
				require.ErrorIs(t, err, tiercache.ErrPanic)
				require.ErrorContains(t, err, "bug")

				remote.beforeGetReturns, src.bug = nil, nil

				assert.Equal(t, "v1", rig.version(t, key), "the cache, and its breaker, work on")
				assert.Zero(t, rig.metric(t, "tiercache_l2_bypassed_total", nil))
			})
		})
	}
}

// The deadline of the caller that started a lookup is that caller's: one
// that joined with time left gets the value, not somebody else's timeout.
func TestAJoinerIsNotFailedByTheFirstCallersDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		src, key := newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, nil, src, nil)
		release := src.hold()

		hurried, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		first, second := make(chan error, 1), make(chan error, 1)

		go func() { _, err := rig.cache.Get(hurried, key); first <- err }()

		synctest.Wait()

		go func() { _, err := rig.cache.Get(t.Context(), key); second <- err }()

		require.ErrorIs(t, <-first, context.DeadlineExceeded)
		synctest.Wait()

		select {
		case err := <-second:
			require.NoError(t, err, "the joiner was handed the first caller's deadline")
		default:
		}

		release()
		require.NoError(t, <-second)
		assert.InDelta(t, 1, rig.reads(t, "error"), 0, "the first caller's alone")
	})
}

// The breaker is told about L2, not about the callers: a lookup whose own
// context is done — cancelled, or out of time — says nothing of L2's health,
// while L2 failing a call that still had time is its failure, a timeout
// included.
func TestTheBreakerCountsFailuresOfL2NotOfTheCallers(t *testing.T) {
	t.Parallel()

	gone := func(ctx context.Context) context.Context {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		return ctx
	}
	late := func(ctx context.Context) context.Context {
		ctx, cancel := context.WithDeadline(ctx, time.Now())
		_ = cancel // expired as it is

		return ctx
	}
	inTime := func(ctx context.Context) context.Context { return ctx }

	for name, tc := range map[string]struct {
		caller   func(context.Context) context.Context
		failure  error
		bypassed bool
	}{
		"callers that had gone away":         {caller: gone},
		"callers whose deadline had passed":  {caller: late},
		"L2 timing out on callers with time": {caller: inTime, failure: context.DeadlineExceeded, bypassed: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var logged bytes.Buffer

				remote, src := newFakeRemote(), newSource()
				rig := newLoggedHarness(t, remote, src, nil, slog.New(slog.NewTextHandler(&logged, nil)))

				synctest.Wait()
				remote.fail(tc.failure)

				for i := range 20 {
					key := fmt.Sprint("k", i)
					src.set(key, 1)

					_, _ = rig.cache.Get(tc.caller(t.Context()), key)

					synctest.Wait() // the lookup goes on without its caller
				}

				remote.fail(nil)
				src.set("healthy", 1)
				require.Equal(t, "v1", rig.version(t, "healthy"))

				assert.Equal(t, tc.bypassed, rig.metric(t, "tiercache_l2_bypassed_total", nil) > 0)
				assert.Equal(t, tc.bypassed, strings.Contains(logged.String(), "L2 is failing"), "the breaker opening is what is logged")
			})
		})
	}
}

// SoftTTL equal to HardTTL (the default) is plain cache-aside: no value is
// served stale and refreshed behind, at any age up to its hard deadline.
func TestWithoutASoftTTLNothingIsServedStale(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		src := newSource()
		rig := newHarness(t, nil, src, func(cfg *tiercache.Config) { cfg.SoftTTL = 0 })
		keys := make([]string, 200)

		for i := range keys {
			keys[i] = fmt.Sprint("k", i)
			src.set(keys[i], 1)
			require.Equal(t, "v1", rig.version(t, keys[i]))
		}

		time.Sleep(hardTTL - 3*time.Second) // inside the ±10% the deadlines are spread over

		for _, key := range keys {
			require.Equal(t, "v1", rig.version(t, key))
		}

		synctest.Wait()
		assert.Zero(t, rig.reads(t, "stale"))
		assert.Zero(t, rig.metric(t, "tiercache_refreshes_total", map[string]string{"result": "ok"}))
	})
}

// A refresh that does not come back is given up after RefreshTimeout, and
// not before.
func TestARefreshIsBoundedByItsTimeout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		src, key := newSource(), tiercache.KeyOf("a")
		src.set(key, 1)

		rig := newHarness(t, nil, src, func(cfg *tiercache.Config) { cfg.RefreshTimeout = 3 * time.Second })
		require.Equal(t, "v1", rig.version(t, key))

		time.Sleep(20 * time.Second)
		t.Cleanup(src.hold())
		require.Equal(t, "v1", rig.version(t, key))

		time.Sleep(3*time.Second - time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(1), src.inFlight.Load())

		time.Sleep(time.Millisecond)
		synctest.Wait()
		assert.Zero(t, src.inFlight.Load())
		assert.InDelta(t, 1, rig.metric(t, "tiercache_refreshes_total", map[string]string{"result": "error"}), 0)
	})
}

// resetCounter is a store that counts how often it was emptied.
type resetCounter struct {
	tiercache.Store

	resets atomic.Int64
}

func (s *resetCounter) Reset() {
	s.resets.Add(1)
	s.Store.Reset()
}

// Close cancels what the cache has in flight — a refresh and a lookup whose
// caller has left — at once, returns only when both have ended, empties L1,
// and leaves a cache that answers ErrClosed.
func TestCloseEndsWhatIsInFlightAndWaitsForIt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var (
			store     *resetCounter
			held      atomic.Bool
			cancelled = make(chan time.Time, 2)
			finish    = make(chan struct{})
		)

		// Once held, a load waits for its context to end, says when it did,
		// and then still takes its time to return.
		load := func(ctx context.Context, _ string) (row, error) {
			if !held.Load() {
				return row{CRMID: "v1"}, nil
			}

			<-ctx.Done()

			cancelled <- time.Now()

			<-finish

			return row{}, ctx.Err()
		}

		cache, err := tiercache.New(tiercache.Config{Name: "closing", SoftTTL: softTTL, HardTTL: hardTTL},
			func(maxBytes int) tiercache.Store {
				store = &resetCounter{Store: suiteStore()(maxBytes)}

				return store
			}, rowCodec{}, load, nil, nil)
		require.NoError(t, err)

		_, err = cache.Get(t.Context(), "refreshed")
		require.NoError(t, err)

		time.Sleep(20 * time.Second)
		held.Store(true)

		_, err = cache.Get(t.Context(), "refreshed") // stale: a refresh starts behind it
		require.NoError(t, err)

		gone, leave := context.WithCancel(t.Context())
		abandoned := make(chan error, 1)

		go func() { _, err := cache.Get(gone, "abandoned"); abandoned <- err }()

		synctest.Wait()
		leave()
		require.ErrorIs(t, <-abandoned, context.Canceled)

		closedAt, closed := time.Now(), make(chan struct{})

		go func() { cache.Close(); close(closed) }()

		synctest.Wait()
		require.Len(t, cancelled, 2, "the refresh and the abandoned lookup")
		assert.Equal(t, closedAt, <-cancelled, "at Close, not at RefreshTimeout")
		assert.Equal(t, closedAt, <-cancelled)

		select {
		case <-closed:
			require.Fail(t, "Close returned while loads were still running")
		default:
		}

		close(finish)
		<-closed
		assert.Equal(t, int64(1), store.resets.Load())

		_, err = cache.Get(t.Context(), "refreshed")
		require.ErrorIs(t, err, tiercache.ErrClosed)
		require.ErrorIs(t, cache.Invalidate(t.Context(), "refreshed"), tiercache.ErrClosed)

		cache.Close() // a second Close is harmless
		assert.Equal(t, int64(1), store.resets.Load())
	})
}
