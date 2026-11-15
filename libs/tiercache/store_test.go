package tiercache

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const mib = 1 << 20

// The capacities the store tests run at: the least of each store, and one
// more of each.
var storeSizes = []int{heapMin, 4 * mib, 16 * mib, offHeapStep}

func TestTheStoreIsChosenByCapacity(t *testing.T) {
	t.Parallel()

	assert.IsType(t, onHeap{}, newStore(offHeapStep-1))
	assert.IsType(t, offHeap{}, newStore(offHeapStep))
	assert.IsType(t, offHeap{}, newStore(4*offHeapStep))

	// The heap store is exactly as large as asked, and never below its least.
	assert.Equal(t, 4*mib, newStore(4*mib).bytes())
	assert.Equal(t, offHeapStep-1, newStore(offHeapStep-1).bytes())
	assert.Equal(t, heapMin, newStore(heapMin-1).bytes())
	assert.Equal(t, heapMin, newStore(1).bytes())

	// An entry of the heap store is 1/1024 of it, less the header.
	assert.Equal(t, 4*mib/1024-24, newStore(4*mib).maxEntry())
	assert.Equal(t, heapMin/1024-24, newStore(1).maxEntry())
	assert.Equal(t, 64<<10-5, newStore(offHeapStep).maxEntry())
}

func TestAStoreHoldsWhatItIsGiven(t *testing.T) {
	t.Parallel()

	for _, size := range storeSizes {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()

			local := newStore(size)
			t.Cleanup(local.reset)

			_, found := local.get(nil, []byte("absent"))
			assert.False(t, found)

			local.set([]byte("a"), []byte("one"))
			local.set([]byte("b"), nil)
			local.set([]byte("ab"), []byte("three"))

			got, found := local.get(nil, []byte("a"))
			assert.True(t, found)
			assert.Equal(t, "one", string(got))

			got, found = local.get(nil, []byte("b"))
			assert.True(t, found, "an empty value is a value")
			assert.Empty(t, got)

			// The value lands at the start of the buffer it is given, whatever
			// the buffer held and however small it is.
			for _, scratch := range [][]byte{nil, make([]byte, 0, 2), make([]byte, 0, 64), []byte("leftover")} {
				got, found = local.get(scratch[:0], []byte("ab"))
				assert.True(t, found)
				assert.Equal(t, "three", string(got))
			}

			local.set([]byte("a"), []byte("replaced, and longer than before"))
			got, _ = local.get(nil, []byte("a"))
			assert.Equal(t, "replaced, and longer than before", string(got))

			local.del([]byte("a"))
			_, found = local.get(nil, []byte("a"))
			assert.False(t, found)

			local.del([]byte("never there"))

			_, found = local.get(nil, []byte("ab"))
			assert.True(t, found, "a deletion removes its key only")

			local.reset()

			for _, key := range []string{"b", "ab"} {
				_, found = local.get(nil, []byte(key))
				assert.False(t, found, key)
			}

			local.set([]byte("a"), []byte("after the reset"))
			got, found = local.get(nil, []byte("a"))
			assert.True(t, found)
			assert.Equal(t, "after the reset", string(got))
		})
	}
}

// maxEntry is a promise about the store underneath: an entry of exactly that
// size is held, and one byte more is refused and counted — never dropped in
// silence. The longest key and the shortest take the same budget.
func TestLargestEntryL1AcceptsIsHeld(t *testing.T) {
	t.Parallel()

	for _, size := range storeSizes {
		for _, key := range []string{"k", KeyOf("largest"), strings.Repeat("k", MaxKeyLen)} {
			t.Run(fmt.Sprint(size, "/", len(key)), func(t *testing.T) {
				t.Parallel()

				cache := newByteCache(t, nil, func(cfg *Config) { cfg.MaxBytes = size })
				limit := cache.local.maxEntry()

				// The least heap store holds entries shorter than the longest
				// key: such a key is never in L1, and that is counted too.
				if limit < len(key) {
					cache.fill(cache.epoch.Load(), key, nil)

					_, found := cache.local.get(nil, []byte(key))
					assert.False(t, found)
					assert.InDelta(t, 1, testutil.ToFloat64(cache.metrics.oversize), 0)

					return
				}

				cache.fill(cache.epoch.Load(), key, make([]byte, limit-len(key)))

				held, found := cache.local.get(nil, []byte(key))
				assert.True(t, found, "an entry of maxEntry bytes is in L1")
				assert.Len(t, held, limit-len(key))
				assert.Zero(t, testutil.ToFloat64(cache.metrics.oversize))

				over := key[:len(key)-1] + "o"
				cache.fill(cache.epoch.Load(), over, make([]byte, limit-len(over)+1))

				_, found = cache.local.get(nil, []byte(over))
				assert.False(t, found)
				assert.InDelta(t, 1, testutil.ToFloat64(cache.metrics.oversize), 0)
			})
		}
	}
}

// Several times its capacity written into a store leaves the memory it
// reports at the capacity, the newest entries readable, and every value
// still its own key's.
func TestAStoreForgetsTheOldestAndNeverMixesValues(t *testing.T) {
	t.Parallel()

	for _, size := range storeSizes {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()

			local := newStore(size)
			t.Cleanup(local.reset)

			const valueLen = 200

			valueOf := func(i int) []byte {
				sum := sha256.Sum256(fmt.Append(nil, "value", i))

				return []byte(strings.Repeat(string(sum[:]), valueLen/len(sum)+1)[:valueLen])
			}

			entries := 4 * size / valueLen
			for i := range entries {
				local.set(fmt.Append(nil, "key", i), valueOf(i))
			}

			assert.LessOrEqual(t, local.bytes(), size)

			held := 0

			for i := range entries {
				got, found := local.get(nil, fmt.Append(nil, "key", i))
				if !found {
					continue
				}

				held++

				require.Equal(t, valueOf(i), got, "key %d", i)
			}

			assert.Less(t, held, entries/2, "what does not fit is forgotten")
			assert.Greater(t, held, size/valueLen/4, "and the store is actually used")

			for i := entries - 100; i < entries; i++ {
				_, found := local.get(nil, fmt.Append(nil, "key", i))
				assert.True(t, found, "the newest entries are held: key %d", i)
			}
		})
	}
}

func TestKeyBytesLendsTheKey(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []byte("tenant:42"), keyBytes("tenant:42"))
	assert.Empty(t, keyBytes(""))
	assert.Len(t, keyBytes(KeyOf("a")), sha256.Size)
}

// Every test builds its caches on the off-heap store unless it says
// otherwise. This runs them all once more on the heap store — the same
// binary, with the suite's capacity set — so a rule that holds on one store
// and not on the other fails here.
func TestTheSuiteHoldsOnTheHeapStore(t *testing.T) {
	t.Parallel()

	if SuiteL1Bytes() != 0 {
		t.Skip("this is the second run")
	}

	args := []string{"-test.count=1", "-test.shuffle=on"}
	if testing.Short() {
		args = append(args, "-test.short")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()

	child := exec.CommandContext(ctx, os.Args[0], args...)

	child.Env = append(os.Environ(), fmt.Sprint(SuiteL1BytesEnv, "=", 16*mib))

	out, err := child.CombinedOutput()
	require.NoError(t, err, "on the heap store:\n%s", out)
}
