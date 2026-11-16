// Package storetest is the conformance suite of [tiercache.Store]: what a
// cache relies on its L1 for, checked against the store itself. The stores
// of offheap and onheap run it; a service that brings its own store runs it
// too, from a test:
//
//	func TestStore(t *testing.T) {
//		storetest.Run(t, mystore.New, storetest.Capacity{Ask: 8 << 20, Bound: 8 << 20})
//	}
//
// It is test code in a non-test file, imported by tests only.
package storetest

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/tiercache"
)

// Capacity is one size the suite runs a store at: what the factory is asked
// for, and the most the store may then report as held — the provider's
// documented reading of Ask (the same number, unless it rounds).
type Capacity struct {
	Ask   int
	Bound int
}

// Run checks the store factory makes against the contract of
// [tiercache.Store], at each capacity.
func Run(t *testing.T, factory tiercache.StoreFactory, capacities ...Capacity) {
	t.Helper()
	require.NotEmpty(t, capacities, "storetest: name at least one capacity")

	cases := map[string]func(*testing.T, tiercache.Store, Capacity){
		"holds what it is given":               holds,
		"appends to the buffer":                appends,
		"shares nothing with its caller":       sharesNothing,
		"forgets on Delete and on Reset":       forgets,
		"keeps an entry of MaxEntry":           keepsTheLargest,
		"never keeps the value a Set replaced": neverKeepsTheReplaced,
		"stays within its capacity":            staysWithinCapacity,
		"never misremembers under load":        neverMisremembers,
	}

	for _, capacity := range capacities {
		for name, check := range cases {
			t.Run(strconv.Itoa(capacity.Ask)+"/"+name, func(t *testing.T) {
				t.Parallel()

				store := factory(capacity.Ask)
				require.NotNil(t, store)
				t.Cleanup(store.Reset)

				check(t, store, capacity)
			})
		}
	}
}

// valueOf is a value of n bytes that says which key and which version it
// belongs to: anything else coming back is a mix-up.
func valueOf(key string, version, n int) []byte {
	sum := sha256.Sum256(fmt.Append(nil, len(key), ":", key, ":", version))
	head := fmt.Appendf(nil, "%016x:%x:", version, sum[:8])

	for len(head) < n {
		head = append(head, sum[len(head)%len(sum)])
	}

	return head[:max(n, 0)]
}

func get(t *testing.T, store tiercache.Store, key string) ([]byte, bool) {
	t.Helper()

	value, found := store.Get(nil, key)
	if !found {
		require.Empty(t, value, "not found: the buffer comes back as it was")
	}

	return value, found
}

func holds(t *testing.T, store tiercache.Store, _ Capacity) {
	t.Helper()

	limit := store.MaxEntry()
	require.Positive(t, limit)

	_, found := get(t, store, "absent")
	assert.False(t, found)

	// Keys are bytes, not text; one is a prefix of another; two of the
	// longest differ in their last byte alone; a value may be empty, and
	// that is not the same as absent.
	long := strings.Repeat("k", min(tiercache.MaxKeyLen, limit/2)-1)

	keys := []string{"a", "ab", "b", "tenant:7/user:Ёж", "\xff\x00\xfe", long + "a", long + "b"}
	for i, key := range keys {
		store.Set(key, valueOf(key, i, 5*i))
	}

	for i, key := range keys {
		got, found := get(t, store, key)
		require.True(t, found, "%q", key)
		assert.Equal(t, string(valueOf(key, i, 5*i)), string(got), "%q", key)
	}

	store.Set("a", valueOf("a", 9, 100))

	got, found := get(t, store, "a")
	require.True(t, found)
	assert.Equal(t, string(valueOf("a", 9, 100)), string(got), "a Set replaces, with a longer value too")

	store.Set("a", nil)

	got, found = get(t, store, "a")
	require.True(t, found, "and with an empty one, which is still a value")
	assert.Empty(t, got)

	assert.Equal(t, limit, store.MaxEntry(), "MaxEntry does not move")
	assert.GreaterOrEqual(t, store.Bytes(), 0)
}

func appends(t *testing.T, store tiercache.Store, _ Capacity) {
	t.Helper()

	value := valueOf("k", 1, 40)
	store.Set("k", value)

	// Whatever the buffer holds and however much room it has left.
	for _, prefix := range []string{"", "p", "a longer prefix than the value has room after"} {
		for _, spare := range []int{0, 1, len(value) - 1, len(value), 4 * len(value)} {
			dst := append(make([]byte, 0, len(prefix)+spare), prefix...)

			got, found := store.Get(dst, "k")
			require.True(t, found)
			assert.Equal(t, prefix+string(value), string(got), "prefix %q, %d spare", prefix, spare)

			missed, found := store.Get(dst, "absent")
			require.False(t, found)
			assert.Equal(t, prefix, string(missed), "a miss returns the buffer as it was")
		}
	}
}

func sharesNothing(t *testing.T, store tiercache.Store, _ Capacity) {
	t.Helper()

	want := valueOf("k", 1, 64)

	given := append([]byte(nil), want...)
	store.Set("k", given)
	clear(given) // the caller reuses its buffer

	first, found := get(t, store, "k")
	require.True(t, found)
	require.Equal(t, string(want), string(first), "the store kept its own copy of the value")

	clear(first) // and what Get returned is the caller's to change

	second, found := get(t, store, "k")
	require.True(t, found)
	assert.Equal(t, string(want), string(second))

	// Nor does Get hold on to the buffer it filled.
	scratch := make([]byte, 0, 256)
	got, _ := store.Get(scratch, "k")
	require.Equal(t, string(want), string(got))

	store.Set("other", valueOf("other", 2, 64))
	_, _ = store.Get(nil, "other")

	assert.Equal(t, string(want), string(got), "a later call does not write into an earlier result")
}

func forgets(t *testing.T, store tiercache.Store, _ Capacity) {
	t.Helper()

	for version := range 3 {
		store.Set("gone", valueOf("gone", version, 30))
	}

	store.Set("stays", valueOf("stays", 1, 30))

	store.Delete("gone")
	store.Delete("never there")

	_, found := get(t, store, "gone")
	assert.False(t, found, "none of the values the key ever had comes back")

	got, found := get(t, store, "stays")
	require.True(t, found, "a Delete removes its key only")
	assert.Equal(t, string(valueOf("stays", 1, 30)), string(got))

	store.Reset()

	_, found = get(t, store, "stays")
	assert.False(t, found)

	store.Set("stays", valueOf("stays", 2, 30))

	got, found = get(t, store, "stays")
	require.True(t, found, "the store is usable after a Reset")
	assert.Equal(t, string(valueOf("stays", 2, 30)), string(got))
}

func keepsTheLargest(t *testing.T, store tiercache.Store, _ Capacity) {
	t.Helper()

	limit := store.MaxEntry()

	for _, keyLen := range []int{1, 32, tiercache.MaxKeyLen} {
		if keyLen >= limit {
			continue // the store holds no entry with a key this long
		}

		key := strings.Repeat("k", keyLen-1) + strconv.Itoa(keyLen%10)
		want := valueOf(key, 1, limit-len(key))
		store.Set(key, want)

		got, found := get(t, store, key)
		require.True(t, found, "an entry of exactly MaxEntry (%d) with a key of %d bytes", limit, keyLen)
		assert.Equal(t, string(want), string(got))
	}
}

func neverKeepsTheReplaced(t *testing.T, store tiercache.Store, _ Capacity) {
	t.Helper()

	limit := store.MaxEntry()

	// Sizes around the limit and far beyond it: whether the store keeps the
	// new value or has no room for it, the old one is gone.
	for i, size := range []int{limit - 1, limit + 1, 2 * limit, 16*limit + 7} {
		key := fmt.Sprint("replaced", i)
		store.Set(key, valueOf(key, 1, 20))

		_, found := get(t, store, key)
		require.True(t, found)

		want := valueOf(key, 2, size-len(key))
		store.Set(key, want)

		if got, found := get(t, store, key); found {
			assert.Equal(t, string(want), string(got), "a Set of %d bytes at MaxEntry %d", size, limit)
		}
	}
}

func staysWithinCapacity(t *testing.T, store tiercache.Store, capacity Capacity) {
	t.Helper()

	valueLen := min(200, store.MaxEntry()/2)
	entries := 4 * capacity.Bound / valueLen

	for i := range entries {
		key := fmt.Sprint("key", i)
		store.Set(key, valueOf(key, i, valueLen))

		// Full or not, a store is of use: what was just set is there.
		if i%64 == 0 {
			got, found := get(t, store, key)
			require.True(t, found, "entry %d of %d, right after its Set", i, entries)
			require.Equal(t, string(valueOf(key, i, valueLen)), string(got))
		}
	}

	assert.LessOrEqual(t, store.Bytes(), capacity.Bound)

	held := 0

	for i := range entries {
		key := fmt.Sprint("key", i)

		got, found := get(t, store, key)
		if !found {
			continue
		}

		held++

		require.Equal(t, string(valueOf(key, i, valueLen)), string(got), "what it still holds is each key's own value")
	}

	assert.Less(t, held, entries, "four capacities were written: something was forgotten")
	assert.Positive(t, held)
}

// neverMisremembers is the contract's central rule under concurrency: one
// writer per key sets versions in order and deletes now and then, while
// readers check that a key never reads as another key's value, a torn one,
// or a version older than one already read or deleted. The store is small
// against what is written, so eviction is part of it.
func neverMisremembers(t *testing.T, store tiercache.Store, _ Capacity) {
	t.Helper()

	const (
		keys     = 16
		versions = 400
		readers  = 4
	)

	valueLen := min(300, store.MaxEntry()/2)

	// floor[k] is the lowest version a read of key k may still return: the
	// last one its writer is known to have replaced or deleted, plus one.
	var floor [keys]atomic.Int64

	var writers, reading sync.WaitGroup

	done := make(chan struct{})
	failed := make(chan string, 1)
	fail := func(problem string) {
		select {
		case failed <- problem:
		default:
		}
	}

	for k := range keys {
		writers.Go(func() {
			key := fmt.Sprint("key", k)

			for version := 1; version <= versions; version++ {
				store.Set(key, valueOf(key, version, valueLen))
				floor[k].Store(int64(version))

				if version%7 == 0 {
					store.Delete(key)
					floor[k].Store(int64(version) + 1)
				}

				// Churn: other keys, to make the store evict.
				filler := fmt.Sprint("filler", k, "/", version)
				store.Set(filler, valueOf(filler, 0, valueLen))
			}
		})
	}

	for range readers {
		reading.Go(func() {
			var scratch []byte

			for {
				select {
				case <-done:
					return
				default:
				}

				for k := range keys {
					var problem string

					scratch, problem = misread(store, scratch, fmt.Sprint("key", k), valueLen, floor[k].Load)
					if problem != "" {
						fail(problem)

						return
					}
				}
			}
		})
	}

	writers.Wait()
	close(done)
	reading.Wait()

	select {
	case message := <-failed:
		require.Fail(t, message)
	default:
	}
}

// misread reads key once and says what is wrong with what came back, if
// anything: not a value of this suite, not this key's, or a version below
// the floor read before the Get. It returns the buffer for the next read.
func misread(store tiercache.Store, scratch []byte, key string, valueLen int, floor func() int64) ([]byte, string) {
	atLeast := floor()

	got, found := store.Get(scratch[:0], key)
	if !found {
		return scratch, ""
	}

	var version int
	if _, err := fmt.Sscanf(string(got), "%016x:", &version); err != nil {
		return got, fmt.Sprintf("%s reads as %q", key, got)
	}

	switch {
	case string(got) != string(valueOf(key, version, valueLen)):
		return got, fmt.Sprintf("%s reads as a value that is not its own: %q", key, got)
	case int64(version) < atLeast:
		return got, fmt.Sprintf("%s reads as version %d after version %d was replaced or deleted", key, version, atLeast)
	}

	return got, ""
}
