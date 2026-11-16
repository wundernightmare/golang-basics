package offheap_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wundernightmare/golang-basics/libs/tiercache"
	"github.com/wundernightmare/golang-basics/libs/tiercache/offheap"
	"github.com/wundernightmare/golang-basics/libs/tiercache/storetest"
)

func TestStore(t *testing.T) {
	t.Parallel()

	// The capacity moves in steps of 32 MiB and is never less.
	storetest.Run(t, offheap.New,
		storetest.Capacity{Ask: 1, Bound: offheap.Step},
		storetest.Capacity{Ask: offheap.Step, Bound: offheap.Step},
		storetest.Capacity{Ask: offheap.Step + 1, Bound: 2 * offheap.Step},
	)
}

func TestTheEntryLimit(t *testing.T) {
	t.Parallel()

	store := offheap.New(offheap.Step)
	t.Cleanup(store.Reset)

	assert.Equal(t, 65_531, offheap.MaxEntry)
	assert.Equal(t, offheap.MaxEntry, store.MaxEntry())
	assert.Zero(t, store.Bytes(), "nothing is mapped before the first entry")

	// One byte over is not kept — and the limit is the library's own, not a
	// number of ours that is merely lower.
	store.Set("k", make([]byte, offheap.MaxEntry))

	_, found := store.Get(nil, "k")
	assert.False(t, found)
}

// A read into a buffer with room allocates nothing, whatever the length of
// the key: the key is lent to the library, not copied.
//
//nolint:paralleltest // serial: testing.AllocsPerRun refuses to run in a parallel test
func TestAReadAllocatesNothing(t *testing.T) {
	store := offheap.New(offheap.Step)
	t.Cleanup(store.Reset)

	key, buf := strings.Repeat("k", tiercache.MaxKeyLen), make([]byte, 0, 64)
	store.Set(key, []byte("value"))

	allocs := testing.AllocsPerRun(1000, func() {
		if _, found := store.Get(buf, key); !found {
			t.Fatal("not found")
		}
	})
	assert.Zero(t, allocs)
}
