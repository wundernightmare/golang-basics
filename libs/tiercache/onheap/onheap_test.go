package onheap_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wundernightmare/golang-basics/libs/tiercache/onheap"
	"github.com/wundernightmare/golang-basics/libs/tiercache/storetest"
)

const mib = 1 << 20

func TestStore(t *testing.T) {
	t.Parallel()

	// The capacity is what is asked for, 512 KiB at least.
	storetest.Run(t, onheap.New,
		storetest.Capacity{Ask: 1, Bound: onheap.Min},
		storetest.Capacity{Ask: onheap.Min, Bound: onheap.Min},
		storetest.Capacity{Ask: 4 * mib, Bound: 4 * mib},
		storetest.Capacity{Ask: 16*mib + 1, Bound: 16*mib + 1},
	)
}

func TestTheCapacityAndTheEntryLimit(t *testing.T) {
	t.Parallel()

	for asked, want := range map[int]struct{ bytes, entry int }{
		1:              {onheap.Min, 488},
		onheap.Min - 1: {onheap.Min, 488},
		onheap.Min:     {onheap.Min, 488},
		4 * mib:        {4 * mib, 4072},
		16 * mib:       {16 * mib, 16_360},
		64 * mib:       {64 * mib, 65_512},
	} {
		store := onheap.New(asked)

		assert.Equal(t, want.bytes, store.Bytes(), "capacity asked: %d", asked)
		assert.Equal(t, want.entry, store.MaxEntry(), "capacity asked: %d", asked)

		// The limit is the library's own: one byte over is not kept.
		store.Set("k", make([]byte, want.entry))

		_, found := store.Get(nil, "k")
		assert.False(t, found, "capacity asked: %d", asked)
	}
}
