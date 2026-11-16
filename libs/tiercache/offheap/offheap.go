// Package offheap is the L1 of libs/tiercache outside the Go heap
// (github.com/VictoriaMetrics/fastcache): for a cache large enough that the
// garbage collector's accounting of it would matter.
//
//   - Capacity moves in steps of 32 MiB and is never less: 1 MiB and 33 MiB
//     mean 32 and 64.
//   - The memory is mapped outside the heap as entries arrive, up to the
//     capacity, and is not returned to the system (Reset hands it to the
//     other off-heap stores of the process). Neither the collector nor the
//     Go memory limit counts it: subtract it from the container limit
//     (docs/RUNTIME.md, "Choosing the ratio").
//   - An entry is at most 65 531 bytes, key and value together.
//   - What does not fit pushes out the oldest written.
//   - An index of about 16 bytes per entry is ordinary heap on top.
package offheap

import (
	"unsafe"

	"github.com/VictoriaMetrics/fastcache"

	"github.com/tracehubmmp/golang-basics/libs/tiercache"
)

// Step is the granularity, and the minimum, of the capacity.
const Step = 32 << 20

// MaxEntry is the largest key+value held: the rings are made of 64 KiB
// chunks, and an entry with its 4 bytes of lengths must be smaller than one
// (fastcache: `4+len(k)+len(v) >= chunkSize` is dropped), hence 5.
const MaxEntry = 64<<10 - 5

type store struct{ cache *fastcache.Cache }

// lend is key as a slice over the string's own memory: a read of any key
// length then costs no allocation. The library copies a key into its ring
// and neither keeps nor changes the slice — and nothing else may be handed
// it: a string is immutable, and a write through the slice would be a write
// into every copy of it.
func lend(key string) []byte {
	return unsafe.Slice(unsafe.StringData(key), len(key)) //nolint:gosec // G103: a read-only view of an immutable string, see above
}

// New is a [tiercache.StoreFactory].
func New(maxBytes int) tiercache.Store { return store{fastcache.New(maxBytes)} }

func (s store) Get(dst []byte, key string) ([]byte, bool) {
	return s.cache.HasGet(dst, lend(key))
}

// Set deletes what it cannot keep: the library drops an entry over the limit
// and leaves the previous value of its key where it was.
func (s store) Set(key string, value []byte) {
	if len(key)+len(value) > MaxEntry {
		s.cache.Del(lend(key))

		return
	}

	s.cache.Set(lend(key), value)
}

func (s store) Delete(key string) { s.cache.Del(lend(key)) }
func (s store) Reset()            { s.cache.Reset() }
func (store) MaxEntry() int       { return MaxEntry }

// Bytes is what the store has mapped so far.
func (s store) Bytes() int {
	var stats fastcache.Stats

	s.cache.UpdateStats(&stats)

	return int(stats.BytesSize) //nolint:gosec // G115: bounded by the capacity
}
