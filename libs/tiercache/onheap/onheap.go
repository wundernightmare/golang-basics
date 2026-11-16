// Package onheap is the L1 of libs/tiercache on the Go heap
// (github.com/coocood/freecache): for a service whose container is small —
// a few MiB of cache, values of a few KiB.
//
//   - Capacity is exactly what is asked for, 512 KiB at least.
//   - The memory is allocated in full at construction, as byte rings: the Go
//     memory limit counts it, the collector does not scan it (no pointers),
//     and it becomes resident as the rings are written.
//   - An entry is at most 1/1024 of the capacity less 24 bytes, key and
//     value together: 4 072 bytes at 4 MiB, 16 360 at 16 MiB, 488 at
//     512 KiB — less than the longest key a cache accepts.
//   - What does not fit pushes out, roughly, the least recently read.
//   - An index is ordinary heap on top: 1 MiB at construction, then 16 bytes
//     per entry.
package onheap

import (
	"unsafe"

	"github.com/coocood/freecache"

	"github.com/wundernightmare/golang-basics/libs/tiercache"
)

// Min is the least capacity.
const Min = 512 << 10

// An entry is at most a quarter of one of the 256 segments, less its header
// (freecache: `len(key)+len(value) > len(seg.rb.data)/4 - ENTRY_HDR_SIZE` is
// refused).
const (
	segments   = 256
	entryShare = 4
)

type store struct {
	cache    *freecache.Cache
	capacity int
}

// lend is key as a slice over the string's own memory: a read of any key
// length then costs no allocation. The library copies a key into its ring
// and neither keeps nor changes the slice — and nothing else may be handed
// it: a string is immutable, and a write through the slice would be a write
// into every copy of it.
func lend(key string) []byte {
	return unsafe.Slice(unsafe.StringData(key), len(key)) //nolint:gosec // G103: a read-only view of an immutable string, see above
}

// New is a [tiercache.StoreFactory].
func New(maxBytes int) tiercache.Store {
	maxBytes = max(maxBytes, Min)

	return store{cache: freecache.NewCache(maxBytes), capacity: maxBytes}
}

func (s store) Get(dst []byte, key string) ([]byte, bool) {
	// The library copies into the buffer it is given when the value fits its
	// capacity — the spare capacity of dst, here — and allocates otherwise.
	// Either way append puts the value after dst: in the first case it is
	// there already, and the copy is of the bytes onto themselves.
	value, err := s.cache.GetWithBuf(lend(key), dst[len(dst):])
	if err != nil {
		return dst, false
	}

	return append(dst, value...), true
}

// Set stores without an expiry of the library's own: a cache's envelope
// carries the deadlines. It deletes what it cannot keep — the library
// refuses an entry over the limit and leaves the previous value of its key.
func (s store) Set(key string, value []byte) {
	if err := s.cache.Set(lend(key), value, 0); err != nil {
		s.cache.Del(lend(key))
	}
}

func (s store) Delete(key string) { s.cache.Del(lend(key)) }
func (s store) Reset()            { s.cache.Clear() }

func (s store) MaxEntry() int {
	return s.capacity/segments/entryShare - freecache.ENTRY_HDR_SIZE
}

// Bytes is the capacity: the rings are allocated in full at construction.
func (s store) Bytes() int { return s.capacity }
