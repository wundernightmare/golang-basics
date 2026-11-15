package tiercache

import (
	"unsafe"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/coocood/freecache"
)

const (
	// offHeapStep is the granularity (and the minimum) of the off-heap
	// store: a capacity below it gets the heap store instead.
	offHeapStep = 32 << 20
	// heapMin is the least the heap store holds (freecache: minBufSize).
	heapMin = 512 << 10

	// offHeapMaxEntry is the largest key+envelope the off-heap store holds:
	// its rings are made of 64 KiB chunks, and an entry with its 4 bytes of
	// lengths must be smaller than one (fastcache: `4+len(k)+len(v) >=
	// chunkSize` is dropped), hence 5.
	offHeapMaxEntry = 64<<10 - 5
	// The heap store holds a key+envelope of at most a quarter of one of
	// its 256 segments, less the entry header (freecache: `len(key)+
	// len(value) > len(seg.rb.data)/4 - ENTRY_HDR_SIZE` is refused).
	heapSegments    = 256
	heapEntryShare  = 4
	heapEntryHeader = freecache.ENTRY_HDR_SIZE
)

// store is L1: a fixed amount of memory that holds what fits and forgets the
// rest. A store never fails and never blocks on anything but its own locks;
// an entry larger than maxEntry is the caller's to refuse — a store drops it
// silently. TestLargestEntryL1AcceptsIsHeld checks maxEntry against each.
type store interface {
	// get appends the value at key to dst.
	get(dst, key []byte) ([]byte, bool)
	set(key, value []byte)
	del(key []byte)
	reset()
	// maxEntry is the largest key+value the store holds.
	maxEntry() int
	// bytes is the memory the store holds now.
	bytes() int
}

// newStore picks the store by capacity. From 32 MiB up it is the off-heap
// one, which is sized in steps of 32 MiB and is not counted by the garbage
// collector; below, a heap one of exactly the capacity asked for (512 KiB at
// least), which the Go memory limit does count.
func newStore(maxBytes int) store {
	if maxBytes >= offHeapStep {
		return offHeap{fastcache.New(maxBytes)}
	}

	maxBytes = max(maxBytes, heapMin)

	return onHeap{cache: freecache.NewCache(maxBytes), capacity: maxBytes}
}

type offHeap struct{ cache *fastcache.Cache }

func (s offHeap) get(dst, key []byte) ([]byte, bool) { return s.cache.HasGet(dst, key) }
func (s offHeap) set(key, value []byte)              { s.cache.Set(key, value) }
func (s offHeap) del(key []byte)                     { s.cache.Del(key) }
func (s offHeap) reset()                             { s.cache.Reset() }
func (offHeap) maxEntry() int                        { return offHeapMaxEntry }

// bytes is what the store has taken from the system so far: it grows to the
// capacity as entries arrive and is never given back.
func (s offHeap) bytes() int {
	var stats fastcache.Stats

	s.cache.UpdateStats(&stats)

	return int(stats.BytesSize) //nolint:gosec // G115: bounded by the capacity
}

type onHeap struct {
	cache    *freecache.Cache
	capacity int
}

func (s onHeap) get(dst, key []byte) ([]byte, bool) {
	// The store copies into the buffer when the value fits its capacity and
	// allocates otherwise; either way the result starts at its first byte.
	value, err := s.cache.GetWithBuf(key, dst[:0])

	return value, err == nil
}

// set stores without an expiry of the store's own: the envelope carries the
// deadlines. The only error is an entry over maxEntry, which the caller has
// refused already.
func (s onHeap) set(key, value []byte) { _ = s.cache.Set(key, value, 0) }
func (s onHeap) del(key []byte)        { s.cache.Del(key) }
func (s onHeap) reset()                { s.cache.Clear() }

func (s onHeap) maxEntry() int {
	return s.capacity/heapSegments/heapEntryShare - heapEntryHeader
}

// bytes is the capacity: the rings are allocated in full at construction.
func (s onHeap) bytes() int { return s.capacity }

// keyBytes is key as the bytes a store reads: no store keeps or changes the
// slice it is given (both copy the key into their ring), so the string's own
// memory is lent instead of copied — a key of any length costs a read no
// allocation.
func keyBytes(key string) []byte {
	return unsafe.Slice(unsafe.StringData(key), len(key)) //nolint:gosec // G103: read-only view of an immutable string, see above
}
