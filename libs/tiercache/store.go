package tiercache

// Store is L1: a bounded amount of memory in the process that holds the
// entries it has room for and forgets the rest. Two are provided — offheap
// and onheap, in the packages of those names — and a service may bring its
// own; storetest.Run is the proof that one keeps this contract.
//
// The contract says what a [Cache] relies on, not how a store is built: what
// it evicts and when, how it is laid out and where its memory lives are its
// own business.
//
//   - Concurrency. Every method may be called from any number of goroutines
//     at once, and takes effect at one instant between its call and its
//     return.
//
//   - No failures, no waiting. A method does no I/O, returns no error and
//     blocks on nothing but the store's own short critical sections: Get is
//     on the path of every read of the cache.
//
//   - Keys and values. A key is a non-empty string of up to [MaxKeyLen]
//     bytes, a value any bytes, none included; neither needs to be text. A
//     value of no bytes is a value: it is found.
//
//   - A store forgets, and never misremembers. Get may report any key as not
//     found, at any time — that is eviction. What it must never do is report
//     a value other than the one given to the latest Set of that key: not
//     another key's, not a part of one, and not an earlier one. In
//     particular, once Delete or Reset has returned, and once a Set has
//     returned whether it kept its value or not, the value that was there
//     before is gone for good.
//
//   - A store is of use. With no other call in between, a Get that follows a
//     Set of an entry within MaxEntry finds it.
//
//   - Nothing is shared. A store keeps no reference to the value given to
//     Set nor to the buffer given to Get after the call returns, and changes
//     neither beyond appending to that buffer; what Get returns is the
//     caller's.
//
//   - Bounded memory. Bytes never exceeds the capacity the store was built
//     with, as its provider documents it, however much is written.
type Store interface {
	// Get appends the value at key to dst and returns the result, or returns
	// dst as it was and false.
	Get(dst []byte, key string) (value []byte, found bool)
	// Set makes value the value at key, or — when the store has no room for
	// it — leaves key not found. An entry of len(key)+len(value) within
	// MaxEntry is kept.
	Set(key string, value []byte)
	// Delete leaves key not found. Deleting a key that is not there is fine.
	Delete(key string)
	// Reset leaves every key not found. The store stays usable; a store that
	// can hand memory back does it here.
	Reset()
	// MaxEntry is the largest len(key)+len(value) the store keeps: positive,
	// and the same for as long as the store lives. A [Cache] does not offer
	// it more — such an entry is served from L2 and counted.
	MaxEntry() int
	// Bytes is the memory the store holds for its entries now, for the
	// tiercache_l1_bytes gauge: cheap enough to be read on every scrape.
	Bytes() int
}

// StoreFactory builds the [Store] of one cache: offheap.New, onheap.New, or
// a service's own. maxBytes is [Config.MaxBytes], always positive; what a
// store does with a capacity it cannot have exactly (a minimum, a step) is
// for its provider to document.
type StoreFactory func(maxBytes int) Store
