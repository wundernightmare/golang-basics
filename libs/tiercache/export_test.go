package tiercache

import "sync"

// mapStore is the L1 of this module's tests, which are about the cache and
// need no more of a store than a map: the real ones are modules of their own
// (offheap, onheap) that import this one, each proven by storetest. It keeps
// everything it is given — eviction is the stores' business.
type mapStore struct {
	mu      sync.Mutex
	entries map[string][]byte
	bytes   int
}

// MapStoreMaxEntry is the entry limit of the test store — offheap's, so that
// a value the tests call too large for L1 is too large for a real one.
const MapStoreMaxEntry = 64<<10 - 5

// NewMapStore is the [StoreFactory] of the tests.
func NewMapStore(int) Store { return &mapStore{entries: map[string][]byte{}} }

func (*mapStore) MaxEntry() int { return MapStoreMaxEntry }

func (s *mapStore) Bytes() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.bytes
}

func (s *mapStore) Get(dst []byte, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, found := s.entries[key]

	return append(dst, value...), found
}

func (s *mapStore) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.forget(key)

	s.entries[key] = append([]byte(nil), value...)
	s.bytes += len(key) + len(value)
}

func (s *mapStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.forget(key)
}

func (s *mapStore) forget(key string) {
	if value, found := s.entries[key]; found {
		s.bytes -= len(key) + len(value)

		delete(s.entries, key)
	}
}

func (s *mapStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.entries)
	s.bytes = 0
}
