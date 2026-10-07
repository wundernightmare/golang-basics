package worker

import (
	"container/list"
	"sync"
)

// lru is a bounded set of strings that forgets the least recently added or
// looked-up entry first: the "last N event ids" of the dedupe.
//
// It is in-memory and per process, which is enough to absorb the common
// duplicates (a redelivered batch after a failed commit, a relay retry) but
// not a redelivery that lands on another replica or after a restart. A
// consumer whose effect must be exactly-once keeps the applied ids next to
// the effect instead — a unique key in the same database transaction.
type lru struct {
	mu    sync.Mutex
	size  int
	order *list.List               // front = most recent
	items map[string]*list.Element // value: the id
}

func newLRU(size int) *lru {
	return &lru{size: size, order: list.New(), items: make(map[string]*list.Element, size)}
}

// contains reports whether id is in the set, refreshing it if so.
func (l *lru) contains(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.items[id]; ok {
		l.order.MoveToFront(e)
		return true
	}
	return false
}

// add inserts id, evicting the oldest entry when full.
func (l *lru) add(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.items[id]; ok {
		l.order.MoveToFront(e)
		return
	}
	l.items[id] = l.order.PushFront(id)
	if l.order.Len() > l.size {
		oldest := l.order.Back()
		l.order.Remove(oldest)
		delete(l.items, oldest.Value.(string))
	}
}

// len is the number of remembered ids.
func (l *lru) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.order.Len()
}
