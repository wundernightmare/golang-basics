package resilience

import "time"

// windowSlots is how many slots a sliding window is cut into: the window
// slides in steps of 1/windowSlots of its length.
const windowSlots = 10

type windowSlot struct {
	idx  int64 // the slot of time this entry counts; stale once out of the window
	a, b int
}

// window counts two quantities over a sliding time window made of
// windowSlots fixed slots. It is not safe for concurrent use: its owner holds
// a lock.
type window struct {
	slot  time.Duration
	slots [windowSlots]windowSlot
}

func newWindow(length time.Duration) window {
	return window{slot: max(length/windowSlots, 1)}
}

// index is the slot now falls in, counted from 1970 and rounded down, so
// the slots before 1970 are as long as the ones after it.
func (w *window) index(now time.Time) int64 {
	ns, slot := now.UnixNano(), int64(w.slot)
	if ns%slot < 0 {
		return ns/slot - 1
	}

	return ns / slot
}

// add counts a and b in now's slot; a slot still holding an earlier turn of
// the ring is zeroed first.
func (w *window) add(now time.Time, a, b int) {
	idx := w.index(now)

	s := &w.slots[(idx%windowSlots+windowSlots)%windowSlots] // idx is negative before 1970
	if s.idx != idx {
		*s = windowSlot{idx: idx}
	}

	s.a += a
	s.b += b
}

// sum returns the totals of the slots inside the window ending at now.
func (w *window) sum(now time.Time) (a, b int) {
	idx := w.index(now)
	for i := range w.slots {
		if s := &w.slots[i]; idx-s.idx < windowSlots {
			a += s.a
			b += s.b
		}
	}

	return a, b
}

func (w *window) reset() { w.slots = [windowSlots]windowSlot{} }
