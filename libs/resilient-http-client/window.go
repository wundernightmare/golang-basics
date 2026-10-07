package resilient

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

// add counts a and b at now.
func (w *window) add(now time.Time, a, b int) {
	idx := now.UnixNano() / int64(w.slot)
	s := &w.slots[idx%windowSlots]
	if s.idx != idx {
		*s = windowSlot{idx: idx}
	}
	s.a += a
	s.b += b
}

// sum returns the totals of the slots inside the window ending at now.
func (w *window) sum(now time.Time) (a, b int) {
	idx := now.UnixNano() / int64(w.slot)
	for i := range w.slots {
		if s := &w.slots[i]; idx-s.idx < windowSlots {
			a += s.a
			b += s.b
		}
	}
	return a, b
}

// reset forgets everything counted.
func (w *window) reset() { w.slots = [windowSlots]windowSlot{} }
