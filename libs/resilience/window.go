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
	base  time.Time // slots are counted from here
	slots [windowSlots]windowSlot
}

// newWindow starts a window of the given length at base, the owner's clock
// reading at construction. Slots are found by the time elapsed since base
// rather than by a calendar position: time.Time.Sub uses the monotonic
// readings when both instants carry one (time.Now does), so a wall clock set
// forward or back neither empties the window nor keeps stale counts in it.
func newWindow(length time.Duration, base time.Time) window {
	return window{slot: max(length/windowSlots, 1), base: base}
}

// index is the slot now falls in, counted from base and rounded down, so
// the slots before base (an injected clock may step back) are as long as
// the ones after it.
func (w *window) index(now time.Time) int64 {
	ns, slot := int64(now.Sub(w.base)), int64(w.slot)
	if ns%slot < 0 {
		return ns/slot - 1
	}

	return ns / slot
}

// add counts a and b in now's slot; a slot still holding an earlier turn of
// the ring is zeroed first.
func (w *window) add(now time.Time, a, b int) {
	idx := w.index(now)

	s := &w.slots[(idx%windowSlots+windowSlots)%windowSlots] // idx is negative before base
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

// reset forgets the counts; the slots stay where base put them.
func (w *window) reset() { w.slots = [windowSlots]windowSlot{} }
