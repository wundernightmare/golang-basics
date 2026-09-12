package resilient

import (
	"testing"
	"time"
)

// FuzzFullJitter: for any attempt/base/cap the delay is within [0, cap], zero
// for a non-positive attempt, base or cap, and never panics on extremes
// (attempt in the millions, base or cap at the int64 edges).
func FuzzFullJitter(f *testing.F) {
	f.Add(0, int64(time.Second), int64(30*time.Second))
	f.Add(1, int64(time.Nanosecond), int64(time.Nanosecond))
	f.Add(62, int64(time.Hour), int64(time.Hour))
	f.Add(1<<20, int64(1<<62), int64(1<<62))
	f.Add(-5, int64(-1), int64(-1))
	f.Fuzz(func(t *testing.T, attempt int, base, capDelay int64) {
		d := FullJitter(attempt, time.Duration(base), time.Duration(capDelay))
		if d < 0 {
			t.Fatalf("negative delay %v", d)
		}
		if attempt <= 0 || base <= 0 || capDelay <= 0 {
			if d != 0 {
				t.Fatalf("expected no delay for attempt=%d base=%d cap=%d, got %v", attempt, base, capDelay, d)
			}
			return
		}
		if d > time.Duration(capDelay) {
			t.Fatalf("delay %v above cap %v", d, time.Duration(capDelay))
		}
	})
}

// FuzzCircuitBreaker: any interleaving of allow/success/failure keeps the
// breaker in a valid state and never panics, whatever the configuration.
func FuzzCircuitBreaker(f *testing.F) {
	f.Add(0.5, uint32(2), int64(100), int64(10), []byte{0, 1, 2, 1, 1, 0})
	f.Add(1.5, uint32(0), int64(0), int64(0), []byte{2, 2, 2, 0})
	f.Fuzz(func(t *testing.T, threshold float64, minReq uint32, windowMS, halfOpenMS int64, ops []byte) {
		cb := NewCircuitBreaker(threshold, minReq, time.Duration(windowMS)*time.Millisecond, time.Duration(halfOpenMS)*time.Millisecond)
		for _, op := range ops {
			switch op % 3 {
			case 0:
				cb.Allow()
			case 1:
				cb.RecordSuccess()
			case 2:
				cb.RecordFailure()
			}
			if s := cb.State(); s > CBHalfOpen {
				t.Fatalf("invalid state %d", s)
			}
		}
	})
}
