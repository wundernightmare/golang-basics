package resilient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// forceOpen drives the breaker Open immediately, for tests.
func (cb *CircuitBreaker) forceOpen() {
	cb.state.Store(uint32(CBOpen))
	cb.openedAtMS.Store(nowMS())
}

// setState is a test hatch for transitioning directly into a state.
func (cb *CircuitBreaker) setState(s CBState) { cb.state.Store(uint32(s)) }

func newCB(threshold float64, minReq uint32) *CircuitBreaker {
	return NewCircuitBreaker(threshold, minReq, 10*time.Second, time.Millisecond)
}

func TestCB_StartsClosedAndAllows(t *testing.T) {
	cb := newCB(0.5, 5)
	assert.Equal(t, CBClosed, cb.State())
	assert.True(t, cb.Allow())
}

func TestCB_OpensWhenRatioExceedsThreshold(t *testing.T) {
	cb := newCB(0.5, 4)
	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordFailure()
	cb.RecordFailure() // 2/4 = 50% ≥ 50% → Open
	assert.Equal(t, CBOpen, cb.State())
	assert.False(t, cb.Allow())
}

func TestCB_DoesNotOpenBeforeMinRequests(t *testing.T) {
	cb := newCB(0.5, 10)
	for range 4 {
		cb.RecordFailure() // 100% failures but only 4 < 10 requests
	}
	assert.Equal(t, CBClosed, cb.State())
}

func TestCB_HalfOpenProbeSuccessCloses(t *testing.T) {
	cb := newCB(0.5, 2)
	cb.RecordFailure()
	cb.RecordFailure() // → Open
	assert.Equal(t, CBOpen, cb.State())

	cb.setState(CBHalfOpen)
	assert.True(t, cb.Allow())
	cb.RecordSuccess()
	assert.Equal(t, CBClosed, cb.State())
}

func TestCB_HalfOpenProbeFailureReopens(t *testing.T) {
	cb := newCB(0.5, 2)
	cb.setState(CBHalfOpen)
	cb.RecordFailure()
	assert.Equal(t, CBOpen, cb.State())
}

func TestCB_OpenTransitionsToHalfOpenAfterTimeout(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 2, 10*time.Second, time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CBOpen, cb.State())

	time.Sleep(3 * time.Millisecond)
	assert.True(t, cb.Allow(), "should admit a probe after the half-open timeout")
	assert.Equal(t, CBHalfOpen, cb.State())
}

func TestCB_WindowRotationResetsCounters(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 5, 80*time.Millisecond, 30*time.Second)
	for range 4 {
		cb.RecordFailure() // below min → stays Closed
	}
	time.Sleep(120 * time.Millisecond)
	cb.RecordSuccess() // triggers window rotation
	// If rotation were a no-op we'd have 5 reqs at 80% failures → Open.
	assert.Equal(t, CBClosed, cb.State(), "window should have rotated")
}

func TestCB_ResetWindowAfterHalfOpenClearsHistory(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 2, time.Minute, 0)
	cb.RecordFailure()
	cb.RecordFailure() // → Open
	assert.Equal(t, CBOpen, cb.State())

	time.Sleep(time.Millisecond)
	assert.True(t, cb.Allow()) // → HalfOpen
	cb.RecordSuccess()         // → Closed + reset window
	assert.Equal(t, CBClosed, cb.State())

	cb.RecordFailure() // 1 req, 1 failure but reset cleared the old 2 → still < min
	assert.Equal(t, CBClosed, cb.State(), "old failures should have been cleared")
}

func TestCB_RatioUsesDivisionNotMultiplication(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 2, time.Minute, 30*time.Second)
	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordFailure() // 1/3 ≈ 0.33 < 0.5 → Closed
	assert.Equal(t, CBClosed, cb.State())
}

func TestCB_ForceOpenRejects(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 2, time.Minute, time.Minute)
	cb.forceOpen()
	assert.Equal(t, CBOpen, cb.State())
	assert.False(t, cb.Allow())
}
