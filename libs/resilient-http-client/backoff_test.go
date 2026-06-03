package resilient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFullJitter_AttemptZeroIsImmediate(t *testing.T) {
	assert.Equal(t, time.Duration(0), FullJitter(0, 100*time.Millisecond, 30*time.Second))
}

func TestFullJitter_ZeroCapIsImmediate(t *testing.T) {
	assert.Equal(t, time.Duration(0), FullJitter(5, 100*time.Millisecond, 0))
}

func TestFullJitter_WithinCap(t *testing.T) {
	capDelay := 5 * time.Second
	for attempt := 1; attempt <= 20; attempt++ {
		for range 50 {
			d := FullJitter(attempt, 100*time.Millisecond, capDelay)
			assert.LessOrEqual(t, d, capDelay)
			assert.GreaterOrEqual(t, d, time.Duration(0))
		}
	}
}

func TestFullJitter_ProducesNonZeroDelays(t *testing.T) {
	// With a large base the probability of all-zero across 200 draws is nil.
	anyNonZero := false
	for range 200 {
		if FullJitter(1, time.Second, 5*time.Second) > 0 {
			anyNonZero = true
			break
		}
	}
	assert.True(t, anyNonZero, "expected at least one non-zero delay")
}

func TestFullJitter_CeilingGrowsWithAttempt(t *testing.T) {
	// The observed maximum at a higher attempt should exceed the base ceiling.
	var maxLow, maxHigh time.Duration
	for range 500 {
		if d := FullJitter(1, time.Second, 100*time.Second); d > maxLow {
			maxLow = d
		}
		if d := FullJitter(4, time.Second, 100*time.Second); d > maxHigh {
			maxHigh = d
		}
	}
	assert.Greater(t, maxHigh, maxLow)
}
