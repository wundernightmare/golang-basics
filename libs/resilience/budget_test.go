package resilience

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBudget(t *testing.T, ratio float64, minRetries int) (*RetryBudget, *fakeClock) {
	t.Helper()

	clk := newFakeClock()
	b := NewRetryBudget(ratio, minRetries, 10*time.Second, clk.now)
	require.NotNil(t, b)

	return b, clk
}

func retries(b *RetryBudget, n int) (granted int) {
	for range n {
		if b.TryRetry() {
			granted++
		}
	}

	return granted
}

func TestRetryBudget_Disabled(t *testing.T) {
	t.Parallel()

	b := NewRetryBudget(0.2, 10, 0, time.Now)
	require.Nil(t, b)
	b.Request()
	assert.Equal(t, 1000, retries(b, 1000))

	assert.Nil(t, NewRetryBudget(0.2, 10, -time.Second, time.Now), "a negative window is no budget either")
}

// Requests and retries counted from many goroutines are all counted, and
// the retries granted between them stay within the bound.
func TestRetryBudget_ConcurrentUseKeepsTheCount(t *testing.T) {
	t.Parallel()

	const goroutines, each = 8, 2000

	b, clk := testBudget(t, 0.5, 0)

	var (
		wg      sync.WaitGroup
		granted atomic.Int64
	)

	for range goroutines {
		wg.Go(func() {
			for range each {
				b.Request()

				if b.TryRetry() {
					granted.Add(1)
				}
			}
		})
	}

	wg.Wait()

	requests, spent := b.w.sum(clk.now())
	assert.Equal(t, goroutines*each, requests)
	assert.Equal(t, int64(goroutines*each/2), granted.Load(), "half of the requests, each grant counted once")
	assert.Equal(t, int(granted.Load()), spent)
}

// A ratio that is not a number must not turn the budget off.
func TestRetryBudget_NaNRatioGrantsNothing(t *testing.T) {
	t.Parallel()

	b, _ := testBudget(t, math.NaN(), 3)
	b.Request()
	assert.Equal(t, 0, retries(b, 10))
}

func TestRetryBudget_MinRetriesWithoutRequests(t *testing.T) {
	t.Parallel()

	b, _ := testBudget(t, 0.2, 3)
	assert.Equal(t, 3, retries(b, 10))
}

func TestRetryBudget_RatioOfRequests(t *testing.T) {
	t.Parallel()

	b, _ := testBudget(t, 0.2, 0)
	for range 100 {
		b.Request()
	}

	assert.Equal(t, 20, retries(b, 50), "20% of 100 requests")

	b.Request()
	b.Request()
	b.Request()
	b.Request()
	assert.Equal(t, 1, retries(b, 5), "20 retries < 0.2×104 = 20.8: one more, then 21 ≥ 20.8")
}

func TestRetryBudget_RatioBoundaryIsExclusive(t *testing.T) {
	t.Parallel()

	b, _ := testBudget(t, 0.5, 1)
	b.Request()
	b.Request()
	assert.Equal(t, 2, retries(b, 5), "1 + 0.5×2 = 2 retries")
}

func TestRetryBudget_WindowSlides(t *testing.T) {
	t.Parallel()

	b, clk := testBudget(t, 0, 2)
	assert.Equal(t, 2, retries(b, 5))
	clk.advance(5 * time.Second)
	assert.Equal(t, 0, retries(b, 1), "still inside the window")
	clk.advance(5 * time.Second)
	assert.Equal(t, 2, retries(b, 5), "the spent retries left the window")
}

func TestWindow_SumAndReset(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	win := newWindow(time.Second, now)
	win.add(now, 1, 2)
	win.add(now.Add(500*time.Millisecond), 3, 4)
	a, b := win.sum(now.Add(950 * time.Millisecond))
	assert.Equal(t, [2]int{4, 6}, [2]int{a, b}, "a slot begins at its boundary: the add made on one is in for the whole window")

	a, b = win.sum(now.Add(time.Second))
	assert.Equal(t, [2]int{3, 4}, [2]int{a, b}, "the first slot slid out")

	win.reset()
	a, b = win.sum(now.Add(900 * time.Millisecond))
	assert.Equal(t, [2]int{0, 0}, [2]int{a, b})

	win.add(now.Add(-50*time.Millisecond), 1, 1)
	a, b = win.sum(now.Add(850 * time.Millisecond))
	assert.Equal(t, [2]int{1, 1}, [2]int{a, b}, "an instant before the base has a slot like any other")

	a, b = win.sum(now.Add(950 * time.Millisecond))
	assert.Equal(t, [2]int{0, 0}, [2]int{a, b}, "and that slot slides out on time")

	tiny := newWindow(time.Nanosecond, now)
	assert.Equal(t, time.Duration(1), tiny.slot, "slots are at least 1ns")
}
