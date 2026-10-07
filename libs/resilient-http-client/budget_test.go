package resilient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBudget(t *testing.T, ratio float64, minRetries int) (*retryBudget, *fakeClock) {
	t.Helper()
	tc := DefaultTarget("t")
	tc.RetryBudgetRatio = ratio
	tc.RetryBudgetMinRetries = minRetries
	tc.RetryBudgetWindow = 10 * time.Second
	clk := newFakeClock()
	b := newRetryBudget(&tc, clk.now)
	require.NotNil(t, b)
	return b, clk
}

func retries(b *retryBudget, n int) (granted int) {
	for range n {
		if b.tryRetry() {
			granted++
		}
	}
	return granted
}

func TestRetryBudget_Disabled(t *testing.T) {
	tc := DefaultTarget("t")
	tc.RetryBudgetWindow = 0
	b := newRetryBudget(&tc, time.Now)
	require.Nil(t, b)
	b.request()
	assert.Equal(t, 1000, retries(b, 1000))
}

func TestRetryBudget_MinRetriesWithoutRequests(t *testing.T) {
	b, _ := testBudget(t, 0.2, 3)
	assert.Equal(t, 3, retries(b, 10))
}

func TestRetryBudget_RatioOfRequests(t *testing.T) {
	b, _ := testBudget(t, 0.2, 0)
	for range 100 {
		b.request()
	}
	assert.Equal(t, 20, retries(b, 50), "20% of 100 requests")

	b.request()
	b.request()
	b.request()
	b.request()
	assert.Equal(t, 1, retries(b, 5), "20 retries < 0.2×104 = 20.8: one more, then 21 ≥ 20.8")
}

func TestRetryBudget_RatioBoundaryIsExclusive(t *testing.T) {
	b, _ := testBudget(t, 0.5, 1)
	b.request()
	b.request()
	assert.Equal(t, 2, retries(b, 5), "1 + 0.5×2 = 2 retries")
}

func TestRetryBudget_WindowSlides(t *testing.T) {
	b, clk := testBudget(t, 0, 2)
	assert.Equal(t, 2, retries(b, 5))
	clk.advance(5 * time.Second)
	assert.Equal(t, 0, retries(b, 1), "still inside the window")
	clk.advance(5 * time.Second)
	assert.Equal(t, 2, retries(b, 5), "the spent retries left the window")
}

func TestWindow_SumAndReset(t *testing.T) {
	w := newWindow(time.Second)
	now := time.Unix(100, 0)
	w.add(now, 1, 2)
	w.add(now.Add(500*time.Millisecond), 3, 4)
	a, b := w.sum(now.Add(900 * time.Millisecond))
	assert.Equal(t, [2]int{4, 6}, [2]int{a, b})

	a, b = w.sum(now.Add(time.Second))
	assert.Equal(t, [2]int{3, 4}, [2]int{a, b}, "the first slot slid out")

	w.reset()
	a, b = w.sum(now.Add(900 * time.Millisecond))
	assert.Equal(t, [2]int{0, 0}, [2]int{a, b})

	tiny := newWindow(time.Nanosecond)
	assert.Equal(t, time.Duration(1), tiny.slot, "slots are at least 1ns")
}
