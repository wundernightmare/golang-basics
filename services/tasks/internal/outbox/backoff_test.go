package outbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	const interval, maxDelay = time.Second, 30 * time.Second
	for n, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 6: 30 * time.Second, 50: 30 * time.Second} {
		for range 50 {
			d := backoff(n, interval, maxDelay)
			assert.GreaterOrEqual(t, d, want/2, "n=%d", n)
			assert.LessOrEqual(t, d, want, "n=%d", n)
		}
	}
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	assert.Equal(t, "abc", truncate("abc", 10))
	assert.Equal(t, "ж", truncate("жж", 3), "a rune cut in half is dropped")
}
