package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateKeepsValidUTF8(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "abc", truncate("abc", 10))
	assert.Equal(t, "ж", truncate("жж", 3), "a rune cut in half is dropped")
}
