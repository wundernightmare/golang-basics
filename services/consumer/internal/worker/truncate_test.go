package worker

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestTruncateNeverCutsARuneInHalf(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "task.created", truncate("task.created", 64), "short enough: unchanged")
	assert.Equal(t, "abcd", truncate("abcdef", 4))

	// "é" is two bytes: a cut after the first one drops the whole rune.
	got := truncate("abcé", 4)
	assert.Equal(t, "abc", got)
	assert.True(t, utf8.ValidString(got))
	assert.Equal(t, "abcé", truncate("abcéf", 5), "a cut on a rune boundary keeps the rune")
}
