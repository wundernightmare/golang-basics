package worker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRU_BoundedAndRecencyOrdered(t *testing.T) {
	l := newLRU(2)
	l.add("a")
	l.add("b")
	assert.True(t, l.contains("a"), "refreshes a")
	l.add("c") // evicts b, the least recent
	assert.True(t, l.contains("a"))
	assert.False(t, l.contains("b"))
	assert.True(t, l.contains("c"))
	assert.Equal(t, 2, l.len())

	big := newLRU(100)
	for i := range 1000 {
		big.add(fmt.Sprint(i))
	}
	assert.Equal(t, 100, big.len(), "never grows past its size")
	assert.True(t, big.contains("999"))
	assert.False(t, big.contains("0"))
}
