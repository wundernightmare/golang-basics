package resilient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOutboundError_Classification(t *testing.T) {
	tr := transient("boom", nil)
	ft := fatal("nope", nil)

	assert.True(t, tr.Transient())
	assert.False(t, tr.Fatal())
	assert.True(t, ft.Fatal())
	assert.False(t, ft.Transient())

	assert.True(t, IsTransient(tr))
	assert.False(t, IsTransient(ft))
	assert.True(t, IsFatal(ft))
	assert.False(t, IsFatal(tr))
}

func TestIsTransient_NilAndPlainError(t *testing.T) {
	assert.False(t, IsTransient(nil))
	assert.False(t, IsFatal(nil))
	assert.False(t, IsTransient(errors.New("plain")))
}

func TestOutboundError_UnwrapsCause(t *testing.T) {
	cause := errors.New("root cause")
	err := transient("wrapper", cause)
	assert.ErrorIs(t, err, cause)
	assert.Contains(t, err.Error(), "root cause")
}

func TestIsTransient_ThroughWrapping(t *testing.T) {
	wrapped := fmt.Errorf("context: %w", transient("inner", nil))
	assert.True(t, IsTransient(wrapped))
}

func TestClassifyTransport(t *testing.T) {
	t.Run("deadline is transient timeout", func(t *testing.T) {
		label, oe := classifyTransport(context.DeadlineExceeded)
		assert.Equal(t, "timeout", label)
		assert.True(t, oe.Transient())
	})
	t.Run("canceled is transient", func(t *testing.T) {
		label, oe := classifyTransport(context.Canceled)
		assert.Equal(t, "canceled", label)
		assert.True(t, oe.Transient())
	})
	t.Run("dns error is transient", func(t *testing.T) {
		label, oe := classifyTransport(&net.DNSError{Err: "no such host", Name: "x"})
		assert.Equal(t, "dns_error", label)
		assert.True(t, oe.Transient())
	})
	t.Run("op error is transient connect", func(t *testing.T) {
		label, oe := classifyTransport(&net.OpError{Op: "dial", Err: errors.New("refused")})
		assert.Equal(t, "connect_error", label)
		assert.True(t, oe.Transient())
	})
	t.Run("unknown is transient transport", func(t *testing.T) {
		label, oe := classifyTransport(errors.New("???"))
		assert.Equal(t, "transport_error", label)
		assert.True(t, oe.Transient())
	})
}
