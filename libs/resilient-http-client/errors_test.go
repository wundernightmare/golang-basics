package resilient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/ozontech/testo"

	"github.com/tracehubmmp/golang-basics/libs/testx"

	"github.com/stretchr/testify/assert"
)

func TestOutboundError_Classification(t *testing.T) {
	testx.Run(t, func(t testx.T) {
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
	}, "resilient-http-client", "unit")
}

func TestIsTransient_NilAndPlainError(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		assert.False(t, IsTransient(nil))
		assert.False(t, IsFatal(nil))
		assert.False(t, IsTransient(errors.New("plain")))
	}, "resilient-http-client", "unit")
}

func TestOutboundError_UnwrapsCause(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		cause := errors.New("root cause")
		err := transient("wrapper", cause)
		assert.ErrorIs(t, err, cause)
		assert.Contains(t, err.Error(), "root cause")
	}, "resilient-http-client", "unit")
}

func TestIsTransient_ThroughWrapping(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		wrapped := fmt.Errorf("context: %w", transient("inner", nil))
		assert.True(t, IsTransient(wrapped))
	}, "resilient-http-client", "unit")
}

func TestClassifyTransport(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		testo.Run(t, "deadline is transient timeout", func(t testx.T) {
			label, oe := classifyTransport(context.DeadlineExceeded)
			assert.Equal(t, "timeout", label)
			assert.True(t, oe.Transient())
		})
		testo.Run(t, "canceled is transient", func(t testx.T) {
			label, oe := classifyTransport(context.Canceled)
			assert.Equal(t, "canceled", label)
			assert.True(t, oe.Transient())
		})
		testo.Run(t, "dns error is transient", func(t testx.T) {
			label, oe := classifyTransport(&net.DNSError{Err: "no such host", Name: "x"})
			assert.Equal(t, "dns_error", label)
			assert.True(t, oe.Transient())
		})
		testo.Run(t, "op error is transient connect", func(t testx.T) {
			label, oe := classifyTransport(&net.OpError{Op: "dial", Err: errors.New("refused")})
			assert.Equal(t, "connect_error", label)
			assert.True(t, oe.Transient())
		})
		testo.Run(t, "unknown is transient transport", func(t testx.T) {
			label, oe := classifyTransport(errors.New("???"))
			assert.Equal(t, "transport_error", label)
			assert.True(t, oe.Transient())
		})
	}, "resilient-http-client", "unit")
}
