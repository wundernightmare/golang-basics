package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"github.com/gin-gonic/gin"
)

// RequestIDHeader carries the per-request correlation id: honoured when the
// client (or an ingress) sends one, generated otherwise, always echoed on the
// response.
const RequestIDHeader = "X-Request-Id"

// maxRequestIDLen bounds an inbound id: it is echoed back and logged, so it
// must not become a vehicle for arbitrary payloads.
const maxRequestIDLen = 128

type requestIDKey struct{}

// RequestIDFromContext returns the request id the server attached to ctx, or
// "" outside a request. Every log record emitted with the request context
// already carries it as request_id; use this to put it somewhere else (a
// downstream call, a message header).
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// requestID is the outermost API middleware: it settles the request id before
// tracing, logging or the handler run, so every one of them sees it.
//
// Why a request id when there is a trace id: tracing is opt-in here (and
// sampled where it is on), so trace_id is absent exactly when someone needs
// to tie a client's "it failed" to a log line. The request id is 100% present,
// costs eight random bytes, and travels in the response header and in every
// log record and problem+json body of the request.
func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(RequestIDHeader)
		if !validRequestID(id) {
			id = newRequestID()
		}
		c.Request = c.Request.WithContext(withRequestID(c.Request.Context(), id))
		c.Header(RequestIDHeader, id)
		c.Next()
	}
}

// validRequestID accepts 1–128 printable, non-space ASCII characters —
// enough for every id scheme in use (UUID, ULID, hex, base32) and nothing
// that could break a log line or a header.
func validRequestID(s string) bool {
	if s == "" || len(s) > maxRequestIDLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= ' ' || s[i] > '~' {
			return false
		}
	}
	return true
}

// newRequestID returns 16 hex characters from 8 CSPRNG bytes (2^64 values:
// plenty for correlation, short enough to read out loud).
func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read does not fail on supported platforms
	return hex.EncodeToString(b[:])
}
