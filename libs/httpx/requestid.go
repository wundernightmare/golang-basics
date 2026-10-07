package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// RequestIDHeader carries the per-request correlation id: honoured when a
// trusted proxy (Config.TrustedProxies) sends one, generated otherwise, always
// echoed on the response.
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

// WithRequestID attaches id to ctx the way the server does for a request, so
// a worker can give one unit of work (a consumed message, a job) the same
// request_id correlation in its logs. [NewRequestID] mints a fresh one.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// ValidRequestID reports whether s is usable as a correlation id: 1–128
// printable, non-space ASCII characters — enough for every id scheme in use
// (UUID, ULID, hex, base32) and nothing that could break a log line or a
// header. The server applies it to an inbound X-Request-Id; a consumer can
// apply it to a message's event id before [WithRequestID].
func ValidRequestID(s string) bool { return validRequestID(s) }

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

// NewRequestID returns 16 hex characters from 8 CSPRNG bytes (2^64 values:
// plenty for correlation, short enough to read out loud).
func NewRequestID() string { return newRequestID() }

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read does not fail on supported platforms
	return hex.EncodeToString(b[:])
}
