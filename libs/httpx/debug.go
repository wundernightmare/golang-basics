package httpx

import (
	"context"
	"crypto/subtle"

	"github.com/gin-gonic/gin"
)

// DebugTokenHeader is the request header that turns on debug logging for one
// request when it carries the value of [Config].DebugToken.
const DebugTokenHeader = "X-Debug-Token" //nolint:gosec // a header name, not a credential

// DebugLoggingHeader is set on the response when the debug token was
// accepted, so the caller can tell "debug was on" from "token ignored".
const DebugLoggingHeader = "X-Debug-Logging"

type debugLoggingKey struct{}

// WithDebugLogging marks ctx so that every record a [NewLogger] logger emits
// with it passes regardless of the current level and is never sampled. The
// server sets it for requests carrying a valid [DebugTokenHeader]; a worker
// can set it for one message (e.g. on a record header) the same way. Use it
// for one unit of work at a time — it is the "debug this request" switch, not
// a level.
func WithDebugLogging(ctx context.Context) context.Context {
	return context.WithValue(ctx, debugLoggingKey{}, true)
}

// DebugLogging reports whether ctx was marked by [WithDebugLogging].
func DebugLogging(ctx context.Context) bool {
	v, _ := ctx.Value(debugLoggingKey{}).(bool)
	return v
}

// debugToken is the gin middleware behind [Config].DebugToken: a request whose
// X-Debug-Token equals token (constant-time compare) runs with a context
// marked by [WithDebugLogging] and gets X-Debug-Logging: on in the response.
// A missing or wrong token is ignored silently — the API port is public, and
// answering "wrong token" would make it an oracle and a log-flood vector.
func debugToken(token string) gin.HandlerFunc {
	want := []byte(token)
	return func(c *gin.Context) {
		got := c.GetHeader(DebugTokenHeader)
		if got == "" || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
			c.Next()
			return
		}
		c.Request = c.Request.WithContext(WithDebugLogging(c.Request.Context()))
		c.Header(DebugLoggingHeader, "on")
		c.Next()
	}
}
