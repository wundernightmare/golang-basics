package httpx

import (
	"context"
	"crypto/sha256"
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

// tokenDigest hashes a token before the constant-time compare, so the
// comparison neither leaks the token's length nor depends on it.
func tokenDigest(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}
