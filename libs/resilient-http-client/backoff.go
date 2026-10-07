package resilient

import (
	"errors"
	// nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used -- retry-backoff jitter is not security-sensitive; a CSPRNG (crypto/rand) is unnecessary here.
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// fullJitter is the delay before retry number retry (1 = first retry), AWS
// "full jitter": uniform in [0, min(capDelay, base × 2^retry)]. retry ≤ 0,
// or a non-positive base or cap, is no delay.
func fullJitter(retry int, base, capDelay time.Duration) time.Duration {
	if retry <= 0 {
		return 0
	}
	// float64 so base × 2^retry cannot overflow; the cap clamps it long before.
	ceiling := min(float64(base)*float64(int64(1)<<min(retry, 62)), float64(capDelay))
	if ceiling < 1 {
		return 0
	}
	//nolint:gosec // jitter smooths a backoff, it is not a security primitive.
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// maxRetryAfter bounds a parsed Retry-After, so absurd values cannot overflow
// a time.Duration; no caller waits anywhere near this long.
const maxRetryAfter = 24 * time.Hour

// parseRetryAfter reads a Retry-After header value: delay-seconds or an
// HTTP-date (RFC 9110 §10.2.3), relative to now. A date in the past is 0. ok
// is false when the value is neither.
func parseRetryAfter(v string, now time.Time) (d time.Duration, ok bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if v[0] >= '0' && v[0] <= '9' {
		secs, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) { // all digits, just huge
				return maxRetryAfter, true
			}
			return 0, false
		}
		if secs >= uint64(maxRetryAfter/time.Second) {
			return maxRetryAfter, true
		}
		return time.Duration(secs) * time.Second, true
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	return min(max(t.Sub(now), 0), maxRetryAfter), true
}

// idempotent reports whether req may be sent more than once: its method is
// idempotent (RFC 9110 §9.2.2) or it carries an Idempotency-Key.
func idempotent(req *http.Request) bool {
	switch req.Method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	}
	return req.Header.Get("Idempotency-Key") != ""
}

// retryable reports whether a failed attempt is worth another one: a timeout,
// a connection error, or a status that says "try again".
func retryable(err *OutboundError) bool {
	switch err.Kind {
	case KindTimeout, KindConnection:
		return true
	case KindStatus:
		return retryableStatus(err.StatusCode)
	}
	return false
}

func retryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	case http.StatusNotImplemented, http.StatusHTTPVersionNotSupported:
		return false
	}
	return code >= 500 && code <= 599
}
