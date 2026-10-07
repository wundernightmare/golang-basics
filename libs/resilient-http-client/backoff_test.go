package resilient

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFullJitter_Bounds(t *testing.T) {
	const base, capDelay = 10 * time.Millisecond, 100 * time.Millisecond
	assert.Zero(t, fullJitter(0, base, capDelay), "no retry, no delay")
	assert.Zero(t, fullJitter(-1, base, capDelay))
	assert.Zero(t, fullJitter(3, 0, capDelay), "zero base")
	assert.Zero(t, fullJitter(3, base, 0), "zero cap")

	for _, tc := range []struct {
		retry   int
		ceiling time.Duration
	}{
		{1, 20 * time.Millisecond},
		{2, 40 * time.Millisecond},
		{3, 80 * time.Millisecond},
		{4, capDelay},
		{1000, capDelay}, // no overflow
	} {
		var top time.Duration
		for range 2000 {
			d := fullJitter(tc.retry, base, capDelay)
			assert.GreaterOrEqual(t, d, time.Duration(0))
			assert.LessOrEqual(t, d, tc.ceiling, "retry %d", tc.retry)
			top = max(top, d)
		}
		assert.Greater(t, top, tc.ceiling/2, "retry %d: the range reaches up to the ceiling", tc.retry)
	}
	assert.LessOrEqual(t, fullJitter(1, 1, 1), time.Duration(1), "a 1ns ceiling is still a valid range")
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"0", 0, true},
		{"3", 3 * time.Second, true},
		{" 120 ", 2 * time.Minute, true},
		{"86399", 86399 * time.Second, true},
		{"86400", maxRetryAfter, true},
		{"99999999999999999999999", maxRetryAfter, true},
		{now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second, true},
		{now.Add(-time.Hour).Format(http.TimeFormat), 0, true},
		{now.Add(48 * time.Hour).Format(http.TimeFormat), maxRetryAfter, true},
		{"", 0, false},
		{"   ", 0, false},
		{"-1", 0, false},
		{"+1", 0, false},
		{"1.5", 0, false},
		{"3s", 0, false},
		{"soon", 0, false},
	} {
		d, ok := parseRetryAfter(tc.in, now)
		assert.Equal(t, tc.ok, ok, "%q", tc.in)
		assert.Equal(t, tc.want, d, "%q", tc.in)
	}
}

func FuzzParseRetryAfter(f *testing.F) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	for _, s := range []string{"0", "5", "18446744073709551616", "Fri, 25 Sep 2026 12:01:00 GMT", "Sunday, 06-Nov-94 08:49:37 GMT", "", "x", "-3"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		d, ok := parseRetryAfter(s, now)
		if !ok {
			if d != 0 {
				t.Fatalf("%q: not ok but %v", s, d)
			}
			return
		}
		if d < 0 || d > maxRetryAfter {
			t.Fatalf("%q: %v out of [0, %v]", s, d, maxRetryAfter)
		}
		if n, err := strconv.ParseUint(s, 10, 64); err == nil && n < 86400 && d != time.Duration(n)*time.Second {
			t.Fatalf("%q: seconds form parsed to %v", s, d)
		}
	})
}

func TestIdempotent(t *testing.T) {
	for _, tc := range []struct {
		method string
		key    bool
		want   bool
	}{
		{"", false, true},
		{http.MethodGet, false, true},
		{http.MethodHead, false, true},
		{http.MethodOptions, false, true},
		{http.MethodPut, false, true},
		{http.MethodDelete, false, true},
		{http.MethodPost, false, false},
		{http.MethodPatch, false, false},
		{http.MethodPost, true, true},
		{http.MethodPatch, true, true},
	} {
		req := &http.Request{Method: tc.method, Header: http.Header{}}
		if tc.key {
			req.Header.Set("Idempotency-Key", "k1")
		}
		assert.Equal(t, tc.want, idempotent(req), "%s key=%v", tc.method, tc.key)
	}
}

func TestRetryable(t *testing.T) {
	for code := 100; code < 600; code++ {
		want := code == 408 || code == 425 || code == 429 || (code >= 500 && code != 501 && code != 505)
		assert.Equal(t, want, retryable(&OutboundError{Kind: KindStatus, StatusCode: code}), "status %d", code)
	}
	assert.False(t, retryableStatus(600), "not a status")
	for k, want := range map[Kind]bool{
		KindTimeout: true, KindConnection: true,
		KindCircuitOpen: false, KindRateLimited: false, KindBulkheadFull: false, KindShutdown: false,
		KindCanceled: false, KindRedirect: false, KindInvalid: false,
	} {
		assert.Equal(t, want, retryable(&OutboundError{Kind: k}), k.String())
	}
	assert.True(t, Retryable(fmt.Errorf("wrapped: %w", &OutboundError{Kind: KindTimeout})))
	assert.False(t, Retryable(errors.New("plain")))
}

func TestOutboundError_IsAsAndMessage(t *testing.T) {
	cause := errors.New("boom")
	err := fmt.Errorf("call: %w", &OutboundError{Kind: KindStatus, Target: "api", StatusCode: 503, Err: cause, Body: []byte("secret")})
	require.ErrorIs(t, err, KindStatus)
	require.NotErrorIs(t, err, KindTimeout)
	require.ErrorIs(t, err, cause)
	var oe *OutboundError
	if assert.ErrorAs(t, err, &oe) {
		assert.Equal(t, 503, oe.StatusCode)
	}
	assert.Equal(t, "call: resilient: api: status 503: boom", err.Error())
	assert.NotContains(t, err.Error(), "secret", "the body stays out of the message")
	assert.Equal(t, "resilient: api: circuit_open", (&OutboundError{Kind: KindCircuitOpen, Target: "api"}).Error())
	assert.Equal(t, "resilient: timeout", KindTimeout.Error())
	assert.Equal(t, "kind(0)", Kind(0).String())
}
