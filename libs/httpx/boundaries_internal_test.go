package httpx

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin the exact edge of a rule — the value at the
// limit, not one comfortably inside it. They exist because mutation testing
// (`just mutate libs/httpx`) showed the edge could move by one unnoticed.

func TestRequestBudget(t *testing.T) {
	t.Parallel()

	const (
		none  = time.Duration(0)
		floor = 50 * time.Millisecond
	)

	for _, tc := range []struct {
		name              string
		configured, floor time.Duration
		header            string
		want              time.Duration
	}{
		{"no header: the configured timeout", time.Second, none, "", time.Second},
		{"no header, no timeout: no deadline", none, none, "", none},
		{"a header gives a deadline to a server without one", none, none, "500", 500 * time.Millisecond},
		{"shorter than configured: the caller's", time.Second, none, "200", 200 * time.Millisecond},
		{"longer than configured: never extends", time.Second, none, "5000", time.Second},
		{"below the floor: the floor", time.Second, floor, "1", floor},
		{"nine digits is the longest value read", none, none, "999999999", 999999999 * time.Millisecond},
		{"ten digits is ignored", time.Second, none, "1000000000", time.Second},
		{"zero is ignored", time.Second, none, "0", time.Second},
		{"not a number is ignored", time.Second, none, "12a", time.Second},
		{"a sign is ignored", time.Second, none, "-5", time.Second},
	} {
		assert.Equal(t, tc.want, requestBudget(tc.configured, tc.floor, tc.header), tc.name)
	}
}

func TestRouteOf(t *testing.T) {
	t.Parallel()

	for pattern, want := range map[string]string{
		"GET /tasks/{id}":             "/tasks/{id}",
		"/tasks/{id}":                 "/tasks/{id}",
		"GET example.com/tasks/{id}":  "/tasks/{id}",
		"example.com/tasks":           "/tasks",
		"POST /":                      "/",
		"GET example.com/a b/{rest…}": "/a b/{rest…}",
	} {
		assert.Equal(t, want, routeOf(pattern), pattern)
	}
}

func TestScheme(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "http", scheme(&http.Request{}))
	assert.Equal(t, "https", scheme(&http.Request{TLS: &tls.ConnectionState{}}))
}

func TestValidRequestIDEdges(t *testing.T) {
	t.Parallel()

	assert.True(t, validRequestID("!"), "the first printable character")
	assert.True(t, validRequestID("~"), "the last printable character")
	assert.False(t, validRequestID(" "), "a space")
	assert.False(t, validRequestID("\x7f"), "DEL")
	assert.True(t, validRequestID(repeat('a', maxRequestIDLen)), "the longest id")
	assert.False(t, validRequestID(repeat('a', maxRequestIDLen+1)))
}

func repeat(c byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = c
	}

	return string(out)
}

func TestProblemStatusEdges(t *testing.T) {
	t.Parallel()

	for status, want := range map[int]int{
		99: http.StatusInternalServerError, 100: 100, 999: 999, 1000: http.StatusInternalServerError,
		0: http.StatusInternalServerError, -1: http.StatusInternalServerError,
	} {
		assert.Equal(t, want, Problem{Status: status}.status(), "status %d", status)
	}
}

func TestIsHexEdges(t *testing.T) {
	t.Parallel()

	assert.True(t, isHex("0123456789abcdef"), "sixteen digits is the longest prefix shown")
	assert.False(t, isHex("0123456789abcdef0"))
	assert.False(t, isHex(""))
	assert.False(t, isHex("xyz"))
}

func TestBucketDepth(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 7, bucketDepth(2.5, 7), "an explicit burst")
	assert.Equal(t, 3, bucketDepth(2.5, 0), "no burst: one second's worth, rounded up")
	assert.Equal(t, 1, bucketDepth(0.1, 0), "never less than one request")
}

func TestRateLimiterExistsOnlyWithALimit(t *testing.T) {
	t.Parallel()

	assert.Nil(t, newRateLimiter(Config{}, prometheus.NewRegistry()), "zero is no limit")
	assert.Nil(t, newRateLimiter(Config{RateLimit: -1, RateLimitPerClient: -1}, prometheus.NewRegistry()))
	assert.NotNil(t, newRateLimiter(Config{RateLimit: 1, RateClientsMax: 1}, prometheus.NewRegistry()))
	assert.NotNil(t, newRateLimiter(Config{RateLimitPerClient: 1, RateClientsMax: 1}, prometheus.NewRegistry()))
}

func TestSampleWindowStartsAtItsDeadline(t *testing.T) {
	t.Parallel()

	var c sampleCounter

	assert.Equal(t, uint64(1), c.inc(10, 5))
	assert.Equal(t, uint64(2), c.inc(14, 5), "still inside the window")
	assert.Equal(t, uint64(1), c.inc(15, 5), "the instant the window ends starts the next one")
	assert.Equal(t, uint64(2), c.inc(19, 5))
}

func TestSamplingHandlerDefaults(t *testing.T) {
	t.Parallel()

	h := newSamplingHandler(slog.DiscardHandler, 1, 0, 0)
	assert.Equal(t, uint64(100), h.thereafter)
	assert.Equal(t, int64(time.Second), h.tick)

	h = newSamplingHandler(slog.DiscardHandler, 1, 7, 5*time.Second)
	assert.Equal(t, uint64(7), h.thereafter)
	assert.Equal(t, int64(5*time.Second), h.tick)
}

type countingHandler struct {
	slog.Handler

	n int
}

func (c *countingHandler) Handle(context.Context, slog.Record) error {
	c.n++

	return nil
}

// The window is the record's own time, not the wall clock: two records an
// hour apart are in different windows even when handled back to back.
func TestSamplingWindowFollowsTheRecordTime(t *testing.T) {
	t.Parallel()

	out := &countingHandler{Handler: slog.DiscardHandler}
	h := newSamplingHandler(out, 1, 1000, time.Minute)
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	require.NoError(t, h.Handle(t.Context(), slog.NewRecord(at, slog.LevelInfo, "m", 0)))
	require.NoError(t, h.Handle(t.Context(), slog.NewRecord(at.Add(time.Second), slog.LevelInfo, "m", 0)))
	assert.Equal(t, 1, out.n, "the second record of a window is dropped")

	require.NoError(t, h.Handle(t.Context(), slog.NewRecord(at.Add(time.Hour), slog.LevelInfo, "m", 0)))
	assert.Equal(t, 2, out.n, "an hour later is a new window")
}

// A record without a time is counted at the wall clock; the zero Time's
// UnixNano would open a window that never ends.
func TestSamplingCountsARecordWithoutATimeNow(t *testing.T) {
	t.Parallel()

	out := &countingHandler{Handler: slog.DiscardHandler}
	h := newSamplingHandler(out, 1, 1000, time.Minute)
	record := slog.NewRecord(time.Time{}, slog.LevelInfo, "m", 0)

	before := time.Now().UnixNano()

	require.NoError(t, h.Handle(t.Context(), record))

	c := &h.counters[h.slot(record)]
	assert.GreaterOrEqual(t, c.resetAt, before+int64(time.Minute), "the window ends a tick from now")
}

func TestHealthDefaultsAndStaleness(t *testing.T) {
	t.Parallel()

	h := NewHealth(0, 0)
	assert.Equal(t, defaultHealthInterval, h.interval)
	assert.Equal(t, defaultHealthTimeout, h.timeout)

	// A result is stale once it is older than two intervals plus one timeout:
	// one missed round and the time the next may take.
	h = NewHealth(10*time.Second, 3*time.Second)
	h.Register("db", func(context.Context) error { return nil })

	assert.True(t, h.stale(), "never evaluated")

	h.results["db"] = result{checkedAt: time.Now().Add(-22 * time.Second)}
	assert.False(t, h.stale(), "22s old, limit 23s")

	h.results["db"] = result{checkedAt: time.Now().Add(-24 * time.Second)}
	assert.True(t, h.stale(), "24s old, limit 23s")
}
