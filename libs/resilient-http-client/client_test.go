package resilient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oneTarget builds a config with a single target named "t" plus overrides.
func clientFor(t *testing.T, tc TargetConfig, opts ...Option) *Client {
	t.Helper()
	if tc.Name == "" {
		tc.Name = "t"
	}
	cfg := DefaultConfig()
	cfg.OutboundTargets = []TargetConfig{tc}
	c, err := New(cfg, opts...)
	require.NoError(t, err)
	return c
}

// readBody reads (but does not close) the body — callers own closing so the
// bodyclose linter can see it.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

func TestSend_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{})
	resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "pong", readBody(t, resp))
}

func TestSend_4xxIsFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{CBMinRequests: 1})
	resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	assert.True(t, IsFatal(err))
}

func TestSend_5xxIsTransientAndTripsBreaker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{CBMinRequests: 1, CBThreshold: 0.5})
	resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	assert.True(t, IsTransient(err))
	assert.Equal(t, CBOpen, c.getPolicy("t").breaker.State(), "one 5xx with min=1 trips the breaker")
}

func TestSend_429IsTransientWithoutBreakerPenalty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{CBMinRequests: 1, CBThreshold: 0.5})
	resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	assert.True(t, IsTransient(err))
	assert.Equal(t, CBClosed, c.getPolicy("t").breaker.State(), "429 must not penalise the breaker")
}

func TestSend_CircuitOpenShortCircuits(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{})
	c.getPolicy("t").breaker.forceOpen()

	resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	assert.True(t, IsTransient(err))
	assert.Contains(t, err.Error(), "circuit breaker")
	assert.Equal(t, int64(0), hits.Load(), "open breaker must not hit the upstream")
}

func TestSend_RateLimitedIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{RateLimit: 1})
	// First call consumes the single token; the immediate second is rejected.
	resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
	require.NoError(t, err)
	_ = resp.Body.Close()

	resp2, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
	if resp2 != nil {
		_ = resp2.Body.Close()
	}
	require.Error(t, err)
	assert.True(t, IsTransient(err))
	assert.Contains(t, err.Error(), "rate limit")
}

func TestSend_PerRequestUserAgent(t *testing.T) {
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{})
	resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL, UserAgent: "probe/9.9"})
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "probe/9.9", gotUA)
}

func TestSendCached_CachesGet(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("data"))
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{}, WithCache(NewInMemoryCache(100, time.Minute)))
	ctx := context.Background()
	req := Request{Target: "t", Method: http.MethodGet, URL: srv.URL}

	r1, err := c.SendCached(ctx, req, "k", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "data", string(r1.Body))

	r2, err := c.SendCached(ctx, req, "k", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "data", string(r2.Body))

	assert.Equal(t, int64(1), hits.Load(), "second call should be served from cache")
}

func TestSendWithRetry_RetriesTransient(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{
		RetryMaxAttempts: 3,
		RetryBase:        time.Millisecond,
		RetryCap:         5 * time.Millisecond,
		CBThreshold:      0.99, // don't let the breaker trip during the retry
		CBMinRequests:    100,
	})
	resp, err := c.SendWithRetry(context.Background(), Request{Target: "t", URL: srv.URL}, 0)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, "ok", readBody(t, resp))
	assert.Equal(t, int64(2), hits.Load(), "one failure then one success")
}

func TestSendWithRetry_FatalNotRetried(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{RetryMaxAttempts: 5, RetryBase: time.Millisecond, CBMinRequests: 100})
	resp, err := c.SendWithRetry(context.Background(), Request{Target: "t", URL: srv.URL}, 0)
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	assert.True(t, IsFatal(err))
	assert.Equal(t, int64(1), hits.Load(), "fatal errors are not retried")
}

func TestSendCoalesced_DedupsConcurrent(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		time.Sleep(100 * time.Millisecond) // hold the entry while followers queue
		_, _ = w.Write([]byte("shared"))
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{RateLimit: 1000})
	ctx := context.Background()
	req := Request{Target: "t", Method: http.MethodGet, URL: srv.URL}

	const n = 12
	var wg sync.WaitGroup
	results := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.SendCoalesced(ctx, req, "key", time.Minute)
			if assert.NoError(t, err) {
				results[i] = string(r.Body)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), hits.Load(), "concurrent callers should share one upstream fetch")
	for _, r := range results {
		assert.Equal(t, "shared", r)
	}
}

func TestSendWithFallback_ServesFallbackOnTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{CBMinRequests: 100},
		WithFallback("t", func() CachedResponse {
			return CachedResponse{Status: 200, Body: []byte("fallback")}
		}),
	)
	got, err := c.SendWithFallback(context.Background(), Request{Target: "t", Method: http.MethodGet, URL: srv.URL}, "k", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "fallback", string(got.Body))
}

func TestSendWithFallback_FatalBypassesFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{CBMinRequests: 100},
		WithFallback("t", func() CachedResponse { return CachedResponse{Status: 200, Body: []byte("fallback")} }),
	)
	_, err := c.SendWithFallback(context.Background(), Request{Target: "t", Method: http.MethodGet, URL: srv.URL}, "k", time.Minute)
	require.Error(t, err)
	assert.True(t, IsFatal(err))
}

func TestShutdown_NoInFlightAndRejects(t *testing.T) {
	c := clientFor(t, TargetConfig{})
	require.NoError(t, c.Shutdown(context.Background()))

	resp, err := c.Send(context.Background(), Request{Target: "t", URL: "http://127.0.0.1:1/"})
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	assert.True(t, IsTransient(err))
	assert.Contains(t, err.Error(), "shutting down")
}

func TestShutdown_TimesOutWithInFlight(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	c := clientFor(t, TargetConfig{})

	started := make(chan struct{})
	go func() {
		close(started)
		resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started
	require.Eventually(t, func() bool { return c.inFlight.Load() == 1 }, time.Second, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.Shutdown(ctx)
	require.Error(t, err)
	var se *ShutdownError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, 1, se.InFlight)
}

func TestSend_UnknownTargetGetsFallbackPolicy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	c, err := New(cfg)
	require.NoError(t, err)

	resp, err := c.Send(context.Background(), Request{Target: "never-declared", URL: srv.URL})
	require.NoError(t, err)
	_ = resp.Body.Close()
	// A policy was materialised on first use.
	_, ok := c.policies.Load("never-declared")
	assert.True(t, ok)
}

func TestMetrics_Exposed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := clientFor(t, TargetConfig{})
	resp, err := c.Send(context.Background(), Request{Target: "t", URL: srv.URL})
	require.NoError(t, err)
	_ = resp.Body.Close()

	rec := httptest.NewRecorder()
	c.Metrics().Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "http_outbound_requests_total")
	assert.Contains(t, body, "circuit_breaker_state")
}
