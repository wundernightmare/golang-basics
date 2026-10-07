package resilient_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	resilient "github.com/tracehubmmp/golang-basics/libs/resilient-http-client"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// target is DefaultTarget("api") with fast retries, then mut.
func target(mut func(*resilient.TargetConfig)) resilient.TargetConfig {
	tc := resilient.DefaultTarget("api")
	tc.RetryBaseDelay = time.Millisecond
	tc.RetryMaxDelay = 5 * time.Millisecond
	if mut != nil {
		mut(&tc)
	}
	return tc
}

// newClient builds a client over tc, shut down when the test ends.
func newClient(t *testing.T, tc resilient.TargetConfig, opts ...resilient.Option) *resilient.Client {
	t.Helper()
	cfg := resilient.DefaultConfig()
	cfg.Targets = []resilient.TargetConfig{tc}
	c, err := resilient.New(cfg, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		assert.NoError(t, c.Shutdown(ctx))
	})
	return c
}

// server is an httptest server counting the requests it served.
func server(t *testing.T, h http.HandlerFunc) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func get(t *testing.T, ctx context.Context, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	return req
}

func metric(t *testing.T, c *resilient.Client, name string, labels map[string]string) float64 {
	t.Helper()
	return testx.Metric(t, c.Registry(), name, labels)
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

func TestSend_SuccessTracesAndMeters(t *testing.T) {
	spans := testx.Recorder(t)
	var traceparent atomic.Value
	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		traceparent.Store(r.Header.Get("traceparent"))
		assert.Equal(t, "svc/1", r.UserAgent())
		_, _ = io.WriteString(w, "hello")
	})
	cfg := resilient.DefaultConfig()
	cfg.UserAgent = "svc/1"
	cfg.Targets = []resilient.TargetConfig{target(nil)}
	c, err := resilient.New(cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Shutdown(t.Context())) }()

	resp, err := c.Send("api", get(t, t.Context(), srv.URL+"/x"))
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "hello", string(body))

	assert.NotEmpty(t, traceparent.Load(), "traceparent is injected")
	ended := spans.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "GET api", ended[0].Name())

	assert.InDelta(t, 1, metric(t, c, "http_client_requests_total", map[string]string{"target": "api", "method": "GET", "outcome": "2xx"}), 0)
	assert.InDelta(t, 1, metric(t, c, "http_client_request_duration_seconds", map[string]string{"target": "api", "method": "GET"}), 0)
	assert.InDelta(t, 0, metric(t, c, "circuit_breaker_state", map[string]string{"target": "api"}), 0)
	testx.LintMetrics(t, c.Registry())
}

func TestSend_RejectsUnknownTargetAndBadURL(t *testing.T) {
	c := newClient(t, target(nil))
	_, err := c.Send("nope", get(t, t.Context(), "http://127.0.0.1:1/")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindInvalid)
	_, err = c.BreakerState("nope")
	require.ErrorIs(t, err, resilient.KindInvalid)

	req := get(t, t.Context(), "/relative")
	_, err = c.Send("api", req) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindInvalid)
	assert.InDelta(t, -1, metric(t, c, "http_client_requests_total", nil), 0, "caller bugs are not metered")
}

func TestNew_RejectsInvalidConfigAndDuplicateRegistration(t *testing.T) {
	_, err := resilient.New(resilient.Config{Targets: []resilient.TargetConfig{{Name: ""}}})
	require.Error(t, err)

	reg := prometheus.NewRegistry()
	c, err := resilient.New(resilient.DefaultConfig("a"), resilient.WithRegisterer(reg))
	require.NoError(t, err)
	assert.Nil(t, c.Registry(), "metrics went to the caller's registerer")
	assert.InDelta(t, 0, testx.Metric(t, reg, "circuit_breaker_state", map[string]string{"target": "a"}), 0)
	_, err = resilient.New(resilient.DefaultConfig("a"), resilient.WithRegisterer(reg))
	require.Error(t, err, "a second client on the same registerer collides")
}

func TestSend_StatusErrorKeepsBodyAndRetryAfter(t *testing.T) {
	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"slow down"}`)
	})
	c := newClient(t, target(nil))
	resp, err := c.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // always nil here
	require.Nil(t, resp)
	var oe *resilient.OutboundError
	require.ErrorAs(t, err, &oe)
	assert.Equal(t, resilient.KindStatus, oe.Kind)
	assert.Equal(t, 429, oe.StatusCode)
	assert.Equal(t, 7*time.Second, oe.RetryAfter)
	assert.JSONEq(t, `{"error":"slow down"}`, string(oe.Body))
	assert.True(t, resilient.Retryable(err))
	assert.InDelta(t, 1, metric(t, c, "http_client_requests_total", map[string]string{"outcome": "4xx"}), 0)
}

func TestSendWithRetry_RetriesTransientStatus(t *testing.T) {
	var n atomic.Int32
	srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	c := newClient(t, target(nil))
	resp, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, int32(3), hits.Load())
	assert.InDelta(t, 2, metric(t, c, "http_client_retries_total", map[string]string{"target": "api"}), 0)
	assert.InDelta(t, 2, metric(t, c, "http_client_requests_total", map[string]string{"outcome": "5xx"}), 0)
}

func TestSendWithRetry_GivesUpAfterMaxAttemptsAndOnNonRetryable(t *testing.T) {
	for _, tc := range []struct {
		code int
		hits int32
	}{
		{http.StatusBadGateway, 3},
		{http.StatusNotImplemented, 1},
		{http.StatusHTTPVersionNotSupported, 1},
		{http.StatusBadRequest, 1},
		{http.StatusRequestTimeout, 3},
		{http.StatusTooEarly, 3},
	} {
		t.Run(strconv.Itoa(tc.code), func(t *testing.T) {
			srv, hits := server(t, status(tc.code))
			c := newClient(t, target(nil))
			_, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // always an error
			var oe *resilient.OutboundError
			require.ErrorAs(t, err, &oe)
			assert.Equal(t, tc.code, oe.StatusCode)
			assert.Equal(t, tc.hits, hits.Load())
		})
	}
}

func TestSendWithRetry_HonoursRetryAfter(t *testing.T) {
	var first atomic.Int64
	var gap atomic.Int64
	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now().UnixNano()
		if first.CompareAndSwap(0, now) {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		gap.Store(now - first.Load())
	})
	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryMaxDelay = 2 * time.Second }))
	resp, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.GreaterOrEqual(t, time.Duration(gap.Load()), time.Second, "backoff was only ≤5ms; Retry-After is the floor")
}

func TestSendWithRetry_RetryAfterBeyondLimitsStops(t *testing.T) {
	srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryMaxDelay = time.Second }))
	_, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindStatus)
	assert.Equal(t, int32(1), hits.Load(), "3s > retry_max_delay: no retry")

	c2 := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryMaxDelay = 10 * time.Second }), resilient.WithRegisterer(prometheus.NewRegistry()))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	start := time.Now()
	_, err = c2.SendWithRetry("api", get(t, ctx, srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindStatus, "the last real error, not a cancellation")
	assert.Less(t, time.Since(start), 500*time.Millisecond, "no pointless wait past the caller's deadline")
	assert.Equal(t, int32(2), hits.Load())
}

func TestSendWithRetry_NonIdempotentNeedsKey(t *testing.T) {
	var bodies []string
	var mu sync.Mutex
	srv, hits := server(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c := newClient(t, target(nil))
	post := func(key string) *http.Request {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL, strings.NewReader("payload"))
		require.NoError(t, err)
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		return req
	}

	_, err := c.SendWithRetry("api", post("")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindStatus)
	assert.Equal(t, int32(1), hits.Load(), "POST without a key is sent once")

	_, err = c.SendWithRetry("api", post("k-1")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindStatus)
	assert.Equal(t, int32(4), hits.Load(), "with a key: 3 attempts")
	assert.Equal(t, []string{"payload", "payload", "payload", "payload"}, bodies, "the body is replayed via GetBody")

	req := post("k-2")
	req.GetBody = nil                    // an unreplayable body
	_, err = c.SendWithRetry("api", req) //nolint:bodyclose // error
	require.Error(t, err)
	assert.Equal(t, int32(5), hits.Load(), "no GetBody: sent once")
}

func TestSendWithRetry_BudgetBoundsAmplification(t *testing.T) {
	srv, hits := server(t, status(http.StatusServiceUnavailable))
	c := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.BreakerFailureRatio = 0
		tc.RetryMaxAttempts = 5
		tc.RetryBudgetRatio = 0.1
		tc.RetryBudgetMinRetries = 2
	}))
	for range 10 {
		_, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindStatus)
	}
	// 10 requests → 2 + 0.1×10 = 3 retries at most, instead of 40.
	assert.Equal(t, int32(13), hits.Load())
	assert.InDelta(t, 3, metric(t, c, "http_client_retries_total", nil), 0)
	assert.InDelta(t, 10, metric(t, c, "http_client_retry_budget_exhausted_total", nil), 0, "every request ended on a spent budget")
}

func TestSendWithRetry_StopsOnCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
		cancel()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c := newClient(t, target(nil))
	_, err := c.SendWithRetry("api", get(t, ctx, srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindCanceled)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(1), hits.Load())
}

func TestSend_TimeoutIsABreakerFailure(t *testing.T) {
	release := make(chan struct{})
	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	c := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.Timeout = 20 * time.Millisecond
		tc.BreakerMinRequests = 2
	}))
	for range 2 {
		_, err := c.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindTimeout)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	st, err := c.BreakerState("api")
	require.NoError(t, err)
	assert.Equal(t, resilient.StateOpen, st)
	_, err = c.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindCircuitOpen)

	assert.InDelta(t, 2, metric(t, c, "http_client_requests_total", map[string]string{"outcome": "timeout"}), 0)
	assert.InDelta(t, 1, metric(t, c, "http_client_requests_total", map[string]string{"outcome": "circuit_open"}), 0)
	assert.InDelta(t, 2, metric(t, c, "http_client_request_duration_seconds", nil), 0, "the rejected one never reached the network")
	assert.InDelta(t, 1, metric(t, c, "circuit_breaker_transitions_total", map[string]string{"from": "closed", "to": "open"}), 0)
	assert.InDelta(t, 1, metric(t, c, "circuit_breaker_state", nil), 0)
}

func TestSend_CallerCancellationDoesNotTripBreaker(t *testing.T) {
	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 }))
	for range 5 {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		_, err := c.Send("api", get(t, ctx, srv.URL)) //nolint:bodyclose // error
		cancel()
		require.ErrorIs(t, err, resilient.KindCanceled, "the caller's deadline is a cancellation, not a timeout")
	}
	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st)
	assert.InDelta(t, 5, metric(t, c, "http_client_requests_total", map[string]string{"outcome": "canceled"}), 0)
}

func TestSend_4xxDoesNotTripBreaker(t *testing.T) {
	srv, _ := server(t, status(http.StatusTooManyRequests))
	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 }))
	for range 5 {
		_, err := c.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindStatus)
	}
	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st)
}

func TestSend_ConnectionErrorTripsBreaker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close()) // nothing listens: connection refused

	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 }))
	_, err = c.Send("api", get(t, t.Context(), "http://"+addr)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindConnection)
	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateOpen, st)
}

func TestSend_HalfOpenAdmitsOneProbeUnderConcurrency(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	probeIn, probeGo := make(chan struct{}, 1), make(chan struct{})
	srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		probeIn <- struct{}{}
		<-probeGo // hold the probe until every other request was decided
	})
	c := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.BreakerMinRequests = 1
		tc.BreakerOpenTimeout = 50 * time.Millisecond
	}))
	_, err := c.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindStatus)
	failing.Store(false)
	time.Sleep(60 * time.Millisecond)

	const n = 32
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			resp, err := c.Send("api", get(t, t.Context(), srv.URL))
			if err == nil {
				_ = resp.Body.Close()
			}
			errs <- err
		})
	}
	<-probeIn
	rejected := 0
	for range n - 1 {
		err := <-errs
		require.ErrorIs(t, err, resilient.KindCircuitOpen)
		rejected++
	}
	close(probeGo)
	wg.Wait()
	require.NoError(t, <-errs, "the probe succeeded")
	assert.Equal(t, n-1, rejected)
	assert.Equal(t, int32(2), hits.Load(), "the tripping request and exactly one probe")
	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st)
	assert.InDelta(t, 1, metric(t, c, "circuit_breaker_transitions_total", map[string]string{"from": "half_open", "to": "closed"}), 0)
}

func TestSend_RateLimitWaitsWithinAttemptTimeout(t *testing.T) {
	srv, hits := server(t, status(http.StatusOK))
	c := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.RateLimit = 20 // a token every 50ms
		tc.RateBurst = 1
		tc.Timeout = 200 * time.Millisecond
	}))
	for range 2 { // the second waits ~50ms for its token
		resp, err := c.Send("api", get(t, t.Context(), srv.URL))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}

	c2 := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.RateLimit = 0.5
		tc.Timeout = 100 * time.Millisecond
		tc.BreakerMinRequests = 1
	}), resilient.WithRegisterer(prometheus.NewRegistry()))
	resp, err := c2.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	start := time.Now()
	_, err = c2.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindRateLimited, "the next token is 2s away, past the attempt timeout")
	assert.Less(t, time.Since(start), 90*time.Millisecond, "fails fast instead of sleeping into the deadline")
	assert.Equal(t, int32(3), hits.Load())
	st, _ := c2.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st, "a local rejection is not a failure")
}

func TestSend_BulkheadCapsConcurrency(t *testing.T) {
	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") })
	c := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.MaxConcurrent = 1
		tc.MaxConcurrentWait = 20 * time.Millisecond
	}))
	held, err := c.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	assert.InDelta(t, 1, metric(t, c, "http_client_bulkhead_in_flight", nil), 0, "the slot is held until the body is closed")

	start := time.Now()
	_, err = c.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindBulkheadFull)
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond, "it waited max_concurrent_wait")
	assert.InDelta(t, 1, metric(t, c, "http_client_bulkhead_rejected_total", nil), 0)

	require.NoError(t, held.Body.Close())
	require.NoError(t, held.Body.Close(), "a second Close does not release twice")
	assert.InDelta(t, 0, metric(t, c, "http_client_bulkhead_in_flight", nil), 0)
	resp, err := c.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func TestSend_Redirects(t *testing.T) {
	other, otherHits := server(t, status(http.StatusOK))
	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/away":
			http.Redirect(w, r, other.URL+"/", http.StatusFound)
		case "/loop":
			http.Redirect(w, r, "/loop", http.StatusFound)
		case "/hop":
			http.Redirect(w, r, "/done", http.StatusFound)
		default:
			_, _ = io.WriteString(w, "done")
		}
	})

	t.Run("same host is followed", func(t *testing.T) {
		c := newClient(t, target(nil))
		resp, err := c.Send("api", get(t, t.Context(), srv.URL+"/hop"))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, "/done", resp.Request.URL.Path)
	})
	t.Run("cross host is refused", func(t *testing.T) {
		c := newClient(t, target(nil))
		_, err := c.Send("api", get(t, t.Context(), srv.URL+"/away")) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindRedirect)
		assert.Contains(t, err.Error(), "cross-host")
		assert.Equal(t, int32(0), otherHits.Load())
		st, _ := c.BreakerState("api")
		assert.Equal(t, resilient.StateClosed, st)
	})
	t.Run("too many", func(t *testing.T) {
		c := newClient(t, target(nil))
		_, err := c.Send("api", get(t, t.Context(), srv.URL+"/loop")) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindRedirect)
		assert.Contains(t, err.Error(), "10 redirects")
	})
	t.Run("zero means return the 3xx", func(t *testing.T) {
		cfg := resilient.DefaultConfig()
		cfg.MaxRedirects = 0
		cfg.Targets = []resilient.TargetConfig{target(nil)}
		c, err := resilient.New(cfg)
		require.NoError(t, err)
		resp, err := c.Send("api", get(t, t.Context(), srv.URL+"/hop"))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusFound, resp.StatusCode)
		require.NoError(t, c.Shutdown(t.Context()))
	})
	t.Run("cross host allowed when configured", func(t *testing.T) {
		cfg := resilient.DefaultConfig()
		cfg.AllowCrossHostRedirects = true
		cfg.Targets = []resilient.TargetConfig{target(nil)}
		c, err := resilient.New(cfg)
		require.NoError(t, err)
		resp, err := c.Send("api", get(t, t.Context(), srv.URL+"/away"))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, int32(1), otherHits.Load())
		require.NoError(t, c.Shutdown(t.Context()))
	})
}

func TestWithHTTPClient_DoesNotMutateCallersClient(t *testing.T) {
	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/r" {
			http.Redirect(w, r, "/", http.StatusFound)
		}
	})
	var callerRedirects atomic.Int32
	rt := http.DefaultTransport.(*http.Transport).Clone()
	defer rt.CloseIdleConnections()
	mine := &http.Client{Transport: rt, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error {
		callerRedirects.Add(1)
		return nil
	}}
	c := newClient(t, target(nil), resilient.WithHTTPClient(mine))

	assert.Same(t, rt, mine.Transport)
	assert.Equal(t, time.Minute, mine.Timeout)
	resp, err := c.Send("api", get(t, t.Context(), srv.URL+"/r"))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, int32(1), callerRedirects.Load(), "the caller's CheckRedirect still runs after the policy")
}

func TestSend_ReusesConnectionsAfterDrain(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/500":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, strings.Repeat("e", 10_000))
		case "/404":
			w.WriteHeader(http.StatusNotFound)
		default:
			_, _ = io.WriteString(w, strings.Repeat("o", 10_000))
		}
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.BreakerFailureRatio = 0 }))
	for _, path := range []string{"/ok", "/500", "/404", "/ok", "/500"} {
		resp, err := c.Send("api", get(t, t.Context(), srv.URL+path))
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			require.NoError(t, resp.Body.Close())
		}
	}
	assert.Equal(t, int32(1), conns.Load(), "error bodies are drained, so one keep-alive connection serves all")
}

func TestSend_LogsWithoutQueryString(t *testing.T) {
	buf := &testx.LogBuffer{}
	srv, _ := server(t, status(http.StatusBadGateway))
	c := newClient(t, target(nil), resilient.WithLogger(slog.New(slog.NewJSONHandler(buf, nil))))
	_, err := c.Send("api", get(t, t.Context(), srv.URL+"/v1/items?token=s3cr3t")) //nolint:bodyclose // error
	require.Error(t, err)

	line := buf.Find(t, map[string]any{"level": "WARN", "target": "api"})
	require.NotNil(t, line, buf.String())
	assert.Equal(t, srv.URL+"/v1/items", line["url"])
	assert.NotContains(t, buf.String(), "s3cr3t")
}

func TestShutdown_WaitsForInFlightThenRejects(t *testing.T) {
	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") })
	c := newClient(t, target(nil))

	open, err := c.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)

	short, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err = c.Shutdown(short)
	require.ErrorIs(t, err, context.DeadlineExceeded, "an unclosed body is still in flight")
	assert.Contains(t, err.Error(), "1 requests in flight")

	_, err = c.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindShutdown)
	assert.InDelta(t, 1, metric(t, c, "http_client_requests_total", map[string]string{"outcome": "shutdown"}), 0)

	done := make(chan error, 1)
	go func() { done <- c.Shutdown(t.Context()) }()
	select {
	case <-done:
		t.Fatal("Shutdown returned with a request in flight")
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, open.Body.Close())
	require.NoError(t, <-done)
}

func TestShutdown_RaceNoRequestEscapes(t *testing.T) {
	for range 20 {
		srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(time.Millisecond)
			_, _ = io.WriteString(w, "x")
		})
		c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryBaseDelay = 0; tc.RetryMaxDelay = 0 }))

		var start, wg sync.WaitGroup
		start.Add(1)
		var ok, refused atomic.Int32
		for range 16 {
			wg.Go(func() {
				start.Wait()
				resp, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL))
				if err != nil {
					if !errors.Is(err, resilient.KindShutdown) {
						t.Errorf("want a shutdown rejection, got %v", err)
					}
					refused.Add(1)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				ok.Add(1)
			})
		}
		start.Done()
		require.NoError(t, c.Shutdown(t.Context()))
		afterShutdown := hits.Load()
		wg.Wait()

		assert.Equal(t, afterShutdown, hits.Load(), "no request reached the server after Shutdown returned")
		assert.Equal(t, ok.Load(), hits.Load(), "every admitted request completed before Shutdown returned")
		assert.Equal(t, int32(16), ok.Load()+refused.Load())
	}
}
