package resilient_test

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/goleak"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	require.NoError(t, err)

	return req
}

func metric(t *testing.T, c *resilient.Client, name string, labels map[string]string) float64 {
	t.Helper()
	return testx.Metric(t, c.Registry(), name, labels)
}

func outbound(t *testing.T, err error) *resilient.OutboundError {
	t.Helper()

	oe, ok := errors.AsType[*resilient.OutboundError](err)
	require.True(t, ok, "%v", err)

	return oe
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

const scriptedURL = "http://scripted.test"

// scripted answers by the request path, with no network: /panic panics, /cut
// is a 200 whose body fails, /<code> is that status. A test in a synctest
// bubble uses it to move the breaker's and the limiters' clock itself.
func scripted() resilient.Option {
	return resilient.WithHTTPClient(&http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: r}

		switch r.URL.Path {
		case "/panic":
			panic("the transport blew up")
		case "/cut":
			resp.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
		default:
			resp.StatusCode, _ = strconv.Atoi(r.URL.Path[1:])
		}

		return resp, nil
	})})
}

func TestSend_SuccessTracesAndMeters(t *testing.T) {
	t.Parallel()

	spans := testx.Recorder(t)

	var traceparent, userAgent atomic.Value

	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		traceparent.Store(r.Header.Get("Traceparent"))
		userAgent.Store(r.UserAgent())

		_, _ = io.WriteString(w, "hello")
	})
	cfg := resilient.DefaultConfig()
	cfg.UserAgent = "svc/1"
	cfg.Targets = []resilient.TargetConfig{target(nil)}
	client, err := resilient.New(cfg,
		resilient.WithTracerProvider(spans.Provider), resilient.WithPropagator(spans.Propagator))

	require.NoError(t, err)
	defer func() { require.NoError(t, client.Shutdown(t.Context())) }()

	req := get(t, t.Context(), srv.URL+"/x?token=s3cr3t")
	req.URL.User = url.UserPassword("svc", "s3cr3t")

	resp, err := client.Send("api", req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "hello", string(body))

	assert.NotEmpty(t, traceparent.Load(), "traceparent is injected")
	assert.Equal(t, "svc/1", userAgent.Load())

	ended := spans.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "GET api", ended[0].Name())
	assert.Contains(t, ended[0].Attributes(), attribute.String("url.full", srv.URL+"/x"), "no query string, no user info")

	own := get(t, t.Context(), srv.URL)
	own.Header.Set("User-Agent", "caller/2")
	resp, err = client.Send("api", own)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "caller/2", userAgent.Load(), "the request's own User-Agent wins")

	assert.InDelta(t, 2, metric(t, client, "http_client_requests_total", map[string]string{"target": "api", "method": "GET", "outcome": "2xx"}), 0)
	assert.InDelta(t, 2, metric(t, client, "http_client_request_duration_seconds", map[string]string{"target": "api", "method": "GET"}), 0)
	assert.InDelta(t, 0, metric(t, client, "http_client_circuit_breaker_state", map[string]string{"target": "api"}), 0)
	testx.LintMetrics(t, client.Registry())
}

func TestSend_RejectsUnknownTargetAndBadURL(t *testing.T) {
	t.Parallel()

	c := newClient(t, target(nil))
	_, err := c.Send("nope", get(t, t.Context(), "http://127.0.0.1:1/"))
	require.ErrorIs(t, err, resilient.KindInvalid)
	_, err = c.BreakerState("nope")
	require.ErrorIs(t, err, resilient.KindInvalid)

	req := get(t, t.Context(), "/relative")
	_, err = c.Send("api", req)
	require.ErrorIs(t, err, resilient.KindInvalid)
	assert.InDelta(t, 1, metric(t, c, "http_client_requests_total", map[string]string{"target": "api", "outcome": "invalid"}), 0,
		"a bad request to a known target is counted")
	assert.InDelta(t, -1, metric(t, c, "http_client_requests_total", map[string]string{"target": "nope"}), 0,
		"an unknown target name never becomes a label value")

	for _, url := range []string{"ftp://example.com/file", "http:///no-host", "//example.com/no-scheme"} {
		_, err = c.Send("api", get(t, t.Context(), url))
		require.ErrorIs(t, err, resilient.KindInvalid, url)
	}
}

func TestSend_HTTPS(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "secure") }))
	t.Cleanup(srv.Close)

	client := newClient(t, target(nil), resilient.WithHTTPClient(srv.Client()))
	resp, err := client.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "secure", string(body))
	assert.NotNil(t, resp.TLS)
}

// "Zero means disabled": a target that sets only its name has no timeout, no
// limits, no breaker and no retries — and still works.
func TestSend_ZeroTargetConfigDisablesEverything(t *testing.T) {
	t.Parallel()

	srv, hits := server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/down" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	client := newClient(t, resilient.TargetConfig{Name: "api"})

	resp, err := client.SendWithRetry("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err, "timeout 0 is no timeout, not an expired one")
	require.NoError(t, resp.Body.Close())

	for range 5 {
		_, err = client.SendWithRetry("api", get(t, t.Context(), srv.URL+"/down"))
		require.ErrorIs(t, err, resilient.KindStatus)
	}

	assert.Equal(t, int32(6), hits.Load(), "no retries, and no breaker to stop the calls")
	assert.InDelta(t, -1, metric(t, client, "http_client_bulkhead_in_flight", nil), 0, "no bulkhead, no gauge")
}

func TestNew_BulkheadGaugeStartsAtZero(t *testing.T) {
	t.Parallel()

	client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.MaxConcurrent = 2 }))
	assert.InDelta(t, 0, metric(t, client, "http_client_bulkhead_in_flight", map[string]string{"target": "api"}), 0,
		"the series exists before the first request")
}

func TestNew_RejectsInvalidConfigAndDuplicateRegistration(t *testing.T) {
	t.Parallel()

	_, err := resilient.New(resilient.Config{Targets: []resilient.TargetConfig{{Name: ""}}})
	require.Error(t, err)

	_, err = resilient.New(resilient.DefaultConfig())
	require.ErrorContains(t, err, "at least one", "a client with no target can send nothing")

	reg := prometheus.NewRegistry()
	c, err := resilient.New(resilient.DefaultConfig("a"), resilient.WithRegisterer(reg))
	require.NoError(t, err)
	assert.Nil(t, c.Registry(), "metrics went to the caller's registerer")
	assert.InDelta(t, 0, testx.Metric(t, reg, "http_client_circuit_breaker_state", map[string]string{"target": "a"}), 0)
	_, err = resilient.New(resilient.DefaultConfig("a"), resilient.WithRegisterer(reg))
	require.Error(t, err, "a second client on the same registerer collides")

	// Each of two clients on one registry gets a label of the caller's: one
	// metric name must carry one set of label names, so both are wrapped.
	shared := prometheus.NewRegistry()
	for _, name := range []string{"first", "second"} {
		_, err = resilient.New(resilient.DefaultConfig("a"),
			resilient.WithRegisterer(prometheus.WrapRegistererWith(prometheus.Labels{"client": name}, shared)))
		require.NoError(t, err, name)
		assert.InDelta(t, 0, testx.Metric(t, shared, "http_client_circuit_breaker_state", map[string]string{"target": "a", "client": name}), 0)
	}
}

// New builds a transport of its own: http.DefaultTransport is a variable, and
// a process that replaced it with a wrapper must still get a client. Not
// parallel: it swaps that variable.
func TestNew_DoesNotReadTheDefaultTransport(t *testing.T) { //nolint:paralleltest // swaps http.DefaultTransport
	std := http.DefaultTransport
	http.DefaultTransport = roundTrip(std.RoundTrip)

	defer func() { http.DefaultTransport = std }()

	client, err := resilient.New(resilient.DefaultConfig("api"))
	require.NoError(t, err)
	require.NoError(t, client.Shutdown(t.Context()))
}

// A target's ca_file is its whole trust store, on the client's own transport.
func TestSend_ATargetsCAFileIsItsTrustStore(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(status(http.StatusNoContent))
	t.Cleanup(srv.Close)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))

	cfg := resilient.DefaultConfig("system", "pinned")
	cfg.Targets[1].CAFile = caFile

	_, err := resilient.New(cfg, resilient.WithHTTPClient(srv.Client()))
	require.ErrorContains(t, err, "WithHTTPClient", "the caller's client has a trust store of its own")

	c, err := resilient.New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, c.Shutdown(context.WithoutCancel(t.Context()))) })

	resp, err := c.Send("pinned", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	_, err = c.Send("system", get(t, t.Context(), srv.URL))
	require.ErrorIs(t, err, resilient.KindConnection, "the file is one target's trust, not the client's")
}

// rate_burst 0 derives the burst from the rate; a rate beyond the range of
// int must still give a bucket that lets requests through.
func TestSend_AHugeRateLimitStillAdmits(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, status(http.StatusNoContent))
	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RateLimit = 1e300 }))

	resp, err := c.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func TestSend_StatusErrorKeepsBodyAndRetryAfter(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"slow down"}`)
	})
	c := newClient(t, target(nil))
	resp, err := c.Send("api", get(t, t.Context(), srv.URL)) // always nil here
	require.Nil(t, resp)

	var oe *resilient.OutboundError
	require.ErrorAs(t, err, &oe)
	assert.Equal(t, resilient.KindStatus, oe.Kind)
	assert.Equal(t, 429, oe.StatusCode)
	assert.Equal(t, 7*time.Second, oe.RetryAfter)
	assert.JSONEq(t, `{"error":"slow down"}`, string(oe.Body))
	assert.Equal(t, "7", oe.Header.Get("Retry-After"), "the response header is kept")
	assert.True(t, resilient.Retryable(err))
	assert.InDelta(t, 1, metric(t, c, "http_client_requests_total", map[string]string{"outcome": "4xx"}), 0)
}

func TestSendWithRetry_RetriesTransientStatus(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
			t.Parallel()

			srv, hits := server(t, status(tc.code))
			c := newClient(t, target(nil))
			_, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL))

			var oe *resilient.OutboundError
			require.ErrorAs(t, err, &oe)
			assert.Equal(t, tc.code, oe.StatusCode)
			assert.Equal(t, tc.hits, hits.Load())
		})
	}
}

func TestSendWithRetry_HonoursRetryAfter(t *testing.T) {
	t.Parallel()

	var (
		first atomic.Int64
		gap   atomic.Int64
	)

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now().UnixNano()
		if first.CompareAndSwap(0, now) {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		gap.Store(now - first.Load())
	})
	// Retry-After equals retry_max_delay: the limit is inclusive.
	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryMaxDelay = time.Second }))
	resp, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.GreaterOrEqual(t, time.Duration(gap.Load()), time.Second, "backoff was only ≤5ms; Retry-After is the floor")
}

func TestSendWithRetry_RetryAfterBeyondLimitsStops(t *testing.T) {
	t.Parallel()

	srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryMaxDelay = time.Second }))
	_, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL))
	require.ErrorIs(t, err, resilient.KindStatus)
	assert.Equal(t, int32(1), hits.Load(), "3s > retry_max_delay: no retry")

	c2 := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryMaxDelay = 10 * time.Second }), resilient.WithRegisterer(prometheus.NewRegistry()))

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	start := time.Now()
	_, err = c2.SendWithRetry("api", get(t, ctx, srv.URL))
	require.ErrorIs(t, err, resilient.KindStatus, "the last real error, not a cancellation")
	assert.Less(t, time.Since(start), 500*time.Millisecond, "no pointless wait past the caller's deadline")
	assert.Equal(t, int32(2), hits.Load())
}

func TestSendWithRetry_NonIdempotentNeedsKey(t *testing.T) {
	t.Parallel()

	var (
		bodies []string
		mu     sync.Mutex
	)

	srv, hits := server(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)

		mu.Lock()

		bodies = append(bodies, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	client := newClient(t, target(nil))
	post := func(key string) *http.Request {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL, strings.NewReader("payload"))
		require.NoError(t, err)

		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}

		return req
	}

	_, err := client.SendWithRetry("api", post(""))
	require.ErrorIs(t, err, resilient.KindStatus)
	assert.Equal(t, int32(1), hits.Load(), "POST without a key is sent once")

	_, err = client.SendWithRetry("api", post("k-1"))
	require.ErrorIs(t, err, resilient.KindStatus)
	assert.Equal(t, int32(4), hits.Load(), "with a key: 3 attempts")
	assert.False(t, outbound(t, err).RetryBudgetExhausted, "the attempts ran out, not the budget")
	assert.Equal(t, []string{"payload", "payload", "payload", "payload"}, bodies, "the body is replayed via GetBody")

	req := post("k-2")
	req.GetBody = nil // an unreplayable body
	_, err = client.SendWithRetry("api", req)
	require.Error(t, err)
	assert.Equal(t, int32(5), hits.Load(), "no GetBody: sent once")
}

func TestSendWithRetry_BudgetBoundsAmplification(t *testing.T) {
	t.Parallel()

	srv, hits := server(t, status(http.StatusServiceUnavailable))

	c := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.Breaker.FailureRatio = 0
		tc.RetryMaxAttempts = 5
		tc.RetryBudgetRatio = 0.1
		tc.RetryBudgetMinRetries = 2
	}))
	for range 10 {
		_, err := c.SendWithRetry("api", get(t, t.Context(), srv.URL))
		require.ErrorIs(t, err, resilient.KindStatus)
		assert.True(t, outbound(t, err).RetryBudgetExhausted)
	}
	// 10 requests → 2 + 0.1×10 = 3 retries at most, instead of 40.
	assert.Equal(t, int32(13), hits.Load())
	assert.InDelta(t, 3, metric(t, c, "http_client_retries_total", nil), 0)
	assert.InDelta(t, 10, metric(t, c, "http_client_retry_budget_exhausted_total", nil), 0, "every request ended on a spent budget")
}

func TestSendWithRetry_StopsOnCallerCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
		cancel()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c := newClient(t, target(nil))
	_, err := c.SendWithRetry("api", get(t, ctx, srv.URL))
	require.ErrorIs(t, err, resilient.KindCanceled)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(1), hits.Load())
}

func TestSend_TimeoutIsABreakerFailure(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})

	defer close(release)

	client := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.Timeout = 20 * time.Millisecond
		tc.Breaker.MinRequests = 2
	}))
	for range 2 {
		_, err := client.Send("api", get(t, t.Context(), srv.URL))
		require.ErrorIs(t, err, resilient.KindTimeout)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}

	st, err := client.BreakerState("api")
	require.NoError(t, err)
	assert.Equal(t, resilient.StateOpen, st)
	_, err = client.Send("api", get(t, t.Context(), srv.URL))
	require.ErrorIs(t, err, resilient.KindCircuitOpen)
	require.ErrorIs(t, err, resilience.ErrOpen, "the sentinel every breaker-backed client shares")

	assert.InDelta(t, 2, metric(t, client, "http_client_requests_total", map[string]string{"outcome": "timeout"}), 0)
	assert.InDelta(t, 1, metric(t, client, "http_client_requests_total", map[string]string{"outcome": "circuit_open"}), 0)
	assert.InDelta(t, 2, metric(t, client, "http_client_request_duration_seconds", nil), 0, "the rejected one never reached the network")
	assert.InDelta(t, 1, metric(t, client, "http_client_circuit_breaker_transitions_total", map[string]string{"from": "closed", "to": "open"}), 0)
	assert.InDelta(t, 1, metric(t, client, "http_client_circuit_breaker_state", nil), 0)
}

func TestSend_CallerCancellationDoesNotTripBreaker(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 5)
	srv, _ := server(t, func(_ http.ResponseWriter, r *http.Request) {
		started <- struct{}{}

		<-r.Context().Done()
	})

	buf := &testx.LogBuffer{}
	client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests = 1 }),
		resilient.WithLogger(slog.New(slog.NewJSONHandler(buf, nil))))

	for range 5 {
		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			<-started
			cancel()
		}()

		_, err := client.Send("api", get(t, ctx, srv.URL))
		require.ErrorIs(t, err, resilient.KindCanceled)
		require.ErrorIs(t, err, context.Canceled)
	}

	st, _ := client.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st, "the caller walking away says nothing about the dependency")
	assert.InDelta(t, 5, metric(t, client, "http_client_requests_total", map[string]string{"outcome": "canceled"}), 0)
	assert.Empty(t, buf.String(), "and is not worth a warning")
}

// A caller whose own deadline is shorter than the target's timeout must not
// hide a hung dependency from the breaker.
func TestSend_CallerDeadlineIsABreakerFailure(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() })

	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests = 2 }))
	for range 2 {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		_, err := c.Send("api", get(t, ctx, srv.URL))

		cancel()
		require.ErrorIs(t, err, resilient.KindCanceled, "the caller's deadline is still reported as a cancellation, not a timeout")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, resilient.Retryable(err))
	}

	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateOpen, st, "the dependency did not answer within the caller's deadline")
	assert.InDelta(t, 2, metric(t, c, "http_client_requests_total", map[string]string{"outcome": "canceled"}), 0)
}

// hung is the base URL of the server [hangAfterHead] serves in memory.
const hung = "http://hung.test"

// pipes is a listener whose connections are net.Pipe pairs: no socket, so
// every goroutine on either side blocks durably and a synctest bubble can
// tell when to move its clock.
type pipes struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (p *pipes) Accept() (net.Conn, error) {
	select {
	case conn := <-p.conns:
		return conn, nil
	case <-p.done:
		return nil, net.ErrClosed
	}
}

func (p *pipes) Close() error {
	p.once.Do(func() { close(p.done) })

	return nil
}

func (*pipes) Addr() net.Addr { return &net.UnixAddr{Name: "pipe", Net: "pipe"} }

func (p *pipes) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()

	select {
	case p.conns <- server:
		return client, nil
	case <-p.done:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// hangAfterHead returns an http.Client whose every request is answered 200
// with the first bytes of the body flushed (path /hang: with nothing at all)
// and then hangs until the test ends. It is served in memory, for a test in
// a synctest bubble: an attempt timeout there is virtual time, so "the
// headers arrive, then the timeout fires" does not depend on how busy the
// machine is. Call it inside the bubble.
func hangAfterHead(t *testing.T) *http.Client {
	t.Helper()

	listener := &pipes{conns: make(chan net.Conn), done: make(chan struct{})}
	srv := &http.Server{
		ReadHeaderTimeout: time.Minute,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/hang" {
				_, _ = io.WriteString(w, "head")
				_ = http.NewResponseController(w).Flush()
			}

			<-r.Context().Done()
		}),
	}

	served := make(chan struct{})

	go func() {
		defer close(served)

		_ = srv.Serve(listener)
	}()

	transport := &http.Transport{DialContext: listener.dial}

	t.Cleanup(func() {
		transport.CloseIdleConnections()

		_ = srv.Close()

		<-served
	})

	return &http.Client{Transport: transport}
}

func TestSend_BodyTimeoutIsATypedBreakerFailure(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		client := newClient(t, target(func(tc *resilient.TargetConfig) {
			tc.Timeout = 30 * time.Millisecond
			tc.Breaker.MinRequests = 2
		}), resilient.WithHTTPClient(hangAfterHead(t)))

		for range 2 {
			resp, err := client.SendWithRetry("api", get(t, t.Context(), hung))
			require.NoError(t, err, "the headers arrived in time")

			_, err = io.ReadAll(resp.Body)
			require.NoError(t, resp.Body.Close())

			var oe *resilient.OutboundError
			require.ErrorAs(t, err, &oe, "a body read failure is an OutboundError too")
			assert.Equal(t, resilient.KindTimeout, oe.Kind)
			assert.Equal(t, "api", oe.Target)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}

		st, _ := client.BreakerState("api")
		assert.Equal(t, resilient.StateOpen, st, "a dependency that hangs mid-body is failing")
	})
}

func TestSend_BodyCutShortIsAConnectionFailure(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = io.WriteString(w, "head")

		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	})
	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests = 1 }))

	resp, err := c.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)

	_, err = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.ErrorIs(t, err, resilient.KindConnection)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateOpen, st)
}

// The breaker hears about an attempt when its timeout fires, even from a
// caller that sits on the response without reading or closing it.
func TestSend_UnreadBodyReportsToBreakerAtAttemptTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const timeout = 30 * time.Millisecond

		client := newClient(t, target(func(tc *resilient.TargetConfig) {
			tc.Timeout = timeout
			tc.Breaker.MinRequests = 2
		}), resilient.WithHTTPClient(hangAfterHead(t)))

		for range 2 {
			resp, err := client.Send("api", get(t, t.Context(), hung))
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })
		}

		st, _ := client.BreakerState("api")
		require.Equal(t, resilient.StateClosed, st, "nothing is reported before the timeout")

		time.Sleep(timeout)
		synctest.Wait()

		st, _ = client.BreakerState("api")
		assert.Equal(t, resilient.StateOpen, st)
	})
}

// A body read to the end, or closed early by the caller, is a success.
func TestSend_BodyReadOrClosedEarlyIsABreakerSuccess(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, strings.Repeat("x", 1<<16)) })
	client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests = 1 }))

	for i := range 4 {
		resp, err := client.Send("api", get(t, t.Context(), srv.URL))
		require.NoError(t, err)

		if i%2 == 0 {
			_, err = io.Copy(io.Discard, resp.Body)
			require.NoError(t, err)
		}

		require.NoError(t, resp.Body.Close())

		_, err = resp.Body.Read(make([]byte, 1))
		require.Error(t, err, "reading a closed body fails")
	}

	st, _ := client.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st)
}

// A 101 response's body is the connection: it stays writable, and once the
// protocol has switched the attempt is over — the target's timeout no longer
// applies to it and the breaker has heard a success.
func TestSend_UpgradedConnectionOutlivesTheAttemptTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 50 * time.Millisecond

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
		_ = rw.Flush()

		line, _ := rw.ReadString('\n')
		_, _ = rw.WriteString(line)
		_ = rw.Flush()
	})
	client := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.Timeout = timeout
		tc.Breaker.MinRequests = 1 // one failure would open it
	}))

	req := get(t, t.Context(), srv.URL)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "echo")

	resp, err := client.Send("api", req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	rw, ok := resp.Body.(io.ReadWriter)
	require.True(t, ok, "the upgraded body must be an io.Writer")

	time.Sleep(3 * timeout)

	_, err = io.WriteString(rw, "ping\n")
	require.NoError(t, err)

	echo := make([]byte, 5)
	_, err = io.ReadFull(rw, echo)
	require.NoError(t, err, "the connection is still there after the target's timeout")
	assert.Equal(t, "ping\n", string(echo))

	st, _ := client.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st)
}

func TestSend_4xxDoesNotTripBreaker(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		code, _ := strconv.Atoi(r.URL.Path[1:])
		w.WriteHeader(code)
	})

	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests = 1 }))
	for _, path := range []string{"/400", "/404", "/429", "/499"} {
		_, err := c.Send("api", get(t, t.Context(), srv.URL+path))
		require.ErrorIs(t, err, resilient.KindStatus)
	}

	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st)
}

func TestSend_ConnectionErrorTripsBreaker(t *testing.T) {
	t.Parallel()

	// A listener that hangs up on every connection. It keeps its port for the
	// whole test: a port that was merely freed could be taken by a parallel
	// test's server.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			_ = conn.Close()
		}
	}()

	addr := ln.Addr().String()

	c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests = 1 }))
	_, err = c.Send("api", get(t, t.Context(), "http://"+addr))
	require.ErrorIs(t, err, resilient.KindConnection)

	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateOpen, st)
}

func TestSend_HalfOpenAdmitsOneProbeUnderConcurrency(t *testing.T) {
	t.Parallel()

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
	client := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.Breaker.MinRequests = 1
		tc.Breaker.OpenTimeout = 50 * time.Millisecond
	}))
	_, err := client.Send("api", get(t, t.Context(), srv.URL))
	require.ErrorIs(t, err, resilient.KindStatus)
	failing.Store(false)
	time.Sleep(60 * time.Millisecond)

	const n = 32

	errs := make(chan error, n)

	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			resp, err := client.Send("api", get(t, t.Context(), srv.URL))
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

	st, _ := client.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st)
	assert.InDelta(t, 1, metric(t, client, "http_client_circuit_breaker_transitions_total", map[string]string{"from": "half_open", "to": "closed"}), 0)
}

func TestSend_RateLimitWaitsWithinAttemptTimeout(t *testing.T) {
	t.Parallel()

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
		tc.Breaker.MinRequests = 1
	}), resilient.WithRegisterer(prometheus.NewRegistry()))
	resp, err := c2.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	start := time.Now()
	_, err = c2.Send("api", get(t, t.Context(), srv.URL))
	require.ErrorIs(t, err, resilient.KindRateLimited, "the next token is 2s away, past the attempt timeout")
	assert.Less(t, time.Since(start), 90*time.Millisecond, "fails fast instead of sleeping into the deadline")
	assert.Equal(t, int32(3), hits.Load())

	st, _ := c2.BreakerState("api")
	assert.Equal(t, resilient.StateClosed, st, "a local rejection is not a failure")
}

func TestSend_BulkheadCapsConcurrency(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") })
	client := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.MaxConcurrent = 1
		tc.MaxConcurrentWait = 20 * time.Millisecond
	}))
	held, err := client.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	assert.InDelta(t, 1, metric(t, client, "http_client_bulkhead_in_flight", nil), 0, "the slot is held until the body is closed")

	start := time.Now()
	_, err = client.Send("api", get(t, t.Context(), srv.URL))
	require.ErrorIs(t, err, resilient.KindBulkheadFull)
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond, "it waited max_concurrent_wait")
	assert.InDelta(t, 1, metric(t, client, "http_client_bulkhead_rejected_total", nil), 0)

	require.NoError(t, held.Body.Close())
	require.NoError(t, held.Body.Close(), "a second Close does not release twice")
	assert.InDelta(t, 0, metric(t, client, "http_client_bulkhead_in_flight", nil), 0)
	resp, err := client.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func TestSend_Redirects(t *testing.T) {
	t.Parallel()

	// Each sub-test gets its own pair of servers: they run in parallel, and
	// two of them assert how often the other host was hit.
	servers := func(t *testing.T) (*httptest.Server, *atomic.Int32) {
		t.Helper()

		other, otherHits := server(t, status(http.StatusOK))
		srv, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/away":
				http.Redirect(w, r, other.URL+"/", http.StatusFound)
			case "/hop":
				http.Redirect(w, r, "/done", http.StatusFound)
			default:
				_, _ = io.WriteString(w, "done")
			}
		})

		return srv, otherHits
	}

	t.Run("same host is followed", func(t *testing.T) {
		t.Parallel()

		srv, _ := servers(t)

		c := newClient(t, target(nil))
		resp, err := c.Send("api", get(t, t.Context(), srv.URL+"/hop"))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, "/done", resp.Request.URL.Path)
	})
	t.Run("cross host is refused", func(t *testing.T) {
		t.Parallel()

		srv, otherHits := servers(t)

		c := newClient(t, target(nil))
		_, err := c.Send("api", get(t, t.Context(), srv.URL+"/away"))
		require.ErrorIs(t, err, resilient.KindRedirect)
		assert.Contains(t, err.Error(), "cross-host")
		assert.Equal(t, int32(0), otherHits.Load())

		st, _ := c.BreakerState("api")
		assert.Equal(t, resilient.StateClosed, st)
	})
	t.Run("too many", func(t *testing.T) {
		t.Parallel()

		srv, hits := server(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/loop", http.StatusFound) })

		cfg := resilient.DefaultConfig()
		cfg.MaxRedirects = 2
		cfg.Targets = []resilient.TargetConfig{target(nil)}
		c, err := resilient.New(cfg)
		require.NoError(t, err)
		_, err = c.Send("api", get(t, t.Context(), srv.URL+"/loop"))
		require.ErrorIs(t, err, resilient.KindRedirect)
		assert.Contains(t, err.Error(), "2 redirects")
		assert.Equal(t, int32(3), hits.Load(), "the request and max_redirects redirects, not one more")
		require.NoError(t, c.Shutdown(t.Context()))
	})
	t.Run("zero means return the 3xx", func(t *testing.T) {
		t.Parallel()

		srv, _ := servers(t)

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
		t.Parallel()

		srv, otherHits := servers(t)

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
	t.Parallel()

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

// The connection of an error response goes back to the pool (statusError
// closes the body; net/http reads off the unread rest), and Shutdown closes
// it. One connection per host is allowed, so a request that follows an error
// response waits for that connection instead of racing net/http for it.
func TestSend_ErrorResponseGivesItsConnectionBackAndShutdownClosesIt(t *testing.T) {
	t.Parallel()

	var opened, closed atomic.Int32

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
		switch s { //nolint:exhaustive // only the two ends of a connection's life are counted
		case http.StateNew:
			opened.Add(1)
		case http.StateClosed:
			closed.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	cfg := resilient.DefaultConfig()
	cfg.MaxConnsPerHost = 1
	cfg.Targets = []resilient.TargetConfig{target(func(tc *resilient.TargetConfig) { tc.Breaker.FailureRatio = 0 })}
	client, err := resilient.New(cfg)
	require.NoError(t, err)

	for _, path := range []string{"/ok", "/500", "/404", "/ok", "/500"} {
		resp, err := client.Send("api", get(t, t.Context(), srv.URL+path))
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			require.NoError(t, resp.Body.Close())

			continue
		}

		var oe *resilient.OutboundError
		require.ErrorAs(t, err, &oe)
		require.Equal(t, resilient.KindStatus, oe.Kind, "%s: %v", path, err)

		if path == "/500" {
			assert.Len(t, oe.Body, 4<<10, "the head of the body is kept, no more")
		}
	}

	assert.Equal(t, int32(1), opened.Load(), "one keep-alive connection served all")

	require.NoError(t, client.Shutdown(t.Context()))
	assert.Eventually(t, func() bool { return closed.Load() == 1 }, 5*time.Second, 5*time.Millisecond, "Shutdown closes the idle connection")
}

// What goes to the log and into the error text names scheme, host and path:
// a query string or user info carries credentials. A transport failure is the
// case that matters most — net/http's own error quotes the whole URL.
func TestSend_LogsAndErrorsWithoutQueryStringOrUserInfo(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, status(http.StatusInternalServerError)) // 500: the lowest status logged at warn

	gone := httptest.NewServer(nil)
	gone.Close() // its port refuses connections

	for name, tc := range map[string]struct{ base, msg string }{
		"status":            {srv.URL, "resilient: server error"},
		"transport failure": {gone.URL, "resilient: request failed"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			buf := &testx.LogBuffer{}
			c := newClient(t, target(nil), resilient.WithLogger(slog.New(slog.NewJSONHandler(buf, nil))))

			req := get(t, t.Context(), tc.base+"/v1/items?token=s3cr3t")
			req.URL.User = url.UserPassword("svc", "s3cr3t")

			_, err := c.Send("api", req)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "s3cr3t")

			line := buf.Find(t, map[string]any{"level": "WARN", "target": "api", "msg": tc.msg})
			require.NotNil(t, line, buf.String())
			assert.Equal(t, tc.base+"/v1/items", line["url"])
			assert.NotContains(t, buf.String(), "s3cr3t")
		})
	}
}

// Warnings are bounded per target: a dependency that fails every request
// must not flood the log. The burst and the rate are warn's.
func TestSend_WarningsAreRateLimited(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		buf := &testx.LogBuffer{}
		client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.Breaker.FailureRatio = 0 }),
			scripted(), resilient.WithLogger(slog.New(slog.NewJSONHandler(buf, nil))))

		fail := func(n int) {
			for range n {
				_, err := client.Send("api", get(t, t.Context(), scriptedURL+"/500"))
				require.ErrorIs(t, err, resilient.KindStatus)
			}
		}

		fail(25)
		assert.Len(t, buf.Lines(t), 10, "a burst, then silence")

		time.Sleep(time.Second)
		fail(25)
		assert.Len(t, buf.Lines(t), 11, "then one line a second")
	})
}

// A RoundTripper that panics costs the target nothing past that request: the
// half-open probe it was is recorded as failed — an unrecorded probe keeps
// its slot and the target stays cut off for good — and its bulkhead slot
// comes back.
func TestSend_APanickingTransportReleasesTheProbeAndTheBulkheadSlot(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		tc := target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests, tc.MaxConcurrent = 1, 1 })
		client := newClient(t, tc, scripted())

		_, err := client.Send("api", get(t, t.Context(), scriptedURL+"/500"))
		require.ErrorIs(t, err, resilient.KindStatus)
		time.Sleep(tc.Breaker.OpenTimeout)

		require.PanicsWithValue(t, "the transport blew up", func() {
			_, _ = client.Send("api", get(t, t.Context(), scriptedURL+"/panic"))
		})
		assert.Equal(t, resilient.StateOpen, mustState(t, client), "the probe that panicked failed")
		time.Sleep(tc.Breaker.OpenTimeout)

		resp, err := client.Send("api", get(t, t.Context(), scriptedURL+"/200"))
		require.NoError(t, err, "the next probe is admitted and finds a free slot")
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, resilient.StateClosed, mustState(t, client))
	})
}

// A probe that a local limit turns away gives its half-open slot back: the
// request after it is a probe again. With the slot kept, half-open would
// admit nothing more and the target would stay cut off until a restart.
func TestSend_AProbeRejectedLocallyReleasesItsHalfOpenSlot(t *testing.T) {
	t.Parallel()

	send := func(t *testing.T, client *resilient.Client, path string) error {
		t.Helper()

		resp, err := client.Send("api", get(t, t.Context(), scriptedURL+path))
		if err == nil {
			_ = resp.Body.Close()
		}

		return err
	}

	t.Run("rate limiter", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			tc := target(func(tc *resilient.TargetConfig) {
				tc.Breaker.MinRequests = 1
				tc.RateLimit, tc.RateBurst = 0.001, 1 // one token: the request that trips the breaker takes it
			})
			client := newClient(t, tc, scripted())

			require.ErrorIs(t, send(t, client, "/500"), resilient.KindStatus)
			time.Sleep(tc.Breaker.OpenTimeout)

			require.ErrorIs(t, send(t, client, "/200"), resilient.KindRateLimited)
			require.ErrorIs(t, send(t, client, "/200"), resilient.KindRateLimited, "a probe again, not circuit_open")
		})
	})

	t.Run("bulkhead", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			tc := target(func(tc *resilient.TargetConfig) {
				tc.Breaker.MinRequests, tc.MaxConcurrent = 1, 1
				tc.Timeout = 0 // the body below keeps its slot until it is closed
			})
			client := newClient(t, tc, scripted())

			// A body that fails trips the breaker and still holds the only slot.
			held, err := client.Send("api", get(t, t.Context(), scriptedURL+"/cut"))
			require.NoError(t, err)

			_, err = held.Body.Read(make([]byte, 1))
			require.ErrorIs(t, err, resilient.KindConnection)
			time.Sleep(tc.Breaker.OpenTimeout)

			require.ErrorIs(t, send(t, client, "/200"), resilient.KindBulkheadFull)
			require.ErrorIs(t, send(t, client, "/200"), resilient.KindBulkheadFull, "a probe again, not circuit_open")
			require.NoError(t, held.Body.Close())
		})
	})

	t.Run("a body that cannot be replayed", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			tc := target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests, tc.Breaker.HalfOpenProbes = 1, 2 })
			client := newClient(t, tc, scripted())

			require.ErrorIs(t, send(t, client, "/500"), resilient.KindStatus)
			time.Sleep(tc.Breaker.OpenTimeout)

			// The first probe is answered 429 (retried, a success for the
			// breaker); the retry is the second probe and fails before it is sent.
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, scriptedURL+"/429", strings.NewReader("payload"))
			require.NoError(t, err)

			req.GetBody = func() (io.ReadCloser, error) { return nil, errNoReplay }

			_, err = client.SendWithRetry("api", req)
			require.ErrorIs(t, err, resilient.KindInvalid)

			require.NoError(t, send(t, client, "/200"), "the second probe's slot is free again")
			assert.Equal(t, resilient.StateClosed, mustState(t, client))
		})
	})
}

// Every way an attempt can fail leaves a warning: a failed body, a failed
// request, a local rejection.
func TestSend_WarnsOnBodyFailureRequestFailureAndRejection(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		buf := &testx.LogBuffer{}
		client := newClient(t, target(func(tc *resilient.TargetConfig) {
			tc.Timeout = 30 * time.Millisecond
			tc.Breaker.MinRequests = 2
		}), resilient.WithHTTPClient(hangAfterHead(t)), resilient.WithLogger(slog.New(slog.NewJSONHandler(buf, nil))))

		resp, err := client.Send("api", get(t, t.Context(), hung+"/body"))
		require.NoError(t, err)

		_, err = io.ReadAll(resp.Body)
		require.ErrorIs(t, err, resilient.KindTimeout)
		require.NoError(t, resp.Body.Close())

		_, err = client.Send("api", get(t, t.Context(), hung+"/hang"))
		require.ErrorIs(t, err, resilient.KindTimeout)

		_, err = client.Send("api", get(t, t.Context(), hung+"/hang"))
		require.ErrorIs(t, err, resilient.KindCircuitOpen)

		for _, want := range []map[string]any{
			{"level": "WARN", "msg": "resilient: response body failed", "reason": "timeout", "url": hung + "/body"},
			{"level": "WARN", "msg": "resilient: request failed", "reason": "timeout", "url": hung + "/hang"},
			{"level": "WARN", "msg": "resilient: request rejected", "reason": "circuit_open", "url": hung + "/hang"},
		} {
			assert.NotNil(t, buf.Find(t, want), "%v\n%s", want, buf.String())
		}
	})
}

func TestShutdown_WaitsForInFlightThenRejects(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") })
	client := newClient(t, target(nil))

	open, err := client.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)

	short, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	err = client.Shutdown(short)
	require.ErrorIs(t, err, context.DeadlineExceeded, "an unclosed body is still in flight")
	assert.Contains(t, err.Error(), "1 requests in flight")

	_, err = client.Send("api", get(t, t.Context(), srv.URL))
	require.ErrorIs(t, err, resilient.KindShutdown)
	require.ErrorIs(t, err, resilient.ErrClosed)
	assert.InDelta(t, 1, metric(t, client, "http_client_requests_total", map[string]string{"outcome": "shutdown"}), 0)

	done := make(chan error, 1)
	go func() { done <- client.Shutdown(t.Context()) }()

	select {
	case err := <-done:
		t.Fatalf("Shutdown returned (err: %v) with a request in flight", err)
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, open.Body.Close())
	require.NoError(t, <-done)
}

// Every concurrent Shutdown caller returns once the client is idle, not just
// the first one.
func TestShutdown_ConcurrentCallersAllReturn(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") })
	client := newClient(t, target(nil))

	open, err := client.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)

	const callers = 4

	done := make(chan error, callers)
	for range callers {
		go func() { done <- client.Shutdown(t.Context()) }()
	}

	select {
	case err := <-done:
		t.Fatalf("Shutdown returned (err: %v) with a request in flight", err)
	case <-time.After(20 * time.Millisecond): // let the callers block
	}

	require.NoError(t, open.Body.Close())

	for range callers {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("a Shutdown caller is still waiting although nothing is in flight")
		}
	}
}

func TestShutdown_RaceNoRequestEscapes(t *testing.T) {
	t.Parallel()

	for range 20 {
		srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(time.Millisecond)

			_, _ = io.WriteString(w, "x")
		})
		client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryBaseDelay = 0; tc.RetryMaxDelay = 0 }))

		var start, wg sync.WaitGroup
		start.Add(1)

		var ok, refused atomic.Int32

		for range 16 {
			wg.Go(func() {
				start.Wait()

				resp, err := client.SendWithRetry("api", get(t, t.Context(), srv.URL))
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
		require.NoError(t, client.Shutdown(t.Context()))

		afterShutdown := hits.Load()

		wg.Wait()

		assert.Equal(t, afterShutdown, hits.Load(), "no request reached the server after Shutdown returned")
		assert.Equal(t, ok.Load(), hits.Load(), "every admitted request completed before Shutdown returned")
		assert.Equal(t, int32(16), ok.Load()+refused.Load())
	}
}
