package httpx_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/httpx"
	"github.com/wundernightmare/golang-basics/libs/testx"
)

func TestAccessLog_LevelsAndFields(t *testing.T) {
	t.Parallel()

	srv, buf := loggedServer(t, httpx.Config{SlowRequest: 20 * time.Millisecond})
	srv.Mux().HandleFunc("GET /ok/{id}", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "hello") })
	srv.Mux().HandleFunc("GET /bad", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusBadRequest, "no"))
	})
	srv.Mux().HandleFunc("GET /slow", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(40 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	srv.Mux().HandleFunc("GET /err", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) })

	for _, p := range []string{"/ok/7?q=1", "/bad", "/slow", "/err"} {
		get(srv.Handler(), p)
	}

	lines := buf.Lines(t)
	require.Len(t, lines, 4)

	ok := lines[0]
	assert.Equal(t, "request", ok["msg"])
	assert.Equal(t, "INFO", ok["level"])
	assert.Equal(t, "GET", ok["method"])
	assert.Equal(t, "/ok/7", ok["path"], "the path, without the query string")
	assert.Equal(t, "/ok/{id}", ok["route"])
	assert.Equal(t, float64(200), ok["status"])
	assert.Equal(t, float64(5), ok["bytes"])
	assert.Equal(t, "192.0.2.1", ok["client_ip"])
	assert.Contains(t, ok, "latency_ms")
	assert.NotEmpty(t, ok["request_id"])

	assert.Equal(t, "INFO", lines[1]["level"], "4xx are info: warn is never sampled, and scanners exist")
	assert.Equal(t, "slow request", lines[2]["msg"])
	assert.Equal(t, "WARN", lines[2]["level"])
	assert.Equal(t, "ERROR", lines[3]["level"])
	assert.Equal(t, float64(502), lines[3]["status"])
}

func TestWithMiddleware_SeesRequestIDAndRoute(t *testing.T) {
	t.Parallel()

	var (
		sawID, sawRoute string
		hadDeadline     bool
	)

	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sawID = httpx.RequestIDFromContext(r.Context())
			_, hadDeadline = r.Context().Deadline()
			next.ServeHTTP(w, r)
			sawRoute = httpx.RouteFromContext(r.Context()) // set once routing ran
		})
	}
	srv, _ := loggedServer(t, httpx.Config{RequestTimeout: time.Minute}, httpx.WithMiddleware(mw))

	var handlerRoute string

	srv.Mux().HandleFunc("GET /tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		handlerRoute = httpx.RouteFromContext(r.Context())

		w.WriteHeader(http.StatusOK)
	})
	rec := get(srv.Handler(), "/tasks/9")
	assert.Equal(t, rec.Header().Get(httpx.RequestIDHeader), sawID)
	assert.True(t, hadDeadline, "the per-request deadline is applied before extra middleware")
	assert.Equal(t, "/tasks/{id}", sawRoute)
	assert.Empty(t, handlerRoute, "the route is recorded after the handler returns")
}

// Where the service's middleware sits in the chain (the order of record is
// the NewServer doc): inside tracing, the access log and the metrics, so
// what it rejects is still observed; behind the server's rate limit, so what
// is shed never reaches it; around the handler's panic recovery, so it sees
// a response and not a panic; and under a recovery of its own, so its own
// panic is a counted 500 rather than a dropped connection.
func TestChain_OrderAroundTheServicesMiddleware(t *testing.T) {
	t.Parallel()

	var entered, returned int

	extra := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered++

			switch r.URL.Path {
			case "/deny":
				w.WriteHeader(http.StatusUnauthorized)
			case "/mw-panic":
				panic("middleware bug")
			default:
				next.ServeHTTP(w, r)
			}

			returned++
		})
	}

	spans := testx.Recorder(t)
	srv, buf := loggedServer(t, httpx.Config{RateLimit: 0.001, RateBurst: 3},
		httpx.WithMiddleware(extra), httpx.WithTracerProvider(spans.Provider), httpx.WithPropagator(spans.Propagator))
	srv.Mux().HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("handler bug") })

	reg := srv.Metrics.Registry
	counted := func(status string) float64 {
		return testx.Metric(t, reg, "http_requests_total", map[string]string{"route": "unmatched", "status": status})
	}

	assert.Equal(t, http.StatusUnauthorized, get(srv.Handler(), "/deny").Code)
	assert.Equal(t, 1.0, counted("401"))
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "request", "path": "/deny", "status": 401.0}))
	assert.Len(t, spans.Ended(), 1)

	assert.Equal(t, http.StatusInternalServerError, get(srv.Handler(), "/boom").Code)
	assert.Equal(t, 2, returned, "the handler's panic was a response by the time it reached the middleware")

	rec := get(srv.Handler(), "/mw-panic")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))
	assert.Equal(t, 1.0, counted("500"))
	assert.Equal(t, 2.0, testx.Metric(t, reg, "http_unhandled_errors_total", nil))
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "request", "path": "/mw-panic", "status": 500.0, "level": "ERROR"}))

	assert.Equal(t, http.StatusTooManyRequests, get(srv.Handler(), "/deny").Code)
	assert.Equal(t, 3, entered, "a shed request costs no authentication")
}

func TestRequestTimeout_IsTheHandlersDeadline(t *testing.T) {
	t.Parallel()

	srv, _ := loggedServer(t, httpx.Config{RequestTimeout: 20 * time.Millisecond})
	srv.Mux().HandleFunc("GET /wait", func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		httpx.WriteError(w, r, r.Context().Err())
	})

	start := time.Now()
	rec := get(srv.Handler(), "/wait")
	assert.Equal(t, http.StatusGatewayTimeout, rec.Code)
	assert.Less(t, time.Since(start), 2*time.Second)

	srv, _ = loggedServer(t, httpx.Config{RequestTimeout: -1})
	srv.Mux().HandleFunc("GET /dl", func(w http.ResponseWriter, r *http.Request) {
		_, ok := r.Context().Deadline()
		assert.False(t, ok, "a non-positive timeout means no deadline")
	})
	get(srv.Handler(), "/dl")
}

func TestRequestTimeoutHeader_ShortensNeverExtends(t *testing.T) {
	t.Parallel()

	remaining := func(cfg time.Duration, header string) (time.Duration, bool) {
		srv, _ := loggedServer(t, httpx.Config{RequestTimeout: cfg})

		var (
			left time.Duration
			ok   bool
		)

		srv.Mux().HandleFunc("GET /dl", func(_ http.ResponseWriter, r *http.Request) {
			var dl time.Time

			dl, ok = r.Context().Deadline()
			left = time.Until(dl)
		})

		req := httptest.NewRequest(http.MethodGet, "/dl", http.NoBody)
		if header != "" {
			req.Header.Set(httpx.RequestTimeoutHeader, header)
		}

		do(srv.Handler(), req)

		return left, ok
	}

	left, _ := remaining(time.Minute, "200")
	assert.LessOrEqual(t, left, 200*time.Millisecond, "a shorter header wins")

	left, _ = remaining(300*time.Millisecond, "60000")
	assert.LessOrEqual(t, left, 300*time.Millisecond, "the header never extends the server's budget")

	left, ok := remaining(-1, "250")
	assert.True(t, ok, "with no configured timeout the header alone sets the deadline")
	assert.LessOrEqual(t, left, 250*time.Millisecond)

	for _, bad := range []string{"0", "-5", "1.5", "abc", "1234567890", " 100"} {
		left, _ = remaining(time.Minute, bad)
		assert.Greater(t, left, 50*time.Second, "invalid %q is ignored", bad)
	}
}

// A caller cannot ask for less than HTTP_REQUEST_TIMEOUT_MIN: a 1 ms budget
// would make every request a 504 the server is blamed (and paged) for.
func TestRequestTimeoutHeader_NotBelowTheFloor(t *testing.T) {
	t.Parallel()

	remaining := func(cfg, floor time.Duration, header string) time.Duration {
		srv, _ := loggedServer(t, httpx.Config{RequestTimeout: cfg, RequestTimeoutMin: floor})

		var left time.Duration

		srv.Mux().HandleFunc("GET /dl", func(_ http.ResponseWriter, r *http.Request) {
			dl, ok := r.Context().Deadline()
			assert.True(t, ok)

			left = time.Until(dl)
		})

		req := httptest.NewRequest(http.MethodGet, "/dl", http.NoBody)
		req.Header.Set(httpx.RequestTimeoutHeader, header)
		do(srv.Handler(), req)

		return left
	}
	assert.Greater(t, remaining(time.Minute, time.Second, "1"), 900*time.Millisecond, "1 ms is raised to the floor")
	assert.LessOrEqual(t, remaining(time.Minute, time.Second, "1"), time.Second)
	assert.LessOrEqual(t, remaining(time.Minute, time.Second, "5000"), 5*time.Second, "above the floor the header still shortens")
	assert.Greater(t, remaining(time.Minute, time.Second, "5000"), 4*time.Second)
	assert.LessOrEqual(t, remaining(500*time.Millisecond, 0, "60000"), 500*time.Millisecond, "never extends")

	slow := func(cfg, floor time.Duration) int {
		srv, _ := loggedServer(t, httpx.Config{RequestTimeout: cfg, RequestTimeoutMin: floor})
		srv.Mux().HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
				httpx.WriteError(w, r, r.Context().Err())
			case <-time.After(150 * time.Millisecond):
				w.WriteHeader(http.StatusNoContent)
			}
		})

		req := httptest.NewRequest(http.MethodGet, "/slow", http.NoBody)
		req.Header.Set(httpx.RequestTimeoutHeader, "1")

		return do(srv.Handler(), req).Code
	}
	assert.Equal(t, http.StatusNoContent, slow(time.Minute, time.Second), "a 1 ms ask cannot turn a 150 ms request into a 504")
	assert.Equal(t, http.StatusGatewayTimeout, slow(time.Minute, 0), "without a floor it would")
}

func TestValidate_RequestTimeoutMin(t *testing.T) {
	t.Parallel()

	base := func() httpx.Config {
		return httpx.Config{Addr: ":0", ShutdownTimeout: time.Second, MaxBodyBytes: 1, MaxHeaderBytes: 1, RequestTimeout: time.Second}
	}
	c := base()
	c.RequestTimeoutMin = -time.Millisecond
	require.ErrorContains(t, c.Validate(), "HTTP_REQUEST_TIMEOUT_MIN must not be negative")
	c = base()
	c.RequestTimeoutMin = 2 * time.Second
	require.ErrorContains(t, c.Validate(), "must not exceed HTTP_REQUEST_TIMEOUT")
	c = base()
	c.RequestTimeoutMin = time.Second
	require.NoError(t, c.Validate())
}

func TestNewServer_NilLoggerIsDiscarded(t *testing.T) {
	t.Parallel()

	srv, err := httpx.NewServer(httpx.Config{Addr: ":0"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, srv.Logger())
	assert.Nil(t, srv.LogLevel)
	rec := get(srv.Handler(), "/nope")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// The shutdown sequence: readiness flips first, requests are still served
// during the drain delay, then Run returns nil once the listeners are closed.
func TestRun_GracefulShutdownSequence(t *testing.T) {
	t.Parallel()

	srv, buf := loggedServer(t, httpx.Config{
		Addr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0",
		ShutdownDelay: 300 * time.Millisecond, ShutdownTimeout: 2 * time.Second,
	})
	srv.Mux().HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "pong") })

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return srv.ListenAddr() != "" && srv.AdminListenAddr() != "" }, 2*time.Second, 5*time.Millisecond)

	api, adm := "http://"+srv.ListenAddr(), "http://"+srv.AdminListenAddr()

	require.Eventually(t, func() bool { return status(t, adm+"/readyz") == http.StatusOK }, 2*time.Second, 5*time.Millisecond)

	resp, err := http.Get(api + "/ping")
	require.NoError(t, err)

	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	assert.Equal(t, "pong", string(b))

	cancel()
	require.Eventually(t, func() bool { return status(t, adm+"/readyz") == http.StatusServiceUnavailable },
		time.Second, 5*time.Millisecond, "readiness flips as soon as shutdown starts")
	assert.Equal(t, http.StatusOK, status(t, api+"/ping"), "…while the API still serves during the delay")

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down")
	}

	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "shutdown requested, draining"}))
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "servers stopped cleanly"}))

	_, err = http.Get(api + "/ping") //nolint:bodyclose // expected to fail
	assert.Error(t, err, "the listener is closed")
}

func TestRun_PortInUseIsReturned(t *testing.T) {
	t.Parallel()

	busy := httptest.NewServer(http.NotFoundHandler())
	defer busy.Close()

	srv, _ := loggedServer(t, httpx.Config{Addr: busy.Listener.Addr().String(), AdminAddr: "127.0.0.1:0"})
	err := srv.Run(context.Background())
	require.ErrorContains(t, err, "bind API listener")

	// The admin port in use: the API listener bound a moment earlier is
	// given back, not left accepting connections nobody will serve.
	srv, _ = loggedServer(t, httpx.Config{Addr: "127.0.0.1:0", AdminAddr: busy.Listener.Addr().String()})
	err = srv.Run(context.Background())
	require.ErrorContains(t, err, "bind admin listener")

	_, err = net.DialTimeout("tcp", srv.ListenAddr(), time.Second)
	require.Error(t, err, "the API listener is closed")
}

// Run evaluates the checks once before it opens the readiness gate, and from
// then on in the background: a probe never has to pay for a check.
func TestRun_EvaluatesChecksOnceThenInTheBackground(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		interval time.Duration
		settles  func(calls *atomic.Int32) bool
	}{
		"at start":          {time.Hour, func(calls *atomic.Int32) bool { return calls.Load() == 1 }},
		"in the background": {20 * time.Millisecond, func(calls *atomic.Int32) bool { return calls.Load() >= 3 }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv, logs := loggedServer(t, httpx.Config{
				AdminAddr: "127.0.0.1:0", HealthInterval: tc.interval, HealthTimeout: 10 * time.Millisecond, ShutdownTimeout: time.Second,
			})

			var calls atomic.Int32

			srv.Health.Register("db", func(context.Context) error { calls.Add(1); return nil })

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)

			go func() { done <- srv.Run(ctx) }()

			require.Eventually(t, func() bool { return tc.settles(&calls) }, 5*time.Second, 5*time.Millisecond)
			cancel()
			require.NoError(t, <-done)
			require.NotNil(t, logs.Find(t, map[string]any{"msg": "servers stopped cleanly"}))

			if tc.interval == time.Hour {
				assert.Equal(t, int32(1), calls.Load(), "one evaluation for the whole start-up")
			}
		})
	}
}

func status(t testing.TB, url string) int {
	t.Helper()

	resp, err := http.Get(url) // test URL on a loopback listener
	if err != nil {
		return 0
	}

	_ = resp.Body.Close()

	return resp.StatusCode
}
