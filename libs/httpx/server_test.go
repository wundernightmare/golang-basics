package httpx_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func newTestServer(t testing.TB, buf *testx.LogBuffer, opts ...httpx.Option) *httpx.Server {
	t.Helper()
	log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: buf})
	srv, err := httpx.NewServer(httpx.Config{
		Service: "test", Addr: ":0", ShutdownTimeout: time.Second, MaxBodyBytes: 64,
		TrustedProxies: []string{"10.0.0.0/8"},
	}, log, opts...)
	require.NoError(t, err)
	return srv
}

func do(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestNewServer_RejectsInvalidConfig(t *testing.T) {
	log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text", Writer: io.Discard})
	_, err := httpx.NewServer(httpx.Config{Addr: ":0", LogLevel: "verbos"}, log)
	require.ErrorContains(t, err, "LOG_LEVEL")

	_, err = httpx.NewServer(httpx.Config{Addr: ":0", AdminAddr: ":9080"}, log)
	require.ErrorContains(t, err, "ADMIN_TOKEN is empty", "an open admin surface on a routable address is rejected")

	_, err = httpx.NewServer(httpx.Config{Addr: ":0", AdminAddr: "127.0.0.1:9080"}, log)
	require.NoError(t, err, "loopback admin without a token is fine")

	_, err = httpx.NewServer(httpx.Config{Addr: ":0", AdminAddr: ":9080", AdminInsecure: true}, log)
	require.NoError(t, err)
}

func TestRouting_UnknownRouteAndMethodAreProblems(t *testing.T) {
	buf := &testx.LogBuffer{}
	srv := newTestServer(t, buf)
	srv.Mux().HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id")})
	})

	rec := do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/items/42", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"id":"42"}`, rec.Body.String())
	assert.Len(t, rec.Header().Get(httpx.RequestIDHeader), 16, "a request id is minted and echoed")

	rec = do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/nope", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), `"request_id"`)

	rec = do(srv.Handler(), httptest.NewRequest(http.MethodDelete, "/items/42", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))
	assert.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))

	lines := buf.Lines(t)
	require.Len(t, lines, 3)
	assert.Equal(t, "/items/{id}", lines[0]["route"], "logged by route template")
	assert.Equal(t, "unmatched", lines[1]["route"])
	assert.Equal(t, "INFO", lines[1]["level"], "a 404 is not a warning")
	assert.Equal(t, "192.0.2.1", lines[0]["client_ip"], "httptest's peer address, no proxy trusted")

	reg := srv.Metrics.Registry
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "GET", "route": "/items/{id}", "status": "200"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "GET", "route": "unmatched", "status": "404"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "DELETE", "route": "unmatched", "status": "405"}))
	testx.LintMetrics(t, reg)
}

func TestPanic_IsA500AndCounted(t *testing.T) {
	buf := &testx.LogBuffer{}
	srv := newTestServer(t, buf)
	srv.Mux().HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("kaboom") })

	rec := do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/boom", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))
	assert.NotContains(t, rec.Body.String(), "kaboom", "the panic value stays in the log")

	reg := srv.Metrics.Registry
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_panics_total", nil))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"route": "/boom", "status": "500"}))
	assert.Equal(t, 0.0, testx.Metric(t, reg, "http_requests_in_flight", nil), "the gauge does not stick after a panic")

	panicLine := buf.Find(t, map[string]any{"msg": "panic in handler"})
	require.NotNil(t, panicLine)
	assert.Equal(t, "kaboom", panicLine["panic"])
	assert.Contains(t, panicLine["stack"], "server_test.go")
	access := buf.Find(t, map[string]any{"msg": "request"})
	require.NotNil(t, access)
	assert.Equal(t, float64(500), access["status"])
	assert.Equal(t, "ERROR", access["level"])
}

func TestBodyLimit_And_DecodeJSON(t *testing.T) {
	srv := newTestServer(t, &testx.LogBuffer{})
	type in struct {
		Title string `json:"title"`
	}
	srv.Mux().HandleFunc("POST /items", func(w http.ResponseWriter, r *http.Request) {
		var v in
		if err := httpx.DecodeJSON(r, &v); err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		httpx.WriteJSON(w, http.StatusCreated, v)
	})

	rec := do(srv.Handler(), httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"title":"x"}`)))
	assert.Equal(t, http.StatusCreated, rec.Code)

	rec = do(srv.Handler(), httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"title":`)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = do(srv.Handler(), httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"title":"`+strings.Repeat("x", 100)+`"}`)))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, "MaxBodyBytes is 64")
}

func TestWriteError_LogsTheCauseOfA500(t *testing.T) {
	buf := &testx.LogBuffer{}
	srv := newTestServer(t, buf)
	srv.Mux().HandleFunc("GET /fail", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.Internal("could not persist", errors.New("pgconn: connection refused")))
	})
	rec := do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/fail", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "could not persist")
	assert.NotContains(t, rec.Body.String(), "pgconn")

	line := buf.Find(t, map[string]any{"msg": "request failed"})
	require.NotNil(t, line)
	assert.Equal(t, "pgconn: connection refused", line["err"])
	assert.NotEmpty(t, line["request_id"])
}

func TestTrustedProxy_ClientIPAndRequestID(t *testing.T) {
	buf := &testx.LogBuffer{}
	srv := newTestServer(t, buf)
	srv.Mux().HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, httpx.ClientIPFromContext(r.Context()))
	})

	// Untrusted peer: the header is ignored, the id is minted.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "6.6.6.6")
	req.Header.Set(httpx.RequestIDHeader, "client-chosen")
	rec := do(srv.Handler(), req)
	assert.Equal(t, "192.0.2.1", rec.Body.String())
	assert.NotEqual(t, "client-chosen", rec.Header().Get(httpx.RequestIDHeader))

	// Trusted peer: the first untrusted hop from the right is the client.
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.1.2.3:4444"
	req.Header.Set("X-Forwarded-For", "6.6.6.6, 203.0.113.7, 10.0.0.9")
	req.Header.Set(httpx.RequestIDHeader, "ingress-id")
	rec = do(srv.Handler(), req)
	assert.Equal(t, "203.0.113.7", rec.Body.String())
	assert.Equal(t, "ingress-id", rec.Header().Get(httpx.RequestIDHeader))
}

func TestAdmin_TokenGuardsAdminAndDebug(t *testing.T) {
	log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text", Writer: io.Discard})
	srv, err := httpx.NewServer(httpx.Config{Addr: ":0", AdminAddr: ":9099", AdminToken: "s3cret"}, log)
	require.NoError(t, err)

	for _, path := range []string{"/healthz", "/livez", "/metrics", "/version"} {
		rec := do(srv.Admin(), httptest.NewRequest(http.MethodGet, path, nil))
		assert.NotEqual(t, http.StatusUnauthorized, rec.Code, path)
	}
	for _, path := range []string{"/admin/config", "/admin/log-level", "/debug/pprof/", "/debug/pprof/cmdline"} {
		rec := do(srv.Admin(), httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusUnauthorized, rec.Code, path)
		assert.Equal(t, `Bearer realm="admin"`, rec.Header().Get("WWW-Authenticate"))
	}
	req := httptest.NewRequest(http.MethodGet, "/admin/config", nil)
	req.Header.Set("Authorization", "bearer s3cret") // scheme is case-insensitive
	rec := do(srv.Admin(), req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"admin_token":"[redacted]"`)
}

func TestReadyz_ProbeCancellationDoesNotPoisonTheCache(t *testing.T) {
	srv := newTestServer(t, &testx.LogBuffer{})
	srv.Health.SetReady(true)
	srv.Health.Register("db", func(ctx context.Context) error {
		select {
		case <-time.After(50 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	// A probe that gives up after 5ms: the inline refresh must not inherit it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	rec := do(srv.Admin(), httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(ctx))
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"db":"ok"`)
}

func TestRun_DrainsThenForcesClose(t *testing.T) {
	log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text", Writer: io.Discard})
	srv, err := httpx.NewServer(httpx.Config{
		Addr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0",
		ShutdownDelay: 0, ShutdownTimeout: 200 * time.Millisecond, RequestTimeout: time.Minute,
	}, log)
	require.NoError(t, err)
	started := make(chan struct{}, 1)
	sawCancel := make(chan bool, 1)
	srv.Mux().HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-time.After(5 * time.Second):
			sawCancel <- false
		case <-r.Context().Done():
			sawCancel <- true // the forced close cancelled the base context
		}
		_, _ = io.WriteString(w, "done")
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	require.Eventually(t, func() bool { return srv.ListenAddr() != "" && srv.AdminListenAddr() != "" }, 2*time.Second, 5*time.Millisecond)

	// Readiness is open once Run has bound and evaluated the checks.
	resp, err := http.Get("http://" + srv.AdminListenAddr() + "/readyz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	go func() {
		resp, err := http.Get("http://" + srv.ListenAddr() + "/slow")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started

	shutdownStart := time.Now()
	cancel()
	select {
	case err := <-done:
		require.Error(t, err, "the drain budget was exhausted by the stuck request")
		assert.Less(t, time.Since(shutdownStart), 2*time.Second, "Run returns once the budget is spent, not when the handler feels like it")
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
	assert.True(t, <-sawCancel, "the in-flight request's context was cancelled by the forced close")
}
