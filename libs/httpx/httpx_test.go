package httpx_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func newTestServer(t *testing.T) *httpx.Server {
	t.Helper()
	log := httpx.NewLogger("error", "text") // quiet during tests
	return httpx.NewServer(httpx.Config{Addr: ":0", ShutdownTimeout: time.Second}, log)
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := httpx.LoadConfig("PING_")
	require.NoError(t, err)
	assert.Equal(t, ":8080", cfg.Addr)
	assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "json", cfg.LogFormat)
}

func TestLoadConfig_PrefixAndOverride(t *testing.T) {
	t.Setenv("PING_HTTP_ADDR", ":9999")
	t.Setenv("PING_HTTP_SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("PING_LOG_LEVEL", "debug")

	cfg, err := httpx.LoadConfig("PING_")
	require.NoError(t, err)
	assert.Equal(t, ":9999", cfg.Addr)
	assert.Equal(t, 3*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, "debug", cfg.LogLevel)
}

func TestHealthz_AlwaysOK(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	srv.Engine().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
}

func TestReadyz_GateClosedThenOpen(t *testing.T) {
	srv := newTestServer(t)

	// Before SetReady the gate is closed → 503.
	rec := httptest.NewRecorder()
	srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	// Open the gate → 200.
	srv.Health.SetReady(true)
	rec = httptest.NewRecorder()
	srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"status":"ready"`)
}

func TestReadyz_FailingCheckIsDegraded(t *testing.T) {
	srv := newTestServer(t)
	srv.Health.SetReady(true)
	srv.Health.Register("db", func(context.Context) error { return errors.New("connection refused") })
	srv.Health.Register("cache", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, `"status":"degraded"`)
	assert.Contains(t, body, "connection refused")
	assert.Contains(t, body, `"cache":"ok"`)
}

func TestMetricsEndpoint_RecordsRequests(t *testing.T) {
	srv := newTestServer(t)
	srv.Engine().GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	// Drive one request so the counter is non-zero.
	srv.Engine().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ping", nil))

	rec := httptest.NewRecorder()
	srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "http_requests_total")
	assert.Contains(t, body, `path="/ping"`)
	assert.Contains(t, body, "http_request_duration_seconds")
}

func TestRun_GracefulShutdownOnContextCancel(t *testing.T) {
	log := httpx.NewLogger("error", "text")
	// A fixed, unlikely-taken port so we can probe it over a real socket
	// (Run binds the listener itself, so ":0" would hide the chosen port).
	srv := httpx.NewServer(httpx.Config{Addr: "127.0.0.1:18099", ShutdownTimeout: 2 * time.Second}, log)
	srv.Engine().GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	// Wait for the listener, then hit /ping over a real socket.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://127.0.0.1:18099/ping")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode == http.StatusOK && string(b) == "pong"
	}, 3*time.Second, 25*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down within timeout")
	}
}
