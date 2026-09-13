package httpx_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/testx"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func newTestServer(t testing.TB) *httpx.Server {
	t.Helper()
	log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text"}) // quiet during tests
	return httpx.NewServer(httpx.Config{Service: "test", Addr: ":0", ShutdownTimeout: time.Second}, log)
}

func TestLoadConfig_Defaults(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		cfg, err := httpx.LoadConfig("PING_")
		require.NoError(t, err)
		assert.Equal(t, ":8080", cfg.Addr)
		assert.Equal(t, ":9080", cfg.AdminAddr)
		assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout)
		assert.Equal(t, time.Second, cfg.SlowRequest)
		assert.Equal(t, 100, cfg.LogSampleInitial)
		assert.Equal(t, 100, cfg.LogSampleThereafter)
		assert.Equal(t, "info", cfg.LogLevel)
		assert.Equal(t, "json", cfg.LogFormat)
	}, "httpx", "unit")
}

func TestLoadConfig_PrefixAndOverride(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		t.Setenv("PING_HTTP_ADDR", ":9999")
		t.Setenv("PING_HTTP_SHUTDOWN_TIMEOUT", "3s")
		t.Setenv("PING_LOG_LEVEL", "debug")

		cfg, err := httpx.LoadConfig("PING_")
		require.NoError(t, err)
		assert.Equal(t, ":9999", cfg.Addr)
		assert.Equal(t, 3*time.Second, cfg.ShutdownTimeout)
		assert.Equal(t, "debug", cfg.LogLevel)
	}, "httpx", "unit")
}

func TestHealthz_AlwaysOK(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv := newTestServer(t)
		// /livez is the same handler under the Kubernetes-style name.
		for _, path := range []string{"/healthz", "/livez"} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			srv.Admin().ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code, path)
			assert.JSONEq(t, `{"status":"ok"}`, rec.Body.String(), path)
		}
	}, "httpx", "unit")
}

func TestReadyz_GateClosedThenOpen(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv := newTestServer(t)

		// Before SetReady the gate is closed → 503.
		rec := httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		// Open the gate → 200.
		srv.Health.SetReady(true)
		rec = httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"status":"ready"`)
	}, "httpx", "unit")
}

func TestReadyz_CriticalFailureIsNotReady_OptionalIsDegraded(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv := newTestServer(t)
		srv.Health.SetReady(true)
		srv.Health.Register("db", func(context.Context) error { return errors.New("connection refused") })
		srv.Health.Register("cache", func(context.Context) error { return nil }, httpx.Optional())

		rec := httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "a failing critical check pulls the pod")
		body := rec.Body.String()
		assert.Contains(t, body, `"status":"not_ready"`)
		assert.Contains(t, body, "connection refused")
		assert.Contains(t, body, `"cache":"ok"`)

		// Swap: db healthy, cache (optional) failing → still ready, reported degraded.
		srv.Health.Register("db", func(context.Context) error { return nil })
		srv.Health.Register("cache", func(context.Context) error { return errors.New("timeout") }, httpx.Optional())
		rec = httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		assert.Equal(t, http.StatusOK, rec.Code, "an optional dependency must not pull the pod")
		assert.Contains(t, rec.Body.String(), `"status":"degraded"`)
		assert.Contains(t, rec.Body.String(), `"cache":"timeout"`)
	}, "httpx", "unit")
}

func TestMetricsEndpoint_RecordsRequests(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv := newTestServer(t)
		srv.Engine().GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

		// Drive one request so the counter is non-zero.
		srv.Engine().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ping", nil))

		rec := httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

		assert.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, `http_requests_total{method="GET",path="/ping",status="200"} 1`)
		assert.Contains(t, body, `http_request_duration_seconds_bucket{method="GET",path="/ping",le="0.001"}`)
		assert.NotContains(t, body, `http_request_duration_seconds_bucket{method="GET",path="/ping",status=`,
			"latency histogram must not be split by status")
		assert.Contains(t, body, "http_requests_in_flight 0")
		assert.Contains(t, body, `build_info{go_version="`)
		assert.Contains(t, body, `service="test"`)
		assert.Contains(t, body, `version="dev"`)
	}, "httpx", "unit")
}

func TestMetrics_OpsEndpointsAreNotCountedAsRequests(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv := newTestServer(t)
		for _, path := range []string{"/healthz", "/livez", "/readyz", "/version", "/metrics"} {
			srv.Admin().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
		}
		rec := httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		assert.NotContains(t, rec.Body.String(), `http_requests_total{`, "admin traffic is not API traffic")
	}, "httpx", "unit")
}

func TestAdmin_VersionAndPprof(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv := newTestServer(t)

		rec := httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/version", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		var build httpx.BuildInfo
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &build))
		assert.Equal(t, "test", build.Service)
		assert.Equal(t, httpx.Version, build.Version)
		assert.NotEmpty(t, build.GoVersion)

		rec = httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "goroutine")

		// pprof must not leak onto the API engine.
		rec = httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		rec = httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		assert.Equal(t, http.StatusNotFound, rec.Code)
	}, "httpx", "unit")
}

func TestRun_GracefulShutdownOnContextCancel(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text"})
		// Fixed, unlikely-taken ports so we can probe them over a real socket
		// (Run binds the listeners itself, so ":0" would hide the chosen port).
		srv := httpx.NewServer(httpx.Config{
			Addr: "127.0.0.1:18099", AdminAddr: "127.0.0.1:19099", ShutdownTimeout: 2 * time.Second,
		}, log)
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

		// The admin listener answers on its own port, and readiness is open.
		resp, err := http.Get("http://127.0.0.1:19099/readyz")
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("server did not shut down within timeout")
		}
	}, "httpx", "unit")
}
