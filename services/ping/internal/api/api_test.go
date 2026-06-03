package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/ping/internal/api"
)

func newServer(t *testing.T) *httpx.Server {
	t.Helper()
	log := httpx.NewLogger("error", "text")
	srv := httpx.NewServer(httpx.Config{Addr: ":0"}, log)
	api.Register(srv)
	return srv
}

func TestPing_ReturnsPong(t *testing.T) {
	srv := newServer(t)

	rec := httptest.NewRecorder()
	srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ping", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	var body api.PongResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "pong", body.Message)
	assert.Empty(t, body.Echo)
}

func TestPing_EchoesMsg(t *testing.T) {
	srv := newServer(t)

	rec := httptest.NewRecorder()
	srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ping?msg=hello", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	var body api.PongResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "pong", body.Message)
	assert.Equal(t, "hello", body.Echo)
}

func TestVersion_ReportsService(t *testing.T) {
	srv := newServer(t)

	rec := httptest.NewRecorder()
	srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/version", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	var body api.VersionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ping", body.Service)
	assert.NotEmpty(t, body.GoVer)
}

// The shared health/metrics endpoints come from httpx for free — assert the
// service wires them up rather than re-testing httpx internals.
func TestSharedEndpointsArePresent(t *testing.T) {
	srv := newServer(t)
	srv.Health.SetReady(true)

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equalf(t, http.StatusOK, rec.Code, "GET %s", path)
	}
}
