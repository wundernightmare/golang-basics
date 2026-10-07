package api_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/contracts/pingapi"
	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx/contract"
	"github.com/tracehubmmp/golang-basics/services/ping/internal/api"
)

func newServer(t testing.TB) *httpx.Server {
	t.Helper()
	log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "json", Writer: io.Discard})
	srv, err := httpx.NewServer(httpx.Config{Service: "ping", Addr: ":0", AdminAddr: "127.0.0.1:0"}, log)
	require.NoError(t, err)
	api.Register(srv)
	return srv
}

// getJSON serves GET target through the full API chain, checks the exchange
// against the OpenAPI contract (api/tsp/ping.tsp) and decodes a 200 body.
func getJSON[V any](t testing.TB, oas *contract.OpenAPI, srv *httpx.Server, target string) (int, V) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	oas.Validate(t, req, nil, rec.Code, rec.Header(), rec.Body.Bytes())
	var v V
	if rec.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v), rec.Body.String())
	}
	return rec.Code, v
}

func TestPing(t *testing.T) {
	t.Parallel()
	oas := contract.LoadOpenAPI(t, "openapi3/ping.openapi.yaml")
	for _, msg := range []string{"", "hello", "привет мир", "a&b=c"} {
		t.Run("msg="+msg, func(t *testing.T) {
			t.Parallel()
			srv := newServer(t)
			target := "/ping"
			if msg != "" {
				target += "?msg=" + url.QueryEscape(msg)
			}
			code, body := getJSON[api.PongResponse](t, oas, srv, target)
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, pingapi.Pong, body.Message)
			if msg == "" {
				assert.Nil(t, body.Echo, "no echo member when ?msg= is absent")
				return
			}
			require.NotNil(t, body.Echo)
			assert.Equal(t, msg, *body.Echo, "echo mirrors ?msg=")
		})
	}
}

func TestVersion(t *testing.T) {
	t.Parallel()
	oas := contract.LoadOpenAPI(t, "openapi3/ping.openapi.yaml")
	srv := newServer(t)
	code, body := getJSON[api.VersionResponse](t, oas, srv, "/version")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ping", body.Service)
	assert.Equal(t, httpx.Version, body.Version)
	assert.NotEmpty(t, body.GoVersion)
	assert.Equal(t, srv.Build, body, "the same identity the admin listener reports")
}

func TestErrorsAreProblems(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	for _, tc := range []struct {
		method, target string
		status         int
	}{
		{http.MethodGet, "/nope", http.StatusNotFound},
		{http.MethodPost, "/ping", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/version", http.StatusMethodNotAllowed},
	} {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, nil))
		assert.Equal(t, tc.status, rec.Code, "%s %s", tc.method, tc.target)
		assert.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))
	}
}

func TestMetricsCountPingByRoute(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	for range 3 {
		srv.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ping?msg=x", nil))
	}
	rec := httptest.NewRecorder()
	srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Contains(t, rec.Body.String(), `http_requests_total{method="GET",route="/ping",status="200"} 3`)
}

// The operational endpoints come from httpx on the admin listener — assert
// the service wires them up (and keeps them off the API port) rather than
// re-testing httpx internals.
func TestAdminEndpoints(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	srv.Health.SetReady(true)
	for _, path := range []string{"/healthz", "/livez", "/readyz", "/metrics", "/version", "/debug/pprof/"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			assert.Equal(t, http.StatusOK, rec.Code, "served on admin")

			if path == "/version" {
				return // deliberately public on the API too
			}
			rec = httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			assert.Equal(t, http.StatusNotFound, rec.Code, "absent from the API port")
		})
	}
}
