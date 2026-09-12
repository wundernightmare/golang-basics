package api_test

// The ping API tests as a testo suite with the Allure plugin: the same
// assertions as before, but each test carries a title, tags and steps, and
// every `go test` run writes allure-results/ (ALLURE_RESULTS_DIR to redirect)
// that `just allure-report` renders. testo tests are plain `go test` tests, so
// coverage, -run, -race and the CI matrix work unchanged.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/ozontech/testo"
	allure "github.com/ozontech/testo-allure"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/services/ping/internal/api"
)

type Suite struct{ testo.Suite[testx.T] }

func TestPingAPI(t *testing.T) { testo.RunSuite(t, new(Suite), testx.Options("ping", "unit")...) }

func newServer(t testx.T) *httpx.Server {
	t.Helper()
	log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text"})
	srv := httpx.NewServer(httpx.Config{Service: "ping", Addr: ":0"}, log)
	api.Register(srv)
	return srv
}

func getJSON[V any](t testx.T, h http.Handler, path string) (int, V) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var v V
	if rec.Code == http.StatusOK {
		t.Require().NoError(json.Unmarshal(rec.Body.Bytes(), &v))
	}
	t.Attach("response", allure.Bytes(rec.Body.String()).As(allure.DocumentJSON))
	return rec.Code, v
}

// CasesMsg parametrises TestPing: one Allure test per echo value.
func (Suite) CasesMsg() []string { return []string{"", "hello", "привет мир"} }

func (Suite) TestPing(t testx.T, p struct{ Msg string }) {
	t.Parallel()
	t.Title("GET /ping answers pong and echoes ?msg=")
	t.Feature("ping")

	srv := newServer(t)
	path := "/ping"
	if p.Msg != "" {
		path += "?msg=" + url.QueryEscape(p.Msg)
	}
	code, body := getJSON[api.PongResponse](t, srv.Engine(), path)
	t.Require().Equal(http.StatusOK, code)
	t.Assert().Equal("pong", body.Message)
	t.Assert().Equal(p.Msg, body.Echo, "echo mirrors ?msg= (empty when absent)")
}

func (Suite) TestVersion(t testx.T) {
	t.Parallel()
	t.Title("GET /version reports the build identity on the API port")
	t.Feature("ping")

	srv := newServer(t)
	code, body := getJSON[api.VersionResponse](t, srv.Engine(), "/version")
	t.Require().Equal(http.StatusOK, code)
	t.Assert().Equal("ping", body.Service)
	t.Assert().Equal(httpx.Version, body.Version)
	t.Assert().NotEmpty(body.GoVersion)
}

// The operational endpoints come from httpx for free on the admin listener —
// assert the service wires them up (and keeps them off the API engine) rather
// than re-testing httpx internals.
func (Suite) TestAdminEndpoints(t testx.T) {
	t.Parallel()
	t.Title("operational routes live on the admin listener only")
	t.Feature("admin")

	srv := newServer(t)
	srv.Health.SetReady(true)
	for _, path := range []string{"/healthz", "/readyz", "/metrics", "/version", "/debug/pprof/"} {
		allure.Step(t, "GET "+path, func(t testx.T) {
			rec := httptest.NewRecorder()
			srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			t.Assert().Equal(http.StatusOK, rec.Code, "served on admin")

			if path == "/version" {
				return // deliberately public on the API too
			}
			rec = httptest.NewRecorder()
			srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			t.Assert().Equal(http.StatusNotFound, rec.Code, "absent from the API port")
		})
	}
}
