package httpx_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func TestMetrics_EndpointRecordsRequestsByRoute(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	srv.Mux().HandleFunc("GET /items/{id}", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	get(srv.Handler(), "/items/1")
	get(srv.Handler(), "/items/2")

	body := get(srv.Admin(), "/metrics").Body.String()
	assert.Contains(t, body, `http_requests_total{method="GET",route="/items/{id}",status="200"} 2`,
		"one series per route template, not per URL")
	assert.Contains(t, body, `http_request_duration_seconds_bucket{method="GET",route="/items/{id}",le="0.001"}`)
	assert.NotContains(t, body, `route="/items/1"`)
	assert.NotContains(t, body, `http_request_duration_seconds_bucket{method="GET",route="/items/{id}",status=`,
		"latency histogram must not be split by status")
	assert.Contains(t, body, "http_requests_in_flight 0")
	assert.Contains(t, body, `build_info{revision="`)
	assert.Contains(t, body, `version="dev"`)
	assert.NotContains(t, body, `build_info{go_version`, "go_info carries the Go version")
	assert.NotContains(t, body, `service="test"`, "the scrape config attaches service; a second one would become exported_service")
}

// Arbitrary methods must not grow the series: anything outside the standard
// set is counted as _OTHER, on matched and unmatched routes alike.
func TestMetrics_NonStandardMethodsCollapseToOther(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	srv.Mux().HandleFunc("/any", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	for _, m := range []string{"FOO", "BAR", "PROPFIND", "get"} {
		do(srv.Handler(), httptest.NewRequest(m, "/any", nil))
		do(srv.Handler(), httptest.NewRequest(m, "/nowhere", nil))
	}
	do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/any", nil))

	reg := srv.Metrics.Registry
	assert.Equal(t, 4.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "_OTHER", "route": "/any", "status": "200"}))
	assert.Equal(t, 4.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "_OTHER", "route": "unmatched", "status": "404"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "GET", "route": "/any"}))
	assert.Len(t, series(t, reg, "http_requests_total"), 3, "four odd methods, still three series")
	for _, m := range series(t, reg, "http_request_duration_seconds") {
		assert.True(t, hasLabels(m, map[string]string{"method": "_OTHER"}) || hasLabels(m, map[string]string{"method": "GET"}))
	}
}

// A request whose client went away before anything was written is a 499, in
// the counter and the log alike, not the 200 net/http would assume; a handler
// that reports the cancellation through WriteError is a 499 too, not a 5xx.
func TestMetrics_ClientGoneIs499(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	srv.Mux().HandleFunc("GET /silent", func(http.ResponseWriter, *http.Request) {})
	srv.Mux().HandleFunc("GET /failed", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, fmt.Errorf("query: %w", r.Context().Err()))
	})
	for _, path := range []string{"/silent", "/failed"} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		do(srv.Handler(), httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx))
	}
	get(srv.Handler(), "/silent")

	reg := srv.Metrics.Registry
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"route": "/silent", "status": "499"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"route": "/silent", "status": "200"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"route": "/failed", "status": "499"}))
	assert.Equal(t, -1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"route": "/failed", "status": "500"}), "no 5xx series")
}

// The exposition itself is contract: names, label sets and types are what
// dashboards and alerts are written against, so lint them the way Prometheus
// would and pin the ones every service must have.
func TestMetrics_ExpositionIsLintCleanAndComplete(t *testing.T) {
	log := httpx.NewLogger(httpx.LogConfig{Level: "error", Writer: &testx.LogBuffer{}, SampleInitial: 100, SampleThereafter: 100})
	srv, err := httpx.NewServer(httpx.Config{Service: "test", Addr: ":0"}, log)
	require.NoError(t, err)
	srv.Health.SetReady(true)
	srv.Health.Register("db", func(context.Context) error { return nil })
	probe(srv)
	// Vectors only expose series that were observed: drive API requests.
	srv.Mux().HandleFunc("POST /x", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv.Mux().HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("x") })
	do(srv.Handler(), httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}")))
	get(srv.Handler(), "/boom")

	testx.LintMetrics(t, srv.Metrics.Registry)

	body := get(srv.Admin(), "/metrics").Body.String()
	for _, want := range []string{
		"# TYPE build_info gauge",
		"# TYPE http_requests_total counter",
		"# TYPE http_request_duration_seconds histogram",
		"# TYPE http_requests_in_flight gauge",
		"# TYPE http_unhandled_errors_total counter",
		"http_unhandled_errors_total 1",
		"# TYPE log_dropped_total counter",
		"# TYPE health_check_up gauge",
		`health_check_up{check="db",critical="true"} 1`,
		"# TYPE health_check_duration_seconds histogram",
		"# TYPE go_goroutines gauge",
		"# TYPE process_cpu_seconds_total counter",
	} {
		assert.Containsf(t, body, want, "exposition must contain %q", want)
	}
	for _, gone := range []string{"http_request_size_bytes", "http_response_size_bytes", "http_panics_total"} {
		assert.NotContainsf(t, body, gone, "%s is not part of the metrics contract", gone)
	}
}

func TestMetrics_AdminTrafficIsNotCounted(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	for _, path := range []string{"/healthz", "/livez", "/readyz", "/version", "/metrics", "/admin/config"} {
		get(srv.Admin(), path)
	}
	body := get(srv.Admin(), "/metrics").Body.String()
	assert.NotContains(t, body, `http_requests_total{`, "admin traffic is not API traffic")
}

// Exemplars link a metric sample to one trace: present when the request's
// span is sampled, absent otherwise.
func TestMetrics_ExemplarCarriesTheSampledTraceID(t *testing.T) {
	testx.Recorder(t) // an always-sampling global provider; not parallel-safe
	srv, _ := loggedServer(t, httpx.Config{})
	srv.Mux().HandleFunc("GET /x", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	do(srv.Handler(), req)

	var found bool
	for _, m := range series(t, srv.Metrics.Registry, "http_requests_total") {
		ex := m.GetCounter().GetExemplar()
		require.NotNil(t, ex, "a sampled request leaves an exemplar")
		for _, l := range ex.GetLabel() {
			if l.GetName() == "trace_id" && l.GetValue() == traceID {
				found = true
			}
		}
	}
	assert.True(t, found, "the exemplar is the request's trace id")
}

func TestMetrics_NoExemplarWithoutASampledSpan(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	srv.Mux().HandleFunc("GET /x", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	get(srv.Handler(), "/x")
	for _, m := range series(t, srv.Metrics.Registry, "http_requests_total") {
		assert.Nil(t, m.GetCounter().GetExemplar())
	}
}

// Metrics.Middleware works outside a Server chain too, labelling the route
// "unmatched" since nothing routed the request.
func TestMetrics_MiddlewareStandalone(t *testing.T) {
	m := httpx.NewMetrics(httpx.Build("t"), nil)
	h := m.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	get(h, "/")
	assert.Equal(t, 1.0, testx.Metric(t, m.Registry, "http_requests_total", map[string]string{"route": "unmatched", "status": "202"}))
	assert.Equal(t, -1.0, testx.Metric(t, m.Registry, "log_dropped_total", nil), "no sampling logger, no drop counter")
}
