package httpx_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// loggedServer builds a server from cfg whose JSON log lands in the returned
// buffer. Service defaults to "test" and, when neither listener is set, the
// API listener to ":0" so the config validates.
func loggedServer(t testing.TB, cfg httpx.Config, opts ...httpx.Option) (*httpx.Server, *testx.LogBuffer) {
	t.Helper()
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: buf})
	if cfg.Service == "" {
		cfg.Service = "test"
	}
	if cfg.Addr == "" && cfg.AdminAddr == "" {
		cfg.Addr = ":0"
	}
	srv, err := httpx.NewServer(cfg, log, opts...)
	require.NoError(t, err)
	return srv, buf
}

// get serves a GET for target on h.
func get(h http.Handler, target string) *httptest.ResponseRecorder {
	return do(h, httptest.NewRequest(http.MethodGet, target, nil))
}

// decode parses a JSON object body.
func decode(t testing.TB, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m), rec.Body.String())
	return m
}

// histogram returns the first series of the histogram name whose labels
// include every pair in labels, or nil.
func histogram(t testing.TB, g prometheus.Gatherer, name string, labels map[string]string) *dto.Histogram {
	t.Helper()
	for _, m := range series(t, g, name) {
		if hasLabels(m, labels) {
			return m.GetHistogram()
		}
	}
	return nil
}

// series returns every series of the metric family name.
func series(t testing.TB, g prometheus.Gatherer, name string) []*dto.Metric {
	t.Helper()
	mfs, err := g.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()
		}
	}
	return nil
}

func hasLabels(m *dto.Metric, labels map[string]string) bool {
	for k, v := range labels {
		found := false
		for _, l := range m.GetLabel() {
			if l.GetName() == k && l.GetValue() == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func filterMsg(lines []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}
