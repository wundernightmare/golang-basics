package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// One request through the whole vertical, wired exactly as main.go does it
// (real Postgres / Valkey / Kafka, real SDK, real logger to a buffer): the
// server span, the INSERT, the cache SET, the Kafka publish are one trace;
// the access-log line names that trace; /metrics and /readyz agree.
func TestTelemetryIsCoherentAcrossTheVertical(t *testing.T) {
	s := bringUp(t)
	sr := installRecorder(t)
	ctx := context.Background()

	buf := &syncBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Service: "tasks", Level: "info", Format: "json", Writer: buf})

	db, err := pgx.New(ctx, pgx.Config{URL: s.dbURL, ConnectTimeout: 5 * time.Second, MaxConns: 5}, log)
	require.NoError(t, err)
	defer db.Close()
	st := store.New(db)
	require.NoError(t, st.Migrate(ctx))
	cache, err := valkey.New(valkey.Config{URL: s.valkeyURL, DialTimeout: 5 * time.Second}, log)
	require.NoError(t, err)
	defer cache.Close()
	producer, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: s.brokers, Topic: topic, ClientID: "tasks-telemetry", DialTimeout: 10 * time.Second,
	}, log)
	require.NoError(t, err)
	defer producer.Close()

	srv := httpx.NewServer(httpx.Config{Service: "tasks", Addr: ":0", HealthInterval: time.Hour}, log,
		httpx.WithMiddleware(otelx.GinMiddleware("tasks")))
	srv.Health.Register("postgres", db.ReadyCheck())
	srv.Health.Register("valkey", cache.ReadyCheck(), httpx.Optional())
	srv.Health.Register("kafka", producer.ReadyCheck(), httpx.Optional())
	srv.Metrics.Registry.MustRegister(db.Collectors()...)
	srv.Metrics.Registry.MustRegister(cache.Collectors()...)
	srv.Metrics.Registry.MustRegister(producer.Collectors()...)
	srv.Health.SetReady(true)
	api.Register(srv, api.Deps{Store: st, Cache: cache, Publisher: producer, Topic: topic, CacheTTL: time.Minute, Logger: log})
	ts := httptest.NewServer(srv.Engine())
	defer ts.Close()

	// --- one create, one cached read ----------------------------------------
	created := postJSON(t, ts, "/tasks", `{"title":"traced"}`, http.StatusCreated)
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)
	resp := get(t, ts, "/tasks/"+id)
	require.Equal(t, "hit", resp.Header.Get("X-Cache"))
	_ = resp.Body.Close()

	// --- traces: everything the POST did is one trace -----------------------
	var post trace.SpanContext
	for _, sp := range sr.Ended() {
		if sp.Name() == "POST /tasks" {
			post = sp.SpanContext()
		}
	}
	require.True(t, post.IsValid(), "server span for the POST")
	kinds := map[string]bool{}
	for _, sp := range sr.Ended() {
		if sp.SpanContext().TraceID() != post.TraceID() || sp.SpanKind() == trace.SpanKindServer {
			continue
		}
		name := strings.ToUpper(sp.Name())
		switch {
		case strings.HasPrefix(name, "INSERT"):
			kinds["insert"] = true
		case strings.HasPrefix(name, "SET"):
			kinds["cache-set"] = true
		case strings.HasSuffix(name, " PUBLISH"):
			kinds["publish"] = true
		}
	}
	require.Equal(t, map[string]bool{"insert": true, "cache-set": true, "publish": true}, kinds,
		"DB, cache and broker spans are children of the request trace")

	// --- logs: the access-log line names the same trace ---------------------
	var accessLines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m), line)
		if m["msg"] == "request" && m["route"] == "/tasks" && m["method"] == "POST" {
			accessLines = append(accessLines, m)
		}
	}
	require.Len(t, accessLines, 1)
	require.Equal(t, post.TraceID().String(), accessLines[0]["trace_id"])
	require.Equal(t, post.SpanID().String(), accessLines[0]["span_id"])
	require.Equal(t, float64(201), accessLines[0]["status"])
	require.Equal(t, "tasks", accessLines[0]["service"])

	// --- metrics: exactly what happened, lint-clean --------------------------
	reg := srv.Metrics.Registry
	require.Equal(t, 1.0, metricValue(t, reg, "http_requests_total", map[string]string{"method": "POST", "path": "/tasks", "status": "201"}))
	require.Equal(t, 1.0, metricValue(t, reg, "http_requests_total", map[string]string{"method": "GET", "path": "/tasks/:id", "status": "200"}))
	require.Equal(t, 1.0, metricValue(t, reg, "cache_lookups_total", map[string]string{"result": "hit"}))
	require.Equal(t, 1.0, metricValue(t, reg, "kafka_producer_records_total", map[string]string{"topic": topic, "result": "ok"}))
	require.Greater(t, metricValue(t, reg, "pgxpool_acquire_total", nil), 0.0)
	require.Equal(t, 0.0, metricValue(t, reg, "http_requests_in_flight", nil))
	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	require.Empty(t, problems)

	// --- readiness: the checks ran for real and the metric agrees -----------
	rec := httptest.NewRecorder()
	srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"status":"ready"`)
	for _, c := range []string{"postgres", "valkey", "kafka"} {
		crit := "false"
		if c == "postgres" {
			crit = "true"
		}
		require.Equal(t, 1.0, metricValue(t, reg, "health_check_up", map[string]string{"check": c, "critical": crit}), c)
	}
}
