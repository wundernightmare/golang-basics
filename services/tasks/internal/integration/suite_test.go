package integration_test

// The vertical as one testo suite with the Allure plugin: every stage of a
// task's life is a step in the report with the HTTP bodies attached, and the
// same run checks that the three signals — the trace, the access-log line,
// the metrics — describe what happened. Wired exactly as main.go wires it,
// against the package's shared Postgres / Valkey / Kafka.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/testo"
	allure "github.com/ozontech/testo-allure"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/contracts/tasksapi"
	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
)

func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }

type Suite struct{ testo.Suite[testx.T] }

func TestTasks(t *testing.T) {
	testo.RunSuite(t, new(Suite), testx.Options("tasks", "integration", "testcontainers", testx.Meta{
		Epic: "golang-basics", Feature: "tasks API", Owner: "@team-platform",
	})...)
}

// wired is the service as main.go assembles it, on top of the shared stack.
type wired struct {
	store *store.Store
	srv   *httpx.Server
	ts    *httptest.Server
	log   *testx.LogBuffer
	topic string
}

// wire builds the service exactly like main.go (tracing middleware outside
// the access log, optional checks for cache and broker, lib collectors on the
// registry) with the logger into a buffer. Everything is created on the
// test's own t: a step's Cleanup would tear it down when the step returns.
func wire(t testx.T) wired {
	t.Helper()
	return wireWith(t, deps{db: testx.Postgres(t), valkey: testx.Valkey(t), brokers: testx.Kafka(t)})
}

// deps are the addresses the service is wired to — the shared containers, or
// (chaos suite) Toxiproxy in front of them.
type deps struct {
	db      string
	valkey  string
	brokers []string
}

func wireWith(t testx.T, d deps) wired {
	t.Helper()
	ctx := context.Background()
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Service: "tasks", Level: "info", Format: "json", Writer: buf})
	topic := testx.Unique("tasks.events")

	db, err := pgx.New(ctx, pgx.Config{URL: d.db, ConnectTimeout: 5 * time.Second, MaxConns: 5}, log)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	st := store.New(db)
	require.NoError(t, st.Migrate(ctx))

	cache, err := valkey.New(valkey.Config{URL: d.valkey, DialTimeout: 5 * time.Second}, log)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	producer, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: d.brokers, Topic: topic, ClientID: testx.Unique("tasks-it"), DialTimeout: 10 * time.Second,
	}, log)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	// HealthInterval short so the chaos suite sees the checks re-evaluate;
	// the end-to-end suite reads the cached results just the same.
	srv := httpx.NewServer(httpx.Config{Service: "tasks", Addr: ":0", HealthInterval: 300 * time.Millisecond, HealthTimeout: 2 * time.Second}, log,
		httpx.WithMiddleware(otelx.GinMiddleware("tasks")))
	srv.Health.Register("postgres", db.ReadyCheck())
	srv.Health.Register("valkey", cache.ReadyCheck(), httpx.Optional())
	srv.Health.Register("kafka", producer.ReadyCheck(), httpx.Optional())
	srv.Metrics.Registry.MustRegister(db.Collectors()...)
	srv.Metrics.Registry.MustRegister(cache.Collectors()...)
	srv.Metrics.Registry.MustRegister(producer.Collectors()...)
	srv.Health.SetReady(true)
	hctx, stopHealth := context.WithCancel(ctx)
	t.Cleanup(stopHealth)
	go srv.Health.Run(hctx)
	api.Register(srv, api.Deps{Store: st, Cache: cache, Publisher: producer, Topic: topic, CacheTTL: time.Minute, Logger: log})
	ts := httptest.NewServer(srv.Engine())
	t.Cleanup(ts.Close)
	return wired{store: st, srv: srv, ts: ts, log: buf, topic: topic}
}

// readyz returns the admin listener's readiness status and body.
func readyz(t testing.TB, srv *httpx.Server) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rec.Code, rec.Body.String()
}

func (Suite) TestEndToEnd(t testx.T) {
	testx.Case(t, "GB-101", "create, read, list, delete a task") // sample TestOps id — replace with your project\'s
	t.Title("a task lives through create → cached read → Postgres → Kafka → list → delete, and the signals agree")
	t.Description("HTTP → Postgres → Valkey → Kafka against real containers, wired exactly as main.go does it; " +
		"one trace across the request, its log lines and the metrics.")
	t.Severity(allure.SeverityCritical)

	sr := testx.Recorder(t) // before wiring: the instrumentation captures the provider then
	w := wire(t)
	var id string

	testx.Step(t, "POST /tasks creates the task", func(t testx.T) {
		created := postJSON(t, w.ts, "/tasks", `{"title":"ship it"}`, http.StatusCreated)
		attachJSON(t, "response", created)
		id, _ = created["id"].(string)
		t.Require().NotEmpty(id)
		t.Assert().Equal("ship it", created["title"])
	})

	testx.Step(t, "GET /tasks/{id} is served from the cache warmed on create", func(t testx.T) {
		resp := get(t, w.ts, "/tasks/"+id)
		defer func() { _ = resp.Body.Close() }()
		t.Require().Equal(http.StatusOK, resp.StatusCode)
		t.Assert().Equal("hit", resp.Header.Get("X-Cache"))
	})

	testx.Step(t, "the row is in Postgres", func(t testx.T) {
		stored, err := w.store.Get(context.Background(), id)
		t.Require().NoError(err)
		t.Assert().Equal("ship it", stored.Title)
	})

	testx.Step(t, "the task.created event reached Kafka", func(t testx.T) {
		requireEventDelivered(t, testx.Kafka(t), w.topic, id)
	})

	testx.Step(t, "the POST is one trace: server span, INSERT, cache SET, Kafka publish", func(t testx.T) {
		var post trace.SpanContext
		for _, sp := range sr.Ended() {
			if sp.Name() == "POST /tasks" {
				post = sp.SpanContext()
			}
		}
		t.Require().True(post.IsValid(), "server span for the POST")
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
		t.Assert().Equal(map[string]bool{"insert": true, "cache-set": true, "publish": true}, kinds)

		line := w.log.Find(t, map[string]any{"msg": "request", "route": "/tasks", "method": "POST"})
		t.Require().NotNil(line, "access-log line for the POST")
		t.Assert().Equal(post.TraceID().String(), line["trace_id"], "the access-log line names the trace")
		t.Assert().Equal(post.SpanID().String(), line["span_id"])
		t.Assert().Equal(float64(201), line["status"])
	})

	testx.Step(t, "metrics count exactly what happened and are lint-clean", func(t testx.T) {
		reg := w.srv.Metrics.Registry
		t.Assert().Equal(1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "POST", "path": "/tasks", "status": "201"}))
		t.Assert().Equal(1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "GET", "path": "/tasks/:id", "status": "200"}))
		t.Assert().Equal(1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "hit"}))
		t.Assert().Equal(1.0, testx.Metric(t, reg, "kafka_producer_records_total", map[string]string{"topic": w.topic, "result": "ok"}))
		t.Assert().Greater(testx.Metric(t, reg, "pgxpool_acquire_total", nil), 0.0)
		t.Assert().Equal(0.0, testx.Metric(t, reg, "http_requests_in_flight", nil))
		testx.LintMetrics(t, reg)
	})

	testx.Step(t, "/readyz is ready and health_check_up agrees per dependency", func(t testx.T) {
		code, body := readyz(t, w.srv)
		t.Require().Equal(http.StatusOK, code)
		t.Attach("readyz", allure.Bytes(body).As(allure.DocumentJSON))
		t.Assert().Contains(body, `"status":"ready"`)
		for c, crit := range map[string]string{"postgres": "true", "valkey": "false", "kafka": "false"} {
			t.Assert().Equal(1.0, testx.Metric(t, w.srv.Metrics.Registry, "health_check_up", map[string]string{"check": c, "critical": crit}), c)
		}
	})

	testx.Step(t, "GET /tasks lists it", func(t testx.T) {
		resp := get(t, w.ts, "/tasks")
		defer func() { _ = resp.Body.Close() }()
		t.Require().Equal(http.StatusOK, resp.StatusCode)
		var body tasksapi.TaskList
		t.Require().NoError(json.NewDecoder(resp.Body).Decode(&body))
		attachJSON(t, "response", body)
		ids := make([]string, 0, len(body.Tasks))
		for _, task := range body.Tasks {
			ids = append(ids, task.Id)
		}
		t.Assert().Contains(ids, id)
	})

	testx.Step(t, "DELETE /tasks/{id}, then GET is a 404 problem", func(t testx.T) {
		del := do(t, w.ts, http.MethodDelete, "/tasks/"+id)
		_ = del.Body.Close()
		t.Require().Equal(http.StatusNoContent, del.StatusCode)

		// The effect: the task is gone. The shape of the 404 is the contract's
		// business (OpenAPI validation in the api tests, Schemathesis end to end).
		missing := get(t, w.ts, "/tasks/"+id)
		_ = missing.Body.Close()
		t.Assert().Equal(http.StatusNotFound, missing.StatusCode)
		_, err := w.store.Get(context.Background(), id)
		t.Assert().ErrorIs(err, domain.ErrNotFound, "the row is gone from Postgres")
	})
}

func attachJSON(t testx.T, name string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	t.Require().NoError(err)
	t.Attach(name, allure.Bytes(b).As(allure.DocumentJSON))
}
