package integration_test

// The end-to-end vertical as a testo suite with the Allure plugin: every
// stage of a task's life is a step in the report, with the HTTP bodies
// attached, so a failure in CI reads as "which step, with which response"
// instead of a line number. Same containers, same assertions as before.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ozontech/testo"
	allure "github.com/ozontech/testo-allure"
	"github.com/ozontech/testo/testoplugin"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
)

type T = struct {
	*testo.T
	*allure.PluginAllure
}

func allureOptions() []testoplugin.Option {
	opts := []testoplugin.Option{allure.WithTags("tasks", "integration", "testcontainers")}
	if dir := os.Getenv("ALLURE_RESULTS_DIR"); dir != "" {
		opts = append(opts, allure.WithOutputDir(dir))
	}
	return opts
}

type Suite struct{ testo.Suite[T] }

func TestTasks(t *testing.T) { testo.RunSuite(t, new(Suite), allureOptions()...) }

// wired is the service as main.go assembles it, on top of a live stack.
type wired struct {
	store *store.Store
	ts    *httptest.Server
}

func wire(t T, s stack) wired {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)

	db, err := pgx.New(ctx, pgx.Config{URL: s.dbURL, ConnectTimeout: 5 * time.Second, MaxConns: 5}, log)
	t.Require().NoError(err)
	t.Cleanup(db.Close)
	st := store.New(db)
	t.Require().NoError(st.Migrate(ctx))

	cache, err := valkey.New(valkey.Config{URL: s.valkeyURL, DialTimeout: 5 * time.Second}, log)
	t.Require().NoError(err)
	t.Cleanup(cache.Close)

	producer, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: s.brokers, Topic: topic, ClientID: "tasks-it", DialTimeout: 10 * time.Second,
	}, log)
	t.Require().NoError(err)
	t.Cleanup(producer.Close)

	srv := httpx.NewServer(httpx.Config{Service: "tasks", LogLevel: "error"}, log)
	api.Register(srv, api.Deps{
		Store: st, Cache: cache, Publisher: producer,
		Topic: topic, CacheTTL: time.Minute, Logger: log,
	})
	ts := httptest.NewServer(srv.Engine())
	t.Cleanup(ts.Close)
	return wired{store: st, ts: ts}
}

func (Suite) TestEndToEnd(t T) {
	t.Title("a task lives through create → cached read → Postgres → Kafka → list → delete")
	t.Description("HTTP → Postgres → Valkey → Kafka against real containers, wired exactly as main.go does it.")
	t.Feature("tasks")
	t.Severity(allure.SeverityCritical)

	// Containers and clients are created on the test's own T, not inside a
	// step: a step is a sub-test, and its t.Cleanup would tear them down the
	// moment the step returns.
	s := bringUp(t)
	w := wire(t, s)
	var id string

	allure.Step(t, "POST /tasks creates the task", func(t T) {
		created := postJSON(t, w.ts, "/tasks", `{"title":"ship it"}`, http.StatusCreated)
		attachJSON(t, "response", created)
		id, _ = created["id"].(string)
		t.Require().NotEmpty(id)
		t.Assert().Equal("ship it", created["title"])
	})

	allure.Step(t, "GET /tasks/{id} is served from the cache warmed on create", func(t T) {
		resp := get(t, w.ts, "/tasks/"+id)
		defer func() { _ = resp.Body.Close() }()
		t.Require().Equal(http.StatusOK, resp.StatusCode)
		t.Assert().Equal("hit", resp.Header.Get("X-Cache"))
	})

	allure.Step(t, "the row is in Postgres", func(t T) {
		stored, err := w.store.Get(context.Background(), id)
		t.Require().NoError(err)
		t.Assert().Equal("ship it", stored.Title)
	})

	allure.Step(t, "the task.created event reached Kafka", func(t T) {
		requireEventDelivered(t, s.brokers, id)
	})

	allure.Step(t, "GET /tasks lists it", func(t T) {
		resp := get(t, w.ts, "/tasks")
		defer func() { _ = resp.Body.Close() }()
		t.Require().Equal(http.StatusOK, resp.StatusCode)
		var body struct {
			Tasks []domain.Task `json:"tasks"`
		}
		t.Require().NoError(json.NewDecoder(resp.Body).Decode(&body))
		attachJSON(t, "response", body)
		t.Assert().Len(body.Tasks, 1)
	})

	allure.Step(t, "DELETE /tasks/{id}, then GET is a 404 problem", func(t T) {
		del := do(t, w.ts, http.MethodDelete, "/tasks/"+id)
		_ = del.Body.Close()
		t.Require().Equal(http.StatusNoContent, del.StatusCode)

		missing := get(t, w.ts, "/tasks/"+id)
		defer func() { _ = missing.Body.Close() }()
		t.Assert().Equal(http.StatusNotFound, missing.StatusCode)
		t.Assert().Equal(httpx.ProblemContentType, missing.Header.Get("Content-Type"))
	})
}

func attachJSON(t T, name string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	t.Require().NoError(err)
	t.Attach(name, allure.Bytes(b).As(allure.DocumentJSON))
}
