// Package integration exercises the whole tasks vertical — HTTP → Postgres →
// Valkey → Kafka — against real containers, proving the libs compose end to end.
// It skips under -short or when Docker is unavailable.
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
)

const topic = "tasks.events.it"

type stack struct {
	dbURL     string
	valkeyURL string
	brokers   []string
}

func bringUp(t *testing.T) stack {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}
	ctx := context.Background()
	inCI := func() bool { _, ok := os.LookupEnv("CI"); return ok }

	pg, err := startWithRetry(ctx, t, func(ctx context.Context) (*tcpostgres.PostgresContainer, error) {
		return tcpostgres.Run(ctx, "postgres:18-alpine",
			tcpostgres.WithDatabase("app"), tcpostgres.WithUsername("app"), tcpostgres.WithPassword("app"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	})
	if err != nil {
		if inCI() {
			require.NoError(t, err)
		}
		t.Skipf("docker unavailable (postgres): %v", err)
	}

	vk, err := startWithRetry(ctx, t, func(ctx context.Context) (*tcvalkey.ValkeyContainer, error) {
		// Override the module's default exec-based readiness probe with a
		// log-based one: rootless podman's `exec inspect` stalls under
		// concurrent container startup, while tailing the server log does not.
		return tcvalkey.Run(ctx, "valkey/valkey:9.0",
			testcontainers.WithWaitStrategy(wait.ForLog("Ready to accept connections").
				WithStartupTimeout(60*time.Second)))
	})
	require.NoError(t, err)

	kf, err := startWithRetry(ctx, t, func(ctx context.Context) (*tckafka.KafkaContainer, error) {
		return tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	})
	require.NoError(t, err)

	dbURL, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	vkURL, err := vk.ConnectionString(ctx)
	require.NoError(t, err)
	brokers, err := kf.Brokers(ctx)
	require.NoError(t, err)

	return stack{dbURL: dbURL, valkeyURL: vkURL, brokers: brokers}
}

func TestTasksEndToEnd(t *testing.T) {
	s := bringUp(t)
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)

	db, err := pgx.New(ctx, pgx.Config{URL: s.dbURL, ConnectTimeout: 5 * time.Second, MaxConns: 5}, log)
	require.NoError(t, err)
	defer db.Close()
	st := store.New(db)
	require.NoError(t, st.Migrate(ctx))

	cache, err := valkey.New(valkey.Config{URL: s.valkeyURL, DialTimeout: 5 * time.Second}, log)
	require.NoError(t, err)
	defer cache.Close()

	producer, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: s.brokers, Topic: topic, ClientID: "tasks-it", DialTimeout: 10 * time.Second,
	}, log)
	require.NoError(t, err)
	defer producer.Close()

	srv := httpx.NewServer(httpx.Config{LogLevel: "error"}, log)
	api.Register(srv, api.Deps{
		Store: st, Cache: cache, Publisher: producer,
		Topic: topic, CacheTTL: time.Minute, Logger: log,
	})
	ts := httptest.NewServer(srv.Engine())
	defer ts.Close()

	// --- create ---------------------------------------------------------------
	created := postJSON(t, ts, "/tasks", `{"title":"ship it"}`, http.StatusCreated)
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)
	require.Equal(t, "ship it", created["title"])

	// --- read (served from the cache warmed on create) ------------------------
	resp := get(t, ts, "/tasks/"+id)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "hit", resp.Header.Get("X-Cache"))
	_ = resp.Body.Close()

	// --- the task is actually in Postgres ------------------------------------
	stored, err := st.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "ship it", stored.Title)

	// --- the task.created event reached Kafka --------------------------------
	requireEventDelivered(t, s.brokers, id)

	// --- list -----------------------------------------------------------------
	listResp := get(t, ts, "/tasks")
	require.Equal(t, http.StatusOK, listResp.StatusCode)
	var listBody struct {
		Tasks []domain.Task `json:"tasks"`
	}
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listBody))
	_ = listResp.Body.Close()
	require.Len(t, listBody.Tasks, 1)

	// --- delete, then 404 -----------------------------------------------------
	delResp := do(t, ts, http.MethodDelete, "/tasks/"+id)
	require.Equal(t, http.StatusNoContent, delResp.StatusCode)
	_ = delResp.Body.Close()

	missing := get(t, ts, "/tasks/"+id)
	require.Equal(t, http.StatusNotFound, missing.StatusCode)
	require.Equal(t, httpx.ProblemContentType, missing.Header.Get("Content-Type"))
	_ = missing.Body.Close()
}

func requireEventDelivered(t *testing.T, brokers []string, wantID string) {
	t.Helper()
	cons, err := kafka.NewConsumer(context.Background(), kafka.Config{
		Brokers: brokers, Topics: []string{topic}, Group: "tasks-it-verify",
		ClientID: "tasks-it-verify", DialTimeout: 10 * time.Second,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer cons.Close()

	runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	found := make(chan struct{})

	go func() {
		_ = cons.Run(runCtx, func(_ context.Context, msg kafka.Message) error {
			var evt domain.TaskCreatedEvent
			if json.Unmarshal(msg.Value, &evt) == nil && evt.ID == wantID {
				select {
				case <-found:
				default:
					close(found)
				}
			}
			return nil
		})
	}()

	select {
	case <-found:
	case <-runCtx.Done():
		t.Fatal("task.created event was not delivered to Kafka within the timeout")
	}
}

// startWithRetry runs a container factory up to three times. Rootless podman's
// API socket can momentarily stall its container-inspect calls when several
// containers come up at once, surfacing as a "context deadline exceeded" on
// start; a real Docker daemon starts first-try, so the retry is a harmless
// belt-and-braces for constrained CI hosts.
func startWithRetry[T testcontainers.Container](
	ctx context.Context, t *testing.T, run func(context.Context) (T, error),
) (T, error) {
	t.Helper()
	var c T
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		c, err = run(ctx)
		if err == nil {
			c := c
			t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
			return c, nil
		}
		t.Logf("container start attempt %d/3 failed: %v", attempt, err)
		func() { defer func() { _ = recover() }(); _ = testcontainers.TerminateContainer(c) }()
		time.Sleep(2 * time.Second)
	}
	return c, err
}

// --- tiny HTTP helpers -------------------------------------------------------

func postJSON(t *testing.T, ts *httptest.Server, path, body string, wantStatus int) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, wantStatus, resp.StatusCode)
	var m map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))
	return m
}

func get(t *testing.T, ts *httptest.Server, path string) *http.Response {
	t.Helper()
	resp, err := ts.Client().Get(ts.URL + path)
	require.NoError(t, err)
	return resp
}

func do(t *testing.T, ts *httptest.Server, method, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	return resp
}
