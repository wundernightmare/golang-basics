package api_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// --- fakes -------------------------------------------------------------------

type fakeStore struct {
	tasks    map[string]domain.Task
	getErr   error
	getCalls int
}

func newFakeStore() *fakeStore { return &fakeStore{tasks: map[string]domain.Task{}} }

func (f *fakeStore) Create(_ context.Context, t domain.Task) error { f.tasks[t.ID] = t; return nil }
func (f *fakeStore) Get(_ context.Context, id string) (domain.Task, error) {
	f.getCalls++
	if f.getErr != nil {
		return domain.Task{}, f.getErr
	}
	t, ok := f.tasks[id]
	if !ok {
		return domain.Task{}, domain.ErrNotFound
	}
	return t, nil
}
func (f *fakeStore) List(_ context.Context, _ int) ([]domain.Task, error) {
	out := make([]domain.Task, 0, len(f.tasks))
	for _, t := range f.tasks {
		out = append(out, t)
	}
	return out, nil
}
func (f *fakeStore) Delete(_ context.Context, id string) error {
	if _, ok := f.tasks[id]; !ok {
		return domain.ErrNotFound
	}
	delete(f.tasks, id)
	return nil
}

type fakeCache struct {
	data map[string]string
}

func newFakeCache() *fakeCache { return &fakeCache{data: map[string]string{}} }

func (f *fakeCache) Get(_ context.Context, key string) (string, bool, error) {
	v, ok := f.data[key]
	return v, ok, nil
}
func (f *fakeCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.data[key] = value
	return nil
}
func (f *fakeCache) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(f.data, k)
	}
	return nil
}

type fakePublisher struct {
	published [][]byte
	topic     string
}

func (f *fakePublisher) Publish(_ context.Context, topic string, _, value []byte) error {
	f.topic = topic
	f.published = append(f.published, value)
	return nil
}

// --- harness -----------------------------------------------------------------

func newServer(t *testing.T) (*httptest.Server, *fakeStore, *fakeCache, *fakePublisher) {
	t.Helper()
	st, cache, pub := newFakeStore(), newFakeCache(), &fakePublisher{}
	srv := httpx.NewServer(httpx.Config{LogLevel: "error"}, slog.New(slog.DiscardHandler))
	api.Register(srv, api.Deps{
		Store: st, Cache: cache, Publisher: pub,
		Topic: "tasks.events", CacheTTL: time.Minute,
		Logger: slog.New(slog.DiscardHandler),
	})
	ts := httptest.NewServer(srv.Engine())
	t.Cleanup(ts.Close)
	return ts, st, cache, pub
}

func do(t *testing.T, ts *httptest.Server, method, path, body string) *http.Response {
	t.Helper()
	var r *http.Request
	var err error
	if body == "" {
		r, err = http.NewRequest(method, ts.URL+path, nil)
	} else {
		r, err = http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	}
	require.NoError(t, err)
	resp, err := ts.Client().Do(r)
	require.NoError(t, err)
	return resp
}

// --- tests -------------------------------------------------------------------

func TestCreateValidationRejectsEmptyTitle(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := do(t, ts, http.MethodPost, "/tasks", `{"title":""}`)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, httpx.ProblemContentType, resp.Header.Get("Content-Type"))
	var p map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&p))
	require.Equal(t, float64(400), p["status"])
}

func TestCreatePersistsPublishesAndCaches(t *testing.T) {
	ts, st, cache, pub := newServer(t)
	resp := do(t, ts, http.MethodPost, "/tasks", `{"title":"write tests"}`)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var task domain.Task
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&task))
	require.NotEmpty(t, task.ID)
	require.Equal(t, "write tests", task.Title)

	require.Len(t, st.tasks, 1, "task persisted to store")
	require.Len(t, pub.published, 1, "one task.created event published")
	require.Equal(t, "tasks.events", pub.topic)

	var evt domain.TaskCreatedEvent
	require.NoError(t, json.Unmarshal(pub.published[0], &evt))
	require.Equal(t, task.ID, evt.ID)

	_, cached := cache.data["task:"+task.ID]
	require.True(t, cached, "task warmed into cache on create")
}

func TestGetServesFromCacheWithoutHittingStore(t *testing.T) {
	ts, st, cache, _ := newServer(t)
	cached := domain.Task{ID: "abc", Title: "cached", CreatedAt: time.Now().UTC()}
	body, _ := json.Marshal(cached)
	cache.data["task:abc"] = string(body)

	resp := do(t, ts, http.MethodGet, "/tasks/abc", "")
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "hit", resp.Header.Get("X-Cache"))
	require.Equal(t, 0, st.getCalls, "a cache hit must not touch the store")
}

func TestGetMissesCacheThenLoadsFromStore(t *testing.T) {
	ts, st, _, _ := newServer(t)
	st.tasks["xyz"] = domain.Task{ID: "xyz", Title: "stored", CreatedAt: time.Now().UTC()}

	resp := do(t, ts, http.MethodGet, "/tasks/xyz", "")
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "miss", resp.Header.Get("X-Cache"))
	require.Equal(t, 1, st.getCalls)
}

func TestGetUnknownReturns404Problem(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := do(t, ts, http.MethodGet, "/tasks/nope", "")
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, httpx.ProblemContentType, resp.Header.Get("Content-Type"))
	var p map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&p))
	require.Equal(t, "task_not_found", p["code"])
	require.Equal(t, "/tasks/nope", p["instance"])
}

func TestDeleteUnknownReturns404(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := do(t, ts, http.MethodDelete, "/tasks/ghost", "")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDeleteEvictsCache(t *testing.T) {
	ts, st, cache, _ := newServer(t)
	st.tasks["d1"] = domain.Task{ID: "d1", Title: "doomed"}
	cache.data["task:d1"] = "{}"

	resp := do(t, ts, http.MethodDelete, "/tasks/d1", "")
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NotContains(t, st.tasks, "d1")
	require.NotContains(t, cache.data, "task:d1", "cache entry evicted on delete")
}
