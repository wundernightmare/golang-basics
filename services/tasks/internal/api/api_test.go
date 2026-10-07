package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/contracts/tasksapi"
	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/contract"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// --- harness -----------------------------------------------------------------

type harness struct {
	ts        *httptest.Server
	store     *fakeStore
	cache     *fakeCache
	log       *testx.LogBuffer
	committed atomic.Int32
	oapi      *contract.OpenAPI
}

func newHarness(t testing.TB) *harness {
	t.Helper()
	h := &harness{store: newFakeStore(), cache: newFakeCache(), log: &testx.LogBuffer{}}
	logger := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: h.log})
	srv, err := httpx.NewServer(httpx.Config{Service: "tasks", Addr: ":0", AdminAddr: ""}, logger)
	require.NoError(t, err)
	api.Register(srv, api.Deps{
		Store: h.store, Cache: h.cache, Logger: logger,
		Committed: func() { h.committed.Add(1) },
	})
	h.ts = httptest.NewServer(srv.Handler())
	t.Cleanup(h.ts.Close)
	h.oapi = contract.LoadOpenAPI(t, "openapi3/tasks.openapi.yaml")
	return h
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) json(t testing.TB, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(r.body, v), string(r.body))
}

func (r response) problem(t testing.TB) map[string]any {
	t.Helper()
	assert.Equal(t, httpx.ProblemContentType, r.header.Get("Content-Type"))
	var p map[string]any
	r.json(t, &p)
	return p
}

// do sends one request and checks the exchange against the OpenAPI document:
// the request must match an operation and the response (status, headers,
// body) must conform to it.
func (h *harness) do(t testing.TB, method, path, body string, headers ...string) response {
	t.Helper()
	resp := h.send(t, method, path, body, headers...)
	req, err := http.NewRequest(method, h.ts.URL+path, nil)
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	h.oapi.Validate(t, req, []byte(body), resp.status, resp.header, resp.body)
	return resp
}

// send is do without the contract check — for the undocumented 5xx.
func (h *harness) send(t testing.TB, method, path, body string, headers ...string) response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, h.ts.URL+path, rd)
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := h.ts.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return response{status: resp.StatusCode, header: resp.Header, body: bytes.TrimSpace(raw)}
}

func (h *harness) create(t testing.TB, title string) tasksapi.Task {
	t.Helper()
	r := h.do(t, http.MethodPost, "/tasks", `{"title":"`+title+`"}`)
	require.Equal(t, http.StatusCreated, r.status, string(r.body))
	var task tasksapi.Task
	r.json(t, &task)
	return task
}

// --- create ------------------------------------------------------------------
//
// Not here: every "malformed input → 4xx in the right shape" case the schema
// already states; Schemathesis generates those (`just schemathesis tasks`).
// These tests own what the schema cannot express — the business codes, the
// trimming, idempotency, preconditions, the cache — and every exchange is
// still validated against the contract.

func TestCreate_PersistsWarmsTheCacheAndAnswersWithETag(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, http.MethodPost, "/tasks", `{"title":"  write tests  "}`)
	require.Equal(t, http.StatusCreated, r.status)

	var task tasksapi.Task
	r.json(t, &task)
	assert.NotEmpty(t, task.Id)
	assert.Equal(t, "write tests", task.Title, "surrounding whitespace is trimmed")
	assert.Equal(t, int64(1), task.Version)
	assert.Equal(t, `"1"`, r.header.Get("ETag"))
	assert.Equal(t, "/tasks/"+task.Id, r.header.Get("Location"))

	assert.Contains(t, h.store.snapshot(), task.Id, "persisted")
	assert.True(t, h.cache.has(task.Id), "warmed into the cache")
	assert.Equal(t, int32(1), h.committed.Load(), "the relay is woken for the outbox event")
}

func TestCreate_RejectsTitlesTheStoreWillNotHold(t *testing.T) {
	h := newHarness(t)
	for name, title := range map[string]string{
		"blank":             `   `,
		"too long":          strings.Repeat("x", domain.MaxTitleRunes+1),
		"NUL":               `a\u0000b`,
		"control character": `a\u0007b`,
	} {
		t.Run(name, func(t *testing.T) {
			r := h.do(t, http.MethodPost, "/tasks", `{"title":"`+title+`"}`)
			require.Equal(t, http.StatusBadRequest, r.status)
			assert.Equal(t, "invalid_title", r.problem(t)["code"])
		})
	}
	assert.Empty(t, h.store.snapshot(), "nothing persisted")
	assert.Equal(t, int32(0), h.committed.Load())
}

func TestCreate_IdempotencyKeyReplaysTheFirstResult(t *testing.T) {
	h := newHarness(t)
	first := h.do(t, http.MethodPost, "/tasks", `{"title":"pay rent"}`, "Idempotency-Key", "k-1")
	require.Equal(t, http.StatusCreated, first.status)
	var created tasksapi.Task
	first.json(t, &created)

	// A retry — even with different whitespace — is the same request.
	replay := h.do(t, http.MethodPost, "/tasks", `{"title":" pay rent "}`, "Idempotency-Key", "k-1")
	require.Equal(t, http.StatusOK, replay.status)
	assert.Equal(t, "true", replay.header.Get("Idempotent-Replayed"))
	assert.Equal(t, `"1"`, replay.header.Get("ETag"))
	var again tasksapi.Task
	replay.json(t, &again)
	assert.Equal(t, created.Id, again.Id, "the same task, not a second one")
	assert.Len(t, h.store.snapshot(), 1)
	assert.Equal(t, int32(1), h.committed.Load(), "a replay commits nothing")

	reused := h.do(t, http.MethodPost, "/tasks", `{"title":"pay taxes"}`, "Idempotency-Key", "k-1")
	require.Equal(t, http.StatusUnprocessableEntity, reused.status)
	assert.Equal(t, "idempotency_key_reused", reused.problem(t)["code"])

	other := h.do(t, http.MethodPost, "/tasks", `{"title":"pay rent"}`, "Idempotency-Key", "k-2")
	assert.Equal(t, http.StatusCreated, other.status, "another key is another request")
}

func TestCreate_InvalidIdempotencyKey(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, http.MethodPost, "/tasks", `{"title":"x"}`, "Idempotency-Key", strings.Repeat("k", 256))
	require.Equal(t, http.StatusBadRequest, r.status)
	assert.Equal(t, "invalid_idempotency_key", r.problem(t)["code"])
	assert.Empty(t, h.store.snapshot())
}

// --- get ---------------------------------------------------------------------

func TestGet_ServesFromCacheWithoutTouchingTheStore(t *testing.T) {
	h := newHarness(t)
	h.cache.Warm(context.Background(), domain.Task{ID: "abc", Title: "cached", Version: 4, CreatedAt: time.Now()})

	r := h.do(t, http.MethodGet, "/tasks/abc", "")
	require.Equal(t, http.StatusOK, r.status)
	assert.Equal(t, "hit", r.header.Get("X-Cache"))
	assert.Equal(t, `"4"`, r.header.Get("ETag"))
	assert.Equal(t, 0, h.store.getCalls, "a cache hit must not touch the store")
}

func TestGet_MissLoadsFromTheStore(t *testing.T) {
	h := newHarness(t)
	h.store.put(domain.Task{ID: "xyz", Title: "stored", CreatedAt: time.Now()})

	r := h.do(t, http.MethodGet, "/tasks/xyz", "")
	require.Equal(t, http.StatusOK, r.status)
	assert.Equal(t, "miss", r.header.Get("X-Cache"))
	assert.Equal(t, 1, h.store.getCalls)

	r = h.do(t, http.MethodGet, "/tasks/xyz", "")
	assert.Equal(t, "hit", r.header.Get("X-Cache"), "filled on the miss")
}

func TestGet_UnknownIsA404ProblemWithACode(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, http.MethodGet, "/tasks/nope", "")
	require.Equal(t, http.StatusNotFound, r.status)
	p := r.problem(t)
	assert.Equal(t, "task_not_found", p["code"])
	assert.Equal(t, "/tasks/nope", p["instance"])
	assert.NotEmpty(t, p["request_id"])
}

// --- list --------------------------------------------------------------------

func TestList_KeysetPagesChainAndEnd(t *testing.T) {
	h := newHarness(t)
	var ids []string
	for _, title := range []string{"a", "b", "c", "d", "e"} {
		ids = append(ids, h.create(t, title).Id)
	}

	var seen []string
	path := "/tasks?limit=2"
	for pages := 0; ; pages++ {
		require.Less(t, pages, 5, "the pages must end")
		r := h.do(t, http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, r.status)
		var page tasksapi.TaskList
		r.json(t, &page)
		assert.LessOrEqual(t, len(page.Tasks), 2)
		for _, task := range page.Tasks {
			seen = append(seen, task.Id)
		}
		if page.NextCursor == nil {
			break
		}
		path = "/tasks?limit=2&cursor=" + *page.NextCursor
	}
	assert.Equal(t, []string{ids[4], ids[3], ids[2], ids[1], ids[0]}, seen, "newest first, each task once")

	r := h.do(t, http.MethodGet, "/tasks", "")
	var all tasksapi.TaskList
	r.json(t, &all)
	assert.Len(t, all.Tasks, 5, "default page size covers them")
	assert.Nil(t, all.NextCursor)
}

func TestList_RejectsBadLimitsAndForeignCursors(t *testing.T) {
	h := newHarness(t)
	for _, q := range []string{"limit=0", "limit=201", "limit=ten"} {
		r := h.do(t, http.MethodGet, "/tasks?"+q, "")
		require.Equal(t, http.StatusBadRequest, r.status, q)
		assert.Equal(t, "invalid_limit", r.problem(t)["code"], q)
	}
	r := h.do(t, http.MethodGet, "/tasks?cursor=not-a-cursor", "")
	require.Equal(t, http.StatusBadRequest, r.status)
	assert.Equal(t, "invalid_cursor", r.problem(t)["code"])
}

// --- update ------------------------------------------------------------------

func TestUpdate_ChangesBumpsTheVersionAndInvalidates(t *testing.T) {
	h := newHarness(t)
	task := h.create(t, "draft")
	require.True(t, h.cache.has(task.Id))

	r := h.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"title":" final ","done":true}`)
	require.Equal(t, http.StatusOK, r.status)
	var got tasksapi.Task
	r.json(t, &got)
	assert.Equal(t, "final", got.Title)
	assert.True(t, got.Done)
	assert.Equal(t, int64(2), got.Version)
	assert.Equal(t, `"2"`, r.header.Get("ETag"))
	assert.Equal(t, []string{task.Id}, h.cache.invalidated, "the cached task is invalidated after the commit")
	assert.Equal(t, int32(2), h.committed.Load())

	r = h.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"done":false}`)
	require.Equal(t, http.StatusOK, r.status)
	r.json(t, &got)
	assert.Equal(t, "final", got.Title, "absent fields are left alone")
	assert.False(t, got.Done)
}

func TestUpdate_IfMatch(t *testing.T) {
	h := newHarness(t)
	task := h.create(t, "contended")

	r := h.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"done":true}`, "If-Match", `"1"`)
	require.Equal(t, http.StatusOK, r.status, "the version read is still current")

	r = h.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"title":"lost update"}`, "If-Match", `"1"`)
	require.Equal(t, http.StatusPreconditionFailed, r.status, "someone changed it since version 1")
	assert.Equal(t, "version_mismatch", r.problem(t)["code"])
	assert.Equal(t, "contended", h.store.snapshot()[task.Id].Title, "the stale write did not apply")

	r = h.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"title":"any version"}`, "If-Match", "*")
	assert.Equal(t, http.StatusOK, r.status)

	for _, bad := range []string{`2`, `W/"2"`, `"two"`, `"0"`, `"1", "2"`} {
		r = h.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"done":true}`, "If-Match", bad)
		require.Equal(t, http.StatusBadRequest, r.status, bad)
		assert.Equal(t, "invalid_if_match", r.problem(t)["code"], bad)
	}
}

func TestUpdate_RejectsEmptyAndInvalidPatches(t *testing.T) {
	h := newHarness(t)
	task := h.create(t, "x")
	for body, code := range map[string]string{
		`{}`:                  "empty_patch",
		`{"unknown":1}`:       "empty_patch",
		`{"title":"   "}`:     "invalid_title",
		`{"title":"a\u0000"}`: "invalid_title",
	} {
		r := h.do(t, http.MethodPatch, "/tasks/"+task.Id, body)
		require.Equal(t, http.StatusBadRequest, r.status, body)
		assert.Equal(t, code, r.problem(t)["code"], body)
	}
	assert.Equal(t, int64(1), h.store.snapshot()[task.Id].Version, "nothing changed")
}

func TestUpdate_UnknownIs404(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, http.MethodPatch, "/tasks/ghost", `{"done":true}`)
	require.Equal(t, http.StatusNotFound, r.status)
	assert.Equal(t, "task_not_found", r.problem(t)["code"])
	assert.Empty(t, h.cache.invalidated)
}

// --- delete ------------------------------------------------------------------

func TestDelete_RemovesAndInvalidates(t *testing.T) {
	h := newHarness(t)
	task := h.create(t, "doomed")

	r := h.do(t, http.MethodDelete, "/tasks/"+task.Id, "", "If-Match", `"7"`)
	require.Equal(t, http.StatusPreconditionFailed, r.status)
	assert.Contains(t, h.store.snapshot(), task.Id)

	r = h.do(t, http.MethodDelete, "/tasks/"+task.Id, "", "If-Match", `"1"`)
	require.Equal(t, http.StatusNoContent, r.status)
	assert.NotContains(t, h.store.snapshot(), task.Id)
	assert.False(t, h.cache.has(task.Id))
	assert.Equal(t, []string{task.Id}, h.cache.invalidated)

	r = h.do(t, http.MethodDelete, "/tasks/"+task.Id, "")
	require.Equal(t, http.StatusNotFound, r.status)
	r = h.do(t, http.MethodGet, "/tasks/"+task.Id, "")
	require.Equal(t, http.StatusNotFound, r.status, "no resurrection from the cache")
}

// A failed invalidation is logged, not failed: the change is committed.
func TestDelete_CacheInvalidationFailureIsAWarning(t *testing.T) {
	h := newHarness(t)
	task := h.create(t, "x")
	h.cache.invErr = errors.New("valkey: connection refused")

	r := h.do(t, http.MethodDelete, "/tasks/"+task.Id, "")
	require.Equal(t, http.StatusNoContent, r.status)
	line := h.log.Find(t, map[string]any{"level": "WARN", "task_id": task.Id})
	require.NotNil(t, line)
	assert.Equal(t, "valkey: connection refused", line["err"])
}

// --- failures ----------------------------------------------------------------

// A store failure is a 500 problem that keeps the driver out of the body and
// puts the cause in the log line of the request; a deadline is a 504.
func TestStoreFailures(t *testing.T) {
	h := newHarness(t)
	h.store.err = errors.New("pgconn: connection refused")

	for _, c := range []struct{ method, path, body string }{
		{http.MethodPost, "/tasks", `{"title":"x"}`},
		{http.MethodGet, "/tasks", ""},
		{http.MethodGet, "/tasks/abc", ""},
		{http.MethodPatch, "/tasks/abc", `{"done":true}`},
		{http.MethodDelete, "/tasks/abc", ""},
	} {
		r := h.send(t, c.method, c.path, c.body)
		require.Equal(t, http.StatusInternalServerError, r.status, c.method+" "+c.path)
		p := r.problem(t)
		assert.NotContains(t, string(r.body), "pgconn", "the cause stays in the log")
		assert.NotEmpty(t, p["detail"])
	}
	failed := 0
	for _, line := range h.log.Lines(t) {
		if line["msg"] == "request failed" {
			failed++
			assert.Equal(t, "pgconn: connection refused", line["err"])
		}
	}
	assert.Equal(t, 5, failed, "every 5xx logs its cause")

	h.store.err = context.DeadlineExceeded
	r := h.send(t, http.MethodGet, "/tasks", "")
	assert.Equal(t, http.StatusGatewayTimeout, r.status)
}
