// Package api holds the tasks service's HTTP routes. Handlers stay thin: the
// cross-cutting concerns (request ids, tracing, logging, metrics, health,
// shutdown) live in libs/httpx, and this package only orchestrates the store
// and the cache behind two small interfaces so the wiring is unit-testable
// with fakes. Events are not published here: the store writes them to the
// outbox in the same transaction as the change, and the relay publishes them
// (internal/outbox) — the request never waits on Kafka.
//
// The wire types are the generated contracts (libs/contracts/tasksapi from
// api/tsp/tasks.tsp); the handlers convert to and from the internal domain
// model at this boundary, and the api tests validate every exchange against
// the OpenAPI document.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/tracehubmmp/golang-basics/libs/contracts/tasksapi"
	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// Store is the persistence dependency (satisfied by internal/store.Store).
// Every method that changes a task also records its event, atomically.
type Store interface {
	Create(ctx context.Context, id, title string, idem *domain.IdempotencyKey) (domain.Task, bool, error)
	Get(ctx context.Context, id string) (domain.Task, error)
	List(ctx context.Context, req domain.PageRequest) (domain.Page, error)
	Update(ctx context.Context, id string, patch domain.Patch, expect *int64) (domain.Task, error)
	Delete(ctx context.Context, id string, expect *int64) error
}

// Cache is the read cache (satisfied by internal/cache.Tasks).
type Cache interface {
	Get(ctx context.Context, id string, load func(context.Context) (domain.Task, error)) (domain.Task, bool, error)
	Warm(ctx context.Context, t domain.Task)
	Invalidate(ctx context.Context, id string) error
}

// Deps bundles everything the handlers need.
type Deps struct {
	Store  Store
	Cache  Cache
	Logger *slog.Logger
	// Committed, when set, is called after every change has committed —
	// main wires the outbox relay's Wake so the event goes out at once
	// instead of on the relay's next poll.
	Committed func()
}

type handlers struct{ Deps }

// Register attaches the tasks routes to the server's mux.
func Register(srv *httpx.Server, deps Deps) {
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Committed == nil {
		deps.Committed = func() {}
	}
	h := &handlers{Deps: deps}
	mux := srv.Mux()
	mux.HandleFunc("POST /tasks", h.create)
	mux.HandleFunc("GET /tasks", h.list)
	mux.HandleFunc("GET /tasks/{id}", h.get)
	mux.HandleFunc("PATCH /tasks/{id}", h.update)
	mux.HandleFunc("DELETE /tasks/{id}", h.delete)
}

// Header names beyond the standard ones.
const (
	HeaderIdempotencyKey    = "Idempotency-Key"
	HeaderIdempotentReplay  = "Idempotent-Replayed"
	HeaderCache             = "X-Cache"
	maxIdempotencyKeyLength = 255
)

// toWire converts the internal model to the contract's Task.
func toWire(t domain.Task) tasksapi.Task {
	return tasksapi.Task{Id: t.ID, Title: t.Title, Done: t.Done, Version: t.Version, CreatedAt: t.CreatedAt.UTC()}
}

// ETag is the entity tag of a task version: the version number, quoted.
func ETag(version int64) string { return `"` + strconv.FormatInt(version, 10) + `"` }

// create persists a new task together with its task.created event. The cache
// is warmed best-effort; the event goes out through the outbox relay.
func (h *handlers) create(w http.ResponseWriter, r *http.Request) {
	var req tasksapi.CreateTaskRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	title, err := domain.NormalizeTitle(req.Title)
	if err != nil {
		httpx.WriteProblem(w, r, badRequest("invalid_title", err))
		return
	}
	idem, err := idempotencyKey(r, title)
	if err != nil {
		httpx.WriteProblem(w, r, badRequest("invalid_idempotency_key", err))
		return
	}

	ctx := r.Context()
	task, replayed, err := h.Store.Create(ctx, uuid.NewString(), title, idem)
	switch {
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		httpx.WriteProblem(w, r, problem(http.StatusUnprocessableEntity, "idempotency_key_reused",
			"Idempotency-Key was already used for a request with a different body"))
		return
	case err != nil:
		h.fail(w, r, "could not persist task", err)
		return
	}

	w.Header().Set("ETag", ETag(task.Version))
	if replayed {
		w.Header().Set(HeaderIdempotentReplay, "true")
		httpx.WriteJSON(w, http.StatusOK, toWire(task))
		return
	}
	h.Committed()
	h.Cache.Warm(ctx, task)
	w.Header().Set("Location", "/tasks/"+task.ID)
	httpx.WriteJSON(w, http.StatusCreated, toWire(task))
}

// idempotencyKey reads the optional Idempotency-Key header. The request hash
// covers what the request asks for — the normalized title — so a retry that
// differs only in whitespace around the title is still the same request.
func idempotencyKey(r *http.Request, title string) (*domain.IdempotencyKey, error) {
	key := r.Header.Get(HeaderIdempotencyKey)
	if key == "" {
		if _, sent := r.Header[HeaderIdempotencyKey]; sent {
			return nil, errors.New("Idempotency-Key must not be empty")
		}
		return nil, nil
	}
	if len(key) > maxIdempotencyKeyLength {
		return nil, errors.New("Idempotency-Key must be at most 255 characters")
	}
	for i := range len(key) {
		if c := key[i]; c < 0x20 || c > 0x7e {
			return nil, errors.New("Idempotency-Key must be printable ASCII")
		}
	}
	sum := sha256.Sum256([]byte(title))
	return &domain.IdempotencyKey{Key: key, Hash: hex.EncodeToString(sum[:])}, nil
}

// get reads a task through the cache (cache-aside, see internal/cache). A
// missing task is a 404 problem and is never cached.
func (h *handlers) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	task, hit, err := h.Cache.Get(r.Context(), id, func(ctx context.Context) (domain.Task, error) {
		return h.Store.Get(ctx, id)
	})
	switch {
	case errors.Is(err, domain.ErrNotFound):
		httpx.WriteProblem(w, r, notFound(id))
		return
	case err != nil:
		h.fail(w, r, "could not load task", err)
		return
	}
	w.Header().Set("ETag", ETag(task.Version))
	if hit {
		w.Header().Set(HeaderCache, "hit")
	} else {
		w.Header().Set(HeaderCache, "miss")
	}
	httpx.WriteJSON(w, http.StatusOK, toWire(task))
}

// list returns one page of tasks, newest first (keyset pagination).
func (h *handlers) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	for name, values := range q {
		switch {
		case name != "limit" && name != "cursor":
			httpx.WriteProblem(w, r, problem(http.StatusBadRequest, "unknown_parameter",
				"unknown query parameter "+strconv.Quote(name)+"; only limit and cursor are accepted"))
			return
		case len(values) > 1:
			httpx.WriteProblem(w, r, problem(http.StatusBadRequest, "repeated_parameter",
				"query parameter "+strconv.Quote(name)+" given more than once"))
			return
		}
	}
	req := domain.PageRequest{Limit: domain.DefaultPageSize}
	if _, sent := q["limit"]; sent {
		n, err := strconv.Atoi(q.Get("limit"))
		if err != nil || n < 1 || n > domain.MaxPageSize {
			httpx.WriteProblem(w, r, problem(http.StatusBadRequest, "invalid_limit",
				"limit must be an integer from 1 to "+strconv.Itoa(domain.MaxPageSize)))
			return
		}
		req.Limit = n
	}
	if _, sent := q["cursor"]; sent {
		c, err := domain.DecodeCursor(q.Get("cursor"))
		if err != nil {
			httpx.WriteProblem(w, r, badRequest("invalid_cursor", err))
			return
		}
		req.Cursor = &c
	}

	page, err := h.Store.List(r.Context(), req)
	if err != nil {
		h.fail(w, r, "could not list tasks", err)
		return
	}
	out := tasksapi.TaskList{Tasks: make([]tasksapi.Task, 0, len(page.Tasks))}
	for _, t := range page.Tasks {
		out.Tasks = append(out.Tasks, toWire(t))
	}
	if page.Next != nil {
		next := page.Next.Encode()
		out.NextCursor = &next
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// update applies a partial update, optionally conditional on If-Match, and
// invalidates the cached task once the change (and its task.updated event)
// has committed.
func (h *handlers) update(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	expect, err := ifMatch(r)
	if err != nil {
		httpx.WriteProblem(w, r, badRequest("invalid_if_match", err))
		return
	}
	var raw map[string]json.RawMessage
	if err := httpx.DecodeJSON(r, &raw); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	req, err := decodePatch(raw)
	if err != nil {
		httpx.WriteProblem(w, r, badRequest("invalid_patch", err))
		return
	}
	patch, err := domain.Patch{Title: req.Title, Done: req.Done}.Normalize()
	switch {
	case errors.Is(err, domain.ErrEmptyPatch):
		httpx.WriteProblem(w, r, badRequest("empty_patch", err))
		return
	case err != nil:
		httpx.WriteProblem(w, r, badRequest("invalid_title", err))
		return
	}

	ctx := r.Context()
	task, err := h.Store.Update(ctx, id, patch, expect)
	if h.writeChangeError(w, r, id, err, "could not update task") {
		return
	}
	h.Committed()
	h.invalidate(ctx, id)
	w.Header().Set("ETag", ETag(task.Version))
	httpx.WriteJSON(w, http.StatusOK, toWire(task))
}

// delete removes a task, optionally conditional on If-Match, and invalidates
// its cache entry once the delete (and its task.deleted event) has committed.
func (h *handlers) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	expect, err := ifMatch(r)
	if err != nil {
		httpx.WriteProblem(w, r, badRequest("invalid_if_match", err))
		return
	}
	ctx := r.Context()
	if h.writeChangeError(w, r, id, h.Store.Delete(ctx, id, expect), "could not delete task") {
		return
	}
	h.Committed()
	h.invalidate(ctx, id)
	w.WriteHeader(http.StatusNoContent)
}

// decodePatch reads a PATCH body field by field: a field the contract does not
// know, or an explicit null, is a 400 instead of being dropped silently —
// {"done": null} must not read as "leave done alone" (found by Schemathesis).
func decodePatch(raw map[string]json.RawMessage) (tasksapi.UpdateTaskRequest, error) {
	var req tasksapi.UpdateTaskRequest
	for name, v := range raw {
		var dst any
		switch name {
		case "title":
			dst = &req.Title
		case "done":
			dst = &req.Done
		default:
			return req, fmt.Errorf("unknown field %q; a patch sets title and/or done", name)
		}
		if string(v) == "null" {
			return req, fmt.Errorf("%s must not be null", name)
		}
		if err := json.Unmarshal(v, dst); err != nil {
			return req, fmt.Errorf("%s: %w", name, err)
		}
	}
	return req, nil
}

// writeChangeError answers a failed update/delete and reports whether it did.
func (h *handlers) writeChangeError(w http.ResponseWriter, r *http.Request, id string, err error, detail string) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, domain.ErrNotFound):
		httpx.WriteProblem(w, r, notFound(id))
	case errors.Is(err, domain.ErrVersionMismatch):
		httpx.WriteProblem(w, r, problem(http.StatusPreconditionFailed, "version_mismatch",
			"the task has changed since the version in If-Match; fetch it again"))
	default:
		h.fail(w, r, detail, err)
	}
	return true
}

// invalidate tombstones the cached task. A failure leaves the old entry for
// at most its TTL: logged loudly, not failed — the change is committed.
func (h *handlers) invalidate(ctx context.Context, id string) {
	if err := h.Cache.Invalidate(ctx, id); err != nil {
		h.Logger.WarnContext(ctx, "cache invalidation failed; a stale entry may be served until it expires",
			"task_id", id, "err", err)
	}
}

// ifMatch parses an optional If-Match: a single strong entity tag ("3") or
// "*". It returns the version to require, or nil for none ("*" requires only
// that the task exists, which update/delete check anyway).
// maxIfMatchLength bounds If-Match as the contract does: 18 digits in quotes.
const maxIfMatchLength = 20

func ifMatch(r *http.Request) (*int64, error) {
	raw := strings.TrimSpace(r.Header.Get("If-Match"))
	if _, sent := r.Header["If-Match"]; !sent || raw == "*" {
		return nil, nil
	}
	if len(raw) < 3 || len(raw) > maxIfMatchLength || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return nil, errors.New(`If-Match must be a single strong entity tag such as "3" (weak tags never match)`)
	}
	v, err := strconv.ParseInt(raw[1:len(raw)-1], 10, 64)
	if err != nil || v < 1 {
		return nil, errors.New(`If-Match must carry a task version such as "3"`)
	}
	return &v, nil
}

// fail answers an unexpected error: 504 when the request's deadline ran out,
// otherwise a 500 whose cause is logged and recorded on the span.
func (h *handlers) fail(w http.ResponseWriter, r *http.Request, detail string, err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteProblem(w, r, httpx.Internal(detail, err))
}

func problem(status int, code, detail string) httpx.Problem {
	return httpx.Problem{Status: status, Detail: detail, Extensions: map[string]any{"code": code}}
}

func badRequest(code string, err error) httpx.Problem {
	return problem(http.StatusBadRequest, code, err.Error())
}

func notFound(id string) httpx.Problem {
	return httpx.Problem{
		Type:       "https://golang-basics/errors/task-not-found",
		Title:      "Task not found",
		Status:     http.StatusNotFound,
		Detail:     "no task with id " + id,
		Extensions: map[string]any{"code": "task_not_found"},
	}
}
