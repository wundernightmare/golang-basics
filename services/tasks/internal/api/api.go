// Package api holds the tasks service's HTTP routes. Handlers stay thin: the
// cross-cutting concerns (logging, metrics, health, shutdown, tracing) live in
// the shared libs, and this package only orchestrates store + cache + producer
// behind a small set of interfaces so the wiring is unit-testable with fakes.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// Store is the persistence dependency (satisfied by internal/store.Store).
type Store interface {
	Create(ctx context.Context, t domain.Task) error
	Get(ctx context.Context, id string) (domain.Task, error)
	List(ctx context.Context, limit int) ([]domain.Task, error)
	Delete(ctx context.Context, id string) error
}

// Cache is the cache-aside dependency (satisfied by libs/valkey.Cache).
type Cache interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) error
}

// Publisher is the event dependency (satisfied by libs/kafka.Producer).
type Publisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
}

// Deps bundles everything the handlers need.
type Deps struct {
	Store     Store
	Cache     Cache
	Publisher Publisher
	Topic     string        // Kafka topic for task.created events
	CacheTTL  time.Duration // TTL for cached task lookups
	Logger    interface{ Warn(msg string, args ...any) }
}

type handlers struct{ Deps }

// Register attaches the tasks routes to the shared server engine.
func Register(srv *httpx.Server, deps Deps) {
	h := &handlers{Deps: deps}
	e := srv.Engine()
	e.POST("/tasks", h.create)
	e.GET("/tasks", h.list)
	e.GET("/tasks/:id", h.get)
	e.DELETE("/tasks/:id", h.delete)
}

type createRequest struct {
	Title string `json:"title"`
}

func cacheKey(id string) string { return "task:" + id }

// create persists a new task, publishes a task.created event and warms the
// cache. The event and cache writes are best-effort: a task is durable once the
// row is committed, so a broker/cache hiccup logs a warning rather than failing
// the request. (Production would close that gap with a transactional outbox.)
func (h *handlers) create(c *gin.Context) {
	var req createRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httpx.AbortProblem(c, httpx.NewProblem(http.StatusBadRequest, "invalid JSON body"))
		return
	}
	if req.Title == "" {
		httpx.AbortProblem(c, httpx.NewProblem(http.StatusBadRequest, domain.ErrEmptyTitle.Error()))
		return
	}

	ctx := c.Request.Context()
	task := domain.Task{ID: uuid.NewString(), Title: req.Title, CreatedAt: time.Now().UTC()}
	if err := h.Store.Create(ctx, task); err != nil {
		httpx.AbortProblem(c, httpx.NewProblem(http.StatusInternalServerError, "could not persist task"))
		return
	}

	h.publishCreated(ctx, task)
	if body, err := json.Marshal(task); err == nil {
		if err := h.Cache.Set(ctx, cacheKey(task.ID), string(body), h.CacheTTL); err != nil {
			h.Logger.Warn("cache write failed", "key", cacheKey(task.ID), "err", err)
		}
	}

	c.JSON(http.StatusCreated, task)
}

func (h *handlers) publishCreated(ctx context.Context, task domain.Task) {
	evt := domain.TaskCreatedEvent{ID: task.ID, Title: task.Title, CreatedAt: task.CreatedAt}
	payload, err := json.Marshal(evt)
	if err != nil {
		h.Logger.Warn("event marshal failed", "id", task.ID, "err", err)
		return
	}
	if err := h.Publisher.Publish(ctx, h.Topic, []byte(task.ID), payload); err != nil {
		h.Logger.Warn("event publish failed", "id", task.ID, "topic", h.Topic, "err", err)
	}
}

// get reads a task using the cache-aside pattern: serve from Valkey on a hit,
// otherwise load from Postgres and warm the cache. A missing task is a 404
// problem and is never cached (the loader returns an error on a miss).
func (h *handlers) get(c *gin.Context) {
	ctx := c.Request.Context()
	id := c.Param("id")

	if raw, ok, err := h.Cache.Get(ctx, cacheKey(id)); err == nil && ok {
		var t domain.Task
		if json.Unmarshal([]byte(raw), &t) == nil {
			c.Header("X-Cache", "hit")
			c.JSON(http.StatusOK, t)
			return
		}
	}

	task, err := h.Store.Get(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		httpx.AbortProblem(c, notFound(id))
		return
	}
	if err != nil {
		httpx.AbortProblem(c, httpx.NewProblem(http.StatusInternalServerError, "could not load task"))
		return
	}

	if body, err := json.Marshal(task); err == nil {
		_ = h.Cache.Set(ctx, cacheKey(id), string(body), h.CacheTTL)
	}
	c.Header("X-Cache", "miss")
	c.JSON(http.StatusOK, task)
}

func (h *handlers) list(c *gin.Context) {
	ctx := c.Request.Context()
	tasks, err := h.Store.List(ctx, 0)
	if err != nil {
		httpx.AbortProblem(c, httpx.NewProblem(http.StatusInternalServerError, "could not list tasks"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks})
}

// delete removes a task and evicts its cache entry.
func (h *handlers) delete(c *gin.Context) {
	ctx := c.Request.Context()
	id := c.Param("id")

	if err := h.Store.Delete(ctx, id); errors.Is(err, domain.ErrNotFound) {
		httpx.AbortProblem(c, notFound(id))
		return
	} else if err != nil {
		httpx.AbortProblem(c, httpx.NewProblem(http.StatusInternalServerError, "could not delete task"))
		return
	}

	if err := h.Cache.Del(ctx, cacheKey(id)); err != nil {
		h.Logger.Warn("cache evict failed", "key", cacheKey(id), "err", err)
	}
	c.Status(http.StatusNoContent)
}

func notFound(id string) httpx.Problem {
	return httpx.Problem{
		Type:       "https://golang-basics/errors/task-not-found",
		Title:      "Task not found",
		Status:     http.StatusNotFound,
		Detail:     "no task with id " + id,
		Instance:   "/tasks/" + id,
		Extensions: map[string]any{"code": "task_not_found"},
	}
}
