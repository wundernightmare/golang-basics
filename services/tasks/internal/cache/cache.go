// Package cache is the tasks service's read cache: cache-aside over
// libs/valkey with the library's delete-safety scheme (fills are SET NX,
// writes leave a short-lived tombstone after they commit — see the
// libs/valkey package doc), so a GET racing a PATCH or DELETE cannot put the
// old task back.
//
// Keys are versioned by the cached shape — "task:v2:<id>" — so a deploy that
// changes what is cached never reads an entry written by the previous
// release; the old keys simply expire.
package cache

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// keyPrefix names the cached shape; bump it when entry changes.
const keyPrefix = "task:v2:"

// Key returns the cache key of task id.
func Key(id string) string { return keyPrefix + id }

// entry is the cached shape of a task.
type entry struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Done      bool      `json:"done"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
}

func toEntry(t domain.Task) entry {
	return entry{ID: t.ID, Title: t.Title, Done: t.Done, Version: t.Version, CreatedAt: t.CreatedAt}
}

func (e entry) task() domain.Task {
	return domain.Task{ID: e.ID, Title: e.Title, Done: e.Done, Version: e.Version, CreatedAt: e.CreatedAt}
}

// Tasks caches tasks by id.
type Tasks struct {
	c            *valkey.Cache
	ttl          time.Duration
	tombstoneTTL time.Duration
	log          *slog.Logger
}

// New returns a task cache over c: entries live ttl (±10%), invalidations
// hold the key for tombstoneTTL.
func New(c *valkey.Cache, ttl, tombstoneTTL time.Duration, log *slog.Logger) *Tasks {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Tasks{c: c, ttl: ttl, tombstoneTTL: tombstoneTTL, log: log}
}

// Get returns task id from the cache, or loads it with load (the store) and
// fills the cache; hit reports which. Concurrent misses share one load.
// load's error — domain.ErrNotFound included — is returned as is and nothing
// is cached.
func (t *Tasks) Get(ctx context.Context, id string, load func(context.Context) (domain.Task, error)) (domain.Task, bool, error) {
	e, hit, err := valkey.Aside(ctx, t.c, Key(id), t.ttl, func(ctx context.Context) (entry, error) {
		task, err := load(ctx)
		return toEntry(task), err
	})
	if err != nil {
		return domain.Task{}, false, err
	}
	return e.task(), hit, nil
}

// Warm caches a task just created (a fill: it never overwrites anything).
// Best-effort — a failure is logged, the next read loads it.
func (t *Tasks) Warm(ctx context.Context, task domain.Task) {
	e := toEntry(task)
	b, err := json.Marshal(e)
	if err == nil {
		_, err = t.c.SetNX(ctx, Key(task.ID), string(b), valkey.Jitter(t.ttl))
	}
	if err != nil {
		t.log.WarnContext(ctx, "cache write failed", "key", Key(task.ID), "err", err)
	}
}

// Invalidate tombstones task id. Call it after the change has committed.
func (t *Tasks) Invalidate(ctx context.Context, id string) error {
	return t.c.Tombstone(ctx, Key(id), t.tombstoneTTL)
}
