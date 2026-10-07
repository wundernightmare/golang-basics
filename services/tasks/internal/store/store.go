// Package store is the tasks service's PostgreSQL persistence layer, built on
// the shared libs/pgx pool. Every change is one transaction that also writes
// the change's event to the outbox (internal/outbox), so the database and the
// event stream cannot disagree. It translates driver errors into the
// domain's ([domain.ErrNotFound], [domain.ErrVersionMismatch], …) so the HTTP
// layer never has to know about pgx.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tracehubmmp/golang-basics/libs/contracts/events"
	pgxlib "github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/outbox"
)

// Store is the task repository.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps a pool whose schema is migrated (see the migrations package).
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const taskColumns = `id, title, done, version, created_at`

func scanTask(row pgx.Row) (domain.Task, error) {
	var t domain.Task
	err := row.Scan(&t.ID, &t.Title, &t.Done, &t.Version, &t.CreatedAt)
	return t, err
}

// inTx runs fn in a transaction, committing when it returns nil.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }() // no-op after commit
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

// Create inserts a task titled title with the given id, and its task.created
// event, in one transaction; created_at comes from the database clock. With
// idem, a repeat of the same key returns the task the first request created
// and replayed=true instead of creating another (the same key with a
// different Hash is [domain.ErrIdempotencyKeyReused]). Two concurrent
// requests with one key serialise on the key's unique index: the second
// waits for the first to commit, then replays it.
func (s *Store) Create(ctx context.Context, id, title string, idem *domain.IdempotencyKey) (task domain.Task, replayed bool, err error) {
	ctx = pgxlib.WithOperation(ctx, "tasks.create")
	var replayID string
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		t, err := scanTask(tx.QueryRow(ctx,
			`INSERT INTO tasks (id, title) VALUES ($1, $2) RETURNING `+taskColumns, id, title))
		if err != nil {
			return fmt.Errorf("store: insert task: %w", err)
		}
		if idem != nil {
			var existingHash, existingID string
			// DO UPDATE of nothing (not DO NOTHING) so RETURNING yields the
			// winner's row on a conflict; xmax <> 0 tells the two cases apart.
			var inserted bool
			if err := tx.QueryRow(ctx,
				`INSERT INTO idempotency_keys (key, request_hash, task_id) VALUES ($1, $2, $3)
				 ON CONFLICT (key) DO UPDATE SET key = EXCLUDED.key
				 RETURNING request_hash, task_id, xmax = 0`,
				idem.Key, idem.Hash, t.ID).Scan(&existingHash, &existingID, &inserted); err != nil {
				return fmt.Errorf("store: record idempotency key: %w", err)
			}
			if !inserted {
				if existingHash != idem.Hash {
					return domain.ErrIdempotencyKeyReused
				}
				replayID = existingID
				return errReplay // roll back this attempt's task
			}
		}
		payload, eventID, err := createdEvent(t)
		if err != nil {
			return err
		}
		task = t
		return outbox.Enqueue(ctx, tx, outbox.Event{
			ID: eventID, AggregateID: t.ID, Type: outbox.TypeTaskCreated, Payload: payload,
		})
	})
	if errors.Is(err, errReplay) {
		t, gerr := s.Get(ctx, replayID)
		if gerr != nil {
			return domain.Task{}, false, fmt.Errorf("store: replay idempotent create: %w", gerr)
		}
		return t, true, nil
	}
	if err != nil {
		return domain.Task{}, false, err
	}
	return task, false, nil
}

// errReplay unwinds the create transaction when its idempotency key already
// belongs to an earlier request.
var errReplay = errors.New("store: idempotent replay")

// Get returns the task with id, or [domain.ErrNotFound] if there is none.
func (s *Store) Get(ctx context.Context, id string) (domain.Task, error) {
	ctx = pgxlib.WithOperation(ctx, "tasks.get")
	t, err := scanTask(s.pool.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Task{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Task{}, fmt.Errorf("store: get task %q: %w", id, err)
	}
	return t, nil
}

// List returns one page of tasks in (created_at DESC, id DESC) order,
// starting after req.Cursor. It reads one row more than asked to know whether
// a next page exists, and serves the query from tasks_created_at_id_idx.
func (s *Store) List(ctx context.Context, req domain.PageRequest) (domain.Page, error) {
	ctx = pgxlib.WithOperation(ctx, "tasks.list")
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxPageSize {
		limit = domain.DefaultPageSize
	}
	var rows pgx.Rows
	var err error
	if req.Cursor == nil {
		rows, err = s.pool.Query(ctx,
			`SELECT `+taskColumns+` FROM tasks ORDER BY created_at DESC, id DESC LIMIT $1`, limit+1)
	} else {
		// Row-value comparison: exactly the index order, one range scan.
		rows, err = s.pool.Query(ctx,
			`SELECT `+taskColumns+` FROM tasks WHERE (created_at, id) < ($1, $2)
			 ORDER BY created_at DESC, id DESC LIMIT $3`, req.Cursor.CreatedAt, req.Cursor.ID, limit+1)
	}
	if err != nil {
		return domain.Page{}, fmt.Errorf("store: list tasks: %w", err)
	}
	tasks, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Task, error) { return scanTask(r) })
	if err != nil {
		return domain.Page{}, fmt.Errorf("store: list tasks: %w", err)
	}
	page := domain.Page{Tasks: tasks}
	if len(tasks) > limit {
		page.Tasks = tasks[:limit]
		next := domain.CursorAfter(page.Tasks[limit-1])
		page.Next = &next
	}
	return page, nil
}

// Update applies patch (already normalized) to the task with id and writes
// its task.updated event, in one transaction. With expect set, the update
// applies only if the task is still at that version, else
// [domain.ErrVersionMismatch]; an unknown id is [domain.ErrNotFound].
func (s *Store) Update(ctx context.Context, id string, patch domain.Patch, expect *int64) (domain.Task, error) {
	ctx = pgxlib.WithOperation(ctx, "tasks.update")
	var out domain.Task
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		t, err := scanTask(tx.QueryRow(ctx,
			`UPDATE tasks SET title = coalesce($2, title), done = coalesce($3, done), version = version + 1
			  WHERE id = $1 AND ($4::bigint IS NULL OR version = $4)
			  RETURNING `+taskColumns, id, patch.Title, patch.Done, expect))
		if errors.Is(err, pgx.ErrNoRows) {
			return s.missOrMismatch(ctx, tx, id)
		}
		if err != nil {
			return fmt.Errorf("store: update task %q: %w", id, err)
		}
		eventID := outbox.NewID()
		payload, err := json.Marshal(events.TaskUpdatedEvent{
			EventId: eventID, Id: t.ID, Title: t.Title, Done: t.Done, Version: int(t.Version),
		})
		if err != nil {
			return fmt.Errorf("store: encode task.updated: %w", err)
		}
		out = t
		return outbox.Enqueue(ctx, tx, outbox.Event{
			ID: eventID, AggregateID: t.ID, Type: outbox.TypeTaskUpdated, Payload: payload,
		})
	})
	return out, err
}

// Delete removes the task with id and writes its task.deleted event, in one
// transaction; expect and the errors are as for [Store.Update]. The task's
// idempotency keys go with it (ON DELETE CASCADE).
func (s *Store) Delete(ctx context.Context, id string, expect *int64) error {
	ctx = pgxlib.WithOperation(ctx, "tasks.delete")
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var version int64
		err := tx.QueryRow(ctx,
			`DELETE FROM tasks WHERE id = $1 AND ($2::bigint IS NULL OR version = $2) RETURNING version`,
			id, expect).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return s.missOrMismatch(ctx, tx, id)
		}
		if err != nil {
			return fmt.Errorf("store: delete task %q: %w", id, err)
		}
		eventID := outbox.NewID()
		payload, err := json.Marshal(events.TaskDeletedEvent{EventId: eventID, Id: id, Version: int(version)})
		if err != nil {
			return fmt.Errorf("store: encode task.deleted: %w", err)
		}
		return outbox.Enqueue(ctx, tx, outbox.Event{
			ID: eventID, AggregateID: id, Type: outbox.TypeTaskDeleted, Payload: payload,
		})
	})
}

// missOrMismatch explains a conditional UPDATE/DELETE that matched no row.
func (s *Store) missOrMismatch(ctx context.Context, tx pgx.Tx, id string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM tasks WHERE id = $1)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("store: check task %q: %w", id, err)
	}
	if exists {
		return domain.ErrVersionMismatch
	}
	return domain.ErrNotFound
}

func createdEvent(t domain.Task) (payload []byte, eventID string, err error) {
	eventID = outbox.NewID()
	version := int(t.Version)
	payload, err = json.Marshal(events.TaskCreatedEvent{
		Id: t.ID, Title: t.Title, CreatedAt: t.CreatedAt.UTC(), EventId: &eventID, Version: &version,
	})
	if err != nil {
		return nil, "", fmt.Errorf("store: encode task.created: %w", err)
	}
	return payload, eventID, nil
}

// PurgeIdempotencyKeys deletes keys older than ttl and returns how many.
func (s *Store) PurgeIdempotencyKeys(ctx context.Context, ttl time.Duration) (int64, error) {
	tag, err := s.pool.Exec(pgxlib.WithOperation(ctx, "tasks.purge_idempotency_keys"),
		`DELETE FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1)`, ttl.Seconds())
	if err != nil {
		return 0, fmt.Errorf("store: purge idempotency keys: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RunJanitor purges expired idempotency keys every interval until ctx ends.
// A failed pass is logged and retried on the next tick.
func (s *Store) RunJanitor(ctx context.Context, ttl, interval time.Duration, log *slog.Logger) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			n, err := s.PurgeIdempotencyKeys(ctx, ttl)
			if err != nil {
				if ctx.Err() == nil {
					log.WarnContext(ctx, "idempotency key purge failed", "err", err)
				}
				continue
			}
			if n > 0 {
				log.InfoContext(ctx, "idempotency keys purged", "count", n)
			}
		}
	}
}
