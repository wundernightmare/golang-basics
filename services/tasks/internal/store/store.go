// Package store is the tasks service's PostgreSQL persistence layer, built on
// the shared libs/pgx pool. It translates pgx's "no rows" into the domain's
// [domain.ErrNotFound] so the HTTP layer never has to know about the driver.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	pgxlib "github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// schema is the table this service owns. It is applied on boot via
// [Store.Migrate] — minimal on purpose (see libs/pgx.Migrate).
const schema = `
CREATE TABLE IF NOT EXISTS tasks (
    id         TEXT PRIMARY KEY,
    title      TEXT        NOT NULL,
    done       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Store is the task repository.
type Store struct {
	db *pgxlib.DB
}

// New wraps a libs/pgx pool as a task store.
func New(db *pgxlib.DB) *Store { return &Store{db: db} }

// Migrate brings the tasks table up. Safe to call on every boot.
func (s *Store) Migrate(ctx context.Context) error {
	return s.db.Migrate(ctx, schema)
}

// Create inserts a task. The caller assigns the ID and timestamps.
func (s *Store) Create(ctx context.Context, t domain.Task) error {
	_, err := s.db.Pool().Exec(ctx,
		`INSERT INTO tasks (id, title, done, created_at) VALUES ($1, $2, $3, $4)`,
		t.ID, t.Title, t.Done, t.CreatedAt)
	if err != nil {
		return fmt.Errorf("store: create task: %w", err)
	}
	return nil
}

// Get returns the task with id, or [domain.ErrNotFound] if there is none.
func (s *Store) Get(ctx context.Context, id string) (domain.Task, error) {
	var t domain.Task
	err := s.db.Pool().QueryRow(ctx,
		`SELECT id, title, done, created_at FROM tasks WHERE id = $1`, id).
		Scan(&t.ID, &t.Title, &t.Done, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Task{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Task{}, fmt.Errorf("store: get task %q: %w", id, err)
	}
	return t, nil
}

// List returns up to limit tasks, newest first.
func (s *Store) List(ctx context.Context, limit int) ([]domain.Task, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.Pool().Query(ctx,
		`SELECT id, title, done, created_at FROM tasks ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]domain.Task, 0, limit)
	for rows.Next() {
		var t domain.Task
		if err := rows.Scan(&t.ID, &t.Title, &t.Done, &t.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: scan task: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate tasks: %w", err)
	}
	return tasks, nil
}

// Delete removes the task with id, returning [domain.ErrNotFound] if it did not
// exist.
func (s *Store) Delete(ctx context.Context, id string) error {
	tag, err := s.db.Pool().Exec(ctx, `DELETE FROM tasks WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("store: delete task %q: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}
