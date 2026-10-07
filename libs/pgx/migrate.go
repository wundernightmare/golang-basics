package pgx

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationFile is the name a migration must have: a version number, an
// underscore, a snake_case name, ".sql" — "001_create_tasks.sql".
var migrationFile = regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.sql$`)

// Migration is one versioned schema change, read from a NNN_name.sql file.
type Migration struct {
	Version int64
	Name    string
	File    string
	SQL     string
}

// LoadMigrations reads every *.sql file at the root of fsys, in version
// order. A .sql file that does not follow the NNN_name.sql pattern, or two
// files with the same version, is an error: a typo must not silently skip a
// migration.
func LoadMigrations(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("pgx: read migrations: %w", err)
	}
	var out []Migration
	seen := map[int64]string{}
	for _, e := range entries {
		if e.IsDir() || path.Ext(e.Name()) != ".sql" {
			continue
		}
		m := migrationFile.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("pgx: migration %s: name must be NNN_snake_name.sql", e.Name())
		}
		v, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("pgx: migration %s: version: %w", e.Name(), err)
		}
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("pgx: migrations %s and %s share version %d", prev, e.Name(), v)
		}
		seen[v] = e.Name()
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("pgx: migration %s: %w", e.Name(), err)
		}
		out = append(out, Migration{Version: v, Name: m[2], File: e.Name(), SQL: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Migrate brings the schema up to date from the NNN_name.sql files at the
// root of fsys (typically an embed.FS; use fs.Sub for a subdirectory). It is
// safe to run on every boot of every replica:
//
//   - applied versions are recorded in schema_migrations(version, name,
//     applied_at), created on first run, in the connection's current schema
//     (search_path) — so one database can hold several isolated schemas;
//   - each migration runs in its own transaction together with its
//     schema_migrations row: it applies completely or not at all, and a
//     failure stops the run with the file name in the error, leaving the
//     earlier migrations applied;
//   - every transaction first takes pg_advisory_xact_lock on a key derived
//     from the current schema and then re-reads schema_migrations, so
//     concurrent runners (replicas booting together) serialise: the second
//     one waits, then finds the version applied and skips it. A transaction-
//     scoped lock is released by commit / rollback / a dropped connection, so
//     a crashed runner cannot leave the lock held, and it works through a
//     transaction-pooling proxy where a session lock would not.
//
// Migrations are forward-only; there is no "down". A migration that must not
// run in a transaction (CREATE INDEX CONCURRENTLY) does not belong here.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, log *slog.Logger) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	migrations, err := LoadMigrations(fsys)
	if err != nil {
		return err
	}
	applied := 0
	for _, m := range migrations {
		ran, err := applyOne(ctx, pool, m)
		if err != nil {
			return err
		}
		if ran {
			applied++
			log.InfoContext(ctx, "postgres migration applied", "version", m.Version, "file", m.File)
		}
	}
	log.InfoContext(ctx, "postgres schema up to date", "migrations", len(migrations), "applied", applied)
	return nil
}

// lockSQL takes the migration lock for the current schema. hashtextextended
// maps the key to the bigint the advisory lock functions take.
const lockSQL = `SELECT pg_advisory_xact_lock(hashtextextended('golang-basics/pgx.Migrate:' || coalesce(current_schema(), ''), 0))`

const ensureTableSQL = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    BIGINT PRIMARY KEY,
    name       TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// applyOne runs m in its own transaction unless it is already recorded; it
// reports whether it ran.
func applyOne(ctx context.Context, pool *pgxpool.Pool, m Migration) (ran bool, err error) {
	ctx = WithOperation(ctx, "pgx.migrate")
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("pgx: migration %s: begin: %w", m.File, err)
	}
	defer func() {
		if !ran || err != nil {
			rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = tx.Rollback(rbCtx)
		}
	}()

	if _, err := tx.Exec(ctx, lockSQL); err != nil {
		return false, fmt.Errorf("pgx: migration %s: lock: %w", m.File, err)
	}
	if _, err := tx.Exec(ctx, ensureTableSQL); err != nil {
		return false, fmt.Errorf("pgx: migration %s: schema_migrations: %w", m.File, err)
	}
	var one int
	switch err := tx.QueryRow(ctx, `SELECT 1 FROM schema_migrations WHERE version = $1`, m.Version).Scan(&one); {
	case err == nil:
		return false, nil // applied (by an earlier boot or a concurrent runner)
	case !errors.Is(err, pgx.ErrNoRows):
		return false, fmt.Errorf("pgx: migration %s: read schema_migrations: %w", m.File, err)
	}

	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return false, fmt.Errorf("pgx: migration %s: %w", m.File, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.Version, m.Name); err != nil {
		return false, fmt.Errorf("pgx: migration %s: record: %w", m.File, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("pgx: migration %s: commit: %w", m.File, err)
	}
	return true, nil
}
