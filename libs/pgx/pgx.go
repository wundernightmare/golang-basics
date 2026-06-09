package pgx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB wraps a pgx connection pool together with a structured logger. Construct
// one with [New] and share it across the whole service; the pool is safe for
// concurrent use. Expose its readiness with [DB.ReadyCheck].
type DB struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// New builds a connection pool from cfg, verifies it with a single ping (so a
// misconfigured database fails fast at boot rather than on first query) and
// returns a ready [DB]. The caller owns it and must call [DB.Close].
func New(ctx context.Context, cfg Config, log *slog.Logger) (*DB, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("pgx: parse dsn: %w", err)
	}
	// Only override pgxpool's own defaults when a value is actually set. A zero
	// is "leave the default", never "set to zero" — pgxpool reads a zero
	// MaxConnLifetime/IdleTime as "expires immediately", which would churn the
	// pool into the "too many failed attempts acquiring connection" failure.
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.ConnectTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgx: build pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgx: initial ping: %w", err)
	}

	log.Info("postgres pool ready",
		"host", cfg.Host, "database", cfg.Name, "max_conns", cfg.MaxConns)
	return &DB{pool: pool, log: log}, nil
}

// Pool returns the underlying pgx pool so services can run queries, batches and
// transactions with the full pgx API.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that pings the
// database with a short timeout. Register it on a server's [httpx.Health] so
// /readyz turns 503 the moment the database becomes unreachable.
func (db *DB) ReadyCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := db.pool.Ping(ctx); err != nil {
			return fmt.Errorf("postgres unreachable: %w", err)
		}
		return nil
	}
}

// Migrate runs the given DDL/DML statements in order inside a single
// transaction, rolling back on the first error. It is intentionally minimal —
// enough to bring a service's own schema up on boot (CREATE TABLE IF NOT
// EXISTS …) without pulling in a migration framework. Real projects graduate to
// a versioned migrator (Flyway, golang-migrate, …).
func (db *DB) Migrate(ctx context.Context, statements ...string) error {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgx: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	for i, stmt := range statements {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("pgx: migration statement %d: %w", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgx: commit migration: %w", err)
	}
	db.log.Info("postgres migration applied", "statements", len(statements))
	return nil
}

// Close releases every connection in the pool. It blocks until in-use
// connections are returned, so call it from the service's shutdown path.
func (db *DB) Close() { db.pool.Close() }
