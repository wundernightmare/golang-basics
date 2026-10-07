package pgx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB wraps a pgx connection pool together with a structured logger. Construct
// one with [New] and share it across the whole service; the pool is safe for
// concurrent use. Expose its readiness with [DB.ReadyCheck] and its metrics
// with [DB.Collectors]. Every query runs under an OpenTelemetry span (client
// span named after the statement) when the calling context carries a trace,
// so a request's DB time shows up inside its HTTP span, and is timed into
// pgx_query_duration_seconds{operation} (see [WithOperation]).
type DB struct {
	pool    *pgxpool.Pool
	log     *slog.Logger
	metrics *queryMetrics
}

// New builds a connection pool from cfg, verifies it with a single ping (so a
// misconfigured database fails fast at boot rather than on first query) and
// returns a ready [DB]. The caller owns it and must call [DB.Close].
//
// Every connection is opened with the session timeouts from cfg
// (statement_timeout, lock_timeout, idle_in_transaction_session_timeout) as
// run-time parameters; a value already in the DSN is overridden only when the
// corresponding field is non-zero.
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
	connectTimeout := cfg.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = defaultConnectTimeout
	}
	poolCfg.ConnConfig.ConnectTimeout = connectTimeout
	for k, v := range cfg.runtimeParams() {
		poolCfg.ConnConfig.RuntimeParams[k] = v
	}

	// Tracing and metrics: a span per query/batch/prepare/connect, child of
	// whatever span is in the query context (no-op provider → no cost beyond
	// the hook call), and the query duration / error metrics beside it.
	metrics := newQueryMetrics()
	poolCfg.ConnConfig.Tracer = multitracer.New(otelpgx.NewTracer(), metrics)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgx: build pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgx: initial ping: %w", err)
	}

	// Log what the DSN resolved to, not the discrete fields: with
	// DATABASE_URL set those are ignored and would name the wrong server.
	log.InfoContext(ctx, "postgres pool ready",
		"host", poolCfg.ConnConfig.Host, "port", poolCfg.ConnConfig.Port,
		"database", poolCfg.ConnConfig.Database, "max_conns", poolCfg.MaxConns,
		"statement_timeout", cfg.StatementTimeout.String())
	return &DB{pool: pool, log: log, metrics: metrics}, nil
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

// Close releases every connection in the pool. It blocks until in-use
// connections are returned, so call it from the service's shutdown path.
func (db *DB) Close() { db.pool.Close() }
