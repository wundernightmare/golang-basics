package pgx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
)

// DB wraps a pgx connection pool together with a structured logger. Construct
// one with [New] and share it across the whole service; the pool is safe for
// concurrent use. Expose its readiness with [DB.ReadyCheck] and its metrics
// with [DB.Collectors]. Every query runs under an OpenTelemetry span (client
// span named after the statement) when the calling context carries a trace,
// so a request's DB time shows up inside its HTTP span, and is timed into
// db_query_duration_seconds{operation} (see [WithOperation]).
type DB struct {
	pool      *pgxpool.Pool
	log       *slog.Logger
	metrics   *queryMetrics
	opTimeout time.Duration
}

// ErrClosed is what a call on a closed [DB] fails with — through [DB.Conn],
// [DB.InTx], [DB.ReadyCheck] or the pool of [DB.Pool]: errors.Is(err,
// ErrClosed). It is the pool's own error, so calls that never pass through
// this package report it too.
var ErrClosed = puddle.ErrClosedPool

// New builds a connection pool from cfg, verifies it with a single ping (so a
// misconfigured database fails fast at boot rather than on first query) and
// returns a ready [DB]. The caller owns it and must call [DB.Close]. A cfg
// that [Config.Validate] refuses is refused here with the same error.
//
// Every connection is opened with the session timeouts from cfg
// (statement_timeout, lock_timeout, idle_in_transaction_session_timeout) as
// run-time parameters; a value already in the DSN is overridden only when the
// corresponding field is non-zero. New then reads them back: a timeout that
// is configured and off on the server — a pooler accepted the parameter and
// dropped it — is [ErrGuardRailOff], not a pool without guard rails.
func New(ctx context.Context, cfg Config, log *slog.Logger, opts ...Option) (*DB, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	db, poolCfg, err := open(ctx, cfg, log, opts...)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, poolCfg.ConnConfig.ConnectTimeout)
	defer cancel()

	if err := db.pool.Ping(pingCtx); err != nil {
		db.pool.Close()
		return nil, fmt.Errorf("pgx: initial ping: %w", err)
	}

	if err := verifyGuardRails(pingCtx, db.pool, cfg, log); err != nil {
		db.pool.Close()
		return nil, err
	}

	// Log what the DSN resolved to, not the discrete fields: with
	// DATABASE_URL set those are ignored and would name the wrong server.
	log.InfoContext(ctx, "postgres pool ready",
		"host", poolCfg.ConnConfig.Host, "port", poolCfg.ConnConfig.Port,
		"database", poolCfg.ConnConfig.Database, "max_conns", poolCfg.MaxConns,
		"statement_timeout", cfg.StatementTimeout.String())

	return db, nil
}

// open builds the DB without touching the network: pgxpool dials on first
// use.
func open(ctx context.Context, cfg Config, log *slog.Logger, opts ...Option) (*DB, *pgxpool.Config, error) {
	var applied options
	for _, opt := range opts {
		opt(&applied)
	}

	poolCfg, err := cfg.poolConfig()
	if err != nil {
		return nil, nil, err
	}

	// Tracing and metrics: a span per query/batch/copy/prepare/connect, child
	// of whatever span is in the query context (no-op provider → no cost
	// beyond the hook call), and the query duration / error metrics beside it.
	metrics := newQueryMetrics()

	var traceOpts []otelpgx.Option
	if applied.tp != nil {
		traceOpts = append(traceOpts, otelpgx.WithTracerProvider(applied.tp))
	}

	poolCfg.ConnConfig.Tracer = multitracer.New(otelpgx.NewTracer(traceOpts...), metrics)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("pgx: build pool: %w", err)
	}

	return &DB{pool: pool, log: log, metrics: metrics, opTimeout: cfg.OpTimeout}, poolCfg, nil
}

// readyCheckTimeout bounds the ping of [DB.ReadyCheck] whatever the caller's
// context allows. Under libs/httpx it is the tighter of two bounds for as
// long as HEALTH_CHECK_TIMEOUT is above it (its default is): the check then
// fails with its own "postgres unreachable", not with httpx's "check did not
// return".
const readyCheckTimeout = 2 * time.Second

// ErrGuardRailOff is returned by [New] when a session timeout the config asks
// for is switched off on the connection the server handed out.
var ErrGuardRailOff = errors.New("pgx: a configured session timeout is not in effect")

// verifyGuardRails checks that the session timeouts of cfg reached the
// server. They travel as startup parameters, and a connection pooler may be
// told to accept and drop those (PgBouncer's ignore_startup_parameters): the
// connection then works and the guard rails are silently gone. A timeout that
// is configured and off is an error — the alternative is finding out during
// the incident the timeout was for. One that is on with another value (set on
// the role or the database, which is how to have it behind a pooler) is
// logged and left alone.
func verifyGuardRails(ctx context.Context, pool *pgxpool.Pool, cfg Config, log *slog.Logger) error {
	for name, want := range cfg.runtimeParams() {
		var got string
		// current_setting with a literal name: a SHOW cannot be parameterised
		// and this must work through the simplest of proxies.
		if err := pool.QueryRow(ctx, `SELECT (extract(epoch FROM current_setting('`+name+`')::interval) * 1000)::bigint::text`,
			pgx.QueryExecModeSimpleProtocol).Scan(&got); err != nil {
			return fmt.Errorf("pgx: read %s: %w", name, err)
		}

		switch got {
		case want:
		case "0":
			return fmt.Errorf("%w: %s is off on the server though the config sets %sms — a pooler dropping startup parameters? "+
				"Set it on the role or database (ALTER ROLE … SET %s) and zero it here, or stop the pooler from ignoring it",
				ErrGuardRailOff, name, want, name)
		default:
			log.WarnContext(ctx, "pgx: session timeout differs from the config; the server's value stands",
				"parameter", name, "configured_ms", want, "effective_ms", got)
		}
	}

	return nil
}

// Pool returns the underlying pgx pool so services can run queries, batches and
// transactions with the full pgx API. It is the raw pool: DB_OP_TIMEOUT does
// not apply to calls made on it, only the context they are given does.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that pings the
// database under a fixed timeout of its own (readyCheckTimeout). Register it
// on a server's [httpx.Health] so /readyz turns 503 once a refresh of the
// checks finds the database unreachable. The ping needs a connection of the
// pool: when it times out with every connection in use, the error says the
// pool is exhausted instead of blaming the database.
func (db *DB) ReadyCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
		defer cancel()

		if err := db.pool.Ping(ctx); err != nil {
			if stat := db.pool.Stat(); errors.Is(err, context.DeadlineExceeded) && stat.AcquiredConns() >= stat.MaxConns() {
				return fmt.Errorf("postgres pool exhausted (%d of %d connections in use): %w",
					stat.AcquiredConns(), stat.MaxConns(), err)
			}

			return fmt.Errorf("postgres unreachable: %w", err)
		}

		return nil
	}
}

// Close releases every connection in the pool. It blocks until in-use
// connections are returned, so call it from the service's shutdown path.
// Calling it again is a no-op; any other call after it fails with
// [ErrClosed].
func (db *DB) Close() { db.pool.Close() }
