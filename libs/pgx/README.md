# pgx

Shared PostgreSQL access for golang-basics services: a `jackc/pgx` pool with
the cross-cutting concerns every service repeats — configuration, server-side
guard rails, readiness, tracing, metrics, versioned migrations and a light
[unit of work](#unit-of-work). It stops at "give me a healthy `*pgxpool.Pool`"
and a transaction boundary; queries, repositories and the domain
model stay in the service ([`services/tasks/internal/store`](../../services/tasks/internal/store)
is the worked example).

## Usage

```go
//go:embed *.sql            // in the service's migrations package
var FS embed.FS

db, err := pgx.New(ctx, cfg.Postgres, logger) // boot ping: fails fast on a bad DSN
if err != nil { … }
defer db.Close()
if err := pgx.Migrate(ctx, db.Pool(), migrations.FS, logger); err != nil { … }

srv.Health.Register("postgres", db.ReadyCheck())
srv.Metrics.Registry.MustRegister(db.Collectors()...)

rows, err := db.Pool().Query(pgx.WithOperation(ctx, "tasks.list"), `SELECT …`)
```

## Configuration

`pgx.Config` carries `env` and `yaml` tags; a service **embeds** it in its own
config (no `envPrefix` — the keys already carry `DATABASE_` / `DB_`), so with
the `TASKS_` prefix the keys are:

| Env | YAML | Default | |
| --- | --- | --- | --- |
| `TASKS_DATABASE_URL` | `url` | — | full DSN; wins over the discrete fields |
| `TASKS_DB_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_NAME` / `_SSLMODE` | `host` … `sslmode` | `localhost` / `5432` / `app` / `app` / `app` / `disable` | |
| `TASKS_DB_PASSWORD_FILE` | `password_file` | — | a mounted secret, read once at boot; wins over `DB_PASSWORD` and the URL's password (trailing newline dropped) |
| `TASKS_DB_MAX_CONNS` / `_MIN_CONNS` | `max_conns` / `min_conns` | `10` / `0` | pool size |
| `TASKS_DB_MAX_CONN_LIFETIME` / `_MAX_CONN_IDLE` | … | `1h` / `30m` | recycling |
| `TASKS_DB_CONNECT_TIMEOUT` | `connect_timeout` | `5s` | dial + boot ping (zero → 5s, never "expire at once") |
| `TASKS_DB_STATEMENT_TIMEOUT` | `statement_timeout` | `30s` | Postgres cancels a statement running longer |
| `TASKS_DB_LOCK_TIMEOUT` | `lock_timeout` | `5s` | … or waiting longer for a lock |
| `TASKS_DB_IDLE_IN_TX_TIMEOUT` | `idle_in_transaction_session_timeout` | `60s` | … and ends a session left idle inside a transaction |

The three timeouts are sent as run-time parameters on every connection
(`SHOW statement_timeout` reports them); zero leaves the server's setting.
They are the database's own guard rails against a runaway query, a migration
queued behind a long lock, or a code path that forgot to commit.

## Migrations

`pgx.Migrate(ctx, pool, fsys, log)` applies the `NNN_snake_name.sql` files at
the root of `fsys`, in version order:

- applied versions are recorded in `schema_migrations(version, name,
  applied_at)` in the current schema, so a re-run is a no-op;
- each file runs in its own transaction with its bookkeeping row — all or
  nothing — and a failure stops the run with the file name in the error;
- every transaction takes `pg_advisory_xact_lock` (keyed by the schema) and
  re-reads `schema_migrations`, so replicas booting together serialise and
  each version is applied exactly once; a transaction-scoped lock cannot leak
  from a crashed runner and works behind a transaction-pooling proxy;
- a misnamed `.sql` file or a duplicate version is an error, not a skip.

Forward-only: never edit a shipped file, add the next number.
`CREATE INDEX CONCURRENTLY` cannot run in a transaction and does not belong
here.

## Unit of work

A use case that touches several repositories needs one transaction around
them, and the repositories should not have to know. `InTx` is the boundary,
`Conn` is what a repository runs its SQL on, `AfterCommit` holds a side effect
back until the data is durable. No change tracking, no identity map — the unit
is the transaction.

```go
// A repository: holds the DB, never a pool or a pgx.Tx parameter.
func (r *Tasks) Insert(ctx context.Context, t Task) error {
    _, err := r.db.Conn(ctx).Exec(ctx, `INSERT INTO tasks (id, title) VALUES ($1, $2)`, t.ID, t.Title)
    return err
}

// A use case: the row, its event and the cache drop, as one unit.
err := db.InTx(ctx, func(ctx context.Context) error {
    if err := tasks.Insert(ctx, task); err != nil {
        return err
    }
    db.AfterCommit(ctx, func(ctx context.Context) { cache.Invalidate(ctx, key) })

    return outbox.Enqueue(ctx, event) // db.Conn(ctx) inside: same transaction
})
```

| | |
| --- | --- |
| `db.InTx(ctx, fn, opts…)` | Commits when `fn` returns nil; rolls back on an error (returned as it is) or a panic. |
| `db.Conn(ctx)` | The transaction of the unit `ctx` belongs to, else the pool. A `Querier`: `Exec`, `Query`, `QueryRow`, `SendBatch`, `CopyFrom`. |
| `db.AfterCommit(ctx, fn)` | Runs `fn` after the outermost commit, in registration order; never for a unit that rolled back. Outside a unit it runs at once. |
| `WithIsolation(level)`, `ReadOnly()` | Transaction mode. |
| `WithRetry(n)` | Up to `n` attempts in total on a serialization failure or deadlock (`40001`, `40P01`), with a short random pause. Each attempt calls `fn` again in a new transaction. |
| `WithSavepoint()` | A nested `InTx` runs in a savepoint: its failure undoes only its own statements and hooks. |

**Nesting.** An `InTx` inside another unit of the same `DB` joins it: one
transaction, committed by the outermost call. So a use case that is a unit on
its own composes into a larger one unchanged. An inner error must be returned
by the outer function too — a failed statement aborts the PostgreSQL
transaction, and committing it fails with `pgx.ErrTxCommitRollback`. To
survive an inner failure, run the inner unit with `WithSavepoint()`.

A nested call cannot change the transaction it joins. One that asks for more
than the outer unit gives — a stricter `WithIsolation`, or `WithRetry` — fails
with `ErrTxOptions` before its function runs, instead of quietly running with
a weaker guarantee. `ReadOnly()` on a nested call is not enforced.

**What to keep in mind.**

- The unit travels in the context, and a transaction is one connection: do
  not use that context from several goroutines at once. A context without the
  unit (`context.WithoutCancel` keeps it; a fresh `context.Background()` does
  not) runs on the pool, outside the transaction.
- A unit holds a pooled connection for as long as `fn` runs. Keep network
  calls out of it; `idle_in_transaction_session_timeout` ends one that stalls.
- `AfterCommit` is not durable: a process that dies between the commit and
  the hook never runs it. What *must* happen goes into the transaction as an
  outbox row; hooks are for what a retry or a TTL repairs (a cache drop, a
  wake-up).
- With `WithRetry`, everything `fn` does outside the database happens once
  per attempt.
- A commit that fails because the connection was lost has an unknown outcome:
  the server may have applied it (`TestChaos_InTx_CommitReplyLost` shows
  exactly that). `InTx` returns the error and runs no hooks; whoever retries
  the operation needs it to be idempotent.
- A hook that panics propagates, and the hooks registered after it do not run.
- A rollback that cannot be sent (the connection is gone) is logged at warn;
  the server ends the transaction on its own.
- The unit belongs to its `DB`: two pools in one process never share a
  transaction through a context.

## Metrics

| Metric | Meaning |
| --- | --- |
| `db_pool_connections{state}`, `db_pool_max_connections` | connections idle / in_use / constructing, and the ceiling |
| `db_pool_acquires_waited_total`, `db_pool_acquire_wait_seconds_total`, `db_pool_acquires_canceled_total` | acquires that found no idle connection, time spent acquiring, acquires given up — "exhausted" is waited acquires climbing while in_use sits at the max |
| `db_query_duration_seconds{operation}` | Exec / Query / QueryRow latency |
| `db_query_errors_total{operation}` | of those, the failed ones |

`operation` is the name set with `pgx.WithOperation(ctx, "tasks.create")`, or
the statement's leading keyword (`select`, `insert`, `begin`, …) — never the
SQL text, so the label set stays bounded. The metrics tracer runs beside
otelpgx (a client span per query, child of the caller's span) through pgx's
`multitracer`. The names follow the workspace's metrics contract
([docs/METRICS.md](../../docs/METRICS.md)); what the server itself sees
(backends, locks, replication) comes from postgres-exporter, not from here.

## Tests

`go test ./...` — container-backed (one Postgres per test binary, a fresh
schema per test via `search_path`), skipped under `-short`: migration runner
(fresh / re-run / concurrent runners / failing file), session timeouts
applied and enforced, spans and metrics, promlint; the unit of work — commit,
rollback on error / panic / cancelled context, joined and savepoint nesting,
nested options refused, `AfterCommit` ordering and a panicking hook, a real
serialization conflict (retried, and surfacing on `COMMIT` without retry), a
real deadlock retried, a pool of one connection, `statement_timeout` and
`idle_in_transaction_session_timeout` ending a unit, two pools kept apart —
and its chaos tests through Toxiproxy in front of the same Postgres: the
connection lost mid-unit, the reply to `COMMIT` lost, a deadline on a slow
link.
