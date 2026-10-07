# pgx

Shared PostgreSQL access for golang-basics services: a `jackc/pgx` pool with
the cross-cutting concerns every service repeats — configuration, server-side
guard rails, readiness, tracing, metrics and versioned migrations. It stops at
"give me a healthy `*pgxpool.Pool`"; queries, repositories and the domain
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

## Metrics

| Metric | Meaning |
| --- | --- |
| `pgxpool_conns{state}`, `pgxpool_max_conns` | connections idle / acquired / constructing, and the ceiling |
| `pgxpool_acquire_total`, `pgxpool_acquire_duration_seconds_total`, `pgxpool_empty_acquire_total`, `pgxpool_canceled_acquire_total` | acquires, time spent waiting, waits on an empty pool — "exhausted" is empty acquires climbing while acquired sits at the max |
| `pgx_query_duration_seconds{operation}` | Exec / Query / QueryRow latency |
| `pgx_query_errors_total{operation}` | of those, the failed ones |

`operation` is the name set with `pgx.WithOperation(ctx, "tasks.create")`, or
the statement's leading keyword (`select`, `insert`, `begin`, …) — never the
SQL text, so the label set stays bounded. The metrics tracer runs beside
otelpgx (a client span per query, child of the caller's span) through pgx's
`multitracer`.

## Tests

`go test ./...` — container-backed (one Postgres per test binary, a fresh
schema per test via `search_path`), skipped under `-short`: migration runner
(fresh / re-run / concurrent runners / failing file), session timeouts
applied and enforced, spans and metrics, promlint.
