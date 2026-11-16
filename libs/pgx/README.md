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
| `TASKS_DATABASE_URL` | `url` | — | full DSN; wins over the discrete fields. A `secret.String`: it carries the password, and printing the config does not print it |
| `TASKS_DB_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_NAME` / `_SSLMODE` | `host` … `sslmode` | `localhost` / `5432` / `app` / `app` / `app` / `disable` | |
| `TASKS_DB_PASSWORD_FILE` | `password_file` | — | a mounted secret, read for every new connection — a rotated mount reaches the connections opened after it, no restart; wins over `DB_PASSWORD` and the URL's password (trailing newline dropped) |
| `TASKS_DB_MAX_CONNS` / `_MIN_CONNS` | `max_conns` / `min_conns` | `10` / `0` | pool size; in a hand-built `Config` a zero `MaxConns` is pgxpool's own default |
| `TASKS_DB_MAX_CONN_LIFETIME` / `_MAX_CONN_IDLE` | … | `1h` / `30m` | recycling |
| `TASKS_DB_CONNECT_TIMEOUT` | `connect_timeout` | `5s` | dial + boot ping (zero → 5s, never "expire at once") |
| `TASKS_DB_STATEMENT_TIMEOUT` | `statement_timeout` | `30s` | Postgres cancels a statement running longer |
| `TASKS_DB_LOCK_TIMEOUT` | `lock_timeout` | `5s` | … or waiting longer for a lock |
| `TASKS_DB_IDLE_IN_TX_TIMEOUT` | `idle_in_transaction_session_timeout` | `60s` | … and ends a session left idle inside a transaction |
| `TASKS_DB_OP_TIMEOUT` | `op_timeout` | `0` (none) | client-side deadline of one call — see [Deadlines](#deadlines) |

`DB_HOST` takes an IPv6 literal with or without brackets.

`Config.Validate()` checks, without connecting, everything `New` would refuse
on the config alone: a DSN pgx cannot parse (host, port, `sslmode`, TLS
files), negative sizes and durations, `DB_MIN_CONNS` above `DB_MAX_CONNS`, a
password file that is missing or empty. `LoadConfig` / `LoadConfigFrom` and
`New` run it; the error never contains the password.

The three timeouts are sent as run-time parameters on every connection
(`SHOW statement_timeout` reports them); zero leaves the server's setting.
They are the database's own guard rails against a runaway query, a migration
queued behind a long lock, or a code path that forgot to commit.

## Deadlines

The session timeouts are enforced by the server, so they bound only what the
server sees. Two waits it does not see last as long as the context of the
call: waiting for a free connection when the pool is exhausted, and waiting
for an answer over a network path that went dead after the connection was
made. A request context usually has a deadline; a worker's, a relay's or
`context.Background()` does not.

`DB_OP_TIMEOUT` is the client-side bound for those. When set, every call on
`db.Conn(ctx)` — in a unit of work or outside — runs under that deadline on
top of the context's own: getting a connection, the statement, and reading
its result (until `Rows` are closed or read to the end, a `Row` is scanned,
`BatchResults` are closed). `InTx` puts the same deadline on its begin, which
is what waits for a connection, and on its commit; the unit as a whole is not
bounded by it. Keep it above `DB_STATEMENT_TIMEOUT`, so that the server's
cancellation answers first: its error names the cause and the connection
stays usable, while a client-side deadline that fires mid-statement closes
the connection.

| Call | Client-side bound |
| --- | --- |
| `db.Conn(ctx).…`, begin and commit of `db.InTx` | `ctx`, and `DB_OP_TIMEOUT` when set |
| `db.Pool().…`, `pgx.Migrate` | `ctx` only — the raw pool |
| opening a connection | `DB_CONNECT_TIMEOUT` |
| `db.ReadyCheck()` | 2s of its own |
| rollback of a failed unit | 5s of its own, detached from a cancelled `ctx` |

The default is no `DB_OP_TIMEOUT`: such calls are then bounded by their
context alone.

After `db.Close()` every call fails with an error for which
`errors.Is(err, pgx.ErrClosed)` holds — through `Conn`, `InTx`, `ReadyCheck`
or the raw pool.

`ReadyCheck` needs a connection of the pool. When its ping times out with
every connection in use it reports `postgres pool exhausted (N of N
connections in use)`, otherwise `postgres unreachable`.

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
- a runner that finds the lock taken waits for as long as its `ctx` allows:
  `lock_timeout` and `statement_timeout` are lifted for that wait and put
  back for the migration;
- a misnamed `.sql` file or a duplicate version is an error, not a skip.

Forward-only: never edit a shipped file, add the next number.
`CREATE INDEX CONCURRENTLY` cannot run in a transaction and does not belong
here.

The migration itself runs under the pool's guard rails, each statement of the
file on its own: one that runs longer than `DB_STATEMENT_TIMEOUT` (30s by
default) is cancelled with `57014`, one that waits for a table lock longer
than `DB_LOCK_TIMEOUT` with `55P03`, and the run stops there. A statement
that needs longer says so in its file — the setting lasts until the end of
that migration's transaction:

```sql
SET LOCAL statement_timeout = '10min';
CREATE INDEX tasks_owner_idx ON tasks (owner_id);
```

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

## Behind PgBouncer

`integration/pgbouncer_test.go` runs the library through a real PgBouncer (1.24, built
from `integration/testdata/pgbouncer/Dockerfile`) in front of the shared Postgres, in
transaction and in session pooling, with PgBouncer's defaults and with a tuned
configuration. What it found, per setting:

| Setting / feature | Session pooling | Transaction pooling |
| --- | --- | --- |
| `DB_STATEMENT_TIMEOUT`, `DB_LOCK_TIMEOUT`, `DB_IDLE_IN_TX_TIMEOUT` (sent as startup parameters) | **Not through a stock PgBouncer**: the connection is refused (`unsupported startup parameter`). **Unsafe with `ignore_startup_parameters`**: the connection is accepted and the timeouts are dropped — in both modes. `New` reads them back and fails with `ErrGuardRailOff` instead of starting without guard rails. | same |
| The same timeouts set on the role or database (`ALTER ROLE app SET statement_timeout = '30s'`), and `0` in the config | **Safe**: in effect on every server connection | **Safe** |
| pgx's default statement cache (named prepared statements) | **Safe**: one server connection per client | **Unsafe with `max_prepared_statements = 0`**: the statement is executed on a connection that never prepared it (`26000`), and it stays on the connection that did, where the next client to prepare the same SQL — a restarted pod, another replica — is refused (`42P05`: pgx names a statement by a hash of its SQL). **Safe with `max_prepared_statements > 0`** (PgBouncer ≥ 1.21 re-prepares), or with `default_query_exec_mode=exec` / `simple_protocol` in the DSN |
| `search_path` in the DSN | Refused by a stock PgBouncer. **Safe with `track_extra_parameters = search_path`** (PostgreSQL 18 reports it, so PgBouncer can carry it to whichever server connection serves the client) | same |
| A session-level `SET` | **Safe**: lasts for the client's connection; `server_reset_query` wipes it before the next client | **Unsafe**: stays on the server connection and is inherited by the next client. Use `SET LOCAL` inside `InTx` |
| `pg_advisory_lock` (session) | Safe | **Unsafe**: the unlock is served by another connection, returns false, and the lock stays held. Use `pg_advisory_xact_lock` — which `Migrate` does |
| `Migrate`, concurrent runners | Safe | **Safe** (transaction-scoped lock) |
| `InTx`, savepoints, `AfterCommit`, batches, `CopyFrom` | Safe | **Safe**: a transaction stays on one server connection |
| Context cancellation | **Safe**: the cancel request reaches the backend and the query stops | **Safe** |
| Pool size | PgBouncer's `pool_size` is the real limit, not `DB_MAX_CONNS`. A query that finds no server connection waits in PgBouncer: a context deadline ends the wait, `query_wait_timeout` ends it otherwise | same — and a unit of work holds a server connection for as long as it runs |

The setup the tests arrive at:

```ini
; pgbouncer.ini
pool_mode = transaction
max_prepared_statements = 200            ; pgx's statement cache keeps working
track_extra_parameters = IntervalStyle,search_path
query_wait_timeout = 5                   ; never wait forever for a server connection
; no ignore_startup_parameters for the timeouts: set them on the role instead
```

```sql
ALTER ROLE app SET statement_timeout = '30s';
ALTER ROLE app SET lock_timeout = '5s';
ALTER ROLE app SET idle_in_transaction_session_timeout = '60s';
```

```sh
DB_STATEMENT_TIMEOUT=0 DB_LOCK_TIMEOUT=0 DB_IDLE_IN_TX_TIMEOUT=0   # the role's values stand
```

Not covered: `LISTEN` / `NOTIFY` (the library does not use them; they need
session pooling), PgBouncer's own TLS and `auth_query`, and statement pooling
mode (it forbids multi-statement transactions, so `InTx` cannot work there).

## Metrics

| Metric | Meaning |
| --- | --- |
| `db_pool_connections{state}`, `db_pool_max_connections` | connections idle / in_use / constructing, and the ceiling |
| `db_pool_acquires_waited_total`, `db_pool_acquire_wait_seconds_total`, `db_pool_acquires_canceled_total` | acquires that found no idle connection, time spent acquiring, acquires given up — "exhausted" is waited acquires climbing while in_use sits at the max |
| `db_query_duration_seconds{operation}` | Exec / Query / QueryRow / SendBatch / CopyFrom latency; a batch is one observation, from `SendBatch` to the `Close` of its results |
| `db_query_errors_total{operation}` | of those, the failed ones |

`operation` is the name set with `pgx.WithOperation(ctx, "tasks.create")`, or
the statement's leading keyword (`select`, `insert`, …; `other` outside the
list), `batch` for a `SendBatch`, `copy` for a `CopyFrom` — never the SQL
text, so the label set stays bounded. `begin`, `commit`, `rollback`,
`savepoint` and `release` are always under their keyword: `InTx` sends them
with the caller's context, and under its name they would count as that
operation's queries. A call that never got a connection is in neither query
metric; one that gave up waiting for it shows in
`db_pool_acquires_canceled_total`.

**Two pools in one process** (primary and replica) have the same metric
names, and registering the second on the same registry fails. Wrap the
registry for each of them — both, not only the second, or the label sets
differ and the registration fails just the same:

```go
for name, db := range map[string]*pgx.DB{"primary": primary, "replica": replica} {
    prometheus.WrapRegistererWith(prometheus.Labels{"pool": name}, srv.Metrics.Registry).MustRegister(db.Collectors()...)
}
```

The metrics tracer runs beside
otelpgx (a client span per query, child of the caller's span) through pgx's
`multitracer`. The names follow the workspace's metrics contract
([docs/METRICS.md](../../docs/METRICS.md)); what the server itself sees
(backends, locks, replication) comes from postgres-exporter, not from here.

## Tests

`go test ./...` here needs no Docker and pulls none into `go.mod`:
configuration and `Validate`, the DSN, migration files, the metric labels,
`DB_OP_TIMEOUT` / `DB_CONNECT_TIMEOUT` / the readiness timeout against a
server that accepts and never answers, `ErrClosed`, two pools on one
registry.

Everything that needs a database is the module next door,
[`integration/`](integration) — container-backed (one Postgres per test
binary, a fresh schema per test via `search_path`), skipped under `-short`:
migration runner (fresh / re-run / concurrent runners / a runner outwaiting
the session timeouts / failing file), session timeouts applied and enforced,
password file re-read, spans and metrics, promlint, an exhausted pool; the
unit of work — commit, rollback on error / panic / cancelled context, joined
and savepoint nesting, nested options refused, `AfterCommit` ordering and a
panicking hook, a real serialization conflict (retried, and surfacing on
`COMMIT` without retry), a real deadlock retried, a pool of one connection,
`statement_timeout` and `idle_in_transaction_session_timeout` ending a unit,
two pools kept apart — and its chaos tests through Toxiproxy in front of the
same Postgres: the connection lost mid-unit, the reply to `COMMIT` lost, a
deadline on a slow link. `pgbouncer_test.go` there runs the library behind
PgBouncer in both pooling modes — see [Behind PgBouncer](#behind-pgbouncer).
