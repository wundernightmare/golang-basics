# pgx / integration

`pgx` against a real PostgreSQL: the pool, the unit of work and `Migrate`,
directly, behind PgBouncer in both pooling modes, and through a network that
fails on purpose. A Go module of its own, inside `pgx`'s folder:
testcontainers, the Docker client and Toxiproxy are this module's
dependencies and never appear in `pgx`'s `go.mod`.

```sh
just mod libs/pgx/integration test   # from the repo root; needs Docker
go test -race -v ./...               # here
```

About 20 seconds. Skipped under `-short` and without Docker; **failed** when
`CI` is set and a container cannot start. On rootless podman set
`TESTCONTAINERS_RYUK_DISABLED=true` — and read the `-v` output: an `ok` with
`--- SKIP` lines has tested nothing.

## The stack

| Container | Image | Role |
| --- | --- | --- |
| postgres | `postgres:18-alpine` (shared, `libs/testx/containers`) | the database; every test owns a schema |
| pgbouncer | built from `testdata/pgbouncer/Dockerfile` (1.24), twice: stock and tuned | the pooler in front of it, session and transaction pooling |
| toxiproxy | `ghcr.io/shopify/toxiproxy` (shared) | cuts, delays and black-holes the connection to Postgres |

## What each file owns

| File | |
| --- | --- |
| `pgx_test.go` | `Migrate` (fresh, re-run, concurrent runners, a runner outwaiting the session timeouts, a failing file), session timeouts applied, the password file, spans and metrics, `DB_OP_TIMEOUT` and an exhausted pool |
| `tx_test.go` | the unit of work: commit, rollback, nesting, savepoints, `AfterCommit`, retries on real serialization failures and deadlocks |
| `tx_chaos_test.go` | the unit of work when the connection is lost, the reply to `COMMIT` is lost, the link is slow |
| `pgbouncer_test.go` | the table of `pgx`'s README "Behind PgBouncer", setting by setting |

What needs no database — configuration, labels, client-side deadlines against
a server that never answers — is in `pgx`'s own unit tests.
