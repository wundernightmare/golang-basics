# valkey

Shared cache layer for golang-basics services over the first-party
[`valkey-go`](https://github.com/valkey-io/valkey-go) client: configuration,
readiness, per-command tracing and metrics, and the cache-aside helpers a
service would otherwise re-implement — with the two races that usually come
with them (stampedes and stale write-backs) closed.

## Usage

```go
c, err := valkey.New(cfg.Valkey, logger) // boot PING: fails fast
if err != nil { … }
defer c.Close()
srv.Health.Register("valkey", c.ReadyCheck(), httpx.Optional()) // a cache degrades, it does not gate readiness
srv.Metrics.Registry.MustRegister(c.Collectors()...)

// read path
task, hit, err := valkey.Aside(ctx, c, "task:v2:"+id, time.Minute,
	func(ctx context.Context) (Task, error) { return store.Get(ctx, id) })

// write path — after the transaction has committed
_ = c.Tombstone(ctx, "task:v2:"+id, 5*time.Second)
```

## `Aside`

- **Singleflight**: concurrent misses on one key share a single load — a hot
  key expiring is one query, not N. Each caller still honours its own
  context while it waits; the shared load runs detached from the first
  caller's cancellation (keeping its deadline and trace).
- **TTL jitter** of ±10% (`valkey.Jitter`), so keys filled together do not
  expire together.
- **The fill is `SET NX`**: it never overwrites a value or a tombstone.
- The cache never turns a readable value into an error: a failed lookup, a
  corrupt entry or a failed fill is a miss or a warning; the loader's own
  error (e.g. not found) is returned as is and nothing is cached.

## Invalidation that cannot be undone by a racing read

The classic bug: a reader misses, loads the row, the writer commits and
`DEL`s the key, then the reader writes the old row back — stale (or deleted)
data for a full TTL. Here writers call `Tombstone(key, ttl)` after commit
instead: the key holds a marker that reads as a miss and that fills (`SET
NX`) cannot overwrite, so the racing write-back is refused. Reads during the
tombstone's TTL go to the source of truth; the first one after it re-fills.
Pick a tombstone TTL a few times the slowest load (seconds). If the tombstone
write itself fails (cache down), the entry's TTL bounds the staleness — log
it.

## Configuration

`valkey.Config` carries `env` and `yaml` tags; a service embeds it (no
`envPrefix` — the keys carry `VALKEY_`):

| Env (`TASKS_` prefix) | YAML | Default | |
| --- | --- | --- | --- |
| `TASKS_VALKEY_URL` | `url` | — | `valkey://[:pw@]host:6379[/db]`; wins over the discrete fields |
| `TASKS_VALKEY_ADDR` / `_PASSWORD` / `_DB` | `addr` / `password` / `db` | `localhost:6379` / — / `0` | |
| `TASKS_VALKEY_DIAL_TIMEOUT` | `dial_timeout` | `5s` | also bounds the boot PING |
| `TASKS_VALKEY_OP_TIMEOUT` | `op_timeout` | `500ms` | every command; past it the caller treats the cache as a miss |

## Metrics

| Metric | Meaning |
| --- | --- |
| `cache_lookups_total{result="hit\|miss\|error"}` | GET outcomes (a tombstone is a miss) |
| `cache_command_duration_seconds{op="get\|set\|del"}` | command latency |
| `cache_command_errors_total{op}` | failed commands, timeouts included (a missing key is not a failure) |

Every command is also a client span (valkeyotel) in the caller's trace.

## Tests

`go test ./...` — container-backed (one Valkey per test binary, keys by
unique prefix, Toxiproxy for the outage case), skipped under `-short`:
round trips and TTLs, `SET NX` vs tombstones, singleflight, per-waiter
contexts, the resurrection race, spans and metrics, fast failure when the
cache is unreachable.
