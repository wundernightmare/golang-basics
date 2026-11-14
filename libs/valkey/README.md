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

## Compare-and-set and Pub/Sub

- `Replace(key, old, value, ttl)` is `SET … IFEQ` (Valkey ≥ 8.1): it writes
  only over exactly `old`, so a refresh cannot overwrite a tombstone or a
  newer value.
- `Publish` / `Subscribe` are at most once. `Subscribe` reconnects and
  resubscribes by itself; its `onSubscribed` callback runs each time, and is
  the only sign that messages may have been missed in between.

Both are what [`libs/tiercache`](../tiercache) is built on.

## Configuration

`valkey.Config` carries `env` and `yaml` tags; a service embeds it (no
`envPrefix` — the keys carry `VALKEY_`):

| Env (`TASKS_` prefix) | YAML | Default | |
| --- | --- | --- | --- |
| `TASKS_VALKEY_URL` | `url` | — | `valkey://[:pw@]host:6379[/db]`; wins over the discrete fields. No default address: `Config.Configured()` is false until URL or ADDR is set |
| `TASKS_VALKEY_ADDR` / `_PASSWORD` / `_DB` | `addr` / `password` / `db` | — / — / `0` | |
| `TASKS_VALKEY_DIAL_TIMEOUT` | `dial_timeout` | `5s` | also bounds the boot PING |
| `TASKS_VALKEY_OP_TIMEOUT` | `op_timeout` | `500ms` | every command; past it the caller treats the cache as a miss |
| `TASKS_VALKEY_PIPELINE_CONNS` | `pipeline_conns` | `4` | shared connections every ordinary command is pipelined over; a power of two, 1–256 |
| `TASKS_VALKEY_POOL_SIZE` | `pool_size` | `32` | dedicated connections (blocking commands, pipelines ≥ 2000, `Dedicated`), at most |
| `TASKS_VALKEY_POOL_MIN_IDLE` / `_POOL_IDLE_TIMEOUT` | `pool_min_idle` / `pool_idle_timeout` | `0` / `5m` | idle dedicated connections kept / closed after (`0`: never) |
| `TASKS_VALKEY_CONN_WRITE_TIMEOUT` | `conn_write_timeout` | `10s` | a connection whose peer is silent this long is dropped and replaced |
| `TASKS_VALKEY_CONN_LIFETIME` | `conn_lifetime` | `1h` | recycle a pipelined connection at this age (`0`: never) |

A value the library would reinterpret (a negative size, a count that is not a
power of two) is an error at `New` (`valkey.ErrConfig`), before any dial.

### Connections

valkey-go does not take a connection per command. There are two kinds:

- **Pipelined** — `pipeline_conns` connections, opened at boot and shared by
  every ordinary command (all of `Cache`'s own, a subscription included).
  That number is the steady footprint on the server whatever the request
  rate: 200 concurrent callers on 2 connections stay on 2 connections.
- **Dedicated** — one per call that cannot share: a blocking command (`BLPOP`,
  `XREAD BLOCK`), a pipeline of 2000 commands or more, `Dedicated` /
  `Dedicate`, all through `Cache.Client()`. `pool_size` bounds them; a caller
  beyond it waits and gets its context's error if that ends first.

So one client holds at most `pipeline_conns + pool_size` connections; multiply
by the replicas and keep it under the server's `maxclients`.

`pool_size` is enforced by this package, not left to the library
(`bounded.go`): in valkey-go ≤ 1.0.79 a caller whose context ends while it
waits for a dedicated connection raises the pool's limit by one for good, so
the limit dissolves under the overload it exists for — in the test, two
connections allowed, seven opened after ten such callers. A test asserts that
defect against the bare library, and will fail when an upgrade fixes it. The
streaming calls (`DoStream`, `DoMultiStream`) would hold connections of a
second library pool with the same flaw until the caller drains the stream —
something a wrapper cannot observe — so `Cache.Client()` refuses them: the
returned stream carries `ErrStreamUnsupported`. Nothing reachable through the
client is outside the bound.

`conn_write_timeout` is not `op_timeout`: the second bounds what one caller
waits, the first how long the client keeps sending into a connection whose
peer stopped answering (a dropped route — nothing closes the socket). All of
this is tested against a real server in `pool_test.go`, the silent connection
through Toxiproxy.

## Metrics

| Metric | Meaning |
| --- | --- |
| `cache_lookups_total{result="hit\|miss\|error"}` | GET outcomes (a tombstone is a miss) |
| `cache_command_duration_seconds{operation="get\|set\|del\|publish"}` | command latency |
| `cache_command_errors_total{operation}` | failed commands, timeouts included (a missing key is not a failure) |

Every command is also a client span (valkeyotel) in the caller's trace.

## Tests

`go test ./...` — container-backed (one Valkey per test binary, keys by
unique prefix, Toxiproxy for the outage case), skipped under `-short`:
round trips and TTLs, `SET NX` vs tombstones, singleflight, per-waiter
contexts, the resurrection race, spans and metrics, fast failure when the
cache is unreachable.
