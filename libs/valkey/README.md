# valkey

Shared cache layer for golang-basics services over the first-party
[`valkey-go`](https://github.com/valkey-io/valkey-go) client: configuration,
readiness, per-command tracing and metrics, and the cache-aside helpers a
service would otherwise re-implement — with the two races that usually come
with them (stampedes and stale write-backs) closed.

## Usage

```go
c, err := valkey.New(cfg.Valkey, logger) // connects and PINGs: a server that is down is an error here
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
  caller's cancellation (keeping its deadline and trace). That deadline is
  the first caller's alone: a caller that joined and still has time when it
  runs out loads again, once, under its own context. One key is one type: a
  caller that joins a load of another `T` gets an error.
- **TTL jitter** of ±10% (`resilience.Jitter`), so keys filled together do
  not expire together.
- **The fill is `SET NX`**: it never overwrites a value or a tombstone.
- The cache never turns a readable value into an error: a failed lookup, a
  corrupt entry or a failed fill is a miss or a warning; the loader's own
  error (e.g. not found) is returned as is and nothing is cached.
- **A corrupt entry is replaced**, not left for its TTL: an entry that does
  not decode into `T` (the format changed, the key did not) is overwritten
  with the loaded value by compare-and-set against the bytes that were read
  — still never over a tombstone or a newer value.
- **A cache that does not answer costs one `op_timeout` per call**: after a
  failed lookup the fill is skipped. Its warnings (`lookup failed`, `cache
  write failed`) are logged once per ten seconds each (a constant, not a
  key), with a `suppressed` count; `cache_command_errors_total` has the rate.
- **A panic in the loader is an error** wrapping `valkey.ErrLoadPanic` for
  every waiter, logged with its stack. (It would otherwise be re-raised on a
  goroutine of singleflight's, past any recover of the caller's.)
- A caller whose context is already over gets its context's error: nothing
  is loaded for it and nothing is logged as a cache failure.
- **The key is logged whole** — in the warnings above, in the loader-panic
  line and in the error texts of `Aside`. Keep secrets and personal data out
  of keys.

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

- `Replace(key, old, value, ttl)` is an atomic compare-and-set: it writes only
  over exactly `old`, so a refresh cannot overwrite a tombstone or a newer
  value. `compare_and_set: ifeq` (the default) uses `SET … IFEQ` on Valkey 8.1+;
  `compare_and_set: lua` supports Valkey 7.2 and needs `EVAL` in the user's ACL.
- `Publish` / `Subscribe` are at most once. `Subscribe` reconnects and
  resubscribes by itself; its `onSubscribed` callback runs each time, and is
  the only sign that messages may have been missed in between.

Both are what [`libs/tiercache`](../tiercache) is built on.

## Configuration

`valkey.Config` carries `env` and `yaml` tags; a service embeds it (no
`envPrefix` — the keys carry `VALKEY_`):

| Env (`TASKS_` prefix) | YAML | Default | |
| --- | --- | --- | --- |
| `TASKS_VALKEY_URL` | `url` | — | `valkey://[user:pw@]host:6379[/db]` (`valkeys://`: TLS); wins over the discrete fields. Several seed addresses (`addr=`) select a cluster; `master_set=` selects Sentinel. No default address: `Config.Configured()` is false until URL or ADDR is set. The field is a `secret.String` (the URL holds the password): hand-built as `secret.New(url)`, never printed by a formatter, a logger or `/admin/config` |
| `TASKS_VALKEY_ADDR` / `_USERNAME` / `_PASSWORD` / `_DB` | `addr` / `username` / `password` / `db` | — / — / — / `0` | the user is an ACL user; empty is the server's default user |
| `TASKS_VALKEY_TLS` | `tls` | `false` | connect over TLS; applies to a `valkey://` URL too |
| `TASKS_VALKEY_CA_FILE` | `ca_file` | — | PEM bundle that verifies the server; it is the trust store (the system roots are not consulted). Needs TLS on — the flag or a TLS scheme — otherwise a config error |
| `TASKS_VALKEY_CERT_FILE` / `_KEY_FILE` | `cert_file` / `key_file` | — | client certificate and its private key (PEM) for a server that asks for one (mTLS). Both or neither, and only with TLS on. Read when the client is built: a rotated pair needs a restart |
| `TASKS_VALKEY_DIAL_TIMEOUT` | `dial_timeout` | `5s` | also bounds the boot PING |
| `TASKS_VALKEY_OP_TIMEOUT` | `op_timeout` | `500ms` | every command of the cache's own methods; past it the caller treats the cache as a miss |
| `TASKS_VALKEY_COMPARE_AND_SET` | `compare_and_set` | `ifeq` | `Replace`: `SET IFEQ` on Valkey 8.1+, or `lua` for Valkey 7.2 (requires `EVAL` permission) |
| `TASKS_VALKEY_PIPELINE_CONNS` | `pipeline_conns` | `4` | shared connections every ordinary command is pipelined over; a power of two, 1–256 |
| `TASKS_VALKEY_POOL_SIZE` | `pool_size` | `32` | dedicated connections (blocking commands, pipelines ≥ 2000, `Dedicated`), at most |
| `TASKS_VALKEY_POOL_MIN_IDLE` / `_POOL_IDLE_TIMEOUT` | `pool_min_idle` / `pool_idle_timeout` | `0` / `5m` | idle dedicated connections kept / closed after (`0`: never) |
| `TASKS_VALKEY_CONN_WRITE_TIMEOUT` | `conn_write_timeout` | `10s` | a connection whose peer is silent this long is dropped and replaced |
| `TASKS_VALKEY_CONN_LIFETIME` | `conn_lifetime` | `1h` | recycle a pipelined connection at this age (`0`: never) |

Topologies are selected by the URL, with no second set of config keys:

```text
standalone: valkey://cache:6379/0
cluster:    valkey://cache-a:6379?addr=cache-b:6379&addr=cache-c:6379
sentinel:   valkey://sentinel-a:26379?addr=sentinel-b:26379&master_set=primary
```

The discrete `addr` form is standalone. A cluster uses DB 0 and discovers the
rest of its nodes from any live seed. A Sentinel URL's addresses are sentinels;
the userinfo authenticates to the data primary, and the client reconciles its
topology with Sentinel every five seconds as well as following failover events.
TLS, the CA file and the client key pair apply to both Sentinel and data
connections.

`Config.Validate()` reports everything `New` rejects before it dials: no
address, a value the library would reinterpret (a negative size or duration,
a count that is not a power of two), a URL that does not parse, a nonzero DB
for several cluster seeds, a CA file or a client key pair that cannot be read.
`LoadConfig` / `LoadConfigFrom` call it when an address is set, `New` always;
every such error wraps `valkey.ErrConfig`. The error for a URL never quotes
the URL — it is where the password is.

`New` takes no context: it cannot be cancelled, and a dial and the boot PING
are each given `dial_timeout`. A server that is down at boot is an error from
`New`, not a cache that connects later.

After `Close` every method of `Cache` fails with an error wrapping
`valkey.ErrClosed` (`Aside` still answers from its loader); calls through
`Cache.Client()` fail with valkey-go's own `ErrClosing`.

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
  `Dedicated` / `Dedicate` take no context in valkey-go, so there the wait
  has no way out but a connection given back.

So one client holds at most `pipeline_conns + pool_size` connections; multiply
by the replicas and keep it under the server's `maxclients`.

`pool_size` is enforced by this package, not left to the library
(`bounded.go`): in valkey-go ≤ 1.0.79 a caller whose context ends while it
waits for a dedicated connection raises the pool's limit by one for good, so
the limit dissolves under the overload it exists for — in the test, two
connections allowed, seven opened after ten such callers. A test asserts that
defect against the bare library, and will fail when an upgrade fixes it. Both
workarounds of this package — this one and the retry below — are listed in
[docs/UPSTREAM-WORKAROUNDS.md](../../docs/UPSTREAM-WORKAROUNDS.md). The
streaming calls (`DoStream`, `DoMultiStream`) would hold connections of a
second library pool with the same flaw until the caller drains the stream —
something a wrapper cannot observe — so `Cache.Client()` refuses them: the
returned stream carries `ErrStreamUnsupported`. Nothing reachable through the
client is outside the bound.

The client does not retry a command. valkey-go would retry read-only ones
after a connection error, but in ≤ 1.0.79 that retry recycles the command
while the failed connection's writer may still read it — a data race
(`TestReadsUnderConnectionFaultsDoNotRace` shows it under `-race` when the
retry is on). So a read that hits a broken connection returns its error
once; `Aside` and `tiercache` treat that as a miss.

`conn_write_timeout` is not `op_timeout`: the second bounds what one caller
waits, the first how long the client keeps sending into a connection whose
peer stopped answering (a dropped route — nothing closes the socket). All of
this is tested against a real server in `integration/pool_test.go`, the
silent connection through Toxiproxy.

## Metrics

| Metric | Meaning |
| --- | --- |
| `cache_lookups_total{result="hit\|miss\|error"}` | GET outcomes (a tombstone is a miss) |
| `cache_command_duration_seconds{operation="get\|set\|del\|publish"}` | command latency |
| `cache_command_errors_total{operation}` | failed commands, timeouts included (a missing key is not a failure) |

Every command is also a client span (valkeyotel) in the caller's trace.

The series carry no label that tells one `Cache` from another. A process
with two caches registers each through a registerer that adds one — each,
not only the second: one metric name carries one set of label names, and a
bare cache beside a wrapped one is refused just the same:

```go
prometheus.WrapRegistererWith(prometheus.Labels{"cache": "sessions"}, srv.Metrics.Registry).
	MustRegister(sessions.Collectors()...)
prometheus.WrapRegistererWith(prometheus.Labels{"cache": "tasks"}, srv.Metrics.Registry).
	MustRegister(tasks.Collectors()...)
```

## Tests

`go test ./...` here needs no Docker: configuration, the pool guard, and the
behaviour of `Cache` and `Aside` that a healthy server never shows — a server
that stops answering (`op_timeout`, the readiness timeout), a refused
subscription, a corrupt entry, a panicking loader, `Close`, TLS with a CA
file and a client certificate — against a RESP3 listener inside the test (`fake_test.go`).

What only Valkey can answer is in [`integration/`](integration) (a module of
its own, so that testcontainers and the Docker client stay out of this
`go.mod`): round trips and TTLs, `SET NX` vs tombstones, compare-and-set, Pub/Sub and
resubscription, what the server sees of the pool, spans and metrics, mTLS with
the configured CA and client key pair, and the outage cases through Toxiproxy.
