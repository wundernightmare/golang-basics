# tiercache

A two-level read cache for values that are read far more often than they
change: a fixed-size, **off-heap L1** inside the process
([`fastcache`](https://github.com/VictoriaMetrics/fastcache)) in front of a
shared **L2** (Valkey, through [`libs/valkey`](../valkey)), in front of the
source of truth (a loader).

It is assembled from tested parts — the store, the client, singleflight, the
TTL jitter and the tombstone scheme — and owns only the envelope and the read
rules.

## Usage

```go
remote, err := valkey.New(cfg.Valkey, logger) // optional: see "Without Valkey"

creds, err := tiercache.New(cfg.Credentials, credential.Codec{}, store.CredentialByKeyHash, remote, logger)
if err != nil { … }
defer creds.Close()
srv.Metrics.Registry.MustRegister(creds.Collectors()...)

// read path
c, err := creds.Get(ctx, tiercache.KeyOf(apiKey)) // errors.Is(err, tiercache.ErrNotFound)

// write path — after the transaction has committed
if err := creds.Invalidate(ctx, tiercache.KeyOf(apiKey)); err != nil {
	log.WarnContext(ctx, "other processes keep the old value until its TTL", "err", err)
}
```

Keys are SHA-256 hashes (`tiercache.Key`, a `[32]byte`); the loader receives
the hash. Values go through a `Codec[T]` — [`credential`](credential) is the
worked protobuf example (`id`, `crm_id`, `key_encrypted`).

## Read rules

Both levels store the same bytes: an envelope of two deadlines in front of
the codec's payload. Nothing is evicted for age — an expired entry is ignored
on read and overwritten by the ring buffer in due course.

| Age of the L1 entry | What `Get` does |
| --- | --- |
| `< SoftTTL` | returns it: no I/O, no exclusive lock, no allocation beyond the decoded value |
| `SoftTTL … HardTTL` | returns it and refreshes it in the background (stale-while-revalidate): one refresh per key, `RefreshConcurrency` in all |
| `≥ HardTTL`, or absent | ignores it: L2, then the loader — one lookup per key however many callers, each honouring its own context |
| … and the loader fails | returns it for `StaleIfError` more (off by default); beyond that, the loader's error |

`SoftTTL == HardTTL` (the default) is plain cache-aside. Deadlines carry the
±10% jitter of `valkey.Jitter`. `ErrNotFound` is not cached.

## Without Valkey

L1 is the always-on level. Pass the untyped `nil` as the remote and the cache
runs on L1 and the loader alone. With a remote that is unreachable, slow or
cutting connections, no read fails either: every L2 command has its own
deadline (`VALKEY_OP_TIMEOUT`), the circuit breaker of
[`libs/resilience`](../resilience) skips L2 for `BreakerOpenTimeout` once
`BreakerFailureRatio` of its recent calls failed, and the loader answers. What is
lost meanwhile is sharing between processes and invalidation, both bounded
by the TTL; when the subscription comes back, L1 starts over.

## Invalidation

`Invalidate(key)` drops the key from this process's L1, replaces it in Valkey
with a tombstone (which refuses the fills of readers that loaded the old row —
the scheme of `libs/valkey`), and publishes it on `tc:<name>:inv` so every
other process drops it from its L1. Three races are closed and tested:

- a load that straddles the commit is not written to L2 (tombstone) nor L1;
- a value read from L2 just before the invalidation is not copied into L1
  just after it (fills are ordered against invalidations by an epoch);
- a refresh replaces only the value it started from (`SET … IFEQ`).

Pub/Sub is at most once: each time the subscription is re-established, L1 is
emptied. For `TombstoneTTL` after an invalidation the key is not cached at
all — reads go to the loader. `DisableInvalidation` runs on TTLs alone.

## Memory

| | |
| --- | --- |
| Capacity | `MaxBytes`, fixed at construction, in steps of 32 MiB and never less: 16 MiB and 33 MiB mean 32 and 64 |
| Where | off the Go heap (`mmap`): invisible to the garbage collector and to `GOGC` |
| Resident size | grows to the capacity and stays: the store never returns memory to the OS — budget the full capacity |
| On top (heap) | the store's index, about 16 bytes per live entry |
| Entry limit | key + envelope under 64 KiB (65 531 bytes: what one chunk of the store holds, checked by a test against the store itself); larger values are served from L2 and counted in `tiercache_l1_oversize_total` |

A service with a tight budget shares one cache between its logical caches
(telling keys apart in what they hash) rather than paying 32 MiB for each.

Valkey 8.1 or newer is required (`SET … IFEQ`).

## Configuration

`tiercache.Config` carries `env` and `yaml` tags; a service embeds one per
cache with an `envPrefix`.

| Env | YAML | Default | |
| --- | --- | --- | --- |
| `TIERCACHE_NAME` | `name` | — | metric label, key prefix, channel; include the value's format version (`credentials:v1`) |
| `TIERCACHE_MAX_BYTES` | `max_bytes` | 32 MiB | L1 capacity |
| `TIERCACHE_SOFT_TTL` / `_HARD_TTL` | `soft_ttl` / `hard_ttl` | `HARD_TTL` / — | refresh after / never serve after |
| `TIERCACHE_STALE_IF_ERROR` | `stale_if_error` | `0` | grace past `HARD_TTL` while the loader fails |
| `TIERCACHE_REFRESH_TIMEOUT` / `_CONCURRENCY` | `refresh_timeout` / `refresh_concurrency` | `5s` / `16` | background refresh bounds |
| `TIERCACHE_TOMBSTONE_TTL` | `tombstone_ttl` | `5s` | a few times the slowest load |
| `TIERCACHE_BREAKER_FAILURE_RATIO` / `_MIN_REQUESTS` / `_WINDOW` | `breaker_failure_ratio` / `breaker_min_requests` / `breaker_window` | `0.5` / `10` / `10s` | when L2 is skipped: this share of at least this many calls in the window failed |
| `TIERCACHE_BREAKER_OPEN_TIMEOUT` | `breaker_open_timeout` | `5s` | for how long, before one call probes it |
| `TIERCACHE_DISABLE_INVALIDATION` | `disable_invalidation` | `false` | no Pub/Sub |

Refresh traffic does not follow the request rate: every key read at least
once per `SoftTTL` is refreshed once per `SoftTTL` per process. With
invalidation on, `SoftTTL` is a safety net and can be long.

## Metrics

Listed in [docs/METRICS.md](../../docs/METRICS.md); every series carries
`cache="<name>"`. The L1 hit ratio is `rate(fresh + stale) / rate(all)`.

## Tests

| Layer | File | What it proves |
| --- | --- | --- |
| Unit | `internal_test.go`, `credential/` | envelope and codec (table, fuzz, byte-compatibility with `google.golang.org/protobuf`), config |
| Unit, virtual time | `cache_test.go` | every read rule and race above against an in-memory L2 (`testing/synctest`: no sleeps), the allocation budget of a fresh read, the L1 capacity bound |
| Integration | `integration_test.go` | the same cache on a real Valkey: TTLs, `SET NX` / `IFEQ`, tombstones, Pub/Sub, values from 0 bytes to 4 MiB |
| Chaos | `chaos_test.go` | Valkey down, frozen, slow, black-holed, resetting, cutting replies mid-value: every read answers correctly within a bound, and L2 is picked up again |
| Soak | `chaos_test.go` | two processes reading and invalidating under flapping faults: no failed read, no foreign or uncommitted value, convergence afterwards |
| Property | `property_test.go`, `internal_test.go` | for any history of commits, invalidations, passing time and reads across two processes: a read never fails, never returns another key's value or an uncommitted version, never one older than the last invalidation or than the hard TTL allows; the codec and the envelope round-trip any value (rapid, virtual time) |
| Mutation | `.gremlins.yaml`, nightly | whether the unit tests notice a deadline or a size limit off by one (`just mutate libs/tiercache` from the repo root) |

Integration, chaos and soak need Docker and skip under `-short`.
