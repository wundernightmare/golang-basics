# tiercache

A two-level read cache for values that are read far more often than they
change: a fixed-size **L1** inside the process — off the heap
([`fastcache`](https://github.com/VictoriaMetrics/fastcache)) from 32 MiB up,
on it ([`freecache`](https://github.com/coocood/freecache)) below — in front
of a shared **L2** (Valkey, through [`libs/valkey`](../valkey)), in front of the
source of truth (a loader).

It is assembled from tested parts — the store, the client, singleflight, the
TTL jitter and the tombstone scheme — and owns only the envelope and the read
rules.

## Usage

```go
remote, err := valkey.New(cfg.Valkey, logger) // optional: see "Without Valkey"

creds, err := tiercache.New(cfg.Credentials, credential.Codec{}, store.CredentialByKeyHash, remote, logger)
users, err := tiercache.New(cfg.Users, user.Codec{}, store.UserByID, remote, logger)
if err != nil { … }
defer creds.Close()
srv.Metrics.Registry.MustRegister(creds.Collectors()...)

// read path
c, err := creds.Get(ctx, tiercache.KeyOf(apiKey)) // errors.Is(err, tiercache.ErrNotFound)

// write path — after the transaction has committed
if err := creds.Invalidate(ctx, tiercache.KeyOf(apiKey)); err != nil {
	log.WarnContext(ctx, "other processes keep the old value until its TTL", "err", err)
}

u, err := users.Get(ctx, userID) // a key is a string: the loader is called with it
```

## Keys

A key is any string of 1 to 512 bytes (`tiercache.MaxKeyLen`; anything else
is `tiercache.ErrKey`): a number, a UUID (36 bytes), `tenant:7/user:42`.
512 bytes are 512 ASCII characters, 256 Cyrillic ones, about 170 CJK ones,
128 emoji.

It is not hashed on the way. The loader is called with it, L2 holds the value
under `tc:<name>:<key>`, and an invalidation publishes it on `tc:<name>:inv`.
Two consequences:

- **A key is readable by whoever can read Valkey** (`MONITOR`, the slow log,
  a dump, the channel). Never make one out of a secret: an API key, a token,
  a session id go through `tiercache.KeyOf` — their SHA-256, 32 bytes — and
  the loader looks the row up by that hash. So does an identifier that may be
  longer than 512 bytes.
- **A key counts towards the L1 entry limit** below, together with its value.

Values go through a `Codec[T]` — [`credential`](credential) is the
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
other process drops it from its L1. Four races are closed and tested:

- a load that straddles the commit is not written to L2 (tombstone) nor L1;
- a value read from L2 just before the invalidation is not copied into L1
  just after it (fills are ordered against invalidations by an epoch);
- a refresh replaces only the value it started from (`SET … IFEQ`);
- a read that begins after the invalidation does not join a lookup that
  began before it (a miss or a background refresh still holding the old
  row): once `Invalidate` has returned, the process reads its own write. The
  callers already waiting for that lookup get its answer — they asked before
  the write.

Pub/Sub is at most once: each time the subscription is re-established, L1 is
emptied. For `TombstoneTTL` after an invalidation the key is not cached at
all — reads go to the loader. `DisableInvalidation` runs on TTLs alone.

## Memory

| | |
| --- | --- |
`MaxBytes` is the capacity, fixed at construction, and it also chooses what
L1 is made of:

| | `MaxBytes` ≥ 32 MiB | `MaxBytes` < 32 MiB |
| --- | --- | --- |
| Store | `fastcache` | `freecache` |
| Capacity | in steps of 32 MiB: 33 MiB mean 64 | exactly what is asked, 512 KiB at least |
| Where | off the Go heap (`mmap`): invisible to the garbage collector, to `GOGC` and to the Go memory limit | on the Go heap, allocated in full at construction: the memory limit counts it, the collector does not scan it (no pointers) |
| Resident size | grows to the capacity and stays: never returned to the OS — budget the full capacity | grows to the capacity as the rings are written |
| On top (heap) | the index, about 16 bytes per live entry | the index: 1 MiB at construction, then 16 bytes per entry |
| Entry limit (key + envelope) | 65 531 bytes | 1/1024 of the capacity less 24 bytes: 4 072 at 4 MiB, 16 360 at 16 MiB, 488 at 512 KiB |
| Forgets | the oldest written | the least recently read, roughly |
| `tiercache_l1_bytes` | what it has taken so far | the capacity |

An entry over the limit is served from L2 and counted in
`tiercache_l1_oversize_total`; both limits are checked by a test against the
stores themselves. Choose the heap store for a service whose container is
small (a few MiB of cache, values of a few KiB), the off-heap one when the
cache is large enough for the collector's accounting of it to matter.

The Go memory limit does not see the off-heap memory. `libs/httpx` derives the limit
from the container's (`RUNTIME_MEMORY_LIMIT_RATIO`, 0.85) and leaves the rest
as headroom, and a full L1 is taken out of that headroom: 32 MiB of the
38 MiB a 256 MiB container has. Subtract the capacity of every cache from
the container limit before choosing the ratio —
[docs/RUNTIME.md](../../docs/RUNTIME.md), "Choosing the ratio".

A service with a tight budget shares one cache between its logical caches
(telling keys apart by a prefix) rather than paying for each.

Valkey 8.1 or newer is required (`SET … IFEQ`).

## Configuration

`tiercache.Config` carries `env` and `yaml` tags; a service embeds one per
cache with an `envPrefix`.

| Env | YAML | Default | |
| --- | --- | --- | --- |
| `TIERCACHE_NAME` | `name` | — | metric label, key prefix, channel; include the value's format version (`credentials:v1`) |
| `TIERCACHE_MAX_BYTES` | `max_bytes` | 32 MiB | L1 capacity; below 32 MiB the heap store (see "Memory") |
| `TIERCACHE_SOFT_TTL` / `_HARD_TTL` | `soft_ttl` / `hard_ttl` | `HARD_TTL` / — | refresh after / never serve after |
| `TIERCACHE_STALE_IF_ERROR` | `stale_if_error` | `0` | grace past `HARD_TTL` while the loader fails |
| `TIERCACHE_REFRESH_TIMEOUT` / `_CONCURRENCY` | `refresh_timeout` / `refresh_concurrency` | `5s` / `16` | background refresh bounds |
| `TIERCACHE_TOMBSTONE_TTL` | `tombstone_ttl` | `5s` | a few times the slowest load |
| `TIERCACHE_BREAKER_FAILURE_RATIO` / `_MIN_REQUESTS` / `_WINDOW` | `breaker.failure_ratio` / `breaker.min_requests` / `breaker.window` | `0.5` / `10` / `10s` | when L2 is skipped: this share of at least this many calls in the window failed |
| `TIERCACHE_BREAKER_OPEN_TIMEOUT` / `_HALF_OPEN_PROBES` | `breaker.open_timeout` / `breaker.half_open_probes` | `5s` / `1` | for how long, and how many calls then probe it |
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
| Unit, virtual time | `cache_test.go` | every read rule and race above against an in-memory L2 (`testing/synctest`: no sleeps), the allocation budget of a fresh read, the L1 capacity bound, what a key may be and where it travels |
| Unit | `store_test.go` | both stores at four capacities: what is set is read back, the entry limit to the byte, the capacity bound, no value under another key |
| Both stores | `store_test.go` (`TestTheSuiteHoldsOnTheHeapStore`) | the whole suite — this table, at the layer being run — once more on the heap store: the test binary runs itself with `TIERCACHE_TEST_L1_BYTES` set |
| Integration | `integration_test.go` | the same cache on a real Valkey: TTLs, `SET NX` / `IFEQ`, tombstones, Pub/Sub, values from 0 bytes to 4 MiB |
| Chaos | `chaos_test.go` | Valkey down, frozen, slow, black-holed, resetting, cutting replies mid-value: every read answers correctly within a bound, and L2 is picked up again |
| Soak | `chaos_test.go` | two processes reading and invalidating under flapping faults: no failed read, no foreign or uncommitted value, convergence afterwards |
| Property | `property_test.go`, `internal_test.go` | for any history of commits, invalidations, passing time and reads across two processes: a read never fails, never returns another key's value or an uncommitted version, never one older than the last invalidation or than the hard TTL allows; the codec and the envelope round-trip any value (rapid, virtual time) |
| Mutation | `.gremlins.yaml`, nightly | whether the unit tests notice a deadline or a size limit off by one (`just mutate libs/tiercache` from the repo root) |

Integration, chaos and soak need Docker and skip under `-short`.
