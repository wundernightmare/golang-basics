# ingest

Accepts batches of events over HTTP and makes each batch durable in **at
least one of two sinks before it answers**: Kafka (`libs/kafka`) and an S3
archive (`libs/s3`). It keeps accepting while either works, holds no state on
disk (no volume, not even an `emptyDir`), and can be killed at any moment
without losing an event it acknowledged.

One binary, two roles (`INGEST_MODE`):

| Mode | What it runs | Replicas |
| --- | --- | --- |
| `ingest` (default) | `POST /events` on `:8084`, admin on `:9084` | any number, behind the ingress |
| `compact` | merges closed hours of the archive into large, deduplicated objects; admin on `:9084` only | **exactly one** (`Recreate`), or a CronJob with `COMPACT_RUN_ONCE=true` |

Contract: [`api/tsp/ingest.tsp`](../../api/tsp/ingest.tsp) →
[`api/openapi3/ingest.openapi.yaml`](../../api/openapi3/ingest.openapi.yaml).

```sh
just infra-up                      # Kafka + the local S3 store (RustFS)
just mod services/ingest run       # :8084, bucket "ingest" created on first run

curl -s localhost:8084/events -H 'content-type: application/json' -d '{
  "events": [
    {"id": "0193a1f0-7c1e-7000-8000-000000000001", "type": "page.viewed", "payload": {"path": "/"}},
    {"id": "0193a1f0-7c1e-7000-8000-000000000002", "payload": {"anything": ["json"]}}
  ]}'
# {"accepted":2}

INGEST_MODE=compact INGEST_COMPACT_RUN_ONCE=true just mod services/ingest run
```

## The guarantee, and what it costs

> An acknowledgement (200) is sent only after a sink confirmed the whole
> batch. Until then the events are in this process's memory and the client
> still holds them.

So nothing that was promised can be lost by the pod dying — OOM kill,
eviction, node loss, `kubectl delete --force`, a rollout. What dies with the
pod is requests that were never answered (the client retries them) and the
**second copy** of events the first sink already holds.

| State | Who answers | Latency | What a kill can take |
| --- | --- | --- | --- |
| both sinks up | Kafka (acks=all) | milliseconds | the archive copy of the last `ARCHIVE_LINGER` (10 s) per pod — those events are in Kafka |
| Kafka down | the archive put | `ARCHIVE_URGENT_LINGER` (1 s) + one put | nothing acknowledged |
| S3 down | Kafka | milliseconds | nothing acknowledged; the archive has a hole for the outage (counted) |
| both down | nobody: 503 + `Retry-After` | immediate once the circuits open | nothing was acknowledged |

The price is on the client: it must **retry** on a connection error, a
timeout, 429 and 503, with the **same event ids**, and everything downstream
deduplicates by id (a retry whose first attempt had succeeded stores the
event twice). The service is at-least-once; it has no way to be more.

Proved by `internal/integration`: chaos tests that freeze Kafka, cut off the
store, both, restart mid-outage, shut down under load — and `kill_test.go`,
which SIGKILLs the real binary under load and reads every acknowledged id
back from the sinks.

## How a request flows

1. The handler validates the batch against the contract and keeps every
   event's JSON byte for byte.
2. `MAX_IN_FLIGHT_BYTES` is checked: the event bytes accepted and not yet
   settled by both sinks. Past it → **429** (rather than the pod growing until
   the kernel kills it).
3. Every event is sent to Kafka (key and `event_id` header = the id), unless
   Kafka's circuit is open; the batch is appended to the open archive batch.
4. The request waits for the first sink to confirm. When Kafka fails, is
   skipped, or is slower than `KAFKA_ACK_GRACE`, the archive batch is flushed
   within `ARCHIVE_URGENT_LINGER` instead of `ARCHIVE_LINGER`.
5. After the answer the other sink still gets its copy.

Each sink has its own circuit breaker (`*_BREAKER_*`): a sink that is down is
skipped at once instead of waited on. Readiness: `kafka` and `s3` are
optional checks (one down = `degraded`, the pod stays in rotation); the one
critical check, `sinks`, fails only when both circuits are open.

## Client guidance

Send **batches** (up to 1000 events, 1 MiB by default — `HTTP_MAX_BODY_BYTES`).
While Kafka is away an answer waits for an archive flush, so a client that
sends one event at a time and awaits each gets one event per
`ARCHIVE_URGENT_LINGER`. Generate the id once per event (UUIDv7), keep it
across retries. From a browser, flush on a timer and on `pagehide` with
`fetch(..., {keepalive: true})` — whose body limit is about 64 KiB.

## The archive

```
<ARCHIVE_PREFIX>raw/dt=2026-11-10/hour=14/<unix ms>-<writer>-<seq>.evt.zst    ingest pods
<ARCHIVE_PREFIX>compacted/dt=2026-11-10/hour=14/part-<run>-<n>.evt.zst        compaction
<ARCHIVE_PREFIX>compacted/dt=2026-11-10/hour=14/_MANIFEST.json                … its commit record
<ARCHIVE_PREFIX>quarantine/raw/…                                              unreadable raw objects
```

- The hour is the service's clock at flush time, UTC — never a client
  timestamp. `<writer>` is a random id per process, so any number of pods
  (and a restarted container) write into the same hour without collisions.
- Objects per pod: one per `ARCHIVE_LINGER` normally (360/h at 10 s), one per
  `ARCHIVE_URGENT_LINGER` while Kafka is away (3600/h at 1 s), or one per
  `ARCHIVE_BATCH_BYTES` (8 MiB) when traffic fills batches faster.
- Format (`internal/shard`): one zstd stream of length-prefixed, CRC-32C'd
  records carrying the id, the type, the receive time and the event. No
  `Content-Encoding` on the object — the stored bytes are the artefact.
- **Reading an hour** (the replay tool's rule): if `_MANIFEST.json` exists,
  the events are its `parts` plus any raw object not in its `sources`;
  otherwise they are the raw objects. Deduplicate by id.

Compaction (`internal/compact` has the full argument) verifies every raw
object completely before using it, streams the records into parts of
`COMPACT_PART_BYTES` through multipart uploads, writes the manifest (the
commit), and only then deletes the raw objects — after re-checking the
manifest and every part against the store. Every step can be repeated after
a crash. Memory is a few raw objects plus the hour's set of ids.

Bucket requirements: **versioning off** (or a rule expiring noncurrent
versions — otherwise deletes free nothing and listings degrade) and a
lifecycle rule that aborts incomplete multipart uploads.

## Configuration

`INGEST_*` environment over an optional YAML file (`INGEST_CONFIG`);
`ingest --config-reference` prints every key with its default, and
`--check-config` validates a deployment's values. Every timeout, batch size
and limit is a key. The ones that shape behaviour:

| Key | Default | Meaning |
| --- | --- | --- |
| `INGEST_MODE` | `ingest` | `ingest` or `compact` |
| `INGEST_S3_BUCKET`, `INGEST_S3_ENDPOINT`, … | — | the archive (required; see `libs/s3`) |
| `INGEST_KAFKA_BROKERS`, `INGEST_KAFKA_TOPIC`, … | off, `ingest.events` | Kafka (optional; see `libs/kafka`). Unset = archive only |
| `INGEST_MAX_EVENT_BYTES` / `INGEST_MAX_BATCH_EVENTS` | 65536 / 1000 | request limits |
| `INGEST_MAX_IN_FLIGHT_BYTES` | 128 MiB | unsettled event bytes before 429. Pod memory limit ≈ 3× this + the Kafka queue (64 MiB) |
| `INGEST_KAFKA_ACK_GRACE` | 1s | wait for Kafka alone before the archive is hurried |
| `INGEST_ARCHIVE_LINGER` / `INGEST_ARCHIVE_URGENT_LINGER` | 10s / 1s | batch wait: normal / when a request depends on it |
| `INGEST_ARCHIVE_BATCH_BYTES` | 8 MiB | flush size (uncompressed) |
| `INGEST_S3_OP_TIMEOUT` | 5s (preset) | one put, retries included |
| `INGEST_KAFKA_PUBLISH_TIMEOUT` | 5s | one send |
| `INGEST_KAFKA_BREAKER_*` / `INGEST_ARCHIVE_BREAKER_*` | ratio 0.5; 10 req/10 s; 2 batches/1 min | the sinks' circuit breakers |
| `INGEST_DRAIN_TIMEOUT` | 15s | shutdown wait for the second copies |
| `INGEST_COMPACT_INTERVAL` / `_CLOSE_DELAY` / `_PART_BYTES` | 5m / 15m / 128 MiB | compaction |

Validation ties them together: `HTTP_REQUEST_TIMEOUT` must exceed
`KAFKA_ACK_GRACE + ARCHIVE_URGENT_LINGER + S3_OP_TIMEOUT`,
`MAX_EVENT_BYTES` must fit `KAFKA_MESSAGE_MAX_BYTES`, `COMPACT_CLOSE_DELAY`
must exceed `S3_OP_TIMEOUT`.

## Shutdown

Readiness off → `HTTP_SHUTDOWN_DELAY` → the server drains within
`HTTP_SHUTDOWN_TIMEOUT` (every request in flight gets its answer from a sink
that is still running, or a 503) → the archive writer flushes its last batch
and the Kafka sends finish, within `DRAIN_TIMEOUT` → the producer closes.
`terminationGracePeriodSeconds` must exceed the sum of the three. What is
still in flight after `DRAIN_TIMEOUT` is second copies, and it is logged.

Kubernetes manifests: [`examples/ingest-k8s`](../../examples/ingest-k8s).

## Tests

| Layer | Where | What |
| --- | --- | --- |
| unit | `internal/{shard,archive,pipeline,api,config}` | the format (+ two fuzz targets), group commit, first-sink acknowledgement, breakers, limits, contract validation |
| integration | `internal/compact`, `internal/integration` | compaction against a real store; the service as `internal/app` assembles it against real Kafka and RustFS, with chaos and SIGKILL |

```sh
just mod services/ingest test-short   # unit
just mod services/ingest test         # + containers (Docker)
just schemathesis ingest              # generated requests against the contract (needs infra-up)
```
