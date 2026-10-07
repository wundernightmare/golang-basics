# tasks

The **"everything" HTTP service** of this monorepo and its worked example of a
durable write path: a small Tasks CRUD API that wires together every shared
infrastructure lib, so the integrations are demonstrated end-to-end rather
than in isolation.

| Concern            | Lib                                    | What it does here                                                   |
| ------------------ | -------------------------------------- | ------------------------------------------------------------------- |
| HTTP scaffolding   | [`libs/httpx`](../../libs/httpx)       | net/http server, request ids, tracing, logging, metrics, health, shutdown |
| Persistence        | [`libs/pgx`](../../libs/pgx)           | Postgres pool, session timeouts, versioned migrations on boot       |
| Cache-aside        | [`libs/valkey`](../../libs/valkey)     | read cache for `GET /tasks/{id}`: singleflight, TTL jitter, tombstones |
| Events             | [`libs/kafka`](../../libs/kafka)       | `task.created` / `task.updated` / `task.deleted` via a transactional outbox |
| Tracing            | [`libs/otelx`](../../libs/otelx)       | OTLP spans per request, carried through the outbox to the broker    |
| Errors             | `httpx.Problem`                        | RFC 9457 `application/problem+json`, business errors carry a `code` |

The contract is [`api/tsp/tasks.tsp`](../../api/tsp/tasks.tsp) (HTTP) and
[`api/tsp/events.tsp`](../../api/tsp/events.tsp) (events); the Go wire types are
generated into [`libs/contracts`](../../libs/contracts).
[`services/consumer`](../consumer) drains the events.

## Endpoints

| Route                  | Returns                                                                                   |
| ---------------------- | ----------------------------------------------------------------------------------------- |
| `POST /tasks`          | `201` the task + `ETag` + `Location`; body `{"title":"…"}`. With `Idempotency-Key`: a retry is `200` + `Idempotent-Replayed: true` (same task), the same key with another body `422` |
| `GET /tasks`           | `200 {"tasks":[…],"next_cursor":"…"}` newest first; `?limit=` 1–200 (default 50), `?cursor=` the previous page's `next_cursor` |
| `GET /tasks/{id}`      | `200` the task + `ETag` + `X-Cache: hit\|miss`; unknown id → `404`                        |
| `PATCH /tasks/{id}`    | `200` the task + `ETag`; body `{"title"?:…, "done"?:…}` (at least one); `If-Match: "<version>"` optional → `412` when the task moved on |
| `DELETE /tasks/{id}`   | `204`; `If-Match` as for PATCH                                                            |

Titles are trimmed, then must be 1–200 characters, not blank, and free of
control characters other than tab (NUL in particular — PostgreSQL `TEXT`
rejects it). Every 4xx is a problem with a stable `code`: `invalid_title`,
`invalid_limit`, `invalid_cursor`, `invalid_if_match`, `empty_patch`,
`invalid_idempotency_key`, `idempotency_key_reused`, `version_mismatch`,
`task_not_found`. A 5xx carries only a generic `detail`; its cause is in the
request's `request failed` log line and on its span.

**Versions and ETags.** Every task has a `version` (1 on create, +1 per
change); its `ETag` is the version in quotes. Send it back as `If-Match` on
PATCH/DELETE to change only what you read: a concurrent writer makes it a
`412`, never a silent lost update (and never a `409`). `If-Match: *` or no
header means unconditional.

**Pagination** is keyset, over `(created_at DESC, id DESC)` with a matching
index: a page is an index range scan however deep it is, and rows added or
removed meanwhile never shift a page the way `OFFSET` does. The cursor is
opaque (base64 of the last row's position); `created_at` comes from the
database clock, not the app's.

On the **admin** listener (`:9082`, from `httpx`): `/healthz`, `/livez`,
`/readyz` (Postgres critical; Valkey and Kafka optional — the service
degrades, it does not leave the load balancer), `/metrics`, `/version`,
`/admin/config` (redacted), `/admin/log-level`, `/debug/pprof`.

## The write path: transactional outbox

A change and the event that describes it are committed in **one** Postgres
transaction; a relay publishes the event afterwards. So a task is never
created without its event, a broker outage delays events instead of losing
them, and no request waits on Kafka.

```
POST/PATCH/DELETE ──► BEGIN; change tasks row; INSERT INTO outbox (event, trace context); COMMIT
                                                     │
relay (internal/outbox) ◄────── wake / poll ─────────┘
  BEGIN; pg_try_advisory_xact_lock (one active relay across replicas)
  SELECT … FROM outbox ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED
  publish each (headers event_id, event_type, content_type, traceparent), stop at first failure
  DELETE the acknowledged rows; COMMIT          → back off exponentially while the broker fails
```

* **At least once.** A crash between the broker's ack and the DELETE
  republishes; every record carries `event_id` (header and payload) for the
  consumer to de-duplicate on.
* **In order per task.** Rows go out in insertion order, a batch stops at the
  first failure, and only one relay (per schema) works at a time; the record
  key is the task id, so one task's events share a partition.
* **Traced.** The request's trace context is stored with the row; the relay's
  span and the produced record continue the request's trace.
* **Visible.** `outbox_pending`, `outbox_relay_lag_seconds` (age of the oldest
  unpublished event — alert on this) and `outbox_published_total{result}`.
  Failed rows keep `attempts` and `last_error` for inspection.
* The HTTP handler wakes the relay after each commit, so events normally go
  out within milliseconds; `TASKS_OUTBOX_POLL_INTERVAL` is only the fallback.

## The read path: cache-aside without resurrection

`GET /tasks/{id}` goes through [`internal/cache`](internal/cache) on top of
`valkey.Aside`: a hit is served from Valkey; concurrent misses on one key
share a single Postgres read (singleflight); the fill uses a ±10% TTL jitter
and is a `SET NX`. PATCH and DELETE, **after their transaction commits**,
replace the key with a short-lived tombstone (`TASKS_CACHE_TOMBSTONE_TTL`,
5s) that reads as a miss and that fills cannot overwrite — so a GET that
loaded the old row just before the commit cannot write it back just after
(the classic stale-or-resurrected-entry race). Keys are versioned by the
cached shape (`task:v2:<id>`). With Valkey down, reads fall through to
Postgres within `VALKEY_OP_TIMEOUT` and writes still succeed (a failed
invalidation is a warn line; the entry's TTL bounds the staleness).

## Configuration

An optional YAML file (`TASKS_CONFIG=/path/config.yaml`) overlaid with
`TASKS_`-prefixed environment variables — env > YAML > default, unknown YAML
keys are an error. The config **embeds each lib's own `Config`** (see
[`internal/config`](internal/config/config.go)), so every knob a lib documents
works here under its documented key: `TASKS_HTTP_*` / `TASKS_ADMIN_*` /
`TASKS_LOG_*` (httpx, top level in YAML), `TASKS_DATABASE_URL` / `TASKS_DB_*`
(pgx, `postgres:`), `TASKS_VALKEY_*` (`valkey:`), `TASKS_KAFKA_*` (`kafka:`),
`TASKS_OTEL_*` (`otel:`). The most used, and the service's own:

| Env                                   | YAML key                     | Default                          |
| ------------------------------------- | ---------------------------- | -------------------------------- |
| `TASKS_HTTP_ADDR`                     | `http_addr`                  | `:8082`                          |
| `TASKS_ADMIN_ADDR`                    | `admin_addr`                 | `:9082`                          |
| `TASKS_ADMIN_TOKEN`                   | `admin_token`                | *(empty: refused on a non-loopback admin address unless `TASKS_ADMIN_INSECURE=true`)* |
| `TASKS_HTTP_REQUEST_TIMEOUT`          | `request_timeout`            | `30s`                            |
| `TASKS_DATABASE_URL`                  | `postgres.url`               | *(empty: `TASKS_DB_HOST`… → `postgres://app:app@localhost:5432/app`)* |
| `TASKS_DB_STATEMENT_TIMEOUT`          | `postgres.statement_timeout` | `30s` (also `DB_LOCK_TIMEOUT` 5s, `DB_IDLE_IN_TX_TIMEOUT` 60s) |
| `TASKS_VALKEY_URL`                    | `valkey.url`                 | *(empty: `TASKS_VALKEY_ADDR` → `localhost:6379`)* |
| `TASKS_KAFKA_BROKERS`                 | `kafka.brokers`              | `localhost:9092`                 |
| `TASKS_KAFKA_TOPIC`                   | `kafka.topic`                | `tasks.events` — where every event goes |
| `TASKS_KAFKA_ALLOW_AUTO_TOPIC_CREATION` | `kafka.allow_auto_topic_creation` | `false` (pre-provision the topic) |
| `TASKS_OTEL_ENABLED`                  | `otel.enabled`               | `false`                          |
| `TASKS_OTEL_EXPORTER_OTLP_ENDPOINT`   | `otel.endpoint`              | `localhost:4318` (OTLP/HTTP, TLS; `TASKS_OTEL_EXPORTER_OTLP_INSECURE=true` for a local collector) |
| `TASKS_CACHE_TTL`                     | `cache.ttl`                  | `1m` (±10%)                      |
| `TASKS_CACHE_TOMBSTONE_TTL`           | `cache.tombstone_ttl`        | `5s`                             |
| `TASKS_OUTBOX_POLL_INTERVAL`          | `outbox.poll_interval`       | `1s`                             |
| `TASKS_OUTBOX_BATCH_SIZE`             | `outbox.batch_size`          | `100`                            |
| `TASKS_OUTBOX_MAX_BACKOFF`            | `outbox.max_backoff`         | `30s`                            |
| `TASKS_IDEMPOTENCY_KEY_TTL`           | `idempotency.ttl`            | `24h`                            |
| `TASKS_IDEMPOTENCY_PURGE_INTERVAL`    | `idempotency.purge_interval` | `10m`                            |

The effective config (secrets redacted) is on `GET /admin/config`.

## Schema

[`migrations/`](migrations) holds `NNN_name.sql` files, embedded and applied on
boot by `pgx.Migrate`: each in its own transaction, recorded in
`schema_migrations`, serialised across replicas by an advisory lock.
Forward-only — never edit a shipped file, add the next number. `001` adopts
the table older releases created.

## Run

```sh
# bring up Postgres + Valkey + Kafka first (repo root):
just infra-up

# then run the service (repo root); a local admin listener needs no token:
TASKS_ADMIN_ADDR=127.0.0.1:9082 TASKS_KAFKA_ALLOW_AUTO_TOPIC_CREATION=true just tasks run

curl -si -XPOST localhost:8082/tasks -H 'Idempotency-Key: demo-1' -d '{"title":"ship it"}'
curl -si localhost:8082/tasks/<id> | head                               # ETag, X-Cache
curl -si -XPATCH localhost:8082/tasks/<id> -H 'If-Match: "1"' -d '{"done":true}'
curl -s 'localhost:8082/tasks?limit=2' | jq .                           # next_cursor
curl -s localhost:9082/metrics | grep ^outbox_
```

## Test

```sh
just tasks test-short   # unit tests: domain, config, handlers against fakes + the OpenAPI contract
just tasks test         # + store / outbox / integration + chaos against containers (Docker or Podman)
```

* [`internal/api`](internal/api) — handlers against in-memory fakes; every
  exchange is validated against the OpenAPI document.
* [`internal/store`](internal/store), [`internal/outbox`](internal/outbox) —
  against Postgres: atomic change + event, idempotency under concurrency,
  keyset pages (and the index plan), optimistic concurrency, relay ordering,
  failure handling and single-relay behaviour.
* [`internal/integration`](internal/integration) — the service wired as
  `main.go` wires it: CRUD with the events read back from Kafka (headers,
  order, JSON Schema), one trace from the request to the record, metrics;
  chaos: Valkey down (reads fall through), Postgres slow / down (readiness
  503, 5xx with the cause logged), Kafka frozen (writes stay fast, the outbox
  backlog grows on `/metrics`, the relay catches up once it is back).

Each test owns a fresh Postgres schema (`search_path`), a Kafka topic and its
own ids, on containers shared per test binary.
