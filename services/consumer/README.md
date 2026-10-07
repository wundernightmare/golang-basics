# consumer

A **Kafka consumer worker** that drains the `tasks.events` topic produced by
[`services/tasks`](../tasks) (through its outbox). It is the broker-fed
counterpart to [`services/heartbeat`](../heartbeat)'s ticker: the worker loop
and the shared [`libs/httpx`](../../libs/httpx) admin server
(health/metrics/pprof) run concurrently under one signal context, so either
half failing tears the other down.

## What it does with an event

- **Dispatch** on the `event_type` header — `task.created`, `task.updated`,
  `task.deleted` — falling back to an `event_type` field in the JSON payload,
  and to `task.created` for a payload with neither (the pre-outbox producer).
- **Permanent failures** — an undecodable payload or an unknown type — are
  returned as `kafka.Permanent`: no retries, straight to the dead-letter
  topic (`consumer_events_rejected_total{reason}`).
- **Transient failures** are retried by [`libs/kafka`](../../libs/kafka) with
  jittered exponential backoff; a record that still fails is dead-lettered to
  `tasks.events.dlq` with `dlq_*` headers and the loop moves on. With
  `CONSUMER_KAFKA_DEAD_LETTER_TOPIC=none` it stops instead (the process exits
  and the record is redelivered on restart).
- **Idempotency**: delivery is at-least-once, so the worker remembers the last
  `CONSUMER_DEDUPE_SIZE` applied `event_id`s (an in-memory LRU) and skips a
  repeat (`consumer_duplicates_total`). An id is remembered only once the
  event is applied, so a failed attempt is retried, not skipped. The LRU is
  per process: a redelivery that lands on another replica or after a restart
  is applied again — a consumer whose effect must be exactly-once keeps the
  applied ids next to the effect (a unique key in the same transaction).
- **Logs** carry ids and sizes only (`event_type`, `event_id`, `task_id`,
  `bytes`, partition/offset) — never the title or other user content — plus
  the `trace_id` of the request that produced the event (the headers carry the
  producer's trace context; `libs/kafka` continues it in a `process` span) and
  `request_id` = the event id.

Shutdown: on SIGTERM the in-flight batch gets
`CONSUMER_KAFKA_CONSUMER_DRAIN_TIMEOUT` to finish, what finished is
committed, the rest is redelivered to the next group member.

## Metrics (on `/metrics`, admin listener `:9083`)

| Metric | Meaning |
| --- | --- |
| `consumer_tasks_consumed_total` | events applied (every type) |
| `consumer_events_total{event_type}` | events applied, by type |
| `consumer_duplicates_total` | redeliveries skipped by `event_id` |
| `consumer_events_rejected_total{reason}` | `undecodable` \| `unknown_type` (dead-lettered) |
| `kafka_consume_total{topic,result}` | `ok` \| `retried` \| `dead_lettered` \| `failed` (from `libs/kafka`) |
| `kafka_consumer_dlq_total{topic}`, `kafka_consumer_commit_errors_total`, `kafka_consumer_handler_duration_seconds` | dead-lettering, commits, handler latency |
| `kafka_consumer_group_lag{topic,partition}` | records not yet committed, for the partitions this replica owns (every `CONSUMER_KAFKA_LAG_INTERVAL`, default 15s) — the worker's availability indicator |
| `kafka_consumer_assigned_partitions`, `kafka_consumer_rebalances_total`, `kafka_consumer_lag_*` | group membership and lag-poll health |
| `build_info`, `log_dropped_total`, `health_*`, `go_*`, `process_*` | from `httpx` |

## Endpoints (admin listener only — a worker has no API port)

| Route | Returns |
| --- | --- |
| `GET /healthz`, `GET /livez` | liveness |
| `GET /readyz` | readiness — pings the Kafka brokers |
| `GET /metrics` | Prometheus exposition |
| `GET /version` | build identity |
| `GET /admin/config` | effective config, secrets redacted (token) |
| `GET/PUT/DELETE /admin/log-level` | runtime log level (token) |
| `GET /debug/pprof` | Go runtime profiles (token) |

## Configuration

An optional YAML file named by `CONSUMER_CONFIG`, overlaid by
`CONSUMER_`-prefixed environment variables (env > YAML > default; unknown
YAML keys are an error). The config embeds the libs' configs, so every
`libs/httpx`, `libs/kafka` and `libs/otelx` key is available — see their
READMEs; the ones you are most likely to set:

| Env | Default | |
| --- | --- | --- |
| `CONSUMER_ADMIN_ADDR` | `:9083` | |
| `CONSUMER_ADMIN_TOKEN` | *(empty)* | required on a non-loopback address… |
| `CONSUMER_ADMIN_INSECURE` | `false` | …unless this is `true` (local compose / e2e) |
| `CONSUMER_LOG_LEVEL` / `_FORMAT` | `info` / `json` | |
| `CONSUMER_KAFKA_BROKERS` | `localhost:9092` | |
| `CONSUMER_KAFKA_TOPIC` | `tasks.events` | |
| `CONSUMER_KAFKA_GROUP` | `tasks-consumer` | |
| `CONSUMER_KAFKA_CONSUMER_MAX_RETRIES` | `5` | negative = none |
| `CONSUMER_KAFKA_CONSUMER_RETRY_BACKOFF` / `_MAX` | `200ms` / `30s` | exponential, jittered |
| `CONSUMER_KAFKA_CONSUMER_HANDLER_TIMEOUT` | `30s` | per attempt |
| `CONSUMER_KAFKA_DEAD_LETTER_TOPIC` | `{topic}.dlq` | `none` = stop on poison |
| `CONSUMER_KAFKA_CONSUMER_DRAIN_TIMEOUT` | `10s` | |
| `CONSUMER_KAFKA_CONSUMER_MAX_POLL_RECORDS` | `100` | |
| `CONSUMER_KAFKA_ALLOW_AUTO_TOPIC_CREATION` | `false` | the DLQ topic must exist unless `true` |
| `CONSUMER_KAFKA_TLS_ENABLED`, `_TLS_CA_CERT`, `_TLS_CLIENT_CERT`, `_TLS_CLIENT_KEY`, `_TLS_INSECURE_SKIP_VERIFY` | off | broker TLS / mTLS (PEM files) |
| `CONSUMER_KAFKA_SASL_MECHANISM`, `_SASL_USERNAME`, `_SASL_PASSWORD` | off | `plain` \| `scram-sha-256` \| `scram-sha-512` |
| `CONSUMER_DEDUPE_SIZE` | `10000` | event ids remembered |
| `CONSUMER_OTEL_ENABLED` | `false` | trace export (OTLP/HTTP, TLS unless `CONSUMER_OTEL_EXPORTER_OTLP_INSECURE=true`) |

A config error is printed to stderr and the process exits non-zero before
anything starts.

## Run

```sh
just infra-up             # bring up Kafka (repo root)
CONSUMER_ADMIN_INSECURE=true CONSUMER_KAFKA_ALLOW_AUTO_TOPIC_CREATION=true just consumer run

# in another shell, create a task so an event flows:
just tasks run &
curl -s -XPOST localhost:8082/tasks -d '{"title":"hello"}'
curl -s localhost:9083/metrics | grep consumer_tasks_consumed_total
```

## Test

```sh
just consumer test-short   # unit: dispatch, dedupe, permanent vs transient, config (no Docker)
just consumer test         # + integration: real Kafka, headers → applied, duplicate skipped, unknown type dead-lettered
```
