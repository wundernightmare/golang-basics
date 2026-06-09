# tasks

The **"everything" HTTP service** of this monorepo: a small Tasks CRUD API that
wires together every shared infrastructure lib at once, so the integrations are
demonstrated end-to-end rather than in isolation.

| Concern            | Lib                                   | What it does here                                  |
| ------------------ | ------------------------------------- | -------------------------------------------------- |
| HTTP scaffolding   | [`libs/httpx`](../../libs/httpx)       | engine, logging, metrics, health, graceful shutdown |
| Persistence        | [`libs/pgx`](../../libs/pgx)           | Postgres pool + `tasks` table (migrated on boot)   |
| Cache-aside        | [`libs/valkey`](../../libs/valkey)     | Valkey read-through cache for `GET /tasks/:id`     |
| Events             | [`libs/kafka`](../../libs/kafka)       | publishes `task.created` on write                  |
| Tracing            | [`libs/otelx`](../../libs/otelx)       | OTLP spans per request, propagated to the broker   |
| Errors             | `httpx.Problem`                        | RFC 9457 `application/problem+json` responses      |

[`services/consumer`](../consumer) drains the `task.created` events this service
produces.

## Endpoints

| Route               | Returns                                                              |
| ------------------- | ------------------------------------------------------------------- |
| `POST /tasks`       | `201` the created task — body `{"title":"…"}`; empty title → `400` problem |
| `GET /tasks`        | `200 {"tasks":[…]}` newest first                                     |
| `GET /tasks/:id`    | `200` the task (`X-Cache: hit\|miss`); unknown id → `404` problem    |
| `DELETE /tasks/:id` | `204`; unknown id → `404` problem                                    |
| `GET /healthz`      | liveness (from `httpx`)                                              |
| `GET /readyz`       | readiness — checks Postgres + Valkey + Kafka (from `httpx`)          |
| `GET /metrics`      | Prometheus exposition (from `httpx`)                                 |

## Configuration

Loaded from an optional YAML file (`TASKS_CONFIG=/path/config.yaml`) overlaid
with `TASKS_`-prefixed environment variables — env wins, see
[`internal/config`](internal/config/config.go). Key settings:

| Env                                | YAML key             | Default                                              |
| ---------------------------------- | -------------------- | ---------------------------------------------------- |
| `TASKS_HTTP_ADDR`                  | `http_addr`          | `:8082`                                              |
| `TASKS_DATABASE_URL`               | `database_url`       | `postgres://app:app@localhost:5432/app?sslmode=disable` |
| `TASKS_VALKEY_URL`                 | `valkey_url`         | `valkey://localhost:6379`                            |
| `TASKS_CACHE_TTL`                  | `cache_ttl`          | `1m`                                                 |
| `TASKS_KAFKA_BROKERS`              | `kafka_brokers`      | `localhost:9092`                                     |
| `TASKS_KAFKA_TOPIC`                | `kafka_topic`        | `tasks.events`                                       |
| `TASKS_OTEL_ENABLED`               | `otel_enabled`       | `false`                                              |
| `TASKS_OTEL_EXPORTER_OTLP_ENDPOINT`| `otel_endpoint`      | `localhost:4317`                                     |

## Run

```sh
# bring up Postgres + Valkey + Kafka first (repo root):
just infra-up

# then run the service (repo root):
just tasks run
# or directly:  cd services/tasks && just run

curl -s -XPOST localhost:8082/tasks -d '{"title":"ship it"}' | jq .
curl -s localhost:8082/tasks/<id> -i | head            # note X-Cache header
curl -s localhost:8082/tasks | jq .
curl -s localhost:8082/readyz | jq .                   # postgres/valkey/kafka checks
```

## Test

```sh
just tasks test-short        # unit tests only (no Docker)
just tasks test-integration  # full stack: spins up PG + Valkey + Kafka (testcontainers)
```

The integration suite ([`internal/integration`](internal/integration)) drives
the real HTTP API against real containers and asserts the `task.created` event
actually lands in Kafka — the end-to-end proof that the libs compose.
