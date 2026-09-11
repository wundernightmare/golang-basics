# consumer

A **Kafka consumer worker** that drains the `tasks.events` topic produced by
[`services/tasks`](../tasks). It is the broker-fed counterpart to
[`services/heartbeat`](../heartbeat)'s ticker: the worker loop and the shared
[`libs/httpx`](../../libs/httpx) admin server (health/metrics/pprof) run
concurrently under one signal context, so either half failing tears the other
down.

For each `task.created` event it bumps a Prometheus counter and logs a line
(with the `trace_id` of the request that created the task — the record headers
carry the producer's trace context and `libs/kafka` continues it in a `process`
span); undecodable messages are counted as skipped and acknowledged (a poison
record must not wedge the loop). Delivery is at-least-once — offsets advance
only past events the handler accepted (see [`libs/kafka`](../../libs/kafka)).

## Metrics (on `/metrics`, admin listener `:9083`)

| Metric                                      | Meaning                                                  |
| ------------------------------------------- | -------------------------------------------------------- |
| `consumer_tasks_consumed_total`             | events consumed successfully                             |
| `consumer_tasks_skipped_total`              | events dropped as undecodable                            |
| `kafka_consumer_group_lag{topic,partition}` | records not yet committed by the group (polled every `CONSUMER_KAFKA_LAG_INTERVAL`, default 15s) — the worker's availability indicator |
| `kafka_consumer_records_total` / `_handler_errors_total` / `_fetch_errors_total` / `_handle_duration_seconds` | per-topic throughput, failures and handler latency (from `libs/kafka`) |
| `build_info`, `log_dropped_total`, `go_*`, `process_*` | from `httpx`                                              |

## Endpoints (admin listener)

| Route              | Returns                                            |
| ------------------ | -------------------------------------------------- |
| `GET /healthz`     | liveness (from `httpx`)                            |
| `GET /readyz`      | readiness — pings the Kafka brokers (from `httpx`) |
| `GET /metrics`     | Prometheus exposition (from `httpx`)               |
| `GET /version`     | build identity (from `httpx`)                      |
| `GET /debug/pprof` | Go runtime profiles (from `httpx`)                 |

## Configuration

`CONSUMER_`-prefixed environment variables (see [`internal/config`](internal/config/config.go)):

| Env                        | Default          |
| -------------------------- | ---------------- |
| `CONSUMER_ADMIN_ADDR`      | `:9083`          |
| `CONSUMER_LOG_SAMPLE_INITIAL` / `_THEREAFTER` | `100` / `100` |
| `CONSUMER_KAFKA_BROKERS`   | `localhost:9092` |
| `CONSUMER_KAFKA_TOPIC`     | `tasks.events`   |
| `CONSUMER_KAFKA_GROUP`     | `tasks-consumer` |
| `CONSUMER_OTEL_ENABLED`    | `false`          |

## Run

```sh
just infra-up             # bring up Kafka (repo root)
just consumer run         # then run the worker

# in another shell, create a task so an event flows:
just tasks run &
curl -s -XPOST localhost:8082/tasks -d '{"title":"hello"}'
curl -s localhost:8083/metrics | grep consumer_tasks_consumed_total
```

## Test

```sh
just consumer test-short   # unit tests only (fake consumer, no Docker)
just consumer test         # + integration: produces to real Kafka, asserts consumption
```
