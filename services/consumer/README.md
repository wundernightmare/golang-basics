# consumer

A **Kafka consumer worker** that drains the `tasks.events` topic produced by
[`services/tasks`](../tasks). It is the broker-fed counterpart to
[`services/heartbeat`](../heartbeat)'s ticker: the worker loop and the shared
[`libs/httpx`](../../libs/httpx) health/metrics server run concurrently under
one signal context, so either half failing tears the other down.

For each `task.created` event it bumps a Prometheus counter and logs a line;
undecodable messages are counted as skipped and acknowledged (a poison record
must not wedge the loop). Delivery is at-least-once — offsets advance only past
events the handler accepted (see [`libs/kafka`](../../libs/kafka)).

## Metrics (on `/metrics`)

| Metric                           | Meaning                                        |
| -------------------------------- | ---------------------------------------------- |
| `consumer_tasks_consumed_total`  | events consumed successfully                   |
| `consumer_tasks_skipped_total`   | events dropped as undecodable                  |
| `http_requests_total` / …        | the standard HTTP metrics (from `httpx`)       |

## Endpoints (health/metrics server)

| Route          | Returns                                            |
| -------------- | -------------------------------------------------- |
| `GET /healthz` | liveness (from `httpx`)                            |
| `GET /readyz`  | readiness — pings the Kafka brokers (from `httpx`) |
| `GET /metrics` | Prometheus exposition (from `httpx`)               |

## Configuration

`CONSUMER_`-prefixed environment variables (see [`internal/config`](internal/config/config.go)):

| Env                        | Default          |
| -------------------------- | ---------------- |
| `CONSUMER_HTTP_ADDR`       | `:8083`          |
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
