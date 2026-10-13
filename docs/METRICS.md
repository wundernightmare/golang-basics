# Metrics contract

One set of metric names for golang-basics and nodejs-basics: an alert or a
dashboard written against one works against the other. This file is the
contract; a new or renamed metric changes it first (here and in the sibling
repo). Where the Go implementation lives is in the last column.

## Rules

- **Prometheus names.** Base units in the name (`_seconds`, `_bytes`),
  counters end in `_total`. No milliseconds anywhere.
- **Label names** come from one list: `method`, `route`, `status`, `target`,
  `outcome`, `topic`, `result`, `operation`, `check`, `critical`, `client`,
  `state`, `event`, `event_type`, `reason`, `integration`.
- **Bounded label values**, from code or config, never from a request:
  `method` is a standard method or `_OTHER`; `route` the template
  (`/tasks/{id}`), `unmatched` when none matched; `status` the code sent, 499
  when the client went away first; a Kafka `topic` a configured name (a
  `^regex` subscription match is `other`); an outbound `target` a configured
  name (an unknown one is an error, never a label).
- **Latency buckets** (seconds): `.001 .0025 .005 .01 .025 .05 .1 .25 .5 1
  2.5 5 10` for HTTP server and client and the database; Kafka adds `30 60`;
  the cache starts at `.0001` and ends at `.5`.
- **What the broker or a server measures better is not collected in the
  app**: consumer-group lag (the broker: Redpanda `/public_metrics`, or
  kafka-exporter / Burrow for Kafka), Postgres and Valkey server state
  (postgres-exporter, valkey-exporter). The services report what only the
  client sees: its pool, its latencies by operation, its own send buffer,
  its view of the brokers.
- **No number twice.** A metric that is the sum or the copy of another is not
  exported (the outbox's publish counter is the producer's; the consumer's
  "applied" total is the sum of its per-type counter).

## HTTP server — `libs/httpx` (API listener; the admin listener is not instrumented)

| Metric | Type | Labels |
| --- | --- | --- |
| `http_requests_total` | counter | `method, route, status` |
| `http_request_duration_seconds` | histogram (classic + native) | `method, route` |
| `http_requests_in_flight` | gauge | — |
| `http_unhandled_errors_total` | counter | — (a recovered panic, answered 500) |

## HTTP client — `libs/resilient-http-client` (registered by the service that uses it)

| Metric | Type | Labels |
| --- | --- | --- |
| `http_client_requests_total` | counter | `target, method, outcome` (`2xx`…`5xx`, or `timeout`, `connection`, `redirect`, `circuit_open`, `rate_limited`, `bulkhead_full`, `shutdown`, `canceled`, `invalid`) |
| `http_client_request_duration_seconds` | histogram | `target, method` |
| `http_client_retries_total` | counter | `target` |
| `http_client_circuit_breaker_state` | gauge | `target` (0 closed, 1 open, 2 half-open) |

Go only (features the Node client does not have):
`http_client_retry_budget_exhausted_total{target}`,
`http_client_circuit_breaker_transitions_total{target,from,to}`,
`http_client_bulkhead_in_flight{target}`,
`http_client_bulkhead_rejected_total{target}`.

## Postgres — `libs/pgx` (client side only)

| Metric | Type | Labels |
| --- | --- | --- |
| `db_pool_connections` | gauge | `state` = `idle` \| `in_use` (Go adds `constructing`) |
| `db_pool_max_connections` | gauge | — |
| `db_query_duration_seconds` | histogram | `operation` |
| `db_query_errors_total` | counter | `operation` |

`operation` is a constant per call site (`pgx.WithOperation(ctx,
"tasks.create")`), else the statement's keyword from an allowlist, else
`other`. Go only (pgxpool counts these): `db_pool_acquires_waited_total`,
`db_pool_acquire_wait_seconds_total`, `db_pool_acquires_canceled_total`.

## Kafka — `libs/kafka` (same names for both Go providers)

| Metric | Type | Labels |
| --- | --- | --- |
| `kafka_producer_records_total` | counter | `client, topic, result` (`ok`, `queue_full`, `unavailable`, `not_provisioned`, `fatal`, `rejected`, `unknown`) |
| `kafka_producer_send_duration_seconds` | histogram | `client, topic` |
| `kafka_consumer_records_total` | counter | `topic, result` (`ok`, `retried`, `dead_lettered`, `failed`) |
| `kafka_consumer_handler_duration_seconds` | histogram | `topic` |
| `kafka_consumer_commit_errors_total` | counter | — |
| `kafka_consumer_assigned_partitions` | gauge | — |
| `kafka_consumer_rebalances_total` | counter | `event` (`assigned`, `revoked`, `lost`) |
| `kafka_client_brokers_up` | gauge | `client` |
| `kafka_client_request_errors_total` | counter | `client` |
| `kafka_producer_queue_bytes`, `kafka_producer_queue_max_bytes` | gauge | — |

`client` is `producer` or `consumer`: the consumer's dead-letter publishes
are producer sends with `client="consumer"` and the DLQ topic as `topic`
(there is no separate DLQ counter — `kafka_consumer_records_total{result=
"dead_lettered"}` is it). Go only: `kafka_client_info{client,provider}`,
`kafka_consumer_fetch_errors_total`.

**Lag** comes from the broker: locally
`redpanda_kafka_consumer_group_lag_sum{redpanda_group}` / `_lag_max`
(`enable_consumer_group_metrics: [group, partition, consumer_lag]`,
docker/deps.yml).

## Outbox — `services/tasks/internal/outbox` (computed once per relay pass, never per scrape)

| Metric | Type |
| --- | --- |
| `outbox_pending` | gauge |
| `outbox_dead` | gauge |
| `outbox_oldest_age_seconds` | gauge (0 when empty) |

## Cache — `libs/valkey` (Go's read cache)

| Metric | Type | Labels |
| --- | --- | --- |
| `cache_lookups_total` | counter | `result` = `hit` \| `miss` \| `error` |
| `cache_command_duration_seconds` | histogram | `operation` = `get` \| `set` \| `del` |
| `cache_command_errors_total` | counter | `operation` |

## Health, build, runtime

| Metric | Type | Labels |
| --- | --- | --- |
| `health_check_up` | gauge (1/0) | `check, critical` |
| `health_check_duration_seconds` | histogram (Go) | `check` |
| `build_info` | gauge (1) | `version, revision` — no `service` (the scrape adds it), no runtime version (`go_info` has it) |
| `app_integration_enabled` | gauge (1/0) | `integration` |
| `log_dropped_total` | counter (Go) | `level` |

Go runtime and process: the client_golang defaults (`go_*`, `process_*`:
CPU, RSS, open / max file descriptors, start time) plus
`go_sched_latencies_seconds` (histogram: how long a runnable goroutine waited
for a thread — Go's counterpart of Node's event-loop delay).

## Service-specific

| Metric | Service |
| --- | --- |
| `consumer_events_total{event_type}`, `consumer_duplicates_total`, `consumer_events_rejected_total{reason}` | consumer |
| `heartbeat_beats_total` | heartbeat |

## Alerts

[`docker/observability/rules.yml`](../docker/observability/rules.yml):
HighErrorRatio, HighP99Latency, HttpUnhandledErrors, ServiceDown,
ReadinessCheckFailing, OptionalDependencyDegraded, KafkaConsumerLagHigh
(broker metric), KafkaDeadLettering, OutboxStuck (oldest age), OutboxDeadEvents,
DbPoolSaturated, DbPoolStarved (Go: waited acquires). The e2e suite checks
that every name the rules and the dashboards read is on a live `/metrics`; the
broker's own names and the error-only counters (no series before the first
error) are listed explicitly in `e2e/observability_test.go`.

## Dashboards

[`docker/observability/dashboards/`](../docker/observability/dashboards),
provisioned into Grafana, the same layout in nodejs-basics:

- **golang-basics** (`golang-basics-overview`): RED by service (requests/s,
  5xx and 4xx ratio, p50/p95/p99) → HTTP by route → client-side dependencies
  (DB pool saturation and waits, query p95 and errors by operation, cache) →
  Kafka and outbox (records by result, send/handle p95, the broker's
  consumer-group lag, oldest outbox age, pending/dead) → up, readiness checks,
  running versions. Thresholds are the alerts'.
- **golang-basics — runtime** (`golang-basics-runtime`): process CPU, RSS,
  file descriptors against the limit, uptime and restarts; goroutines and
  threads, heap against the next GC target and GOMEMLIMIT, allocation rate,
  GC pauses and cycles, scheduler latency, GOMAXPROCS / GOGC, Go version.

Both take `$datasource`, `$service` and `$instance` and filter every query
by them; windows are `$__rate_interval`; services are keyed by the scrape's
`service` label, never `job`. `TestDashboards_Lint` (e2e, static) enforces
those three. Exemplars: the services expose them (trace_id on the HTTP
latency histogram, OpenMetrics), but VictoriaMetrics single-node does not
store them, so the panels do not ask for them; a Prometheus backend with
`--enable-feature=exemplar-storage` plus the commented datasource setting in
`docker/observability/grafana-datasources.yml` turns them on.
