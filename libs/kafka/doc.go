// Package kafka is the shared Kafka layer for golang-basics services — the
// broker analogue of libs/pgx and libs/valkey. It wraps the
// [github.com/twmb/franz-go] client with environment/YAML-driven config
// (TLS, SASL, retries, dead-lettering; see [Config]), a readiness check that
// plugs into libs/httpx, Prometheus collectors, OpenTelemetry trace
// propagation through record headers, and two shapes:
//
//   - [Producer] — synchronous publishing ([Producer.Publish]) acknowledged
//     by every in-sync replica, idempotent, bounded by PublishTimeout, with
//     [Header]s (event_id, event_type, content_type, traceparent).
//   - [Consumer] — a consumer-group poll / process / commit loop
//     ([Consumer.Run]) that hands each record to a [Handler] with its trace,
//     request id and deadline; retries failures with jittered exponential
//     backoff; parks records that still fail on a dead-letter topic (or stops,
//     when the DLQ is disabled); commits only what it finished, including on
//     shutdown and before a rebalance moves a partition.
//
// Delivery is at-least-once: a handler may see a record again after a crash,
// a failed commit or a lost partition, so handlers are idempotent (see
// services/consumer's dedupe by event_id).
//
// franz-go is the pure-Go, dependency-free Kafka client with full protocol
// coverage (no CGO/librdkafka), which makes it the reliable choice for a
// static, distroless service binary.
//
// services/tasks publishes task events through its outbox with the
// [Producer]; services/consumer drains the same topic with the [Consumer].
package kafka
