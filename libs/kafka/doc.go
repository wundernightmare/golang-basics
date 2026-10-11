// Package kafka is the shared Kafka layer for golang-basics services — the
// broker analogue of libs/pgx and libs/valkey. It is provider-neutral and
// pure Go: the client library is a [Driver] linked in by importing one
// provider module for its side effect —
//
//   - libs/kafka-rdkafka: librdkafka via confluent-kafka-go (the default;
//     cgo, a static musl binary in the image; any librdkafka property
//     through KAFKA_*_EXTRA_PROPERTIES), or
//   - libs/kafka-franz: franz-go (pure Go, CGO_ENABLED=0).
//
// Whichever is linked, this package owns the behaviour: config (one key set,
// mapped explicitly onto either client, every memory bound named — see
// [Config]), the error taxonomy ([SendError], [Rejected]), W3C trace
// propagation through record headers with producer / consumer spans, the
// metric names, a readiness check for libs/httpx, and two shapes:
//
//   - [Producer] — synchronous sends ([Producer.Send]) acknowledged by every
//     in-sync replica, idempotent, bounded by KAFKA_PUBLISH_TIMEOUT, with
//     [Header]s (event_id, event_type, content_type, traceparent).
//   - [Consumer] — a consumer-group poll / process / commit loop
//     ([Consumer.Run]) that hands each record to a [Handler] with its trace,
//     request id and deadline; retries failures with jittered exponential
//     backoff; parks records that still fail on a dead-letter topic (or stops,
//     when the DLQ is disabled); commits only what it finished (offset + 1),
//     including on shutdown and before a rebalance moves a partition.
//
// Delivery is at-least-once: a handler may see a record again after a crash,
// a failed commit or a lost partition, so handlers are idempotent (see
// services/consumer's dedupe by event_id).
//
// libs/kafka/kafkatest is the conformance suite every provider runs against
// a real broker: the same tests, the same expectations.
//
// services/tasks publishes task events through its outbox with the
// [Producer]; services/consumer drains the same topic with the [Consumer].
// README.md compares the providers and explains every safe default.
package kafka
