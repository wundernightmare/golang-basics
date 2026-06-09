// Package kafka is the shared Kafka layer for golang-basics services — the
// broker analogue of libs/pgx and libs/valkey. It wraps the
// [github.com/twmb/franz-go] client with environment-driven config, a
// readiness check that plugs into libs/httpx, and two thin shapes:
//
//   - [Producer] — synchronous, at-least-once publishing ([Producer.Publish]).
//   - [Consumer] — a consumer-group poll loop ([Consumer.Run]) that hands each
//     record to a [Handler] and commits only what the handler accepts, so a
//     failing handler leaves the record for redelivery.
//
// franz-go is the pure-Go, dependency-free Kafka client with full protocol
// coverage (no CGO/librdkafka), which makes it the reliable choice for a
// static, distroless service binary.
//
// services/tasks publishes a task.created event with the [Producer];
// services/consumer drains the same topic with the [Consumer].
package kafka
