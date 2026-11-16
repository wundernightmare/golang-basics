// Package resilience holds the resilience primitives the outbound clients
// share — libs/resilient-http-client and libs/grpcx — so both mean the same
// thing by "the circuit is open":
//
//   - [Breaker]: a circuit breaker over a sliding failure ratio, with
//     generation-tagged tickets (a late result never decides a newer state)
//     and an "ignored" outcome that releases a half-open probe slot;
//     [ErrOpen] is the rejection every client's own error wraps;
//   - [RetryBudget]: retries capped at a fraction of the requests in a
//     sliding window, rounded up;
//   - [Backoff]: the exponential, equal-jitter delay of whatever retries on
//     its own clock (the Kafka consumer, the tasks outbox relay);
//   - [Jitter]: a TTL spread over ±10%, so entries cached together do not
//     expire together.
//
// Pure logic: no I/O, no metrics, no logging, an injected clock. What a
// failure is (an HTTP status, a gRPC code, a timeout) is the client's call.
package resilience
