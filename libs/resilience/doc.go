// Package resilience holds the resilience primitives the outbound clients
// share — libs/resilient-http-client and libs/grpcx — so both mean the same
// thing by "the circuit is open":
//
//   - [Breaker]: a circuit breaker over a sliding failure ratio, with
//     generation-tagged tickets (a late result never decides a newer state)
//     and an "ignored" outcome that releases a half-open probe slot;
//   - [RetryBudget]: retries capped at a fraction of the requests in a
//     sliding window.
//
// Pure logic: no I/O, no metrics, no logging, an injected clock. What a
// failure is (an HTTP status, a gRPC code, a timeout) is the client's call.
package resilience
