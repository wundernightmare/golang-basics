// Package resilient is a lock-minimal, policy-per-target HTTP client for
// high-throughput Go services — the Go analogue of the Rust
// `resilient-http-client` crate and the TypeScript `resilient-client` package
// in the sibling tracehub repos.
//
// A single [Client] is safe for concurrent use and is meant to be shared across
// the whole process. Every outbound request is tagged with a logical target
// (a [ResourceGroup]); each target carries its own independently-tuned policy
// set — rate limiter, circuit breaker and optional adaptive-concurrency gate —
// looked up on the hot path without locks.
//
// # Features
//
//   - Per-target rate limiting — token-bucket via golang.org/x/time/rate.
//   - Per-target circuit breaker — lock-free atomic sliding window
//     (see [CircuitBreaker]).
//   - Adaptive concurrency — AIMD in-flight limit, +1 on success, ÷2 on
//     failure (see [AdaptiveLimiter]).
//   - Jittered exponential backoff retry — AWS "full jitter"
//     (see [Client.SendWithRetry] and [FullJitter]).
//   - Read-through response cache — pluggable [CacheAdapter] with a built-in
//     LRU+TTL [InMemoryCache].
//   - Request coalescing — single-flight dedup of concurrent GET/HEAD with the
//     same cache key (see [Client.SendCoalesced]).
//   - Graceful degradation — stale-cache or static fallback when the circuit is
//     open or retries are exhausted (see [Client.SendWithFallback]).
//   - Tuned connection pool with an optional TTL-aware DNS cache.
//   - Prometheus metrics on a private registry (see [Metrics]).
//   - Typed transient/fatal errors so callers own the retry decision
//     (see [OutboundError]).
//   - Graceful shutdown that drains in-flight requests (see [Client.Shutdown]).
//
// # Quick start
//
//	cfg, _ := resilient.LoadConfig(yamlBytes)
//	client, _ := resilient.New(cfg, resilient.WithLogger(log))
//	defer client.Shutdown(context.Background())
//
//	resp, err := client.Send(ctx, resilient.Request{
//		Target: "meta_events",
//		Method: http.MethodPost,
//		URL:    "https://graph.facebook.com/123/events",
//		Body:   []byte(`{"data":[]}`),
//	})
//	switch {
//	case err == nil:
//		defer resp.Body.Close() // caller owns the body
//	case resilient.IsTransient(err):
//		// re-queue for a later retry
//	default:
//		// fatal — log and drop
//	}
package resilient
