# resilient-http-client

Lock-minimal, policy-per-target **outbound** HTTP client for high-throughput Go
services — the Go counterpart of the Rust `resilient-http-client` crate and the
TypeScript `resilient-client` package in the sibling tracehub repos.

Where [`httpx`](../httpx) is the **server** scaffolding (inbound), this is the
**client** scaffolding (outbound): one shared, concurrency-safe [`Client`] that
tags every request with a logical *target* and applies that target's own
rate limiter, circuit breaker and adaptive-concurrency gate.

| Concern | Implementation |
| --- | --- |
| Per-target rate limiting | token bucket (`golang.org/x/time/rate`) |
| Per-target circuit breaker | lock-free atomic sliding window |
| Adaptive concurrency | AIMD in-flight limit (+1 success / ÷2 failure) |
| Jittered retry | AWS full-jitter exponential backoff |
| Response cache | pluggable `CacheAdapter` + built-in LRU+TTL `InMemoryCache` |
| Request coalescing | single-flight dedup of concurrent GET/HEAD |
| Graceful degradation | stale-cache → static fallback on transient failure |
| Connection pool | tuned `http.Transport` + optional TTL DNS cache |
| Observability | Prometheus metrics on a private registry |
| Error model | typed transient/fatal — caller owns the retry decision |
| Graceful shutdown | drains in-flight requests or times out |

## Quick start

```go
cfg, err := resilient.LoadConfig(yamlBytes) // or resilient.DefaultConfig()
if err != nil {
    log.Fatal(err)
}

client, err := resilient.New(cfg,
    resilient.WithLogger(logger),
    resilient.WithCache(resilient.NewInMemoryCache(10_000, time.Minute)),
    resilient.WithFallback("san_api", func() resilient.CachedResponse {
        return resilient.CachedResponse{Status: 200, Body: []byte(`{}`)}
    }),
)
if err != nil {
    log.Fatal(err)
}
defer func() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    _ = client.Shutdown(ctx)
}()

resp, err := client.Send(ctx, resilient.Request{
    Target: "meta_events",
    Method: http.MethodPost,
    URL:    "https://graph.facebook.com/123/events",
    Body:   []byte(`{"data":[]}`),
})
switch {
case err == nil:
    defer resp.Body.Close() // caller owns the body
    // process resp
case resilient.IsTransient(err):
    // network timeout, 5xx, 429, circuit open, rate limited, shutting down:
    // re-queue for a later retry (e.g. a Kafka retry topic).
default: // resilient.IsFatal(err)
    // 4xx (≠429), TLS error, bad request: log and drop.
}
```

## Status → error mapping

| Result | Error | Rationale |
| --- | --- | --- |
| 2xx | `nil` (returns `*http.Response`) | success; breaker & limiter recorded |
| 429 | transient | upstream rate limit — does **not** penalise the breaker |
| 4xx (other) | fatal | malformed request; retry won't help |
| 5xx | transient | server-side error; retry after backoff |
| timeout / connection error | transient | network issue |
| TLS certificate error | fatal | peer identity won't change on retry |

Branch with [`resilient.IsTransient`] / [`resilient.IsFatal`]; both unwrap, so
they see the error through `fmt.Errorf("...: %w", err)` wrapping.

## Send variants

| Method | Behaviour |
| --- | --- |
| `Send` | one attempt; returns `*http.Response` (caller closes the body) |
| `SendWithRetry` | full-jitter retries on transient errors; fatal returned at once |
| `SendCached` | read-through cache for GET/HEAD 2xx; returns a buffered `CachedResponse` |
| `SendCoalesced` | single-flight: concurrent GET/HEAD with the same key share one fetch |
| `SendWithFallback` | on transient failure: stale cache → static fallback → original error |

## Configuration

`LoadConfig` parses YAML (unknown keys rejected); every field has a default, so
you only set what differs. Durations use Go syntax (`90s`, `1500ms`, `5m`).

```yaml
pool_max_idle_per_host: 100
pool_idle_timeout: 90s
tcp_keepalive: 30s
default_timeout: 5s
user_agent: "my-service/1.0"
dns_cache_enabled: true
dns_min_ttl: 10s
dns_max_ttl: 5m

outbound_targets:
  - name: "meta_events"
    selector: "://graph.facebook.com/{pixel_id}/events"  # metric label only
    rate_limit: 5000          # sustained req/sec
    timeout: 2s
    cb_threshold: 0.5         # failure ratio that opens the breaker
    cb_min_requests: 10       # min requests in-window before evaluating
    cb_window: 10s
    cb_half_open_timeout: 30s
    retry_max_attempts: 3     # 0 = no automatic retries
    retry_base: 100ms
    retry_cap: 30s
    adaptive_concurrency_enabled: true
    adaptive_concurrency_initial: 100
    adaptive_concurrency_min: 1
    adaptive_concurrency_max: 1000
```

A request whose `Target` matches no declared target transparently gets a
defaulted fallback policy on first use (1000 req/s, 50% breaker threshold).

## Metrics

A private `*prometheus.Registry` (expose via `client.Metrics().Handler()`):

```
http_outbound_requests_total{outbound_target,template_url,method,status,error_type}
http_outbound_request_duration_seconds{outbound_target,method}
circuit_breaker_state{outbound_target}                       0=closed 1=open 2=half_open
http_outbound_coalesce_hits_total{outbound_target}
http_outbound_fallback_hits_total{outbound_target}
http_outbound_retry_attempts_total{outbound_target}
http_outbound_adaptive_concurrency_limit{outbound_target}
```

## Concurrency

`Client` is safe for concurrent use; share one across the process. Policy lookup
is a lock-free `sync.Map` read; the rate limiter and circuit breaker are
atomic-backed; the adaptive limiter and coalescer take a short mutex only for
O(1) bookkeeping. The connection pool is the shared `http.Transport`.

## Develop

```sh
just test          # go test ./...
just test-verbose  # + race detector
just lint          # golangci-lint run
just bench         # micro-benchmarks for the hot paths
just cov           # coverage summary
```
