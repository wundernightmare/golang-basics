# resilient-http-client

An outbound HTTP client with a resilience policy per logical **target** (a
dependency). A template library: nothing in this repo imports it yet; copy or
depend on it when a service calls other services.

One `Client` per process; each request names its target, and the target's
policy applies:

| Concern | Implementation |
| --- | --- |
| Transport | `http.DefaultTransport` clone (proxy from env, HTTP/2, dial/TLS timeouts) + pool settings + HTTP/2 PING health check (`net/http`'s `HTTP2Config`, no `x/net`) |
| Tracing | `otelhttp` transport: `traceparent` injected, client span `"METHOD target"` |
| Attempt timeout | per target; starts **before** the rate-limiter and bulkhead waits |
| Rate limit | `golang.org/x/time/rate` token bucket (fractional rates) |
| Bulkhead | fixed concurrency cap, bounded wait |
| Circuit breaker | closed / open / half-open, CAS transitions, generation-tagged probes |
| Retries | `SendWithRetry`: idempotent requests only, full-jitter backoff, `Retry-After`, retry budget |
| Redirects | same host only, at most 10 (both configurable) |
| Metrics | Prometheus, on your registerer or a private registry |
| Shutdown | refuses new requests, waits for in-flight ones, closes idle connections |

## Use

```go
cfg, err := resilient.LoadConfig(yamlBytes) // or resilient.DefaultConfig("billing")
if err != nil { return err }
c, err := resilient.New(cfg, resilient.WithLogger(log), resilient.WithRegisterer(prometheus.DefaultRegisterer))
if err != nil { return err }
defer c.Shutdown(shutdownCtx)

req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://billing.internal/v1/invoices/42", nil)
resp, err := c.SendWithRetry("billing", req)
switch {
case err == nil:
    defer resp.Body.Close() // status < 400; Close releases the bulkhead slot and the attempt timeout
case errors.Is(err, resilient.KindStatus):
    var oe *resilient.OutboundError
    errors.As(err, &oe) // oe.StatusCode, oe.RetryAfter, oe.Body (first 4 KiB)
case errors.Is(err, resilient.KindCircuitOpen), errors.Is(err, resilient.KindRateLimited):
    // shed load: the dependency is down or we are over our own limit
}
```

The request's context is the overall deadline; `Send` sends once,
`SendWithRetry` retries when it is safe to. Every failure is an
`*OutboundError` whose `Kind` works with `errors.Is`:
`KindTimeout`, `KindConnection`, `KindStatus` (≥ 400), `KindCircuitOpen`,
`KindRateLimited`, `KindBulkheadFull`, `KindShutdown`, `KindCanceled`,
`KindRedirect`, `KindInvalid` (unknown target, relative URL).

## Semantics

**Breaker** — counts 5xx, attempt timeouts and connection errors as
failures. It ignores the caller's cancellation (the parent context ending,
deadline included), 4xx (429 included: the dependency answered) and local
rejections. When open for `breaker_open_timeout`, it admits exactly
`breaker_half_open_probes` probes; all must succeed to close it, any failure
re-opens it. A probe rejected locally releases its slot; a request admitted
before the breaker opened cannot decide a probe (results carry the generation
they were admitted in).

**Retries** — only GET, HEAD, OPTIONS, PUT, DELETE, or a request with an
`Idempotency-Key` header, and only with a replayable body (`GetBody`, which
`http.NewRequest` sets for in-memory bodies). Retried: timeouts, connection
errors, 408, 425, 429, 5xx except 501/505. Never: local rejections,
cancellation. The delay is full-jitter backoff or `Retry-After` (seconds or
HTTP-date), whichever is longer; a `Retry-After` above `retry_max_delay`, or a
delay past the caller's deadline, ends the retries. The **retry budget** caps
retries per target at `retry_budget_min_retries + retry_budget_ratio ×
requests` over a sliding `retry_budget_window`, so an outage cannot multiply
the load on the dependency.

**Shutdown** — a request counts as in flight from before the shutdown check to
the end of its whole retry loop, or until its body is closed on success; so
`Shutdown` never returns while a request can still reach the network.

## Configuration

`LoadConfig` overlays the YAML on the defaults (`go.yaml.in/yaml/v3`, unknown
keys rejected) and runs `Validate`. **Zero means disabled**, never "use the
default": an omitted key keeps its default, an explicit `0` turns the feature
off. In code, start from `DefaultConfig(names...)` / `DefaultTarget(name)`.

```yaml
max_idle_conns: 100              # 0 = unlimited
max_idle_conns_per_host: 32      # 0 = net/http's 2
max_conns_per_host: 0            # 0 = unlimited
idle_conn_timeout: 90s           # 0 = never
response_header_timeout: 0s      # 0 = none
http2_ping_interval: 30s         # 0 = no HTTP/2 health check
http2_ping_timeout: 15s
max_redirects: 10                # 0 = return the 3xx as is
allow_cross_host_redirects: false
user_agent: ""
targets:
  - name: billing                # required, unique; the metric label
    timeout: 5s                  # per attempt, waits included; 0 = none
    rate_limit: 0                # req/s, fractions ok; 0 = off
    rate_burst: 0                # 0 = max(1, ceil(rate_limit))
    max_concurrent: 0            # bulkhead; 0 = off
    max_concurrent_wait: 0s      # 0 = reject at once when full
    breaker_failure_ratio: 0.5   # (0, 1]; 0 = no breaker
    breaker_min_requests: 20
    breaker_window: 10s
    breaker_open_timeout: 30s
    breaker_half_open_probes: 1
    retry_max_attempts: 3        # total, first included; 0/1 = no retries
    retry_base_delay: 100ms
    retry_max_delay: 5s
    retry_budget_ratio: 0.2      # [0, 1]
    retry_budget_min_retries: 10
    retry_budget_window: 10s     # 0 = no budget
```

An undeclared target name is an error at `Send` — there is no lazily created
fallback policy.

## Metrics

```
http_client_requests_total{target,method,outcome}      one per attempt
http_client_request_duration_seconds{target,method}    attempts that reached the network
http_client_retries_total{target}
http_client_retry_budget_exhausted_total{target}
circuit_breaker_state{target}                          0 closed, 1 open, 2 half-open
circuit_breaker_transitions_total{target,from,to}
http_client_bulkhead_in_flight{target}
http_client_bulkhead_rejected_total{target}
```

`outcome`: `2xx 3xx 4xx 5xx timeout connection redirect circuit_open
rate_limited bulkhead_full shutdown canceled`. Without `WithRegisterer` they
live on a private registry, `client.Registry()`.

Logs (`WithLogger`) carry scheme, host and path only — never the query string
or user info — and warnings are rate-limited per target.

## What it does not do

No response cache, request coalescing, fallbacks, DNS cache or adaptive
concurrency (removed: they were incorrect or not worth their weight). No
hedged requests, no per-request policy override, no retry of non-idempotent
requests without an `Idempotency-Key`. `WithHTTPClient` builds on a copy of
your client, which is never modified; its idle connections are yours to close.

## Develop

```sh
just test          # go test ./...
just test-verbose  # + race detector
just lint          # golangci-lint run
just bench         # breaker and Send micro-benchmarks
just mutate libs/resilient-http-client   # from the repo root; scope in .gremlins.yaml
```
