# resilience

The resilience primitives the outbound clients share —
[`resilient-http-client`](../resilient-http-client) and [`grpcx`](../grpcx) —
so "the circuit is open" means the same thing over HTTP and over gRPC. Pure
logic: no I/O, no metrics, no logging, an injected clock.

| Type | What it is |
| --- | --- |
| `Breaker` | closed / open / half-open over a sliding failure ratio; CAS transitions, lock-free admission |
| `RetryBudget` | retries capped at `min_retries + ratio × requests` over a sliding window |

```go
b := resilience.NewBreaker(resilience.DefaultBreakerConfig(), time.Now, onChange) // nil when FailureRatio is 0

grant, ok := b.Allow()
if !ok { /* reject: the dependency is down */ }
// … send …
b.Record(grant, resilience.OutcomeFailure) // exactly once per granted ticket
```

**Breaker semantics.** Closed counts requests and failures in
`breaker_window`; with at least `breaker_min_requests` and failures ≥
`breaker_failure_ratio`, it opens. After `breaker_open_timeout` it admits
exactly `breaker_half_open_probes` probes: all must succeed to close it, any
failure re-opens it. Three outcomes: `OutcomeSuccess`, `OutcomeFailure`, and
`OutcomeIgnored` — the request says nothing about the dependency (the caller
cancelled, a local limit rejected it) and, for a probe, gives its slot back.
Every transition starts a new generation and a ticket carries the one it was
admitted in, so a result that arrives late can never decide a newer state.

A nil `*Breaker` / `*RetryBudget` admits everything: "disabled" needs no
branch at the call site. What counts as a failure — an HTTP status, a gRPC
code, a timeout — is the client's decision, not this package's.

`BreakerConfig` carries the yaml keys (`breaker_*`) and their validation, so
both clients' target configs read the same.

## Develop

```sh
just test          # go test ./...
just test-verbose  # + race detector
just bench         # Allow/Record under contention
just mutate libs/resilience   # from the repo root; thresholds in .gremlins.yaml
```
