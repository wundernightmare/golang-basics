# resilience

The resilience primitives the outbound clients share —
[`resilient-http-client`](../resilient-http-client) and [`grpcx`](../grpcx) —
so "the circuit is open" means the same thing over HTTP and over gRPC. Pure
logic: no I/O, no metrics, no logging, an injected clock.

| Type | What it is |
| --- | --- |
| `Breaker` | closed / open / half-open over a sliding failure ratio; CAS transitions, lock-free admission |
| `RetryBudget` | retries capped at `min_retries + ratio × requests` over a sliding window |
| `Backoff` | the delay before retry N: exponential from an initial value, capped, with equal jitter (in `[d/2, d]`) — the schedule of the Kafka consumer and of the tasks outbox relay |

```go
b := resilience.NewBreaker(resilience.DefaultBreakerConfig(), time.Now, onChange) // nil when FailureRatio is 0

grant, ok := b.Allow()
if !ok { /* reject: the dependency is down */ }
// … send …
b.Record(grant, resilience.OutcomeFailure) // exactly once per granted ticket
```

**Breaker semantics.** Closed counts requests and failures in
`breaker.window`; with at least `breaker.min_requests` and failures ≥
`breaker.failure_ratio`, it opens. After `breaker.open_timeout` it admits
exactly `breaker.half_open_probes` probes: all must succeed to close it, any
failure re-opens it. Three outcomes: `OutcomeSuccess`, `OutcomeFailure`, and
`OutcomeIgnored` — the request says nothing about the dependency (the caller
cancelled, a local limit rejected it) and, for a probe, gives its slot back.
Every transition starts a new generation and a ticket carries the one it was
admitted in, so a result that arrives late can never decide a newer state.

A nil `*Breaker` / `*RetryBudget` admits everything: "disabled" needs no
branch at the call site. What counts as a failure — an HTTP status, a gRPC
code, a timeout — is the client's decision, not this package's.

`BreakerConfig` is the one way to configure a breaker. Whatever has one has
a field of that type — `Breaker resilience.BreakerConfig` under
`envPrefix:"BREAKER_"` / `yaml:"breaker"` — so the HTTP client, the gRPC
client, the cache and the ingest sinks all read the same five keys under
their own name (`breaker.failure_ratio`, `…_BREAKER_FAILURE_RATIO`). The
tags carry no defaults: each owner sets its own policy before loading.

## Tests

Examples (`breaker_test.go`, `budget_test.go`) fix one path each, with a fake
clock. `property_internal_test.go` states the rules for every path
([rapid](https://pgregory.net/rapid) draws the sequences and shrinks a
failing one): the breaker as a state machine — legal transitions only,
nothing admitted while open, the half-open probe slots, late results
changing nothing; the sliding window against a model that keeps every event;
the retry budget granting exactly what its rule allows. Mutation testing
(`.gremlins.yaml`) checks that the two together notice a wrong comparison.

A failed property prints the sequence and a `-rapid.failfile` to replay it;
`-rapid.checks=5000` searches longer.

## Develop

```sh
just test          # go test ./...
just test-verbose  # + race detector
just bench         # Allow/Record under contention
just mutate libs/resilience   # from the repo root; thresholds in .gremlins.yaml
```
