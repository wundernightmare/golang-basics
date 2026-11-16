# resilience

The resilience primitives the outbound clients share —
[`resilient-http-client`](../resilient-http-client) and [`grpcx`](../grpcx) —
so "the circuit is open" means the same thing over HTTP and over gRPC. Pure
logic: no I/O, no metrics, no logging, an injected clock.

| Type | What it is |
| --- | --- |
| `Breaker` | closed / open / half-open over a sliding failure ratio; CAS transitions, lock-free admission |
| `RetryBudget` | retries capped at `min_retries + ratio × requests` over a sliding window. A retry is granted while the retries in the window are *below* that bound, so the bound is rounded up: with a ratio above 0, one request in the window is enough for one retry — at one request per window every request can be retried |
| `Backoff` | the delay before retry N: exponential from an initial value, capped, with equal jitter (in `[d/2, d]`) — the schedule of the Kafka consumer and of the tasks outbox relay |
| `Jitter` | a TTL spread uniformly over ±10%, so entries cached together do not expire together |
| `ErrOpen` | the rejection: what `Breaker.Do` returns, and what a client's own "circuit open" error wraps, so `errors.Is(err, resilience.ErrOpen)` works over any of them |

```go
b, err := resilience.NewBreaker(resilience.DefaultBreakerConfig(), time.Now, onChange)
// err: what cfg.Validate() reports; b is nil, without an error, when the policy is not Enabled()

grant, ok := b.Allow()
if !ok { /* reject: the dependency is down */ }
// … send …
b.Record(grant, resilience.OutcomeFailure) // exactly once per granted ticket
```

`Do` is `Allow` and `Record` in one, and keeps "exactly once" on every way
out of the request:

```go
err := b.Do(func() resilience.Outcome {
	resp, sendErr = send()
	return classify(resp, sendErr) // Success, Failure or Ignored
})
if errors.Is(err, resilience.ErrOpen) { /* rejected: send was not called */ }
```

If the function panics, the request is recorded as a failure and the panic
goes on to the caller. With bare `Allow` / `Record` that is the caller's job
(`defer`): half-open has no deadline, so a probe that is never recorded keeps
its slot and the breaker admits nothing more until the process restarts.

A state mirrored somewhere (a gauge) is written through `Mirror`, from the
`onChange` hook and once at start. Transitions notify concurrently and
possibly out of order, so the hook's `to` may already be stale; `Mirror`
reads the state and calls the setter under one lock, so the last write is the
latest state:

```go
set := func(s resilience.State) { gauge.Set(float64(s)) }
b, err = resilience.NewBreaker(cfg, time.Now, func(from, to resilience.State) { b.Mirror(set) })
b.Mirror(set) // the series exists before the first transition; a nil breaker reports closed
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

`BreakerConfig.Validate() error` reports every problem at once
(`errors.Join`), named by key; `NaN` is not a ratio. `NewBreaker` runs it
and returns its error, so a policy that skipped validation cannot become a
breaker.

`failure_ratio: 0` means "the breaker is off" for every owner of a
`BreakerConfig`: `Enabled()` is false, `Validate` accepts it whatever the
other keys say, and `NewBreaker` returns nil. The zero `BreakerConfig` is
therefore off. An owner's default ratio lives in what it loads over (a
preset, an env default) and is not substituted for an explicit 0; an owner
that cannot run without a breaker rejects a policy that is not `Enabled()`.

The clocks are injected and must not be nil; each is read once at
construction. The sliding windows count the time elapsed since that reading
(`time.Time.Sub`), not a calendar position: with `time.Now` that is the
monotonic clock, so setting the wall clock forward or back neither empties a
window nor keeps stale counts in it. Any instant works — before 1970 or
beyond what `UnixNano` can express.

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
