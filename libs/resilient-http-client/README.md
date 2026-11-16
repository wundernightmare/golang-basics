# resilient-http-client

An outbound HTTP client with a resilience policy per logical **target** (a
dependency). A template library: nothing in this repo imports it yet; copy or
depend on it when a service calls other services.

One `Client` per process; each request names its target, and the target's
policy applies:

| Concern | Implementation |
| --- | --- |
| Transport | its own `http.Transport`, set up as net/http sets up its default one (proxy from env, HTTP/2, dial/TLS timeouts) — never a clone of the `http.DefaultTransport` variable — + pool settings + HTTP/2 PING health check (`net/http`'s `HTTP2Config`, no `x/net`) |
| TLS trust | the system's authorities, or per target a `ca_file` that is its whole trust store ([`libs/secret`](../secret) `CertPool`) |
| Tracing | `otelhttp` transport: `traceparent` injected, client span `"METHOD target"`; its `url.full` is scheme, host and path |
| Attempt timeout | per target; starts **before** the rate-limiter and bulkhead waits |
| Rate limit | `golang.org/x/time/rate` token bucket (fractional rates) |
| Bulkhead | fixed concurrency cap, bounded wait |
| Circuit breaker | [`libs/resilience`](../resilience) (shared with [`grpcx`](../grpcx)): closed / open / half-open, CAS transitions, generation-tagged probes |
| Retries | `SendWithRetry`: idempotent requests only, full-jitter backoff, `Retry-After`, retry budget |
| Redirects | same host only, at most 10 (both configurable) |
| Metrics | Prometheus, on your registerer or a private registry |
| Shutdown | refuses new requests, waits for in-flight ones, closes idle connections |

## Use

```go
cfg, err := resilient.ParseYAML(yamlBytes) // or resilient.DefaultConfig("billing")
if err != nil { return err }
c, err := resilient.New(cfg, resilient.WithLogger(log), resilient.WithRegisterer(prometheus.DefaultRegisterer))
if err != nil { return err }
defer c.Shutdown(shutdownCtx)

req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://billing.internal/v1/invoices/42", nil)
resp, err := c.SendWithRetry("billing", req)
switch {
case err == nil:
    defer resp.Body.Close() // status < 400; Close releases the bulkhead slot and the attempt timeout
    // reading resp.Body can fail with an *OutboundError too (KindTimeout, KindConnection)
case errors.Is(err, resilient.KindStatus):
    var oe *resilient.OutboundError
    errors.As(err, &oe) // oe.StatusCode, oe.Header, oe.RetryAfter, oe.Body (first 4 KiB)
case errors.Is(err, resilient.KindCircuitOpen), errors.Is(err, resilient.KindRateLimited):
    // shed load: the dependency is down or we are over our own limit
}
```

The request's context is the overall deadline; `Send` sends once,
`SendWithRetry` retries when it is safe to. Every failure is an
`*OutboundError` whose `Kind` works with `errors.Is`:
`KindTimeout`, `KindConnection`, `KindStatus` (≥ 400), `KindCircuitOpen`,
`KindRateLimited`, `KindBulkheadFull`, `KindShutdown`, `KindCanceled`,
`KindRedirect`, `KindInvalid` (unknown target, relative URL). Two kinds also
answer to the names the workspace's other clients use:
`errors.Is(err, resilience.ErrOpen)` for `KindCircuitOpen` and
`errors.Is(err, resilient.ErrClosed)` for `KindShutdown`.

`Error()` never contains the request's query string or user info: of a failed
request it keeps the cause net/http's `*url.Error` wraps, not that error
(`errors.As(err, &urlErr)` does not match).

## Semantics

**Breaker** — counts 5xx, attempt timeouts and connection errors as
failures, and so the caller's own **deadline** passing while the request is
with the dependency (still reported as `KindCanceled`): a caller deadline
shorter than the target's `timeout` must not hide an outage. A 4xx is a
success for it (429 included: the dependency answered). The caller
cancelling and local rejections tell it nothing.

**Response body** — an attempt lasts until its body is read to the end or
closed, and only then does the breaker hear a success. The attempt timeout
covers the read; a read that fails returns an `*OutboundError`
(`KindTimeout`, `KindConnection`, `KindCanceled`) and counts for the breaker
like the same failure before the headers. If the attempt's context ends while
the caller still holds the body, the breaker hears that too — and the attempt
is released (bulkhead slot, in-flight count) without waiting for a `Close`
that may never come: a forgotten body costs one attempt timeout, not a slot
for the life of the process. A failed body is
not retried (the response was already handed over), and
`http_client_requests_total` keeps the outcome known at the headers (`2xx`).

An **upgraded connection** (101 Switching Protocols) is the exception. Its
body is the connection and stays writable; once the protocol has switched the
attempt is over: the breaker hears a success and the target's `timeout` no
longer applies. The connection lasts until the body is closed or the caller's
context ends, and holds its bulkhead slot and its place in `Shutdown`'s wait
that long.

The body of an **error response** (≥ 400) is closed after its first 4 KiB are
kept; reading off the rest to keep the connection is left to net/http.

**Half-open** — when open for `breaker.open_timeout`, the breaker admits exactly
`breaker.half_open_probes` probes; all must succeed to close it, any failure
re-opens it. A probe rejected locally releases its slot, and a probe whose
`RoundTripper` panics has failed (the panic reaches the caller; the bulkhead
slot is released too); a request admitted
before the breaker opened cannot decide a probe (results carry the generation
they were admitted in).

**Retries** — only GET, HEAD, OPTIONS, PUT, DELETE, or a request with an
`Idempotency-Key` header, and only with a replayable body (`GetBody`, which
`http.NewRequest` sets for in-memory bodies). Retried: timeouts, connection
errors, 408, 425, 429, 5xx except 501/505. Never: local rejections,
cancellation, a server certificate the client rejects (unknown authority,
wrong name, expired — the next attempt would meet the same certificate; it
still counts against the breaker). The delay is full-jitter backoff or `Retry-After` (seconds or
HTTP-date), whichever is longer; a `Retry-After` above `retry_max_delay`, or a
delay past the caller's deadline, ends the retries. The **retry budget** caps
retries per target at `retry_budget_min_retries + retry_budget_ratio ×
requests` over a sliding `retry_budget_window`, so an outage cannot multiply
the load on the dependency. An error the budget refused to retry has
`OutboundError.RetryBudgetExhausted` set — unlike one whose attempts ran out.

**DNS** — the client has no DNS code: net/http resolves a name once per new
connection, and what follows from that is tested against a real name server
(`dns_test.go`):

- A name that does not resolve (`NXDOMAIN`, `SERVFAIL`) is `KindConnection`:
  a breaker failure, retried, the `*net.DNSError` kept as the cause. Every
  attempt resolves again, so a record that appears or is corrected while the
  client retries is found.
- A name server that does not answer costs an attempt its `timeout`
  (`KindTimeout`), not the resolver's own patience (seconds per try, several
  tries). The lookup itself lives on in the background until the resolver
  gives up.
- **A live connection never asks again.** After a record moves, requests
  keep going to the old address for as long as that server keeps the
  connection open; the new one is found by the next connection — after the
  old server closes this one, or after `idle_conn_timeout` without traffic.
  Under steady traffic to a server that stays up, that is never: net/http has
  no maximum connection age. Retire the old server (it closes its
  connections) rather than wait for clients to leave it.

**Shutdown** — a request counts as in flight from before the shutdown check to
the end of its whole retry loop, or until its body is closed on success; so
`Shutdown` never returns while a request can still reach the network. It may
be called more than once, concurrently too: every caller returns when the
client is idle.

## Configuration

`ParseYAML` overlays the YAML on the defaults (`go.yaml.in/yaml/v3`, unknown
keys rejected) and runs `Validate`; what either refuses matches
`errors.Is(err, resilient.ErrConfig)`. **Zero means disabled**, never "use the
default": an omitted key keeps its default, an explicit `0` turns the feature
off. In code, start from `DefaultConfig(names...)` / `DefaultTarget(name)`.
At least one target is required: `Validate`, and so `ParseYAML` and `New`,
refuse a config without any.

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
    ca_file: ""                  # PEM bundle = the target's whole trust store; "" = the system's
    timeout: 5s                  # per attempt, waits included; 0 = none
    rate_limit: 0                # req/s, fractions ok, finite; 0 = off
    rate_burst: 0                # 0 = max(1, ceil(rate_limit)), at most MaxInt32
    max_concurrent: 0            # bulkhead; 0 = off
    max_concurrent_wait: 0s      # 0 = reject at once when full
    breaker:
      failure_ratio: 0.5   # (0, 1]; 0 = no breaker
      min_requests: 20
      window: 10s
      open_timeout: 30s
      half_open_probes: 1
    retry_max_attempts: 3        # total, first included; 0/1 = no retries
    retry_base_delay: 100ms
    retry_max_delay: 5s
    retry_budget_ratio: 0.2      # [0, 1]
    retry_budget_min_retries: 10
    retry_budget_window: 10s     # 0 = no budget
```

**`ca_file`** — a server certificate must chain to the bundle; the system's
authorities are not consulted for that target. `Validate` reads the file (a
missing file, a damaged block or no certificate is a configuration error),
`New` reads it once more and never again: a rotated bundle needs a restart.
Targets naming the same file share a transport and its connection pool,
separate from the others', so `max_idle_conns` and the per-host limits count
per trust store. It does not combine with `WithHTTPClient` (`New` fails):
there the trust is that client's. There is no key for a client certificate —
mTLS goes through `WithHTTPClient`.

**`rate_limit` with `timeout: 0`** — the wait for a token is bounded by the
attempt timeout and the caller's context only. With neither, a request waits
for as long as the bucket takes to refill.

**In a service config.** A service does not call `ParseYAML`: it embeds
`resilient.Config` under a key of its own config and loads everything with
the workspace's one loader, `httpx.LoadYAML` (pinned by `integration/embed_test.go`):

```go
type Config struct {
    httpx.Config `yaml:",inline"`
    HTTPClient resilient.Config `yaml:"http_client"`
}

cfg := Config{HTTPClient: resilient.DefaultConfig()} // preset: see below
err := httpx.LoadYAML(path, prefix, &cfg)
```

**Preset `DefaultConfig()` before loading.** The transport defaults
(`max_idle_conns_per_host: 32`, the HTTP/2 ping, …) are not struct tags, so
the loader cannot supply them: without the preset an omitted key is zero —
net/http's 2 idle connections per host, no health check. Targets default
themselves either way. Two more things are the service's: call
`cfg.HTTPClient.Validate()` in its own `Validate` (the loader parses, it does
not validate; a service that declared no target fails there), and keep per-environment values in that environment's file —
the client's keys have no environment form.

An undeclared target name is an error at `Send` — there is no lazily created
fallback policy.

## Metrics

```
http_client_requests_total{target,method,outcome}      one per attempt
http_client_request_duration_seconds{target,method}    attempts that reached the network
http_client_retries_total{target}
http_client_retry_budget_exhausted_total{target}
http_client_circuit_breaker_state{target}              0 closed, 1 open, 2 half-open
http_client_circuit_breaker_transitions_total{target,from,to}
http_client_bulkhead_in_flight{target}
http_client_bulkhead_rejected_total{target}
```

`outcome`: `2xx 3xx 4xx 5xx timeout connection redirect circuit_open
rate_limited bulkhead_full shutdown canceled invalid` (`invalid`: a request
that could not be sent — a relative URL, a body that cannot be replayed; an
undeclared target name is an error but never a label value). `method` is a
standard method or `_OTHER`. The names are the workspace's metrics contract
([docs/METRICS.md](../../docs/METRICS.md)), shared with nodejs-basics.

Without `WithRegisterer` they live on a private registry, `client.Registry()`,
which no `/metrics` endpoint serves: a service passes
`resilient.WithRegisterer(srv.Metrics.Registry)` so its outbound calls show up
next to everything else.

**Two clients on one registry** collide — `New` returns the registration
error. Give each a label of your own; each, because one metric name carries
one set of label names:

```go
billing, err := resilient.New(billingCfg, resilient.WithRegisterer(
    prometheus.WrapRegistererWith(prometheus.Labels{"client": "billing"}, reg)))
search, err := resilient.New(searchCfg, resilient.WithRegisterer(
    prometheus.WrapRegistererWith(prometheus.Labels{"client": "search"}, reg)))
```

**otelhttp's own metrics** — the tracing transport also records
`http.client.request.duration` and `http.client.request.body.size` on the
global OpenTelemetry `MeterProvider` (`otel.GetMeterProvider()`); the client
has no option to point them elsewhere. They are not on the Prometheus
registry and not part of `docs/METRICS.md`: with no meter provider installed
they go nowhere, with one they are exported next to the series above.

Logs (`WithLogger`), error texts and the span's `url.full` carry scheme, host
and path only — never the query string or user info — and warnings are
rate-limited per target.

## What it does not do

No response cache, request coalescing, fallbacks, DNS cache or adaptive
concurrency. No hedged requests, no per-request policy override, no retry of
non-idempotent requests without an `Idempotency-Key`. `WithHTTPClient` builds
on a copy of your client, which is never modified; its idle connections are
yours to close.

## Develop

```sh
just test          # go test ./... — unit, on loopback sockets, no Docker
just test-verbose  # + race detector
just lint          # golangci-lint run
just bench         # Send micro-benchmark (the breaker's is in libs/resilience)
just mutate libs/resilient-http-client   # from the repo root; scope in .gremlins.yaml
just mod libs/resilient-http-client/integration test   # from the repo root; needs Docker
```

## Tests

| Layer | Where | Owns |
| --- | --- | --- |
| Unit | `*_test.go` here — `httptest` on loopback, no mocks of the transport; `dns_test.go` with a real name server on loopback (`testx.DNS`) and Go's resolver | every decision: what is retried, what the breaker hears, the order of an attempt, shutdown, config; a name that does not resolve, a silent name server, a record that moves |
| Mutation | `.gremlins.yaml`, nightly | whether those tests notice a flipped comparison in the decision logic |
| Fuzz | `FuzzParseRetryAfter` | the one parser of external bytes |
| Integration | [`integration/`](integration) — a module of its own, so testcontainers, Toxiproxy and an HTTP/3 stack stay out of this `go.mod` | what only real servers and a real network show: the protocol the server saw (HTTP/1.1, h2c, HTTP/2, HTTP/3), TLS trust and handshake failures, TCP faults at a named point, a real API's view of retries |

The integration suite does not repeat the unit layer: it has no test of the
backoff arithmetic or the breaker's window. See its
[README](integration/README.md).
