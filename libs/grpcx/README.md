# grpcx

Outbound gRPC connections with a resilience policy per logical **target** (a
dependency) — the gRPC counterpart of
[`resilient-http-client`](../resilient-http-client). A template library:
nothing in this repo imports it yet.

It is **wiring, not a client of its own**. grpc-go already implements most of
the policy; this package configures it from one YAML shape and adds what
grpc-go has no answer for:

| Concern | Implementation |
| --- | --- |
| Default deadline | gRPC service config `timeout` (a caller's shorter deadline wins) |
| Retries | gRPC service config `retryPolicy`: listed status codes, exponential backoff ±20 % jitter, server pushback (`grpc-retry-pushback-ms`) |
| Retry amplification | gRPC service config `retryThrottling` (token bucket per connection) |
| Load balancing | `round_robin` (default) or `pick_first` |
| Dead connections | gRPC keepalive pings — off by default, see below |
| Transport | TLS with the system roots unless `insecure: true`; other credentials via `WithDialOptions` |
| Circuit breaker | **added**: [`libs/resilience`](../resilience), the one the HTTP client uses, as an interceptor |
| Metrics | **added**: Prometheus under the workspace contract, on your registerer or a private registry |
| Tracing | `otelgrpc` stats handler: `traceparent` injected, a client span per attempt |

## Use

```go
cfg, err := grpcx.LoadConfig(yamlBytes)
if err != nil { return err }
c, err := grpcx.New(cfg, grpcx.WithLogger(log), grpcx.WithRegisterer(srv.Metrics.Registry))
if err != nil { return err }
defer c.Close()

conn, err := c.Conn("billing") // shared; never close it yourself
if err != nil { return err }
invoices := billingv1.NewInvoicesClient(conn)

resp, err := invoices.Get(ctx, req)
switch {
case errors.Is(err, grpcx.ErrCircuitOpen):
    // shed load: the dependency is down (status.Code(err) is Unavailable too)
case status.Code(err) == codes.NotFound:
    // an answer, not a failure
}
```

Errors are gRPC's own status errors, untouched. The one this package makes —
a call rejected by the breaker — is `errors.Is(err, grpcx.ErrCircuitOpen)`
and reads as `Unavailable` to code that only looks at the status.

## Semantics

**Order of a call** — the interceptor admits the call (breaker) and counts
it; below it gRPC applies the default deadline and makes the attempts. So the
breaker and `grpc_client_requests_total` see a call **once**, after its
retries; `grpc_client_retries_total` counts the attempts after the first.

**Breaker** — counts `Unavailable`, `DeadlineExceeded`, `Internal`, `Unknown`
and `DataLoss` as failures. `DeadlineExceeded` includes the caller's own
deadline: one shorter than the target's `timeout` must not hide an outage.
Every other status means the dependency answered (`NotFound`,
`InvalidArgument`, `ResourceExhausted`, `Unimplemented`, …); `Canceled` — the
caller walked away — is ignored. Half-open, generations and probes are
[`libs/resilience`](../resilience)'s.

**Streams** — a stream is one call, settled when it **ends**: the server's
status (`io.EOF` is OK), the single response of a client-streaming call, or
the caller's context ending first. A message on a stream decides nothing. A
caller that stops reading must cancel its context (gRPC's own rule) — that is
when an abandoned stream reports, and until then a half-open probe keeps its
slot. gRPC retries a stream only until the server's first message.

**Retries** — only the codes in `retry_codes` (default `UNAVAILABLE`), at
most 5 attempts (grpc-go's own cap; a larger value is rejected, not clamped).
gRPC does not know which methods are idempotent: when a retried code can mean
"partly done", list the safe ones in `retry_methods`. There is no per-attempt
timeout in grpc-go — `timeout` bounds the whole call. Retry throttling: a
failed call takes a token, a successful one returns `token_ratio`, and
retries stop while the bucket is at most half full.

**Keepalive** — off by default, on purpose: a gRPC server closes a connection
that pings more often than its enforcement policy allows (every 5 minutes by
default), so `keepalive_time` has to be agreed with the server side.

**Close** — closes every connection; calls in flight fail with `Canceled`
(gRPC has no client-side drain). Close after whatever makes the calls has
stopped.

## Configuration

`LoadConfig` overlays each target on the defaults (`go.yaml.in/yaml/v3`,
unknown keys rejected) and runs `Validate`. **Zero means disabled**, never
"use the default": an omitted key keeps its default, an explicit `0` turns the
feature off. In code, start from `DefaultTarget(name, address)`.

```yaml
targets:
  - name: billing                      # required, unique; the metric label
    address: dns:///billing.internal:443   # required: a gRPC target URI
    insecure: false                    # true = plaintext
    timeout: 5s                        # whole call, retries included; 0 = only the caller's deadline
    wait_for_ready: false              # true = queue calls while the connection is down
    load_balancing: round_robin        # or pick_first
    keepalive_time: 0s                 # 0 = off; else ≥ 10s, agreed with the server
    keepalive_timeout: 20s
    retry_max_attempts: 3              # total, first included; ≤ 5; 0/1 = no retries
    retry_base_delay: 100ms            # doubles per retry
    retry_max_delay: 5s
    retry_codes: [UNAVAILABLE]         # canonical status names
    retry_methods: []                  # empty = every method; else pkg.Service or pkg.Service/Method
    retry_throttle_max_tokens: 10      # ≤ 1000; 0 = no throttling
    retry_throttle_token_ratio: 0.1
    breaker_failure_ratio: 0.5         # (0, 1]; 0 = no breaker
    breaker_min_requests: 20
    breaker_window: 10s
    breaker_open_timeout: 30s
    breaker_half_open_probes: 1
```

**In a service config.** A service does not call `LoadConfig`: it embeds
`grpcx.Config` under a key of its own config and loads everything with the
workspace's one loader, `httpx.LoadYAML` (pinned by `embed_test.go`):

```go
type Config struct {
    httpx.Config `yaml:",inline"`
    GRPC grpcx.Config `yaml:"grpc"` // grpc: {targets: [...]} in the file
}
```

Targets still default themselves and unknown keys are still rejected. Two
things are the service's: call `cfg.GRPC.Validate()` in its own `Validate`
(the loader parses, it does not validate), and put a per-environment address
into that environment's file — targets are a list, so the environment overlay
does not reach them.

The service config comes from this file only: one a resolver would supply (a
DNS TXT record) is ignored. An undeclared target name is an error at `Conn`.

## Metrics

```
grpc_client_requests_total{target,method,outcome}      one per call (not per attempt)
grpc_client_request_duration_seconds{target,method}    calls that reached gRPC, retries included, until they end
grpc_client_retries_total{target}                      attempts after a call's first
grpc_client_circuit_breaker_state{target}              0 closed, 1 open, 2 half-open
grpc_client_circuit_breaker_transitions_total{target,from,to}
```

`outcome` is the status code in lower case (`ok`, `unavailable`,
`deadline_exceeded`, … — `canceled` spelt like the HTTP client's) or
`circuit_open`; a code gRPC does not define is `unknown`. `method` is the full
method name (`/pkg.Service/Method`), which comes from generated code. The
names are the workspace's metrics contract
([docs/METRICS.md](../../docs/METRICS.md)).

Without `WithRegisterer` they live on a private registry, `client.Registry()`,
which no `/metrics` endpoint serves (one client per registry — a second one
collides).

## What it does not do

No rate limiter or bulkhead (add an interceptor through `WithDialOptions` if a
target needs one), no hedging and no per-attempt timeout (grpc-go has
neither), no outlier detection or xDS, no server side. The breaker is per
target, not per backend address: it is meant for a dependency behind one
virtual address; with client-side balancing over many addresses, gRPC's
outlier detection is the finer tool.

## Develop

```sh
just test          # go test ./... — a real gRPC server on loopback, no Docker
just test-verbose  # + race detector
just lint          # golangci-lint run
just mutate libs/grpcx   # from the repo root; thresholds in .gremlins.yaml
```
