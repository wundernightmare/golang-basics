# grpcx / integration

`grpcx` against a real gRPC server, over cleartext and TLS, through a network
that fails on purpose. A Go module of its own, inside `grpcx`'s folder:
testcontainers, the Docker client, Toxiproxy and the OTLP stubs are this
module's dependencies and never appear in `grpcx`'s `go.mod`.

```sh
just mod libs/grpcx/integration test   # from the repo root; needs Docker
go test -race ./...                    # here
```

About 40 seconds, eleven of them one keepalive test (grpc-go will not ping
more often than every ten seconds). Skipped under `-short` and without Docker;
**failed** when `CI` is set and a container cannot start. On rootless podman
set `TESTCONTAINERS_RYUK_DISABLED=true`.

## The stack

| Container | Image | Role |
| --- | --- | --- |
| jaeger | `jaegertracing/jaeger:2.21.0` | a real gRPC server: OTLP `TraceService/Export` (unary) on listeners for cleartext, TLS, TLS 1.3 only, a required client certificate and an expired certificate; `QueryService/FindTraces` (server-streaming, one message per trace); its own count of spans received |
| gateway | `caddy:2.10.2` | an HTTP/2 server that is *not* gRPC: what answers when an ingress or a load balancer stands where the service should be |
| postgres | `postgres:18-alpine` (shared, `libs/testx/containers`) | a peer that does not speak HTTP/2 at all |
| toxiproxy | `ghcr.io/shopify/toxiproxy:2.12.0` (shared) | cuts, resets, delays, throttles and black-holes the TCP connection in front of Jaeger |

**Certificates.** As in the HTTP client's suite: `TestMain` issues a CA for
the run and points `SSL_CERT_FILE` at it before anything dials, so a target
with no option at all — the library's default, TLS with the system roots — is
what talks to the TLS listener.

**Ground truth comes from the server.** `stored` reads, through Jaeger's query
API on a separate connection, how many spans the server holds for the test's
own service name. `arrived` reads the server's counter of spans received on
one listener that a single test owns. The two differ on purpose: Jaeger folds
byte-identical spans, so a request delivered twice is *received* twice and
*kept* once — which is exactly what the retry test needs to tell apart.

**No generated code for Jaeger's API.** The export uses the OTLP stubs. The
query is called by method name with a pass-through codec: the request is four
protobuf fields written with `protowire`, the answer is OTLP `TracesData`,
which the suite has the type for.

## What each file owns

| File | Layer | Asserts |
| --- | --- | --- |
| `protocol_test.go` | a healthy network | an export arrives over cleartext, default TLS, a TLS 1.3-only server and mutual TLS; the server's own verdicts (`ResourceExhausted` for eight megabytes, `Unimplemented`) are answers — breaker closed, nothing retried; a stream of a hundred messages is one call, counted when it ends; the target's timeout cuts a slow reader's stream mid-way and `timeout: 0` does not; a caller cancelling a stream is not a failure |
| `tls_test.go` | TLS | a server certificate from an unknown authority, for another name, expired; a client too old for the server; a client certificate required; TLS to the cleartext port and cleartext to the TLS port — all `Unavailable` with the cause in the message, fast, nothing reaching the server, breaker open; `wait_for_ready` on a handshake that can never succeed ends at the deadline and still names the cause |
| `peers_test.go` | the wrong peer | an HTTP gateway's 401 / 403 / 404 / 429 / 502 / 503 / 504 and a non-gRPC 200, over h2c and HTTP/2 with TLS: the code each becomes, which are retried, which the breaker counts; Postgres at a gRPC address; a lost acknowledgement — an error for a write that happened, an `OK` for a write delivered twice, and `retry_methods` as the remedy |
| `chaos_test.go` | TCP faults (Toxiproxy) | the TLS handshake black-holed in each direction, reset, cut; an outage that opens the breaker, rejections that touch no network, the probe that closes it; a short outage that retries do not bridge and `wait_for_ready` does; latency against the default deadline and a caller's shorter one; a stream cut mid-way, and one throttled into its deadline; a connection killed while idle, which `wait_for_ready` hides from the next call; keepalive failing a call on a silent connection in eleven seconds instead of sixty; `round_robin` and `pick_first` with a dead address |

## What it does not cover

- **Keepalive against a server's enforcement policy.** A server closes a
  connection whose client pings more often than it allows (every five minutes
  by default, `too_many_pings`). Reproducing it needs a call held open across
  three pings with no data flowing: half a minute of test for a rule that is
  grpc-go's, not this library's. The README of `grpcx` says to agree
  `keepalive_time` with the server.
- **Client-streaming and bidirectional calls against a real server.** Jaeger
  has none; the unit layer covers them with an in-process server.
- **Packet loss as such**, for the reason given in the HTTP client's suite:
  Toxiproxy works on the TCP stream.
- **The logic the unit layer owns**: which status the breaker counts, the
  service config rendered from a target, config validation.

## Adding a test

If an in-process server on loopback can show the behaviour, it belongs in
`../`. It belongs here when the answer depends on a real server, on TLS as a
real peer does it, on what stands between the client and the service, or on
what the network does to a long-lived connection.

Tests that add a toxic share the proxy with every other test that uses it:
they are not parallel. Tests count their effects under a service name of
their own (`testx.Unique`), never under a listener's counter — except the one
test that owns the `counted` listener.
