# resilient-http-client / integration

The client against real servers, over real protocols, through a network that
fails on purpose. A Go module of its own, inside the client's folder:
testcontainers, the Docker client, Toxiproxy and an HTTP/3 implementation are
this module's dependencies and never appear in the client's `go.mod`.

```sh
just mod libs/resilient-http-client/integration test   # from the repo root; needs Docker
go test -race ./...                                    # here
```

About 45 seconds. Skipped under `-short` and without Docker; **failed** when
`CI` is set and a container cannot start. On rootless podman set
`TESTCONTAINERS_RYUK_DISABLED=true`, or every test skips after three minutes.

## The stack

| Container | Image | Role |
| --- | --- | --- |
| edge | `caddy:2.10.2` | a production HTTP server: HTTP/1.1 and h2c in cleartext, HTTP/1.1 and HTTP/2 over TLS, HTTP/3 over QUIC; listeners for TLS 1.3 only, for a required client certificate and for an expired certificate |
| redpanda | `redpandadata/redpanda:v26.2.3` | a real, stateful HTTP API (the HTTP Proxy): how many times a produce request arrived is how many records the topic holds; a fetch on an empty topic is a server that takes its time |
| postgres | `postgres:18-alpine` (shared, `libs/testx/containers`) | a peer that does not speak HTTP |
| toxiproxy | `ghcr.io/shopify/toxiproxy:2.12.0` (shared) | cuts, resets, delays, throttles and black-holes the TCP connection in front of the edge and Redpanda |

Caddy and Redpanda are this suite's own fixtures (`containers.Fixture`);
Toxiproxy fronts them through `containers.ProxyTo`. One of each per test
binary; tests isolate by topic name.

**Certificates.** `TestMain` issues a CA and leaf certificates for the run and
points `SSL_CERT_FILE` at the CA before anything dials. The client under test
is therefore the one a service gets — `resilient.New(cfg)`, its own transport,
no test hook — and it trusts the edge because the process's trust store says
so. Only tests that need something the default negotiation would not pick
(HTTP/1.1 over TLS, h2c, HTTP/3, a client certificate, an empty trust store)
hand in a transport with `WithHTTPClient`.

**Ground truth comes from the server.** `/whoami` on the edge reports the
protocol and TLS version the server spoke and the client's source port — which
identifies the connection. A test that claims "HTTP/3" or "the connection was
replaced" asserts what Caddy saw, not what the client believes. Records are
counted in Redpanda on a separate, direct connection.

## What each file owns

| File | Layer | Asserts |
| --- | --- | --- |
| `protocol_test.go` | protocols, healthy network — every test runs over all five wires | the server saw the protocol; one connection is reused; a second request during a slow one shares the connection on HTTP/2 and 3 and opens another on HTTP/1.1; a megabyte up and four down arrive intact; statuses, `Retry-After` and retry counts against a real server; the attempt timeout on a slow server, and what it does to the connection per protocol; a forgotten body gives its bulkhead slot back; closing a body early; a record produced to Redpanda over each protocol |
| `tls_test.go` | TLS | a certificate from an unknown authority, for another name, expired — refused once, not retried, counted by the breaker; a server that refuses the client's TLS version or demands a certificate; HTTP on the TLS port and TLS on the cleartext port; the redirect policy against real `Location` headers (loop, cross-host, https→http); HTTP/3 to a port nobody listens on |
| `chaos_test.go` | TCP faults (Toxiproxy) | the TLS handshake black-holed in each direction, reset, cut mid-flight, delivered in 16-byte pieces; an outage that opens the breaker, rejections that touch no network, the probe that closes it; an outage that ends while the client retries; latency against the attempt timeout; a connection cut in the middle of the body; a throttled body against the timeout; idle connections killed behind the client's back; `http2_ping_interval` failing a request on a silent connection in ping time instead of at the timeout; an HTTP/2 connection dying under concurrent streams |
| `peers_test.go` | peers that are not a well-behaved HTTP server | a lost acknowledgement from Redpanda: one record for a POST, one for a POST under `SendWithRetry`, three for a POST with an `Idempotency-Key` the server does not honour; Postgres on an `http://` and an `https://` URL |

## What it does not cover

- **Packet loss as such.** Toxiproxy works on the TCP stream: it can stop,
  delay, cut and reset it, not drop individual IP packets (that is `tc netem`,
  which needs privileges CI containers do not have). A black-holed stream is
  what sustained loss looks like to the client; a 30 % loss rate with
  retransmissions is not reproduced.
- **Faults under HTTP/3.** Toxiproxy does not proxy UDP. HTTP/3 gets the whole
  protocol layer on a healthy network and one dead-port test.
- **The logic the unit layer owns.** Backoff arithmetic, the retry budget, the
  breaker's window and half-open admission, config validation: `../*_test.go`
  and the mutation run, on loopback.

## Adding a test

Put it at the lowest layer that can observe it. If `httptest` on loopback can
show the behaviour, it belongs in `../`. It belongs here when the answer
depends on a real server's implementation of a protocol, on TLS as a real
peer does it, or on what the network does to a connection.

Tests that add a toxic share the proxy with every other test that uses it:
they are not parallel. Tests that make Redpanda hold a request run their
protocols in turn (`eachWireInTurn`) — its HTTP Proxy serves long polls one at
a time.
