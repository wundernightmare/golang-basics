# valkey integration suite

`libs/valkey` against a real Valkey: the shared plaintext server from
`libs/testx/containers.Valkey` and a dedicated fixture that requires a client
certificate. A module of its own so that testcontainers, the Docker client and
Toxiproxy stay out of the `go.mod` of `libs/valkey`.

It owns what only a real server can say:

- commands round-trip with their TTLs; `SET NX` against a value and a
  tombstone, compare-and-set (`Replace`), and `Aside` on top of them — one load
  for concurrent misses, no resurrection after an invalidation;
- Pub/Sub delivery, resubscription after the connection was lost, and the
  pause between attempts;
- what the server sees of the connections (`CLIENT LIST`): the pipelined
  footprint, the `pool_size` bound, recycling by `conn_lifetime`, a silent
  connection dropped after `conn_write_timeout`;
- spans and metrics of real commands; a server that refuses connections;
- a TLS server that trusts only the generated CA and requires the configured
  client certificate;
- the two valkey-go defects the library works around
  (`docs/UPSTREAM-WORKAROUNDS.md`).

What a healthy server never does — going silent, refusing a subscription,
serving a corrupt entry — and everything about configuration is in the unit
tests of `libs/valkey`, against a RESP3 listener inside the test.

```sh
just mod libs/valkey/integration test        # needs Docker; skipped under -short
TESTCONTAINERS_RYUK_DISABLED=true just mod libs/valkey/integration test   # rootless podman
```
