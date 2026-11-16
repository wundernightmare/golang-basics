# tiercache / integration

`tiercache` against a real Valkey through [`libs/valkey`](../../valkey),
healthy and failing. A Go module of its own, inside `tiercache`'s folder:
`libs/valkey` with its client, testcontainers, the Docker client and
Toxiproxy are this module's dependencies and never appear in `tiercache`'s
`go.mod`.

```sh
just mod libs/tiercache/integration test   # from the repo root; needs Docker
go test -race ./...                        # here
```

Skipped under `-short` and without Docker; **failed** when `CI` is set and a
container cannot start. On rootless podman set
`TESTCONTAINERS_RYUK_DISABLED=true`.

| File | What it owns |
| --- | --- |
| `integration_test.go` | what the in-memory L2 of the unit tests cannot prove: that the commands of `libs/valkey` mean what that fake says (`PX`, `SET NX`, `SET IFEQ`, tombstones, Pub/Sub), and that values survive the wire from 0 bytes to 4 MiB |
| `chaos_test.go` | Valkey down, frozen, slow, black-holed, resetting, cutting replies mid-value (Toxiproxy, `CLIENT PAUSE`): every read answers correctly within a bound, and L2 is picked up again; the soak — two processes reading and invalidating under flapping faults |
| `helpers_test.go` | the source of truth and the harness, on the [`credential`](../credential) codec and the `offheap` store (one reader of the invalidation test is on `onheap`) |

The read rules themselves are the unit layer's, in `tiercache` on virtual
time; nothing here repeats them.
