# s3 integration suite

`libs/s3` against RustFS (`libs/testx/containers.S3`). A module of its own so
that testcontainers, the Docker client and Toxiproxy stay out of the `go.mod`
of `libs/s3`.

It owns what only a real store can say:

- every operation round-trips: put / get / head / delete, listings past one
  page and one delete batch, common prefixes;
- a streamed upload becomes one object with the bytes written, and is not
  visible before `Close`; an abort is accepted by the store;
- a bucket that is not there is `ErrBucketNotFound` — for `Ensure` and for a
  read — in the form a real store answers it;
- a store that refuses connections fails a call inside `S3_OP_TIMEOUT`, and
  the same client works again when the store is back.

Everything a healthy store never does — hanging, failing midway, each
deadline, retries, metrics and spans — is in the unit tests of `libs/s3`,
against a local HTTP server.

```sh
just mod libs/s3/integration test        # needs Docker; skipped under -short
TESTCONTAINERS_RYUK_DISABLED=true just mod libs/s3/integration test   # rootless podman
```
