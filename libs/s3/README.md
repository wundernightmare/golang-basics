# s3

Shared S3 client for golang-basics services, on
[`aws-sdk-go-v2`](https://github.com/aws/aws-sdk-go-v2): AWS S3 and
S3-compatible stores (the tests and the local stack run
[RustFS](https://github.com/rustfs/rustfs)).

What it adds to the SDK:

- **Every call has its own deadline** — `S3_OP_TIMEOUT` for small calls,
  `S3_TRANSFER_TIMEOUT` for a get or a multipart part — with the SDK's
  retries (`S3_MAX_ATTEMPTS`) inside it. A request context is not a deadline.
- **No network at construction.** `New` only builds the client, so a store
  that is down does not stop a service that can work without it;
  `Ensure` checks (and optionally creates) the bucket, `ReadyCheck` plugs
  into `libs/httpx`.
- **Streaming uploads** (`NewUpload`): an `io.Writer` that sends multipart
  parts of `S3_UPLOAD_PART_BYTES`, so memory is one part whatever the object
  size; the object appears only on a successful `Close`.
- **Bounded reads**: `Get` takes the most bytes the caller will accept.
- Metrics (`Collectors`) and a client span per call.
- Secure by default: an `http://` endpoint needs `S3_INSECURE=true`;
  `S3_CA_FILE` for a private CA.

```go
store, err := s3.New(ctx, cfg.S3, logger, s3.WithTracerProvider(tp))
defer store.Close()
srv.Health.Register("s3", store.ReadyCheck(), httpx.Optional())
srv.Metrics.Registry.MustRegister(store.Collectors()...)

err = store.Put(ctx, "raw/2026/a.bin", body, "application/octet-stream")
body, err := store.Get(ctx, "raw/2026/a.bin", 16<<20) // s3.ErrNotFound, s3.ErrTooLarge

for obj, err := range store.List(ctx, "raw/") { … }

up := store.NewUpload(ctx, "compacted/part-0.bin", "application/zstd")
defer up.Abort() // no-op after a successful Close
_, err = io.Copy(up, source)
err = up.Close()
```

## Configuration

Keys with a service prefix (`INGEST_S3_BUCKET`), or the `s3:` section of a
service's YAML; `config.go` documents each.

| Key | Default | |
| --- | --- | --- |
| `S3_BUCKET` | — | required; no default, so "set" is the on switch |
| `S3_ENDPOINT` | "" (AWS) | base URL of an S3-compatible store |
| `S3_REGION` | `us-east-1` | signing region |
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` | "" | static keys; unset = the AWS default chain (IRSA, instance role) |
| `S3_USE_PATH_STYLE` | `true` | `endpoint/bucket/key` |
| `S3_INSECURE` / `S3_CA_FILE` | `false` / "" | plaintext endpoint opt-in / private CA |
| `S3_AUTO_CREATE_BUCKET` | `false` | create the bucket in `Ensure` (local, tests) |
| `S3_OP_TIMEOUT` / `S3_TRANSFER_TIMEOUT` / `S3_DIAL_TIMEOUT` | 10s / 60s / 5s | per-call deadlines |
| `S3_MAX_ATTEMPTS` / `S3_RETRY_MAX_BACKOFF` | 3 / 2s | retries inside the deadline |
| `S3_MAX_CONNS_PER_HOST` / `S3_UPLOAD_PART_BYTES` | 32 / 16 MiB | bounded pool and part buffer |
| `S3_REQUEST_CHECKSUM` | `when_required` | `when_supported` is the SDK's own default (a CRC32 trailer on every upload), which several S3-compatible stores reject |

With static keys the SDK configuration is built by hand: nothing but this
config decides how the client behaves. Without them the SDK's default chain
is loaded, which also reads the standard `AWS_*` environment.

## Metrics

`s3_request_duration_seconds{operation}`, `s3_request_errors_total{operation}`
— see [`docs/METRICS.md`](../../docs/METRICS.md).

## Tests

`just mod libs/s3 test` runs against RustFS (`libs/testx/containers.S3`):
every operation, listings past one page, multipart uploads and aborts, and
chaos — a store that refuses connections and one that hangs must fail a call
inside `S3_OP_TIMEOUT`. Skipped under `-short`.
