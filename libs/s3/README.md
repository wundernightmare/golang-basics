# s3

Shared S3 client for golang-basics services, on
[`aws-sdk-go-v2`](https://github.com/aws/aws-sdk-go-v2): AWS S3 and
S3-compatible stores (the tests and the local stack run
[RustFS](https://github.com/rustfs/rustfs)).

What it adds to the SDK:

- **Every call has its own deadline** — `S3_TRANSFER_TIMEOUT` for a get, a
  multipart part and the completion of a multipart upload, `S3_OP_TIMEOUT`
  for every other call (an upload that never filled a part is one put, of up
  to `S3_UPLOAD_PART_BYTES`) — with the SDK's retries (`S3_MAX_ATTEMPTS`)
  inside it. A request context is not a deadline. The deadline is of the
  call, not of one attempt: a store that fails fast (5xx, a refused
  connection) is retried, one that takes the request and stays silent uses
  the whole deadline on the first attempt.
- **No network at construction.** `New` only builds the client, so a store
  that is down does not stop a service that can work without it;
  `Ensure` checks (and optionally creates) the bucket, `ReadyCheck` plugs
  into `libs/httpx`.
- **Streaming uploads** (`NewUpload`): an `io.Writer` that sends multipart
  parts of `S3_UPLOAD_PART_BYTES`, so memory is one part whatever the object
  size; the object appears only on a successful `Close`, and after a failed
  `Write` `Close` publishes nothing — both return that error, `Abort` drops
  the parts.
- **Bounded reads**: `Get` takes the most bytes the caller will accept.
- **A missing bucket is not a missing key**: `ErrNotFound` is the answer of
  `Get`/`Head` for a key that is not there; "no such bucket" is the failure
  `ErrBucketNotFound`, from any call (`Head` excepted: a HEAD's 404 has no
  body to say which of the two is missing).
- Metrics (`Collectors`) and a client span per call.
- Secure by default: an `http://` endpoint needs `S3_INSECURE=true`;
  `S3_CA_FILE` for a private CA.

```go
store, err := s3.New(ctx, cfg.S3, logger, s3.WithTracerProvider(tp))
defer store.Close()
srv.Health.Register("s3", store.ReadyCheck(), httpx.Optional())
srv.Metrics.Registry.MustRegister(store.Collectors()...)

err = store.Put(ctx, "raw/2026/a.bin", body, "application/octet-stream")
body, err := store.Get(ctx, "raw/2026/a.bin", 16<<20) // s3.ErrNotFound, s3.ErrTooLarge, s3.ErrBucketNotFound

for obj, err := range store.List(ctx, "raw/") { … }

up := store.NewUpload(ctx, "compacted/part-0.bin", "application/zstd")
defer up.Abort() // no-op after a successful Close
_, err = io.Copy(up, source)
err = up.Close()
```

## Configuration

Keys with a service prefix (`INGEST_S3_BUCKET`), or the `s3:` section of a
service's YAML; `config.go` documents each. `Config.Validate` refuses
everything `New` would refuse for the config alone — the CA file is parsed,
not just opened — and `LoadConfig` runs it when a bucket is set; its errors
match `errors.Is(err, s3.ErrConfig)`.

| Key | Default | |
| --- | --- | --- |
| `S3_BUCKET` | — | required; no default, so "set" is the on switch |
| `S3_ENDPOINT` | "" (AWS) | base URL of an S3-compatible store |
| `S3_REGION` | `us-east-1` | signing region |
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` | "" | static keys; unset = the AWS default chain (IRSA, instance role) |
| `S3_USE_PATH_STYLE` | `true` | `endpoint/bucket/key`; the default comes from the env tag, so a `s3.Config` built by hand has it `false` until set |
| `S3_INSECURE` / `S3_CA_FILE` | `false` / "" | plaintext endpoint opt-in / private CA |
| `S3_AUTO_CREATE_BUCKET` | `false` | create the bucket in `Ensure` (local, tests) |
| `S3_OP_TIMEOUT` / `S3_TRANSFER_TIMEOUT` / `S3_DIAL_TIMEOUT` | 10s / 60s / 5s | per-call deadlines |
| `S3_MAX_ATTEMPTS` / `S3_RETRY_MAX_BACKOFF` | 3 / 2s | retries inside the call's deadline; there is no per-attempt timeout |
| `S3_MAX_CONNS_PER_HOST` / `S3_UPLOAD_PART_BYTES` | 32 / 16 MiB | bounded pool and part buffer |
| `S3_REQUEST_CHECKSUM` | `when_required` | `when_supported` is the SDK's own default (a CRC32 trailer on every upload), which several S3-compatible stores reject |

With static keys the SDK configuration is built by hand: nothing but this
config decides how the client behaves. Without them the SDK's default chain
is loaded, which also reads the standard `AWS_*` environment.

## Metrics

`s3_request_duration_seconds{operation}`, `s3_request_errors_total{operation}`
— see [`docs/METRICS.md`](../../docs/METRICS.md). What answers the caller's
own question is not counted as an error: a missing key on get/head
(`ErrNotFound`) and an object over the caller's limit (`ErrTooLarge`).

The metrics carry no label that names the client, so two clients (two
buckets) cannot be registered on one registry as they are. Give each its own
const label where they are registered — each, not only the second: one metric
name carries one set of label names, and a bare client beside a wrapped one
is refused just the same:

```go
archive := prometheus.WrapRegistererWith(prometheus.Labels{"store": "archive"}, srv.Metrics.Registry)
archive.MustRegister(archiveStore.Collectors()...)

dumps := prometheus.WrapRegistererWith(prometheus.Labels{"store": "dumps"}, srv.Metrics.Registry)
dumps.MustRegister(dumpStore.Collectors()...)
```

## Tests

Two layers, each behaviour in the lower one that can observe it:

- `just mod libs/s3 test` — no Docker. A local HTTP server plays the store
  that a healthy one never is: it hangs (every deadline: `S3_OP_TIMEOUT`,
  `S3_TRANSFER_TIMEOUT`, `S3_DIAL_TIMEOUT`, the readiness probe, the upload's
  context), fails (retries, error wrapping, a spoiled upload, aborts), or
  answers "not found" in each of its forms; metrics, spans and the CA file
  are checked there too.
- `just mod libs/s3/integration test` — against RustFS
  (`libs/testx/containers.S3`): every operation, listings past one page,
  multipart uploads and aborts on a real store, a missing bucket, and a store
  that refuses connections and comes back. A module of its own, so
  testcontainers stays out of this one's `go.mod`.
