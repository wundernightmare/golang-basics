# Upstream workarounds

Code in this repo that exists only because a dependency misbehaves. Each one
is marked in the source with
`UPSTREAM-WORKAROUND(<dependency> <version>): <what> (docs/UPSTREAM-WORKAROUNDS.md)`,
so

```sh
grep -rn "UPSTREAM-WORKAROUND" --include='*.go' .
```

lists them all, and each has a row here saying what would let it go.

Rules:

- A workaround names the dependency **and the version it was seen in**, in
  the comment next to the code.
- Where a test can assert the defect itself, it does (`TestUpstream…`): it
  passes while the defect is there and **fails when an upgrade fixes it** —
  the bump PR is then where the workaround is removed. Where no such test can
  be written, the row says so, and the dependency's changelog has to be read
  on every bump.
- A new workaround adds the marker, a row here, and the test if one is
  possible. Removing one removes all three.
- This is not the place for a dependency's documented behaviour we merely
  configure around (a timeout, a pool size): only defects.

| Dependency (seen in) | Defect | Workaround | Tells us it is fixed | Reported upstream |
|---|---|---|---|---|
| `valkey-io/valkey-go` (≤ 1.0.79) | The dedicated-connection pool loses its limit: a caller whose context ends while it waits for a connection is handed an uncounted placeholder, and returning it lowers the pool's count — one extra connection allowed per such caller, for the life of the client. | `libs/valkey/bounded.go`: a semaphore of `pool_size` in front of everything that takes a dedicated connection; `DoStream` / `DoMultiStream` are refused (`ErrStreamUnsupported`) because their connection cannot be accounted for. | `TestUpstreamPoolLimitLeaksOnACancelledWaiter` (`libs/valkey/integration/pool_test.go`) fails. | no |
| `valkey-io/valkey-go` (≤ 1.0.79) | Data race: after a connection error a read-only command is retried with the same command object and recycled on success, while the writer of the failed connection may still read it. | `libs/valkey/config.go`: `DisableRetry`; `libs/valkey/pubsub.go`: `Subscribe` resubscribes itself, a new command per attempt. | **Nothing.** `TestReadsUnderConnectionFaultsDoNotRace` guards our client and cannot see a fix in the library (a test cannot expect the race detector to fire). On a bump: read the changelog for the retry path of `singleClient.Do`, or re-enable the retry and run that test under `-race`. | no |
| `ozontech/testo-allure` (1.5.0) | An attachment without a media type is written to `os.TempDir` and renamed into the results directory; when those are different filesystems the rename fails and the attachment is dropped without an error. | `e2e/report_test.go`, `attachOutput`: every attachment names its media type. | `TestUpstreamAttachmentWithoutATypeIsLostAcrossFilesystems` (`e2e/report_test.go`) fails. It skips where `/dev/shm` and the checkout are one filesystem. | no |
| `go.yaml.in/yaml/v3` (3.0.6) | A decoder's `KnownFields(true)` does not reach `yaml.Node.Decode`: a type that decodes its own node from `UnmarshalYAML` accepts unknown keys in silence. | `libs/resilient-http-client/config.go` and `libs/grpcx/config.go`, `TargetConfig.UnmarshalYAML`: the node is re-encoded and decoded with a strict decoder. | `TestUpstreamYAMLNodeDecodeIgnoresKnownFields` (`libs/resilient-http-client/config_test.go`) fails. | no |
| `rustfs/rustfs` (1.0.5) | Two clients creating the same bucket at the same moment leave the store answering `NoSuchBucket` for a bucket both were told exists. | `libs/s3/s3.go`: `ensureLocks` serializes `Client.Ensure` per endpoint + bucket within the process (the race across processes stays). | **Nothing.** No test asserts the race; on an image bump, drop the lock and run the `libs/s3/integration` tests repeatedly in parallel. | no |
| `rustfs/rustfs` (1.0.5) | `/health` answers before the S3 API is ready: for the first seconds a signed call can still answer 503 (seen on CI runners). | The suites that create a bucket first retry it for up to a minute: `libs/s3/integration/s3_test.go` (`sharedClient`), `services/ingest/internal/integration` (`suite_test.go`, `ingest_test.go`), `services/ingest/internal/compact/compact_test.go`. | **Nothing** automated; on an image bump, remove the waits and run the suites on a cold container. | no |
