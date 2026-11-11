# ingest on Kubernetes

Manifests for [`services/ingest`](../../services/ingest): the stateless
ingest pods and the single compactor, on a cluster where pods get **no
persistent volume**. The service keeps unconfirmed events in memory only, by
design (its README has the argument); the one `emptyDir` is for crash
reports.

```sh
kubectl create secret generic ingest-s3 \
  --from-literal=access-key-id=… --from-literal=secret-access-key=…
kubectl create secret generic ingest-admin --from-literal=token="$(openssl rand -hex 24)"
kubectl apply -f ingest.yaml
kubectl apply -f vpa.yaml   # optional: needs the VerticalPodAutoscaler CRD
```

Validate a changed ConfigMap before rolling it out — the same binary, the
same validation: `ingest --check-config` (exit 78 on a bad config).

## The numbers that have to agree

| Setting | Value here | Must be |
| --- | --- | --- |
| `terminationGracePeriodSeconds` | 45 | > `shutdown_delay` (5s) + `shutdown_timeout` (10s) + `ingest.drain_timeout` (15s), with slack |
| memory limit | 768Mi | ≈ 3 × `ingest.max_in_flight_bytes` (128 MiB) + `kafka.producer_queue_max_kbytes` (64 MiB) + the runtime |
| requests | = limits | Guaranteed QoS; the CPU limit is also what the Go runtime sizes `GOMAXPROCS` from |
| `GOMEMLIMIT`, `GOMAXPROCS` | not set | the binary reads the container's cgroup: 85% of the memory limit (`runtime_memory_limit_ratio`), CPU limit rounded up — see [docs/RUNTIME.md](../../docs/RUNTIME.md) |
| VPA `minAllowed.memory` ([vpa.yaml](vpa.yaml)) | 768Mi | ≥ the memory row above: the buffers are sized in bytes in the ConfigMap and do not shrink with the pod |
| client timeout | ≥ 10s | > `ingest.kafka_ack_grace` + `archive.urgent_linger` + `s3.op_timeout` (7s) |
| `request_timeout` | 15s | the same sum; the service refuses to start otherwise |

## What Kubernetes can do to a pod, and what it costs here

Nothing acknowledged is lost in any row: an answer is sent only after a sink
confirmed.

| Cause | Signal | Effect |
| --- | --- | --- |
| rollout, scale-down, `kubectl delete pod`, drain, preemption, failed liveness | SIGTERM, then the grace period | graceful: requests in flight are answered, the last archive batch is flushed, Kafka sends finish |
| OOM kill, node loss, hard eviction (memory / disk / PID pressure), `--force --grace-period=0`, grace period exceeded | SIGKILL or nothing | requests in flight see a reset connection and are retried by the client; second copies not yet written are lost (see the service README's table) |

Readiness never takes every pod out at once for one sink: `kafka` and `s3`
are optional checks; only `sinks` (both circuits open) is critical. Liveness
is the process alone (`/livez`) — a dependency outage must not restart pods.

## Bucket

Versioning off (or expire noncurrent versions), a lifecycle rule aborting
incomplete multipart uploads after a day, and credentials limited to the
bucket: put / get / list / delete, plus the multipart calls.

Crash reports and memory-pressure profiles of both Deployments land in the
same bucket under `diagnostics/` ([docs/DIAGNOSTICS.md](../../docs/DIAGNOSTICS.md));
give that prefix an expiry rule. `/dumps` is an `emptyDir` that holds a crash
report across the container restart — the only thing these pods write to
disk, and never an event.

## The compactor

One replica, `strategy: Recreate` — two compactors must never overlap (the
manifest is updated without a lock). As a CronJob instead: the same
container with `INGEST_COMPACT_RUN_ONCE=true`, `concurrencyPolicy: Forbid`,
and no Prometheus scrape (the process is gone before one).
