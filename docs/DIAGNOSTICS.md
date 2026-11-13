# Post-mortem diagnostics

What is left of a pod after it died, and where to find it.

`/debug/pprof` on the admin port answers while the process lives and somebody
is looking. A pod that crashed or was OOM-killed at night gives neither, so
`libs/httpx` takes the evidence at the moment it exists and ships it to
object storage. It is built into `Server.Run`; a service turns it on by
installing a sink:

```go
store, _ := s3.New(ctx, cfg.S3, logger)   // libs/s3
srv.SetDumpSink(store)                    // any Put(ctx, key, body, contentType) error
```

and, for crash reports, by setting `DIAG_DUMP_DIR` to a writable directory
(an `emptyDir`). With neither a sink nor a directory nothing runs.

| Event | What is captured | When it is shipped |
| --- | --- | --- |
| crash: an unrecovered panic or a runtime fatal error | the runtime's report with every goroutine (`RUNTIME_TRACEBACK=all`), as JSON | by the next start of the container |
| memory pressure: working set ≥ `DIAG_MEMORY_THRESHOLD` of the limit | heap profile, goroutine profile, a JSON summary of where the memory is | at once |
| an anomaly, with `DIAG_FLIGHT_RECORDER=true`: a request slower than `DIAG_FLIGHT_SLOW_REQUEST`, the memory-pressure capture, or `srv.CaptureFlightTrace(reason)` | the last `DIAG_FLIGHT_MIN_AGE` of the Go execution trace (`…-flight.trace`): what every goroutine was doing — scheduling, blocking, GC, syscalls — in the seconds before | at once, at most one per `DIAG_FLIGHT_COOLDOWN` |
| OOM kill, SIGKILL, node loss | nothing at that moment — there is no hook | the memory-pressure capture is the evidence, taken before |

## Where it goes

```
<diag_key_prefix>/<service>/dt=2026-11-11/<pod>/20261111T050938.994Z-memory.json
                                                 20261111T050938.994Z-heap.pb.gz
                                                 20261111T050938.994Z-goroutine.pb.gz
                                                 20261111T050937.095Z-crash.json
```

One listing per service and day finds everything, the pod name keeps
replicas apart, the timestamp groups the artifacts of one event. Give the
prefix a lifecycle rule (30 days is plenty).

```sh
aws s3 ls s3://$BUCKET/diagnostics/ingest/dt=2026-11-11/ --recursive
aws s3 cp s3://$BUCKET/diagnostics/ingest/dt=…/ingest-7d9f-xkp2q/…-heap.pb.gz .
go tool pprof -top -sample_index=inuse_space heap.pb.gz      # no binary needed: Go profiles carry their symbols
go tool pprof -http=:0 goroutine.pb.gz
```

## Reading a memory capture

Start with `memory.json`; it says which of the other two files matters.

| Field | Meaning |
| --- | --- |
| `container_usage_bytes` / `container_limit_bytes` | the cgroup's working set (usage minus inactive file cache — the kubelet's figure) and limit |
| `go_memory_limit_bytes` | the runtime's soft limit ([RUNTIME.md](RUNTIME.md)) |
| `go_heap_inuse_bytes` | large → the heap profile explains it |
| `go_stack_inuse_bytes`, `goroutines` | large → a goroutine leak; the goroutine profile shows where they wait |
| `outside_go_runtime_bytes` | container usage minus what the runtime holds. Large → the memory is not Go's: C allocations (librdkafka), a memory-backed volume, page cache. No Go profile will show it |
| `go_gc_cpu_fraction` | near 0.5 → the collector is already fighting the limit |

The heap profile is sampled (one allocation per 512 KiB on average) and
reflects the last completed collection — near the limit collections are
frequent, so it is current.

## Reading a flight trace

```sh
go tool trace 20261114T101500.000Z-flight.trace
```

A profile says where the CPU went on average. The execution trace says what
happened in one window of time: the goroutine of the slow request, what it
was blocked on and for how long, whether a GC cycle or a scheduler delay sat
in the middle of it. The recorder keeps that window in memory all the time
(`runtime/trace.FlightRecorder`) and writes it out only on an anomaly, so
the evidence exists for the request that was slow, not for one somebody tried
to reproduce later.

It is off by default: the runtime traces continuously while it is on — a per
cent or two of CPU and up to `DIAG_FLIGHT_MAX_BYTES` of memory. The runtime
allows one flight recorder per process.

## Reading a crash report

```json
{
  "service": "ingest", "version": "1.4.2", "revision": "2e9ad90…",
  "instance": "ingest-7d9f-xkp2q",
  "crashed_at": "2026-11-11T05:09:37.095Z", "found_at": "2026-11-11T05:09:41.310Z",
  "reason": "panic: runtime error: invalid memory address or nil pointer dereference",
  "truncated": false,
  "output": "panic: …\n\ngoroutine 184 [running]:\n…"
}
```

`output` is exactly what the runtime printed to stderr, so the same text is
in the container log; the object is for when the log is gone or was sampled
away. `version` / `revision` are those of the binary that *found* the file —
after a bad rollout, the one that crashed. Go tracebacks are symbolized:
there is no debug-symbol lookup step.

How it works: `debug.SetCrashOutput` makes the runtime write its report to a
file in `DIAG_DUMP_DIR` as well as to stderr. A crashed container restarts
inside the same pod, the `emptyDir` is still there, and the new process
ships the file and deletes it. No sidecar. A file that is empty belongs to a
run that was killed rather than crashed, and is removed.

## Configuration

Keys of `httpx.Config` (with the service's prefix; YAML name = lower case):

| Variable | Default | |
| --- | --- | --- |
| `DIAG_DUMP_DIR` | *(none)* | directory for crash capture and for artifacts not uploaded yet; empty = no crash capture |
| `DIAG_KEY_PREFIX` | `diagnostics` | first segment of the object keys |
| `DIAG_MEMORY_THRESHOLD` | `0.9` | share of the container's memory limit that triggers a capture; negative = never |
| `DIAG_MEMORY_COOLDOWN` | `10m` | shortest time between two captures |
| `DIAG_FLIGHT_RECORDER` | `false` | keep the last seconds of the execution trace in memory |
| `DIAG_FLIGHT_MIN_AGE` | `10s` | how far back the window reaches |
| `DIAG_FLIGHT_MAX_BYTES` | `8388608` | upper bound of the window in memory |
| `DIAG_FLIGHT_SLOW_REQUEST` | `0s` | a request this slow triggers a snapshot; 0 = requests never do |
| `DIAG_FLIGHT_COOLDOWN` | `5m` | shortest time between two snapshots |
| `DIAG_CHECK_INTERVAL` | `5s` | how often container memory is read |
| `DIAG_UPLOAD_TIMEOUT` | `15s` | one upload |
| `DIAG_SPOOL_RETRY` | `1m` | how often files waiting in the directory are re-sent |
| `DIAG_SPOOL_MAX_FILES` | `20` | files kept in the directory (newest win) |
| `DIAG_CRASH_MAX_BYTES` | `8388608` | longest crash report shipped |

The threshold sits above `RUNTIME_MEMORY_LIMIT_RATIO` (0.85) on purpose: up
to that point the collector holds the line by itself; a working set beyond
it means the collector is losing, or the memory is not the collector's.

Metric: `diag_dumps_total{kind, result}` — `kind` crash / memory, `result`
uploaded / spooled / failed. Any `kind="crash"` is worth an alert; so is
`result="failed"`.

## Kubernetes

```yaml
volumeMounts:
  - { name: dumps, mountPath: /dumps }
volumes:
  - name: dumps
    emptyDir: { sizeLimit: 64Mi }
```

The credentials need `PutObject` on `<bucket>/diagnostics/*`, nothing else.
`examples/ingest-k8s` has it wired.

## What it does not do

- **A sudden allocation.** The watcher looks every `DIAG_CHECK_INTERVAL`; a
  request that allocates the whole limit in one second kills the pod between
  two looks. Gradual growth — a leak, a queue that stopped draining — is what
  it catches.
- **A crash before `Server.Run`** (configuration loading) is on stderr only.
- **A crash followed by the pod's deletion** (eviction, scale-down in the
  same minute) loses the report with the `emptyDir`; stderr still has it.
- **Full heap dumps.** `debug.WriteHeapDump` is the size of the heap, stops
  the process while it writes, contains every secret and every customer
  event in memory, and has no maintained reader. Not taken, on purpose.
- **Continuous profiling.** This is evidence for incidents, not a profiler;
  CPU regressions between releases are a job for Pyroscope or Parca reading
  the same `/debug/pprof` endpoints.
- **When the sink is down** artifacts wait in the directory and are re-sent;
  without a directory they are counted as `failed` and lost.
