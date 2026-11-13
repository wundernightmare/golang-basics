# Go runtime settings in a container

What a service needs from `GOMAXPROCS`, `GOMEMLIMIT` and their neighbours,
and why none of them is written in a manifest here.

**The rule:** runtime settings that depend on the container's resources are
derived from the container at run time, never from the chart. A
VerticalPodAutoscaler rewrites requests and limits at admission (or in place);
an environment variable written next to them does not follow, and a stale
`GOMEMLIMIT` above the new limit is an OOM kill waiting for a traffic spike.

| Setting | Who sets it | In a manifest |
| --- | --- | --- |
| `GOMAXPROCS` | the Go runtime, from the cgroup CPU limit | never |
| memory limit (`GOMEMLIMIT`) | `libs/httpx`, from the cgroup memory limit | only as a deliberate override |
| traceback level (`GOTRACEBACK`) | `libs/httpx`, `all` by default | no |
| `GOGC` | nobody: the default | only with a profile that says so |
| `GODEBUG` | nobody | only for a diagnosis, then removed |
| FIPS 140-3 mode (`fips140`) | the build: `FIPS=1` | no — see [FIPS 140-3](#fips-140-3--a-build-mode) |

Every service built on `libs/httpx` gets this from `Server.Run`; there is
nothing to call. The keys (with the service's prefix, e.g. `INGEST_`):

| Variable | YAML | Default | |
| --- | --- | --- | --- |
| `RUNTIME_MEMORY_LIMIT_RATIO` | `runtime_memory_limit_ratio` | `0.85` | Go memory limit as a share of the container's; negative = leave the runtime alone |
| `RUNTIME_MEMORY_LIMIT_REFRESH` | `runtime_memory_limit_refresh` | `30s` | how often the container limit is re-read; negative = only at start |
| `RUNTIME_TRACEBACK` | `runtime_traceback` | `all` | `none` / `single` / `all` / `system` / `crash` |

These are ratios and constants, so they are safe in a chart or a ConfigMap:
they stay right whatever the autoscaler does to the pod.

## Memory limit — the one that matters

Without a limit the collector knows nothing about the container: it lets the
heap grow to twice the live data (`GOGC=100`) even when that crosses the
cgroup limit, and the kernel kills the process although most of that memory
was garbage. With a limit the collector runs more often as the heap
approaches it. It is a soft limit: a process whose *live* data does not fit
still dies, only later and after burning CPU on collection (the runtime caps
that at about half of the CPU time, so it degrades instead of stalling).

The runtime has no automation for this, unlike `GOMAXPROCS`. `libs/httpx`
does it:

1. reads the cgroup memory limit — v2 `memory.max`, the tightest one from the
   process's cgroup up to the root, or the v1 `memory.limit_in_bytes`;
2. sets the runtime limit to `ratio × limit` and logs both numbers;
3. re-reads every `RUNTIME_MEMORY_LIMIT_REFRESH`, because an in-place pod
   resize (VPA `InPlaceOrRecreate`) changes the limit without a restart.

No cgroup limit, or no cgroup at all (a laptop): nothing is set. A
`GOMEMLIMIT` in the environment wins and switches the automation off — the
operator said what they want — and the start-up log says so.

**Choosing the ratio.** The limit counts what the Go runtime manages. The
headroom is for what it does not, and the kernel does:

- memory allocated by C code. With the librdkafka provider the producer queue
  and fetch buffers live there: use about `0.7`, or subtract
  `kafka.producer_queue_max_kbytes` and take 0.85 of the rest;
- files in a memory-backed `emptyDir` and, under pressure, page cache and
  socket buffers charged to the container;
- the overshoot between the moment the heap crosses the limit and the moment
  the collection that was triggered finishes.

A pure-Go service (franz-go, pgx, the HTTP stack) is fine at the default.

**What it does not do.** It does not make a too-small pod work. A service
that sizes buffers in bytes (`ingest.max_in_flight_bytes`, a Kafka send
queue) needs that much memory whatever the autoscaler thinks: put the sum in
the VPA's `minAllowed.memory` (see `examples/ingest-k8s/vpa.yaml`).

To check: the start-up log line `runtime memory limit set from the container
limit`, and the gauge `go_gc_gomemlimit_bytes` next to
`container_memory_working_set_bytes` and the limit.

## GOMAXPROCS — leave it alone

Since Go 1.25 the default follows the cgroup **CPU limit**, and keeps
following it while the process runs. Measured on this toolchain, 32-core
host:

| CPU limit | `GOMAXPROCS` |
| --- | --- |
| 250m | 2 |
| 1 | 2 |
| 1500m | 2 |
| 3 | 3 |
| none | 32 (every core of the node) |

So: the limit rounded up, never below 2, never above the node.

- Setting the `GOMAXPROCS` variable, or calling `runtime.GOMAXPROCS(n)`,
  switches the automation off for good, in-place resizes included. Do neither.
  `go.uber.org/automaxprocs` is no longer needed.
- Only the limit counts; the CPU *request* is ignored. A container with a
  request and no limit runs with one thread of Go code per node core. Where
  requests equal limits (Guaranteed QoS) this does not come up.
- Prefer whole cores. Below 1 CPU the runtime still runs two threads and the
  collector's background work is charged to the same quota; when the quota for
  a 100 ms period is spent the whole process stops until the next one, and
  that shows as a latency tail no profile explains. `container_cpu_cfs_throttled_periods_total`
  is the metric to look at.

To check: `go_sched_gomaxprocs_threads`.

## GOTRACEBACK — `all`

It decides what is printed when the process dies of an unrecovered panic or a
runtime fatal error; while the process lives it costs nothing — no CPU, no
memory, nothing on the request path.

| Level | Prints | Then |
| --- | --- | --- |
| `single` (the Go default) | the goroutine that panicked | exit 2 |
| `all` (the default here) | every goroutine created by user code | exit 2 |
| `system` | also the runtime's own goroutines | exit 2 |
| `crash` | as `system` | aborts with SIGABRT: a core dump if the node allows one |

Why `all`: a crash is rarely explained by the one goroutine that tripped —
the other side of the deadlock, the worker that held the lock, the request
that was in flight are in the other stacks, and the process is gone before
anyone can ask for a profile.

What it costs at the moment of the crash, to be honest about it:

- log volume: a few hundred bytes per goroutine. Ten thousand goroutines are
  a few megabytes on stderr, once, as plain text rather than JSON lines;
- stack frames show argument words as hex numbers — pointers and integers,
  not the strings they point to. Treat a crash log like any other log;
- `crash` is a different matter: a core file contains the heap, secrets
  included, and needs somewhere to go. Not a default.

`libs/httpx` sets it with `debug.SetTraceback` when the server starts. A
`GOTRACEBACK` in the environment can raise the level but the code cannot
lower it below that. A panic before `Server.Run` — while the configuration
loads — still prints at the Go default; set `ENV GOTRACEBACK=all` in the
image if that matters.

## The rest

- **`GOGC`** — leave at 100. It trades memory for collector CPU, and with a
  memory limit in place the dangerous end of that trade is covered. Change it
  only with a CPU profile that shows the collector at the top. `GOGC=off`
  together with a memory limit (collect only at the limit) is a batch-job
  setting: for a server it turns every approach to the limit into a
  collection storm.
- **`GODEBUG`** — nothing is needed for normal operation. `gctrace=1` or
  `schedtrace=1000` are diagnosis tools: set on one pod, read, remove.
  `containermaxprocs=0` / `updatemaxprocs=0` switch the `GOMAXPROCS`
  automation off; there is no reason to.
- **`GOMEMLIMIT` by hand** — legitimate as an override on one deployment
  while investigating. In a chart shared by many services it is the stale
  constant this document exists to remove.

## FIPS 140-3 — a build mode

Opt-in, off by default. `FIPS=1` builds a service against Go's own FIPS 140-3
module — the Go Cryptographic Module, pure Go: no cgo, no BoringCrypto, the
same static binary on the same `distroless/static` image.

```sh
FIPS=1 scripts/build-service.sh ping                        # no Kafka: nothing else to choose
FIPS=1 KAFKA_PROVIDER=franz scripts/build-service.sh tasks  # Kafka: the pure-Go client only
FIPS=1 KAFKA_PROVIDER=franz just docker-build tasks         # the image; docker/stack.yml and CI read the same names
```

What the knob does: the build runs with `GOFIPS140=$GOFIPS140_VERSION`. The
toolchain then compiles `crypto/...` against that frozen snapshot of the
module instead of the tree's current code, and makes `fips140=on` the
binary's default `GODEBUG` — nothing to set in a manifest.

**The version is pinned.** `GOFIPS140_VERSION` defaults to `v1.0.0`, the
snapshot the pinned toolchain lists as certified
(`$(go env GOROOT)/lib/fips140/certified.txt`); only snapshots shipped in that
directory can be built. The default is written in `scripts/build-service.sh`
and `docker/service.Dockerfile` (the two build lines); override it with the
variable, never with `GOFIPS140` — the script sets that itself from `FIPS`.
Not `latest`: that is the unvalidated code of the current tree. A toolchain
bump that drops the snapshot fails the build, it does not fall back.
`go version -m <binary>` shows what was linked (`GOFIPS140=v1.0.0-…`,
`DefaultGODEBUG=fips140=on`).

**What it guarantees.** In a process running in this mode:

- the cryptography of the standard library — TLS, hashes, HMAC, signatures,
  key agreement, AES, random numbers — is served by the validated module,
  which checks its own integrity and runs its self-tests at start;
- `crypto/tls` negotiates only FIPS-approved protocol versions, cipher suites
  and signature algorithms, on every connection that uses it: the HTTP
  listeners and clients, pgx, valkey-go, franz-go, the S3 client, OTLP export;
- `crypto/rand` draws from the module's DRBG.

**How to see it.** `GET /version` on the admin port carries `"fips140": true`
and the `<service> starting` log line `fips140=true`. Both are
`crypto/fips140.Enabled()` asked of the running process, not a flag stamped at
build time: a FIPS binary started with `GODEBUG=fips140=off` honestly reports
`false`, and an ordinary binary started with `GODEBUG=fips140=on` reports
`true` (that runs the tree's own, unpinned module — a way to try the mode, not
a release). There is no `build_info` label for it: metric labels are a
contract with nodejs-basics (`docs/METRICS.md`).

**The limits.**

- **Only the Go module.** Anything linked through cgo is outside it. The
  default Kafka provider is librdkafka with its bundled OpenSSL, so
  `scripts/build-service.sh` and the Dockerfile refuse `FIPS=1` for a service
  that would link it (`FIPS=1 needs KAFKA_PROVIDER=franz`); build with
  `KAFKA_PROVIDER=franz` (`CGO_ENABLED=0`). The same goes for whatever
  terminates TLS in front of the pod, and for Postgres, Valkey and the broker
  themselves.
- **`fips140=on` restricts TLS and nothing else.** Code that calls a
  non-approved algorithm directly (MD5, SHA-1 signatures, ChaCha20-Poly1305,
  a third-party cipher) still works. `GODEBUG=fips140=only` is the strict
  variant: such a call returns an error or panics. It is a tool for finding
  those calls on a stand; the Go documentation describes it as best-effort,
  so it is not the default here. Set it on one deployment, read the logs.
- **A mode, not a certificate.** Compliance also needs an operating
  environment the module's Security Policy lists, and is judged for the
  system as a whole. This build gives the technical precondition and a way to
  prove it is on.
- Cryptography written outside the standard library (a dependency's own
  implementation of an algorithm) is not covered by the module either.

The e2e suite builds ping both ways and checks what the process reports
(`e2e/fips_test.go`), and that the rdkafka combination is refused.

## Kubernetes checklist

- requests == limits for CPU and memory: Guaranteed QoS, and the CPU limit is
  what sizes `GOMAXPROCS`.
- VPA with `controlledValues: RequestsAndLimits` keeps them equal (it scales
  the limit by the request's ratio).
- `minAllowed.cpu` ≥ 1; `minAllowed.memory` ≥ the service's own byte-sized
  buffers plus the runtime.
- No `GOMAXPROCS`, `GOMEMLIMIT` or `GOGC` in the pod spec.
- Not a CPU- or memory-based HPA on the same workload as the VPA.

`examples/ingest-k8s` is this checklist applied.
