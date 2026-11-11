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
