# otelx

Shared distributed-tracing setup for golang-basics services: one call to
`otelx.Init` installs the OpenTelemetry tracer provider (OTLP over HTTP, TLS
by default) and the W3C trace-context + baggage propagators. Everything else
hangs off that global provider:

- `libs/httpx` starts a server span per request (`GET /tasks/{id}`,
  `SpanKindServer`, continuing an inbound `traceparent`), stamps the trace
  and span ids on every log line written with the request context
  (`trace_id` for a trace that is exported, `trace_unsampled_id` for one that
  is not — see [below](#retroactive-sampling-the-trace-of-a-failure-the-ratio-dropped)),
  and puts the `trace_id` of sampled requests on the Prometheus metrics as an
  exemplar;
- the data libs' instrumentation (otelpgx, kotel, valkeyotel) adds client
  spans for Postgres, Kafka and Valkey.

It lives outside `libs/httpx` on purpose: ping and heartbeat carry no
exporter, while the services that span process boundaries
(`services/tasks` → Kafka → `services/consumer`) opt in by importing it.
Tracing is opt-in at runtime too: with `OTEL_ENABLED=false` (the default)
`Init` installs only the propagators, so context is still forwarded to the
next hop — and logs still carry the caller's `trace_id` — without exporting
anything.

## Usage

```go
otelCfg, err := otelx.LoadConfig("TASKS_")
if err != nil { … }
otelCfg.ServiceName, otelCfg.Version = "tasks", httpx.Version
tel, err := otelx.Init(ctx, otelCfg, logger)
if err != nil { … }
defer tel.Stop() // exports what is buffered, within five seconds

srv, err := httpx.NewServer(cfg, logger,
    httpx.WithTracerProvider(tel.TracerProvider), httpx.WithPropagator(tel.Propagator))
```

(`example_test.go` is this snippet, compiled.)

Call `Init` before building the clients whose instrumentation should record
spans: the instrumentation libraries capture the global provider when they
are constructed.

**Shutdown.** `defer tel.Stop()` is the one recipe. It is
`tel.Shutdown(context.Background())` with the error dropped; `Shutdown`
itself

- lasts until the deadline of the context it is given, or five seconds when
  the context has none — not "batches × export timeout";
- is not cancelled with that context: the context of a service is cancelled
  by the signal that stops it, and a provider shut down under a cancelled
  context drops its spans for good;
- returns an error, and logs `tracing shutdown: spans may have been lost` at
  warn, when the time ran out or the collector did not take the last export —
  past the throttle of the export errors, which has no later line to count
  that failure in;
- unbinds the logger from the retroactive buffer: a warn line written after
  it carries `trace_unsampled_id`.

`tel.Stop()` and `tel.Shutdown(ctx)` do nothing on a nil or zero `Telemetry`.

`Config.Validate()` reports what `Init` refuses — a negative queue, batch,
timeout or trigger rate, a sampling ratio outside `[0, 1]`, an endpoint that
is a URL or has no port, half of an mTLS pair, a certificate file that cannot
be read (with export on over TLS) — by the name of the environment key.
`LoadConfig` and `Init` call it; a service that loads the config as a section
of its own (`httpx.LoadYAML`) calls it from its `Validate`, which is what
`--check-config` runs. A key this package no longer reads is named by
`Config.RetiredKeys()`; `LoadConfig` and `httpx.LoadYAML` refuse an
environment that still sets one. Every error about the configuration matches
`otelx.ErrConfig` with `errors.Is`.

A `Config` built in code needs only what differs from the defaults: a zero
field is "not set" — except `SamplerRatio`, whose zero samples no root. Its
default of 1.0 is applied where a config is loaded (`LoadConfig`,
`httpx.LoadYAML`), so a `Config` built by hand sets `SamplerRatio: 1` to
sample.

## Signals: traces here, metrics in Prometheus

Only traces go through OpenTelemetry. Metrics are Prometheus-native and
scraped from each service's admin listener (`libs/httpx.Metrics` plus the
libs' collectors); `otelx` installs no meter provider, so the OTel meters of
the instrumentation libraries stay no-ops. Logs are plain JSON on stdout with
the trace ids in them (see `httpx.NewLogger`).

## Export

- **OTLP/HTTP** (`otlptracehttp`) rather than gRPC: the same wire protocol,
  one fewer dependency tree, and it passes through any HTTP proxy. The
  endpoint is a `host:port` (default `localhost:4318`) — not a URL, which is
  what the standard variable of the same name holds: the scheme comes from
  `…_INSECURE`, the path is `/v1/traces`, and a value with either is refused
  at start.
- **TLS by default**, with the system roots; `OTEL_EXPORTER_OTLP_CERTIFICATE`
  replaces them with a CA bundle, `…_CLIENT_CERTIFICATE` + `…_CLIENT_KEY` enable mTLS
  (both or neither). `OTEL_EXPORTER_OTLP_INSECURE=true` is for a local
  collector only.
- **Headers** (`k=v,k=v`) carry auth tokens or tenant ids; they are tagged
  secret, so `/admin/config` redacts them, and never logged.
- **Batching**: the batch span processor flushes every
  `OTEL_BSP_SCHEDULE_DELAY`, buffers up to `OTEL_BSP_MAX_QUEUE_SIZE` spans
  (dropping beyond that rather than blocking the service) and exports
  `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` per request.
- **Export failures** — a collector that is down, a rejected batch — are
  logged at warn at most once every 30s, with a count of what was suppressed
  in between. They never affect the service.
- **The environment of the process** does not configure the exporter:
  `Config` does. The exporter library reads the standard, unprefixed
  `OTEL_EXPORTER_OTLP_*` variables (and their `…_TRACES_*` forms) —
  `ENDPOINT`, `INSECURE`, `CERTIFICATE`, `CLIENT_CERTIFICATE`, `CLIENT_KEY`,
  `HEADERS`, `COMPRESSION`, `PROTOCOL`, `TIMEOUT` — and `otelx` overrides
  every one of them, as it does `OTEL_BSP_*` and `OTEL_TRACES_SAMPLER*` of
  the SDK: a variable an operator or an injector sets for another SDK in the
  pod changes nothing here. What still acts without a key of `Config`:
  `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME`
  ([Resource](#resource)); the SDK's span limits (`OTEL_SPAN_*_LIMIT`,
  `OTEL_ATTRIBUTE_*_LIMIT`, `OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT`,
  `OTEL_LINK_ATTRIBUTE_COUNT_LIMIT`); the experimental `OTEL_GO_X_*`
  switches; and `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY`, which the
  exporter's HTTP client follows.

## Sampling: head in the service, tail in the collector

There are two places to decide which traces are kept, and they are not
interchangeable.

**Head sampling** is decided here, in the service, when a trace *starts*
(`sampler.go`). One rule for a pipeline: the decision is made once, at the
root of the trace, and everyone downstream follows it.

- A **root** span is sampled with probability `OTEL_TRACES_SAMPLER_RATIO`
  (`0` samples no root; the default, 1.0, is applied when the config is loaded).
- A span **with a parent** takes the parent's sampled flag, whatever this
  service's ratio: a request sampled at the edge stays sampled in every
  service behind it, and an unsampled one is never sampled again downstream.
- On an **internet-facing edge** set `OTEL_TRACES_REDRAW_REMOTE_PARENT=true`.
  A `traceparent` on a request from outside is the client's: its sampled
  flag, and its trace id, whose low bits a ratio sampler keys on — following
  either lets any client have all of its requests traced through the whole
  pipeline. For a remote parent the edge therefore draws the decision afresh
  at its ratio, from a random id, and drops the client's `tracestate`. The
  trace id itself is kept, so a caller that sends `traceparent` still finds
  its trace. Parents inside the process are followed as everywhere.
- A request with the **debug token** (`X-Debug-Token`, see
  [`libs/httpx`](../httpx#runtime-debugging)) is sampled whatever the ratio
  and marked `dbg=1` in its `tracestate`. The flag makes every service behind
  the edge export its spans; the mark makes one log that request from debug
  up when it is told to trust it (`DEBUG_TRACE_TRUST_REMOTE=true`, a key of
  `libs/httpx`). A client cannot send the mark: an httpx server without that
  key — the default, and what an edge keeps — removes `dbg` from an incoming
  `tracestate` before the span starts, whatever the sampler's settings and
  with tracing on or off.

Head sampling is cheap — an unsampled trace costs no export — and blind: at
the start nobody knows whether the request will fail or be slow. The
retroactive buffer below is what makes a ratio under 1.0 usable.

## Retroactive sampling: the trace of a failure the ratio dropped

With head sampling under 1.0 the trace of a failure is kept only by luck.
`libs/otelx/retro` keeps it on purpose, within a fixed budget, inside the
process:

1. the sampler *records* a span that is not sampled instead of dropping it
   (its sampled flag, and so the decision downstream, does not change);
2. `retro.Processor`, in front of the batch processor, passes sampled spans
   on and encodes an unsampled one into an **arena** — byte rings allocated
   once at `OTEL_RETRO_BUFFER_MIB` (16 MiB by default) and never grown; the
   oldest spans leave when a ring is full;
3. a **warn or error log line** written with the context of an unsampled span
   triggers its trace: the spans of it in the arena are decoded and exported,
   and so is every span of it that ends later. `Init` binds the logger it is
   given (`httpx.BindTraceTrigger`), so a service does nothing for this.

```text
{"level":"INFO","msg":"loading task","trace_unsampled_id":"4bf9…","span_id":"…"}   ← no trace to link to yet
{"level":"WARN","msg":"store is slow","trace_id":"4bf9…","span_id":"…"}            ← this line exported it
{"level":"INFO","msg":"task stored","trace_id":"4bf9…","span_id":"…"}
```

`trace_id` is a link — a log viewer opens the trace from it — so only a
trace that is exported carries it: sampled, or triggered. The id of any
other trace is logged as `trace_unsampled_id`: the lines of one request are
still found together, and a `trace_id` never opens an empty page.

**The budget.** The memory is the arena and nothing else: a span is copied
into it as bytes (attribute keys, span names and the values of closed sets —
`http.request.method`, `messaging.*`, `db.*` — as numbers in a dictionary of
at most 4096 strings, of which values may take a quarter and names a half,
the rest being written out as text; a server span of `libs/httpx` takes
about 220 bytes, so 16 MiB hold some 75 thousand of them), so the heap holds
no span after `End`, and the garbage collector has nothing to scan — a ring
is a `[]byte`. `OTEL_RETRO_TRIGGERS_PER_SECOND` (50) bounds the exports: a
storm of errors does not become a storm of traces; a trigger above it is
refused and its line keeps `trace_unsampled_id`. A negative
`OTEL_RETRO_BUFFER_MIB` turns the buffer off.

**An unused buffer.** The arena is allocated at start whatever the sampling
ratio — a service at ratio 1 can still be handed an unsampled trace by a
trusted parent. Unused, it costs address space rather than memory: the pages
of a fresh allocation are not resident until a span is written to them, so an
empty 16 MiB buffer adds 2–3 MiB to the resident set and fills in as the
shards do (`TestProcessor_AnEmptyBufferIsNotResident`, Linux). It is counted
as live heap from the start, though: `GOMEMLIMIT` and the collector's heap
goal see all 16 MiB, and so does `go_memstats_heap_alloc_bytes`.

**What it costs** (`go test -bench . ./retro`, 32 cores): keeping a span of
eight attributes is 0.8 µs on one core and no allocation; under 32 writers
the shards (64 at 16 MiB, by a seeded hash of the trace id) keep it at 80 ns
of wall time per span. The check every span and log line makes while nothing
is triggered is one atomic load.

**Limits.**

- The past is not guaranteed: a span that left the arena before the trigger
  is gone. `trace_retro_buffer_horizon_seconds` shows how far back a trigger
  reaches; size the buffer so that it is longer than a slow request.
- The trigger is local. The services behind this one saw "not sampled" and
  keep their spans in their own buffers; their lines carry the same id as
  `trace_unsampled_id`. `Telemetry.Retro` is where a bus between services
  would plug in: `Trigger(traceID)` exports a trace another service asked
  for, `OnLocalTrigger` reports what this one triggered.
- A restart loses the arena.

Metrics (`Telemetry.Collectors()`, [docs/METRICS.md](../../docs/METRICS.md)):
`trace_retro_buffer_{bytes,capacity_bytes,spans,horizon_seconds}`,
`trace_retro_spans_total{outcome}`, `trace_retro_triggers_total{outcome}`.

**Tail sampling** is decided in the OpenTelemetry Collector, when a trace is
*over*: the collector holds the spans of a trace for a while, then keeps the
whole trace or drops it. The repository ships that configuration,
[`docker/observability/otel-collector.yml`](../../docker/observability/otel-collector.yml):

| Kept | Policy | Knob (collector environment) |
| --- | --- | --- |
| always | a span in status ERROR anywhere in the trace | — |
| always | the trace took longer than the threshold | `OTEL_TAIL_SLOW_THRESHOLD_MS` (`500`) |
| always | a span has the attribute `debug.subject`, any value | — |
| a share | everything else | `OTEL_TAIL_SAMPLING_PERCENTAGE` (`1`) |

`OTEL_TAIL_DECISION_WAIT` (`10s`) is how long a trace is held before the
decision, `OTEL_TAIL_NUM_TRACES` (`50000`) how many are held at once.

`debug.subject` is the handle for an investigation: a service sets it on a
span for the tenant or the user being looked at —
`span.SetAttributes(attribute.String("debug.subject", tenantID))` — and every
trace of theirs is kept while everybody else's stay at the baseline share.

Two rules follow, and both fail silently when broken:

1. **With tail sampling, head-sample at 1.0.** The collector decides on what
   it receives. `OTEL_TRACES_SAMPLER_RATIO=0.1` in a service means the
   collector sees a tenth of the traces — and keeps "100% of errors" out of
   that tenth, plus what the retroactive buffer sends after a warn or error
   line: the spans of *that* service, not the whole trace, and nothing for a
   request that was merely slow. Leave the ratio at its default (1.0) and
   move the volume decision to `OTEL_TAIL_SAMPLING_PERCENTAGE`. Lower the
   head ratio only when the cost of *exporting* everything is the problem,
   knowing that it multiplies every tail policy.
2. **One collector must see every span of a trace.** The decision is made
   from the spans one collector process holds. Several replicas behind a
   plain load balancer each get a fragment: the replica with the failed span
   keeps its part, the others drop theirs at the baseline share, and the
   trace arrives with holes. Run one sampling collector, or two tiers: a
   stateless first tier with the `loadbalancing` exporter
   (`routing_key: traceID`) in front of the sampling replicas. The same
   reason sets a floor for `OTEL_TAIL_DECISION_WAIT`: it has to outlast the
   slowest trace worth keeping whole plus the services' batch delay
   (`OTEL_BSP_SCHEDULE_DELAY`).

Tail sampling also costs memory in the collector (every trace is held for
the wait) and adds the wait to the time a trace takes to appear in the
backend.

[`integration/`](integration) runs that configuration file in a real
collector, fed through `otelx.Init`, and asserts each line of the table —
and rule 1.

## Resource

Every span carries `service.name`, `service.version`,
`service.instance.id` (the hostname, i.e. the pod name),
`deployment.environment.name` (when set), host, container id, process and
runtime attributes, the SDK's own, and whatever `OTEL_RESOURCE_ATTRIBUTES`
adds. An empty `ServiceName` falls back to `OTEL_SERVICE_NAME` and then the
SDK's `unknown_service:<exe>`.

## Configuration

With prefix `TASKS_` (YAML keys for `httpx.LoadYAML` in brackets):

| Variable                                         | Default          | Meaning                                         |
| ------------------------------------------------ | ---------------- | ----------------------------------------------- |
| `TASKS_OTEL_ENABLED` (`enabled`)                 | `false`          | export traces                                   |
| `TASKS_OTEL_SERVICE_NAME` (`service_name`)       | *(service sets)* | resource `service.name`                         |
| `TASKS_OTEL_SERVICE_VERSION` (`service_version`) | *(service sets)* | resource `service.version`                      |
| `TASKS_OTEL_DEPLOYMENT_ENVIRONMENT` (`deployment_environment`) | *(empty)* | resource `deployment.environment.name` |
| `TASKS_OTEL_EXPORTER_OTLP_ENDPOINT` (`endpoint`) | `localhost:4318` | collector `host:port`, OTLP over HTTP; a URL, or a host without a port, is refused |
| `TASKS_OTEL_EXPORTER_OTLP_INSECURE` (`insecure`) | `false`          | plaintext HTTP instead of TLS                   |
| `TASKS_OTEL_EXPORTER_OTLP_HEADERS` (`headers`)   | *(none)*         | `k=v,k=v` request headers (secret)              |
| `TASKS_OTEL_EXPORTER_OTLP_CERTIFICATE` (`ca_certificate`) | system roots | CA bundle (PEM) of the collector       |
| `TASKS_OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` (`client_certificate`) | *(none)* | client certificate (PEM), mTLS |
| `TASKS_OTEL_EXPORTER_OTLP_CLIENT_KEY` (`client_key`) | *(none)*     | client key (PEM), mTLS                          |
| `TASKS_OTEL_EXPORTER_OTLP_TIMEOUT` (`export_timeout`) | `10s`       | per-export request timeout                      |
| `TASKS_OTEL_TRACES_SAMPLER_RATIO` (`sampler_ratio`) | `1.0`         | head sampling ratio in `[0, 1]`; `0` samples no root (a `Config` built in code has no default: its zero is `0`); keep 1.0 under tail sampling |
| `TASKS_OTEL_TRACES_REDRAW_REMOTE_PARENT` (`redraw_remote_parent`) | `false` | `true` on an internet-facing edge: ignore a remote parent's sampled flag (draw at the ratio) and drop its `tracestate`. `LoadConfig` / `LoadConfigFrom` refuse an environment that sets `TASKS_OTEL_TRACES_TRUST_REMOTE_PARENT`, the key this one replaced with the opposite sense; `httpx.LoadYAML` refuses it too, in the file (unknown key) and in the environment (`Config.RetiredKeys`) |
| `TASKS_OTEL_RETRO_BUFFER_MIB` (`retro.buffer_mib`) | `16`          | retroactive sampling buffer, MiB, allocated at start; negative = off (`0` is read as "not set") |
| `TASKS_OTEL_RETRO_TRIGGERS_PER_SECOND` (`retro.triggers_per_second`) | `50` | traces a warn / error line may export a second; negative is refused |
| `TASKS_OTEL_BSP_SCHEDULE_DELAY` (`batch_timeout`) | `5s`            | batch flush interval                            |
| `TASKS_OTEL_BSP_MAX_QUEUE_SIZE` (`max_queue_size`) | `2048`         | spans buffered before dropping                  |
| `TASKS_OTEL_BSP_MAX_EXPORT_BATCH_SIZE` (`max_export_batch_size`) | `512` | spans per export request             |

The standard, unprefixed `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME`
are honoured for the resource as well; the unprefixed exporter variables are
not ([Export](#export) has the list).

## Tests

`telemetry_test.go` is the telemetry contract: a real `httpx` server with the
real SDK behind `testx.Recorder`, the real logger into `testx.LogBuffer` and
the real Prometheus registry, asserting that the span (name, kind, parent,
`http.route`, `http.response.status_code`, error status on 5xx), the log lines
(`trace_id`/`span_id`/`request_id`, access-log fields) and the metrics
(`route` label, `trace_id` exemplar, promlint-clean) describe the same
request. `otelx_test.go` covers config loading and validation, TLS setup,
the resource and the throttled error handler; `exporter_test.go` runs `Init`
against an `httptest` collector — the CA file as the trust store, headers,
queue and batch sizes, the ambient `OTEL_EXPORTER_OTLP_*` variables, and
shutdown (a refusing collector, a cancelled context);
`sampler_test.go` the head-sampling policy
(roots, children, the untrusted remote parent, forced traces);
`retro_pipeline_test.go` the buffer and the debug trace through the provider
`Init` builds, the real logger and two real servers. In `retro/`: the rings
against a model (a property test), the codec (round trip of every field,
every truncation, a fuzz target), and the processor — conservation of spans
under concurrent offers and triggers, zero allocations per span, a heap that
does not grow. [`integration/`](integration) (a
module of its own, container-backed) covers the path from `Init` through the
shipped collector configuration: what tail sampling keeps, and a warn line
bringing an unsampled trace to Jaeger through the real exporter.

```sh
just test
just lint
```
