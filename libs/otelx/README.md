# otelx

Shared distributed-tracing setup for golang-basics services: one call to
`otelx.Init` installs the OpenTelemetry tracer provider (OTLP over HTTP, TLS
by default) and the W3C trace-context + baggage propagators. Everything else
hangs off that global provider:

- `libs/httpx` starts a server span per request (`GET /tasks/{id}`,
  `SpanKindServer`, continuing an inbound `traceparent`), stamps
  `trace_id`/`span_id` on every log line written with the request context,
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
shutdown, err := otelx.Init(ctx, otelCfg, logger)
if err != nil { … }
defer func() { _ = shutdown(context.Background()) }() // flushes buffered spans
```

Call `Init` before building the clients whose instrumentation should record
spans: the instrumentation libraries capture the global provider when they
are constructed.

## Signals: traces here, metrics in Prometheus

Only traces go through OpenTelemetry. Metrics are Prometheus-native and
scraped from each service's admin listener (`libs/httpx.Metrics` plus the
libs' collectors); `otelx` installs no meter provider, so the OTel meters of
the instrumentation libraries stay no-ops. Logs are plain JSON on stdout with
the trace ids in them (see `httpx.NewLogger`).

## Export

- **OTLP/HTTP** (`otlptracehttp`) rather than gRPC: the same wire protocol,
  one fewer dependency tree, and it passes through any HTTP proxy. The
  endpoint is a `host:port` (default `localhost:4318`).
- **TLS by default**, with the system roots; `OTEL_EXPORTER_OTLP_CERTIFICATE`
  adds a CA bundle, `…_CLIENT_CERTIFICATE` + `…_CLIENT_KEY` enable mTLS
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

## Sampling: head in the service, tail in the collector

There are two places to decide which traces are kept, and they are not
interchangeable.

**Head sampling** is decided here, in the service, when a trace *starts*:
parent-based ratio sampling. A root span is sampled with probability
`OTEL_TRACES_SAMPLER_RATIO` (a negative ratio never samples; `0` is read as
"not set" and means 1.0), a child follows its parent.
`OTEL_TRACES_TRUST_REMOTE_PARENT=true` (the OpenTelemetry default) keeps a
sampled caller's trace intact through the service; turn it off on an
internet-facing edge, where an untrusted client could otherwise force
sampling — the local ratio then decides for remote parents too. Head
sampling is cheap — an unsampled trace costs no export — and blind: at the
start nobody knows whether the request will fail or be slow.

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
   that tenth: nine error traces in ten were never sent, and no policy brings
   them back. Leave the ratio at its default (1.0) and move the volume
   decision to `OTEL_TAIL_SAMPLING_PERCENTAGE`. Lower the head ratio only
   when the cost of *exporting* everything is the problem, knowing that it
   multiplies every tail policy.
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
| `TASKS_OTEL_EXPORTER_OTLP_ENDPOINT` (`endpoint`) | `localhost:4318` | collector `host:port`, OTLP over HTTP           |
| `TASKS_OTEL_EXPORTER_OTLP_INSECURE` (`insecure`) | `false`          | plaintext HTTP instead of TLS                   |
| `TASKS_OTEL_EXPORTER_OTLP_HEADERS` (`headers`)   | *(none)*         | `k=v,k=v` request headers (secret)              |
| `TASKS_OTEL_EXPORTER_OTLP_CERTIFICATE` (`ca_certificate`) | system roots | CA bundle (PEM) of the collector       |
| `TASKS_OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` (`client_certificate`) | *(none)* | client certificate (PEM), mTLS |
| `TASKS_OTEL_EXPORTER_OTLP_CLIENT_KEY` (`client_key`) | *(none)*     | client key (PEM), mTLS                          |
| `TASKS_OTEL_EXPORTER_OTLP_TIMEOUT` (`export_timeout`) | `10s`       | per-export request timeout                      |
| `TASKS_OTEL_TRACES_SAMPLER_RATIO` (`sampler_ratio`) | `1.0`         | head sampling ratio; negative = never; keep 1.0 under tail sampling |
| `TASKS_OTEL_TRACES_TRUST_REMOTE_PARENT` (`trust_remote_parent`) | `true` | honour a caller's sampled flag       |
| `TASKS_OTEL_BSP_SCHEDULE_DELAY` (`batch_timeout`) | `5s`            | batch flush interval                            |
| `TASKS_OTEL_BSP_MAX_QUEUE_SIZE` (`max_queue_size`) | `2048`         | spans buffered before dropping                  |
| `TASKS_OTEL_BSP_MAX_EXPORT_BATCH_SIZE` (`max_export_batch_size`) | `512` | spans per export request             |

The standard, unprefixed `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME`
are honoured for the resource as well.

## Tests

`telemetry_test.go` is the telemetry contract: a real `httpx` server with the
real SDK behind `testx.Recorder`, the real logger into `testx.LogBuffer` and
the real Prometheus registry, asserting that the span (name, kind, parent,
`http.route`, `http.response.status_code`, error status on 5xx), the log lines
(`trace_id`/`span_id`/`request_id`, access-log fields) and the metrics
(`route` label, `trace_id` exemplar, promlint-clean) describe the same
request. `otelx_test.go` covers config loading, TLS setup, the sampler, the
resource and the throttled error handler. [`integration/`](integration) (a
module of its own, container-backed) covers the path from `Init` through the
shipped collector configuration: what tail sampling keeps.

```sh
just test
just lint
```
