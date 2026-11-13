# otelx / integration

The path a span really takes: the SDK that `otelx.Init` builds → OTLP over
HTTP → an OpenTelemetry Collector running **the configuration this repository
ships** ([`docker/observability/otel-collector.yml`](../../../docker/observability/otel-collector.yml),
mounted as it is, not a copy) → a trace store. What is asserted is what the
collector's tail sampling lets through. A Go module of its own, inside
`otelx`'s folder: testcontainers and the Docker client are this module's
dependencies and never appear in `otelx`'s `go.mod`.

```sh
just mod libs/otelx/integration test   # from the repo root; needs Docker
go test -race -v ./...                 # here
```

About 20 seconds. Skipped under `-short` and without Docker; **failed** when
`CI` is set and a container cannot start. On rootless podman set
`TESTCONTAINERS_RYUK_DISABLED=true` — and read the `-v` output: an `ok` with
`--- SKIP` lines has tested nothing.

## The stack

| Container | Image | Role |
| --- | --- | --- |
| collector (baseline 0) | `otel/opentelemetry-collector-contrib:0.162.0` — the image `docker/observability.yml` runs | the shipped configuration with `OTEL_TAIL_SAMPLING_PERCENTAGE=0`: the verdict on a plain trace is "dropped", not a matter of chance |
| collector (baseline 100) | same | the same configuration with the share at 100, to show the knob reaches the policy |
| jaeger | `jaegertracing/jaeger:2.21.0` | the trace store behind both collectors; its query API is the ground truth |

Only three things differ from the local stack, all through the environment
variables the configuration file reads: the baseline share, the decision wait
(`OTEL_TAIL_DECISION_WAIT=1s` instead of 10s — the suite's traces arrive in
one batch) and where the collector exports to
(`OTEL_COLLECTOR_TRACES_ENDPOINT`, Jaeger's host-mapped port, so no container
network is needed). The slow threshold is the shipped default.

**The configuration file is validated by starting.** A collector that exits
instead of becoming healthy has refused the file; the fixture turns that into
a failure of every test, with the collector's log — never into a skip.

**Ground truth comes from the store.** `stored` asks Jaeger
(`/api/v3/traces`) which trace ids it holds for the test's own service name
(`testx.Unique`) and how many spans each has.

**Absence is proven by a sentinel, not by a sleep.** To assert that a trace
was dropped the test sends, after it, a trace the collector certainly keeps,
and waits (`require.Eventually`) for that one to be stored: by then the
collector has decided on everything that arrived earlier.

**Durations are timestamps.** A "slow" trace is a span whose start and end
are 800 ms apart; nothing sleeps.

## What each test owns

| Test | Asserts |
| --- | --- |
| `TestTailSamplingKeepsErrorsSlowAndDebugTraces` | baseline 0: every trace with an ERROR span is kept — whole, root included, when the error is in a child; every trace longer than the threshold is kept; every trace with `debug.subject` is kept, whatever the value (a string, an empty string, a number); none of the fast successful traces is; nothing else is stored |
| `TestTailSamplingBaselineShareIsConfigurable` | baseline 100: every fast successful trace is kept |
| `TestHeadSamplingDecidesWhatTailSamplingSees` | the documented rule: with head sampling off in the service, error traces never reach the collector, while the same trace from a service sampling at 1.0 does |
| `TestCollectorImageIsTheShippedOne` | the image the suite runs is the one in `docker/observability.yml` |

## What it does not cover

- **A share between 0 and 100.** Whether 1% is 1% is the collector's
  arithmetic; asserting it needs thousands of traces and a tolerance.
- **Spans that arrive after the decision**, and a trace split between
  collector replicas: the README of `otelx` states the rules
  (`OTEL_TAIL_DECISION_WAIT`, one collector per trace); reproducing them is
  a test of the collector, not of this configuration.
- **`OTEL_TAIL_NUM_TRACES` and the memory limiter under load.**
- **The export itself** (TLS, headers, batching, the resource): the unit
  layer of `otelx` owns it.

## Adding a test

A new policy in the configuration file gets a kind of trace here: emit it
under a `testx.Unique` service name, add a sentinel after anything that must
be absent, assert on `settled`. Tests are serial — `otelx.Init` installs the
OpenTelemetry globals. A new knob is an environment variable of the collector
with a default in the file; a different value is another `collector(tb, …)`
fixture, not another configuration file.
