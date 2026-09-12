# golang-basics

A small, idiomatic **Go monorepo** modelled on the Rust `tracehub-edge`
workspace: a `go.work` workspace of service modules + shared library modules
(the Go analogue of "one crate = one module"), a `just`-driven build/test/lint/
security/CI surface, per-package justfiles, multi-stage distroless Docker
images, a Playwright e2e suite, and k6 load tests. It exists as a learning /
template scaffold, wired up the way a real Go monorepo would be.

It comes in two layers: **dependency-free** building blocks (the `ping` HTTP
service, the `heartbeat` worker and the `httpx` / `resilient-http-client` libs)
and a **data-services** vertical that adds real infrastructure — the `tasks`
CRUD service (PostgreSQL + Valkey cache + Kafka producer + OpenTelemetry +
RFC 9457 errors) and the `consumer` worker that drains the events `tasks`
produces — backed by the `pgx` / `valkey` / `kafka` / `otelx` libs.

It lives in a **bare-repo worktree container** (see [Worktrees](#worktrees-multi-branch-dev)),
exactly like its Rust sibling: the repo root is a `.bare/` container and the
code lives in per-branch worktrees (`master/` is canonical).

---

## Modules

| Module                              | Kind        | Port(s)                       | One-liner                                                                                          |
| ----------------------------------- | ----------- | ----------------------------- | ------------------------------------------------------------------------------------------------- |
| [`services/ping`](services/ping)           | HTTP service | `:8080` API, `:9080` admin   | Ping/pong HTTP service. `GET /ping` → `pong`, with `?msg=` echo + `/version`.                      |
| [`services/heartbeat`](services/heartbeat) | Worker       | `:9081` admin                | Background ticker worker — emits a beat + bumps `heartbeat_beats_total` every interval.            |
| [`services/tasks`](services/tasks)         | HTTP service | `:8082` API, `:9082` admin   | Tasks CRUD over **Postgres + Valkey + Kafka**, traced end-to-end, with `problem+json` errors. Publishes `task.created`. |
| [`services/consumer`](services/consumer)   | Worker       | `:9083` admin                | Kafka consumer draining `tasks.events`; continues the producer's trace; exports group lag.         |
| [`api`](api)                               | Contracts    | —                            | TypeSpec source of the tasks HTTP API and the Kafka events; emits OpenAPI 3.0 + JSON Schema (committed). |
| [`libs/contracts`](libs/contracts)         | Library      | —                            | Generated Go types of those contracts (`tasksapi`, `events`), shared by producer and consumer. Never edited by hand. |
| [`libs/httpx`](libs/httpx)                 | Library      | —                            | Shared HTTP scaffolding: gin engine, sampled trace-correlated logging, Prometheus metrics, admin listener (health/metrics/version/pprof), graceful shutdown, env + YAML config, RFC 9457 `Problem`. |
| [`libs/resilient-http-client`](libs/resilient-http-client) | Library | —              | Policy-per-target **outbound** HTTP client: rate limiting, circuit breaker, adaptive concurrency, jittered retry, response cache, coalescing, fallbacks, metrics. |
| [`libs/pgx`](libs/pgx)                     | Library      | —                            | PostgreSQL pool (`jackc/pgx`): env config, readiness check, boot-time migrations.                  |
| [`libs/valkey`](libs/valkey)               | Library      | —                            | Valkey cache (`valkey-go`): get/set/del, readiness check, generic cache-aside helper.              |
| [`libs/kafka`](libs/kafka)                 | Library      | —                            | Kafka producer + consumer (`franz-go`): sync publish, at-least-once consumer-group loop, readiness. |
| [`libs/otelx`](libs/otelx)                 | Library      | —                            | OpenTelemetry tracing: OTLP exporter, W3C propagation, gin middleware (opt-in).                    |
| [`libs/testx`](libs/testx)                 | Test harness | —                            | `_test`-only: Allure handle (testo), one shared container per test binary, span recorder, log buffer, metric helpers. |

The dependency graph is `services/* → libs/*`. Every service — HTTP-first or
worker — reuses `httpx` for its **admin listener** (`/healthz`, `/readyz`,
`/metrics`, `/version`, `/debug/pprof`, always API port + 1000), so a worker is
as observable as a server and the API port never carries operational routes. `ping`/`heartbeat` stay dependency-free;
`tasks`/`consumer` compose the data libs (`pgx`/`valkey`/`kafka`/`otelx`) and
need the backing services from [`docker/deps.yml`](docker/deps.yml) (`just infra-up`).

---

## Layout

```
golang-basics/              ← bare-repo CONTAINER (.bare + .git pointer + wt + CLAUDE.md)
└── master/                 ← canonical worktree (this tree)
    ├── go.work             ← workspace: ties all ten modules together
    ├── justfile            ← workspace task runner (fan-out + delegation)
    ├── mise.toml           ← pinned toolchain (go, golangci-lint, node, k6, AppSec tools)
    ├── lefthook.yml        ← optional git hooks
    ├── .golangci.yml       ← lint config
    ├── libs/httpx/         ← shared HTTP scaffolding (engine, health, metrics, problem+json, YAML config)
    ├── libs/resilient-http-client/ ← outbound HTTP client library (rate limit, CB, retry, cache…)
    ├── libs/pgx/           ← PostgreSQL pool library
    ├── libs/valkey/        ← Valkey cache library
    ├── libs/kafka/         ← Kafka producer/consumer library
    ├── libs/otelx/         ← OpenTelemetry tracing library
    ├── services/ping/      ← HTTP service module (+ Dockerfile)
    ├── services/heartbeat/ ← worker module (+ Dockerfile)
    ├── services/tasks/     ← Postgres+Valkey+Kafka CRUD service (+ Dockerfile)
    ├── services/consumer/  ← Kafka consumer worker (+ Dockerfile)
    ├── docker/             ← deps.yml (Postgres+Valkey+Kafka) + stack.yml (the app images)
    ├── e2e/                ← Playwright API tests (spawns the service binaries)
    ├── benchmarks/         ← k6 load tests
    └── scripts/            ← host-services-spawn.sh (just up / down)
```

---

## Quick start

```sh
# 1. Install the pinned toolchain (go, golangci-lint, node, k6, …) once.
mise trust && mise install        # or: just setup

# 2. Build + test everything.
just ci                           # fmt-check → vet → lint → test

# 3. Run the dependency-free services on the host.
just up                           # ping :8080 (admin :9080) + heartbeat (admin :9081)
curl -s localhost:8080/ping | jq .
curl -s localhost:9081/metrics | grep heartbeat_beats_total
curl -s localhost:9080/version | jq .
just down

# 4. Run the data-services vertical (Postgres + Valkey + Kafka).
just infra-up                     # docker compose deps (postgres/valkey/kafka)
just tasks run &                  # tasks :8082 (admin :9082)
just consumer run &               # consumer (admin :9083)
curl -s -XPOST localhost:8082/tasks -d '{"title":"hello"}' | jq .
curl -s localhost:9083/metrics | grep -E 'consumer_tasks_consumed_total|kafka_consumer_group_lag'
#   …or run the whole thing in containers instead:
just stack-up                     # deps + tasks + consumer images, all wired up

# 5. Exercise them end-to-end / under load.
just e2e                          # Playwright, dependency-free services
just e2e-deps                     # Playwright incl. tasks + consumer (needs `just infra-up`)
just bench-smoke                  # k6, 50 VUs × 30s against ping
just bench-tasks smoke            # k6 against tasks (needs `just infra-up`)
```

---

## Common workspace commands

A go.work workspace root is not itself a module, so the workspace-wide recipes
fan out over each module rather than relying on `./...`.

```sh
just check           # go vet ./...        in every module
just build           # go build ./...      in every module
just test            # go test ./...       in every module
just test-race       # go test -race       in every module
just cov             # per-module coverage summary
just fmt             # gofmt -w + golangci-lint fmt
just fmt-check       # gofmt -l gate (CI)
just lint            # golangci-lint run    in every module
just tidy            # go mod tidy everywhere + go work sync
just audit           # govulncheck          in every module
just ci              # fmt-check → vet → lint → test
just ci-full         # + race tests + audit
just clean           # remove build/test/coverage artefacts
```

### Per-package commands

Forward any recipe to a single module's justfile:

```sh
just httpx <recipe>      # also: resilient, pgx, valkey, kafka, otelx
just ping <recipe>       # also: heartbeat, tasks, consumer

# examples
just ping test
just tasks test-integration   # full PG+Valkey+Kafka stack via testcontainers
just httpx cov-html
just heartbeat lint
```

### Run a recipe across every module

```sh
just each test         # in dependency order: libs first, then services
just each lint
just each ci
```

### Infra dependencies (Postgres + Valkey + Kafka)

`tasks` and `consumer` need backing services. `docker/deps.yml` brings them up;
`docker/stack.yml` runs the app images on the same network.

```sh
just infra-up        # postgres :5432 + valkey :6379 + kafka :9092
just infra-logs      # tail them
just infra-down      # stop + drop volumes
just stack-up        # deps + build & run the tasks/consumer images
just stack-down      # tear the whole stack down
```

---

## E2E tests (Playwright)

API tests (no browser). The harness builds the service binaries, spawns them,
waits for `/healthz`, runs the specs, then stops them. See
[`e2e/README.md`](e2e/README.md).

```sh
just e2e-install     # pnpm install (once)
just e2e             # build binaries + run the dependency-free suite
just e2e-deps        # + tasks & consumer specs (needs `just infra-up`)
just e2e-ui          # Playwright UI
just e2e-filter ping # subset
just e2e-report      # open last report
```

The `tasks`/`consumer` specs run only under `E2E_WITH_DEPS=1` (set by
`just e2e-deps`); otherwise they skip, so the default suite stays Docker-free.

## Benchmarks (k6)

Profiles mirror the Rust sibling repo (`smoke` / `load` / `stress` / `soak` /
`peak`). See [`benchmarks/README.md`](benchmarks/README.md).

```sh
just bench-smoke         # ping: 50 VUs × 30s
just bench-load          # ping: ramp 0→500 VUs
just bench-stress        # ping: ramp 0→2000 VUs
just bench-soak          # ping: 500 VUs × 30m
just bench-peak          # ping: constant-arrival-rate 25k req/s × 1m
just bench-tasks smoke   # tasks (create+read): needs `just infra-up`
```

---

## Security tooling

AppSec tools are pinned in `mise.toml` and installed by `just setup-sec`.

| Recipe              | Tool        | Config              | Covers                                            |
| ------------------- | ----------- | ------------------- | ------------------------------------------------- |
| `just sec-secrets`  | gitleaks    | `.gitleaks.toml`    | secrets in tree + history                         |
| `just sec-sast`     | semgrep     | `.semgrepignore`    | `p/owasp-top-ten` + `p/golang` packs              |
| `just sec-deps`     | osv-scanner | `osv-scanner.toml`  | OSV.dev advisories over `go.mod` + `pnpm-lock`    |
| `just sec-iac`      | hadolint    | `.hadolint.yaml`    | every `services/*/Dockerfile`                     |
| `just audit`        | govulncheck | —                   | Go-native reachable-vuln scan per module          |
| `just sec`          | —           | —                   | runs the four source-side checks fail-fast        |

Container side (against a locally-built image):

```sh
just docker-build ping        # build golang-basics-ping:dev
just docker-scan ping         # syft SBOM + grype CVE scan
just docker-scan-ci ping      # same, --fail-on high (CI gate)
just docker-sign ping dev     # cosign sign (key-mode, no Rekor)
just docker-verify ping dev   # offline verify against cosign.pub
```

---

## CI

Two pipelines, deliberately kept at parity: `.github/workflows/{ci,appsec,docker}.yml`
and `.gitlab-ci.yml`.

**Nothing hard-codes a tool version or a module list.** Both pipelines start
with a `versions` job that runs `scripts/mise-pins.sh` (tool pins out of
`mise.toml`) and `scripts/touched-modules.sh --all` (modules out of `go.work`);
GitHub consumes them as job outputs / `fromJSON` matrices, GitLab as a `dotenv`
artifact that later jobs use as ordinary variables, including inside `image:`.
Bump a version in `mise.toml` or add a module to `go.work` and both pipelines
follow; there is no second place to remember. The scanners (gitleaks, semgrep,
hadolint, syft, grype) run from the same version-pinned images on both sides,
so the two pipelines cannot disagree about a finding.

**Nothing hard-codes a host either** — see "Closed networks" below.

The GitLab side is shaped around not burning runner minutes:

| | |
|---|---|
| `prepare` | downloads the module graph **once** and warms `GOMODCACHE`/`GOCACHE`, then every other job pulls that cache — instead of 25 cold downloads across the matrices. It also builds gotestsum / gocover-cobertura / govulncheck once and passes them on as an artifact. |
| `needs:` | the stage list is a DAG, so the security and packaging jobs start as soon as `versions` is done rather than queueing behind the test matrix. |
| `interruptible: true` | a superseded pipeline is cancelled instead of running to completion. |
| `rules:changes` | a docs-only merge request skips the Go pipeline; the default branch always runs everything. |
| pinned images | golangci-lint, gitleaks, semgrep, hadolint, osv-scanner, syft and grype all come from their own version-pinned images — no `curl \| sh` per matrix job, no `:latest`. |

It also uses what GitLab gives you and GitHub does not: `artifacts:reports:junit`
puts failing tests in the MR widget, and `coverage:` plus a Cobertura report put
the percentage and per-line annotations in the diff. The GitLab `e2e` job runs
the full suite (tasks + consumer included) against Postgres / Valkey / Redpanda
declared as `services:` — no docker-in-docker, no compose, no package install,
and the job image is plain `node`.

The caches live under `.cache/` inside the project (GitLab only caches paths
below `$CI_PROJECT_DIR`) — which is why the `gofmt` gate is scoped to
`./libs ./services` rather than `.`, in all three of `just fmt-check`, the
GitHub job and the GitLab job.

Run any job locally, in the same container CI would use:

```sh
npx gitlab-ci-local --list          # resolve the DAG without running anything
npx gitlab-ci-local fmt
npx gitlab-ci-local 'unit: [libs/httpx]'
```

---

## Docker

Each service has a multi-stage **distroless** Dockerfile (static `CGO_ENABLED=0`
binary on `gcr.io/distroless/static-debian12:nonroot`, uid 65532, no shell) —
including `tasks`/`consumer`, since `pgx`, `valkey-go` and `franz-go` are all
pure Go (no `libpq`/`librdkafka` to link). The build context is the **workspace
root** so the build sees every module it imports:

```sh
docker build -f services/ping/Dockerfile -t ping:dev .
docker run --rm -p 8080:8080 ping:dev

# the data-services stack (deps + app images), one command:
just stack-up        # docker compose deps.yml + stack.yml
curl -s -XPOST localhost:8082/tasks -d '{"title":"hi"}' | jq .
just stack-down
```

`docker/deps.yml` runs Postgres + Valkey + Redpanda (the Kafka API broker, dual listeners so both
host processes and in-network containers reach the broker); `docker/stack.yml`
runs the `tasks`/`consumer` images against it.

---

## Observability

What every service emits, and where — designed so an existing platform (a
stdout log collector, an OTLP trace collector, a runtime agent scraping
metrics and pprof) plugs in without code changes:

| Signal  | Where                                            | What                                                                                                                                       |
| ------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Logs    | stdout, one JSON line per record                 | `service`, `trace_id`/`span_id` when the context carries a span, one access-log line per API request (`route`, `status`, `latency_ms`, `bytes`); debug/info **sampled** per message (first 100/s, then every 100th — `*_LOG_SAMPLE_*`), warn/error/slow never dropped |
| Traces  | OTLP gRPC to `*_OTEL_EXPORTER_OTLP_ENDPOINT`     | HTTP server span → pgx query spans (otelpgx) → Valkey command spans (valkeyotel) → Kafka produce span; the record headers carry the context so the consumer's `process` span continues the same trace |
| Metrics | `/metrics` on the **admin** port (API port + 1000) | `build_info`, RED per route (`http_requests_total`, `http_request_duration_seconds` classic + native histogram, `http_requests_in_flight`), `log_dropped_total`, pgx pool (`pgxpool_*`), cache `cache_lookups_total{result}`, Kafka `kafka_producer_*` / `kafka_consumer_*` incl. `kafka_consumer_group_lag` |
| Profiles| `/debug/pprof/` on the admin port                | cpu / heap / goroutine / block / mutex / trace, for a runtime agent or `go tool pprof http://host:9080/debug/pprof/heap` |
| Identity| `/version` on the admin port                     | service, version (`-X libs/httpx.Version`, set by `scripts/build-service.sh` / `VERSION` build arg), VCS revision, Go version |

Log with the context (`log.InfoContext(ctx, …)`) and pass `ctx` down; that is
all a handler has to do for its log lines, DB calls, cache calls and Kafka
records to share one trace id.

`docker/observability.yml` is a *local* receiving end for the above, shaped
like the platform the services meet in production — a collector in front of
the trace store, a stdout shipper in front of the log store, a scraper on the
admin ports — so the correlation story can be checked on a laptop. Optional:
nothing in the app path depends on it.

```sh
just infra-up     # deps first — the stack joins that network
just obs-up
#   OTLP     localhost:4317 (gRPC) / :4318 (HTTP)   ← *_OTEL_EXPORTER_OTLP_ENDPOINT
#   Jaeger   http://localhost:16686
#   Metrics  http://localhost:9095   (VictoriaMetrics)
#   Loki     http://localhost:3100
#   Grafana  http://localhost:3000   (admin / admin, dashboard "golang-basics")
just stack-up-otel   # tasks + consumer in containers, exporting traces
just obs-down
```

| Piece | Role | Cost |
|---|---|---|
| OpenTelemetry Collector | the one OTLP endpoint services know; batches, forwards traces to Jaeger; where tail sampling / redaction / a second exporter would go | ~60 MB |
| Jaeger all-in-one | trace store + UI, in memory; only the collector talks to it | ~50 MB |
| VictoriaMetrics | scrapes `/metrics` on the admin ports + the dependency exporters; native histograms | ~50 MB |
| Loki + Vector | Vector tails every `golang-basics-*` container's stdout through the Docker socket, lifts the JSON fields (`service`, `level`, `trace_id`…) and ships to Loki | ~60 + 40 MB |
| Grafana | datasources provisioned and cross-linked (span → its log lines, log line → its trace); one dashboard from `docker/observability/dashboards/` | the heavy one |
| postgres-exporter, redis_exporter | Postgres / Valkey metrics; Redpanda is scraped directly on `/public_metrics` | ~10 MB each |

Each scrape job carries two targets — the container name and
`host.docker.internal` — so the same config works whether the services run via
`just stack-up` or on the host via `just up`. Whichever set is not running just
shows as down.

What "it works" looks like, after `just stack-up-otel` and one `POST /tasks`:
the Jaeger trace holds the `POST /tasks` server span, `INSERT`, `SET`,
`tasks.events publish` from `tasks` and `tasks.events receive` / `process` from
`consumer`; a Loki query `{service="consumer"} |= "<trace_id>"` finds the
consumer's log line and `{service="tasks"} |= "<trace_id>"` the access-log
line; the dashboard shows the request, the cache hit, the publish and the lag.
That path is what the telemetry tests assert (see "Tests").

**Tracing is opt-in.** With `*_OTEL_ENABLED` unset, `otelx` installs only the
W3C propagators and a no-op provider, so nothing depends on a collector being
up. To export:

```sh
# host process → local collector
TASKS_OTEL_ENABLED=true TASKS_OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 just tasks run
# or a single service on the host / under the debugger:
CONSUMER_OTEL_ENABLED=true CONSUMER_OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 just consumer run
```

---

## Tests

Every test in the workspace is an Allure test, every layer feeds one merged
coverage number, and every behaviour is checked at exactly one layer. The
harness that makes that cheap is [`libs/testx`](libs/testx) (`_test`-only).

### Layers and what each one owns

| Layer | Where | Owns | Does not repeat |
|---|---|---|---|
| **Unit** | `*_test.go` next to the code, `-short` | pure logic, config parsing, handlers behind the small interfaces with fakes, readiness cache semantics, log sampling arithmetic, route ↔ listener wiring of a service | anything that needs a real dependency |
| **Contract** (telemetry) | `libs/otelx/telemetry_test.go`, `libs/httpx/health_test.go`, `*/metrics_test.go` | the access-log line, the span and the counters describe the same request; sampling is exact under concurrency; every exposition is promlint-clean; readiness never stampedes a dependency | route behaviour (unit), real dependencies (integration) |
| **Integration** | `libs/{pgx,valkey,kafka}`, `services/tasks/internal/integration`, `services/consumer/internal/worker` — one shared container per test binary | the libs against real Postgres / Valkey / Kafka: query and command spans inside the caller's span, a record carrying its trace through the broker, pool / hit-miss / lag moving with real usage; the tasks vertical wired exactly as `main.go` wires it, one trace across request, log lines and metrics, readiness with its checks | route contracts already proven with fakes; process-level behaviour |
| **E2E** | `e2e/` (Playwright, real binaries) | what only a real process shows: it starts, is ready on its admin listener, is a scrape target that identifies itself, reports the build stamp, and the cross-process flow tasks → Kafka → consumer | per-route behaviour, error bodies, admin route inventory (unit), the CRUD flow (integration) |
| **Chaos** | `ChaosSuite` in `services/tasks/internal/integration` (Toxiproxy in front of Postgres / Valkey, a frozen Kafka — `testx.Proxied`, `testx.Pause`) | what the design promises when a dependency fails, slows down or hangs: the cache is bypassed, events are best-effort, readiness degrades for optional dependencies and turns 503 only for the critical one, probes stay fast, everything recovers | happy-path behaviour (integration), the shape of error responses (contract) |
| **Generative** | `just schemathesis <svc>` (Schemathesis against the real binary and its OpenAPI document) | inputs nobody wrote a test for: every operation with generated positive and negative requests and stateful sequences, no 5xx, every response in the contract's shape — "bad input → 4xx problem" cases are owned here, not hand-written | business semantics the schema cannot express (unit), effects on dependencies (integration) |
| **Mutation** | `libs/resilient-http-client` (`just mutate`) | whether the unit tests of the pure decision logic would notice a wrong comparison, operator or increment | — |
| **Load** | `benchmarks/` (k6) | latency / error thresholds under load; runs on the load stand and reports there, outside Allure | — |

When you add a behaviour, put its test at the lowest layer that can observe
it, and only there. If a higher layer needs it as a precondition, it waits for
it (the e2e harness waits for `/readyz`), it does not assert it again.

### Harness: `libs/testx`

- `testx.Run(t, func(t testx.T) { … }, tags...)` makes a plain test an Allure
  test; `testo.Suite[testx.T]` with `testo.RunSuite(t, new(Suite), testx.Options(tags...)...)`
  is the suite form (titles, steps, attachments, parametrised `Cases*`).
  `testx.T` is a `testing.TB`, so testify keeps working; sub-tests are
  `testo.Run` / `testx.Step`, never `t.Run`.
- `testx.Postgres(t)`, `testx.Valkey(t)`, `testx.Kafka(t)` start one
  container per test binary (retrying the start), skip locally without Docker
  and fail when `CI` is set; `func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }`
  terminates them. Tests isolate through `testx.Unique(prefix)` — a table, a
  key prefix, a topic, a group — never through a fresh container. This is what
  took the integration layer from ~2 minutes to ~50 seconds.
- `testx.Recorder(t)` (real SDK, in-memory spans), `testx.LogBuffer` (real
  logger, in memory), `testx.Metric` / `testx.LintMetrics` (real registry).
- Resources that must outlive an Allure step (containers, clients, servers)
  are created on the test's own `t`: a step is a sub-test and its `Cleanup`
  runs when the step returns.

```sh
export DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock   # OrbStack / rootless Docker
just test                     # every Go layer (integration needs Docker)
just e2e                      # Playwright against real binaries
just allure-report            # one HTML report from every layer's allure-results/
```

### Allure

Go suites (testo + testo-allure through `testx`) and Playwright
(`allure-playwright`) write to `allure-results/` next to the package, or to
`ALLURE_RESULTS_DIR` — CI points every job at one directory and publishes the
raw results (for an Allure server / TestOps) plus a single-file HTML report
(`allure-report` job; locally `just allure-report`, `allure-commandline` from
the root `package.json`, JRE pinned in `mise.toml`). Tags name the module and
the layer (`pgx`, `integration`, `telemetry`, `e2e`), so the report can be
sliced by either.

### Coverage: one number, three layers

`scripts/cover.sh` collects each layer in Go's binary coverage format
(`-test.gocoverdir` for tests, `GOCOVERDIR` for the cover-instrumented
binaries the e2e harness spawns; every workspace package instrumented via
`-coverpkg`) and `go tool covdata merge` unions them — counters summed per
block, nothing counted twice. `just cov-check` gates the merged profile with
[`.testcoverage.yml`](.testcoverage.yml); `just cov-all` collects all three
layers first. Both pipelines do the same: the test jobs upload `.cover/<layer>`,
the `coverage` job merges and gates and prints the per-layer and merged totals.

| Layer | Alone |
|---|---|
| unit | ~70 % |
| integration | ~86 % |
| e2e | ~74 % of the packages it touches |
| **merged** | **~87 %** (gate: 70 %, ratchet up) |

`main.go` and wiring are covered by e2e, the libs by unit + integration, the
service internals by all three — which is the point of merging.

Two gates, both on the merged profile: the absolute total in
`.testcoverage.yml`, and **no regression** — the `coverage` job keeps
master's per-package breakdown (`coverage-breakdown.json`, a cache on GitHub,
the master pipeline's artifact on GitLab) and a pull request is compared to it
with `--diff-threshold 0`. Old code's coverage cannot pay for new code's.
Every layer is collected with `-covermode=atomic`: `covdata` refuses to merge
mixed modes, and `-race` switches a run to atomic silently, so the mode is
pinned rather than inferred, and `scripts/cover.sh merge` fails rather than
gate on a partial merge.

### Flakiness policy

- CI runs every Go test with `-shuffle=on` and `TZ=UTC` (the seed is printed
  on failure); Playwright runs with `retries: 0`. A flake is reported, never
  hidden behind a retry.
- The nightly `stress` job runs every layer three times under `-race`,
  shuffled. A test that passes there and fails on a PR is a real flake: mark it
  (`t.Flaky()` in an Allure suite) with a ticket, fix or delete it — do not
  raise the retry count.
- Time-based tests inject the clock (the circuit breaker's `nowMS`, the
  cache's `cacheNow`) or observe state (`waitQueued`, `require.Eventually`);
  a `time.Sleep` in a test is a review finding unless it *is* the behaviour
  under test (a slow upstream in the coalescing test).

### Fuzzing

Everything that parses bytes from outside the process has a `Fuzz*` target:
`problem+json` rendering, the YAML config loader, the retry jitter, the
circuit-breaker state machine. `just fuzz` runs each for a budget locally; the
nightly `fuzz` job does the same in CI. A crash writes its input to
`testdata/fuzz/<Target>/` — it is committed and runs as a plain regression
test from then on (`libs/httpx/testdata/fuzz/FuzzProblemJSON` is the first
one: an out-of-range status used to reach `WriteHeader` and panic; it now
degrades to a 500 problem).

### Contracts

The tasks HTTP API and the Kafka events are written once, in
[TypeSpec](https://typespec.io) under [`api/tsp`](api/tsp) (the same
toolchain as `tracehub-spec`, TypeSpec 1.16, OpenAPI 3.0 output), and
everything else is generated from it:

```
api/tsp/tasks.tsp   ─tsp compile─▶ api/openapi3/tasks.openapi.yaml ─oapi-codegen─▶ libs/contracts/tasksapi   (Go types)
api/tsp/events.tsp  ─tsp compile─▶ api/jsonschema/TaskCreatedEvent.json ─go-jsonschema─▶ libs/contracts/events (Go types)
```

`just contracts` regenerates all of it; the outputs are committed, so a
reviewer sees the contract diff next to the code diff. `just contracts-check`
(the `contracts` CI job, part of `just ci`) fails when the committed outputs
are stale and, on a pull request, when `oasdiff` finds a breaking change
against master's OpenAPI document. A change that is breaking by the rules but
safe in practice is waived in `api/oasdiff-breaking.ignore`, one line per
change with the reason and the removal trigger — the same discipline as the
CVE waivers.

The generated types are the wire types: `services/tasks` binds
`tasksapi.CreateTaskRequest`, answers `tasksapi.Task` / `TaskList`, publishes
`events.TaskCreatedEvent`; `services/consumer` decodes the same
`events.TaskCreatedEvent`. The former hand-copied event struct is gone.

Tests enforce it: every exchange in `services/tasks/internal/api`'s tests goes
through `testx.LoadOpenAPI(...).Validate` (request matched an operation,
response status / headers / body conform, a request the contract rejects was
answered with a 4xx problem), and the published event bytes are checked with
`testx.LoadJSONSchema(...).Validate`. A handler that drifts from the contract
fails its own test.

Event compatibility rule (no tool checks JSON Schema evolution): a field may
be added as optional; a field is never removed or changed in type. Bump the
schema `$id` for anything else and keep both consumers running.

Both HTTP services have a contract (`api/tsp/tasks.tsp`, `api/tsp/ping.tsp`),
one OpenAPI document each.

### Chaos

The resilience code paths — `httpx.Optional()` readiness, the cache-aside
fall-through, best-effort publishing, timeouts — were designed for failing
dependencies but, until the chaos suite, never saw one. `libs/testx` now
provides two primitives on the package's shared containers: `testx.Proxied`
puts [Toxiproxy](https://github.com/Shopify/toxiproxy) in front of Postgres
or Valkey (`Down()`, `Up()`, `Latency(d)`, `Reset()`), and `testx.Pause`
freezes a container with SIGSTOP (Kafka's advertised listeners let a client
bypass a proxy, so it is frozen instead). The `ChaosSuite` scenarios: Valkey
unreachable, Postgres slow beyond the check timeout then down, Kafka hung.
Every scenario restores the dependency on cleanup.

Its first run found three real defects, all in the "designed for, never
tested" category: a refused Valkey made `GET /tasks/{id}` hang forever
(valkey-go retries a refused connection until the caller's context ends, and
a request has none — `VALKEY_OP_TIMEOUT`, 500ms, now bounds every command);
a hung Kafka held `POST /tasks` for the life of the request
(`KAFKA_PUBLISH_TIMEOUT`, 5s, bounds a best-effort publish); and a slow
Postgres turned every readiness probe into a 2-second inline refresh because
the staleness window did not account for a check running to its timeout.

### TestOps metadata

Every suite carries the identity an Allure TestOps needs: `Epic`, `Feature`
and `Owner` on the suite (`testx.Meta`), the case id, the `Story` and a TMS
link on each test (`testx.Case(t, "GB-101", "…")`), bare ids turned into links
by `testx.LinkTransformer`. **The values are samples.** This repository is a
template: `golang-basics` / `tasks API` / `@team-platform` / `GB-<n>` and the
`*.example.internal` hosts show the shape; replace them with your project's
tree, owner handles, case ids and TestOps / tracker URLs. Two rules survive
the replacement: an id appears in exactly one test, and a test without an id
is not in the test plan (`-allure.invert` runs everything else).

### Benchmarks

`bench_test.go` files run nightly (`bench` job) with enough repetitions for
[benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat); master's
numbers are kept and the comparison is printed on the run page. It is
informational — shared runners are too noisy for a hard gate — but a
regression there is a finding. Locally: `just bench`, `just bench-compare`.

### Schemathesis

[Schemathesis](https://schemathesis.io) (the validator `tracehub-spec` targets
its OpenAPI 3.0 output at) is the generative layer: it reads a service's
OpenAPI document and drives the *real binary* with requests it derives from
the schema — boundary values, invalid bodies, unknown methods, stateful
create → get → delete chains — checking that nothing answers 5xx and that
every response's status, headers and body are in the contract.

```sh
just schemathesis ping          # dependency-free
just infra-up && just schemathesis tasks
```

It runs from its pinned image (`SCHEMATHESIS_VERSION` in `mise.toml`,
`DOCKER_HUB` for a closed network), reports natively to Allure, and is a job
in both pipelines (`schemathesis`, in the gate). Its first run found that an
undocumented method got a 404 instead of a 405; `httpx` now answers 404/405
as problem+json. With it in place, hand-written "bad input → 4xx" tests were
removed — the schema and the generator own that class of case.

### Test hygiene, linted

`golangci-lint` runs `testifylint`, `thelper`, `tparallel` and `usetesting`
on the test files, so the conventions above are checked, not remembered.

### Mutation testing

Coverage says a line ran; mutation testing says a test would notice if it
were wrong. [gremlins](https://github.com/go-gremlins/gremlins) mutates the
code (flips a comparison, an arithmetic operator, an increment), re-runs the
tests, and reports every mutant that *lived*. It is worth its cost on small,
pure, decision-heavy code and noise elsewhere — so it is scoped, not global:

```sh
just mutate                             # libs/resilient-http-client (default)
just mutate libs/resilient-http-client  # backoff.go, circuitbreaker.go, adaptive.go
```

Scope and thresholds live in the module's [`.gremlins.yaml`](libs/resilient-http-client/.gremlins.yaml);
the run fails below them. `libs/resilient-http-client/mutation_test.go` is the
worked example: each test names the mutant that survived before it existed.
Two findings were code, not tests: redundant guards in `FullJitter` and clamps
in `NewAdaptiveLimiter` produced unkillable mutants and were rewritten with
`min`/`max`, and the breaker clock became injectable so those tests no longer
sleep. CI runs it nightly and on demand (`mutation` job), never per PR. The run
must not hit the `go test` cache (`GOFLAGS=-count=1`, the recipe sets it): the
per-mutant timeout is derived from the initial coverage run.

---

## Closed networks (proxies, mirrors, no direct internet)

This repo is meant to be the template for projects on a self-hosted GitLab
behind a corporate proxy, so **every upstream it touches is a variable with the
public default**. An open-network clone needs no configuration; a closed one
sets the same names in three places and edits nothing else:

| Where                | How                                                                    |
| -------------------- | ---------------------------------------------------------------------- |
| local shell / `just` | `cp .env.example .env` and uncomment what you need (`just` loads it via `set dotenv-load`, mise via `[env] _.file`) |
| GitLab               | group- or project-level CI/CD variables with the same names            |
| GitHub               | repository / organisation *variables* (`vars.*`) with the same names   |

[`.env.example`](.env.example) documents every knob with an example value. The
short list of what a closed network has to mirror, and which variable points at
it:

| Upstream                                   | Variable(s)                                                   | Notes |
| ------------------------------------------ | ------------------------------------------------------------- | ----- |
| proxy.golang.org / sum.golang.org          | `GOPROXY`, `GONOSUMDB` (or `GOSUMDB=off`)                     | Athens / Nexus / Artifactory proxy the sumdb through `GOPROXY`; GitLab's Go proxy does not — use `GONOSUMDB` |
| Docker Hub, ghcr.io, gcr.io                | `DOCKER_HUB`, `GHCR`, `GCR`                                   | image prefix in every Dockerfile (`--build-arg`), compose file and CI `image:`; `.hadolint.yaml` `trustedRegistries` lists the allowed hosts |
| Docker Hub (testcontainers)                | `TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX`                        | trailing slash; postgres / valkey / confluent-local in the integration suites |
| vuln.go.dev                                | `GOVULNDB`                                                    | static site — mirror over HTTP or `file://` |
| grype DB                                   | `GRYPE_DB_UPDATE_URL`, `GRYPE_DB_AUTO_UPDATE=false`           | or `grype db import` a tarball |
| api.osv.dev                                | `OSV_SCANNER_FLAGS="--offline --local-db-path …"`             | fetch the DB once with `--download-offline-databases` on a connected host |
| semgrep.dev rule packs                     | `SEMGREP_CONFIG=semgrep/rules`                                | vendor the packs (`semgrep --config p/golang --dump-config`); the recipes already pass `--metrics=off` |
| registry.npmjs.org                         | `registry=` in `.npmrc`, `COREPACK_NPM_REGISTRY`              | corepack fetches pnpm before `.npmrc` is read |
| nodejs.org/dist                            | `NODE_DIST_URL`                                               | the GitLab `allure-report` job runs on a JRE image and adds Node from a tarball |
| GitHub Releases / PyPI (mise-installed tools) | `HTTPS_PROXY`, `MISE_GITHUB_API_TOKEN`, `PIP_INDEX_URL`    | with no egress at all, run those tools as the pinned images instead — exactly what `.gitlab-ci.yml` does |

Design rules that keep it that way:

- No `# syntax=docker/dockerfile:…` directive in the Dockerfiles — it makes
  BuildKit pull the frontend image on every build.
- No CI job installs packages (`apt`, `curl | sh`, nodesource); tools are
  either in the pinned job image or built once from the Go module proxy. The
  one tarball fetch (Node for `allure-report`) comes from `NODE_DIST_URL`.
- `--build-arg X` with no value forwards `X` from the environment and is
  skipped when unset, so proxy / registry settings never appear in a
  Dockerfile or a compose file — only their defaults do.
- `.env` is gitignored and `.dockerignore`d; only `.env.example` is committed.

---

## Worktrees (multi-branch dev)

This repo uses a **bare-repo container** so each branch is a clean sibling
checkout — don't nest worktrees inside a live checkout, or tooling will scan
every branch's build output.

```sh
# one-time container
git clone --bare git@github.com:tracehubmmp/golang-basics.git golang-basics/.bare
cd golang-basics && echo 'gitdir: ./.bare' > .git
git --git-dir=.bare config remote.origin.fetch '+refs/heads/*:refs/remotes/origin/*'
git fetch origin
git worktree add master master

# per branch — the `wt` helper wraps the extra setup:
./wt add feat/x          # worktree + mise trust + go work sync + pnpm install
./wt list
./wt rm  feat/x
```

What `git worktree add` does **not** do, and `wt` does:

- `mise trust` the new worktree (else mise-shimmed tools fail with a misleading
  "error parsing config file");
- `go work sync` to wire up the workspace module set;
- `pnpm install` in `e2e/`.

**Build cache.** Go's `GOCACHE` is global and content-addressed, so build/test
reuse across worktrees is automatic — no per-worktree cache wiring (unlike the
Rust sibling repo's sccache). **Docker** `docker/deps.yml` is a singleton (fixed
project name `golang-basics-deps` + host ports) — run one deps stack and every
worktree reaches it at `localhost:<port>`.

---

## Toolchain notes

- **Go**: module directives target `go 1.27.0`; the toolchain is pinned to the
  latest `1.27.x` in `mise.toml`. golangci-lint 2.13.2 is itself built with
  go1.27.0, so the linter and the module target move together.
- **just** drives everything (language-agnostic, same as the Rust sibling).
- **gin** for HTTP, **log/slog** for logging, **prometheus/client_golang** for
  metrics, **testify** for assertions, **caarlos0/env** for config.
- Data libs use the actively-maintained, pure-Go drivers: **jackc/pgx**
  (Postgres), **valkey-io/valkey-go** (Valkey), **twmb/franz-go** (Kafka) and
  **go.opentelemetry.io/otel** (tracing).
- **testcontainers-go** backs the integration suites — `just <module> test`
  spins up real Postgres/Valkey/Kafka, so those tests need a Docker daemon (they
  skip under `-short`).

## VS Code

The Go extension normally `go install`s its helper binaries into `GOPATH/bin` at
whatever version is latest that day, per machine. They are pinned in
`mise.toml` instead, so every clone and worktree resolves the same ones:

| Tool | Version | What the extension uses it for |
|---|---|---|
| `gopls` | 0.23.0 | language server — completion, go-to-def, diagnostics, refactors |
| `dlv` | 1.27.1 | debugger — F5, breakpoints, debug-a-single-test |
| `gotests` | 1.9.0 | *Go: Generate Unit Tests* |
| `gomodifytags` | 1.17.0 | *Go: Add/Remove Struct Tags* |
| `impl` | 1.5.0 | *Go: Generate Interface Stubs* |

They use mise's `go:` backend (`go install` from source against the pinned Go),
so the **first** `mise install` after cloning takes a few minutes; after that
they are cached like any other tool.

`goplay` (*Go: Run on Go Playground*) is deliberately not pinned — its only
release is v1.0.0 from 2016, which predates Go modules and has no `go.mod`, so
`go install …@v1.0.0` cannot resolve it.

Copy the editor config in [`.vscode-example/`](.vscode-example/) to make the
extension actually use those pins:

```sh
cp .vscode-example/settings.json .vscode/
cp .vscode-example/launch.json   .vscode/
cp .vscode-example/tasks.json    .vscode/
```

`.vscode/extensions.json` is already committed (the only file `.gitignore`
un-ignores under `.vscode/`), so the recommended-extensions prompt works without
copying anything.

`settings.json` wires `go.alternateTools` to the mise **shims** and turns
`go.toolsManagement.autoUpdate` off, so the extension stops installing its own.
Shims resolve the version from the `mise.toml` of whatever directory they run
in, which is what makes a worktree on a different pin get the right binary — run
`mise install` once per clone, and `mise trust` in a fresh worktree or the shims
fail with a misleading "error parsing config file".

Linting is wired to `golangci-lint` with the repo's `.golangci.yml`, matching
`just lint` and CI. gopls' own staticcheck is off because the `standard` set
already includes it and both would report the same finding twice.

`launch.json` debugs each of the four services against the local dependency
stack — start it with `just infra-up` first (`just stack-up` would also run the
services in containers and fight for the same host ports). `tasks.json` maps
Terminal → Run Task… onto the `just` recipes.

See [`CLAUDE.md`](CLAUDE.md) for the high-signal, easy-to-miss bits and
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the dev workflow.
