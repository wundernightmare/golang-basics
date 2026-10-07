# golang-basics

A small, idiomatic **Go monorepo** modelled on the Rust `tracehub-edge`
workspace: a `go.work` workspace of service and library modules (one crate =
one module), a `just`-driven build / test / lint / security / CI surface, one
parametrised distroless Docker image recipe, a Go e2e suite and k6 load tests.
A learning / template scaffold, wired the way a real Go monorepo would be.

Two layers: **dependency-free** building blocks (`ping`, `heartbeat`, `httpx`,
`resilient-http-client`) and a **data-services** vertical — `tasks` (Postgres +
Valkey + Kafka through a transactional outbox, OpenTelemetry, RFC 9457
errors) and `consumer` (retries, dead-letter topic) on the `pgx` / `valkey` /
`kafka` / `otelx` libs. It lives in a **bare-repo worktree container** (see
[Worktrees](#worktrees-multi-branch-dev)); [`CLAUDE.md`](CLAUDE.md) holds the
easy-to-miss conventions, [`CONTRIBUTING.md`](CONTRIBUTING.md) the workflow.

## Modules

15 Go modules in `go.work` (14 + the e2e suite) plus the TypeSpec contracts:

| Module | Kind | Port(s) | One-liner |
| --- | --- | --- | --- |
| [`services/ping`](services/ping) | HTTP service | `:8080` API, `:9080` admin | `GET /ping` → `pong`, `?msg=` echo. |
| [`services/heartbeat`](services/heartbeat) | Worker | `:9081` admin | Ticker worker — a beat + `heartbeat_beats_total` every interval. |
| [`services/tasks`](services/tasks) | HTTP service | `:8082` API, `:9082` admin | Tasks CRUD over **Postgres + Valkey**; `task.created` goes through a **transactional outbox** (row + event in one transaction, a relay publishes to Kafka); traced end to end, `problem+json` errors. |
| [`services/consumer`](services/consumer) | Worker | `:9083` admin | Drains `tasks.events`: continues the producer's trace, idempotent by `event_id`, retries with backoff, dead-letters to `tasks.events.dlq`; exports group lag. |
| [`api`](api) | Contracts | — | TypeSpec source of the ping + tasks HTTP APIs and the Kafka events → OpenAPI 3 + JSON Schema (committed). Node lives only here. |
| [`libs/contracts`](libs/contracts) | Library | — | Generated Go types of those contracts (`pingapi`, `tasksapi`, `events`). Never edited by hand. |
| [`libs/httpx`](libs/httpx) | Library | — | **net/http** (Go 1.22 ServeMux, no framework) server scaffolding: request ids, tracing, sampled trace-correlated logs, RED metrics (`route` label), admin listener (health / metrics / version / config / runtime log level / pprof, token-guarded), graceful shutdown, env + YAML config, RFC 9457 `Problem`. |
| [`libs/resilient-http-client`](libs/resilient-http-client) | Library | — | Policy-per-target **outbound** client: rate limit, circuit breaker, adaptive concurrency, jittered retry, cache, coalescing, fallbacks, metrics. |
| [`libs/pgx`](libs/pgx) | Library | — | PostgreSQL pool (`jackc/pgx`): config, readiness, migrations, pool metrics. |
| [`libs/valkey`](libs/valkey) | Library | — | Valkey cache (`valkey-go`): get/set/del, readiness, cache-aside helper. |
| [`libs/kafka`](libs/kafka) | Library | — | Kafka (`franz-go`): producer, at-least-once consumer group, TLS / SASL, lag metrics; no auto topic creation unless allowed. |
| [`libs/otelx`](libs/otelx) | Library | — | OpenTelemetry tracing: OTLP over HTTP (TLS by default), W3C propagation; opt-in. |
| [`libs/testx`](libs/testx) | Test helpers | — | `_test`-only, no Docker: span recorder, log buffer, metric helpers, unique names. |
| [`libs/testx/containers`](libs/testx/containers) | Test helpers | — | `_test`-only: one shared Postgres / Valkey / Kafka per test binary (testcontainers), Toxiproxy chaos. A module needs Docker exactly when it requires this. |
| [`libs/testx/contract`](libs/testx/contract) | Test helpers | — | `_test`-only: validates exchanges against the OpenAPI / JSON Schema documents. |
| [`e2e`](e2e) | Test suite | — | Real binaries over HTTP, build tag `e2e` (see "E2E tests"). |

`services/* → libs/*`. Every service — server or worker — gets its **admin
listener** from `httpx` (`/healthz` = `/livez`, `/readyz`, `/metrics`,
`/version`, `/admin/*`, `/debug/pprof`; API port + 1000), so the API port never
carries operational routes. A non-loopback admin address needs
`*_ADMIN_TOKEN` (or `*_ADMIN_INSECURE=true`, what the local compose stack
sets); `just up` / `run` bind it to `127.0.0.1`.

## Layout

```
golang-basics/                 ← bare-repo CONTAINER (.bare + .git pointer + wt + CLAUDE.md)
└── master/                    ← canonical worktree (this tree)
    ├── go.work                ← the workspace: every module (the one module list)
    ├── justfile               ← workspace task runner (+ a justfile per module)
    ├── mise.toml              ← core toolchain pins; mise.{appsec,ide,perf,report}.toml = opt-in profiles
    ├── api/                   ← TypeSpec contracts (→ api/openapi3, api/jsonschema)
    ├── libs/                  ← contracts, httpx, resilient-http-client, pgx, valkey, kafka, otelx, testx{,/containers,/contract}
    ├── services/              ← ping, heartbeat, tasks, consumer (main.go + internal/…)
    ├── e2e/                   ← Go e2e suite (build tag e2e)
    ├── docker/                ← service.Dockerfile, deps.yml, stack*.yml, observability*.{yml,/}
    ├── benchmarks/            ← k6 load tests
    └── scripts/               ← see below
```

`scripts/`: `touched-modules.sh` (module lists from go.work — `--all`,
`--integration`, `--services`, `--mutation`, or files → modules),
`mise-pins.sh` (every tool pin for CI), `build-service.sh` (the release build
line, `COVER=1` for e2e), `cover.sh` (coverage layers + merge),
`check-waivers.sh` (expired waivers), `host-services-spawn.sh` (`just up`),
`schemathesis.sh`, `fuzz.sh`, `bench.sh`, `allure-meta.sh`.

## Quick start

```sh
mise trust --all && just setup     # core toolchain + pnpm workspace + go work sync
just ci                            # fmt-check → tidy-check → vet → lint → test-short

just up                            # ping :8080 (admin 127.0.0.1:9080) + heartbeat (admin :9081)
curl -s localhost:8080/ping | jq . && just down

just infra-up                      # Postgres + Valkey + Redpanda (docker/deps.yml, --wait)
just mod services/tasks run &      # tasks :8082 (admin 127.0.0.1:9082)
just mod services/consumer run &   # consumer (admin 127.0.0.1:9083)
curl -s -XPOST localhost:8082/tasks -d '{"title":"hello"}' | jq .
just stack-up                      # …or all four services as containers, waiting for /readyz

just e2e                           # Go e2e suite (data flow too while infra is up)
just bench-smoke                   # k6 against ping (perf profile)
```

## Common workspace commands

```sh
just vet | build | test | test-short | test-race    # every module
just test-integration     # Docker-backed modules (require libs/testx/containers), -race
just test-junit -short    # gotestsum → .reports/junit/<module>.xml; then `just allure`
just lint                 # golangci-lint every module + shellcheck (lint-sh)
just fmt | fmt-check      # gofmt over the module dirs
just tidy | tidy-check    # go mod tidy; the gate runs it per module with GOWORK=off
just ci | ci-full         # the pure-Go gate | + race tests + govulncheck
just contracts | contracts-check   # TypeSpec → OpenAPI/JSON Schema → Go; the CI gate
just release              # stamped release binaries (services/<svc>/bin)
just mod services/tasks test       # any recipe of one module's justfile
just each lint            # a recipe in every module's justfile
just infra-up | infra-down | stack-up | stack-down | obs-up
just setup-appsec | setup-ide | setup-perf | setup-report   # opt-in tool profiles
```

Profiles are mise's environment config files (`MISE_ENV=appsec mise install`
= `just setup-appsec`); recipes that need one select it themselves. Details
in [CONTRIBUTING](CONTRIBUTING.md#dev-setup).

## E2E tests (Go)

A Go module ([`e2e/`](e2e)) that spawns the real service binaries and talks to
them over HTTP — plain `go test` + testify, behind the `e2e` build tag (so
`go test ./...` without the tag runs nothing). Each service gets free loopback
ports picked by the harness, its output is captured and printed when a test
fails, the harness waits for `/readyz` on the admin port and checks that
`/version` names the service it started, and teardown is SIGTERM + wait so a
cover-built binary flushes its counters. See [`e2e/README.md`](e2e/README.md).

```sh
# binaries into .build/ (cover-instrumented, as CI builds them)
for s in ping heartbeat tasks consumer; do COVER=1 scripts/build-service.sh $s .build/$s; done

cd e2e && go test -tags e2e -count=1 ./...          # ping + heartbeat; tasks/consumer skip

# + the tasks → Kafka → consumer flow (needs `just infra-up`)
E2E_DATABASE_URL='postgres://app:app@localhost:5432/app?sslmode=disable' \
E2E_VALKEY_URL=valkey://localhost:6379 E2E_KAFKA_BROKERS=localhost:9092 \
  go test -tags e2e -count=1 ./...
```

`E2E_BIN_DIR` (default `../.build`) says where the binaries are,
`E2E_COVER_DIR` makes every child write coverage there (`GOCOVERDIR`).

## Benchmarks (k6)

Profiles mirror the Rust sibling repo; k6 comes from the **perf** profile
(`just setup-perf`). See [`benchmarks/README.md`](benchmarks/README.md).

```sh
just bench-smoke | bench-load | bench-stress | bench-soak | bench-peak   # ping
just bench-tasks smoke                                                  # tasks (needs `just infra-up`)
just bench out.txt && just bench-compare base.txt out.txt               # Go micro-benchmarks + benchstat
```

## Security tooling

The scanners are the **appsec** profile (`just setup-appsec`); CI runs the
same versions from their pinned images.

| Recipe | Tool | Config | Covers |
| --- | --- | --- | --- |
| `just sec-waivers` | `scripts/check-waivers.sh` | — | any `Remove after YYYY-MM-DD` waiver whose date has passed |
| `just sec-secrets` | gitleaks | `.gitleaks.toml` | secrets in tree + history |
| `just sec-sast` | semgrep | `.semgrepignore` | `p/owasp-top-ten` + `p/golang` (`--metrics=off`) |
| `just sec-deps` | osv-scanner | `osv-scanner.toml` | OSV advisories over every `go.mod` + `pnpm-lock.yaml` |
| `just sec-iac` | hadolint | `.hadolint.yaml` | `docker/service.Dockerfile` |
| `just audit` | govulncheck | — | reachable Go vulnerabilities per module |
| `just sec` | — | — | the five source-side checks, fail-fast |

Images: `just docker-build ping`, then `docker-scan` (syft SBOM + grype),
`docker-scan-ci` (`--fail-on high`), `docker-sign` / `docker-verify` (cosign
key mode, no Rekor). The `sast` job also guards the pipelines: SHA-pinned
`uses:` (Dependabot bumps them), no `${{ github.* }}` in `run:` text, and the
pnpm supply-chain settings (`minimumReleaseAge`, `blockExoticSubdeps`,
`trustPolicy`).

## CI

Two pipelines kept at parity: `.github/workflows/{ci,appsec,docker}.yml` and
`.gitlab-ci.yml`. **No module list, tool version or host is written in
either.** A `versions` job runs `scripts/mise-pins.sh` (every pin of
`mise.toml` + the profile files → step outputs / a GitLab dotenv, used even
in `image:`); module sets come from `scripts/touched-modules.sh` — GitHub fans
them out as matrices, GitLab loops over them in one job per stage.

| Job | What |
| --- | --- |
| `static` / `lint` | `just fmt-check tidy-check`, shellcheck (pinned image), `go vet` + golangci-lint per module (`-tags e2e`) |
| `contracts` | `just contracts-check` — the only job with Node |
| unit (`lint-test` / `unit`) | `-short -race`, JUnit via gotestsum, binary coverage into `.cover/unit` |
| `integration` | modules requiring `libs/testx/containers`, `-race` (GitLab: dind) |
| `e2e` | `-cover` binaries into `.build/`, deps up (`docker compose … up --wait` on GitHub, `services:` on GitLab — a dependency that never gets healthy fails the job), `go test -tags e2e`, counters → `.cover/e2e` |
| `schemathesis` | generated requests against ping + tasks, JUnit |
| `coverage` | covdata merge of unit + integration + e2e, gated by `.testcoverage.yml`; a PR may not lower it vs master |
| `allure-report` | one HTML report from every JUnit file (`allure generate`, JRE) |
| appsec | waivers, gitleaks, semgrep, osv-scanner, govulncheck, hadolint |
| docker | every service through `docker/service.Dockerfile`, syft SBOM, grype `--fail-on high` |
| nightly | `stress` (`-count=3 -race`), `fuzz`, `bench` (benchstat vs master), `mutation` (modules with `.gremlins.yaml`) |

GitLab: `prepare` warms the Go caches (under `.cache/`) and builds the tools
once, `needs:` makes a DAG, docs-only MRs skip the Go jobs, JUnit + Cobertura
feed the MR widget. The dind jobs need a privileged runner (the file header
documents a rootless BuildKit alternative). Locally: `npx gitlab-ci-local <job>`.

## Docker

One recipe for every service: [`docker/service.Dockerfile`](docker/service.Dockerfile)
(`ARG SERVICE`). Multi-stage, static `CGO_ENABLED=0` binary on distroless
`static-debian12:nonroot` (uid 65532, no shell, no healthcheck — probes are the
orchestrator's job), `-trimpath -s -w`, Version / Revision / BuildTime stamped
from `VERSION` / `GIT_SHA` / `BUILD_TIME`, BuildKit cache mounts, base images
behind `DOCKER_HUB` / `GCR` and pinned by digest. `GO_VERSION` is required and
always the mise pin. The context is the workspace root, filtered by the
`.dockerignore` allowlist; only `go.work*`, `libs/` and `services/<SERVICE>/`
are copied.

```sh
just docker-build ping           # = docker build -f docker/service.Dockerfile --build-arg SERVICE=ping --build-arg GO_VERSION=<pin> …
just stack-up                    # deps + all four images (docker/stack.yml), up --wait on every /readyz
curl -s -XPOST localhost:8082/tasks -d '{"title":"hi"}' | jq .
just stack-down
```

`docker/deps.yml` (Postgres, Valkey, Redpanda — `localhost:9092` for host
processes, `redpanda:29092` for containers) is a singleton shared by every
worktree. Every compose file publishes on `127.0.0.1` only.

## Observability

| Signal | Where | What |
| --- | --- | --- |
| Logs | stdout, JSON | `service`, `trace_id`/`span_id`, `request_id`, one access-log line per request (`route`, `status`, `latency_ms`); debug/info sampled (`*_LOG_SAMPLE_*`), warn/error never dropped |
| Traces | OTLP over **HTTP** to `*_OTEL_EXPORTER_OTLP_ENDPOINT` (TLS unless `*_OTEL_EXPORTER_OTLP_INSECURE`) | server span → pgx / Valkey spans → outbox relay publish; record headers carry the context, so the consumer's span continues the trace |
| Metrics | `/metrics` on the admin port — Prometheus only, nothing pushed | `build_info`, RED per `route`, `health_check_up`, `pgxpool_*`, `cache_lookups_total`, `kafka_producer_*` / `kafka_consumer_*` incl. `kafka_consumer_group_lag`, outbox backlog |
| Profiles | `/debug/pprof/` on the admin port | for a runtime agent or `go tool pprof` |
| Identity / debugging | admin port | `/version` (version, revision, build time), `PUT /admin/log-level?level=debug&ttl=30m`, `GET /admin/config` (redacted), `X-Debug-Token` per request — see [`libs/httpx`](libs/httpx#runtime-debugging) |

`docker/observability.yml` is an optional local receiving end shaped like
production: an OpenTelemetry Collector (OTLP/HTTP `:4318`, traces → Jaeger;
no metrics / logs pipeline), Jaeger, VictoriaMetrics scraping the admin ports
and the dependency exporters, **vmalert** with starter rules
([`docker/observability/rules.yml`](docker/observability/rules.yml): 5xx
ratio, p99 latency, target / readiness down, consumer lag, outbox backlog,
pool saturation), Loki fed by Vector from container stdout, and Grafana with
the datasources cross-linked (span ⇄ log lines) and one dashboard.

```sh
just obs-up          # needs infra-up; Grafana http://localhost:3000 (admin/admin), Jaeger :16686, vmalert :8880
just stack-up-otel   # the four services in containers, exporting to otel-collector:4318
TASKS_OTEL_ENABLED=true TASKS_OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318 \
  TASKS_OTEL_EXPORTER_OTLP_INSECURE=true just mod services/tasks run   # a host process instead
```

Tracing is opt-in (`*_OTEL_ENABLED`); unset, `otelx` installs only the W3C
propagators and a no-op provider.

## Tests

Tests are plain `go test`: `func TestX(t *testing.T)`, testify, `t.Run`
sub-tests, table tests, `t.Parallel()` where nothing swaps a global. No test
framework and no per-test report plugin — the Allure report is built from the
JUnit XML that `gotestsum` writes. Every layer feeds one merged coverage
number, and every behaviour is checked at exactly one layer.

### Layers and what each one owns

| Layer | Where | Owns | Does not repeat |
|---|---|---|---|
| **Unit** | `*_test.go` next to the code, `-short` | pure logic, config parsing, handlers behind small interfaces with fakes, readiness cache semantics, log sampling arithmetic, route ↔ listener wiring | anything that needs a real dependency |
| **Contract** | telemetry: `libs/otelx/telemetry_test.go`, `*/metrics_test.go`; API: exchanges validated with `libs/testx/contract` | the access-log line, the span and the counters describe the same request; every exposition is promlint-clean; every request/response and every published event conforms to the TypeSpec-generated OpenAPI / JSON Schema | route behaviour (unit), real dependencies (integration) |
| **Integration** | `libs/{pgx,valkey,kafka}`, `services/tasks/internal/integration`, `services/consumer/internal/worker` — one shared container per test binary | the libs against real Postgres / Valkey / Kafka (spans, trace propagation through the broker, pool / hit-miss / lag metrics); the tasks vertical wired as `main.go` wires it, including the outbox relay | route contracts proven with fakes; process-level behaviour |
| **Chaos** | `services/tasks/internal/integration/chaos_test.go` (`containers.Proxied`, `containers.Pause`) | what the design promises when a dependency fails, slows down or hangs: cache bypassed, outbox catches up, readiness degrades only for the critical dependency, probes stay fast, everything recovers | happy-path behaviour (integration) |
| **E2E** | `e2e/` (Go, real binaries, `-tags e2e`) | what only a real process shows: it boots from env, is ready on its admin listener, `/version` carries the build stamp, the admin surface is token-guarded, SIGTERM exits 0 with "servers stopped cleanly", and tasks → outbox → Kafka → consumer across processes | per-route behaviour, error bodies (unit), the CRUD flow (integration) |
| **Generative** | `just schemathesis <svc>` | "bad input → 4xx problem", no 5xx, every response in the contract's shape | business semantics (unit), effects on dependencies (integration) |
| **Mutation** | `libs/resilient-http-client` (`just mutate`) | whether the unit tests of the pure decision logic notice a wrong comparison or operator | — |
| **Load** | `benchmarks/` (k6) | latency / error thresholds under load; reports on the load stand, outside Allure | — |

Put a behaviour's test at the lowest layer that can observe it, and only
there. A higher layer that needs it as a precondition waits for it (the e2e
harness waits for `/readyz`; asynchronous effects are polled with
`assert.EventuallyWithT`, never a fixed sleep) — it does not assert it again.

### Helpers: three `_test`-only modules

| Module | What | Pulls in |
|---|---|---|
| [`libs/testx`](libs/testx) | `Recorder` (real OTel SDK, in-memory spans), `LogBuffer` (real logger into memory; `Lines`, `Find`), `Metric` / `LintMetrics` (real registry), `Unique(prefix)` | OTel SDK, Prometheus — no Docker |
| [`libs/testx/containers`](libs/testx/containers) | `Postgres(t)`, `Valkey(t)`, `Kafka(t)`: one container per test binary, started on first use, terminated by `func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }`; skipped under `-short` and without Docker locally, **failed** when `CI` is set. Chaos: `Proxied(t, dep)` (Toxiproxy: `Down`, `Up`, `Latency`, `Reset`), `Pause(t, dep)` (SIGSTOP) | testcontainers, Docker client |
| [`libs/testx/contract`](libs/testx/contract) | `LoadOpenAPI(t, rel).Validate(...)` for an HTTP exchange, `LoadJSONSchema(t, rel).Validate(t, doc)` for an event | kin-openapi, JSON Schema validator |

Tests isolate through `testx.Unique` — a schema, a key prefix, a topic, a
consumer group — never through a fresh container. A module needs Docker
exactly when its `go.mod` requires `libs/testx/containers`, which keeps
`ping`, `heartbeat`, `httpx` and `resilient-http-client` Docker-free. A
`depguard` rule in `.golangci.yml` forbids all three modules, testify and
testcontainers in non-test files.

```sh
export DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock   # OrbStack / rootless Docker
just test                     # every Go layer (integration needs Docker)
cd e2e && go test -tags e2e -count=1 ./...   # e2e against .build/ binaries (see "E2E tests (Go)")
just allure-report            # one HTML report from every layer's JUnit XML
```

### Allure (via JUnit)

Every layer runs under [gotestsum](https://github.com/gotestyourself/gotestsum)
with `--junitfile <results-dir>/<layer>-<module>.xml`; Allure 2 reads JUnit XML
natively, so `allure generate <results-dir>` needs no Go-side plugin. A test's
identity in the report is its package and name (`TestTasksPipeline/created_task_reaches_the_consumer`),
so name tests for what they check. Before `allure generate`,
`scripts/allure-meta.sh` drops [`allure/categories.json`](allure/categories.json)
(failure buckets matched on the JUnit failure text — build/setup failure,
infrastructure, product defect, quarantined flake, skipped; first match wins)
and an `executor.json` naming the run next to the XML. CI publishes the raw
results and a single-file HTML report; locally `just allure-report`. See
[`allure/README.md`](allure/README.md).

### Coverage: one number, three layers

`scripts/cover.sh` collects each layer in Go's binary coverage format
(`-test.gocoverdir` for tests, `GOCOVERDIR` for the cover-instrumented
binaries the e2e harness spawns — `E2E_COVER_DIR`; every workspace package
instrumented via `-coverpkg`) and `go tool covdata merge` unions them —
counters summed per block, nothing counted twice. `just cov-check` gates the
merged profile with [`.testcoverage.yml`](.testcoverage.yml): a total floor
(70 %), a per-package floor (50 %), a per-file floor (30 %) and higher
overrides for pure-logic packages (`libs/httpx` 85 %,
`libs/resilient-http-client` 80 %); the test harness modules, `e2e/` and the
generated `libs/contracts` are excluded.

The merge is **strict in CI**: with `CI` set, a layer whose `.cover/<layer>`
directory is missing or empty fails the merge instead of silently lowering the
number. Locally it is lenient and prints a loud warning. `just cov-all` is the
safe local entry point: it wipes `.cover/`, collects all three layers, then
gates. Every layer uses `-covermode=atomic` (covdata cannot merge mixed modes,
and `-race` implies atomic), and the merge fails on any covdata error rather
than gate a partial profile.

Two gates, both on the merged profile: the floors above, and **no
regression** — the `coverage` job keeps master's per-package breakdown
(`coverage-breakdown.json`) and compares a pull request to it with
`--diff-threshold 0`. `main.go` and wiring are covered by e2e, the libs by
unit + integration, the service internals by all three — which is the point of
merging.

### Flakiness policy

- Every Go test runs with `-shuffle=on` and `TZ=UTC` (the seed is printed on
  failure), e2e included; no retry flags anywhere. A flake is reported, never
  hidden behind a retry.
- The nightly `stress` job runs every layer three times under `-race`. A test
  that passes there and fails on a PR is a real flake: `t.Skip("flaky: GB-123 …")`
  with a ticket (the report buckets it as a quarantined flake), then fix or
  delete it.
- Time-based tests inject the clock (the circuit breaker's `nowMS`, the
  cache's `cacheNow`) or observe state (`require.Eventually`); a `time.Sleep`
  in a test is a review finding unless it *is* the behaviour under test.

### Fuzzing

Everything that parses bytes from outside the process has a `Fuzz*` target:
`problem+json` rendering, the YAML config loader, the retry jitter, the
circuit-breaker state machine. `just fuzz` runs each for a budget locally; the
nightly `fuzz` job does the same in CI. A crash writes its input to
`testdata/fuzz/<Target>/` — it is committed and runs as a plain regression
test from then on.

### Contracts

The tasks and ping HTTP APIs and the Kafka events are written once, in
[TypeSpec](https://typespec.io) under [`api/tsp`](api/tsp), and everything
else is generated from it:

```
api/tsp/tasks.tsp   ─tsp compile─▶ api/openapi3/tasks.openapi.yaml ─oapi-codegen─▶ libs/contracts/tasksapi   (Go types)
api/tsp/events.tsp  ─tsp compile─▶ api/jsonschema/TaskCreatedEvent.json ─go-jsonschema─▶ libs/contracts/events (Go types)
```

`just contracts` regenerates all of it; the outputs are committed.
`just contracts-check` (CI `contracts`) fails when they are stale and, on a
pull request, when `oasdiff` finds a breaking change against master (waivers
with reason and removal trigger in `api/oasdiff-breaking.ignore`). The
generated types are the wire types, and tests enforce them: exchanges in
`services/tasks/internal/api`'s tests go through
`contract.LoadOpenAPI(...).Validate`, published event bytes through
`contract.LoadJSONSchema(...).Validate`. Event compatibility rule: add
optional fields only; never remove or retype.

### Chaos

`libs/testx/containers` puts [Toxiproxy](https://github.com/Shopify/toxiproxy)
in front of Postgres or Valkey (`containers.Proxied`: `Down()`, `Up()`,
`Latency(d)`, `Reset()`) and freezes Kafka with SIGSTOP (`containers.Pause`;
its advertised listeners let a client bypass a proxy). The chaos tests in
`services/tasks/internal/integration` cover Valkey unreachable, Postgres slow
beyond the check timeout then down, Kafka hung; each restores the dependency
on cleanup. Their first run found three defects in "designed for, never
tested" code — a refused Valkey hanging `GET /tasks/{id}`, a hung Kafka
holding `POST /tasks`, a slow Postgres turning every readiness probe into an
inline refresh — now bounded by `VALKEY_OP_TIMEOUT`, `KAFKA_PUBLISH_TIMEOUT`
and the readiness staleness window.

### Benchmarks

`bench_test.go` files run nightly (`bench` job) with enough repetitions for
[benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat); master's
numbers are kept and the comparison is printed on the run page. Informational
— shared runners are too noisy for a hard gate. Locally: `just bench`,
`just bench-compare`.

### Schemathesis

[Schemathesis](https://schemathesis.io) reads a service's OpenAPI document and
drives the *real binary* with requests derived from the schema — boundary
values, invalid bodies, unknown methods, stateful create → get chains —
checking that nothing answers 5xx and every response is in the contract.

```sh
just schemathesis ping          # dependency-free
just infra-up && just schemathesis tasks
```

It runs from its pinned image (`SCHEMATHESIS_VERSION` in `mise.toml`,
`DOCKER_HUB` for a closed network) and is a job in both pipelines. With it in
place, hand-written "bad input → 4xx" tests are not written — the schema and
the generator own that class of case.

### Test hygiene, linted

`golangci-lint` runs `testifylint`, `thelper`, `tparallel` and `usetesting`
on the test files and `depguard` on everything else (no test-only module in
production code), so the conventions above are checked, not remembered. The
`lefthook` pre-push hook vets, lints and runs `-short` tests for the touched
modules **and every module that depends on them** (a change to `libs/httpx`
tests the services too).

### Mutation testing

Coverage says a line ran; mutation testing says a test would notice if it
were wrong. [gremlins](https://github.com/go-gremlins/gremlins) mutates the
code, re-runs the tests and reports every mutant that *lived*. It is scoped to
small, pure, decision-heavy code:

```sh
just mutate                             # libs/resilient-http-client (default)
just mutate libs/resilient-http-client  # backoff.go, circuitbreaker.go, adaptive.go
```

Scope and thresholds live in the module's [`.gremlins.yaml`](libs/resilient-http-client/.gremlins.yaml);
the run fails below them. `libs/resilient-http-client/mutation_test.go` is the
worked example: each test names the mutant that survived before it existed.
CI runs it nightly and on demand (`mutation` job), never per PR. The run must
not hit the `go test` cache (`GOFLAGS=-count=1`, the recipe sets it).

## Closed networks (proxies, mirrors, no direct internet)

The repo is the template for projects on a self-hosted GitLab behind a
proxy, so **every upstream is a variable with the public default**. Set the
same names in `.env` (`cp .env.example .env`; loaded by `just` and mise), as
GitLab CI/CD variables, or as GitHub `vars.*`. [`.env.example`](.env.example)
documents each one.

| Upstream | Variable(s) |
| --- | --- |
| proxy.golang.org / sum.golang.org / vuln.go.dev | `GOPROXY`, `GONOSUMDB` (or `GOSUMDB=off`), `GOVULNDB` |
| Docker Hub, ghcr.io, gcr.io (Dockerfile, compose, CI `image:`) | `DOCKER_HUB`, `GHCR`, `GCR`; testcontainers: `TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX` |
| grype DB / OSV / semgrep rule packs | `GRYPE_DB_UPDATE_URL` + `GRYPE_DB_AUTO_UPDATE=false`, `OSV_SCANNER_FLAGS="--offline …"`, `SEMGREP_CONFIG=<vendored dir>` |
| registry.npmjs.org, nodejs.org/dist | `npm_config_registry` (or `registry=` in `.npmrc`), `NODE_DIST_URL` / `MISE_NODE_MIRROR_URL` |
| GitHub Releases / PyPI (mise-installed tools) | `HTTPS_PROXY`, `MISE_GITHUB_API_TOKEN`, `PIP_INDEX_URL` — or run the pinned images, as `.gitlab-ci.yml` does |

No `# syntax=` directive in Dockerfiles; no CI job runs `apt` or `curl | sh`
(pinned images, `go install` via `GOPROXY`, the npm registry; Node via
`NODE_DIST_URL`); `.env` never enters an image.

## Worktrees (multi-branch dev)

A **bare-repo container**: each branch is a sibling checkout, never nested.

```sh
git clone --bare git@github.com:tracehubmmp/golang-basics.git golang-basics/.bare
cd golang-basics && echo 'gitdir: ./.bare' > .git
git --git-dir=.bare config remote.origin.fetch '+refs/heads/*:refs/remotes/origin/*'
git fetch origin && git worktree add master master

./wt add feat/x          # worktree + mise trust + go work sync + pnpm install
./wt list | ./wt rm feat/x
```

Plain `git worktree add` skips `mise trust --all` (shims then fail with a
misleading "error parsing config file") and `go work sync`. `GOCACHE` is
shared across worktrees; so is the `docker/deps.yml` stack.

## Toolchain notes

- **Go** `1.27.0` in every `go.mod` and `mise.toml`; golangci-lint is built
  with a Go at least as new, so the linter moves first (CLAUDE.md).
- **net/http** (Go 1.22 ServeMux, no framework), **log/slog**,
  **prometheus/client_golang**, **caarlos0/env** + YAML, **testify**; pure-Go
  drivers **pgx**, **valkey-go**, **franz-go**; **OpenTelemetry** over OTLP/HTTP.
- **testcontainers-go** only behind `libs/testx/containers`.
- **VS Code**: the extension's helpers (gopls, dlv, gotests, gomodifytags,
  impl) are the **ide** profile — `just setup-ide`, export `MISE_ENV=ide`, and
  `cp .vscode-example/{settings,launch,tasks}.json .vscode/` (the settings
  point `go.alternateTools` at the mise shims with auto-update off).
