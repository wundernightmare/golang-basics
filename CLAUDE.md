# CLAUDE.md

Guidance for AI agents working in this repo. Deep docs live in
[README.md](README.md), [CONTRIBUTING.md](CONTRIBUTING.md), and each module's
README; this file is only the high-signal, easy-to-miss bits.

## Build & test

- A `go.work` workspace root is **not itself a module** — `go build ./...` at
  the root fails. The workspace-wide `just` recipes (`just check` / `test` /
  `lint`) fan out over each module dir instead; run
  `go` directly only from inside a module dir (`libs/httpx`, `services/ping`,
  …). `just ci` is pure Go (fmt → vet → lint → tidy-check → test-short);
  `just contracts-check` is the only recipe that needs Node.
- Workspace-wide: `just check` / `just test` / `just lint` / `just ci`
  (full list in README "Common workspace commands").
- AppSec gate: `just sec` (source) + `just docker-scan-ci SVC` (image,
  `--fail-on high`). CVE waivers go in `.grype.yaml` / `osv-scanner.toml` with a
  documented removal trigger.

## Go version

- Module directives target `go 1.27.0`; the toolchain is pinned to the latest
  `1.27.x` patch in `mise.toml` (and the builder image digest), which is how
  stdlib security fixes reach the binaries. golangci-lint has to be built
  against a stdlib at least as new as the module target, or it panics with
  "file requires newer Go version" and CI goes red. The pinned golangci-lint
  2.15.0 is built with go1.27.0, so raise a directive only after the linter.

## Lint & format

- **golangci-lint runs `default: all`**; what is off is in `.golangci.yml`
  `disable`, each with its reason. Read the policy block at the top of that
  file before fixing findings: a finding is a signal, not an order — a fix
  must not change behaviour or API silently; otherwise it is a
  `//nolint:<linter> // <why>` (nolintlint requires both) or a reasoned
  `disable`. Never trade a panic on a broken invariant for a silent skip.
- **Formatting is `just fmt`, never by hand**: the formatters (gofumpt, gci,
  goimports) plus the auto-fixable layout linters in `FMT_LINTERS`
  (wsl_v5, whitespace, godot). A layout rule without an auto-fix does not
  get enabled. No line-length limit, on purpose.
- `just lint` starts with `golangci-lint config verify`: `run` silently
  ignores an unknown or misspelt key.

## Dev workflow

- **Worktrees**: multi-branch work uses a bare-repo container — see README
  "Worktrees". The repo root is a *bare container*, not a checkout: code and
  this file live one level down in `master/` (or a branch worktree), so `cd`
  into one before running `go`/`just` (`git worktree list` to see them). Per
  worktree, `git worktree add` does **not** `mise trust` or `go work sync` —
  the `./wt` helper does both (skipping `mise trust` makes mise-shimmed tools
  fail with a misleading "error parsing config file").
- **VS Code tooling is pinned too.** gopls / dlv / gotests / gomodifytags /
  impl live in `mise.ide.toml` (an opt-in profile: `just setup-ide`, or
  `MISE_ENV=ide mise install`) under the `go:` backend, and
  `.vscode-example/settings.json` points `go.alternateTools` at the mise shims
  with `go.toolsManagement.autoUpdate` off. Don't let the extension install its
  own into `GOPATH/bin` — that silently un-pins them. Bump versions in
  `mise.ide.toml`, then `just setup-ide`. Only `.vscode/extensions.json` is committed;
  everything else under `.vscode/` is gitignored and copied from
  `.vscode-example/`.
- **CI reads mise*.toml and go.work, never hard-codes versions or module lists.**
  Tools are split into the core `mise.toml` (go, golangci-lint, just,
  gotestsum, codegen, node/pnpm, shellcheck) and opt-in profiles
  `mise.appsec.toml` / `mise.ide.toml` / `mise.perf.toml` / `mise.report.toml`
  (`MISE_ENV=<profile>`, `just setup-<profile>`); new worktrees need
  `mise trust --all`.
  Both pipelines have a `versions` job that runs `scripts/mise-pins.sh` (tool
  pins → GitHub step outputs / GitLab `dotenv` artifact, used even in `image:`)
  and `scripts/touched-modules.sh --all` (module matrices). If you need a tool
  in CI, pin it in `mise.toml` and read it from there — a literal version or a
  module list in a workflow file is a bug waiting to drift. The git hooks use
  the same script to scope lint/test to touched modules.
- **Every upstream host is a variable with the public default** (`DOCKER_HUB`,
  `GHCR`, `GCR`, `GOPROXY`, `GOVULNDB`, `GRYPE_DB_UPDATE_URL`,
  `OSV_SCANNER_FLAGS`, `SEMGREP_CONFIG`, …) — the repo is the template for
  closed networks behind a proxy. Never write a bare `FROM golang:` /
  `image: alpine` / `curl https://…` / `apt-get install` into a Dockerfile,
  compose file or CI job; prefix images with the registry variable and put new
  knobs in `.env.example` (loaded by `just` and mise) with the same name used
  as a CI variable. No `# syntax=` directive in Dockerfiles.
- **`semgrep scan --metrics=off`, never `semgrep ci`** (it calls semgrep.dev).
- **GitHub Actions are pinned to a commit SHA** (`uses: owner/repo@<40-hex> # vX.Y.Z`),
  never a tag; `.github/dependabot.yml` moves the SHA and the comment
  together. `github.*` context never goes into `run:` text — pass it through
  `env:` and expand the variable in the script (shell injection). The `sast`
  job blocks both. The pnpm workspace keeps `minimumReleaseAge`,
  `blockExoticSubdeps` and `trustPolicy` set (and `.npmrc` `min-release-age`
  in step) for the same reason.
- **`gofmt` runs per module directory (`scripts/touched-modules.sh --all`),
  never `gofmt -l .` at the root** — GitLab can only cache paths under
  `$CI_PROJECT_DIR`, so `GOMODCACHE` lives in `.cache/`, and a bare walk hits
  the module cache's deliberately-malformed test fixtures. Keep `just
  fmt-check`, the GitHub job and the GitLab job identical.
- **go.mod / go.sum / go.work are checked, not trusted.** `scripts/tidy-check.sh`
  (`just tidy-check`, the CI matrix jobs per module, the pre-push hook) fails
  when `just tidy` would change anything. A dependency bump that leaves an
  `// indirect` require in a module that does not import it, or a go.sum
  missing the workspace-resolved entries, builds fine and is invisible until
  this check — run `just tidy` after any `go get` and commit the result.
- **Observability is opt-in.** `docker/observability.yml` (Jaeger +
  VictoriaMetrics + Grafana) is the receiving end; `otelx` installs only
  propagators and a no-op provider unless `*_OTEL_ENABLED=true`, so no code path
  requires a collector. Export is OTLP over **HTTP** (`:4318`) and **TLS by
  default**: a local collector needs `*_OTEL_EXPORTER_OTLP_INSECURE=true`.
  Metrics are Prometheus-only (scraped from the admin port); there is no OTel
  meter provider, on purpose. **Sampling is decided in two places**: the
  collector tail-samples (`docker/observability/otel-collector.yml`: errors,
  slow, `debug.subject`, a share of the rest — `OTEL_TAIL_*`), so services
  keep head sampling at 1.0 (`*_OTEL_TRACES_SAMPLER_RATIO`); a lower ratio
  hides traces from the collector, errors included. That file is tested as
  shipped by `libs/otelx/integration` — change a policy there, prove it there.
  Metric names, labels, units and buckets are a
  contract shared with nodejs-basics, written down in `docs/METRICS.md`: a new
  or renamed metric changes that file (and the sibling repo) first. What the
  broker or a server-side exporter measures better (consumer-group lag,
  Postgres/Valkey server state) is not collected in the services.
- Go runtime settings come from the container, not the manifest
  (`docs/RUNTIME.md`): `httpx.Server.Run` sets the memory limit from the cgroup
  (`*_RUNTIME_MEMORY_LIMIT_RATIO`) and the traceback level; `GOMAXPROCS` is the
  runtime's own. Never put `GOMEMLIMIT` / `GOMAXPROCS` in a pod spec or chart.
- Post-mortem diagnostics (`docs/DIAGNOSTICS.md`, `libs/httpx/diagnostics.go`):
  a service with object storage calls `srv.SetDumpSink(store)`; crash reports
  (`*_DIAG_DUMP_DIR`, an emptyDir) and memory-pressure heap/goroutine profiles
  then land under `diagnostics/<service>/dt=…/<pod>/`. No full heap dumps.
- **Secure by default.** `*_ADMIN_TOKEN` guards `/admin/*` and `/debug/*`;
  `httpx.Config.Validate()` refuses an empty token on a non-loopback admin
  address unless `*_ADMIN_INSECURE=true` (local compose / e2e set it). Kafka
  has `*_KAFKA_SECURITY_PROTOCOL` / `*_KAFKA_SASL_*` / `*_KAFKA_SSL_*`; auto
  topic creation is off unless `*_KAFKA_ALLOW_AUTO_TOPIC_CREATION=true`.
  Never hard-code `Insecure: true` in a service — it comes from config.
- **Kafka has two providers behind one API.** `libs/kafka` is pure Go (API,
  config, commit/retry/DLQ policy, metrics); `libs/kafka-rdkafka`
  (librdkafka, cgo — the default) and `libs/kafka-franz` (franz-go) implement
  its `Driver` and register on import. Services link one through
  `internal/kafkaprovider` (build tag `kafka_franz`; `KAFKA_PROVIDER` in
  `scripts/build-service.sh` and the Dockerfile). Never import a provider
  anywhere else, never add behaviour to one provider only — put it in
  `libs/kafka` and prove it in `libs/kafka/kafkatest`, which both run. Every
  client buffer is a named key (never a client default), and
  `KAFKA_*_EXTRA_PROPERTIES` may not override a named key. Building the
  rdkafka provider (and `go test -race` anywhere) needs cgo / gcc.
- **Local cross-module deps** resolve via `go.work`; each service `go.mod` also
  has a `replace … => ../../libs/httpx` so `go build` works outside the
  workspace too (e.g. inside the per-service Docker build).
- **Docker deps are a singleton**: `docker/deps.yml` hardcodes the project name
  (`golang-basics-deps`) + host ports, so one Postgres/Valkey/Kafka stack serves
  every worktree (`just infra-up`). `docker/stack.yml` runs the `tasks`/`consumer`
  images on that network (`just stack-up`).
- **Data services need deps.** `tasks`/`consumer` (and the `pgx`/`valkey`/`kafka`
  libs) talk to Postgres/Valkey/Kafka. Their integration tests use
  testcontainers through `libs/testx/containers`, so `just mod <m> test` needs
  a Docker daemon and the suites skip under `-short`. A module needs Docker
  exactly when its `go.mod` requires `libs/testx/containers`; CI derives the
  integration matrix from that (and runs a service with internal/kafkaprovider
  once more with `-tags kafka_franz`). `ping`, `heartbeat`, `httpx` and
  `resilient-http-client`, `grpcx`, `resilience` and `secret` stay dependency-free — and so do their module
  graphs: `libs/testx` (the light harness) pulls no Docker client. A dependency-free library that
  needs a container-backed suite gets a nested module next to its code
  (`libs/resilient-http-client/integration`, `libs/grpcx/integration`, `libs/otelx/integration`, `libs/secret/compat`: own `go.mod`, in `go.work`), never a
  `libs/testx/containers` require in its own `go.mod` and never one shared "all integration tests"
  module.

## Conventions

- All cross-cutting HTTP concerns live in `libs/httpx`; services stay thin
  (`main.go` + `internal/…`). Add shared behaviour to `httpx`, not per service.
  httpx is **net/http + the Go 1.22 ServeMux**, no framework: routes are
  `srv.Mux().HandleFunc("GET /tasks/{id}", h)`, path params `r.PathValue`,
  bodies `httpx.DecodeJSON`, responses `httpx.WriteJSON`, errors
  `httpx.WriteError(w, r, err)` — a 5xx always carries its cause
  (`httpx.Internal(detail, err)`), which is what gets logged and put on the span.
- **Two listeners per service.** API routes go on `srv.Mux()` (`*_HTTP_ADDR`);
  everything operational (`/healthz`, `/readyz`, `/metrics`, `/version`,
  `/admin/*`, `/debug/pprof`) is on the admin listener (`*_ADMIN_ADDR`, API
  port + 1000, workers have only this one). Never register ops routes on the
  API mux, and point probes / scrapes / e2e health waits at the admin port.
- **Tracing is built into the httpx chain** (request id → debug token →
  tracing → access log → metrics → your `WithMiddleware` extras → recovery →
  body limit → routing; a 401/429 from your authenticator is still traced,
  logged and counted). Do not add a tracing middleware; `otelx.Init` installs
  the provider and propagators, httpx starts the server span. The otelx
  contract test (`telemetry_test.go`) guards that logs, span and metrics
  describe the same request.
- **A credential in a config is a `secret.String`** (libs/secret), not a string with a tag: it
  cannot be printed by `fmt`, `slog`, JSON or YAML, shows as `[redacted:<fingerprint>]` on
  `/admin/config`, and its value is one explicit `Expose()` away, at the point of use. The
  `secret:"true"` tag remains for what cannot be that type (a map of headers).
- **Service configs embed the lib configs** (`httpx.Config` inline,
  `pgx.Config`/`valkey.Config`/`kafka.Config`/`otelx.Config` nested with an
  `envPrefix`) and load through `httpx.LoadYAML`: env > YAML > `envDefault` tag.
  Never copy fields by hand into a lib Config — that is how tuning knobs get
  silently dropped. Unknown YAML keys are an error.
- **Shutdown order is readiness off → `HTTP_SHUTDOWN_DELAY` → drain within
  `HTTP_SHUTDOWN_TIMEOUT` → forced close → admin listener.** `Run` binds
  synchronously and evaluates readiness once before opening the gate; tests
  use `Addr: "127.0.0.1:0"` and `srv.ListenAddr()`, never fixed ports.
- **A worker a request can wait for outlives the server**: run it with
  `srv.RunWith(ctx, httpx.Worker{…})` — the server drains, then the workers
  stop, each guarded against a panic (tasks: relay and janitor; ingest: the
  archive writer). A worker nothing waits for (consumer, heartbeat) runs under
  `srv.Guard` with the server's own context. Never a bare `go worker.Run`.
- **Durability patterns live in the tasks vertical**: writes go through a
  transactional outbox (row + event in one transaction, relay publishes), the
  consumer retries with backoff and dead-letters to `<topic>.dlq` (or stops
  when DLQ is disabled), processing is idempotent by `event_id`. Copy those,
  not a dual write.
- **The ingest vertical is the one sanctioned dual write**, and only because
  of its acknowledgement rule: `services/ingest` answers a batch after Kafka
  *or* the S3 archive made it durable, never before, so it holds no state
  (no volume, no `emptyDir`) and a killed pod loses nothing it promised.
  Keep that rule when changing `internal/pipeline`: nothing may acknowledge
  from memory. The sinks are driven through `resilience.Breaker` (libs/resilience, one each),
  what is unconfirmed is bounded (`MAX_IN_FLIGHT_BYTES` → 429), every wait
  is a config key, and the archive format and layout (`internal/shard`,
  `internal/archive`) are a contract with the compactor and any replay tool
  — version the header, do not change records in place. Its chaos and
  SIGKILL tests (`internal/integration`) are the proof; a change to the
  write path that does not keep them green is not done.
- **Log with the context**: `log.InfoContext(ctx, …)`, never bare `log.Info` in
  request or message handling — that is what stamps `trace_id`/`span_id`.
  Debug/info are sampled (`*_LOG_SAMPLE_*`); anything that must always be seen
  is warn or error.
- **Readiness is cached and tiered.** `/readyz` answers from results the
  background loop refreshes every `HEALTH_CHECK_INTERVAL`; it never calls a
  dependency per probe. Register a check with `httpx.Optional()` when the
  service can keep serving without that dependency (cache, best-effort event
  bus) — only critical checks turn readiness to 503.
- **Telemetry has tests that use the real thing**: the real SDK with an
  in-memory exporter, the real logger into a buffer, the real registry, real
  containers for the data libs. When you add a signal (a metric, a span, a log
  field), extend the contract test in `libs/otelx/telemetry_test.go` or the
  module's `telemetry_test.go`; do not assert it against a mock.
- **Tests are plain `go test` + testify.** `func TestX(t *testing.T)`,
  `t.Run` sub-tests, table tests, `t.Parallel()` where nothing swaps globals
  (`testx.Recorder` does). No test framework, no per-test Allure plugin in
  the unit and integration layers: their report is `gotestsum --junitfile` →
  `allure generate`. **The e2e suite alone uses testo + testo-allure**
  (`e2e/report_test.go`; depguard keeps both out of every other module):
  every Test function there is one `scenario(t, feature, func(t T) {…})`,
  sub-tests are `testo.Run`, and nothing a later line needs may sit inside a
  step — a step is a sub-test, so `-run` with a sub-test pattern skips it.
  `scripts/allure-collect.sh` merges its native results with the JUnit files
  and drops the e2e JUnit they replace. Helpers come from
  three `_test`-only modules — `libs/testx` (Recorder, LogBuffer, Metric,
  LintMetrics, Unique), `libs/testx/containers` (`Postgres/Valkey/Kafka/S3`, one
  per test binary via `TestMain` → `containers.Main`; `Proxied`/`Pause` for
  chaos) and `libs/testx/contract` (OpenAPI / JSON Schema validation). Tests
  isolate with `testx.Unique` (a schema, a key prefix, a topic), never a
  fresh container. A depguard rule keeps all of them out of non-test code.
- **Rules over sequences are property tests** (rapid; `libs/resilience`,
  `libs/tiercache`): a state machine, a model to compare against, an invariant
  of any history — not for what one table of examples says as well. They are
  unit tests (`-short`, virtual time via `inBubble{t}` under synctest) and keep
  rapid's full check count there. A property is only worth its name once a
  deliberately broken implementation fails it: try that before committing one.
- **One layer per behaviour** (README "Tests" table): unit for logic and
  wiring, contract for the telemetry signals, integration for the libs against
  real dependencies and the tasks vertical, e2e only for what a real process
  shows (readiness on the admin port, build stamp, tasks → Kafka → consumer).
  Do not re-assert a lower layer's behaviour in a higher one; wait for it.
- **Coverage is the covdata merge of unit + integration + e2e**
  (`scripts/cover.sh`, `just cov-check`, `.testcoverage.yml`). Tests write
  `-test.gocoverdir`, e2e binaries are built with `COVER=1` and write
  `GOCOVERDIR`. Raise the threshold when it improves; lower only with a reason.
  The pure-logic libraries also have a floor on the unit layer alone
  (`.testcoverage.unit.yml`): logic there is pinned by `-short` tests, not
  by an integration test passing through it.
  A new package must reach some layer's profile: the gate
  (`scripts/cover-files.go`) fails on a file with code that no instrumented
  binary contained — add a test or have e2e run the binary, or exclude the
  path in `.testcoverage.yml` with the reason (test code only).
  The gate is `scripts/cover.sh gate`, never a bare `go-test-coverage`: with
  no go.mod at the workspace root the tool names files per module, and every
  `^libs/…` exclude and override in `.testcoverage.yml` matches nothing.
- **Mutation testing is scoped**: `just mutate <module>` runs gremlins on the
  modules with a `.gremlins.yaml` (`libs/resilient-http-client`, `libs/grpcx`,
  `libs/resilience`, `libs/httpx`, `libs/tiercache`, `libs/secret`; each file
  says what it excludes and which survivors are known equivalents) with
  thresholds that fail the run. The oracle is the unit layer (`-short`). Always through `scripts/mutate.sh`, never a bare `gremlins
  unleash`: gremlins tests a copy of the module outside the workspace, and a
  copy that does not build counts every mutant as killed. When a mutant lives, first ask
  whether the code has an unkillable branch (rewrite with `min`/`max`, inject
  the clock) before adding a test. Nightly/manual in CI, not per PR.
- **A workaround for a dependency's defect is marked and listed**:
  `UPSTREAM-WORKAROUND(<dependency>)` in the comment (with the version it was
  seen in), a row in `docs/UPSTREAM-WORKAROUNDS.md`, and — where one can be
  written — a `TestUpstream…` test that asserts the defect and fails when an
  upgrade fixes it. Removing the workaround removes all three.
- **Chaos before trusting resilience code**: a code path written for a
  failing dependency (optional readiness, the outbox relay, cache
  fall-through, a timeout) gets a chaos test with `containers.Proxied` /
  `containers.Pause`, and restores the dependency on cleanup.
  Every outbound call must carry its own deadline (`VALKEY_OP_TIMEOUT`,
  `KAFKA_PUBLISH_TIMEOUT`, the readiness check timeout) — a request context
  is not a deadline.
- **Schemathesis owns "bad input → 4xx"**: do not hand-write validation
  tests for what the schema already says (`minLength`, documented responses);
  `just schemathesis <svc>` generates them. Unit tests own business
  semantics, integration tests own effects on dependencies. Every HTTP
  service has a `.tsp` contract; a new service gets one before its first
  handler test.
- **Contracts are generated, never edited**: change `api/tsp/*.tsp`, run
  `just contracts`, commit the emitted OpenAPI / JSON Schema and the Go types
  in `libs/contracts`. Services use `tasksapi.*` / `events.*` at the wire;
  the `domain` model is internal. `just contracts-check` (CI `contracts`)
  fails on stale outputs and on oasdiff breaking changes (waivers with a
  reason and removal trigger go in `api/oasdiff-breaking.ignore`). Event schemas:
  add optional fields only; never remove or retype.
- **Load tests (k6) stay outside Allure**: they run on the load stand and
  report there; do not wire k6 summaries into the test report.
- **Coverage must not regress**: the `coverage` job diffs a PR's breakdown
  against master's; the total may drop by at most `diff.threshold` in
  `.testcoverage.yml` (0.05 points — the measured run-to-run noise of
  chaos-covered error branches, not room for untested code). Do not re-run the
  job to get past it and do not widen it without a measurement. Every layer is collected with
  `-covermode=atomic` (covdata cannot merge mixed modes; `-race` implies
  atomic) — keep that flag on any new `go test -cover` / `go build -cover`.
- **No retries to hide flakes**: `-shuffle=on`, `TZ=UTC`, no retry flags
  anywhere (the Go e2e harness in `e2e/` included); nightly `stress`
  (`-count=3 -race`) and `fuzz` jobs. A found flake gets a `t.Skip` with a
  ticket, not a retry. Anything parsing external
  bytes gets a `Fuzz*` target; crashers under `testdata/fuzz/` are committed.
- **Container-backed tests need `DOCKER_HOST` on OrbStack/rootless Docker**
  (`unix://$HOME/.orbstack/run/docker.sock`); they skip when Docker is
  unreachable and fail when `CI` is set.
- **Test containers are disposable, and are treated so** (`libs/testx/containers`):
  Postgres and the S3 store keep their data on tmpfs, and every container is
  removed without a graceful stop. Both were measured (a store test waited
  mostly for fsync through the overlay filesystem; Kafka ignored SIGTERM for
  10s per package) — keep them when adding a fixture. `just test-integration`
  runs the modules `JOBS` at a time (default 4).
- **Metrics come from the libs.** `pgx`, `valkey`, `kafka` expose `Collectors()`;
  a service registers them on `srv.Metrics.Registry`. Route-level labels use the
  route template (`route="/tasks/{id}"`), never the raw path; methods outside
  the standard set are `_OTHER`; no `status` on latency histograms; sampled
  requests carry a `trace_id` exemplar.
- **Version is injected, not hard-coded**: `scripts/build-service.sh` / the
  `VERSION`, `GIT_SHA`, `BUILD_TIME` Docker build args set `libs/httpx.Version`,
  `.Revision`, `.BuildTime` (a Docker build has no `.git`); read them via
  `httpx.Build`, do not add per-service version constants.
- **FIPS 140-3 is a build mode** (`FIPS=1` in `scripts/build-service.sh` / the
  Dockerfile → `GOFIPS140=$GOFIPS140_VERSION`, Go's native module; `docs/RUNTIME.md`).
  It covers Go's crypto only, so both build lines refuse it with the rdkafka
  provider — keep that refusal. `/version` reports `fips140` from
  `crypto/fips140.Enabled()`; never a `build_info` label.
- Both binaries use the `run() error` + `os.Exit` pattern in `main` so deferred
  cleanup runs before exit (golangci-lint `gocritic:exitAfterDefer` enforces it).
