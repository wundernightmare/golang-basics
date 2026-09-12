# CLAUDE.md

Guidance for AI agents working in this repo. Deep docs live in
[README.md](README.md), [CONTRIBUTING.md](CONTRIBUTING.md), and each module's
README; this file is only the high-signal, easy-to-miss bits.

## Build & test

- A `go.work` workspace root is **not itself a module** — `go build ./...` at
  the root fails. The workspace-wide `just` recipes (`just check` / `test` /
  `lint`) fan out over each module dir instead; run `go` directly only from
  inside `libs/httpx`, `services/ping`, or `services/heartbeat`.
- Workspace-wide: `just check` / `just test` / `just lint` / `just ci`
  (full list in README "Common workspace commands").
- AppSec gate: `just sec` (source) + `just docker-scan-ci SVC` (image,
  `--fail-on high`). CVE waivers go in `.grype.yaml` / `osv-scanner.toml` with a
  documented removal trigger.

## Go version

- Module directives target `go 1.27.0`; the toolchain is pinned to `1.27.x` in
  `mise.toml`. Keep the two in step: golangci-lint has to be built against a
  stdlib at least as new as the module target, or it panics with "file requires
  newer Go version" and CI goes red. The pinned golangci-lint 2.13.2 is built
  with go1.27.0, so bump the linter first, then the module/toolchain target.

## Dev workflow

- **Worktrees**: multi-branch work uses a bare-repo container — see README
  "Worktrees". The repo root is a *bare container*, not a checkout: code and
  this file live one level down in `master/` (or a branch worktree), so `cd`
  into one before running `go`/`just` (`git worktree list` to see them). Per
  worktree, `git worktree add` does **not** `mise trust` or `go work sync` —
  the `./wt` helper does both (skipping `mise trust` makes mise-shimmed tools
  fail with a misleading "error parsing config file").
- **VS Code tooling is pinned too.** gopls / dlv / gotests / gomodifytags /
  impl live in `mise.toml` under the `go:` backend, and
  `.vscode-example/settings.json` points `go.alternateTools` at the mise shims
  with `go.toolsManagement.autoUpdate` off. Don't let the extension install its
  own into `GOPATH/bin` — that silently un-pins them. Bump versions in
  `mise.toml`, then `mise install`. Only `.vscode/extensions.json` is committed;
  everything else under `.vscode/` is gitignored and copied from
  `.vscode-example/`.
- **CI reads mise.toml and go.work, never hard-codes versions or module lists.**
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
- **`gofmt` is scoped to `./libs ./services`, not `.`** — GitLab can only cache
  paths under `$CI_PROJECT_DIR`, so `GOMODCACHE` lives in `.cache/`, and a bare
  `gofmt -l .` walks into the module cache's deliberately-malformed test
  fixtures. Keep `just fmt-check`, the GitHub job and the GitLab job identical.
- **Observability is opt-in.** `docker/observability.yml` (Jaeger +
  VictoriaMetrics + Grafana) is the receiving end; `otelx` installs only
  propagators and a no-op provider unless `*_OTEL_ENABLED=true`, so no code path
  requires a collector.
- **Local cross-module deps** resolve via `go.work`; each service `go.mod` also
  has a `replace … => ../../libs/httpx` so `go build` works outside the
  workspace too (e.g. inside the per-service Docker build).
- **Docker deps are a singleton**: `docker/deps.yml` hardcodes the project name
  (`golang-basics-deps`) + host ports, so one Postgres/Valkey/Kafka stack serves
  every worktree (`just infra-up`). `docker/stack.yml` runs the `tasks`/`consumer`
  images on that network (`just stack-up`).
- **Data services need deps.** `tasks`/`consumer` (and the `pgx`/`valkey`/`kafka`
  libs) talk to Postgres/Valkey/Kafka. Their integration tests use
  testcontainers, so `just <mod> test` needs a Docker daemon and the suites skip
  under `-short` (`just <mod> test-short` for the unit-only subset). `ping`,
  `heartbeat`, `httpx` and `resilient-http-client` stay dependency-free.

## Conventions

- All cross-cutting HTTP concerns live in `libs/httpx`; services stay thin
  (`main.go` + `internal/…`). Add shared behaviour to `httpx`, not per service.
- **Two listeners per service.** API routes go on `srv.Engine()` (`*_HTTP_ADDR`);
  everything operational (`/healthz`, `/readyz`, `/metrics`, `/version`,
  `/debug/pprof`) is on the admin listener (`*_ADMIN_ADDR`, API port + 1000,
  workers have only this one). Never register ops routes on the API engine, and
  point probes / scrapes / e2e health waits at the admin port.
- **Tracing middleware goes through `httpx.WithMiddleware(otelx.GinMiddleware(…))`**,
  never `srv.Engine().Use(…)` after `NewServer` — otelgin restores the request
  context when it returns, so a tracer registered inside the access-log
  middleware leaves the access-log line without `trace_id`. The otelx contract
  test (`telemetry_test.go`) guards this.
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
- **Every test goes through `libs/testx`**: `testx.Run` for a plain test,
  `testo.Suite[testx.T]` + `testx.Options(tags...)` for a suite; sub-tests are
  `testo.Run`/`testx.Step`, never `t.Run`. Containers come from
  `testx.Postgres/Valkey/Kafka` (one per test binary, `TestMain` →
  `testx.Main`) and tests isolate with `testx.Unique`, never a fresh container.
  Spans/logs/metrics are read with `testx.Recorder/LogBuffer/Metric`. No
  helper copies in packages.
- **One layer per behaviour** (README "Tests" table): unit for logic and
  wiring, contract for the telemetry signals, integration for the libs against
  real dependencies and the tasks vertical, e2e only for what a real process
  shows (readiness on the admin port, build stamp, tasks → Kafka → consumer).
  Do not re-assert a lower layer's behaviour in a higher one; wait for it.
- **Coverage is the covdata merge of unit + integration + e2e**
  (`scripts/cover.sh`, `just cov-check`, `.testcoverage.yml`). Tests write
  `-test.gocoverdir`, e2e binaries are built with `COVER=1` and write
  `GOCOVERDIR`. Raise the threshold when it improves; lower only with a reason.
- **Mutation testing is scoped**: `just mutate` runs gremlins on
  `libs/resilient-http-client`'s pure files (its `.gremlins.yaml` excludes the
  rest) with thresholds that fail the run. When a mutant lives, first ask
  whether the code has an unkillable branch (rewrite with `min`/`max`, inject
  the clock) before adding a test. Nightly/manual in CI, not per PR.
- **Chaos before trusting resilience code**: a code path written for a
  failing dependency (optional readiness, best-effort publish, cache
  fall-through, a timeout) gets a `ChaosSuite` scenario with
  `testx.Proxied` / `testx.Pause`, and restores the dependency on cleanup.
  Every outbound call must carry its own deadline (`VALKEY_OP_TIMEOUT`,
  `KAFKA_PUBLISH_TIMEOUT`, the readiness check timeout) — a request context
  is not a deadline.
- **TestOps metadata is sample data**: `testx.Meta` on suites and
  `testx.Case(t, id, story)` on tests carry the shape; `golang-basics`,
  `@team-platform`, `GB-<n>`, `*.example.internal` are placeholders. Keep one
  id per test; keep the metadata when copying a test, change the id.
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
  against master's (`--diff-threshold 0`). Every layer is collected with
  `-covermode=atomic` (covdata cannot merge mixed modes; `-race` implies
  atomic) — keep that flag on any new `go test -cover` / `go build -cover`.
- **No retries to hide flakes**: `-shuffle=on`, `TZ=UTC`, Playwright
  `retries: 0`; nightly `stress` (`-count=3 -race`) and `fuzz` jobs. A found
  flake gets `t.Flaky()` + a ticket, not a retry. Anything parsing external
  bytes gets a `Fuzz*` target; crashers under `testdata/fuzz/` are committed.
- **Container-backed tests need `DOCKER_HOST` on OrbStack/rootless Docker**
  (`unix://$HOME/.orbstack/run/docker.sock`); they skip when Docker is
  unreachable and fail when `CI` is set.
- **Metrics come from the libs.** `pgx`, `valkey`, `kafka` expose `Collectors()`;
  a service registers them on `srv.Metrics.Registry`. Route-level labels use the
  route template, never the raw path; no `status` on latency histograms.
- **Version is injected, not hard-coded**: `scripts/build-service.sh` / the
  `VERSION` Docker build arg set `libs/httpx.Version`; read it via
  `httpx.Version` / `httpx.Build`, do not add per-service version constants.
- Both binaries use the `run() error` + `os.Exit` pattern in `main` so deferred
  cleanup runs before exit (golangci-lint `gocritic:exitAfterDefer` enforces it).
