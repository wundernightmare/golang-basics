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
