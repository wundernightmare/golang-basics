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
- **CI reads mise.toml, never hard-codes versions.** Both pipelines have a
  `versions` job that parses `mise.toml` (GitHub → step outputs, GitLab →
  `dotenv` artifact, used even in `image:`). If you need a tool in CI, pin it in
  `mise.toml` and read it from there — a literal version in a workflow file is a
  bug waiting to drift.
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
- Both binaries use the `run() error` + `os.Exit` pattern in `main` so deferred
  cleanup runs before exit (golangci-lint `gocritic:exitAfterDefer` enforces it).
