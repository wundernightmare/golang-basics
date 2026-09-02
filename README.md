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
| [`services/ping`](services/ping)           | HTTP service | `:8080`                      | Ping/pong HTTP service. `GET /ping` → `pong`, with `?msg=` echo + `/version`.                      |
| [`services/heartbeat`](services/heartbeat) | Worker       | `:8081` (health/metrics)     | Background ticker worker — emits a beat + bumps `heartbeat_beats_total` every interval.            |
| [`services/tasks`](services/tasks)         | HTTP service | `:8082`                      | Tasks CRUD over **Postgres + Valkey + Kafka**, traced, with `problem+json` errors. Publishes `task.created`. |
| [`services/consumer`](services/consumer)   | Worker       | `:8083` (health/metrics)     | Kafka consumer draining `tasks.events`; bumps `consumer_tasks_consumed_total`.                     |
| [`libs/httpx`](libs/httpx)                 | Library      | —                            | Shared HTTP scaffolding: gin engine, structured logging, Prometheus metrics, health, graceful shutdown, env + YAML config, RFC 9457 `Problem`. |
| [`libs/resilient-http-client`](libs/resilient-http-client) | Library | —              | Policy-per-target **outbound** HTTP client: rate limiting, circuit breaker, adaptive concurrency, jittered retry, response cache, coalescing, fallbacks, metrics. |
| [`libs/pgx`](libs/pgx)                     | Library      | —                            | PostgreSQL pool (`jackc/pgx`): env config, readiness check, boot-time migrations.                  |
| [`libs/valkey`](libs/valkey)               | Library      | —                            | Valkey cache (`valkey-go`): get/set/del, readiness check, generic cache-aside helper.              |
| [`libs/kafka`](libs/kafka)                 | Library      | —                            | Kafka producer + consumer (`franz-go`): sync publish, at-least-once consumer-group loop, readiness. |
| [`libs/otelx`](libs/otelx)                 | Library      | —                            | OpenTelemetry tracing: OTLP exporter, W3C propagation, gin middleware (opt-in).                    |

The dependency graph is `services/* → libs/*`. Every service — HTTP-first or
worker — reuses `httpx` for its `/healthz`, `/readyz` and `/metrics` surface, so
a worker is as observable as a server. `ping`/`heartbeat` stay dependency-free;
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
just up                           # ping :8080 + heartbeat :8081
curl -s localhost:8080/ping | jq .
curl -s localhost:8081/metrics | grep heartbeat_beats_total
just down

# 4. Run the data-services vertical (Postgres + Valkey + Kafka).
just infra-up                     # docker compose deps (postgres/valkey/kafka)
just tasks run &                  # tasks :8082
just consumer run &               # consumer :8083
curl -s -XPOST localhost:8082/tasks -d '{"title":"hello"}' | jq .
curl -s localhost:8083/metrics | grep consumer_tasks_consumed_total
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
