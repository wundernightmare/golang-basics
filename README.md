# golang-basics

A small, idiomatic **Go monorepo** modelled on the Rust `tracehub-edge`
workspace: a `go.work` workspace of service modules + shared library modules
(the Go analogue of "one crate = one module"), a `just`-driven build/test/lint/
security/CI surface, per-package justfiles, multi-stage distroless Docker
images, a Playwright e2e suite, and k6 load tests. It exists as a learning /
template scaffold — two tiny services and one shared library, wired up the way
a real Go monorepo would be.

It lives in a **bare-repo worktree container** (see [Worktrees](#worktrees-multi-branch-dev)),
exactly like its Rust sibling: the repo root is a `.bare/` container and the
code lives in per-branch worktrees (`master/` is canonical).

---

## Modules

| Module                              | Kind        | Port(s)                       | One-liner                                                                                          |
| ----------------------------------- | ----------- | ----------------------------- | ------------------------------------------------------------------------------------------------- |
| [`services/ping`](services/ping)           | HTTP service | `:8080`                      | Ping/pong HTTP service. `GET /ping` → `pong`, with `?msg=` echo + `/version`.                      |
| [`services/heartbeat`](services/heartbeat) | Worker       | `:8081` (health/metrics)     | Background ticker worker — emits a beat + bumps `heartbeat_beats_total` every interval.            |
| [`libs/httpx`](libs/httpx)                 | Library      | —                            | Shared HTTP scaffolding: gin engine, structured logging, Prometheus metrics, health, graceful shutdown, env config. |

The dependency graph is `services/* → libs/httpx`. Both services — an HTTP-first
one and a worker — reuse the same library for their `/healthz`, `/readyz` and
`/metrics` surface, so a worker is as observable as a server.

---

## Layout

```
golang-basics/              ← bare-repo CONTAINER (.bare + .git pointer + wt + CLAUDE.md)
└── master/                 ← canonical worktree (this tree)
    ├── go.work             ← workspace: ties the three modules together
    ├── justfile            ← workspace task runner (fan-out + delegation)
    ├── mise.toml           ← pinned toolchain (go, golangci-lint, node, k6, AppSec tools)
    ├── lefthook.yml        ← optional git hooks
    ├── .golangci.yml       ← lint config
    ├── libs/httpx/         ← shared library module (go.mod + justfile + README + tests)
    ├── services/ping/      ← HTTP service module (+ Dockerfile)
    ├── services/heartbeat/ ← worker module (+ Dockerfile)
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

# 3. Run the services on the host.
just up                           # ping :8080 + heartbeat :8081
curl -s localhost:8080/ping | jq .
curl -s localhost:8081/metrics | grep heartbeat_beats_total
just down

# 4. Exercise them end-to-end / under load.
just e2e                          # Playwright (spawns the binaries itself)
just bench-smoke                  # k6, 50 VUs × 30s against ping
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
just httpx <recipe>
just ping <recipe>
just heartbeat <recipe>

# examples
just ping test
just httpx cov-html
just heartbeat lint
```

### Run a recipe across every module

```sh
just each test         # in dependency order: httpx → ping → heartbeat
just each lint
just each ci
```

---

## E2E tests (Playwright)

API tests (no browser). The harness builds the service binaries, spawns them,
waits for `/healthz`, runs the specs, then stops them. See
[`e2e/README.md`](e2e/README.md).

```sh
just e2e-install     # pnpm install (once)
just e2e             # build binaries + run the suite
just e2e-ui          # Playwright UI
just e2e-filter ping # subset
just e2e-report      # open last report
```

## Benchmarks (k6)

Profiles mirror the Rust sibling repo (`smoke` / `load` / `stress` / `soak` /
`peak`). See [`benchmarks/README.md`](benchmarks/README.md).

```sh
just bench-smoke     # 50 VUs × 30s
just bench-load      # ramp 0→500 VUs
just bench-stress    # ramp 0→2000 VUs
just bench-soak      # 500 VUs × 30m
just bench-peak      # constant-arrival-rate 25k req/s × 1m
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
binary on `gcr.io/distroless/static-debian12:nonroot`, uid 65532, no shell). The
build context is the **workspace root** so the build sees `go.work` + every
module:

```sh
docker build -f services/ping/Dockerfile -t ping:dev .
docker run --rm -p 8080:8080 ping:dev
```

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
Rust sibling repo's sccache). **Docker** `docker/local.yml` would be a singleton
(fixed project name + host ports) — run one stack and reach it at `localhost:<port>`.

---

## Toolchain notes

- **Go**: module directives target `go 1.25.0`; the toolchain is pinned to the
  latest `1.25.x` in `mise.toml` because golangci-lint (and most of the Go tool
  ecosystem) tracks one minor behind the bleeding edge. Bump to 1.26 once it's
  broadly supported.
- **just** drives everything (language-agnostic, same as the Rust sibling).
- **gin** for HTTP, **log/slog** for logging, **prometheus/client_golang** for
  metrics, **testify** for assertions, **caarlos0/env** for config.

See [`CLAUDE.md`](CLAUDE.md) for the high-signal, easy-to-miss bits and
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the dev workflow.
