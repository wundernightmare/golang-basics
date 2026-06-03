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

- Module directives target `go 1.25.0`; the toolchain is pinned to `1.25.x` in
  `mise.toml`. golangci-lint can't typecheck against a go1.26 stdlib yet, so do
  **not** bump module/toolchain to 1.26 until the linter supports it (CI would
  go red). If `golangci-lint` panics with "file requires newer Go version",
  that's the symptom.

## Dev workflow

- **Worktrees**: multi-branch work uses a bare-repo container — see README
  "Worktrees". The repo root is a *bare container*, not a checkout: code and
  this file live one level down in `master/` (or a branch worktree), so `cd`
  into one before running `go`/`just` (`git worktree list` to see them). Per
  worktree, `git worktree add` does **not** `mise trust` or `go work sync` —
  the `./wt` helper does both (skipping `mise trust` makes mise-shimmed tools
  fail with a misleading "error parsing config file").
- **Local cross-module deps** resolve via `go.work`; each service `go.mod` also
  has a `replace … => ../../libs/httpx` so `go build` works outside the
  workspace too (e.g. inside the per-service Docker build).
- **Docker is a singleton** if/when a `docker/local.yml` is added: hardcode the
  project name + host ports so only one stack runs across worktrees.

## Conventions

- All cross-cutting HTTP concerns live in `libs/httpx`; services stay thin
  (`main.go` + `internal/…`). Add shared behaviour to `httpx`, not per service.
- Both binaries use the `run() error` + `os.Exit` pattern in `main` so deferred
  cleanup runs before exit (golangci-lint `gocritic:exitAfterDefer` enforces it).
