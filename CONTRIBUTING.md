# Contributing

## Dev setup

```sh
mise trust && mise install     # pinned go, golangci-lint, node, k6, AppSec tools
just setup                     # + govulncheck, go work sync
just hooks-install             # optional: lefthook pre-commit/pre-push gates
```

## Workflow

1. Branch via the worktree helper: `./wt add feat/my-change` (from the container
   root). See README "Worktrees".
2. Make your change. Keep services thin — shared HTTP behaviour belongs in
   `libs/httpx`.
3. Before pushing:
   ```sh
   just ci          # fmt-check → vet → lint → test
   just e2e         # Playwright (optional but recommended)
   ```
4. Commit with a conventional-commit subject (`feat:`, `fix:`, `docs:`, …).

## Adding a module

1. Create `services/<name>/` or `libs/<name>/` with its own `go.mod`
   (`module github.com/tracehubmmp/golang-basics/<path>`).
2. Add it to `go.work` (`use ./<path>`), in dependency order.
3. Copy a sibling's `justfile` (the recipes are module-generic).
4. Add it to `MODULES` (and `SERVICES`, if it builds a binary) in the root
   `justfile`, and to the per-package delegation block.
5. `just tidy` to wire up `go.sum` + `go.work.sum`.

## Tests

- Unit/integration tests live next to the code (`*_test.go`); table-driven where
  it helps. Use `testify` for assertions.
- E2E lives in `e2e/` (Playwright, API-only). Add a `*.spec.ts` and, if it
  should run in the fast subset, tag it `@smoke`.
- Load tests live in `benchmarks/` (k6).

## Style

- `gofmt` + `goimports` (grouped, local prefix
  `github.com/tracehubmmp/golang-basics`) — enforced by `just fmt-check`.
- `golangci-lint` config is `.golangci.yml`; exported symbols need doc comments
  (`revive:exported`).
