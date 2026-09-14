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

Behind a proxy or without direct internet: `cp .env.example .env` and
uncomment what your network needs (README "Closed networks").

## Changing an API or an event

1. Edit `api/tsp/tasks.tsp` (HTTP) or `api/tsp/events.tsp` (Kafka).
2. `just contracts` — regenerates `api/openapi3`, `api/jsonschema` and
   `libs/contracts`; commit all of it.
3. Adjust the handler / consumer to the new `tasksapi` / `events` types; the
   api tests validate every exchange against the OpenAPI document, so a
   mismatch fails there.
4. `just contracts-check` — stale outputs and breaking changes fail here and
   in CI. Events: optional additions only.

## Adding a module

1. Create `services/<name>/` or `libs/<name>/` with its own `go.mod`
   (`module github.com/tracehubmmp/golang-basics/<path>`).
2. Add it to `go.work` (`use ./<path>`), in dependency order.
3. Copy a sibling's `justfile` (the recipes are module-generic).
4. Add it to `MODULES` (and `SERVICES`, if it builds a binary) in the root
   `justfile`, and to the per-package delegation block.
5. `just tidy` to wire up `go.sum` + `go.work.sum` (`just tidy-check` is what
   CI and the pre-push hook run: an untidy module or a `go.work` that
   `go work sync` would rewrite fails the pipeline).

Nothing else: both CI pipelines and the git hooks discover modules from
`go.work` (`scripts/touched-modules.sh`), integration suites from the modules
that import testcontainers, and Dockerfiles / services from `services/*`.

## Tests

- Every test is an Allure test through `libs/testx` (`testx.Run` or a
  `testo.Suite[testx.T]`); testify assertions work as before. Put a behaviour's
  test at the lowest layer that can observe it, once — README "Tests" says
  which layer owns what.
- Container-backed tests take their dependency from `testx.Postgres/Valkey/Kafka`
  and isolate by name (`testx.Unique`), never by starting their own container.
- `just cov-check` gates the merged (unit + integration + e2e) coverage;
  `just allure-report` renders every layer; `just mutate` for pure logic.
- E2E lives in `e2e/` (Playwright, API-only). Add a `*.spec.ts` and, if it
  should run in the fast subset, tag it `@smoke`.
- Load tests live in `benchmarks/` (k6).

## Style

- `gofmt` + `goimports` (grouped, local prefix
  `github.com/tracehubmmp/golang-basics`) — enforced by `just fmt-check`.
- `golangci-lint` config is `.golangci.yml`; exported symbols need doc comments
  (`revive:exported`).
