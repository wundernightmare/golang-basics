# Contributing

## Dev setup

```sh
mise trust --all               # mise.toml + the profile files (mise.<profile>.toml)
just setup                     # core toolchain, pnpm install (contracts), go work sync
just hooks-install             # optional: lefthook pre-commit / pre-push gates
```

`just setup` installs only the **core** profile — what `just ci`,
`just contracts` and `just e2e` need. The rest is opt-in, layered on top via
mise's `MISE_ENV` config files:

| Profile  | File               | Install              | Tools                                                     |
| -------- | ------------------ | -------------------- | --------------------------------------------------------- |
| core     | `mise.toml`        | `just setup`         | go, golangci-lint, just, shellcheck, gotestsum, oapi-codegen, go-jsonschema, oasdiff, node, pnpm |
| appsec   | `mise.appsec.toml` | `just setup-appsec`  | semgrep, gitleaks, osv-scanner, hadolint, syft, grype, cosign, govulncheck |
| ide      | `mise.ide.toml`    | `just setup-ide`     | gopls, dlv, gotests, gomodifytags, impl (VS Code)         |
| perf     | `mise.perf.toml`   | `just setup-perf`    | k6, benchstat, gremlins, go-test-coverage, gocover-cobertura |
| report   | `mise.report.toml` | `just setup-report`  | java (the Allure renderer)                                |

Recipes that need a profile tool select the profile themselves
(`MISE_ENV=appsec mise exec -- …`); export `MISE_ENV=ide` (comma-separate
several) in your shell to get a profile's tools on `PATH`.

## Workflow

1. Branch via the worktree helper: `./wt add feat/my-change` (from the container
   root). See README "Worktrees".
2. Make your change. Keep services thin — shared HTTP behaviour belongs in
   `libs/httpx`.
3. Before pushing:
   ```sh
   just ci                # fmt-check → tidy-check → vet → lint → test-short (pure Go)
   just contracts-check   # if api/ or libs/contracts changed (needs node/pnpm)
   just e2e               # Go e2e suite; `just infra-up` first for the data flow
   ```
4. Commit with a conventional-commit subject (`feat:`, `fix:`, `docs:`, …).

Behind a proxy or without direct internet: `cp .env.example .env` and
uncomment what your network needs (README "Closed networks").

## Changing an API or an event

1. Edit `api/tsp/tasks.tsp` / `ping.tsp` (HTTP) or `api/tsp/events.tsp` (Kafka).
2. `just contracts` — regenerates `api/openapi3`, `api/jsonschema` and
   `libs/contracts`; commit all of it.
3. Adjust the handler / consumer to the new `tasksapi` / `events` types; the
   api tests validate every exchange against the OpenAPI document
   (`libs/testx/contract`), so a mismatch fails there.
4. `just contracts-check` — stale outputs and breaking changes (oasdiff over
   every `api/openapi3/*.openapi.yaml`) fail here and in both CI pipelines.
   Events: optional additions only.

## Adding a module

1. Create `services/<name>/` or `libs/<name>/` with its own `go.mod`
   (`module github.com/tracehubmmp/golang-basics/<path>`) and a `replace` for
   every local module it requires (so `GOWORK=off` builds and `just
   tidy-check` work).
2. Add it to `go.work` (`use ./<path>`).
3. Copy a sibling's `justfile` (the recipes are module-generic; services add
   `release` / `run`).
4. `just tidy` to wire up `go.sum` + `go.work.sum`.

Nothing else. `scripts/touched-modules.sh` derives every module list from
`go.work` — the root justfile, both CI pipelines, Dependabot's globs and the
git hooks use it: `--all` (lint / test / vuln matrices), `--integration`
(Docker-backed: the go.mod requires `libs/testx/containers`), `--services`
(binaries + images from `docker/service.Dockerfile`), `--mutation` (has a
`.gremlins.yaml`). A new service needs no Dockerfile of its own.

## Tests

- Plain `go test` + testify; helpers from `libs/testx`, Docker-backed
  dependencies from `libs/testx/containers`, contract validation from
  `libs/testx/contract` (all `_test`-only). Put a behaviour's test at the
  lowest layer that can observe it, once — README "Tests" says which layer
  owns what.
- Container-backed tests take their dependency from `containers.Postgres/
  Valkey/Kafka` and isolate by name (`testx.Unique`), never by starting their
  own container.
- E2E lives in `e2e/` (Go, build tag `e2e`): `just e2e` builds the service
  binaries into `.build/` and runs the suite against them.
- Reports: `just test-junit` (gotestsum → `.reports/junit`) then `just allure`.
  Coverage: `just cov-all` gates the merged unit + integration + e2e number;
  `just mutate` for pure logic.
- Load tests live in `benchmarks/` (k6, perf profile).

## Style

- `gofmt` + `goimports` (grouped, local prefix
  `github.com/tracehubmmp/golang-basics`) — enforced by `just fmt-check`.
- `golangci-lint` config is `.golangci.yml`; exported symbols need doc comments
  (`revive:exported`). Shell scripts pass `shellcheck` (`just lint-sh`).
- Waivers (CVE ignores, oasdiff exceptions, pnpm release-age exclusions)
  carry a reason and a removal trigger; a date trigger is written
  `Remove after YYYY-MM-DD` and `just sec-waivers` / CI fail once it passes.
