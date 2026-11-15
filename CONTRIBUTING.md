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
- Reports: `just test-junit` (gotestsum → `.reports/junit`) and/or `just e2e` (native Allure results → `.reports/allure-native`), then `just allure`.
  Coverage: `just cov-all` gates the merged unit + integration + e2e number;
  `just mutate` for pure logic.
- Load tests live in `benchmarks/` (k6, perf profile).

## Style

- `gofmt` + `goimports` (grouped, local prefix
  `github.com/tracehubmmp/golang-basics`) — enforced by `just fmt-check`.
- `golangci-lint` config is `.golangci.yml`; exported symbols need doc comments
  (`revive:exported`). Shell scripts pass `shellcheck` (`just lint-sh`).
- Comments: see "Comments" below.
- Waivers (CVE ignores, oasdiff exceptions, pnpm release-age exclusions)
  carry a reason and a removal trigger; a date trigger is written
  `Remove after YYYY-MM-DD` and `just sec-waivers` / CI fail once it passes.

## Comments

A comment is for what the code cannot say: **why** it is the way it is. What
it does and how are read from the code, and a comment repeating them is one
more thing to keep in step.

Two audiences are served differently, on purpose:

- **Configuration and exported API** — a `Config` field, an environment key,
  a default, an exported identifier, a package doc. The reader turns a knob
  or calls a function without reading the code: say what it does, the unit,
  the default, what zero means, what breaks at the edges. Detail is welcome
  here; only a wrong or stale sentence is a defect.
- **Everything else** — the reader has the code open. The rules below apply.

Rules:

1. **Say why.** The failure the code prevents, the invariant it keeps, the
   trade-off taken, a consequence that is not obvious. Not the name restated
   (`// release frees a slot`), not the next lines narrated, not a bare
   `// Close implements io.Closer` on an unexported type.
2. **One home per fact.** A list, an order, a default, a number taken from a
   dependency is written in one place — next to the code that makes it true —
   and other comments point there (`see newAdminMux`). Copies drift: every
   list of admin endpoints but the real one had lost an entry.
3. **Name the criterion, not the members**: "the services with
   `internal/kafkaprovider`", not "tasks and consumer".
4. **"Never", "only", "first", "every" are claims.** Write one where the
   reader can check it in the same function; otherwise scope it ("first
   within `tuneRuntime`"). A comment on a returned bool describes the
   condition the code tests, not the intent behind it.
5. **Text in shared code is true for every implementation behind it** (both
   Kafka providers, both L1 stores, both HTTP and gRPC clients). What holds
   for one lives in that one.
6. **Constants and locks carry their reason.** A literal wait or size
   outside configuration has a name or a clause saying why this value; a
   mutex says which fields it guards and against whom; a helper that needs a
   lock held says `x.mu must be held`.
7. **A suppression is a claim too.** `//nolint:<linter> // <why>` and
   `// nosemgrep: <rule> -- <why>` give a reason that stands on its own (no
   "same", no "as above") and can be checked (name the function that bounds
   the value); gosec reasons carry the rule (`// G115: …`). When the cause
   goes, the directive goes — and a directive for a linter that is disabled
   in `.golangci.yml` is not written.
8. **A claim about a dependency or the OS** carries the version it was seen
   in or the test that pins it. A workaround for a defect is
   `UPSTREAM-WORKAROUND(<dependency> <version>): <what> (docs/UPSTREAM-WORKAROUNDS.md)`
   with its row there.
9. **History lives in the commit.** "The old code…", "was removed", "as
   before" describe something the reader cannot see; say what the code
   guarantees now.
10. **A doc comment sits on the declaration it describes.** A design note
    longer than its identifier deserves goes in a block of its own after the
    imports (or in `doc.go`), and the identifier keeps one line.
11. **No invented reasons.** If the reason for a value cannot be established
    from the code, its tests or its history, leave it uncommented and ask —
    a plausible guess is worse than nothing.

Tests follow the same rules: the comment on a test states the rule it
proves, not the steps it takes.
