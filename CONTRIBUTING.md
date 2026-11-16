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
   just ci                # fmt-check → tidy-check → leak-check → vet → lint → test-short (pure Go)
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
- Every test package ends with a goroutine-leak check: its `TestMain` is
  `os.Exit(testx.Main(m))` — or `containers.Main(m)` with the shared
  containers, or `goleak.VerifyTestMain(m)` in a module without `libs/testx`.
  A goroutine still running after the last test is a missing `Close`, `Stop`
  or `cancel`: fix it, do not ignore it. `just leak-check` (in `just ci` and
  the `static` jobs) fails a test package without one.
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

A comment carries what the reader cannot get from the code in front of them:
the **reason** (the failure prevented, the trade-off taken, a consequence
that is not obvious), an **invariant**, or a **contract** — what a caller may
rely on without opening the body. What the name or the next lines already
say is not repeated: a comment that restates them is one more thing to keep
in step.

For every comment:

1. **One home per fact.** A list, an order, a number taken from a dependency
   is written in one place — next to the code that makes it true — and other
   comments point there (`see newAdminMux`); a copy drifts. The home of a
   default is its `Config` field (the tag and the comment beside it):
   elsewhere name the key, not the value. Where a list is unavoidable, name
   the criterion, not the members: "the services with
   `internal/kafkaprovider`", not "tasks and consumer". Text in shared code
   holds for every implementation behind it (both Kafka providers, both L1
   stores); what holds for one lives in that one.
2. **A suppression is a claim.** `//nolint:<linter> // <why>` and
   `// nosemgrep: <rule> -- <why>` give a reason that stands on its own (no
   "same", no "as above") and can be checked (name the function that bounds
   the value); gosec reasons carry the rule (`// G115: …`). When the cause
   goes, the directive goes — and a directive for a linter that is disabled
   in `.golangci.yml` is not written.
3. **A claim about a dependency or the OS** carries the version it was seen
   in or the test that pins it. A workaround for a defect is
   `UPSTREAM-WORKAROUND(<dependency> <version>): <what> (docs/UPSTREAM-WORKAROUNDS.md)`
   with its row there.
4. **History lives in the commit.** "The old code…", "was removed", "as
   before" describe something the reader cannot see; say what the code
   guarantees now.
5. **No invented reasons.** If a reason cannot be established from the code,
   its tests or its history, leave the comment out and ask — a plausible
   guess is worse than nothing. This outranks rule 8.

**Configuration and exported API** — a `Config` field, an environment key,
an exported identifier, a package doc — are read by someone who turns a knob
or calls a function without the code open. Say what it does, the unit, what
zero means, what breaks at the edges, at whatever length that takes.

**Everything else** is read with the code open, so additionally:

6. **No restating.** Not the name (`// release frees a slot`,
   `// len is the number of ids`), not the next lines narrated, not a bare
   `// Close implements io.Closer`. An unexported identifier whose name and
   signature say it all has no comment.
7. **A contract earns its line** when the signature does not show it: what a
   returned bool or nil means, the order of a result, who owns a buffer,
   that a lock must be held (`x.mu must be held`), which edge is accepted.
   Describe the condition the code tests, not the intent behind it.
8. **A literal wait or size outside configuration** has a name or a clause
   saying why this value.
9. **A mutex guards the fields below it, up to the next blank line**; it is
   declared that way and needs no comment. A comment is owed only when it
   guards something else (a field elsewhere, a call order, another lock's
   ordering).
10. **"Never", "only", "first", "every" are claims.** Write one where the
    reader can check it in the same function; otherwise scope it ("first
    within `tuneRuntime`").
11. **A design note longer than its identifier deserves** goes in a block of
    its own after the imports (or in `doc.go`); the identifier keeps its
    contract.

Tests follow the same rules: the comment on a test states the rule it
proves when the test's name does not, never the steps it takes.
