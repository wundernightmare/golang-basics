# golang-basics — workspace task runner
#
# Install: cargo install just | brew install just | mise use just
# Usage:   just <recipe>      | just --list
#
# This is the Go analogue of the Rust sibling repo's root justfile: workspace-
# wide recipes that fan out over every module, per-package delegation, a `just
# each` loop, an AppSec block, Docker/CI gates, e2e (Playwright) and k6 bench
# recipes. Because a go.work workspace root is not itself a module, the
# fan-out recipes iterate MODULES explicitly rather than relying on `./...`.

# Load the (gitignored) .env — proxy, GOPROXY, registry mirrors, scanner DB
# mirrors — into every recipe. Absent file = no-op. Knobs: .env.example.
set dotenv-load := true

# Every Go module in the workspace, in dependency order (libs first).
MODULES := "libs/contracts libs/httpx libs/testx libs/resilient-http-client libs/pgx libs/valkey libs/kafka libs/otelx services/ping services/heartbeat services/tasks services/consumer"
# Buildable service binaries (module dir : binary name).
SERVICES := "ping heartbeat tasks consumer"

# Build args forwarded from the environment into every `docker build`. A
# value-less --build-arg takes the variable from the environment and is
# skipped when unset, so an open-network build sees the Dockerfile defaults.
DOCKER_BUILD_ARGS := "--build-arg DOCKER_HUB --build-arg GCR --build-arg GOPROXY --build-arg GOSUMDB --build-arg GONOSUMDB --build-arg GOFLAGS --build-arg HTTP_PROXY --build-arg HTTPS_PROXY --build-arg NO_PROXY"
# Scanner inputs that a closed network points at vendored rules / offline DBs.
SEMGREP_CONFIG := env("SEMGREP_CONFIG", "p/owasp-top-ten p/golang")
OSV_SCANNER_FLAGS := env("OSV_SCANNER_FLAGS", "")

# Show all available recipes
default:
    @just --list --unsorted

# ── Workspace build ───────────────────────────────────────────────────────────

# Vet + type-check every module (fastest workspace-wide feedback)
check:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── vet $m"; (cd "$m" && go vet ./...); done

# Debug-build every module
build:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── build $m"; (cd "$m" && go build ./...); done

# Build stripped release binaries for every service into <svc>/bin/
# (version from VERSION or `git describe`; see scripts/build-service.sh)
release:
    #!/usr/bin/env bash
    set -euo pipefail
    for s in {{SERVICES}}; do
      echo "── release services/$s"
      scripts/build-service.sh "$s"
    done

# ── Workspace test ────────────────────────────────────────────────────────────

# Run every module's tests (container-backed suites need Docker; on OrbStack /
# rootless setups export DOCKER_HOST, see README "Tests"). Shuffled like CI.
test *args:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── test $m"; (cd "$m" && go test -shuffle=on ./... {{args}}); done

# Flakiness hunt: every layer three times, shuffled, under -race (what CI runs nightly)
stress:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── stress $m"; (cd "$m" && go test -count=3 -race -shuffle=on ./...); done

# Run every Fuzz* target for FUZZTIME each (default 20s); crashers land in testdata/fuzz/
fuzz FUZZTIME="20s":
    FUZZTIME={{FUZZTIME}} scripts/fuzz.sh

# Micro-benchmarks of every module (Benchmark* functions, benchstat-ready) into OUT;
# `just bench-compare base.txt head.txt` prints the benchstat diff
bench OUT="bench.txt":
    scripts/bench.sh {{OUT}}

bench-compare BASE HEAD:
    mise exec -- benchstat {{BASE}} {{HEAD}}

# Run every module's tests with the race detector
test-race:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── test -race $m"; (cd "$m" && go test -race ./...); done

# Run tests with verbose output
test-verbose:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── test -v $m"; (cd "$m" && go test -v ./...); done

# Aggregate per-function coverage across the workspace
cov:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do
      echo "── cov $m"
      (cd "$m" && go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1)
    done

# Coverage per layer into .cover/<layer> (binary format, merged by covdata):
#   just cov-layer unit | integration | e2e     (see scripts/cover.sh)
cov-layer LAYER:
    scripts/cover.sh {{LAYER}}

# Merge every collected layer into coverage-merged.out and gate it with
# .testcoverage.yml — the number that counts is the union of unit +
# integration + e2e, not any single layer.
#
# Gates whatever is under .cover/ right now: a missing layer gates a partial
# (lower) number and a stale one can pass what the tree no longer earns.
# `just cov-all` (wipe .cover, collect all three, then this) is the only safe
# entry point; alone, this is for re-gating a cov-all that just ran.
cov-check:
    #!/usr/bin/env bash
    set -euo pipefail
    for l in unit integration e2e; do
      if [ -d ".cover/$l" ] && [ -n "$(ls -A ".cover/$l")" ]; then echo "cov-check: layer $l found (.cover/$l)"
      else echo "cov-check: layer $l MISSING (.cover/$l) — the gate runs on a partial merge; use just cov-all"; fi
    done
    scripts/cover.sh merge
    mise exec -- go-test-coverage --config .testcoverage.yml
    echo "Allure: results from every layer are in */allure-results — just allure-report"

# Everything: all three layers (integration needs Docker, e2e builds cover-
# instrumented binaries and runs Playwright), then the merged gate.
cov-all:
    rm -rf .cover
    scripts/cover.sh unit
    scripts/cover.sh integration
    scripts/cover.sh e2e
    just cov-check

# Mutation testing (gremlins) of a module's pure logic; config: <module>/.gremlins.yaml.
# GOFLAGS=-count=1: gremlins derives the per-mutant timeout from the initial
# coverage run, and a cached run (milliseconds) would make every mutant time out.
mutate MODULE="libs/resilient-http-client":
    cd {{MODULE}} && GOFLAGS=-count=1 mise exec -- gremlins unleash --output "$OLDPWD/gremlins-$(basename {{MODULE}}).json"

# Render the Allure HTML report from every module's allure-results/ (needs a JRE,
# pinned in mise.toml). The results are merged into one directory first so the
# categories.json / executor.json that scripts/allure-meta.sh drops next to
# them apply to the whole report, the way the CI allure-report jobs do it.
allure-report:
    #!/usr/bin/env bash
    set -euo pipefail
    dirs=$(find . -type d -name allure-results -not -path '*/node_modules/*' -not -path './.cache/*')
    [ -n "$dirs" ] || { echo "no allure-results/ found — run the tests first (just test)"; exit 1; }
    merged="$(mktemp -d)"; trap 'rm -rf "$merged"' EXIT
    for d in $dirs; do cp -r "$d/." "$merged/"; done
    scripts/allure-meta.sh "$merged"
    mise exec -- pnpm exec allure generate --clean --single-file -o allure-report "$merged"
    echo "report: allure-report/index.html"

# ── Lint & format ─────────────────────────────────────────────────────────────

# golangci-lint across every module (config: .golangci.yml at repo root)
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── lint $m"; (cd "$m" && golangci-lint run); done

# golangci-lint --fix across every module
lint-fix:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do (cd "$m" && golangci-lint run --fix); done

# Format all Go source in-place (gofmt + golangci-lint fmt).
# Scoped to the module trees — CI relocates GOMODCACHE under .cache/ so GitLab
# can cache it, and a bare `.` would walk into that cache's test fixtures.
fmt:
    #!/usr/bin/env bash
    set -euo pipefail
    gofmt -w ./libs ./services
    for m in {{MODULES}}; do (cd "$m" && golangci-lint fmt); done

# Check formatting without modifying files (CI gate)
fmt-check:
    #!/usr/bin/env bash
    set -euo pipefail
    out="$(gofmt -l ./libs ./services)"
    if [ -n "$out" ]; then echo "gofmt needed in:"; echo "$out"; exit 1; fi
    echo "gofmt clean"

# Tidy every module's go.mod / go.sum
tidy:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── tidy $m"; (cd "$m" && go mod tidy); done
    go work sync

# Fail if `just tidy` would change anything (scripts/tidy-check.sh — the same
# script the CI matrix jobs, the fmt jobs and the pre-push hook run)
tidy-check:
    scripts/tidy-check.sh

# ── Security (AppSec) — tools pinned in mise.toml, installed by `just setup-sec` ─

# One-time: install the AppSec toolchain via mise (idempotent)
setup-sec:
    mise install semgrep gitleaks osv-scanner hadolint syft grype cosign

# Run all source-side AppSec checks fail-fast
sec: sec-secrets sec-sast sec-deps sec-iac
    @echo "AppSec source checks passed"

# Secrets — gitleaks across the working tree + history
sec-secrets:
    mise exec -- gitleaks detect --source . --config .gitleaks.toml --verbose

# SAST — semgrep rule packs (SEMGREP_CONFIG; a directory of vendored rules offline)
sec-sast:
    #!/usr/bin/env bash
    set -euo pipefail
    cfg=(); for c in {{SEMGREP_CONFIG}}; do cfg+=(--config "$c"); done
    mise exec -- semgrep scan "${cfg[@]}" --error --metrics=off

# Dependencies — osv-scanner over go.mod + pnpm-lock.yaml (OSV_SCANNER_FLAGS: --offline …)
sec-deps:
    mise exec -- osv-scanner scan --config osv-scanner.toml --recursive {{OSV_SCANNER_FLAGS}} .

# IaC — hadolint on every Dockerfile
sec-iac:
    #!/usr/bin/env bash
    set -euo pipefail
    find services -name Dockerfile -print0 | xargs -0 -I{} mise exec -- hadolint --config .hadolint.yaml {}

# Go-native known-vulnerability scan (govulncheck, pinned in mise.toml; GOVULNDB for a mirror)
audit:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── govulncheck $m"; (cd "$m" && mise exec -- govulncheck ./...); done

# ── Container CVE / SBOM / signing ────────────────────────────────────────────

# Build a single service image locally (context = workspace root)
docker-build SVC:
    docker build {{DOCKER_BUILD_ARGS}} -f services/{{SVC}}/Dockerfile -t golang-basics-{{SVC}}:dev .

# Build all service images
docker-build-all:
    #!/usr/bin/env bash
    set -euo pipefail
    for s in {{SERVICES}}; do echo "── image $s"; docker build {{DOCKER_BUILD_ARGS}} -f "services/$s/Dockerfile" -t "golang-basics-$s:dev" .; done

# syft SBOM + grype CVE scan of a locally-built image (interactive)
docker-scan SVC:
    mise exec -- syft golang-basics-{{SVC}}:dev -o cyclonedx-json=sbom-{{SVC}}.json
    mise exec -- grype golang-basics-{{SVC}}:dev --config .grype.yaml

# Same scan but fail on HIGH+ — the CI variant
docker-scan-ci SVC:
    mise exec -- grype golang-basics-{{SVC}}:dev --config .grype.yaml --fail-on high

# Sign an image with cosign (key-mode, no Rekor); needs COSIGN_PRIVATE_KEY
docker-sign SVC TAG:
    mise exec -- cosign sign --key env://COSIGN_PRIVATE_KEY --tlog-upload=false golang-basics-{{SVC}}:{{TAG}}

# Offline-verify an image against cosign.pub
docker-verify SVC TAG:
    mise exec -- cosign verify --key cosign.pub --insecure-ignore-tlog=true golang-basics-{{SVC}}:{{TAG}}

# ── CI gates ──────────────────────────────────────────────────────────────────

# Standard pipeline: fmt-check → contracts-check → vet → lint → test
ci: fmt-check tidy-check contracts-check check lint test
    @echo "CI passed"

# Extended pipeline: + race tests + supply-chain audit
ci-full: fmt-check check lint test-race audit
    @echo "CI-full passed"

# ── Per-package delegation ────────────────────────────────────────────────────
# Forward any recipe to a single module's justfile, e.g. `just ping test`.

httpx +args:
    just --justfile libs/httpx/justfile {{args}}

resilient +args:
    just --justfile libs/resilient-http-client/justfile {{args}}

ping +args:
    just --justfile services/ping/justfile {{args}}

heartbeat +args:
    just --justfile services/heartbeat/justfile {{args}}

pgx +args:
    just --justfile libs/pgx/justfile {{args}}

valkey +args:
    just --justfile libs/valkey/justfile {{args}}

kafka +args:
    just --justfile libs/kafka/justfile {{args}}

otelx +args:
    just --justfile libs/otelx/justfile {{args}}

testx +args:
    just --justfile libs/testx/justfile {{args}}

contracts-mod +args:
    just --justfile libs/contracts/justfile {{args}}

tasks +args:
    just --justfile services/tasks/justfile {{args}}

consumer +args:
    just --justfile services/consumer/justfile {{args}}

# Run a recipe in every module's justfile, in dependency order
each RECIPE:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "══ $m: {{RECIPE}}"; just --justfile "$m/justfile" {{RECIPE}}; done

# ── Infra dependencies (Postgres + Valkey + Kafka via docker compose) ──────────

# Bring up the backing services (Postgres, Valkey, Kafka) in the background
infra-up:
    docker compose -f docker/deps.yml up -d

# Stop and remove the backing services + their volumes
infra-down:
    docker compose -f docker/deps.yml down -v

# Tail the backing-service logs
infra-logs:
    docker compose -f docker/deps.yml logs -f

# Build the app images (VERSION from git describe), then run the whole stack (deps + tasks + consumer)
stack-up: infra-up
    VERSION="$(git describe --tags --always --dirty)" docker compose -f docker/stack.yml up -d --build

# Same, with tracing exported to the local OTel Collector (needs `just obs-up`)
stack-up-otel: infra-up
    VERSION="$(git describe --tags --always --dirty)" docker compose -f docker/stack.yml -f docker/stack.otel.yml up -d --build

# Tear the whole stack down (app + deps + volumes)
stack-down:
    docker compose -f docker/stack.yml down -v
    docker compose -f docker/deps.yml down -v

# ── Observability (Jaeger + VictoriaMetrics + Grafana) ────────────────────────
# Optional; nothing in the app path needs it. Joins the deps network, so
# `just infra-up` has to have run first.

# Bring up collector :4317, Jaeger :16686, VictoriaMetrics :9095, Loki :3100, Grafana :3000
obs-up: infra-up
    docker compose -f docker/observability.yml up -d
    @echo "OTLP     localhost:4317 (gRPC) / :4318 (HTTP)  ← *_OTEL_EXPORTER_OTLP_ENDPOINT"
    @echo "Jaeger   http://localhost:16686"
    @echo "Metrics  http://localhost:9095"
    @echo "Loki     http://localhost:3100"
    @echo "Grafana  http://localhost:3000  (admin / admin, dashboard: golang-basics)"

# Stop the observability stack (keeps its volumes)
obs-down:
    docker compose -f docker/observability.yml down

# Stop it and wipe the metric/dashboard volumes
obs-down-wipe:
    docker compose -f docker/observability.yml down -v

# Tail the observability logs
obs-logs:
    docker compose -f docker/observability.yml logs -f

# ── Local bring-up (host services) ────────────────────────────────────────────

# Build + run every service on the host (logs+pids under .run/)
up *services:
    ./scripts/host-services-spawn.sh {{services}}

# Stop host services started by `just up`
down:
    ./scripts/host-services-spawn.sh --stop

# ── E2E (Playwright) ──────────────────────────────────────────────────────────

# Install the Node workspace (Playwright e2e deps; API tests need no browsers)
e2e-install:
    pnpm install

# Build the service binaries the e2e harness spawns
e2e-build: release

# Run the full Playwright e2e suite (spawns services itself)
e2e: e2e-build
    cd e2e && pnpm test

# Playwright interactive UI
e2e-ui: e2e-build
    cd e2e && pnpm test:ui

# Run e2e tests matching a string
e2e-filter GREP: e2e-build
    cd e2e && pnpm test --grep "{{GREP}}"

# Run the e2e suite including tasks + consumer (needs `just infra-up` first)
e2e-deps: e2e-build
    cd e2e && E2E_WITH_DEPS=1 pnpm test

# Open the last Playwright report
e2e-report:
    cd e2e && pnpm report

# ── k6 benchmarks ─────────────────────────────────────────────────────────────

# 50 VUs × 30s sanity load against ping
bench-smoke: release
    ./benchmarks/run-k6.sh smoke

# ramp 0→500 VUs (~3.5m)
bench-load: release
    ./benchmarks/run-k6.sh load

# ramp 0→2000 VUs (~4m)
bench-stress: release
    ./benchmarks/run-k6.sh stress

# 500 VUs × 30m (leak detection)
bench-soak: release
    ./benchmarks/run-k6.sh soak

# constant-arrival-rate peak profile
bench-peak: release
    ./benchmarks/run-k6.sh peak

# Load-test the tasks service (needs the deps up: `just infra-up`).
# PROFILE is one of smoke|load|stress|soak.
bench-tasks PROFILE="smoke":
    ./benchmarks/run-k6-tasks.sh {{PROFILE}}

# ── Contracts (TypeSpec → OpenAPI + JSON Schema → Go types) ───────────────────

# Regenerate every contract artefact from api/tsp: the OpenAPI document, the
# event JSON Schemas, and the Go types in libs/contracts. Commit the result.
contracts:
    #!/usr/bin/env bash
    set -euo pipefail
    pnpm --filter @golang-basics/api compile
    mise exec -- oapi-codegen -config api/oapi-codegen.yaml api/openapi3/tasks.openapi.yaml
    mise exec -- oapi-codegen -config api/oapi-codegen-ping.yaml api/openapi3/ping.openapi.yaml
    mise exec -- go-jsonschema -p events --only-models \
      --schema-root-type TaskCreatedEvent=TaskCreatedEvent \
      -o libs/contracts/events/task_created.gen.go api/jsonschema/TaskCreatedEvent.json
    (cd libs/contracts && gofmt -w . && go build ./...)

# CI gate: generated artefacts are up to date, and the HTTP contract has no
# breaking change against master (oasdiff). BASE overrides the git ref.
contracts-check BASE="origin/master":
    #!/usr/bin/env bash
    set -euo pipefail
    just contracts >/dev/null
    if ! git diff --exit-code --stat -- api/openapi3 api/jsonschema libs/contracts; then
      echo "contracts: generated files are stale — run 'just contracts' and commit" >&2; exit 1
    fi
    base="$(mktemp)"; trap 'rm -f "$base"' EXIT
    for spec in api/openapi3/*.openapi.yaml; do
      if git show "{{BASE}}:$spec" > "$base" 2>/dev/null; then
        mise exec -- oasdiff breaking "$base" "$spec" --fail-on ERR --err-ignore api/oasdiff-breaking.ignore
      else
        echo "contracts: no base for $spec at {{BASE}} (first version) — skipping breaking-change check"
      fi
    done
    echo "contracts OK"

# Property-based API testing against a real binary and its OpenAPI document
# (Schemathesis; results go to Allure). tasks needs `just infra-up` first.
schemathesis SVC="ping":
    scripts/schemathesis.sh {{SVC}}

# ── Setup & housekeeping ──────────────────────────────────────────────────────

# Install dev tools (Go toolchain + linters via mise) and sync the workspace
setup:
    mise install
    go work sync
    @echo "Toolchain installed (govulncheck, gotestsum, … come pinned from mise.toml) — run 'just setup-sec' for the AppSec tools"

# Wire git hooks → lefthook (opt-in per clone; bypass with LEFTHOOK=0)
hooks-install:
    pnpm install
    pnpm exec lefthook install

# Remove lefthook-managed git hooks
hooks-uninstall:
    pnpm exec lefthook uninstall

# Show the workspace dependency graph
deps:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "══ $m"; (cd "$m" && go list -m all); done

# Check for newer dependency versions (lines with a [vX] suffix have updates)
outdated:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "══ $m"; (cd "$m" && go list -m -u all 2>/dev/null | grep '\[' || echo "  all up to date"); done

# Remove build/test/coverage artefacts
clean:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do (cd "$m" && rm -f coverage.out && go clean ./...); done
    for s in {{SERVICES}}; do rm -rf "services/$s/bin"; done
    rm -rf e2e/playwright-report e2e/test-results e2e/.e2e-state.json benchmarks/results .run
    @echo "cleaned"
