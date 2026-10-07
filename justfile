# golang-basics — workspace task runner
#
# Install: mise install (just is pinned in mise.toml) | brew install just
# Usage:   just <recipe>      | just --list
#
# A go.work workspace root is not itself a module, so the workspace-wide
# recipes fan out over each module dir. The module list is never written
# here: it comes from go.work through scripts/touched-modules.sh, the same
# script both CI pipelines and the git hooks use.
#
# Tools: every recipe runs its tool through `mise exec --` when mise is
# installed (so the pinned version is used without a shell hook), or from
# PATH when it is not (CI). Tools outside the core profile name their mise
# profile (MISE_ENV=appsec|perf|report, see mise.toml); `just setup-<profile>`
# installs one.

# Load the (gitignored) .env — proxy, GOPROXY, registry mirrors, scanner DB
# mirrors — into every recipe. Absent file = no-op. Knobs: .env.example.
set dotenv-load := true
set shell := ["bash", "-euo", "pipefail", "-c"]

# ── Discovery (go.work, mise*.toml) ───────────────────────────────────────────

# Every Go module in the workspace (go.work order).
MODULES := `scripts/touched-modules.sh --all | xargs`
# Service names (services/<name>) — each builds one binary / one image.
SERVICES := `scripts/touched-modules.sh --services | xargs`
# Modules with Docker-backed suites: their go.mod requires libs/testx/containers.
INTEGRATION_MODULES := `scripts/touched-modules.sh --integration | xargs`
# The Go pin, handed to every docker build (the Dockerfile requires it).
GO_VERSION := `scripts/mise-pins.sh | sed -n 's/^GO_VERSION=//p'`

# `mise exec --` when mise is installed, nothing when it is not (CI images).
MISE := `command -v mise >/dev/null 2>&1 && echo "mise exec --" || true`
# Profile prefixes: `{{APPSEC}} {{MISE}} gitleaks …` adds the profile to
# whatever MISE_ENV the caller already has.
APPSEC := 'MISE_ENV="${MISE_ENV:+$MISE_ENV,}appsec"'
PERF := 'MISE_ENV="${MISE_ENV:+$MISE_ENV,}perf"'
REPORT := 'MISE_ENV="${MISE_ENV:+$MISE_ENV,}report"'

# Build stamp → libs/httpx.Version / Revision / BuildTime (binaries + images).
# BUILD_TIME defaults to the HEAD commit time: the same commit stamps the
# same time, so rebuilds are reproducible.
VERSION := env("VERSION", `git describe --tags --always --dirty 2>/dev/null || echo dev`)
GIT_SHA := env("GIT_SHA", `git rev-parse HEAD 2>/dev/null || true`)
BUILD_TIME := env("BUILD_TIME", `TZ=UTC git log -1 --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ 2>/dev/null || true`)
STAMP := "VERSION='" + VERSION + "' GIT_SHA='" + GIT_SHA + "' BUILD_TIME='" + BUILD_TIME + "'"

# Every `docker build`: the required GO_VERSION + build stamp, then the
# closed-network knobs in value-less form (taken from the environment, skipped
# when unset, so an open-network build sees the Dockerfile defaults).
DOCKER_BUILD_ARGS := "--build-arg GO_VERSION=" + GO_VERSION + " --build-arg VERSION=" + VERSION + " --build-arg GIT_SHA=" + GIT_SHA + " --build-arg BUILD_TIME=" + BUILD_TIME + " --build-arg DOCKER_HUB --build-arg GCR --build-arg GOPROXY --build-arg GOSUMDB --build-arg GONOSUMDB --build-arg GOFLAGS --build-arg HTTP_PROXY --build-arg HTTPS_PROXY --build-arg NO_PROXY"

# Scanner inputs that a closed network points at vendored rules / offline DBs.
SEMGREP_CONFIG := env("SEMGREP_CONFIG", "p/owasp-top-ten p/golang")
OSV_SCANNER_FLAGS := env("OSV_SCANNER_FLAGS", "")

# Where e2e binaries and reports go (gitignored).
BUILD_DIR := justfile_directory() / ".build"
JUNIT_DIR := justfile_directory() / ".reports/junit"

# Show all available recipes
default:
    @just --list --unsorted

# ── Setup ─────────────────────────────────────────────────────────────────────

# Core toolchain: go, golangci-lint, just, shellcheck, gotestsum, contracts
# tools, node + pnpm. Profiles are opt-in (setup-<profile>).

# Core toolchain + pnpm workspace + go work sync (fast; profiles are opt-in)
setup:
    mise install
    go work sync
    {{MISE}} pnpm install --frozen-lockfile
    @echo "Core toolchain ready. Profiles: just setup-appsec | setup-ide | setup-perf | setup-report"

# AppSec profile: semgrep, gitleaks, osv-scanner, hadolint, syft, grype, cosign, govulncheck
setup-appsec:
    {{APPSEC}} mise install

alias setup-sec := setup-appsec

# IDE profile: gopls, dlv, gotests, gomodifytags, impl (export MISE_ENV=ide for VS Code)
setup-ide:
    MISE_ENV="${MISE_ENV:+$MISE_ENV,}ide" mise install

# Perf profile: k6, benchstat, gremlins, go-test-coverage, gocover-cobertura
setup-perf:
    {{PERF}} mise install

# Report profile: java (the Allure renderer's JRE)
setup-report:
    {{REPORT}} mise install

# Wire git hooks → lefthook (opt-in per clone; bypass with LEFTHOOK=0)
hooks-install:
    {{MISE}} pnpm install --frozen-lockfile
    {{MISE}} pnpm exec lefthook install

# Remove lefthook-managed git hooks
hooks-uninstall:
    {{MISE}} pnpm exec lefthook uninstall

# ── Workspace build ───────────────────────────────────────────────────────────

# go vet every module (fastest workspace-wide feedback; `-tags e2e` so the e2e suite is vetted too)
vet:
    for m in {{MODULES}}; do echo "── vet $m"; (cd "$m" && go vet -tags e2e ./...); done

alias check := vet

# Debug-build every module
build:
    for m in {{MODULES}}; do echo "── build $m"; (cd "$m" && go build ./...); done

# Stripped release binaries for every service into services/<svc>/bin/ (scripts/build-service.sh)
release:
    for s in {{SERVICES}}; do echo "── release services/$s"; {{STAMP}} scripts/build-service.sh "$s"; done

# ── Workspace test ────────────────────────────────────────────────────────────

# Docker-backed suites need a daemon; on OrbStack / rootless Docker export
# DOCKER_HOST (README "Tests"). Shuffled like CI.

# Every module's full suite (Docker-backed ones need a daemon)
test *args:
    for m in {{MODULES}}; do echo "── test $m"; (cd "$m" && go test -shuffle=on ./... {{args}}); done

# Unit tests only (-short: the Docker-backed suites skip) — what `just ci` runs
test-short *args:
    for m in {{MODULES}}; do echo "── test -short $m"; (cd "$m" && go test -short -shuffle=on ./... {{args}}); done

# Docker-backed suites (modules requiring libs/testx/containers) under -race
test-integration *args:
    for m in {{INTEGRATION_MODULES}}; do echo "── integration $m"; (cd "$m" && go test -race -shuffle=on ./... {{args}}); done

# Every module's tests with the race detector
test-race *args:
    for m in {{MODULES}}; do echo "── test -race $m"; (cd "$m" && go test -race -shuffle=on ./... {{args}}); done

# JUnit XML per module into .reports/junit/<module>.xml (gotestsum), e.g. `just test-junit -short`
test-junit *args:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{JUNIT_DIR}}"
    status=0
    for m in {{MODULES}}; do
      echo "── junit $m"
      (cd "$m" && {{MISE}} gotestsum --format testname --junitfile "{{JUNIT_DIR}}/${m//\//-}.xml" -- -shuffle=on ./... {{args}}) || status=1
    done
    exit $status

# Flakiness hunt: every module three times, shuffled, under -race (CI nightly)
stress:
    for m in {{MODULES}}; do echo "── stress $m"; (cd "$m" && go test -count=3 -race -shuffle=on ./...); done

# Run every Fuzz* target for FUZZTIME each (default 20s); crashers land in testdata/fuzz/
fuzz FUZZTIME="20s":
    FUZZTIME={{FUZZTIME}} scripts/fuzz.sh

# Micro-benchmarks of every module (benchstat-ready) into OUT
bench OUT="bench.txt":
    scripts/bench.sh {{OUT}}

# benchstat diff of two `just bench` outputs (perf profile)
bench-compare BASE HEAD:
    {{PERF}} {{MISE}} benchstat {{BASE}} {{HEAD}}

# Per-module coverage summary (statements, this module's packages only)
cov:
    for m in {{MODULES}}; do echo "── cov $m"; (cd "$m" && go test -short -covermode=atomic -coverprofile=coverage.out ./... >/dev/null && go tool cover -func=coverage.out | tail -1); done

# Coverage per layer into .cover/<layer>: unit | integration | e2e (scripts/cover.sh)
cov-layer LAYER:
    {{PERF}} scripts/cover.sh {{LAYER}}

# Gates whatever is under .cover/ now — `just cov-all` is the safe entry
# point; alone, this re-gates a cov-all that just ran.

# Merge the collected coverage layers and gate them (.testcoverage.yml, perf profile)
cov-check:
    for l in unit integration e2e; do if [ -n "$(ls -A ".cover/$l" 2>/dev/null)" ]; then echo "cov-check: layer $l found"; else echo "cov-check: layer $l MISSING — partial merge; use just cov-all"; fi; done
    {{PERF}} scripts/cover.sh merge
    {{PERF}} {{MISE}} go-test-coverage --config .testcoverage.yml

# Integration needs Docker; e2e builds cover-instrumented binaries.

# All three coverage layers (unit, integration, e2e), then the merged gate
cov-all:
    rm -rf .cover
    {{PERF}} scripts/cover.sh unit
    {{PERF}} scripts/cover.sh integration
    {{PERF}} scripts/cover.sh e2e
    just cov-check

# GOFLAGS=-count=1: the per-mutant timeout comes from the coverage run, a
# cached run would time every mutant out.

# Mutation testing (gremlins, perf profile); config: <module>/.gremlins.yaml
mutate MODULE="libs/resilient-http-client":
    cd {{MODULE}} && GOFLAGS=-count=1 {{PERF}} {{MISE}} gremlins unleash --output "{{justfile_directory()}}/gremlins-$(basename {{MODULE}}).json"

# Input: the JUnit XML in .reports/junit (`just test-junit`, `just
# schemathesis`). allure-commandline is in the root package.json.

# Allure HTML report from .reports/junit → .reports/allure (report profile: JRE)
allure:
    #!/usr/bin/env bash
    set -euo pipefail
    compgen -G "{{JUNIT_DIR}}/*.xml" >/dev/null || { echo "no JUnit XML in {{JUNIT_DIR}} — run just test-junit first" >&2; exit 1; }
    [ -d node_modules/allure-commandline ] || {{MISE}} pnpm install --frozen-lockfile
    results=".reports/allure-results"
    rm -rf "$results" && mkdir -p "$results" && cp "{{JUNIT_DIR}}"/*.xml "$results/"
    scripts/allure-meta.sh "$results"
    {{REPORT}} {{MISE}} pnpm exec allure generate --clean --single-file -o .reports/allure "$results"
    echo "report: .reports/allure/index.html"

alias allure-report := allure

# ── Lint & format ─────────────────────────────────────────────────────────────

# golangci-lint in every module (config: .golangci.yml) + shellcheck
lint: lint-sh
    for m in {{MODULES}}; do echo "── lint $m"; (cd "$m" && {{MISE}} golangci-lint run --build-tags e2e); done

# shellcheck every shell script (scripts/, benchmarks/)
lint-sh:
    {{MISE}} shellcheck scripts/*.sh benchmarks/*.sh

# golangci-lint --fix in every module
lint-fix:
    for m in {{MODULES}}; do (cd "$m" && {{MISE}} golangci-lint run --build-tags e2e --fix); done

# Scoped to the module dirs, never `.`: CI keeps GOMODCACHE under .cache/,
# whose test fixtures a bare `gofmt .` would walk into.

# Format all Go source in place (gofmt + golangci-lint fmt)
fmt:
    gofmt -w {{MODULES}}
    for m in {{MODULES}}; do (cd "$m" && {{MISE}} golangci-lint fmt); done

# Formatting gate (CI) — same module scope as `fmt`
fmt-check:
    out="$(gofmt -l {{MODULES}})"; if [ -n "$out" ]; then echo "gofmt needed in:"; echo "$out"; exit 1; fi; echo "gofmt clean"

# go mod tidy every module + go work sync
tidy:
    for m in {{MODULES}}; do echo "── tidy $m"; (cd "$m" && go mod tidy); done
    go work sync

# Gate: fail if `just tidy` would change anything — every go.mod / go.sum tidy
# on its own (GOWORK=off, go.mod replaces) and `go work sync` a no-op.
# scripts/tidy-check.sh is the one script CI, GitLab and the pre-push hook run.
tidy-check:
    scripts/tidy-check.sh

# ── CI gates ──────────────────────────────────────────────────────────────────

# Contracts are a separate gate: `just contracts-check` (needs node/pnpm).

# Pure-Go pipeline (no Node, no Docker): fmt-check → tidy-check → vet → lint → test-short
ci: fmt-check tidy-check vet lint test-short
    @echo "CI passed"

# Extended: + race tests (incl. Docker-backed suites) + govulncheck
ci-full: ci test-race audit
    @echo "CI-full passed"

# ── Per-module delegation ─────────────────────────────────────────────────────

# Run recipe(s) from one module's justfile: `just mod services/ping test`
mod MODULE +args:
    just --justfile "{{MODULE}}/justfile" {{args}}

# Run a recipe in every module's justfile (go.work order)
each RECIPE:
    for m in {{MODULES}}; do echo "══ $m: {{RECIPE}}"; just --justfile "$m/justfile" {{RECIPE}}; done

# ── Security (AppSec profile: `just setup-appsec`) ────────────────────────────

# All source-side AppSec checks, fail-fast
sec: sec-waivers sec-secrets sec-sast sec-deps sec-iac
    @echo "AppSec source checks passed"

# Expired `Remove after YYYY-MM-DD` waivers anywhere in the repo
sec-waivers:
    scripts/check-waivers.sh

# Secrets — gitleaks across the working tree + history
sec-secrets:
    {{APPSEC}} {{MISE}} gitleaks detect --source . --config .gitleaks.toml --verbose

# SAST — semgrep rule packs (SEMGREP_CONFIG; a directory of vendored rules offline)
sec-sast:
    #!/usr/bin/env bash
    set -euo pipefail
    cfg=(); for c in {{SEMGREP_CONFIG}}; do cfg+=(--config "$c"); done
    {{APPSEC}} {{MISE}} semgrep scan "${cfg[@]}" --error --metrics=off

# Dependencies — osv-scanner over every go.mod + pnpm-lock.yaml (OSV_SCANNER_FLAGS: --offline …)
sec-deps:
    {{APPSEC}} {{MISE}} osv-scanner scan --config osv-scanner.toml --recursive {{OSV_SCANNER_FLAGS}} .

# IaC — hadolint on the Dockerfile(s) under docker/
sec-iac:
    find docker -name '*Dockerfile' -print0 | xargs -0 -I{} env {{APPSEC}} {{MISE}} hadolint --config .hadolint.yaml {}

# Go-native reachable-vulnerability scan per module (GOVULNDB for a mirror)
audit:
    for m in {{MODULES}}; do echo "── govulncheck $m"; (cd "$m" && {{APPSEC}} {{MISE}} govulncheck ./...); done

# ── Container images (docker/service.Dockerfile) ──────────────────────────────

# Build one service image (context = workspace root) → golang-basics-SVC:dev
docker-build SVC:
    docker build --load {{DOCKER_BUILD_ARGS}} --build-arg SERVICE={{SVC}} -f docker/service.Dockerfile -t golang-basics-{{SVC}}:dev .

# Build every service image
docker-build-all:
    for s in {{SERVICES}}; do echo "── image $s"; just docker-build "$s"; done

# syft SBOM + grype CVE scan of a locally built image (appsec profile)
docker-scan SVC:
    {{APPSEC}} {{MISE}} syft golang-basics-{{SVC}}:dev -o cyclonedx-json=sbom-{{SVC}}.json
    {{APPSEC}} {{MISE}} grype golang-basics-{{SVC}}:dev --config .grype.yaml

# Same scan, failing on HIGH+ — the CI variant
docker-scan-ci SVC:
    {{APPSEC}} {{MISE}} grype golang-basics-{{SVC}}:dev --config .grype.yaml --fail-on high

# Sign an image with cosign (key-mode, no Rekor); needs COSIGN_PRIVATE_KEY
docker-sign SVC TAG:
    {{APPSEC}} {{MISE}} cosign sign --key env://COSIGN_PRIVATE_KEY --tlog-upload=false golang-basics-{{SVC}}:{{TAG}}

# Offline-verify an image against cosign.pub
docker-verify SVC TAG:
    {{APPSEC}} {{MISE}} cosign verify --key cosign.pub --insecure-ignore-tlog=true golang-basics-{{SVC}}:{{TAG}}

# ── Infra (docker/deps.yml: Postgres + Valkey + Redpanda, singleton) ──────────

# Bring up the backing services and wait until every healthcheck passes
infra-up:
    docker compose -f docker/deps.yml up -d --wait

# Stop and remove the backing services + their volumes
infra-down:
    docker compose -f docker/deps.yml down -v

# Tail the backing-service logs
infra-logs:
    docker compose -f docker/deps.yml logs -f

# Deps + all four service images (built with the stamp), waiting for every /readyz
stack-up: infra-up
    GO_VERSION={{GO_VERSION}} {{STAMP}} docker compose -f docker/stack.yml up -d --build --wait

# Same, exporting traces to the local collector (needs `just obs-up`)
stack-up-otel: infra-up
    GO_VERSION={{GO_VERSION}} {{STAMP}} docker compose -f docker/stack.yml -f docker/stack.otel.yml up -d --build --wait

# Tear the whole stack down (app + deps + volumes)
stack-down:
    docker compose -f docker/stack.yml down -v
    docker compose -f docker/deps.yml down -v

# ── Observability (optional; joins the deps network) ──────────────────────────

# Collector :4318, Jaeger :16686, VictoriaMetrics :9095, vmalert :8880, Loki :3100, Grafana :3000
obs-up: infra-up
    docker compose -f docker/observability.yml up -d
    @echo "OTLP     http://localhost:4318 (OTLP/HTTP) ← *_OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318 + *_OTEL_EXPORTER_OTLP_INSECURE=true"
    @echo "Jaeger   http://localhost:16686"
    @echo "Metrics  http://localhost:9095   alerts: http://localhost:8880 (vmalert, docker/observability/rules.yml)"
    @echo "Loki     http://localhost:3100"
    @echo "Grafana  http://localhost:3000   (admin / admin, dashboard: golang-basics)"

# Stop the observability stack (keeps its volumes)
obs-down:
    docker compose -f docker/observability.yml down

# Stop it and wipe the metric/dashboard volumes
obs-down-wipe:
    docker compose -f docker/observability.yml down -v

# Tail the observability logs
obs-logs:
    docker compose -f docker/observability.yml logs -f

# ── Local bring-up (host processes) ───────────────────────────────────────────

# Build + run ping / heartbeat on the host (logs + pids under .run/)
up *services:
    scripts/host-services-spawn.sh {{services}}

# Stop host services started by `just up`
down:
    scripts/host-services-spawn.sh --stop

# ── E2E (Go suite in e2e/, build tag `e2e`) ───────────────────────────────────

# Build the service binaries the suite spawns into .build/ (COVER=1: cover-instrumented)
e2e-build:
    mkdir -p "{{BUILD_DIR}}"
    for s in {{SERVICES}}; do echo "── build $s${COVER:+ (cover)}"; {{STAMP}} scripts/build-service.sh "$s" "{{BUILD_DIR}}/$s"; done

# With `just infra-up` running, the data flow (tasks → Kafka → consumer)
# runs too — its URLs are read from docker/deps.yml's published ports;
# without the deps those tests skip.

# Run the Go e2e suite against the binaries in .build/
e2e *args: e2e-build
    #!/usr/bin/env bash
    set -euo pipefail
    export E2E_BIN_DIR="${E2E_BIN_DIR:-{{BUILD_DIR}}}"
    port() { docker compose -f docker/deps.yml port "$1" "$2" 2>/dev/null | tail -1; }
    if command -v docker >/dev/null && pg="$(port postgres 5432)" && [ -n "$pg" ]; then
      vk="$(port valkey 6379)"; kf="$(port redpanda 9092)"
      export E2E_DATABASE_URL="${E2E_DATABASE_URL:-postgres://app:app@$pg/app?sslmode=disable}"
      export E2E_VALKEY_URL="${E2E_VALKEY_URL:-valkey://$vk}"
      export E2E_KAFKA_BROKERS="${E2E_KAFKA_BROKERS:-$kf}"
      echo "e2e: data flow against docker/deps.yml (postgres $pg, valkey $vk, kafka $kf)"
    else
      echo "e2e: docker/deps.yml not up — data-flow tests skip (just infra-up to include them)"
    fi
    cd e2e && go test -tags e2e -count=1 ./... {{args}}

# e2e with cover-instrumented binaries; counters land in .cover/e2e (merged by cov-check)
e2e-cover *args:
    rm -rf .cover/e2e && mkdir -p .cover/e2e
    COVER=1 E2E_COVER_DIR="{{justfile_directory()}}/.cover/e2e" just e2e {{args}}

# ── k6 load tests (perf profile) ──────────────────────────────────────────────

# 50 VUs × 30s sanity load against ping
bench-smoke:
    {{PERF}} {{MISE}} benchmarks/run-k6.sh smoke

# ramp 0→500 VUs (~3.5m)
bench-load:
    {{PERF}} {{MISE}} benchmarks/run-k6.sh load

# ramp 0→2000 VUs (~4m)
bench-stress:
    {{PERF}} {{MISE}} benchmarks/run-k6.sh stress

# 500 VUs × 30m (leak detection)
bench-soak:
    {{PERF}} {{MISE}} benchmarks/run-k6.sh soak

# constant-arrival-rate peak profile
bench-peak:
    {{PERF}} {{MISE}} benchmarks/run-k6.sh peak

# Load-test tasks (needs `just infra-up`); PROFILE: smoke|load|stress|soak
bench-tasks PROFILE="smoke":
    {{PERF}} {{MISE}} benchmarks/run-k6-tasks.sh {{PROFILE}}

# ── Contracts (TypeSpec → OpenAPI + JSON Schema → Go types) ───────────────────

# OpenAPI for ping + tasks, the event JSON Schemas, and the Go types in
# libs/contracts. Commit the result.

# Regenerate every contract artefact from api/tsp
contracts:
    [ -d api/node_modules ] || {{MISE}} pnpm install --frozen-lockfile
    {{MISE}} pnpm --filter @golang-basics/api compile
    {{MISE}} oapi-codegen -config api/oapi-codegen.yaml api/openapi3/tasks.openapi.yaml
    {{MISE}} oapi-codegen -config api/oapi-codegen-ping.yaml api/openapi3/ping.openapi.yaml
    {{MISE}} go-jsonschema -p events --only-models --schema-root-type TaskCreatedEvent=TaskCreatedEvent -o libs/contracts/events/task_created.gen.go api/jsonschema/TaskCreatedEvent.json
    {{MISE}} go-jsonschema -p events --only-models --schema-root-type TaskUpdatedEvent=TaskUpdatedEvent -o libs/contracts/events/task_updated.gen.go api/jsonschema/TaskUpdatedEvent.json
    {{MISE}} go-jsonschema -p events --only-models --schema-root-type TaskDeletedEvent=TaskDeletedEvent -o libs/contracts/events/task_deleted.gen.go api/jsonschema/TaskDeletedEvent.json
    cd libs/contracts && gofmt -w . && go build ./...

# Both pipelines' `contracts` job: generated artefacts are up to date, and no
# OpenAPI document (every api/openapi3/*.openapi.yaml) has a breaking change
# against BASE (oasdiff; waivers in api/oasdiff-breaking.ignore).

# CI gate: contracts regenerate to the committed files, no breaking change vs BASE
contracts-check BASE="origin/master":
    #!/usr/bin/env bash
    set -euo pipefail
    just contracts >/dev/null
    if ! git diff --exit-code --stat -- api/openapi3 api/jsonschema ':(glob)libs/contracts/**/*.go'; then
      echo "contracts: generated files are stale — run 'just contracts' and commit" >&2; exit 1
    fi
    if [ -n "$(git status --porcelain --untracked-files=all -- api/openapi3 api/jsonschema ':(glob)libs/contracts/**/*.go')" ]; then
      git status --short -- api/openapi3 api/jsonschema ':(glob)libs/contracts/**/*.go'
      echo "contracts: generation produced untracked files — commit them" >&2; exit 1
    fi
    base="$(mktemp)"; trap 'rm -f "$base"' EXIT
    for spec in api/openapi3/*.openapi.yaml; do
      if git show "{{BASE}}:$spec" > "$base" 2>/dev/null; then
        echo "── oasdiff $spec against {{BASE}}"
        {{MISE}} oasdiff breaking "$base" "$spec" --fail-on ERR --err-ignore api/oasdiff-breaking.ignore
      else
        echo "contracts: no $spec at {{BASE}} (first version) — skipping the breaking-change check"
      fi
    done
    echo "contracts OK"

# Property-based API testing (Schemathesis; JUnit → .reports/junit); tasks needs infra-up
schemathesis SVC="ping":
    scripts/schemathesis.sh {{SVC}}

# ── Housekeeping ──────────────────────────────────────────────────────────────

# Show every module's dependency graph
deps:
    for m in {{MODULES}}; do echo "══ $m"; (cd "$m" && go list -m all); done

# Newer dependency versions (lines with a [vX] suffix have updates)
outdated:
    for m in {{MODULES}}; do echo "══ $m"; (cd "$m" && go list -m -u all 2>/dev/null | grep '\[' || echo "  all up to date"); done

# Remove build/test/coverage artefacts
clean:
    for m in {{MODULES}}; do (cd "$m" && rm -f coverage.out && go clean ./...); done
    for s in {{SERVICES}}; do rm -rf "services/$s/bin"; done
    rm -rf .build .reports .cover .run benchmarks/results coverage-merged.out gremlins-*.json
    @echo "cleaned"
