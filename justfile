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

# Every Go module in the workspace, in dependency order (libs first).
MODULES := "libs/httpx libs/resilient-http-client libs/pgx libs/valkey libs/kafka libs/otelx services/ping services/heartbeat services/tasks services/consumer"
# Buildable service binaries (module dir : binary name).
SERVICES := "ping heartbeat tasks consumer"

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
release:
    #!/usr/bin/env bash
    set -euo pipefail
    for s in {{SERVICES}}; do
      echo "── release services/$s"
      (cd "services/$s" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "bin/$s" .)
    done

# ── Workspace test ────────────────────────────────────────────────────────────

# Run every module's tests
test *args:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── test $m"; (cd "$m" && go test ./... {{args}}); done

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

# Format all Go source in-place (gofmt + golangci-lint fmt)
fmt:
    gofmt -w .
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do (cd "$m" && golangci-lint fmt); done

# Check formatting without modifying files (CI gate)
fmt-check:
    #!/usr/bin/env bash
    set -euo pipefail
    out="$(gofmt -l .)"
    if [ -n "$out" ]; then echo "gofmt needed in:"; echo "$out"; exit 1; fi
    echo "gofmt clean"

# Tidy every module's go.mod / go.sum
tidy:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── tidy $m"; (cd "$m" && go mod tidy); done
    go work sync

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

# SAST — semgrep OWASP + Go rule packs
sec-sast:
    mise exec -- semgrep --config p/owasp-top-ten --config p/golang --error

# Dependencies — osv-scanner over go.mod + pnpm-lock.yaml
sec-deps:
    mise exec -- osv-scanner scan --config osv-scanner.toml --recursive .

# IaC — hadolint on every Dockerfile
sec-iac:
    #!/usr/bin/env bash
    set -euo pipefail
    find services -name Dockerfile -print0 | xargs -0 -I{} mise exec -- hadolint --config .hadolint.yaml {}

# Go-native known-vulnerability scan (govulncheck) across every module
audit:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in {{MODULES}}; do echo "── govulncheck $m"; (cd "$m" && go run golang.org/x/vuln/cmd/govulncheck@latest ./...); done

# ── Container CVE / SBOM / signing ────────────────────────────────────────────

# Build a single service image locally (context = workspace root)
docker-build SVC:
    docker build -f services/{{SVC}}/Dockerfile -t golang-basics-{{SVC}}:dev .

# Build all service images
docker-build-all:
    #!/usr/bin/env bash
    set -euo pipefail
    for s in {{SERVICES}}; do echo "── image $s"; docker build -f "services/$s/Dockerfile" -t "golang-basics-$s:dev" .; done

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

# Standard pipeline: fmt-check → vet → lint → test
ci: fmt-check check lint test
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

# Build the app images, then run the whole stack (deps + tasks + consumer)
stack-up: infra-up
    docker compose -f docker/stack.yml up -d --build

# Tear the whole stack down (app + deps + volumes)
stack-down:
    docker compose -f docker/stack.yml down -v
    docker compose -f docker/deps.yml down -v

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

# ── Setup & housekeeping ──────────────────────────────────────────────────────

# Install dev tools (Go toolchain + linters via mise) and sync the workspace
setup:
    mise install
    go work sync
    @echo "Installing Go dev tools…"
    go install golang.org/x/vuln/cmd/govulncheck@latest
    @echo "Dev tools installed — run 'just setup-sec' for the AppSec toolchain"

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
