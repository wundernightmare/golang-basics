# ─────────────────────────────────────────────────────────────────────────────
# tasks — multi-stage production image
#
# Build context — workspace ROOT (two levels up from this file). The build is
# per-module (GOWORK=off): it uses this service's own go.mod + its
# `replace … => ../../libs/<lib>` directives, so it copies ONLY the libs this
# service imports (httpx, pgx, valkey, kafka, otelx) + this service — never
# go.work or sibling services. Editing another service does not invalidate this
# image's layer cache.
#
#   docker build -f services/tasks/Dockerfile --tag tasks:dev .   ← workspace root
#
# AppSec properties of the final image
# ─────────────────────────────────────
#   • Distroless static runtime — no shell, no package manager, no libc surface
#   • Non-root principal (uid 65532 / "nonroot")
#   • Fully static binary (CGO_ENABLED=0) — pgx, valkey-go and franz-go are all
#     pure Go, so no dynamic libc/libpq/librdkafka dependency ships
#   • No secrets, credentials or TLS material baked into any layer
#   • OCI annotations for supply-chain tooling (Cosign, Syft, etc.)
#
# Runtime injection (never bake these into the image)
# ────────────────────────────────────────────────────
#   TASKS_CONFIG         optional path to a mounted config.yaml
#   TASKS_DATABASE_URL   postgres://app:app@db:5432/app?sslmode=disable
#   TASKS_VALKEY_URL     valkey://cache:6379
#   TASKS_KAFKA_BROKERS  broker1:9092,broker2:9092
#   TASKS_OTEL_ENABLED   true to export traces to TASKS_OTEL_EXPORTER_OTLP_ENDPOINT
#
# Production image pinning
# ────────────────────────
# Replace the version tags with @sha256:… digests in CI for byte-for-byte
# reproducibility and protection against tag-mutation attacks.
#
# No `# syntax=docker/dockerfile:…` directive on purpose: it makes BuildKit
# pull the frontend image from Docker Hub on every build, which a closed
# network cannot do. Everything used here (`--mount=type=cache`) is in the
# frontend bundled with any current Docker / BuildKit.
# ─────────────────────────────────────────────────────────────────────────────

ARG GO_VERSION=1.27
ARG DEBIAN_CODENAME=bookworm
# Registry hosts — override with the pull-through cache of a closed network
# (`--build-arg DOCKER_HUB=harbor.internal/dockerhub`); see .env.example.
ARG DOCKER_HUB=docker.io
ARG GCR=gcr.io
# Stamped into libs/httpx.Version → build_info, /version, OTel service.version.
# CI passes the tag / commit sha; a local build leaves "dev".
ARG VERSION=dev

# ── Stage 1 – builder ────────────────────────────────────────────────────────
FROM ${DOCKER_HUB}/library/golang:${GO_VERSION}-${DEBIAN_CODENAME} AS builder
WORKDIR /build

# Module proxy / checksum-db settings for the builder, passed
# through from the environment (`--build-arg GOPROXY` with no value). Unset
# means the Go defaults (proxy.golang.org + sum.golang.org). HTTP_PROXY /
# HTTPS_PROXY / NO_PROXY are predefined build args and need no declaration.
ARG GOPROXY
ARG GOSUMDB
ARG GONOSUMDB
ARG GOFLAGS
ARG VERSION

# Dependency layer — manifests only (this module + its replace targets), so the
# module download is cached until a go.mod / go.sum changes. GOWORK=off makes
# the build resolve via go.mod's `replace`, not the workspace.
ENV GOWORK=off
COPY libs/httpx/go.mod libs/httpx/go.sum ./libs/httpx/
COPY libs/pgx/go.mod libs/pgx/go.sum ./libs/pgx/
COPY libs/valkey/go.mod libs/valkey/go.sum ./libs/valkey/
COPY libs/kafka/go.mod libs/kafka/go.sum ./libs/kafka/
COPY libs/otelx/go.mod libs/otelx/go.sum ./libs/otelx/
# libs/testx is a _test-only dependency of the service module; only its
# go.mod/go.sum are needed to resolve the module graph, no source is compiled.
COPY libs/contracts/go.mod ./libs/contracts/
COPY libs/testx/go.mod libs/testx/go.sum ./libs/testx/
COPY services/tasks/go.mod services/tasks/go.sum ./services/tasks/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd services/tasks && go mod download

# Source layer — only what this service compiles.
COPY libs/contracts ./libs/contracts
COPY libs/httpx ./libs/httpx
COPY libs/pgx ./libs/pgx
COPY libs/valkey ./libs/valkey
COPY libs/kafka ./libs/kafka
COPY libs/otelx ./libs/otelx
COPY services/tasks ./services/tasks

# Static, trimmed, stripped binary. CGO off → no dynamic libc dependency, so
# the distroless/static base suffices.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd services/tasks && CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X github.com/tracehubmmp/golang-basics/libs/httpx.Version=${VERSION}" \
      -o /out/tasks .

# ── Stage 2 – runtime ────────────────────────────────────────────────────────
# distroless/static contains only ca-certificates, tzdata and /etc/passwd —
# no shell, no package manager, nothing writable outside /tmp.
FROM ${GCR}/distroless/static-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="tasks" \
      org.opencontainers.image.description="Tasks CRUD service over Postgres + Valkey + Kafka (golang-basics)" \
      org.opencontainers.image.vendor="tracehub" \
      org.opencontainers.image.source="https://github.com/tracehubmmp/golang-basics" \
      org.opencontainers.image.licenses="UNLICENSED"

WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/tasks ./tasks

# API listener; the admin listener (health, /metrics, /version, pprof) is
# separate on purpose — publish 9082 only to the monitoring network, never
# through the ingress.
EXPOSE 8082
EXPOSE 9082

USER 65532:65532
ENTRYPOINT ["/app/tasks"]
