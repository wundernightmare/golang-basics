# syntax=docker/dockerfile:1.7
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
# ─────────────────────────────────────────────────────────────────────────────

ARG GO_VERSION=1.25
ARG DEBIAN_CODENAME=bookworm

# ── Stage 1 – builder ────────────────────────────────────────────────────────
FROM golang:${GO_VERSION}-${DEBIAN_CODENAME} AS builder
WORKDIR /build

# Dependency layer — manifests only (this module + its replace targets), so the
# module download is cached until a go.mod / go.sum changes. GOWORK=off makes
# the build resolve via go.mod's `replace`, not the workspace.
ENV GOWORK=off
COPY libs/httpx/go.mod libs/httpx/go.sum ./libs/httpx/
COPY libs/pgx/go.mod libs/pgx/go.sum ./libs/pgx/
COPY libs/valkey/go.mod libs/valkey/go.sum ./libs/valkey/
COPY libs/kafka/go.mod libs/kafka/go.sum ./libs/kafka/
COPY libs/otelx/go.mod libs/otelx/go.sum ./libs/otelx/
COPY services/tasks/go.mod services/tasks/go.sum ./services/tasks/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd services/tasks && go mod download

# Source layer — only what this service compiles.
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
    cd services/tasks && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
      -o /out/tasks .

# ── Stage 2 – runtime ────────────────────────────────────────────────────────
# distroless/static contains only ca-certificates, tzdata and /etc/passwd —
# no shell, no package manager, nothing writable outside /tmp.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="tasks" \
      org.opencontainers.image.description="Tasks CRUD service over Postgres + Valkey + Kafka (golang-basics)" \
      org.opencontainers.image.vendor="tracehub" \
      org.opencontainers.image.source="https://github.com/tracehubmmp/golang-basics" \
      org.opencontainers.image.licenses="UNLICENSED"

WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/tasks ./tasks

# HTTP (and Prometheus /metrics on the same port)
EXPOSE 8082

USER 65532:65532
ENTRYPOINT ["/app/tasks"]
