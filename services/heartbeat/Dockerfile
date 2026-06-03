# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# heartbeat — multi-stage production image
#
# Build context — workspace ROOT (two levels up from this file), so the build
# sees go.work + every module (a service module's `replace` points at
# ../../libs/httpx, which must be present in the context):
#
#   docker build -f services/heartbeat/Dockerfile --tag heartbeat:dev .   ← workspace root
#
# AppSec properties of the final image
# ─────────────────────────────────────
#   • Distroless static runtime — no shell, no package manager, no libc surface
#   • Non-root principal (uid 65532 / "nonroot")
#   • Fully static binary (CGO_ENABLED=0) — only the executable ships
#   • No secrets, credentials or TLS material baked into any layer
#   • OCI annotations for supply-chain tooling (Cosign, Syft, etc.)
#
# Runtime injection (never bake these into the image)
# ────────────────────────────────────────────────────
#   HEARTBEAT_HTTP_ADDR    health/metrics listen address (default :8081)
#   HEARTBEAT_INTERVAL     tick period                   (default 5s)
#   HEARTBEAT_LOG_LEVEL    log filter                    (default info)
#   HEARTBEAT_LOG_FORMAT   json | text                   (default json)
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

# Dependency layer — copy only the workspace + manifest files first so the
# module download is cached until a go.mod / go.sum / go.work changes.
COPY go.work go.work.sum* ./
COPY libs/httpx/go.mod libs/httpx/go.sum libs/httpx/
COPY services/ping/go.mod services/ping/go.sum services/ping/
COPY services/heartbeat/go.mod services/heartbeat/go.sum services/heartbeat/
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Source layer.
COPY libs ./libs
COPY services ./services

# Static, trimmed, stripped binary. CGO off → no dynamic libc dependency, so
# the distroless/static base suffices.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
      -o /out/heartbeat ./services/heartbeat

# ── Stage 2 – runtime ────────────────────────────────────────────────────────
# distroless/static contains only ca-certificates, tzdata and /etc/passwd —
# no shell, no package manager, nothing writable outside /tmp.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="heartbeat" \
      org.opencontainers.image.description="Minimal background heartbeat worker (golang-basics)" \
      org.opencontainers.image.vendor="tracehub" \
      org.opencontainers.image.source="https://github.com/tracehubmmp/golang-basics" \
      org.opencontainers.image.licenses="UNLICENSED"

WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/heartbeat ./heartbeat

# health/readiness + Prometheus /metrics
EXPOSE 8081

USER 65532:65532
ENTRYPOINT ["/app/heartbeat"]
