# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# ping — multi-stage production image
#
# Build context — workspace ROOT (two levels up from this file). The build is
# per-module (GOWORK=off): it uses this service's own go.mod + its
# `replace … => ../../libs/httpx` directive, so it copies ONLY libs/httpx +
# this service — never go.work or sibling services. That keeps the layer cache
# isolated: editing another service does not invalidate this image.
#
#   docker build -f services/ping/Dockerfile --tag ping:dev .   ← workspace root
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
#   PING_HTTP_ADDR     listen address (default :8080)
#   PING_LOG_LEVEL     log filter     (default info)
#   PING_LOG_FORMAT    json | text    (default json)
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

# Dependency layer — manifests only (this module + its replace target), so the
# module download is cached until a go.mod / go.sum changes. GOWORK=off makes
# the build resolve via go.mod's `replace`, not the workspace.
ENV GOWORK=off
COPY libs/httpx/go.mod libs/httpx/go.sum ./libs/httpx/
COPY services/ping/go.mod services/ping/go.sum ./services/ping/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd services/ping && go mod download

# Source layer — only what this service compiles.
COPY libs/httpx ./libs/httpx
COPY services/ping ./services/ping

# Static, trimmed, stripped binary. CGO off → no dynamic libc dependency, so
# the distroless/static base suffices.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd services/ping && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
      -o /out/ping .

# ── Stage 2 – runtime ────────────────────────────────────────────────────────
# distroless/static contains only ca-certificates, tzdata and /etc/passwd —
# no shell, no package manager, nothing writable outside /tmp.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="ping" \
      org.opencontainers.image.description="Minimal ping/pong HTTP service (golang-basics)" \
      org.opencontainers.image.vendor="tracehub" \
      org.opencontainers.image.source="https://github.com/tracehubmmp/golang-basics" \
      org.opencontainers.image.licenses="UNLICENSED"

WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/ping ./ping

# HTTP (and Prometheus /metrics on the same port)
EXPOSE 8080

USER 65532:65532
ENTRYPOINT ["/app/ping"]
