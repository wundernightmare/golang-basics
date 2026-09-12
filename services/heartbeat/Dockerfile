# ─────────────────────────────────────────────────────────────────────────────
# heartbeat — multi-stage production image
#
# Build context — workspace ROOT (two levels up from this file). The build is
# per-module (GOWORK=off): it uses this service's own go.mod + its
# `replace … => ../../libs/httpx` directive, so it copies ONLY libs/httpx +
# this service — never go.work or sibling services. That keeps the layer cache
# isolated: editing another service does not invalidate this image.
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
#   HEARTBEAT_ADMIN_ADDR   health/metrics/pprof listen address (default :9081)
#   HEARTBEAT_INTERVAL     tick period                   (default 5s)
#   HEARTBEAT_LOG_LEVEL    log filter                    (default info)
#   HEARTBEAT_LOG_FORMAT   json | text                   (default json)
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

# Dependency layer — manifests only (this module + its replace target), so the
# module download is cached until a go.mod / go.sum changes. GOWORK=off makes
# the build resolve via go.mod's `replace`, not the workspace.
ENV GOWORK=off
COPY libs/httpx/go.mod libs/httpx/go.sum ./libs/httpx/
# libs/testx is a _test-only dependency of the service module; only its
# go.mod/go.sum are needed to resolve the module graph, no source is compiled.
COPY libs/testx/go.mod libs/testx/go.sum ./libs/testx/
COPY services/heartbeat/go.mod services/heartbeat/go.sum ./services/heartbeat/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd services/heartbeat && go mod download

# Source layer — only what this service compiles.
COPY libs/httpx ./libs/httpx
COPY services/heartbeat ./services/heartbeat

# Static, trimmed, stripped binary. CGO off → no dynamic libc dependency, so
# the distroless/static base suffices.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd services/heartbeat && CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X github.com/tracehubmmp/golang-basics/libs/httpx.Version=${VERSION}" \
      -o /out/heartbeat .

# ── Stage 2 – runtime ────────────────────────────────────────────────────────
# distroless/static contains only ca-certificates, tzdata and /etc/passwd —
# no shell, no package manager, nothing writable outside /tmp.
FROM ${GCR}/distroless/static-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="heartbeat" \
      org.opencontainers.image.description="Minimal background heartbeat worker (golang-basics)" \
      org.opencontainers.image.vendor="tracehub" \
      org.opencontainers.image.source="https://github.com/tracehubmmp/golang-basics" \
      org.opencontainers.image.licenses="UNLICENSED"

WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/heartbeat ./heartbeat

# Admin listener only (health, /metrics, /version, pprof) — a worker has no
# API. Publish it to the monitoring network, never through the ingress.
EXPOSE 9081

USER 65532:65532
ENTRYPOINT ["/app/heartbeat"]
