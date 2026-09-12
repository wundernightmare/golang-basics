# ─────────────────────────────────────────────────────────────────────────────
# consumer — multi-stage production image
#
# Build context — workspace ROOT (two levels up from this file). Per-module
# build (GOWORK=off) via go.mod's `replace` directives: copies only the libs
# this service imports (httpx, kafka, otelx) + this service.
#
#   docker build -f services/consumer/Dockerfile --tag consumer:dev .   ← workspace root
#
# AppSec: distroless static runtime, non-root (uid 65532), fully static
# (CGO_ENABLED=0; franz-go is pure Go, no librdkafka), no secrets baked in.
#
# Runtime injection (never bake these into the image)
# ────────────────────────────────────────────────────
#   CONSUMER_KAFKA_BROKERS  broker1:9092,broker2:9092
#   CONSUMER_KAFKA_TOPIC    tasks.events
#   CONSUMER_KAFKA_GROUP    tasks-consumer
#   CONSUMER_OTEL_ENABLED   true to export traces
# ─────────────────────────────────────────────────────────────────────────────

# No `# syntax=docker/dockerfile:…` directive on purpose: it makes BuildKit pull
# the frontend image from Docker Hub on every build, which a closed network
# cannot do; `--mount=type=cache` is in the frontend bundled with any current
# Docker / BuildKit.
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
ENV GOWORK=off

COPY libs/httpx/go.mod libs/httpx/go.sum ./libs/httpx/
COPY libs/kafka/go.mod libs/kafka/go.sum ./libs/kafka/
COPY libs/otelx/go.mod libs/otelx/go.sum ./libs/otelx/
# libs/testx is a _test-only dependency of the service module; only its
# go.mod/go.sum are needed to resolve the module graph, no source is compiled.
COPY libs/testx/go.mod libs/testx/go.sum ./libs/testx/
COPY services/consumer/go.mod services/consumer/go.sum ./services/consumer/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd services/consumer && go mod download

COPY libs/httpx ./libs/httpx
COPY libs/kafka ./libs/kafka
COPY libs/otelx ./libs/otelx
COPY services/consumer ./services/consumer

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd services/consumer && CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X github.com/tracehubmmp/golang-basics/libs/httpx.Version=${VERSION}" \
      -o /out/consumer .

# ── Stage 2 – runtime ────────────────────────────────────────────────────────
FROM ${GCR}/distroless/static-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="consumer" \
      org.opencontainers.image.description="Kafka consumer worker for task.created events (golang-basics)" \
      org.opencontainers.image.vendor="tracehub" \
      org.opencontainers.image.source="https://github.com/tracehubmmp/golang-basics" \
      org.opencontainers.image.licenses="UNLICENSED"

WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/consumer ./consumer

# Admin listener only (health, /metrics, /version, pprof) — a worker has no
# API. Publish it to the monitoring network, never through the ingress.
EXPOSE 9083

USER 65532:65532
ENTRYPOINT ["/app/consumer"]
