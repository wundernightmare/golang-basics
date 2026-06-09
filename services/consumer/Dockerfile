# syntax=docker/dockerfile:1.7
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

ARG GO_VERSION=1.25
ARG DEBIAN_CODENAME=bookworm

# ── Stage 1 – builder ────────────────────────────────────────────────────────
FROM golang:${GO_VERSION}-${DEBIAN_CODENAME} AS builder
WORKDIR /build
ENV GOWORK=off

COPY libs/httpx/go.mod libs/httpx/go.sum ./libs/httpx/
COPY libs/kafka/go.mod libs/kafka/go.sum ./libs/kafka/
COPY libs/otelx/go.mod libs/otelx/go.sum ./libs/otelx/
COPY services/consumer/go.mod services/consumer/go.sum ./services/consumer/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd services/consumer && go mod download

COPY libs/httpx ./libs/httpx
COPY libs/kafka ./libs/kafka
COPY libs/otelx ./libs/otelx
COPY services/consumer ./services/consumer

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd services/consumer && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
      -o /out/consumer .

# ── Stage 2 – runtime ────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="consumer" \
      org.opencontainers.image.description="Kafka consumer worker for task.created events (golang-basics)" \
      org.opencontainers.image.vendor="tracehub" \
      org.opencontainers.image.source="https://github.com/tracehubmmp/golang-basics" \
      org.opencontainers.image.licenses="UNLICENSED"

WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/consumer ./consumer

# Health/metrics HTTP (Prometheus /metrics on the same port)
EXPOSE 8083

USER 65532:65532
ENTRYPOINT ["/app/consumer"]
