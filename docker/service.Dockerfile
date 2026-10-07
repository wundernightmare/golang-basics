# check=skip=InvalidDefaultArgInFrom
# ─────────────────────────────────────────────────────────────────────────────
# One image recipe for every service — parametrised by SERVICE.
#
#   docker build -f docker/service.Dockerfile \
#     --build-arg SERVICE=ping \
#     --build-arg GO_VERSION="$(scripts/mise-pins.sh | sed -n 's/^GO_VERSION=//p')" \
#     --build-arg VERSION="$(git describe --tags --always --dirty)" \
#     --build-arg GIT_SHA="$(git rev-parse HEAD)" \
#     --build-arg BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
#     -t golang-basics-ping:dev .                      ← context: workspace root
#
# `just docker-build ping`, docker/stack.yml and both CI pipelines pass exactly
# these arguments (GO_VERSION always from the mise pin — the build refuses to
# run without it, and checks that the builder image really is that Go).
#
# Build context = workspace root, filtered by the .dockerignore allowlist.
# Only go.work / go.work.sum, libs/ and services/<SERVICE>/ are copied; the
# go.work `use` entries that are not in the image (other services, e2e) are
# dropped before the build, so the workspace resolves exactly as it does on a
# laptop (same go.work.sum) and editing another service never invalidates
# this image's layers.
#
# Final image
# ───────────
#   • distroless/static, non-root (uid 65532), fully static (CGO_ENABLED=0 —
#     pgx, valkey-go and franz-go are pure Go: no libpq / librdkafka)
#   • -trimpath -s -w; Version / Revision / BuildTime stamped into libs/httpx
#     (build_info, /version, OTel service.version) from VERSION / GIT_SHA /
#     BUILD_TIME, mirrored in the OCI labels
#   • no HEALTHCHECK: distroless has no shell or curl to run one, and probes
#     are the orchestrator's job (k8s liveness /livez, readiness /readyz on the
#     admin port; docker/stack.yml uses the same endpoints)
#   • no EXPOSE: the ports are configuration (*_HTTP_ADDR / *_ADMIN_ADDR),
#     published by compose / the Service manifest
#   • no secrets, credentials or TLS material in any layer; config is env
#
# Base images: registry host by variable, pinned by digest
# ───────────────────────────────────────────────────────
# DOCKER_HUB / GCR (see .env.example, CLAUDE.md) prefix every image so a
# closed network points them at its pull-through cache. Both bases are pinned
# by digest for byte-for-byte reproducibility and against tag mutation; the tag
# stays next to the digest for humans. Bumping them:
#   • Renovate: the `# renovate:` comments are written for a regexManagers
#     rule (datasource=docker) that rewrites the tag and the digest together —
#     Renovate is what self-hosted GitLab uses.
#   • Dependabot (`docker` ecosystem on /docker in .github/dependabot.yml)
#     cannot follow a `${DOCKER_HUB}/…` FROM line, so on GitHub the weekly
#     Dependabot run won't bump these: refresh by hand with
#       docker buildx imagetools inspect <image>:<tag> | sed -n 's/^Digest: *//p'
#     GOLANG_DIGEST must move with the Go pin in mise.toml — a mismatch fails
#     the build at the `go env GOVERSION` check below, never silently.
#
# The `# check=` directive above only silences BuildKit's warning about the
# deliberately default-less GO_VERSION; it is read by the built-in frontend.
# No `# syntax=docker/dockerfile:…` directive on purpose: it makes BuildKit
# pull the frontend image from Docker Hub on every build, which a closed
# network cannot do. `--mount=type=cache` is in the frontend bundled with any
# current Docker / BuildKit.
# ─────────────────────────────────────────────────────────────────────────────

# Required: the Go toolchain version, always the mise.toml pin (scripts/
# mise-pins.sh → GO_VERSION). No default so an image can never be built on a
# Go the rest of the repo does not use.
ARG GO_VERSION
ARG DEBIAN_CODENAME=bookworm
ARG DOCKER_HUB=docker.io
ARG GCR=gcr.io
# renovate: datasource=docker depName=golang versioning=docker
ARG GOLANG_DIGEST=sha256:ded31c68586d2e49e760acc2e65a884b23d032e9bbbed0ae0c55abd3fcaf4452
# renovate: datasource=docker depName=gcr.io/distroless/static-debian12 currentValue=nonroot
ARG DISTROLESS_DIGEST=sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

# ── Stage 1 – builder ────────────────────────────────────────────────────────
FROM ${DOCKER_HUB}/library/golang:${GO_VERSION}-${DEBIAN_CODENAME}@${GOLANG_DIGEST} AS builder

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG GO_VERSION
ARG SERVICE
# Module proxy / checksum-db settings, forwarded from the environment
# (`--build-arg GOPROXY` with no value; unset = the Go defaults).
# HTTP_PROXY / HTTPS_PROXY / NO_PROXY are predefined build args.
ARG GOPROXY
ARG GOSUMDB
ARG GONOSUMDB
ARG GOFLAGS
ARG VERSION=dev
ARG GIT_SHA=
ARG BUILD_TIME=

# Fail fast on a missing SERVICE (COPY services/ would otherwise copy them
# all) and on a builder image that is not the pinned Go (stale GOLANG_DIGEST).
RUN test -n "${SERVICE}" || { echo "build-arg SERVICE is required (ping, heartbeat, tasks, consumer)" >&2; exit 1; } \
 && test "$(go env GOVERSION)" = "go${GO_VERSION}" \
    || { echo "builder is $(go env GOVERSION), GO_VERSION is ${GO_VERSION}: update GOLANG_DIGEST" >&2; exit 1; }

ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src

COPY go.work go.work.sum ./
COPY libs ./libs
COPY services/${SERVICE} ./services/${SERVICE}

# Drop the go.work members that are not in the context, then build in
# workspace mode. GOMODCACHE / GOCACHE are BuildKit cache mounts: modules and
# compiled packages survive between builds without ending up in a layer.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for d in $(go work edit -json | sed -n 's/.*"DiskPath": "\(.*\)".*/\1/p'); do \
      [ -f "$d/go.mod" ] || go work edit -dropuse="$d"; \
    done \
 && cd "services/${SERVICE}" \
 && go build -trimpath -buildvcs=false \
      -ldflags="-s -w \
        -X github.com/tracehubmmp/golang-basics/libs/httpx.Version=${VERSION} \
        -X github.com/tracehubmmp/golang-basics/libs/httpx.Revision=${GIT_SHA} \
        -X github.com/tracehubmmp/golang-basics/libs/httpx.BuildTime=${BUILD_TIME}" \
      -o /out/service .

# ── Stage 2 – runtime ────────────────────────────────────────────────────────
# distroless/static: ca-certificates, tzdata and /etc/passwd — no shell, no
# package manager, nothing writable outside /tmp.
FROM ${GCR}/distroless/static-debian12:nonroot@${DISTROLESS_DIGEST} AS runtime

ARG SERVICE
ARG VERSION=dev
ARG GIT_SHA=
ARG BUILD_TIME=

LABEL org.opencontainers.image.title="${SERVICE}" \
      org.opencontainers.image.description="golang-basics ${SERVICE} service" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_TIME}" \
      org.opencontainers.image.vendor="tracehub" \
      org.opencontainers.image.source="https://github.com/tracehubmmp/golang-basics" \
      org.opencontainers.image.licenses="UNLICENSED"

COPY --from=builder --chown=65532:65532 /out/service /app/service

USER 65532:65532
ENTRYPOINT ["/app/service"]
