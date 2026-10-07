# heartbeat

A minimal **background worker** — the example "worker" of this monorepo (the Go
analogue of a Kafka-consumer crate in the Rust sibling repo, minus the broker).
It runs a ticker loop that emits a structured log line and bumps a Prometheus
counter on every tick, while still serving `/healthz`, `/readyz`, `/metrics`,
`/version` and `/debug/pprof` on the admin listener from the shared
[`libs/httpx`](../../libs/httpx) scaffolding — so a worker is as observable as
an HTTP service. It has no API listener at all.

The HTTP server and the worker loop run concurrently under one signal-driven
context (`errgroup`); either one failing tears down the other.

## Endpoints (admin listener, `:9081`)

| Route                  | Returns                                                     |
| ---------------------- | ----------------------------------------------------------- |
| `GET /healthz`, `/livez` | liveness (from `httpx`)                                   |
| `GET /readyz`          | readiness (from `httpx`)                                    |
| `GET /metrics`         | Prometheus exposition incl. `heartbeat_beats_total`         |
| `GET /version`         | build identity (from `httpx`)                               |
| `GET /admin/config`    | the effective config, secrets redacted — **token**          |
| `GET/PUT/DELETE /admin/log-level` | runtime log level with an auto-revert TTL — **token** |
| `GET /debug/pprof/…`   | Go runtime profiles — **token**                             |

**token**: `Authorization: Bearer $HEARTBEAT_ADMIN_TOKEN`.

## Run

```sh
# the admin listener binds every interface by default, so it needs a token —
# or a loopback address, or an explicit "insecure" for local development
HEARTBEAT_ADMIN_ADDR=127.0.0.1:9081 just run          # ticks every 5s
HEARTBEAT_ADMIN_INSECURE=true HEARTBEAT_INTERVAL=1s HEARTBEAT_LOG_FORMAT=text just run

curl -s localhost:9081/metrics | grep heartbeat_beats_total
```

An invalid configuration is reported on stderr and the process exits 1 before
anything starts.

## Configuration

`internal/config.Config` embeds `httpx.Config`, so every
[`libs/httpx` key](../../libs/httpx#configuration) is available with the
`HEARTBEAT_` prefix and the same defaults — except `HEARTBEAT_ADMIN_ADDR`
(`:9081`, off ping's port) and `HEARTBEAT_HTTP_ADDR`, which is ignored: a
worker has no API listener. The ones you are most likely to set:

| Variable                          | Default   | Meaning                       |
| --------------------------------- | --------- | ----------------------------- |
| `HEARTBEAT_ADMIN_ADDR`            | `:9081`   | admin (health/metrics/pprof) listen address |
| `HEARTBEAT_ADMIN_TOKEN`           | *(empty)* | bearer token for `/admin/*` and `/debug/*`; required unless the admin address is loopback or `HEARTBEAT_ADMIN_INSECURE=true` |
| `HEARTBEAT_ADMIN_INSECURE`        | `false`   | accept an unauthenticated admin listener on a routable address |
| `HEARTBEAT_HTTP_SHUTDOWN_DELAY`   | `2s`      | keep answering probes after readiness flips |
| `HEARTBEAT_HTTP_SHUTDOWN_TIMEOUT` | `10s`     | graceful-shutdown budget      |
| `HEARTBEAT_LOG_LEVEL`             | `info`    | log level                     |
| `HEARTBEAT_LOG_FORMAT`            | `json`    | `json` or `text`              |
| `HEARTBEAT_LOG_SAMPLE_INITIAL`    | `100`     | log sampling, first N per msg per second (`0` = off) |
| `HEARTBEAT_LOG_SAMPLE_THEREAFTER` | `100`     | … then every M-th             |
| `HEARTBEAT_INTERVAL`              | `5s`      | tick period (must be positive) |

## Develop

```sh
just test      # go test ./...
just lint      # golangci-lint run
just release   # static binary into ./bin/heartbeat
just ci        # fmt-check → vet → lint → test
```

## Docker

One image recipe serves every service ([`docker/service.Dockerfile`](../../docker/service.Dockerfile)):

```sh
just docker-build heartbeat     # from the workspace root
docker run --rm -p 9081:9081 -e HEARTBEAT_ADMIN_TOKEN=change-me golang-basics-heartbeat:dev
```
