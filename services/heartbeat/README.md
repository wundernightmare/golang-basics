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

| Route              | Returns                                                       |
| ------------------ | ------------------------------------------------------------ |
| `GET /healthz`     | liveness (from `httpx`)                                       |
| `GET /readyz`      | readiness (from `httpx`)                                      |
| `GET /metrics`     | Prometheus exposition incl. `heartbeat_beats_total`          |
| `GET /version`     | build identity (from `httpx`)                                 |
| `GET /debug/pprof` | Go runtime profiles (from `httpx`)                            |

## Run

```sh
just run                                  # ticks every 5s, admin server on :9081
HEARTBEAT_INTERVAL=1s HEARTBEAT_LOG_FORMAT=text just run

curl -s localhost:9081/metrics | grep heartbeat_beats_total
```

## Configuration

All keys are prefixed `HEARTBEAT_`:

| Variable                          | Default | Meaning                       |
| --------------------------------- | ------- | ----------------------------- |
| `HEARTBEAT_ADMIN_ADDR`            | `:9081` | admin (health/metrics/pprof) listen address |
| `HEARTBEAT_LOG_SAMPLE_INITIAL`    | `100`   | log sampling, first N per msg per second (`0` = off) |
| `HEARTBEAT_LOG_SAMPLE_THEREAFTER` | `100`   | … then every M-th             |
| `HEARTBEAT_HTTP_SHUTDOWN_TIMEOUT` | `10s`   | graceful-shutdown budget      |
| `HEARTBEAT_LOG_LEVEL`             | `info`  | log level                     |
| `HEARTBEAT_LOG_FORMAT`            | `json`  | `json` or `text`              |
| `HEARTBEAT_INTERVAL`              | `5s`    | tick period                   |

## Develop

```sh
just test      # go test ./...
just lint      # golangci-lint run
just release   # static binary into ./bin/heartbeat
just ci        # fmt-check → vet → lint → test
```

## Docker

```sh
# from the workspace root (build context = root):
docker build -f services/heartbeat/Dockerfile -t heartbeat:dev .
docker run --rm -p 8081:8081 heartbeat:dev
```
