# ping

A minimal **ping/pong HTTP service** — the example "HTTP service" of this
monorepo (the Go analogue of an edge crate in the Rust sibling repo). It is
deliberately tiny: all cross-cutting concerns live in the shared
[`libs/httpx`](../../libs/httpx) package, so `main.go` + `internal/api` is the
whole service.

## Endpoints

| Route       | Returns                                                        |
| ----------- | ------------------------------------------------------------- |
| `GET /ping` | `200 {"message":"pong"}` — add `?msg=hi` to echo: `{"message":"pong","echo":"hi"}` |
| `GET /version` | `200` build metadata (service, VCS revision, Go version, time) |
| `GET /healthz` | liveness (from `httpx`)                                    |
| `GET /readyz`  | readiness (from `httpx`)                                   |
| `GET /metrics` | Prometheus exposition (from `httpx`)                      |

## Run

```sh
just run                      # go run . on :8080
# or with overrides
PING_HTTP_ADDR=:9000 PING_LOG_FORMAT=text just run

curl -s localhost:8080/ping | jq .
curl -s 'localhost:8080/ping?msg=hello' | jq .
curl -s localhost:8080/metrics | head
```

## Configuration

All keys are prefixed `PING_` (see [`libs/httpx`](../../libs/httpx#configuration)):

| Variable                     | Default | Meaning                  |
| ---------------------------- | ------- | ------------------------ |
| `PING_HTTP_ADDR`             | `:8080` | listen address           |
| `PING_HTTP_SHUTDOWN_TIMEOUT` | `10s`   | graceful-shutdown budget |
| `PING_LOG_LEVEL`             | `info`  | log level                |
| `PING_LOG_FORMAT`            | `json`  | `json` or `text`         |

## Develop

```sh
just test      # go test ./...
just lint      # golangci-lint run
just release   # static binary into ./bin/ping
just ci        # fmt-check → vet → lint → test
```

## Docker

```sh
# from the workspace root (build context = root):
docker build -f services/ping/Dockerfile -t ping:dev .
docker run --rm -p 8080:8080 ping:dev
```
