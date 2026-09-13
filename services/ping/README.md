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
| `GET /version` | `200` build identity (service, version, VCS revision, Go version) — the same body the admin listener serves |

Operational routes — `/healthz`, `/readyz`, `/metrics`, `/version`,
`/debug/pprof` — live on the **admin** listener (`:9080`), not on the API port.

## Run

```sh
just run                      # go run . on :8080 (admin :9080)
# or with overrides
PING_HTTP_ADDR=:9000 PING_ADMIN_ADDR=:9900 PING_LOG_FORMAT=text just run

curl -s localhost:8080/ping | jq .
curl -s 'localhost:8080/ping?msg=hello' | jq .
curl -s localhost:9080/metrics | head
go tool pprof http://localhost:9080/debug/pprof/heap
```

## Configuration

All keys are prefixed `PING_` (see [`libs/httpx`](../../libs/httpx#configuration)):

| Variable                     | Default | Meaning                  |
| ---------------------------- | ------- | ------------------------ |
| `PING_HTTP_ADDR`             | `:8080` | API listen address       |
| `PING_ADMIN_ADDR`            | `:9080` | admin listen address     |
| `PING_ADMIN_TOKEN`           | *(empty)* | bearer token for `PUT`/`DELETE /admin/*` (empty = open) |
| `PING_DEBUG_TOKEN`           | *(empty)* | `X-Debug-Token` value for per-request debug logging (empty = off) |
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
