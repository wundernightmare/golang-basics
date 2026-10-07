# ping

A minimal **ping/pong HTTP service** — the example "HTTP service" of this
monorepo (the Go analogue of an edge crate in the Rust sibling repo). It is
deliberately tiny: all cross-cutting concerns live in the shared
[`libs/httpx`](../../libs/httpx) package (plain `net/http`, no framework), so
`main.go` + `internal/api` is the whole service.

## Endpoints

| Route          | Returns                                                        |
| -------------- | -------------------------------------------------------------- |
| `GET /ping`    | `200 {"message":"pong"}` — add `?msg=hi` to echo: `{"message":"pong","echo":"hi"}` |
| `GET /version` | `200` build identity (service, version, VCS revision, build time, Go version) — the `BuildInfo` the admin listener also reports |

The contract is [`api/tsp/ping.tsp`](../../api/tsp) → `api/openapi3/ping.openapi.yaml`;
the response types are generated into `libs/contracts/pingapi`, and the tests
validate every exchange against the OpenAPI document. Any other path is a
`404 problem+json`, a wrong method a `405 problem+json` with `Allow`.

Operational routes — `/healthz`, `/livez`, `/readyz`, `/metrics`, `/version`,
`/admin/config`, `/admin/log-level`, `/debug/pprof` — live on the **admin**
listener (`:9080`), not on the API port; `/admin/*` and `/debug/*` need
`Authorization: Bearer $PING_ADMIN_TOKEN`.

## Run

```sh
# the admin listener is on every interface by default: give it a token,
# bind it to loopback, or acknowledge it is open (local development only)
PING_ADMIN_ADDR=127.0.0.1:9080 just run      # go run . on :8080

curl -s localhost:8080/ping | jq .
curl -s 'localhost:8080/ping?msg=hello' | jq .
curl -s localhost:9080/metrics | grep http_requests_total
go tool pprof http://localhost:9080/debug/pprof/heap
```

A config the server cannot run with (a typo in `PING_LOG_LEVEL`, an open admin
listener on a routable address, …) is reported on stderr and the process exits 1
before binding anything.

## Configuration

All keys are prefixed `PING_`; the full table (timeouts, limits, trusted
proxies, sampling, …) is in [`libs/httpx`](../../libs/httpx#configuration).
The ones you are most likely to set:

| Variable                     | Default   | Meaning                  |
| ---------------------------- | --------- | ------------------------ |
| `PING_HTTP_ADDR`             | `:8080`   | API listen address       |
| `PING_ADMIN_ADDR`            | `:9080`   | admin listen address     |
| `PING_ADMIN_TOKEN`           | *(empty)* | bearer token for `/admin/*` and `/debug/*`; required unless the admin address is loopback or `PING_ADMIN_INSECURE=true` |
| `PING_DEBUG_TOKEN`           | *(empty)* | `X-Debug-Token` value for per-request debug logging (empty = off) |
| `PING_HTTP_TRUSTED_PROXIES`  | *(none)*  | CIDRs of the load balancers whose `X-Forwarded-For` / `X-Request-Id` are honoured |
| `PING_HTTP_SHUTDOWN_DELAY`   | `2s`      | keep serving after readiness flips, before draining |
| `PING_HTTP_SHUTDOWN_TIMEOUT` | `10s`     | drain budget             |
| `PING_LOG_LEVEL`             | `info`    | log level                |
| `PING_LOG_FORMAT`            | `json`    | `json` or `text`         |

## Develop

```sh
just test      # go test ./...  (handler + contract tests through srv.Handler())
just lint      # golangci-lint run
just release   # static binary into ./bin/ping
just ci        # fmt-check → vet → lint → test
```

## Docker

One image recipe serves every service ([`docker/service.Dockerfile`](../../docker/service.Dockerfile)):

```sh
just docker-build ping          # from the workspace root
docker run --rm -p 8080:8080 -e PING_ADMIN_TOKEN=change-me golang-basics-ping:dev
```
