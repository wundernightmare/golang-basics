# e2e

End-to-end tests for the golang-basics services: a Go module that spawns the
real service binaries as child processes and drives them over HTTP. It runs
with plain `go test` and asserts with testify; [testo](https://github.com/ozontech/testo)
wraps each test so the suite reports to Allure natively ([Report](#report)).
Every test file carries the `e2e` build tag, so
`go test ./...` without the tag compiles only `doc.go` and runs nothing.

## What it checks

Only what a real process shows — everything else is owned by a lower layer
(README → "Tests"):

| Test | Checks |
|---|---|
| `TestPing` | ready on the admin port; `/version` build stamp; `GET /ping?msg=x` echoes; `/metrics` is not on the API port; `/admin/config` requires the admin token |
| `TestHeartbeat` | `/version`; `heartbeat_beats_total` increases (polled); admin token |
| `TestGracefulShutdown` | SIGTERM → exit 0 within 5s, no error-level log line, `servers stopped cleanly` logged and the last line says stopped cleanly (ping, heartbeat) |
| `TestTasksPipeline` | tasks + consumer: `/version`, admin token, `POST /tasks` → 201, `GET /tasks/{id}` → 200, the consumer's `consumer_events_total` increases within 30s (outbox relay → Kafka → consumer), then both shut down cleanly |
| `TestFIPSBuild` | builds ping itself through `scripts/build-service.sh` with `FIPS=1` and without: `/version` and the start-up line report `fips140` true / false (docs/RUNTIME.md "FIPS 140-3") |
| `TestFIPSBuildRefusesRdkafka` | the build script refuses `FIPS=1` for a service that would link librdkafka (exit 2, no binary) |
| `TestEveryTestIsAScenario` | every Test function of the suite goes through `scenario`, i.e. is in the Allure report ([Report](#report)) |

## Harness (`harness_test.go`)

For every `start(t, spec)`:

- binary `$E2E_BIN_DIR/<name>` (default `../.build/<name>`), or the one a
  test built itself (`spec.bin` — the FIPS tests);
- **free ports**: the harness listens on `127.0.0.1:0`, closes, and passes the
  address as `<SVC>_HTTP_ADDR` / `<SVC>_ADMIN_ADDR` — never a fixed port, so
  runs (and worktrees) do not collide;
- env: `<SVC>_ADMIN_TOKEN` (random per process; tests use it),
  `<SVC>_HTTP_SHUTDOWN_DELAY=0s`, `<SVC>_LOG_FORMAT=json`, plus the test's
  own keys; inherited variables with the service's prefix are dropped so a
  developer's `PING_HTTP_ADDR` cannot leak in;
- stdout/stderr captured into a buffer, attached to the report when the
  service stops and printed as well when the test fails;
- waits for `/readyz` 200 on the admin port (30s deadline), failing at once
  with the output if the child exits first;
- asserts `/version` reports `service == <name>` — never passes against a
  stray process on the port;
- teardown: SIGTERM, **wait for exit** (15s), then SIGKILL — a cover-built
  binary writes its counters only on a normal exit.

## Environment

| Variable | Meaning |
|---|---|
| `E2E_BIN_DIR` | where the binaries are (default `../.build`) |
| `E2E_COVER_DIR` | passed to every child as `GOCOVERDIR` (binaries built with `-cover`) |
| `E2E_DATABASE_URL` | → `TASKS_DATABASE_URL` |
| `E2E_VALKEY_URL` | → `TASKS_VALKEY_URL` |
| `E2E_KAFKA_BROKERS` | → `TASKS_KAFKA_BROKERS`, `CONSUMER_KAFKA_BROKERS` |

`TestTasksPipeline` skips unless all three data URLs are set. It uses a topic
(`e2e.tasks.events.<timestamp>`) and consumer group of its own, so runs
against the same broker do not interfere, and enables auto topic creation for
that topic on both sides.

## Run

```sh
# from the workspace root: build the binaries (cover-instrumented, like CI)
for s in ping heartbeat tasks consumer; do COVER=1 scripts/build-service.sh $s .build/$s; done

cd e2e
go test -tags e2e -count=1 ./...                  # ping + heartbeat (tasks/consumer skip)
just test                                         # same

# the data services (docker/deps.yml up: `just infra-up`; `just e2e` finds
# their published ports itself, DEPS_*_PORT included)
E2E_DATABASE_URL='postgres://app:app@localhost:5432/app?sslmode=disable' \
E2E_VALKEY_URL=valkey://localhost:6379 \
E2E_KAFKA_BROKERS=localhost:9092 \
  go test -tags e2e -count=1 ./...

# coverage from the child processes
E2E_COVER_DIR=../.cover/e2e go test -tags e2e -count=1 ./...
```

`scripts/cover.sh e2e` does the build + coverage run in one step.

## Report

The layers below report through JUnit XML: a name, a status, one blob of
output. A scenario here drives two or three processes through several phases,
and a failure needs more than that, so this suite writes Allure's own result
files through [testo](https://github.com/ozontech/testo) and
[testo-allure](https://github.com/ozontech/testo-allure) (`report_test.go`):

- every Test function is one `scenario(t, feature, func(t T) { … })` — an
  Allure test case with an epic, a feature, the `e2e` layer and, in CI, the
  Kafka provider as a parameter (the suite runs once per provider);
- a sub-test is `testo.Run(t, name, func(t T) { … })` and shows as a step with
  its own status and duration;
- `start` adds a marker with how long the service took to become ready, and
  attaches the service's whole output when it stops — on a green run too;
- `T` satisfies `testing.TB`, so the harness helpers and testify take it as is.

Results go to `$ALLURE_RESULTS_DIR` (default `../.reports/allure-native/e2e`);
`just allure` at the root builds the report from them and the other layers'
JUnit files.

Three things to know:

- **Test names gain levels.** testo runs `TestPing` as
  `TestPing/#00/testo!/TestPing/<sub-test>`. `-run TestPing` works as before;
  to name a sub-test, leave the middle levels empty:
  `go test -tags e2e -run 'TestPing////echoes' ./...`.
- **A step is a sub-test**, so such a filter skips every step it does not
  name. Nothing a later line depends on may be inside `testo.Run` or
  `allure.Step` — which is why `start` waits for readiness outside of one.
- **A test that bypasses `scenario` would be missing from the report** while
  still gating the build; `TestEveryTestIsAScenario` reads the sources and
  fails on one.

testo-allure 1.5.0 writes an attachment without a media type through
`os.TempDir` and a rename, which fails when `/tmp` is another filesystem;
attachments here always name their type (`attachOutput`).
