# e2e

End-to-end tests for the golang-basics services, using
[Playwright](https://playwright.dev) as a pure **API** test runner (no browser).
The Go analogue of the Rust sibling repo's `e2e/` — same "spawn the service
binaries, drive them over HTTP, assert" shape, minus the Testcontainers infra
(these services have no datastores).

## How it works

```
playwright.config.ts   API-only config; baseURL = PING_URL
global-setup.ts    →   spawn services/{ping,heartbeat}/bin/*  (fixtures/services.ts)
                       wait for /healthz, persist pids to .e2e-state.json
tests/*.spec.ts        run against the live services
global-teardown.ts →   SIGTERM every spawned pid
```

The harness spawns **pre-built** binaries, so build them first (the `just e2e`
recipe does this for you via `just e2e-build` → `just release`).

## Run

```sh
# from the workspace root:
just e2e-install     # pnpm install (once)
just e2e             # build binaries + run the whole suite
just e2e-ui          # Playwright UI mode
just e2e-filter ping # only specs matching "ping"
just e2e-report      # open the last HTML report

# smoke subset (tag @smoke):
pnpm --filter @golang-basics/e2e test:smoke
```

Point the suite at an already-running stack (e.g. `just up`) by overriding the
URLs:

```sh
PING_URL=http://localhost:8080 HEARTBEAT_URL=http://localhost:8081 pnpm test
```

## Specs

| File                      | Covers                                                   |
| ------------------------- | -------------------------------------------------------- |
| `tests/ping.spec.ts`      | `/ping`, `?msg=` echo, `/version`, 404 handling          |
| `tests/health.spec.ts`    | `/healthz` / `/readyz` / `/metrics` on **both** services |
| `tests/heartbeat.spec.ts` | `heartbeat_beats_total` increases over time              |
