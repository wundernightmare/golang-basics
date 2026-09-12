import * as path from "node:path";

import { defineConfig } from "@playwright/test";

import { PING_URL } from "./helpers/env";

/**
 * Playwright configuration for the golang-basics e2e suite.
 *
 * Tests are pure API tests (no browser) — they drive the services through
 * Playwright's APIRequestContext. Unlike the Rust sibling repo there is no
 * Testcontainers infrastructure to start: these services have no datastores,
 * so globalSetup just spawns the (pre-built) Go binaries and globalTeardown
 * stops them.
 *
 *   globalSetup    → spawn services/ping/bin/ping + services/heartbeat/bin/heartbeat
 *   globalTeardown → kill them (pids tracked in .e2e-state.json)
 *
 * Run `just e2e-build` first (it builds the binaries the harness spawns).
 *
 * What this layer owns (and the Go tests do not): real binaries, real
 * listeners on real ports, cross-process flows (tasks → Kafka → consumer),
 * the build stamp. Route-level behaviour, error bodies and per-endpoint
 * contracts are unit / integration tests in Go — see README "Tests".
 *
 * Results go to Allure (allure-playwright) next to the Go suites' results:
 * ALLURE_RESULTS_DIR redirects them, as it does for the Go packages.
 */
export default defineConfig({
  testDir: "./tests",
  fullyParallel: false, // services are shared singletons on fixed ports
  forbidOnly: !!process.env["CI"],
  retries: 0, // a flake is reported (Allure), never hidden behind a retry
  workers: 1,
  reporter: [
    ["list"],
    ["html", { outputFolder: "playwright-report", open: "never" }],
    [
      "allure-playwright",
      {
        resultsDir: process.env["ALLURE_RESULTS_DIR"] ?? "allure-results",
        detail: false,
        suiteTitle: false,
        environmentInfo: { layer: "e2e", runner: "playwright" },
      },
    ],
  ],

  use: {
    baseURL: PING_URL,
    extraHTTPHeaders: { Accept: "application/json" },
    actionTimeout: 10_000,
  },

  projects: [{ name: "api" }],

  globalSetup: path.resolve(__dirname, "global-setup.ts"),
  globalTeardown: path.resolve(__dirname, "global-teardown.ts"),

  timeout: 30_000,
  expect: { timeout: 10_000 },
  outputDir: "test-results",
});
