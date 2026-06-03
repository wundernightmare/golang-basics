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
 */
export default defineConfig({
  testDir: "./tests",
  fullyParallel: false, // services are shared singletons on fixed ports
  forbidOnly: !!process.env["CI"],
  retries: process.env["CI"] ? 1 : 0,
  workers: 1,
  reporter: [["list"], ["html", { outputFolder: "playwright-report", open: "never" }]],

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
