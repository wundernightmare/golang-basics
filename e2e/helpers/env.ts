/**
 * Service URLs the e2e tests target. These are fixed host ports (the services
 * are singletons spawned by globalSetup), overridable via env so the same
 * specs can run against an already-running stack (e.g. `just up`).
 */
export const PING_URL = process.env["PING_URL"] ?? "http://localhost:8080";
export const HEARTBEAT_URL = process.env["HEARTBEAT_URL"] ?? "http://localhost:8081";

// tasks + consumer are only spawned when E2E_WITH_DEPS=1 (Postgres + Valkey +
// Kafka required — `just infra-up`); tasks.spec.ts skips itself otherwise.
export const TASKS_URL = process.env["TASKS_URL"] ?? "http://localhost:8082";
export const CONSUMER_URL = process.env["CONSUMER_URL"] ?? "http://localhost:8083";
export const WITH_DEPS = process.env["E2E_WITH_DEPS"] === "1";
