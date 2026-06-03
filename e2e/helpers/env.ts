/**
 * Service URLs the e2e tests target. These are fixed host ports (the services
 * are singletons spawned by globalSetup), overridable via env so the same
 * specs can run against an already-running stack (e.g. `just up`).
 */
export const PING_URL = process.env["PING_URL"] ?? "http://localhost:8080";
export const HEARTBEAT_URL = process.env["HEARTBEAT_URL"] ?? "http://localhost:8081";
