/**
 * Service-spawn fixtures for the e2e harness.
 *
 * The Go analogue of the Rust sibling repo's e2e/fixtures/services.ts: launch
 * each service binary as a child process, wait for its /healthz to come up,
 * and hand back the pids so globalTeardown can stop them. No Testcontainers /
 * infra here — these services have no external dependencies.
 */
import { type ChildProcess, spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";

const ROOT = path.resolve(__dirname, "..", "..");

export interface ServiceSpec {
  name: string;
  bin: string;
  healthUrl: string;
  env: Record<string, string>;
}

export const SERVICES: ServiceSpec[] = [
  {
    name: "ping",
    bin: path.join(ROOT, "services/ping/bin/ping"),
    healthUrl: "http://localhost:8080/healthz",
    env: { PING_HTTP_ADDR: ":8080", PING_LOG_LEVEL: "warn" },
  },
  {
    name: "heartbeat",
    bin: path.join(ROOT, "services/heartbeat/bin/heartbeat"),
    healthUrl: "http://localhost:8081/healthz",
    // Fast tick so the heartbeat_beats_total assertion doesn't wait long.
    env: { HEARTBEAT_HTTP_ADDR: ":8081", HEARTBEAT_INTERVAL: "200ms", HEARTBEAT_LOG_LEVEL: "warn" },
  },
];

const STATE_FILE = path.join(ROOT, "e2e", ".e2e-state.json");

async function waitForHealth(url: string, tries = 100): Promise<void> {
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`service did not become healthy at ${url}`);
}

/** Build a helpful error if a binary the harness expects is missing. */
function assertBinariesExist(): void {
  const missing = SERVICES.filter((s) => !fs.existsSync(s.bin)).map((s) => s.name);
  if (missing.length > 0) {
    throw new Error(
      `missing service binaries: ${missing.join(", ")}. Run \`just e2e-build\` (or \`just release\`) first.`,
    );
  }
}

/** Spawn every service, wait for health, and persist pids for teardown. */
export async function startServices(): Promise<void> {
  assertBinariesExist();

  const children: ChildProcess[] = [];
  const pids: number[] = [];

  for (const svc of SERVICES) {
    const child = spawn(svc.bin, {
      cwd: ROOT,
      env: { ...process.env, ...svc.env },
      stdio: "ignore",
    });
    children.push(child);
    if (child.pid) pids.push(child.pid);
  }

  try {
    await Promise.all(SERVICES.map((s) => waitForHealth(s.healthUrl)));
  } catch (err) {
    for (const c of children) c.kill("SIGKILL");
    throw err;
  }

  fs.writeFileSync(STATE_FILE, JSON.stringify({ pids }, null, 2));
}

/** Kill every service started by startServices(). */
export function stopServices(): void {
  if (!fs.existsSync(STATE_FILE)) return;
  try {
    const { pids } = JSON.parse(fs.readFileSync(STATE_FILE, "utf8")) as { pids: number[] };
    for (const pid of pids) {
      try {
        process.kill(pid, "SIGTERM");
      } catch {
        // already gone
      }
    }
  } finally {
    fs.rmSync(STATE_FILE, { force: true });
  }
}
