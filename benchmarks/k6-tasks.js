/**
 * k6 load test for the tasks service (HTTP → Postgres → Valkey → Kafka).
 *
 * Each iteration creates a task (POST, exercising the DB write + cache warm +
 * Kafka publish) and then reads it back (GET, exercising the Valkey cache-aside
 * path). Bring the backing services up first: `just infra-up`. run-k6-tasks.sh
 * wires this against a freshly-built tasks binary.
 *
 * Profiles (SCENARIO env var) — lighter than k6-ping.js because each iteration
 * touches Postgres + Kafka, not just an in-memory handler:
 *   smoke   — 20 VUs × 30s              (sanity)
 *   load    — ramp 0→200 VUs (~3.5m)
 *   stress  — ramp 0→800 VUs (~4m)
 *   soak    — 200 VUs × 30m             (leak detection)
 *
 * Usage:
 *   k6 run -e BASE_URL=http://localhost:8082 -e SCENARIO=smoke benchmarks/k6-tasks.js
 */
import { check } from "k6";
import http from "k6/http";
import { Counter, Rate, Trend } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8082";
const SCENARIO = __ENV.SCENARIO || "smoke";

// ── Custom metrics ────────────────────────────────────────────────────────────
const taskErrors = new Rate("task_errors");
const tasksCreated = new Counter("tasks_created");
const cacheHits = new Counter("cache_hits");
const createLatency = new Trend("create_latency_ms", true);
const readLatency = new Trend("read_latency_ms", true);

// ── Profiles ──────────────────────────────────────────────────────────────────
const PROFILES = {
  smoke: { executor: "constant-vus", vus: 20, duration: "30s" },
  load: {
    executor: "ramping-vus",
    startVUs: 0,
    stages: [
      { duration: "15s", target: 100 },
      { duration: "1m", target: 200 },
      { duration: "2m", target: 200 },
      { duration: "15s", target: 0 },
    ],
  },
  stress: {
    executor: "ramping-vus",
    startVUs: 0,
    stages: [
      { duration: "15s", target: 200 },
      { duration: "30s", target: 400 },
      { duration: "30s", target: 800 },
      { duration: "2m", target: 800 },
      { duration: "15s", target: 0 },
    ],
  },
  soak: { executor: "constant-vus", vus: 200, duration: "30m" },
};

if (!PROFILES[SCENARIO]) {
  throw new Error(`unknown SCENARIO '${SCENARIO}'. Known: ${Object.keys(PROFILES).join(", ")}`);
}

export const options = {
  scenarios: { [SCENARIO]: PROFILES[SCENARIO] },
  thresholds: {
    task_errors: ["rate<0.01"], // <1% failed checks
    http_req_failed: ["rate<0.01"],
    // Two dependency-touching round-trips; looser than the trivial ping handler.
    http_req_duration: ["p(95)<250"],
  },
};

const JSON_HEADERS = { headers: { "Content-Type": "application/json" } };

// ── Iteration ─────────────────────────────────────────────────────────────────
export default function () {
  // 1. Create — DB insert + cache warm + Kafka publish.
  const create = http.post(`${BASE_URL}/tasks`, JSON.stringify({ title: "k6 load" }), JSON_HEADERS);
  const created = check(create, { "create 201": (r) => r.status === 201 });
  createLatency.add(create.timings.duration);
  taskErrors.add(!created);
  if (!created) return;

  tasksCreated.add(1);
  const id = JSON.parse(create.body).id;

  // 2. Read back — should hit the Valkey cache warmed on create.
  const read = http.get(`${BASE_URL}/tasks/${id}`);
  const readOk = check(read, { "read 200": (r) => r.status === 200 });
  readLatency.add(read.timings.duration);
  taskErrors.add(!readOk);
  if (readOk && read.headers["X-Cache"] === "hit") cacheHits.add(1);
}
