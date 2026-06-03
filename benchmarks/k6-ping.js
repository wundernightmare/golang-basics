/**
 * k6 load test for the ping service.
 *
 * Profiles (SCENARIO env var) mirror the Rust sibling repo's bench profiles:
 *   smoke   — 50 VUs × 30s              (sanity)
 *   load    — ramp 0→500 VUs (~3.5m)
 *   stress  — ramp 0→2000 VUs (~4m)
 *   soak    — 500 VUs × 30m             (leak detection)
 *   peak    — constant-arrival-rate 25k req/s × 1m
 *
 * Usage (run-k6.sh wires this up; manual invocation):
 *   k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=smoke benchmarks/k6-ping.js
 */
import { check } from "k6";
import http from "k6/http";
import { Counter, Rate, Trend } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const SCENARIO = __ENV.SCENARIO || "smoke";

// ── Custom metrics ────────────────────────────────────────────────────────────
const pongErrors = new Rate("pong_errors");
const pongOk = new Counter("pong_ok");
const pongLatency = new Trend("pong_latency_ms", true);

// ── Profiles ──────────────────────────────────────────────────────────────────
const PROFILES = {
  smoke: { executor: "constant-vus", vus: 50, duration: "30s" },
  load: {
    executor: "ramping-vus",
    startVUs: 0,
    stages: [
      { duration: "15s", target: 200 },
      { duration: "1m", target: 500 },
      { duration: "2m", target: 500 },
      { duration: "15s", target: 0 },
    ],
  },
  stress: {
    executor: "ramping-vus",
    startVUs: 0,
    stages: [
      { duration: "15s", target: 500 },
      { duration: "30s", target: 1000 },
      { duration: "30s", target: 2000 },
      { duration: "2m", target: 2000 },
      { duration: "15s", target: 0 },
    ],
  },
  soak: { executor: "constant-vus", vus: 500, duration: "30m" },
  peak: {
    executor: "constant-arrival-rate",
    rate: 25_000,
    timeUnit: "1s",
    duration: "1m",
    preAllocatedVUs: 2_000,
    maxVUs: 8_000,
  },
};

if (!PROFILES[SCENARIO]) {
  throw new Error(`unknown SCENARIO '${SCENARIO}'. Known: ${Object.keys(PROFILES).join(", ")}`);
}

export const options = {
  scenarios: { [SCENARIO]: PROFILES[SCENARIO] },
  thresholds: {
    pong_errors: ["rate<0.01"], // <1% failed checks
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<50"], // p95 under 50ms for a trivial handler
  },
};

// ── Iteration ─────────────────────────────────────────────────────────────────
export default function () {
  const res = http.get(`${BASE_URL}/ping?msg=k6`);
  const ok = check(res, {
    "status is 200": (r) => r.status === 200,
    "body says pong": (r) => {
      try {
        return JSON.parse(r.body).message === "pong";
      } catch {
        return false;
      }
    },
  });

  pongErrors.add(!ok);
  pongLatency.add(res.timings.duration);
  if (ok) pongOk.add(1);
}
