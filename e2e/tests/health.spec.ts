import { expect, test } from "@playwright/test";

import { HEARTBEAT_URL, PING_URL } from "../helpers/env";

// Both services get health + metrics for free from libs/httpx — assert the
// shared surface behaves identically across an HTTP service and a worker.
for (const [name, base] of [
  ["ping", PING_URL],
  ["heartbeat", HEARTBEAT_URL],
] as const) {
  test.describe(`${name} shared endpoints`, () => {
    test("GET /healthz is 200 ok @smoke", async ({ request }) => {
      const res = await request.get(`${base}/healthz`);
      expect(res.status()).toBe(200);
      expect(await res.json()).toMatchObject({ status: "ok" });
    });

    test("GET /readyz is ready", async ({ request }) => {
      const res = await request.get(`${base}/readyz`);
      expect(res.status()).toBe(200);
      expect((await res.json()).status).toBe("ready");
    });

    test("GET /metrics exposes Prometheus text", async ({ request }) => {
      const res = await request.get(`${base}/metrics`);
      expect(res.status()).toBe(200);
      const body = await res.text();
      expect(body).toContain("http_requests_total");
      expect(body).toContain("go_goroutines");
    });
  });
}
