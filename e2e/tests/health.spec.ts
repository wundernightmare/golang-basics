import { expect, test } from "@playwright/test";

import { HEARTBEAT_ADMIN_URL, PING_ADMIN_URL, PING_URL } from "../helpers/env";

// Both services get the operational surface for free from libs/httpx on their
// admin listener — assert it behaves identically across an HTTP service and a
// worker, and that the API port does not carry it.
for (const [name, admin] of [
  ["ping", PING_ADMIN_URL],
  ["heartbeat", HEARTBEAT_ADMIN_URL],
] as const) {
  test.describe(`${name} admin endpoints`, () => {
    test("GET /healthz is 200 ok @smoke", async ({ request }) => {
      const res = await request.get(`${admin}/healthz`);
      expect(res.status()).toBe(200);
      expect(await res.json()).toMatchObject({ status: "ok" });
    });

    test("GET /readyz is ready", async ({ request }) => {
      const res = await request.get(`${admin}/readyz`);
      expect(res.status()).toBe(200);
      expect((await res.json()).status).toBe("ready");
    });

    test("GET /metrics exposes Prometheus text with build_info", async ({ request }) => {
      const res = await request.get(`${admin}/metrics`);
      expect(res.status()).toBe(200);
      const body = await res.text();
      expect(body).toContain("go_goroutines");
      expect(body).toMatch(new RegExp(`build_info\\{[^}]*service="${name}"`));
    });

    test("GET /version reports the service", async ({ request }) => {
      const res = await request.get(`${admin}/version`);
      expect(res.status()).toBe(200);
      const body = await res.json();
      expect(body.service).toBe(name);
      expect(body.version).toBeTruthy();
      expect(body.go_version).toMatch(/^go/);
    });

    test("GET /debug/pprof/ is served", async ({ request }) => {
      const res = await request.get(`${admin}/debug/pprof/`);
      expect(res.status()).toBe(200);
      expect(await res.text()).toContain("goroutine");
    });
  });
}

test.describe("ping API port carries no operational routes", () => {
  for (const path of ["/healthz", "/readyz", "/metrics", "/debug/pprof/"]) {
    test(`GET ${path} is 404 on the API port`, async ({ request }) => {
      const res = await request.get(`${PING_URL}${path}`);
      expect(res.status()).toBe(404);
    });
  }
});
