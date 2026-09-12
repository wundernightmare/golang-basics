import { expect, test } from "@playwright/test";

import { HEARTBEAT_ADMIN_URL, PING_ADMIN_URL } from "../helpers/env";

// Owned by this layer: each real process (an HTTP service and a worker) is
// ready on its admin listener and is a scrapeable target that identifies
// itself. Which routes exist on which listener, the readiness semantics and
// the metric set are libs/httpx and services/ping tests.
for (const [name, admin] of [
  ["ping", PING_ADMIN_URL],
  ["heartbeat", HEARTBEAT_ADMIN_URL],
] as const) {
  test.describe(`${name} process`, () => {
    test("is ready on its admin listener @smoke", async ({ request }) => {
      const res = await request.get(`${admin}/readyz`);
      expect(res.status()).toBe(200);
      expect((await res.json()).status).toBe("ready");
    });

    test("is a scrape target that identifies itself", async ({ request }) => {
      const res = await request.get(`${admin}/metrics`);
      expect(res.status()).toBe(200);
      expect(await res.text()).toMatch(new RegExp(`build_info\\{[^}]*service="${name}"`));
    });
  });
}
