import { expect, test } from "@playwright/test";

import { PING_URL } from "../helpers/env";

// Owned by this layer: the real ping binary serves its API on its API port
// and reports the version stamped into it at build time. Echo, 404 handling
// and the route contract are services/ping/internal/api unit tests.
test.describe("ping service", () => {
  test("GET /ping answers through the real process @smoke", async ({ request }) => {
    const res = await request.get(`${PING_URL}/ping`);
    expect(res.status()).toBe(200);
    expect(await res.json()).toMatchObject({ message: "pong" });
  });

  test("GET /version carries the build stamp", async ({ request }) => {
    const res = await request.get(`${PING_URL}/version`);
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.service).toBe("ping");
    expect(body.version, "libs/httpx.Version injected by scripts/build-service.sh").toBeTruthy();
    expect(body.go_version).toMatch(/^go/);
  });
});
