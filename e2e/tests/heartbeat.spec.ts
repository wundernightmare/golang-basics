import { expect, test } from "@playwright/test";

import { HEARTBEAT_URL } from "../helpers/env";

/** Pull the heartbeat_beats_total counter value out of the Prometheus text. */
async function readBeats(request: import("@playwright/test").APIRequestContext): Promise<number> {
  const res = await request.get(`${HEARTBEAT_URL}/metrics`);
  expect(res.status()).toBe(200);
  const line = (await res.text()).split("\n").find((l) => l.startsWith("heartbeat_beats_total"));
  expect(line, "heartbeat_beats_total present").toBeTruthy();
  return Number(line!.split(/\s+/)[1]);
}

test.describe("heartbeat worker", () => {
  test("emits beats that increase over time", async ({ request }) => {
    const first = await readBeats(request);
    // The worker ticks every 200ms in the e2e env; wait for a few ticks.
    await new Promise((r) => setTimeout(r, 700));
    const second = await readBeats(request);
    expect(second).toBeGreaterThan(first);
  });
});
