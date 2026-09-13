import { expect, test } from "@playwright/test";

import { meta, testCase } from "../fixtures/meta";
import { CONSUMER_ADMIN_URL, TASKS_URL, WITH_DEPS } from "../helpers/env";

// Owned by this layer: the cross-process flow — a task created through the
// real tasks binary reaches the real consumer binary over the real broker.
// The CRUD contract, cache-hit path, problem+json bodies and readiness with
// its checks are the Go suite in services/tasks/internal/integration.
// Only runs when E2E_WITH_DEPS=1 — see fixtures/services.ts and `just e2e-deps`.
test.describe("tasks service", () => {
  meta({ feature: "tasks pipeline" });
  test.skip(!WITH_DEPS, "needs Postgres + Valkey + Kafka (run `just e2e-deps`)");

  test("the consumer drains the task.created event", async ({ request }) => {
    await testCase("GB-531", "tasks → Kafka → consumer across real processes", "critical");
    const before = await consumedTotal(request);

    const created = await request.post(`${TASKS_URL}/tasks`, {
      data: { title: "for the consumer" },
    });
    expect(created.status()).toBe(201);

    // The consumer polls Kafka, so poll its metric until it advances.
    await expect
      .poll(async () => consumedTotal(request), { timeout: 15_000, intervals: [250] })
      .toBeGreaterThan(before);
  });
});

/** Read consumer_tasks_consumed_total from the consumer's /metrics endpoint. */
async function consumedTotal(
  request: import("@playwright/test").APIRequestContext,
): Promise<number> {
  const res = await request.get(`${CONSUMER_ADMIN_URL}/metrics`);
  if (!res.ok()) return 0;
  const line = (await res.text())
    .split("\n")
    .find((l) => l.startsWith("consumer_tasks_consumed_total"));
  return line ? Number(line.split(/\s+/)[1]) : 0;
}
