/**
 * TestOps metadata for the e2e layer — the Playwright twin of libs/testx's
 * `testx.Meta` (suite identity) and `testx.Case` (per-test id + story), so the
 * Go suites and these specs land in the same Epic / Feature tree of the Allure
 * report and in the same test plan.
 *
 * SAMPLE VALUES, exactly like the Go side: "golang-basics", "@team-platform"
 * and the "GB-5xx" ids (5xx = e2e; the Go suites use GB-1…4xx) are
 * placeholders whose *shape* is the point — replace them with your TestOps
 * project's tree, owner handles and case ids. The TMS link template lives in
 * playwright.config.ts (`links`), the counterpart of testx.LinkTransformer.
 * One id per test; keep the metadata when copying a test, change the id.
 *
 *   test.describe("ping process", () => {
 *     meta({ feature: "admin listener" });
 *     test("is ready @smoke", async ({ request }) => {
 *       await testCase("GB-501", "readiness on the admin listener");
 *       …
 *     });
 *   });
 */
import { test } from "@playwright/test";
import { allure } from "allure-playwright";

export const EPIC = "golang-basics";
export const OWNER = "@team-platform";
export const LAYER = "e2e";

export type Severity = "blocker" | "critical" | "normal" | "minor" | "trivial";

export interface Meta {
  epic?: string;
  feature: string;
  owner?: string;
}

/**
 * Suite identity for the enclosing `test.describe`: Epic / Feature / Owner plus
 * the `layer` label the Go suites carry as a tag. Registered as a beforeEach so
 * every test in the block gets it without repeating itself.
 */
export function meta({ epic = EPIC, feature, owner = OWNER }: Meta): void {
  test.beforeEach(async () => {
    await allure.epic(epic);
    await allure.feature(feature);
    await allure.owner(owner);
    await allure.label("layer", LAYER);
  });
}

/**
 * Per-test identity: the Allure id (what test plans and history key on), the
 * Story, the severity and a TMS link built from the id by the reporter's
 * `links.tms.urlTemplate`. Call it first thing inside the test body.
 */
export async function testCase(
  id: string,
  story: string,
  severity: Severity = "normal",
): Promise<void> {
  await allure.allureId(id);
  await allure.story(story);
  await allure.severity(severity);
  await allure.tms(id, id);
}
