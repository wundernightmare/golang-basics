import { expect, test } from "@playwright/test";

import { PING_URL } from "../helpers/env";

test.describe("ping service", () => {
  test("GET /ping returns pong @smoke", async ({ request }) => {
    const res = await request.get(`${PING_URL}/ping`);
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body).toMatchObject({ message: "pong" });
    expect(body.echo).toBeUndefined();
  });

  test("GET /ping?msg= echoes the message", async ({ request }) => {
    const res = await request.get(`${PING_URL}/ping?msg=hello`);
    expect(res.status()).toBe(200);
    expect(await res.json()).toMatchObject({ message: "pong", echo: "hello" });
  });

  test("GET /version reports the service name", async ({ request }) => {
    const res = await request.get(`${PING_URL}/version`);
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.service).toBe("ping");
    expect(body.go_version).toBeTruthy();
  });

  test("unknown route returns 404", async ({ request }) => {
    const res = await request.get(`${PING_URL}/nope`);
    expect(res.status()).toBe(404);
  });
});
