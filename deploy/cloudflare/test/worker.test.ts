import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { ATTRIBUTION_HEADER } from "../src/logic";

// Runs against the real Worker entry point (`src/index.ts`) inside the
// Workers runtime via Miniflare, using `wrangler.test.jsonc` (no
// `containers` block — see that file). Only routes that never reach
// `ENGINE`/`UPDATER` are exercised here; the container-backed routes
// (`/api/interpreter`, `/api/status`, `/api/timestamp`, `/healthz`) need
// a real container runtime and are left to the runbook's manual
// verification step and to `src/logic.test.ts`'s coverage of the pure
// routing/cache-key/rate-limit decisions they're built from.
describe("Worker: routes not backed by a container", () => {
  it("404s an unknown path and still attaches attribution", async () => {
    const response = await SELF.fetch("https://osmpq.example.org/nope");
    expect(response.status).toBe(404);
    expect(response.headers.get(ATTRIBUTION_HEADER)).toContain("OpenStreetMap");
  });

  it("rejects an admin action with no Authorization header", async () => {
    const response = await SELF.fetch(
      "https://osmpq.example.org/admin/scheduler/status",
      { method: "GET" },
    );
    expect(response.status).toBe(401);
  });

  it("rejects an admin action with the wrong token", async () => {
    const response = await SELF.fetch(
      "https://osmpq.example.org/admin/scheduler/status",
      { headers: { Authorization: "Bearer wrong-token" } },
    );
    expect(response.status).toBe(401);
  });

  it("accepts the admin token and reaches the UpdaterScheduler DO", async () => {
    const response = await SELF.fetch(
      "https://osmpq.example.org/admin/scheduler/status",
      { headers: { Authorization: "Bearer test-admin-token" } },
    );
    expect(response.status).toBe(200);
    const body = await response.json();
    expect(body).toMatchObject({ running: false, scheduled: false });
  });
});
