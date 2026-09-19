import { describe, expect, it } from "vitest";
import {
  ATTRIBUTION_VALUE,
  RATE_LIMITED_BODY,
  RATE_LIMIT_STATUS,
  cacheKeyUrl,
  canStartRun,
  checkRateLimit,
  clientKeyFor,
  isAdminAuthorized,
  nextAlarmAfterRun,
  normalizeQuery,
  routeFor,
  sha256Hex,
} from "./logic";

describe("normalizeQuery", () => {
  it("trims surrounding whitespace only", () => {
    expect(normalizeQuery("  node(1);out;\n")).toBe("node(1);out;");
  });

  it("does not touch internal whitespace", () => {
    expect(normalizeQuery("node(1);\n  out;")).toBe("node(1);\n  out;");
  });
});

describe("sha256Hex / cacheKeyUrl", () => {
  it("matches a known sha256 vector", async () => {
    // echo -n "hello" | sha256sum
    expect(await sha256Hex("hello")).toBe(
      "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
    );
  });

  it("is deterministic and content-addressed", async () => {
    const a = await cacheKeyUrl("node(1);out;");
    const b = await cacheKeyUrl("node(1);out;");
    const c = await cacheKeyUrl("node(2);out;");
    expect(a).toBe(b);
    expect(a).not.toBe(c);
  });

  it("produces a stable https URL under the documented prefix", async () => {
    const key = await cacheKeyUrl("node(1);out;");
    expect(key).toMatch(/^https:\/\/osmpq\.cache\/interpreter\/[0-9a-f]{64}$/);
  });

  it("distinguishes queries that differ only by output hint", async () => {
    const a = await cacheKeyUrl("node(1);out;", "json");
    const b = await cacheKeyUrl("node(1);out;", "xml");
    expect(a).not.toBe(b);
  });
});

describe("rate limiting", () => {
  function fakeBinding(success: boolean) {
    const calls: string[] = [];
    return {
      calls,
      binding: {
        async limit(options: { key: string }) {
          calls.push(options.key);
          return { success };
        },
      },
    };
  }

  it("allows when the binding reports success", async () => {
    const { binding } = fakeBinding(true);
    const decision = await checkRateLimit(binding, "1.2.3.4");
    expect(decision.allowed).toBe(true);
  });

  it("denies when the binding reports failure", async () => {
    const { binding } = fakeBinding(false);
    const decision = await checkRateLimit(binding, "1.2.3.4");
    expect(decision.allowed).toBe(false);
  });

  it("passes the client key through unchanged", async () => {
    const { binding, calls } = fakeBinding(true);
    await checkRateLimit(binding, "9.9.9.9");
    expect(calls).toEqual(["9.9.9.9"]);
  });

  it("429 body keeps the substrings overpass turbo looks for", () => {
    expect(RATE_LIMITED_BODY).toContain("rate_limited");
    expect(RATE_LIMITED_BODY).toContain("/api/status");
    expect(RATE_LIMIT_STATUS).toBe(429);
  });
});

describe("clientKeyFor", () => {
  it("uses CF-Connecting-IP", () => {
    const headers = new Headers({ "CF-Connecting-IP": "203.0.113.5" });
    expect(clientKeyFor(headers)).toBe("203.0.113.5");
  });

  it("falls back to a fixed key when absent", () => {
    const headers = new Headers();
    expect(clientKeyFor(headers)).toBe("unknown");
  });
});

describe("isAdminAuthorized", () => {
  const token = "s3cret-token-value";

  it("accepts a bare matching token", () => {
    expect(isAdminAuthorized(token, token)).toBe(true);
  });

  it("accepts a Bearer-prefixed matching token", () => {
    expect(isAdminAuthorized(`Bearer ${token}`, token)).toBe(true);
  });

  it("rejects a wrong token", () => {
    expect(isAdminAuthorized("Bearer wrong", token)).toBe(false);
  });

  it("rejects a missing header", () => {
    expect(isAdminAuthorized(null, token)).toBe(false);
  });

  it("rejects everything when no token is configured", () => {
    expect(isAdminAuthorized(token, "")).toBe(false);
  });

  it("rejects a token differing only in length", () => {
    expect(isAdminAuthorized(token + "x", token)).toBe(false);
  });
});

describe("routeFor", () => {
  it("routes GET and POST /api/interpreter", () => {
    expect(routeFor("GET", "/api/interpreter")).toEqual({ kind: "interpreter" });
    expect(routeFor("POST", "/api/interpreter")).toEqual({ kind: "interpreter" });
  });

  it("rejects other methods on /api/interpreter", () => {
    expect(routeFor("DELETE", "/api/interpreter")).toEqual({ kind: "not_found" });
  });

  it("routes the pass-through endpoints", () => {
    expect(routeFor("GET", "/api/status")).toEqual({ kind: "status" });
    expect(routeFor("GET", "/api/timestamp")).toEqual({ kind: "timestamp" });
    expect(routeFor("GET", "/healthz")).toEqual({ kind: "healthz" });
  });

  it("routes each admin scheduler action", () => {
    for (const action of ["start", "stop", "status", "run"] as const) {
      expect(routeFor("POST", `/admin/scheduler/${action}`)).toEqual({
        kind: "admin",
        action,
      });
    }
  });

  it("404s an unknown admin action", () => {
    expect(routeFor("POST", "/admin/scheduler/bogus")).toEqual({ kind: "not_found" });
  });

  it("404s everything else", () => {
    expect(routeFor("GET", "/")).toEqual({ kind: "not_found" });
    expect(routeFor("GET", "/favicon.ico")).toEqual({ kind: "not_found" });
  });

  it("ignores a trailing slash", () => {
    expect(routeFor("GET", "/api/status/")).toEqual({ kind: "status" });
  });
});

describe("attribution", () => {
  it("is the ODbL line", () => {
    expect(ATTRIBUTION_VALUE).toContain("OpenStreetMap contributors");
    expect(ATTRIBUTION_VALUE).toContain("ODbL");
  });
});

describe("nextAlarmAfterRun", () => {
  it("re-arms 60s after the run started, not after now", () => {
    const started = 1_000_000;
    const now = started + 5_000; // run has been going 5s
    const { fireAtMs, delayMs } = nextAlarmAfterRun(started, now, 60_000);
    expect(fireAtMs).toBe(started + 60_000);
    expect(delayMs).toBe(55_000);
  });

  it("fires immediately (delay 0) if the run already overran the interval", () => {
    const started = 1_000_000;
    const now = started + 90_000; // ran for 90s against a 60s interval
    const { delayMs } = nextAlarmAfterRun(started, now, 60_000);
    expect(delayMs).toBe(0);
  });
});

describe("canStartRun", () => {
  it("allows starting when nothing is running", () => {
    expect(canStartRun({ running: false })).toBe(true);
  });

  it("refuses to start a second overlapping run", () => {
    expect(canStartRun({ running: true })).toBe(false);
  });
});
