/**
 * Pure logic for the osmpq Worker: cache key derivation, the rate-limit
 * decision shape, admin auth, URL routing, and the updater scheduler's
 * next-alarm computation. Nothing here touches `fetch`, bindings, or any
 * Cloudflare runtime API, so it is exercised with plain `vitest` (no
 * `@cloudflare/vitest-pool-workers` needed) and reused unchanged by
 * `index.ts` / `updater.ts`.
 */

// ---------------------------------------------------------------------
// Query normalization and cache key
// ---------------------------------------------------------------------

/** Trim leading/trailing whitespace. This is the entire "normalization":
 * we deliberately do not reformat or reorder the query, since Overpass QL
 * is whitespace-insensitive in ways that are not safe to assume (string
 * literals, comments), and identical queries from real clients already
 * differ only in surrounding whitespace in practice (trailing newline from
 * a textarea, a leading space from string concatenation). */
export function normalizeQuery(raw: string): string {
  return raw.trim();
}

/** Lowercase hex sha256 of `text`, via WebCrypto (`crypto.subtle`), which
 * is available both in the Workers runtime and in Node >= 19 so this
 * function needs no polyfill under plain vitest. */
export async function sha256Hex(text: string): Promise<string> {
  const bytes = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/** The Cache API key for a normalized query, contract section 7: a
 * `https://osmpq.cache/...` URL (any `https://` origin works as a Cache
 * API key; it is never actually fetched) keyed by the sha256 of the
 * normalized query text plus the output format, since the same query
 * text under `[out:json]` vs `[out:xml]` must not share a cache entry. */
export async function cacheKeyUrl(
  normalizedQuery: string,
  outputHint: string = "",
): Promise<string> {
  const hash = await sha256Hex(`${outputHint}\n${normalizedQuery}`);
  return `https://osmpq.cache/interpreter/${hash}`;
}

// ---------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------

/** The subset of the Workers rate-limiting binding this module needs.
 * Matches the binding shape Cloudflare generates for a `ratelimits`
 * entry: `limit({ key }) -> { success: boolean }`. */
export interface RateLimiterBinding {
  limit(options: { key: string }): Promise<{ success: boolean }>;
}

export interface RateLimitDecision {
  allowed: boolean;
}

/** Ask the binding whether `clientKey` (normally the client IP) may
 * proceed. Factored out so the 429-vs-forward branch in `index.ts` is a
 * single call and the shape of the decision is independently testable
 * (see the fake binding in `logic.test.ts`). */
export async function checkRateLimit(
  binding: RateLimiterBinding,
  clientKey: string,
): Promise<RateLimitDecision> {
  const { success } = await binding.limit({ key: clientKey });
  return { allowed: success };
}

/** The Overpass-shaped 429 body. overpass turbo and other clients look
 * for the literal substring "rate_limited" and the `/api/status` hint
 * (contract section 6.1 / `docs/m3-contracts.md` section 6.1), so this
 * mirrors the reference server's text byte-for-byte in the parts that
 * matter. */
export const RATE_LIMITED_BODY =
  "<p>Error: runtime error: open64: 0 Success /osm3s_v0.7.62_osm_base " +
  "Dispatcher_Client::request_read_and_idx::rate_limited. Please check " +
  "/api/status for the quota of your IP address.</p>";

export const RATE_LIMIT_STATUS = 429;

// ---------------------------------------------------------------------
// Client IP
// ---------------------------------------------------------------------

/** The key used both for the rate limiter and for `OSMPQ_TRUST_PROXY`
 * downstream: Cloudflare's own `CF-Connecting-IP`, which is trustworthy
 * at the Worker (it cannot be spoofed by the client past Cloudflare's
 * edge) unlike `X-Forwarded-For`. Falls back to a fixed key so a request
 * that somehow lacks the header (should not happen in production) still
 * gets *a* consistent bucket instead of bypassing the limiter. */
export function clientKeyFor(headers: {
  get(name: string): string | null;
}): string {
  return headers.get("CF-Connecting-IP") ?? "unknown";
}

// ---------------------------------------------------------------------
// Admin auth
// ---------------------------------------------------------------------

/** Constant-time-ish comparison is not critical here (the token is a
 * high-entropy secret compared over HTTPS, not a password gate), but we
 * still avoid `===` on attacker-controlled length by checking length
 * first, and use a fixed-time XOR fold otherwise. */
export function isAdminAuthorized(
  headerValue: string | null,
  expectedToken: string,
): boolean {
  if (!expectedToken) return false;
  if (!headerValue) return false;
  const prefix = "Bearer ";
  const presented = headerValue.startsWith(prefix)
    ? headerValue.slice(prefix.length)
    : headerValue;
  if (presented.length !== expectedToken.length) return false;
  let diff = 0;
  for (let i = 0; i < presented.length; i++) {
    diff |= presented.charCodeAt(i) ^ expectedToken.charCodeAt(i);
  }
  return diff === 0;
}

// ---------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------

export type Route =
  | { kind: "interpreter" }
  | { kind: "status" }
  | { kind: "timestamp" }
  | { kind: "healthz" }
  | { kind: "admin"; action: "start" | "stop" | "status" | "run" }
  | { kind: "not_found" };

/** Pure routing decision from method + pathname, contract section 7:
 * `/api/interpreter` (GET/POST), `/api/status`, `/api/timestamp`,
 * `/healthz` pass through to the engine container; `/admin/scheduler/*`
 * is handled by the Worker itself against the `UpdaterScheduler` DO;
 * everything else is a 404. */
export function routeFor(method: string, pathname: string): Route {
  const path = pathname.replace(/\/+$/, "") || "/";
  if (path === "/api/interpreter") {
    if (method === "GET" || method === "POST") return { kind: "interpreter" };
    return { kind: "not_found" };
  }
  if (path === "/api/status" && method === "GET") return { kind: "status" };
  if (path === "/api/timestamp" && method === "GET") return { kind: "timestamp" };
  if (path === "/healthz" && method === "GET") return { kind: "healthz" };
  const adminMatch = /^\/admin\/scheduler\/(start|stop|status|run)$/.exec(path);
  if (adminMatch) {
    const action = adminMatch[1] as "start" | "stop" | "status" | "run";
    return { kind: "admin", action };
  }
  return { kind: "not_found" };
}

// ---------------------------------------------------------------------
// Attribution
// ---------------------------------------------------------------------

export const ATTRIBUTION_HEADER = "X-Attribution";
export const ATTRIBUTION_VALUE = "© OpenStreetMap contributors, ODbL";

// ---------------------------------------------------------------------
// Scheduler: next-alarm computation
// ---------------------------------------------------------------------

/** The updater scheduler re-arms 60s after the run *started* (not after
 * it finished), so a slow run just delays the next one rather than
 * compounding a fixed interval on top of an already-late one; contract
 * section 7 / `docs/design.md` section 4.2. Given `runStartedAtMs` and
 * the current time, returns the epoch ms at which the next alarm should
 * fire, and the delay from now (clamped to 0 so a run that took longer
 * than the interval fires immediately instead of scheduling in the
 * past). */
export function nextAlarmAfterRun(
  runStartedAtMs: number,
  nowMs: number,
  intervalMs: number = 60_000,
): { fireAtMs: number; delayMs: number } {
  const fireAtMs = runStartedAtMs + intervalMs;
  const delayMs = Math.max(0, fireAtMs - nowMs);
  return { fireAtMs, delayMs };
}

/** Whether the scheduler may start a new run right now, given the last
 * known run state. The DO is single-threaded so this is really a
 * belt-and-suspenders check (the container itself answers 409 while
 * busy, per contract section 7); it exists as a pure function so the
 * "never overlapping" rule has a unit test independent of Durable Object
 * plumbing. */
export function canStartRun(state: { running: boolean }): boolean {
  return !state.running;
}
