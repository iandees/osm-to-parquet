/**
 * osmpq Worker: the public front door (contract section 7,
 * `docs/design.md` section 6.1). Cheap, high-volume work happens here —
 * rate limiting, response caching, routing — and every byte of dataset
 * access happens in the container this Worker forwards to.
 */
import { getRandom, getContainer } from "@cloudflare/containers";
import {
  ATTRIBUTION_HEADER,
  ATTRIBUTION_VALUE,
  RATE_LIMITED_BODY,
  RATE_LIMIT_STATUS,
  cacheKeyUrl,
  checkRateLimit,
  clientKeyFor,
  isAdminAuthorized,
  normalizeQuery,
  routeFor,
} from "./logic";
import { EngineContainer } from "./engine";
import { UpdaterContainer, UpdaterScheduler } from "./updater";

// Durable Object classes must be exported from the Worker's main module
// for `wrangler.jsonc`'s `durable_objects.bindings` to find them.
export { EngineContainer } from "./engine";
export { UpdaterContainer, UpdaterScheduler } from "./updater";

/** The full environment this Worker needs. Extends the config-derived
 * `Cloudflare.Env` (from generated `worker-configuration.d.ts`) with the
 * bindings and secrets that aren't visible to `wrangler types` from
 * `wrangler.jsonc` alone: `ADMIN_TOKEN` and the `OSMPQ_S3_*` credentials
 * are Worker secrets (`wrangler secret put`, see the README), and the
 * three Durable Object namespaces are re-typed to their concrete classes
 * so `getRandom`/`getContainer` type-check. */
export interface WorkerEnv extends Omit<Cloudflare.Env, "ENGINE" | "UPDATER" | "SCHEDULER"> {
  ENGINE: DurableObjectNamespace<EngineContainer>;
  UPDATER: DurableObjectNamespace<UpdaterContainer>;
  SCHEDULER: DurableObjectNamespace<UpdaterScheduler>;
  ADMIN_TOKEN: string;
  OSMPQ_S3_KEY_ID?: string;
  OSMPQ_S3_SECRET?: string;
  OSMPQ_S3_ENDPOINT?: string;
  OSMPQ_S3_REGION?: string;
}

const SCHEDULER_INSTANCE_NAME = "scheduler";

function withAttribution(response: Response): Response {
  const out = new Response(response.body, response);
  out.headers.set(ATTRIBUTION_HEADER, ATTRIBUTION_VALUE);
  return out;
}

function rateLimitedResponse(): Response {
  return new Response(RATE_LIMITED_BODY, {
    status: RATE_LIMIT_STATUS,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

/** Extract the query text the same way `server.py::_extract_query` does:
 * a `data` query-string parameter on either method, else (POST only) a
 * form-encoded `data` field in the body, else the raw body text. Reads
 * from a clone so the original request body is still available to
 * forward to the container unread. */
async function extractQuery(request: Request, url: URL): Promise<string> {
  const dataParam = url.searchParams.get("data");
  if (dataParam !== null) return dataParam;
  if (request.method !== "POST") return "";
  const bodyText = await request.clone().text();
  if (!bodyText) return "";
  try {
    const parsed = new URLSearchParams(bodyText);
    const fromForm = parsed.get("data");
    if (fromForm !== null) return fromForm;
  } catch {
    // Not form-encoded; the raw body *is* the query.
  }
  return bodyText;
}

async function handleInterpreter(
  request: Request,
  url: URL,
  env: WorkerEnv,
): Promise<Response> {
  const clientKey = clientKeyFor(request.headers);
  const decision = await checkRateLimit(env.RATE_LIMITER, clientKey);
  if (!decision.allowed) {
    return rateLimitedResponse();
  }

  const queryText = await extractQuery(request, url);
  const normalized = normalizeQuery(queryText);
  // A plain string cache key is treated as a GET request by the Cache
  // API, so a POST query still hits/populates the cache under this key
  // (contract section 7: "look up the Cache API with key
  // https://osmpq.cache/interpreter/<sha256(query)>").
  const cacheKey = await cacheKeyUrl(normalized);
  const cache = caches.default;

  const cached = await cache.match(cacheKey);
  if (cached) {
    return withAttribution(cached);
  }

  const instances = Number(env.ENGINE_INSTANCES) || 1;
  const container = await getRandom(env.ENGINE, instances);
  const response = await container.containerFetch(request, 8080);

  // A timeout/cancellation/runtime error still comes back as `200 OK`
  // with a `remark` field (Overpass's own convention), so `response.ok`
  // alone can't tell a real result from a failed one -- the container
  // sets `Cache-Control: no-store` on those (`src/osmpq/server.py`)
  // instead of the usual `public, max-age=60`. Check for that explicitly
  // rather than relying on `cache.put` to no-op gracefully on a
  // `no-store` response -- its documented behavior there isn't precise
  // enough to trust for "never caches a timed-out query result".
  const cacheControl = response.headers.get("Cache-Control") ?? "";
  if (response.ok && !cacheControl.includes("no-store")) {
    // Cache a clone; the original body still needs to go back to the
    // client.
    await cache.put(cacheKey, response.clone());
  }
  return withAttribution(response);
}

async function passThrough(request: Request, env: WorkerEnv): Promise<Response> {
  const instances = Number(env.ENGINE_INSTANCES) || 1;
  const container = await getRandom(env.ENGINE, instances);
  const response = await container.containerFetch(request, 8080);
  return withAttribution(response);
}

async function handleAdmin(
  request: Request,
  action: "start" | "stop" | "status" | "run",
  env: WorkerEnv,
): Promise<Response> {
  if (!isAdminAuthorized(request.headers.get("Authorization"), env.ADMIN_TOKEN)) {
    return withAttribution(new Response("unauthorized", { status: 401 }));
  }
  const id = env.SCHEDULER.idFromName(SCHEDULER_INSTANCE_NAME);
  const stub = env.SCHEDULER.get(id);
  const response = await stub.fetch(`https://scheduler/${action}`, {
    method: action === "status" ? "GET" : "POST",
  });
  return withAttribution(response);
}

export default {
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    const url = new URL(request.url);
    const route = routeFor(request.method, url.pathname);

    switch (route.kind) {
      case "interpreter":
        return handleInterpreter(request, url, env);
      case "status":
      case "timestamp":
      case "healthz":
        return passThrough(request, env);
      case "admin":
        return handleAdmin(request, route.action, env);
      case "not_found":
      default:
        return withAttribution(new Response("not found", { status: 404 }));
    }
  },
} satisfies ExportedHandler<WorkerEnv>;
