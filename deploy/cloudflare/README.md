# osmpq on Cloudflare

The Worker + Containers + Durable Object front end for osmpq (contract
section 7 of `docs/m3-contracts.md`; shape from `docs/design.md` section
6). For the full zero-to-public-endpoint procedure, see
`docs/m3-runbook.md`. This file just covers what lives here and how to
work on it.

## Layout

| File | What it is |
| --- | --- |
| `src/logic.ts` | Pure functions: query normalization + cache key, the rate-limit decision, admin auth, URL routing, the updater scheduler's next-alarm math. No `fetch`, no bindings — this is what `src/logic.test.ts` exercises with plain vitest. |
| `src/index.ts` | The Worker: routes requests, enforces the rate limit, serves from/populates the Cache API, forwards to an `EngineContainer`, and handles `/admin/scheduler/*` against the `UpdaterScheduler` Durable Object. |
| `src/engine.ts` | `EngineContainer`: one instance of the osmpq image running `osmpq serve` on port 8080. |
| `src/updater.ts` | `UpdaterContainer` (the same image running `osmpq updater-server` on port 8081) and `UpdaterScheduler`, the Durable Object that owns the "cron with a lock" for it. |
| `wrangler.jsonc` | Deployment config: the two container apps, the three Durable Object bindings + SQLite migration, the rate-limit binding, non-secret `vars`. |
| `wrangler.test.jsonc` | A narrower config used only by the Worker-level vitest suite (`test/worker.test.ts`) — see its header comment for why it drops the `containers` block. |
| `docs/m3-runbook.md` (repo root `docs/`) | Bucket + token setup, `wrangler deploy`, secrets, starting the scheduler, verifying against overpass turbo, cost, compaction, failure handling. |

## Setup

```
npm install
npm run cf-typegen   # wrangler types -> worker-configuration.d.ts (gitignored, regenerate after any wrangler.jsonc change)
npm run typecheck    # tsc --noEmit
npm test             # plain-vitest suite over src/logic.ts
npm run test:worker  # Worker-level suite against the real Workers runtime (Miniflare)
```

`cf-typegen` needs the repo-root `Dockerfile` (workstream W4) to already
exist, because `wrangler` validates every `containers[].image` path —
including a plain type-generation run — before it will do anything else.
If you're working in this directory before that file lands, point
`wrangler.jsonc`'s two `image` fields at a placeholder
(`"docker.io/library/httpd:2.4"` works) just long enough to run
`cf-typegen`, then put `"../../Dockerfile"` back; `worker-configuration.d.ts`
itself doesn't depend on the image path, only the validation gate does.

## Environment variables and secrets

Non-secret (`wrangler.jsonc`'s `vars`, editable directly):

| Var | Meaning |
| --- | --- |
| `OSMPQ_ROOT` | The dataset root as an `s3://` URL (R2 bucket + prefix). |
| `OSMPQ_ANNOUNCED_ENDPOINT` | The public URL shown in `/api/status`. |
| `OSMPQ_REPLICATION_SOURCE` | Minutely-replication base URL the updater polls. |
| `ENGINE_INSTANCES` | How many `EngineContainer` instances the Worker load-balances across (`getRandom`). Raise alongside `containers[0].max_instances`. |

Secrets (`wrangler secret put <NAME>`, never in `wrangler.jsonc`):

| Secret | Meaning |
| --- | --- |
| `OSMPQ_S3_KEY_ID` | R2 API token access key id. |
| `OSMPQ_S3_SECRET` | R2 API token secret. |
| `OSMPQ_S3_ENDPOINT` | `<account>.r2.cloudflarestorage.com` (host only, no scheme). |
| `ADMIN_TOKEN` | Bearer token guarding `/admin/scheduler/*`. |

The Worker passes the `OSMPQ_S3_*` values and `OSMPQ_ROOT` into both
container classes' `envVars`; `EngineContainer` also sets
`OSMPQ_TRUST_PROXY=1` (the Worker is the only path in, so
`CF-Connecting-IP` is trustworthy) and `UpdaterContainer` adds
`OSMPQ_REPLICATION_SOURCE`. See `src/osmpq/engine/executor.py`'s module
docstring for what the engine does with each `OSMPQ_S3_*` variable.

## Design notes worth knowing before changing this code

- **Cache key.** `logic.cacheKeyUrl` hashes the *normalized* (trimmed)
  query text, not the raw request — so a trailing newline from a
  textarea doesn't fragment the cache. It intentionally does not fold in
  the manifest generation number: the origin's own `Cache-Control:
  public, max-age=60` already bounds staleness to a minute, which is
  simpler than invalidating on every manifest swap (the manifest changes
  roughly every minute anyway under the updater).
- **Cache API and POST.** A plain string is passed to `caches.default.
  match`/`.put` rather than a `Request` object, which the Cache API
  treats as a `GET` regardless of the original method — so a POSTed
  query (the common case for anything but the shortest queries) is still
  cached.
- **Rate limiting is by `CF-Connecting-IP` only**, not by API key; there
  is no API-key concept yet. It only guards `/api/interpreter` —
  `/api/status`, `/api/timestamp` and `/healthz` pass straight through,
  same as the reference Overpass server.
- **The scheduler never overlaps a run with itself** two ways at once:
  the Durable Object is single-threaded (so its own `running` flag can't
  race), and the container answers `409` if a run is already in flight
  even after a DO restart lost that flag. Either one alone would be
  enough; both together is what `docs/design.md` section 4.2 calls "cron
  with a lock."
- **Re-arming uses the run's start time, not its finish time**
  (`logic.nextAlarmAfterRun`): a slow run delays the next one instead of
  the fixed interval compounding on top of an already-late one.
- **Attribution header.** `X-Attribution: © OpenStreetMap contributors,
  ODbL` is added to every response the Worker returns, matching contract
  section 7. Miniflare warns that the `©` byte is technically outside
  plain ASCII for a header value (a Fetch-spec quirk some strict browser
  clients could choke on reading it back via `fetch()`); it is sent
  as-is because the contract specifies this exact text and no client in
  the corpus reads this header programmatically.

## Testing strategy

`src/logic.test.ts` (plain vitest, `npm test`) covers every pure
function directly: cache-key determinism and collision-avoidance across
different queries/output hints, the rate-limit allow/deny shape, admin
auth (bare token, `Bearer `-prefixed, wrong token, empty configured
token, length mismatch), the full routing table including the 404
fallback and trailing-slash handling, and the scheduler's re-arm math
including the "run overran the interval" edge case.

`test/worker.test.ts` (`npm run test:worker`,
`@cloudflare/vitest-pool-workers`) runs the real `src/index.ts` inside
Miniflare/workerd, but only exercises the routes that don't need a
running container: the 404 fallback (with the attribution header
attached) and the `/admin/scheduler/*` auth + Durable Object round trip.
The container-backed routes (`/api/interpreter` and friends) need an
actual container instance, which Miniflare has no way to start without a
Docker daemon; `wrangler.test.jsonc` deliberately has no `containers`
block for that reason. Those routes are covered indirectly (their
routing decision, cache key and rate-limit logic are all pure functions
tested above) and directly by the runbook's manual verification step
against a real deployment.
