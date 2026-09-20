# M3 runbook: Cloudflare deployment, zero to public endpoint

This is the procedure for turning a built dataset root (`docs/m1-runbook.md`)
into a running public Overpass-QL endpoint on Cloudflare: a Worker in front
of `EngineContainer`/`UpdaterContainer` instances (workstream W4's image)
and an `UpdaterScheduler` Durable Object that drives minutely updates. It
assumes `deploy/cloudflare/` as built by workstream W5; see that
directory's `README.md` for what each file does and does not cover.

Nothing here can be exercised in the sandbox that developed this code (no
Cloudflare account, no `wrangler login`); every command below is untested
against a real account and the numbers are the design document's estimates
(`docs/design.md` section 7) until a real run corrects them.

## 1. R2 bucket and API token

1. In the Cloudflare dashboard, create an R2 bucket (say `osm-planet`) in
   the same account the Worker will deploy to. R2's cross-region egress to
   Cloudflare Workers/Containers in the same account is free, which is the
   entire reason this design uses R2 over S3.
2. Create an R2 API token (**R2 → Manage API tokens**) scoped to that
   bucket with **Object Read & Write**. Record its access key id, secret,
   and the account's R2 endpoint host: `<account-id>.r2.cloudflarestorage.com`.
   These become `OSMPQ_S3_KEY_ID`, `OSMPQ_S3_SECRET`, `OSMPQ_S3_ENDPOINT`
   (section 4 below).
3. This is the same bucket `docs/m1-runbook.md` section 6 syncs a built
   root into with `rclone`; if you haven't run that yet, do it now — the
   Worker has nothing to serve without a `manifest/LATEST` in the bucket.

## 2. Sync a built root

Follow `docs/m1-runbook.md` end to end (Minnesota extract first, planet
once that works) to produce a root and `rclone sync` it to
`r2:osm-planet/current`. Confirm it landed:

```
rclone ls r2:osm-planet/current/manifest/LATEST
```

Record the path you synced to (`s3://osm-planet/current` here) — it's
`OSMPQ_ROOT` below.

## 2a. History (M4): give the root an attic before syncing

A root gets its history dataset in two steps (`docs/m4-contracts.md`
sections 2 and 5; `tools/m4_dataset.sh` is the scripted version):

```
osmpq history init /fast/root --threads 4 --memory-limit 6GB --tmpdir /fast/tmp
osmpq update /fast/root --source <replication url> --max-diffs 60   # repeat until current
osmpq compact /fast/root && osmpq gc /fast/root --keep 1
osmpq validate /fast/root
```

`history init` turns every current row into its first state (one Parquet
file at a time, flat memory: 70 s and 2.6 GB for Minnesota); every later
`osmpq update` run appends the new versions, minor versions and
tombstones to the rolling history tiers, and `osmpq compact` folds them
into the base history. `[date:]`, `retro`, `timeline`, `[diff:]`/`[adiff:]`
and exact `(changed:)` then work from the base timestamp on. Without a
`history` section in the manifest those settings return the "no history"
error and nothing else changes.

`tools/upload_root.py /fast/root s3://<bucket>/<prefix>` uploads exactly
the files the latest manifest references (manifest last), resuming where
it left off; it reads the same `OSMPQ_S3_*` variables as the engine.
Minnesota with history (5.3 GB, 1,333 files) took 102 s to R2 from the
development sandbox. The engine and the updater then take the `s3://`
root directly (`OSMPQ_ROOT`, or `osmpq update s3://... --source ...`).

## 3. Install and configure `deploy/cloudflare`

```
cd deploy/cloudflare
npm install
npm run cf-typegen   # generates worker-configuration.d.ts; needs the
                      # repo-root Dockerfile to already exist (W4)
npm run typecheck
npm test
```

Edit `wrangler.jsonc`'s `vars`:

- `OSMPQ_ROOT`: the `s3://` URL from step 2.
- `OSMPQ_ANNOUNCED_ENDPOINT`: the public URL you intend to hand out, e.g.
  `https://osmpq.<your-subdomain>.workers.dev/api/interpreter` for a first
  deploy on the free `workers.dev` domain, or your own domain once you've
  attached a route.
- `OSMPQ_REPLICATION_SOURCE`: leave the OSM planet's own minutely
  replication server unless you're pointing at a regional mirror.
- `ENGINE_INSTANCES`: start at `2`; see section 9 for raising it later.

`wrangler login` once, interactively, to authenticate the CLI (there is
no way to script this step; it opens a browser).

## 4. First deploy

```
npx wrangler deploy
```

This is the first deploy that actually builds the container image from
the repo-root `Dockerfile` (workstream W4) and pushes it to Cloudflare's
registry, creates the two container apps (`osmpq-engine`, `osmpq-updater`),
the three Durable Object classes with their SQLite migration, and the
Worker itself. Expect the image build/push to be the slowest part (several
minutes; DuckDB's `spatial`/`httpfs` extensions are baked in, so the image
is not tiny).

A rate-limiting binding (`ratelimits` in `wrangler.jsonc`) needs a
namespace id Cloudflare allocates on first use; if `wrangler deploy`
reports that `namespace_id: "1001"` (the placeholder committed in
`wrangler.jsonc`) doesn't exist, create the namespace as the error message
directs (or via the dashboard's Rate Limiting product) and put the real id
in `wrangler.jsonc`, then redeploy.

## 5. Secrets

```
npx wrangler secret put OSMPQ_S3_KEY_ID
npx wrangler secret put OSMPQ_S3_SECRET
npx wrangler secret put OSMPQ_S3_ENDPOINT
npx wrangler secret put ADMIN_TOKEN        # any high-entropy string you generate, e.g. `openssl rand -hex 32`
```

Secrets take effect for new container/Worker invocations without a
redeploy, but an already-running `EngineContainer`/`UpdaterContainer`
instance keeps the environment it started with — either wait for it to
sleep (`sleepAfter`, section 9) or force a redeploy (`npx wrangler deploy`)
to restart with the new secrets picked up.

## 6. Start the scheduler

The updater never runs on its own until the `UpdaterScheduler` Durable
Object is told to start:

```
curl -X POST https://<your-worker>/admin/scheduler/start \
    -H "Authorization: Bearer <ADMIN_TOKEN>"
```

Check it's actually running:

```
curl https://<your-worker>/admin/scheduler/status \
    -H "Authorization: Bearer <ADMIN_TOKEN>"
```

`running` should go `true` then back to `false` roughly every minute as
each run completes, `lastSummary` should start filling in, and
`nextAlarmAt` should keep moving forward. If `lastError` is non-null, see
section 10.

## 7. Verify

Direct check:

```
curl https://<your-worker>/api/status
curl 'https://<your-worker>/api/interpreter' \
    --data-urlencode 'data=[out:json];node(44.970,-93.280,44.985,-93.255)["amenity"="cafe"];out;'
```

The first response should show `Announced endpoint:` matching
`OSMPQ_ANNOUNCED_ENDPOINT` and a nonzero rate limit; the second should
return real Minneapolis cafes if you synced Minnesota, or whatever your
bbox covers on a planet dataset.

Through overpass turbo (https://overpass-turbo.eu/): gear icon → **Settings**
→ set **Overpass API Server** to `https://<your-worker>/api/` (note the
trailing slash; overpass turbo appends `interpreter`) → run any query
against your bbox. If it returns results and the response's headers (dev
tools → Network tab) show `X-Attribution: © OpenStreetMap contributors,
ODbL`, the deployment is serving correctly end to end, including the
Worker's cache and rate-limit path.

Cold-start expectation (`docs/design.md` section 6.1): the first query
after a container has slept pays roughly a second or two for container
boot plus DuckDB init; subsequent queries within `sleepAfter` should feel
close to instant, dominated by however many R2 range reads the query
itself needs.

## 8. Cost expectations

From `docs/design.md` section 7, all 2026 list prices — treat these as
starting estimates, not guarantees, and watch the Cloudflare dashboard's
R2/Workers/Containers usage pages for the first real month:

- **Storage**: current dataset (spatial + id-sorted copies, indexes,
  areas) roughly 600-900 GB for the planet → ~$9-14/month; a Minnesota-only
  extract is a small fraction of that. R2 has zero egress to Workers in
  the same account.
- **R2 operations**: a typical query issues 10-200 range requests, so
  roughly $0.01-0.07 per 1,000 queries; the updater's minutely rewrites
  add a few hundred writes per hour.
- **Container compute**: `standard-3` active time is about $0.00006/active
  second (~$0.22/active hour); a two-second query is a fraction of a
  cent. Sleeping instances (see `sleepAfter`, section 9) cost nothing.
  The Workers Paid plan ($5/month) includes an allowance that covers
  light traffic outright.
- **Updater container**: one run/minute, active maybe 15-40s each →
  roughly $20-45/month of active-equivalent compute, plus a few thousand
  R2 reads/minute (~$1-2/month). Weekly compaction (section 9 of
  `docs/design.md`) on a bigger instance type adds a few dollars when it
  runs.
- **Crossover point**: at sustained heavy load (very roughly 100k
  two-second queries/day) serverless container compute starts costing
  more per day than a small fixed VM pool would; nothing here forces that
  choice early, since the same image runs unmodified on a VM if it ever
  makes sense to switch.

## 9. Lengthening `sleepAfter` / adding instances

Both are one-line edits to `deploy/cloudflare/src/engine.ts` /
`src/updater.ts` (or `wrangler.jsonc` for instance counts) plus a
redeploy — no design change:

- **Keep an engine instance warm longer**: raise `sleepAfter` in
  `src/engine.ts` (e.g. `"10m"` or `"30m"` once there's steady daytime
  traffic worth not paying a cold start for). `docs/design.md` section 6.1
  explicitly frames the initial short `sleepAfter` as a "pay for cold
  starts, not idle time" choice for a service with few users, to be
  revisited as traffic grows.
- **More concurrent engine capacity**: raise both `ENGINE_INSTANCES` in
  `wrangler.jsonc`'s `vars` (what the Worker's `getRandom` load-balances
  across) and `containers[0].max_instances` for `osmpq-engine` in the same
  file to at least that number, then `npx wrangler deploy`.
- **Bigger instances**: `instance_type` in `wrangler.jsonc`'s `containers`
  entries goes up to `standard-4` (4 vCPU, 12 GiB, 20 GB disk) without any
  code change; a planet-scale query that needs more than `standard-3`'s
  8 GiB is the usual reason.
- The updater container's `sleepAfter` (`src/updater.ts`, `"10m"`) trades
  off differently: it exists to keep the updater's local disk cache warm
  between one-per-minute runs (`docs/design.md` section 4.1), not to serve
  concurrent traffic, so there is normally no reason to raise
  `max_instances` for `osmpq-updater` past `1` — see the next section for
  why.

## 10. Compaction procedure

Compaction (rewriting touched base cell files into a new generation,
`docs/design.md` section 4.4) runs on a bigger node against a local
mirror, not on the always-on updater container, because it needs local
scratch space and can take hours on a full planet dataset:

1. **Pause the scheduler** so the updater stops writing new rolling deltas
   mid-compaction:
   ```
   curl -X POST https://<your-worker>/admin/scheduler/stop \
       -H "Authorization: Bearer <ADMIN_TOKEN>"
   ```
   Confirm with `/admin/scheduler/status` that `scheduled: false`.
2. **Mirror the current root locally** on the big node (or the same node
   used for the initial load):
   ```
   rclone sync r2:osm-planet/current /fast/root --transfers 32 --checkers 64 -P
   ```
3. **Compact** against that local mirror — this is the same `osmpq compact`
   from the M2 contracts, run locally, not through the updater container:
   ```
   osmpq compact /fast/root --threads $(nproc) --memory-limit 48GB
   osmpq validate /fast/root
   ```
4. **Sync the new generation back**, manifest last (same ordering caveat
   as the initial sync in `docs/m1-runbook.md` section 6):
   ```
   rclone sync /fast/root r2:osm-planet/current --exclude 'manifest/**' --transfers 32 -P
   rclone copy /fast/root/manifest r2:osm-planet/current/manifest
   ```
5. **Resume the scheduler**:
   ```
   curl -X POST https://<your-worker>/admin/scheduler/start \
       -H "Authorization: Bearer <ADMIN_TOKEN>"
   ```
   The next run picks up the new generation's manifest and starts fresh
   rolling deltas against it, same as the updater does every run
   (`docs/design.md` section 4.2, step 1).

Readers never see a broken intermediate state during any of this, because
manifests are immutable and `LATEST` is swapped atomically; the only
user-visible effect of skipping step 1 would be the updater and the
compaction job racing to write the next generation, which step 1 avoids
entirely.

## 11. Failure handling

- **`/admin/scheduler/status` shows a non-null `lastError`**: the updater
  container returned an error or the DO couldn't reach it. Check the
  container's logs (Cloudflare dashboard → your account → Containers →
  `osmpq-updater`, or `npx wrangler tail` while a run fires) for the
  underlying `osmpq updater-server` error. Common causes: a replication
  gap or a temporarily unreachable `OSMPQ_REPLICATION_SOURCE`, or stale/
  wrong `OSMPQ_S3_*` secrets after a token rotation. Fix the cause and
  either wait for the next alarm or trigger one immediately:
  ```
  curl -X POST https://<your-worker>/admin/scheduler/run \
      -H "Authorization: Bearer <ADMIN_TOKEN>"
  ```
- **A run reports 409 from the container repeatedly**: the DO's own
  `running` flag and the container's internal lock disagree — normally
  self-heals within one more interval once the stuck run's `/run` request
  finally completes or times out. If it persists, `stop` the scheduler,
  give the container a minute to finish or be recycled, then `start`
  again.
- **Queries return stale data**: check `/api/timestamp` against
  `/admin/scheduler/status`'s `lastFinishedAt`; if the scheduler is
  healthy but the timestamp isn't moving, the engine's manifest refresh
  interval (`OSMPQ_MANIFEST_REFRESH_SECONDS`, default 60s server-side) is
  the likely lag, not a broken updater — wait a refresh interval and
  recheck before treating it as a failure.
- **A single query times out or looks like it's scanning too much**: this
  is the engine's own `[timeout:]`/`[maxsize:]` clamps and `429` rate
  limiting doing their job (contract section 6.1), not a deployment
  failure; see `docs/m3-contracts.md` section 6.1 for the exact limits
  and how to raise them (`OSMPQ_MAX_TIMEOUT`, `OSMPQ_MAX_MAXSIZE`) if a
  legitimate query needs more room.
- **Cold-start latency feels too high**: raise `sleepAfter` (section 9)
  before assuming something is broken — a fully cold container paying
  DuckDB init plus its first few uncached Parquet footer fetches is
  expected to take a second or two per `docs/design.md` section 6.1's
  cold-start budget.
