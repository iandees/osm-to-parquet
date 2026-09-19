# M3 contracts: public beta

M3 goal: turn the M2 engine into a service. Two halves, developed in
parallel and validated on Minnesota:

1. **Tier-2 language** (`docs/overpass-ql-support.md`): `around`, `poly`,
   areas (`area[...]`, `(area)`, `(pivot)`, `is_in`, `map_to_area`), meta
   filters (`newer`, `changed`, `user`, `uid`), an evaluator subset for
   `if`, `foreach` and `(if:)`, `out geom(bbox)` and `[out:csv]`.
2. **Service**: request limits and Overpass-shaped status endpoints, a
   container image, an updater that runs against an `s3://` root, and the
   Cloudflare Worker + Containers + Durable Object scheduler from
   `docs/design.md` section 6 and 4.2.

M0–M2 contracts still hold. Nothing here changes the base/delta layout for
nodes, ways or relations; areas are a new, additive table.

## 1. Division of work and file ownership

Five workstreams run concurrently in separate worktrees. Each owns the
files listed; touching another workstream's file is allowed only for the
one-line integration points named in its section, so merges stay trivial.

| workstream | owns |
| --- | --- |
| W1 geometry filters + outputs | `src/osmpq/engine/geofilters.py` (new), `src/osmpq/engine/render.py` (clipping only), `src/osmpq/engine/result.py` (csv), `planner.check_settings` (lift the csv rejection), `tools/difftest.py` (csv comparison), `tests/test_engine_geofilters.py`, `tests/test_out_csv.py`, corpus 33–35, 46–47 |
| W2 areas | `src/osmpq/build/areas.py` (new), `src/osmpq/engine/areas.py` (new), `src/osmpq/layout/manifest.py` (v4 `areas` field), `src/osmpq/engine/catalog.py` (area table in `cells_for_bbox` and file listing), `render.py`/`result.py` (the `area` element branch only), `src/osmpq/build/compact.py` (area re-derivation), `builder.py` (call areas at the end), `validate.py`, `cli.py` (`areas` subcommand), `tests/fixtures/make_fixture.py` (v4), `tests/test_areas*.py`, corpus 36–40, 49 |
| W3 meta filters + evaluator | `src/osmpq/ql/evaluator.py` (new), `src/osmpq/engine/evalsql.py` (new), `src/osmpq/engine/metafilters.py` (new), `src/osmpq/engine/evalfilter.py` (new), `tests/test_evaluator.py`, `tests/test_engine_metafilters.py`, `tests/test_engine_control.py`, corpus 41–45, 48 |
| W4 service | `src/osmpq/server.py`, `src/osmpq/engine/executor.py` (manifest refresh, cancel), `src/osmpq/store.py` (new), `src/osmpq/update/updater.py` (s3 root), `src/osmpq/update/server.py` (new), `layout/manifest.py` (writes through the store), `cli.py` (`serve`, `updater-server`), `Dockerfile`, `.dockerignore`, `tests/test_server_limits.py`, `tests/test_store.py`, `tests/test_updater_s3.py` |
| W5 deployment | `deploy/cloudflare/**`, `docs/m3-runbook.md` |

The coordinator owns `docs/m3-contracts.md`, `src/osmpq/engine/hooks.py`,
`planner.py` (already prepared, section 2) and the merge.

Common rules: Python 3.12, DuckDB 1.5.5 with `spatial`/`httpfs`, tests
under `tests/` with pytest, no new dependencies beyond `shapely` (W2) and
`boto3` (W4), which go into `pyproject.toml`. Every feature gets unit
tests on the synthetic fixture (`tests/fixtures/make_fixture.py`) and a
corpus query (`tests/corpus/NN_name.overpassql`, bbox names from
`tests/corpus/bboxes.json`) the harness can grade against the reference.
Commit in the worktree; do not push.

## 2. Planner extension points (done; read `src/osmpq/engine/hooks.py`)

`planner.execute_query` builds the base SELECT exactly as in M2 and then:

* takes the **effective bbox** = explicit/global bbox ∩ every hooked
  filter's `implied_bbox(ctx, q, f)`; that bbox drives cell and row-group
  selection. An empty intersection short-circuits to an empty set;
* wraps the base select as `SELECT * FROM (<base>) __q WHERE p1 AND p2 …`
  with each hooked filter's `predicate(ctx, q, f, "__q")` — a SQL boolean
  over the canonical set columns (`engine/schema.py`). Predicates may
  create temp tables via `ctx.fresh_name`.

Statement classes the core does not handle (`IsIn`, `MapToArea`,
`Foreach`, `If`) dispatch through `hooks.STATEMENT_HOOKS`; a `Query` with
type `area` goes to `hooks.AREA_QUERY_HOOK`. Modules named in
`hooks.HOOK_MODULES` are imported lazily on the first run and register
themselves at import; a missing module means "unsupported", never an
error. Each engine workstream adds only its own module(s) and never edits
`planner.py`.

Canonical columns that matter for hooks: nodes have `lat_e7`/`lon_e7`;
ways and relations always have `xmin_e7..ymax_e7`; `geometry` is a
LINESTRING for ways read from spatial files and NULL otherwise (byid path,
relations). `ctx.manifest`, `ctx.promoted_keys`, `ctx.con` (this run's
cursor), `ctx.files_read` (add what you read) and `ctx.warnings` are
available.

## 3. W1: `around`, `poly`, `out geom(bbox)`, `[out:csv]`

### 3.1 Distances

All distances are meters on the WGS84 sphere/spheroid. Implementation
choice is the agent's, but it must be verified: a unit test compares the
engine's point-to-point distance with a haversine reference for a few
pairs at Minnesota latitudes to within 0.2%, and a point-to-linestring
case is asserted geometrically. Acceptable approaches: DuckDB
`ST_Distance_Spheroid`/`ST_DWithin_Spheroid` if their axis order is
confirmed empirically (document it), or a local equirectangular projection
(`ST_Transform` with `+proj=eqc +lat_ts=<bbox centre lat>`) and planar
`ST_DWithin`. Whatever is chosen is stated in the module docstring.

### 3.2 `(around:r)`, `(around.set:r)`, `(around:r,lat,lon[,lat,lon…])`

Sources: the named set (default `_`) or the literal point/polyline (two or
more coordinate pairs form a LINESTRING). Source geometry per element type:
node → point; way → its LINESTRING (`geometry`, hydrated through
`render.hydrate_way_geometry`-style logic when NULL); relation → the
collection returned by `relation_geometry_table` (3.5). A candidate
matches if its distance to **any** source geometry is ≤ r. Candidate
geometry: node point; way LINESTRING (bbox-only fallback when NULL, with a
`ctx.warnings` entry); relation via 3.5.

`implied_bbox`: the union bbox of the source geometries expanded by r
(degrees via 111 320 m per degree latitude and the cosine of the bbox's
largest absolute latitude). Empty source set → empty result.

### 3.3 `(poly:"lat lon lat lon …")`

Polygon from the coordinate list (closed automatically; fewer than three
points is a parse-time error, already handled by the parser or raised as
`ParseError`). Node matches if `ST_Within(point, poly)`; way if
`ST_Intersects(linestring, poly)`; relation if any member point is within
or any member way intersects (3.5). `implied_bbox` = polygon bbox.

### 3.4 `out geom(s,w,n,e)`

Way geometry outside the bbox is clipped the way the reference does it.
Establish the exact behaviour empirically against the reference
(`https://maps.mail.ru/osm/tools/overpass/api/interpreter` with `[date:]`;
see `tools/README.md`) for both JSON and XML on a way that crosses the
bbox, mirror it (which vertices keep coordinates, whether JSON emits
`null` entries, what XML `<nd>` looks like), and write the finding into
the `render.py` docstring. Relation member geometry follows the same rule.
Nodes outside the bbox are unaffected (the bbox restricts geometry, not
membership).

### 3.5 `geofilters.relation_geometry_table(ctx, relation_rows_sql) -> str`

Public helper reused by W2 (`(area)` on relations, `is_in` on relations).
Input: a SELECT of canonical rows of type `relation`. Output: the name of a
temp table `(id BIGINT, geometry GEOMETRY)` holding, per relation, a
GEOMETRYCOLLECTION of its member node points and member way linestrings,
resolved from the spatial files of the cells covering the relation's bbox
(reuse `sources.build_way_hydrate_via_bbox_select` and
`sources.build_node_hydrate_via_bbox_select`, the same helpers `out geom`
uses). Relations with no resolvable member are absent from the table;
callers treat absence as "no match" and may fall back to the bbox test
with a warning. Add the files read to `ctx.files_read`.

### 3.6 `[out:csv(f1, f2, …; header; "sep")]`

Lift the rejection in `planner.check_settings`. Fields: tag keys, and
`::id`, `::type`, `::otype`, `::lat`, `::lon`, `::count`, `::version`,
`::timestamp`, `::changeset`, `::uid`, `::user`. `::lat`/`::lon` are the
node position, or the `center` for ways/relations when `out center` was
used, else empty. Header on by default, separator tab by default. `out
count` rows fill `::count` and leave the rest empty. Content type
`text/csv; charset=utf-8`. Values are written raw (no quoting), as
Overpass does. `tools/difftest.py` learns to grade csv corpus entries:
compare the multiset of lines after the header, order-insensitively.

### 3.7 Corpus

33 `around` from a node set (cafés within 300 m of a named station);
34 `around.set` on ways; 35 `poly`; 46 `out geom(bbox)` on highways
crossing the bbox; 47 `[out:csv(name,::id,::lat,::lon)]`.

## 4. W2: areas

### 4.1 Derivation rule (documented divergence from `areas.osm3s`)

An **area** is derived from:

* every relation with `type=multipolygon` or `type=boundary` whose member
  ways assemble into at least one valid ring, and
* every closed way with `is_area = true` (already computed by the Rust
  producer) that has at least one of the keys `name`, `ref`,
  `admin_level`, `boundary`, `place`, `postal_code`, `addr:postcode`,
  `landuse`, `natural`, `leisure`, `amenity`, `tourism`, `historic`,
  `military`, `aeroway`, `water`, `area`. Buildings without any of these
  keys are **not** areas (the planet has ~600M of them; Overpass's rules
  also exclude bare buildings). State this in `docs/overpass-ql-support.md`.

Ids follow Overpass: `way_id + 2400000000`, `relation_id + 3600000000`.

Ring assembly runs in Python with `shapely` (build-time only, never in the
engine): member ways with role `outer`, `inner` or empty are merged
(`shapely.ops.linemerge`), closed rings become polygons, inner rings are
subtracted from the outer polygons that contain them, the result is
`make_valid`-ed and stored as POLYGON/MULTIPOLYGON WKB with GeoParquet
`geo` metadata (same convention as way geometry). Unresolvable members are
ignored; a relation with no ring yields no area. Way areas use
`ST_MakePolygon` on the stored LINESTRING (≥ 4 points, first == last).

### 4.2 Layout and manifest v4

* `spatial/<gen>/area/cell=<cell>/part-0.parquet` — columns `id`,
  `pivot_type` (`'way'|'relation'`), `pivot_id`, `tags` (copy of the pivot
  tags), promoted columns, meta columns (copied), `xmin_e7..ymax_e7`,
  `geometry`, `cell`, `hilbert`; sorted by `(cell, hilbert, id)`; cell by
  the v2 loose-placement rule from the bbox. Row groups ~1 MB.
* `index/<gen>/areas.parquet` — every column except `geometry`, sorted by
  `id`, one file, row groups of 4 MB. This is the table `area[...]`
  queries scan; geometry is fetched from the spatial files only for
  matching rows.
* Manifest `manifest_version: 4` adds `areas: {"index": {"path", "rows",
  "bytes"}, "cells": {<cell>: {"path", "rows", "bytes", "bbox"}}}`
  mirroring `tables.relation`, and `stats.areas`. Readers of v1–v3 (and v4
  without `areas`) treat areas as absent: area statements produce an empty
  set and add a `ctx.warnings` entry `"areas are not available for this
  dataset"`.
* `catalog.cells_for_bbox(manifest, "area", bbox)` works like `relation`.
  `osmpq validate` checks the area files and index like other tables.

### 4.3 Commands and lifecycle

* `osmpq areas <root> [--threads] [--memory-limit] [--tmpdir]` derives the
  full table for the current generation and writes manifest n+1 (v4). It
  reads relations and their member ways from the byid/spatial files
  (deltas shadowing per M2 rules, via the same `sources.current_rows`
  helpers or the updater's `_fetch_current`), so it can run on a dataset
  that has deltas.
* `osmpq build` runs it at the end unless `--no-areas`.
* `osmpq compact` re-derives areas for touched pivots: winners of type
  way/relation (plus every relation that lists a touched way as a member,
  which the updater already put into the touched set) get their area rows
  recomputed or removed; only area cell files containing changed rows are
  rewritten, the rest are hardlinked into the new generation; the index
  file is rewritten. Deltas carry no area rows: areas lag until the next
  compaction (documented; Overpass's own area loop lags hours).

### 4.4 Engine semantics (`engine/areas.py`)

* `area[...]`, `area(id)`, `area.a[...]`: rows from the index file with
  tag/id pushdown (`tagsql`); set rows have `type='area'`, `id`, `tags`,
  meta, bbox, `cell`, `hilbert`, `geometry` NULL. Areas in a set behave in
  union/difference/`out` like any element; `out` prints `{"type":"area",
  "id":…, "tags":…}` / `<area id="…">` with no geometry; `out count`
  reports `areas`.
* `(area)`, `(area.a)`, `(area:id)` filters (registered in
  `hooks.FILTER_HOOKS`): load the geometry of the referenced areas from the
  spatial area files (cell + id) into a temp table; predicate: node
  `ST_Within(point, geometry)`; way `ST_Intersects(linestring, geometry)`
  (bbox fallback + warning when NULL); relation via
  `geofilters.relation_geometry_table` (import lazily; if the module is
  absent use the bbox test and warn). `implied_bbox` = union bbox of the
  referenced areas.
* `(pivot.a)`: `way(pivot.a)` = ways whose `id + 2400000000` is an area id
  in `a`; `rel(pivot.a)` likewise with `3600000000`; nodes never match.
  `implied_bbox` = union bbox of those areas; predicate an `IN (SELECT …)`.
* `is_in` / `is_in(lat,lon)` / `.a is_in -> .b`: output the areas that
  contain the input. Candidate area files: `cells_for_bbox(manifest,
  "area", bbox of the inputs)`; exact test: node → `ST_Within`; way →
  `ST_Intersects`; relation → 3.5 collection intersects. Output rows are
  area rows as above.
* `map_to_area`: ways/relations in the input set → the area rows with the
  corresponding ids, if they exist in the index.

### 4.5 Fixture and tests

`make_fixture.build(root, manifest_version=4)` adds: two area ways (one
named park inside leaf "000", one `landuse` polygon spanning "000"/"002"),
one multipolygon relation with an inner ring (hole) whose members are
fixture ways, one `type=boundary` relation, plus `index/areas.parquet`
and the v4 manifest. `FixtureInfo` names their ids and a point inside the
hole. Tests cover: derivation (ring assembly incl. the hole, way area
rule, the bare-building exclusion), `area[name]` → `node(area)` /
`way(area)` / `rel(area)`, `(area:id)`, `(pivot)`, `is_in` for a point in
the hole (not inside) and outside it (inside), `map_to_area`, `out count`
with areas, v3 manifest → empty + warning, and compaction re-derivation
after a fixture delta that moves a pivot way.

### 4.6 Corpus

36 `area[name="Minneapolis"]->.a; node[amenity=cafe](area.a)`;
37 `way[highway=primary](area.a)`; 38 `rel[route=bus](area.a)`;
39 `is_in` on a node; 40 `way(pivot.a)`; 49 `map_to_area`.

## 5. W3: meta filters, evaluator, `foreach`, `if`, `(if:)`

### 5.1 Meta filters (`engine/metafilters.py`)

| filter | predicate |
| --- | --- |
| `(newer:"T")` | `timestamp >= T` |
| `(changed:"A")` | `timestamp >= A` (no attic: "last edit at or after A", as Overpass without history) |
| `(changed:"A","B")` | `timestamp BETWEEN A AND B` |
| `(user:"n1","n2")` | `"user" IN (…)` |
| `(uid:1,2)` | `uid IN (…)` |

Timestamps are `YYYY-MM-DDThh:mm:ssZ`; anything else raises
`RuntimeQueryError` with an Overpass-style message. `implied_bbox` is
None. Datasets built with `raw-py` lack untagged-node metadata (M1
report); note it, nothing else to do.

### 5.2 Evaluator (`ql/evaluator.py`, `engine/evalsql.py`)

Parse the source text the parser already captures (`IfFilter.expression`,
`If.condition`) into an expression AST: literals (numbers, strings),
`t["k"]`, `is_tag("k")`, `id()`, `type()`, `version()`, `timestamp()`,
`changeset()`, `uid()`, `user()`, `count_tags()`, `count_members()`,
`count_distinct_members()`, `count_by_role("r")`, `is_closed()`,
`length()` (meters, ways only), `lat()`/`lon()` (nodes), unary `!`/`-`,
`* / + -`, `< <= > >= == !=`, `&& ||`, `?:`, parentheses, `number()`,
`is_number()`, and the set-scoped `count(nodes|ways|relations|areas|nwr)`,
`.a.count(...)`, and aggregators `u(e)`, `min(e)`, `max(e)`, `sum(e)`,
`set(e)` over the input set. Overpass typing: values are strings; a
comparison is numeric when both sides parse as numbers, else lexical;
truthiness: `""` and `"0"` are false; a missing tag is `""`.

* **Element-scoped** compilation (`evalsql.compile_element(expr, alias,
  promoted_keys) -> SQL`) for `(if:)`: every element function maps to a
  canonical column expression; `count(...)`/aggregators raise
  `UnsupportedError("set-scoped … in (if:)")`.
* **Set-scoped** evaluation (`evalsql.evaluate_set(ctx, expr, set_name)
  -> str`) for `if`: `count(...)` and aggregators run one SQL each over
  `set_<name>`; element functions outside an aggregator are an error
  (Overpass requires `u(...)` there).

### 5.3 Control flow (`engine/evalfilter.py`)

* `if (cond) { … } else { … }`: evaluate on set `_`, run the chosen block
  through `planner.execute_statement`.
* `foreach.a->.b { … }` (defaults `_`/`_`): snapshot `set_a`; for each
  row in `(type, id)` order create `set_b` as that single element and run
  the body. Elements `out` inside the body accumulate in `ctx.elements`.
  No iteration cap; the run timeout is the guard.
* `(if:)` filter registered in `hooks.FILTER_HOOKS` with the element-scoped
  predicate; `implied_bbox` None.

### 5.4 Corpus

41 `(newer:)` on ways; 42 `(user:)`; 43 `(changed:a,b)`;
44 `foreach` with `out` per element; 45 `if (count(ways) > 0)`;
48 `way[highway](if:t["lanes"] > 2)`.

## 6. W4: service hardening, image, updater on S3

### 6.1 `server.py`

Environment (defaults in parentheses):

| var | meaning |
| --- | --- |
| `OSMPQ_ROOT` | dataset root, local or `s3://` |
| `OSMPQ_SLOTS_PER_IP` (2) | concurrent queries per client IP |
| `OSMPQ_MAX_CONCURRENT` (8) | concurrent queries per process |
| `OSMPQ_MAX_TIMEOUT` (180) | `[timeout:]` is clamped to this |
| `OSMPQ_MAX_MAXSIZE` (1073741824) | `[maxsize:]` clamp |
| `OSMPQ_TRUST_PROXY` (0) | when 1, client IP is `CF-Connecting-IP`, else first `X-Forwarded-For`, else peer |
| `OSMPQ_ANNOUNCED_ENDPOINT` ("none") | shown in `/api/status` |
| `OSMPQ_MANIFEST_REFRESH_SECONDS` (60) | how often the engine re-reads `manifest/LATEST` |
| `OSMPQ_LOG_QUERIES` (0) | include query text in the request log |

Behaviour:

* A query over either limit gets HTTP 429 with the Overpass text
  (`<p>Error: runtime error: open64: 0 Success /osm3s_v0.7.62_osm_base
  Dispatcher_Client::request_read_and_idx::rate_limited. Please check
  /api/status for the quota of your IP address.</p>` shape is what overpass
  turbo looks for; keep "rate_limited" and the `/api/status` hint).
* `/api/status` (plain text) in Overpass's format: `Connected as: <ip>`,
  `Current time: …`, `Announced endpoint: …`, `Rate limit: <slots per
  ip>`, `<n> slots available now.`, then the running-queries block with
  one line per running query `<id> <maxsize> <timeout> <start>` (only the
  caller's own queries, as Overpass).
* `/api/kill_my_queries`: interrupts the caller's running queries (needs
  `Engine.run(..., cancel=CancelToken())` whose `cancel()` calls the run's
  `con.interrupt()`); returns the Overpass HTML page.
* `/healthz`: 200 JSON `{"manifest": n, "timestamp_osm_base": …}`; 503
  until the manifest loads.
* Every `/api/interpreter` response carries `X-OSMPQ-Manifest: <n>` and
  `Cache-Control: public, max-age=60`.
* Request log: one JSON line per request on stdout (`ts`, `ip`, `status`,
  `seconds`, `files_read`, `elements`, `bytes`, `query_sha256`,
  `timed_out`, `remark`, and `query` when enabled).
* **Manifest refresh**: `Engine` re-reads `manifest/LATEST` at most every
  `OSMPQ_MANIFEST_REFRESH_SECONDS` (on the next request after the interval;
  no background thread), reloads the manifest and drops the row-group
  index cache when the number changed, and swaps the reference
  atomically; a run in flight keeps the manifest it started with
  (`run_program` captures `self.manifest` once). Test: write a new
  manifest to the fixture root, advance a monkeypatched clock, assert the
  next query sees the new `timestamp_osm_base`.
* `osmpq serve [--host 0.0.0.0] [--port 8080] [--workers 1]` runs uvicorn.

### 6.2 Object store (`store.py`) and the updater on `s3://`

`Store` interface: `read_bytes(rel)`, `write_bytes(rel, data)`,
`exists(rel)`, `list(prefix) -> list[str]`, `delete(rel)`,
`upload_file(local_path, rel)`, `url(rel) -> str` (the string DuckDB
reads: local path or `s3://…`). Implementations: `LocalStore(root_dir)`,
`S3Store(bucket, prefix, boto3 client built from the same `OSMPQ_S3_*`
variables the engine uses, `endpoint_url = https://<OSMPQ_S3_ENDPOINT>`)`,
and `MemoryStore` for tests. `store.for_root(root) -> Store`.

`osmpq update` on an `s3://` root: manifest and tier/byid/index files are
read by DuckDB through `store.url(...)` (the `httpfs` secret is already
set up by `Engine`-equivalent code; factor `_s3_secret_sql` so the updater
can issue it too), tier files are written to the local tmpdir and uploaded
with `upload_file`, the manifest JSON then `LATEST` are written with
`write_bytes` (`LATEST` last). `Path` arithmetic on `root` in `updater.py`
goes through the store. `write_manifest`/`load_latest` in
`layout/manifest.py` accept either root kind. Compaction and gc stay local
in M3 (documented in the runbook: compact on the big node against a local
mirror, then sync). Tests: `LocalStore` and `MemoryStore` through the full
updater e2e (existing `tests/test_update_e2e.py` fixtures) and `S3Store`
methods through `botocore.stub.Stubber`.

### 6.3 `osmpq updater-server [--port 8081]` (`update/server.py`)

FastAPI app: `POST /run` executes one `run_once` (options from
`OSMPQ_ROOT`, `OSMPQ_REPLICATION_SOURCE`, `OSMPQ_UPDATE_MAX_DIFFS` (60),
`OSMPQ_UPDATE_TMPDIR`, threads/memory from env) and returns the
`RunSummary` as JSON; a second `/run` while one is in flight returns 409;
`GET /status` returns `{"running": bool, "last": <summary or null>,
"manifest": n, "timestamp_osm_base": …}`; `GET /healthz`. Errors from a
run return 500 with the message and are logged; the lock is always
released.

### 6.4 Image

`Dockerfile` at the repo root: `python:3.12-slim`, `pip install .`
(no Rust; the image serves and updates, it does not build planets), DuckDB
extensions `spatial` and `httpfs` installed at image build time so no
network `INSTALL` happens at runtime (verify `LOAD` works with network
disabled in a test step of the Dockerfile: `python -c "import duckdb;
duckdb.connect().execute('LOAD spatial; LOAD httpfs')"`), default `CMD
["osmpq","serve","--host","0.0.0.0","--port","8080"]`; the updater runs the
same image with `osmpq updater-server --port 8081`. `.dockerignore`
excludes `data/`, `rust/osmpq-raw/target/`, `tests/corpus/.cache/`. If
`docker` is unavailable in the sandbox, say so in the report; the
Dockerfile's steps must still be individually exercised.

## 7. W5: Cloudflare deployment (`deploy/cloudflare/`)

TypeScript, `wrangler` ≥ 4, `@cloudflare/containers`. Files:
`package.json`, `tsconfig.json`, `wrangler.jsonc`, `src/index.ts`,
`src/engine.ts`, `src/updater.ts`, `README.md`. `npx tsc --noEmit` must
pass; a vitest suite is welcome if `@cloudflare/vitest-pool-workers` can be
installed in the sandbox, otherwise the Worker logic (cache key,
rate-limit decision, routing) is factored into pure functions with plain
vitest tests.

* **Worker** (`src/index.ts`): `/api/interpreter` GET/POST: rate-limit by
  client IP through the Workers rate-limiting binding (`RATE_LIMITER`,
  e.g. 30 requests/60 s; 429 with the Overpass text on exceed), normalize
  the query (trim), look up the Cache API with key
  `https://osmpq.cache/interpreter/<sha256(query)>`, forward to the engine
  container (`getRandom(env.ENGINE, N)` over `ENGINE_INSTANCES` instances),
  store the response with its `Cache-Control` (60 s). `/api/status`,
  `/api/timestamp`, `/healthz` pass through. `/admin/scheduler/{start,
  stop,status,run}` guarded by the `ADMIN_TOKEN` secret control the
  updater scheduler. Everything else 404. Add the ODbL attribution header
  `X-Attribution: © OpenStreetMap contributors, ODbL`.
* **`EngineContainer`** (`src/engine.ts`): `extends Container`,
  `defaultPort = 8080`, `sleepAfter = "2m"`, `envVars` from the Worker's
  vars/secrets (`OSMPQ_ROOT`, `OSMPQ_S3_*`, `OSMPQ_TRUST_PROXY=1`,
  `OSMPQ_ANNOUNCED_ENDPOINT`).
* **`UpdaterContainer`** (`src/updater.ts`): `defaultPort = 8081`,
  `sleepAfter = "10m"`, same env plus `OSMPQ_REPLICATION_SOURCE`.
* **`UpdaterScheduler`** Durable Object (`src/updater.ts`): `alarm()`
  fetches `POST /run` on the single updater container instance
  (`getContainer(env.UPDATER, "updater")`), records the summary and
  timestamps in storage, and re-arms the alarm 60 s after the run started
  (never overlapping: the DO is single-threaded and the container answers
  409 while busy, so a slow run just delays the next). `fetch()` handles
  `start` (arm the first alarm), `stop` (delete the alarm), `run`
  (immediate), `status` (last summaries, next alarm).
* `wrangler.jsonc`: containers config for both classes pointing at the
  repo-root `Dockerfile` (`"image": "../../Dockerfile"`),
  `instance_type: "standard-3"`, `max_instances`, durable object bindings
  with `new_sqlite_classes` migrations, the rate-limit binding, `vars`,
  and the secrets list in the README (`OSMPQ_S3_KEY_ID`, `OSMPQ_S3_SECRET`,
  `OSMPQ_S3_ENDPOINT`, `ADMIN_TOKEN`).
* `docs/m3-runbook.md`: from zero to a running public endpoint: R2 bucket
  and API token, sync a built root (`docs/m1-runbook.md`), `wrangler
  deploy`, set secrets, start the scheduler, verify `/api/status` and a
  query from overpass turbo (custom server URL), watch cost; how to run
  compaction (pause the scheduler, compact on the big node against a local
  mirror, sync, resume) and what to do when a run fails.

## 8. Validation targets on Minnesota (report table)

Same shape as M2: fill each row in `docs/m3-report.md` with the number
observed, not a target.

| item | target |
| --- | --- |
| unit tests | all pass, including every M0–M2 test |
| `osmpq areas` on Minnesota | wall clock, area count, bytes of spatial + index |
| harness at the dataset timestamp | every new corpus entry gradable and passing, or the difference documented per entry |
| `is_in` at a Minneapolis point, cold | seconds, files read |
| `node(around:500)` on a 50-node set, cold | seconds, files read |
| `area[name="Minneapolis"]->.a; way[highway](area.a)` cold | seconds, files read |
| server limits | 429 on the third concurrent query from one IP (test), `/api/status` shows the slot; `kill_my_queries` interrupts a long query |
| manifest refresh | a query after `osmpq update --once` sees the new timestamp without restart |
| updater on `s3://` | one `run_once` against a MemoryStore/LocalStore root through the store interface in tests; real R2 left to the runbook |
| image | `docker build` if available; extension `LOAD` without network verified |
| deployment code | `tsc --noEmit` clean; pure-function tests pass |
