# M3 report: public beta

Contract: `docs/m3-contracts.md`. Dataset: the Minnesota extract built by
the Rust producer (`minnesota-rs3`, timestamp `2026-09-19T00:21:52Z`,
manifest 1) with areas derived on a hardlinked copy; the M2 updated copy
(`minnesota-upd-c2`, deltas through `2026-09-19T16:55:36Z`) for the
updater/refresh checks. Reference: `maps.mail.ru` (Overpass 0.7.62) with
`[date:]` pinned to the dataset timestamp.

## 1. What was built

| workstream | delivered |
| --- | --- |
| W1 geometry | `(around:r)`, `(around.set:r)`, `(around:r,lat,lon,…)`, `(poly:…)` as planner hooks with implied bboxes; distances via a local equirectangular projection (verified within 0.2% of haversine); `out geom(s,w,n,e)` clipping matched to the reference; `[out:csv(...)]` incl. the reference's `@id` header spelling; csv grading in `tools/difftest.py` |
| W2 areas | relation areas materialized per the reference's `areas.osm3s` rules, closed ways as areas without storage (section 9 of the contract, after probing the reference); `area[...]`, `(area)`, `(pivot)`, `is_in`, `map_to_area`; `osmpq areas`, `build` folds it into the manifest (v4), compaction re-derives; fixture v4 |
| W3 evaluators | `(newer)`, `(changed)`, `(user)`, `(uid)`; expression parser + SQL compiler for `(if:)`; set-scoped evaluation for `if`; `foreach` |
| W4 service | per-IP and global slots with the Overpass 429 text, `/api/status` in Overpass format, `/api/kill_my_queries`, `/healthz`, `X-OSMPQ-Manifest`, JSON request log, manifest refresh without restart, `osmpq serve`; object store abstraction (local, S3, memory) and the updater on an `s3://` root; `osmpq updater-server` (`POST /run`); `Dockerfile` with baked DuckDB extensions |
| W5 deployment | `deploy/cloudflare`: Worker (rate-limit binding, Cache API, routing), `EngineContainer`, `UpdaterContainer`, `UpdaterScheduler` Durable Object with the alarm loop; `docs/m3-runbook.md` |
| coordinator | planner extension points (`engine/hooks.py`), merges, the areas amendment, `out count` `areas` tag semantics, gc for area files |

Findings that changed the design during the milestone:

- **Areas.** The first implementation followed the contract's guessed rule
  (stored polygons for tagged closed ways: 1.02M areas, 354 MB for
  Minnesota, 350k of them buildings with a postcode). Probing the
  reference showed that every closed way is an area there, printed as the
  way itself, and that `area(2400000000+id)` does not resolve; relation
  areas follow `areas.osm3s` exactly. The redesign (contract section 9)
  stores relation areas only and derives way polygons on demand.
- **`way(area)` semantics.** A way is selected when at least one vertex is
  strictly inside the polygon; ways crossing the boundary with no inside
  vertex are not (verified on ten partially-inside primary ways). The
  first implementation used `ST_Intersects` and returned three river
  bridges too many.
- **Area ids collide.** `way_id + 2400000000` can equal `relation_id +
  3600000000` for way ids above 1.2 billion; joining by area id
  cross-multiplied 58 pairs on Minnesota. Found by `osmpq validate`.
- **`ST_Length_Spheroid` / `ST_Distance_Spheroid`** in DuckDB 1.5.5 take
  (lat, lon) order and the spheroid distance only accepts points; the
  engine flips coordinates for lengths and projects locally for `around`.
- **Untracked dependency.** `osmium` was imported by the updater but not
  declared, so a clean `pip install .` (the image) produced a CLI that
  crashed on import. Fixed with the Dockerfile work.

## 2. Validation on Minnesota

| item | result |
| --- | --- |
| unit tests | 563 passed (M0–M3), `python3 -m pytest -q`, 66 s |
| `osmpq areas` on Minnesota | 136 s wall clock; 9,919 relation areas in 150 cells, 43.2 MB of polygon files + 0.67 MB index; way-area index 93,089 closed ways, 6.1 MB; `osmpq validate` clean. (The first, wrong rule had produced 1,021,803 stored areas in 354 MB in 462 s.) |
| harness at the dataset timestamp | 69 of 80 query × bbox rows pass; all 11 others are explained in section 3. Of the 17 new tier-2 entries, 15 pass and 2 differ for documented reasons (39, 43) |
| `is_in(44.9778,-93.2650)` cold | 0.47 s, 10 files, 6 areas (Minneapolis, Minnesota, Hennepin County, Metropolitan Council, Central, Downtown West) |
| `node(id:…); is_in` cold | 0.51 s, 10 files, 9 elements: the 2 closed ways containing the node printed as ways, 7 relation areas |
| `node(around:500)` from 50 cafés, cold | 0.07 s, 2 files, 110 elements |
| `area[name="Minneapolis"]->.a; node[amenity=cafe](area.a)` cold | 0.10 s, 9 files, 186 elements (reference: 186) |
| `area[name="Minneapolis"]->.a; way[highway=primary](area.a)` cold | 0.24 s, 15 files, 652 elements (reference: 652; the three river bridges with only boundary vertices are correctly excluded) |
| `area[name="Minneapolis"]->.a; rel[route=bus](area.a)` cold | 1.95 s, 128 files, 236 elements (reference: 236). Member geometry is resolved for the 236 tag-filtered candidates only; resolving it for every relation in the area's cells, as the first version did, exceeded the default 512 MB `[maxsize:]` |
| server limits | third concurrent query from one IP: HTTP 429 in 0.08 s while the first two ran (35 s and 54 s); `/api/status` showed `Rate limit: 2`, `0 slots available now.` and both running queries as `<id> <maxsize> <timeout> <start>`; `kill_my_queries` interrupts a running query (tested in `tests/test_server_limits.py`) |
| manifest refresh | `osmpq updater-server` applied 5 diffs per `POST /run` on a copy of the M2 dataset (manifest 25 → 26 → 27, timestamp 17:00:36Z → 17:05:41Z); a running `osmpq serve` with a 5 s refresh interval reported the new timestamp on `/api/timestamp`, `/healthz` and `X-OSMPQ-Manifest` without restart |
| updater on `s3://` | full `run_once` through `LocalStore` on a fixture root and the S3 write path through `botocore.stub.Stubber` (tests); the real bucket is left to the runbook |
| image | the docker daemon is unavailable in the sandbox, so `docker build` was not run; each step was exercised by hand: `pip install .` into a clean venv gives a working CLI, extensions install into the image's HOME at build time, and `LOAD spatial; LOAD httpfs` succeeds with the network disabled |
| deployment code | `tsc --noEmit` clean; 30 pure-function vitest tests; 4 Workers-runtime tests (Miniflare) for routing, admin auth and the scheduler round trip; container-backed routes cannot run without Docker |

## 3. Harness rows that do not pass

Full corpus (80 query × bbox rows) against the cached reference answers,
served from the base dataset with the final areas implementation:

| rows | cause |
| --- | --- |
| 04 `highway=residential` (6 bboxes), 26, 28 | reference unavailable (out of memory / dispatcher timeout on the mirror), as in M0–M2; ungradable |
| 06 `natural=water` at `duluth_harbor` | pre-existing: the reference returns Lake Superior nodes outside the extract |
| 39 `is_in` | 5 relation areas missing (United States, America/Chicago timezone, Contiguous United States, UTC−06:00, Minneapolis–Saint Paul): their rings leave the Minnesota extract, so they cannot be assembled; everything else (2 closed ways as `way` elements, 7 relation areas) matches |
| 43 `(changed:"a","b")` | 19 vs 16 ways: attic-only difference (elements edited again after `b`, or changed through node edits) |

Rows fixed during the milestone: 37 (bridges, any-vertex rule), 40 (the
corpus query needed a bbox because the reference is worldwide), the
`out count` `areas` tag on 41/42/45/48.

## 4. Follow-ups

- **`area[...]` by tag without a bbox at planet scale.** The way-area
  index (closed ways with `name`/`ref`/`admin_level`/`boundary`/`place`)
  is 6 MB for Minnesota; the planet's would be around a gigabyte, too much
  to scan per query. Sort a copy by `name` (row-group pruning makes
  `area[name=X]` a one-row-group read) or keep only admin/place ways.
- **`(changed:a,b)`** needs attic data (M4) to match the reference.
- **Relation areas at an extract's edge** are absent; the planet build
  does not have this problem.
- **`osmpq manifest`** does not print the way-area index yet.
- Compaction rewrites the way-area index in full (6 MB here; fine until
  the planet, where it joins the byid-rewrite concern from M2).

## 5. Addendum (2026-09-20): relation-area assembly at planet scale

`osmpq areas`'s 136 s on Minnesota (9,919 relation areas, section 2) did
not predict its cost at scale: benchmarking against Geofabrik's
`us-midwest` extract (54,376 relation areas, 5.5x Minnesota's count) took
**6,518 s (108.6 min) single-threaded** -- 47.9x Minnesota's time for only
5.5x the areas, i.e. wildly super-linear, not the roughly-linear cost the
Minnesota number alone suggested. Two distinct causes, both in
`src/osmpq/build/areas.py`, both fixed:

1. **No parallelism.** `_relation_area_rows`'s per-relation ring assembly
   (`shapely`: `linemerge`, `.difference()`, `unary_union`, `make_valid`)
   ran in a single-threaded Python loop that ignored `--threads` entirely
   (that flag only ever controlled DuckDB's own SQL phases elsewhere in
   the module) -- 99.7% of the whole build pass's wall time on
   `us-midwest`. Fixed by fanning the loop out across a
   `ProcessPoolExecutor` sized from `--threads`, with `way_wkt` (the
   dict every candidate relation's member-way geometry is read from, up
   to ~255 MB pickled at `us-midwest` scale) sent to each worker exactly
   once via the executor's `initializer` rather than per task.
2. **Pathological cost skew, and a chunking bug that made it worse.**
   Relation cost is not just variable, it is power-law skewed: one
   relation, Lake Huron (id 1205151, a multipolygon with 524 outer-role
   and 14,228 inner-role members -- 14,105 separate islands after
   `linemerge`), took **27+ minutes on its own**, single-threaded, and
   was still running when killed for inspection. The parallel fix's first
   cut used a chunksize of ~200 relations per pool task to amortize IPC;
   with cost this skewed, `ProcessPoolExecutor.map` can only rebalance
   *between* chunks, so Lake Huron and one other large relation landed in
   different chunks assigned to only 2 of 14 workers, which then ran for
   the whole job while the other 12 sat idle (~1.15x wall-clock
   improvement for a 14-core machine -- confirmed by process inspection
   mid-run, not just inferred). Switching to `chunksize=1` fixed the
   distribution (all 14 cores stayed busy) but only dropped total time to
   89.2 min, because Lake Huron's own single-relation cost was still the
   floor. The real fix was algorithmic: `_assemble_relation_geometry`
   subtracted inner rings from an outer ring one `.difference()` call at
   a time in a loop, so each of Lake Huron's ~14,105 intersecting islands
   made the next call operate on an increasingly complex polygon (more
   accumulated cutouts). Since set difference distributes over union
   (`A - (B1 ∪ B2 ∪ ...) == A - B1 - B2 - ...`), unioning all of an outer
   ring's intersecting holes once and subtracting in a single call
   (`_subtract_holes`, falling back to the old one-at-a-time loop only if
   the batched call itself raises, to keep the same per-hole exception
   tolerance) took Lake Huron from >27 min to **6.9 s** -- confirmed
   identical output via a 200-trial randomized equivalence test between
   the two algorithms (`tests/test_areas_parallel.py`) plus the full
   `us-midwest` run producing the same 54,376/684,202 row counts as every
   earlier attempt.

**Combined result**: `us-midwest`'s `osmpq areas` went from 6,518 s to
**86.5 s** -- a 75x speedup, with identical output at every step
(relation/way-area row counts, and a WKB hash comparison on the parallel
vs. serial path for a real 3,000-relation subset). Linearly extrapolated
to the planet's ~14M relations (36.5x `us-midwest`'s count), the original
algorithm implied 60-70+ *hours* for this one build step alone -- by far
the dominant risk to `docs/m1-runbook.md`'s planet-build estimate; the
fixed algorithm implies roughly 50-60 *minutes*, back in line with the
rest of the pipeline. Not yet measured at actual planet scale, and a
planet-scale run could still contain a relation more extreme than Lake
Huron (a real coastline, Antarctica, a very large country boundary) that
takes meaningfully longer than 6.9 s on its own -- but the fix addresses
the root cause (an algorithm whose cost grew with hole count in an
increasingly expensive way) rather than papering over one specific
relation, so it should generalize.
