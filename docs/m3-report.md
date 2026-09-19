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
| unit tests | (filled after the areas redesign) |
| `osmpq areas` on Minnesota (relation areas + way index) | (filled after the areas redesign) |
| harness at the dataset timestamp | (filled after the areas redesign; the pre-redesign run is in section 3) |
| `is_in` at a downtown Minneapolis point, cold | (filled after the areas redesign) |
| `node(around:500)` from 50 cafés, cold | 0.11 s, 6 files, 110 elements |
| `area[name="Minneapolis"]->.a; way[highway=primary](area.a)` cold | (filled after the areas redesign) |
| server limits | third concurrent query from one IP: HTTP 429 in 0.08 s while the first two ran (35 s and 54 s); `/api/status` showed `Rate limit: 2`, `0 slots available now.` and both running queries as `<id> <maxsize> <timeout> <start>`; `kill_my_queries` interrupts a running query (tested in `tests/test_server_limits.py`) |
| manifest refresh | `osmpq updater-server` applied 5 diffs per `POST /run` on a copy of the M2 dataset (manifest 25 → 26 → 27, timestamp 17:00:36Z → 17:05:41Z); a running `osmpq serve` with a 5 s refresh interval reported the new timestamp on `/api/timestamp`, `/healthz` and `X-OSMPQ-Manifest` without restart |
| updater on `s3://` | full `run_once` through `LocalStore` on a fixture root and the S3 write path through `botocore.stub.Stubber` (tests); the real bucket is left to the runbook |
| image | the docker daemon is unavailable in the sandbox, so `docker build` was not run; each step was exercised by hand: `pip install .` into a clean venv gives a working CLI, extensions install into the image's HOME at build time, and `LOAD spatial; LOAD httpfs` succeeds with the network disabled |
| deployment code | `tsc --noEmit` clean; 30 pure-function vitest tests; 4 Workers-runtime tests (Miniflare) for routing, admin auth and the scheduler round trip; container-backed routes cannot run without Docker |

## 3. Harness before the areas redesign

Full corpus (80 query × bbox rows) against the cached reference answers,
served from the base dataset with the first areas implementation:
67 of 80 rows pass. The 13 non-passing rows:

| rows | cause |
| --- | --- |
| 06 `natural=water` at `duluth_harbor` | pre-existing: the reference returns Lake Superior nodes outside the extract |
| 26, 28 | reference unavailable (dispatcher timeout / OOM), as in M0–M2 |
| 37 `way[highway=primary](area.a)` | 3 extra bridges: `ST_Intersects` instead of any-vertex-inside (fixed by the redesign) |
| 39 `is_in` | reference returns closed ways as `way` elements, we returned `area` ids; 7 relation areas (United States, timezones, CONUS, …) extend beyond the extract (documented) |
| 40 `way(pivot.a)` | the corpus query lacked a bbox and the reference is worldwide; fixed the query, now passes |
| 43 `(changed:"a","b")` | 19 vs 16 ways: attic-only difference (documented) |

Every other tier-2 entry (33–36, 38, 41, 42, 44–49) passes.
