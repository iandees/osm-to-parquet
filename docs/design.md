# Design: Overpass QL over object storage

Status: proposal, September 2026. Nothing here is implemented yet.

## 1. Goal and constraints

Serve Overpass QL queries for the whole planet, current to within about a
minute, from data that lives on a blob store (Cloudflare R2 or S3), with:

- **No large fast local disk on the query path.** Query workers may have a
  local cache, but must be able to start from nothing and serve correctly.
- **Cheap to replicate.** Adding a read worker must cost a small VM, not a
  copy of a 600+ GB database.
- **Overpass compatibility first.** Existing clients (overpass turbo, JOSM,
  Overpass Ultra, scripts) should work unchanged for the common subset of the
  language. We implement Overpass QL, not a new language.
- **Full history ("attic") is a later phase**, but the design must not paint
  us into a corner.

What makes Overpass expensive today: its storage engine assumes random access
to hundreds of gigabytes of block files (quadtile-indexed node/way/relation
data plus id indexes), and every instance carries the whole thing. Object
storage has the opposite profile: ~20-100 ms first-byte latency per request,
very high aggregate throughput, immutable objects, near-zero cost per GB, and
on R2 no egress charge. So the design has to turn "many small random reads"
into "few, large, well-targeted range reads", and turn "in-place updates"
into "append small deltas, compact periodically".

Sizes we are designing for (September 2026): planet PBF 88 GB, history PBF
150 GB; on the order of 10 billion nodes, 1.1 billion ways, 13 million
relations; a few million element changes per day arriving in minutely
`.osc` files.

## 2. Architecture overview

```
                         weekly planet PBF               minutely .osc diffs
                                |                                |
                         [ base builder ]                  [ updater ]  <- one small VM with local
                                |                                |          key-value "replication store"
                                v                                v
   +------------------------------------------------------------------------------+
   |  R2 / S3 bucket                                                              |
   |  base/<gen>/node/cell=.../*.parquet       delta/<seq>/{node,way,relation}... |
   |  base/<gen>/way/cell=.../*.parquet        manifest/<n>.json, manifest/LATEST |
   |  base/<gen>/relation/...  area/  member_index/  node_idx/  way_idx/  tag_stats/ |
   +------------------------------------------------------------------------------+
                                ^                                ^
                                | range reads (httpfs)           | polls LATEST every minute,
                                |                                | loads recent deltas locally
                     [ query worker ] x N  (stateless: Overpass QL parser -> planner -> DuckDB)
                                ^
                                | /api/interpreter  (Overpass JSON/XML/CSV, byte-compatible)
                         overpass turbo, JOSM, scripts
```

Three processes:

1. **Base builder** (batch, runs weekly or less): planet PBF to the partitioned
   Parquet layout described in section 3, plus derived tables.
2. **Updater** (always on, exactly one): applies minutely diffs to a local
   replication store, re-resolves geometry for everything a change touches,
   publishes delta Parquet files and a new manifest. Also runs compaction.
3. **Query workers** (stateless, N of them): parse Overpass QL, plan SQL,
   execute in an embedded DuckDB that reads base files over HTTP range
   requests and keeps the recent deltas in local memory/disk.

The only machine that needs fast local storage is the updater, and it needs
it as a *working set for writing* (a few hundred GB of key-value data), not
as a serving copy. This is the same decision ohsome-planet made (RocksDB
replication store) and it is what OSMExpress is built for.

## 3. Storage layout

### 3.1 Two access patterns, one clustering

Overpass queries need exactly two kinds of access:

- **Spatial**: everything of some kind inside a bbox / polygon / area / radius.
- **By id**: `node(123)`, and the recursion operators `>`, `<`, `>>`, `<<`
  that follow node references and relation memberships.

Overpass keeps two indexes (quadtile block files and id block files). On
object storage we can afford only one clustering per table, so we cluster
everything spatially and make id lookups *also* spatially scoped wherever
the data model allows it:

- A way's nodes lie inside the way's bbox. So `>` from a set of ways needs
  node lookups only within the cells covering those ways.
- A way containing a node has a bbox containing that node. So `<` from nodes
  to ways needs to scan only the ways stored in the cells around that node.
- Relations are the exception (a route relation can span a continent), so
  they get an explicit reverse membership table.
- Pure id queries (`node(123);`) use a small id-to-cell index.

### 3.2 Cells

Use a quadtree over plain lon/lat (quadkey-style, same family as Overpass's
quadtiles and Bing tiles), with **adaptive depth**: a cell is split until its
file size falls under a target (roughly 64-512 MB per file). Dense cities end
up at deep levels, oceans at shallow ones. The leaf cells that exist are
listed in the manifest, so a query computes "which leaf cells intersect my
bbox" without any directory listing.

Elements with an extent (ways, relations, areas) are placed **loosely**: an
element goes into the smallest cell that fully contains its bbox, which can
be an interior (non-leaf) cell. A bbox query therefore reads the intersecting
leaf cells *and* all their ancestors up to the root. Ancestor cells are few
(depth ~ 4-14) and mostly hold long features (coastlines, rivers, motorways,
boundaries), which is exactly what Overpass does for large objects too.
Elements whose bbox crosses the antimeridian or a root split go in the root
cell.

Within a file, rows are sorted by a space-filling curve (Hilbert or Z-order)
of the element's bbox center, then by id. Row groups are kept small-ish
(about 50k-200k rows, a few MB compressed) so that min/max statistics on the
flat `xmin/ymin/xmax/ymax` (or `lat/lon` for nodes) columns let DuckDB skip
most row groups of a file when the query bbox covers only part of the cell.
This is the standard GeoParquet "bbox covering column" trick and is the one
that reliably works for remote reads across engines; DuckDB 1.5's native
GEOMETRY statistics are a bonus on top of it.

### 3.3 Tables

All tables are Parquet, zstd, dictionary-encoded strings, Hive-style
`cell=<quadkey>` directories where partitioned.

**`node`** (partitioned by cell, then by `tagged=true|false`)

| column | type | note |
| --- | --- | --- |
| id | INT64 | |
| lat, lon | INT32 (1e-7 degrees) or DOUBLE | int32 nanodegree-ish encoding delta-compresses far better; convert on read |
| tags | MAP<VARCHAR,VARCHAR> | NULL for untagged partition |
| promoted tag columns | VARCHAR | see 3.4 |
| version, changeset, timestamp, uid, user | INT32, INT64, TIMESTAMP, INT32, VARCHAR | Overpass `meta` |
| hilbert | UINT64 | sort key; also the `qt` order for output |

Roughly 96-97% of nodes are untagged way vertices. Putting them in a separate
partition means `node[amenity=cafe](bbox)` scans only the tagged partition,
while `>` / `out skel` can still fetch untagged nodes by (cell, id).

**`way`** (partitioned by loose cell)

| column | type | note |
| --- | --- | --- |
| id | INT64 | |
| refs | LIST<INT64> | node ids in order; required for exact `out body`/`skel` output and for `>` |
| tags, promoted tag columns, meta | | as for node |
| xmin, ymin, xmax, ymax | | flat bbox for pruning |
| geometry | GEOMETRY (Parquet native) | LINESTRING of the node coordinates in ref order (closed if first==last) |
| is_closed, is_area | BOOLEAN | area per Overpass/osm conventions (closed + not `area=no`, or tag-based); lets `area`/`is_in`/`around` build polygons on demand |
| centroid_lat, centroid_lon | | for `out center` without reading geometry |
| hilbert | UINT64 | |

Because the way row carries its coordinates, `out geom`, `out center`,
`out bb`, `around`, `area` and `poly` filters never join to `node`. Only
`out body/meta` *after* `>` (which returns node objects) needs node rows, and
those are looked up by (cell, id).

**`relation`** (partitioned by loose cell; most land in shallow cells)

| column | type | note |
| --- | --- | --- |
| id | INT64 | |
| members | LIST<STRUCT<type: UINT8, ref: INT64, role: VARCHAR>> | |
| tags, promoted tag columns, meta | | |
| xmin..ymax | | bbox over resolved members (NULL if nothing resolvable) |
| geometry | GEOMETRY | MULTIPOLYGON for multipolygon/boundary relations, GEOMETRYCOLLECTION otherwise, NULL if unresolvable |
| centroid_lat, centroid_lon | | |

**`member_index`** (sorted by member_type, member_id; not spatially partitioned; ~150M rows)

`(member_type, member_id, parent_id, role, parent_cell)`. Answers node→relation,
way→relation, relation→relation for `<`, `<<` and the `(bn|bw|br)` filters.
node→way is *not* materialized initially (it would be ~11 billion rows);
instead `<` from nodes scans ways in the node's cell and ancestors with
`list_contains(refs, id)`. If that proves too slow for dense cells, add a
`node_way_index` table later.

**`node_idx`, `way_idx`** (sorted by id; `(id, cell)`)

Id-to-cell index for `node(123)`-style queries and for id-set inputs. Node
ids are nearly monotonic in creation time, so delta encoding makes this
about 3-4 bytes per row (tens of GB for nodes, a few GB for ways). A lookup
for a sorted batch of ids touches one row group per id-range. Relations do
not need one (13M rows, fits in one small table).

**`area`** (partitioned by loose cell)

Derived exactly per Overpass's `areas.osm3s` rules (multipolygon/boundary
relations with `name`, relations with `admin_level`+`name`, relations with
`postal_code` / `addr:postcode`, ways with `area=yes`+`name`, ...), with the
Overpass id convention (`way_id + 2400000000`, `relation_id + 3600000000`),
polygon geometry, copied tags, and the `pivot` (source type, id). Regenerated
for touched pivots at every compaction; Overpass itself regenerates areas in
a background loop that takes 4-12 hours per pass, so hourly/daily is no worse.

**`tag_stats`** (small): `(key, value, cell, count)` for every key/value pair
with fewer than some threshold of occurrences (say 100k). Global queries
without a bbox on a rare tag (`nwr["amenity"="nuclear_explosion_site"];`) go
straight to the cells that contain matches instead of scanning the planet.
Common tags without a bbox are refused or capped, as Overpass effectively
does through timeouts.

**`changeset`** (later; for `user:`/`uid:` filters we only need what is on
the element rows, but attic/adiff output wants changeset metadata).

### 3.4 Tags: map column plus promoted columns

`tags` is a `MAP<VARCHAR,VARCHAR>`. DuckDB evaluates `tags['amenity'] = 'cafe'`
fine but cannot push it into the Parquet reader, so a tag-only filter reads
every tag page in the pruned cells. To get real pruning, the writer **promotes
the most-queried keys into their own nullable VARCHAR columns** (order of 30-50
keys: `amenity, shop, highway, building, name, natural, landuse, leisure,
railway, power, waterway, place, boundary, admin_level, type, tourism,
office, craft, public_transport, route, addr:street, addr:postcode,
addr:city, ...`). Those columns get dictionary encoding, min/max statistics,
and Parquet bloom filters, which DuckDB reads for equality predicates. The
planner rewrites `["amenity"="cafe"]` to `amenity = 'cafe'` when the key is
promoted and to `tags['amenity'] = 'cafe'` otherwise. Regex filters always go
through the map. A `tag_keys LIST<VARCHAR>` column (or a `has_<key>` bitmap)
serves the existence filter `["name"]`.

The promoted list is part of the schema version in the manifest and can
change between base generations without breaking readers, since the planner
reads it from the manifest.

### 3.5 Manifest and table format

Readers must see a consistent snapshot: which base generation, which delta
files, up to which replication sequence. Start with a **hand-rolled manifest**:

- `manifest/<n>.json`: schema version, promoted-key list, base generation
  path, leaf-cell list per table (with per-cell file sizes and bboxes), list
  of delta files (sequence range each), latest applied replication sequence
  and its timestamp (this becomes `timestamp_osm_base` in responses).
- `manifest/LATEST`: the number of the newest manifest. Written last; objects
  are immutable, so a reader that has loaded manifest *n* stays consistent.
- Old base generations are kept until no manifest references them.

This is a few hundred lines and keeps the bespoke placement rules (loose
cells, id index) explicit. Two table formats could replace it later:

- **DuckLake** (metadata in a SQL DB, Parquet on the bucket, cheap snapshots,
  time travel, `ducklake_table_changes`). Attractive for phase 4 (attic)
  because time travel is built in, and for compaction tooling. Costs a
  catalog database that every worker must reach.
- **Iceberg via R2 Data Catalog** (managed REST catalog in the bucket; DuckDB,
  PyIceberg, Spark can read; R2 SQL can query it serverlessly). Attractive if
  we want other engines to read the lake directly. Iceberg's write path from
  DuckDB is younger and partition transforms are less flexible than our
  loose-cell scheme.

Either can be adopted per table without changing the query planner, because
the planner only asks the manifest layer "which files hold table T for cells
C and which deltas apply".

## 4. Update pipeline

### 4.1 Replication store

The updater keeps, on local NVMe (budget about 300 GB, growing slowly):

- latest version of every node (id → lat, lon, tags-or-null, meta),
- every way (id → refs, tags, meta) and relation (id → members, tags, meta),
- reverse indexes: node→ways, node→relations, way→relations, relation→relations,
- the cell each element currently lives in (so a delta can say "supersedes
  row in cell X" and compaction knows which files to rewrite).

Candidates: RocksDB (what ohsome-planet uses), LMDB via OSMExpress (already
implements all of the above plus `.osc` application and S2 indexing, ~1,500
lines of C++), or a purpose-built store in the project language. Recommendation:
prototype with OSMExpress to avoid writing the store, then decide whether to
replace it.

### 4.2 Per-minute cycle

1. Fetch the next `.osc.gz` by replication sequence (pyosmium-style state
   handling, retry, gap detection).
2. Apply to the store; collect **touched elements**: every created/modified/
   deleted node, way, relation, plus every parent way of a moved/deleted node
   and every parent relation of a touched node/way/relation (transitively for
   nested relations). This is the step QLever's `osm-live-updates` and
   ohsome-planet also perform; Freiburg reports under 7 s per minute for the
   whole planet.
3. Re-resolve geometry, bbox, centroid, `is_area` and cell for touched ways
   and relations. Re-derive area rows for touched pivots.
4. Write `delta/<seq>/{node,way,relation,area,member_index,node_idx,way_idx}.parquet`
   containing one row per touched element in its **new** state, with a
   `deleted BOOLEAN` column for deletions, and `prev_cell` so readers and
   compactors can shadow the old row wherever it was.
5. Write manifest *n+1* and update `LATEST`.

Target latency: under 60 seconds behind planet.openstreetmap.org, same as a
healthy Overpass instance.

### 4.3 Reading base ⊕ deltas

A query worker loads every delta since the current base generation into a
local DuckDB (memory or local disk). Change volume is a few million elements
per day, so a week of deltas is a few GB at most, and the worker refreshes
incrementally as new manifests appear. The read path for a table becomes:

```sql
-- pruned remote scan of the base
SELECT ... FROM read_parquet(<base files for cells>) b
WHERE <bbox pruning> AND <predicates>
  AND NOT EXISTS (SELECT 1 FROM delta_way d WHERE d.id = b.id)   -- shadowed
UNION ALL
SELECT ... FROM delta_way d WHERE NOT d.deleted AND <same predicates>
```

The anti-join is against a small local table, so it is cheap. Deleted
elements are just shadowed rows with `deleted = true`.

### 4.4 Compaction

- Hourly: merge the last 60 minute-deltas into one (last version wins).
- Daily: merge hourlies into a daily delta.
- Periodically (weekly, or when accumulated deltas exceed a few percent of
  base): rewrite the touched cell files of the base into a new generation and
  drop the folded deltas. R2 has no egress fee and cheap writes, so rewriting
  is a compute cost only. Cells that received no changes are hard-linked by
  reference in the new manifest, not rewritten.
- Areas and `tag_stats` are recomputed for touched cells during the daily
  compaction.

All of this runs on the updater VM; none of it blocks readers because
manifests are immutable and swapped atomically.

### 4.5 Attic / history (later phase)

Keep an append-only `history/` dataset with the ohsome-planet-style
`valid_from / valid_to` columns per element version (every delta row is also
appended there with `valid_from = timestamp`; the previous version gets its
`valid_to` filled at compaction). `[date:...]`, `retro`, `timeline`, `diff`
and `adiff` then become "same query, different validity predicate". The
initial history load comes from the history PBF. If DuckLake is adopted for
this dataset, its snapshot time travel covers most of the mechanism.

## 5. Query engine

### 5.1 Pipeline

```
Overpass QL text -> lexer/parser -> AST -> semantic pass (set names, types, settings)
   -> planner (AST statement -> SQL over manifest-resolved files, with set temp tables)
   -> DuckDB execution (one connection per query, memory/timeout limits)
   -> Overpass-compatible serializer (JSON / XML / CSV / popup ... , out modes)
```

Overpass semantics are set-based: each statement reads the default set `_`
or a named set and writes a set. We model a set as a temporary table
`set_<name>(type, id, cell, payload...)` where payload is the full row
(tags, geometry/refs/members, meta) so `out` never re-fetches. Sets are
small in practice; if a set grows past a threshold we spill to a DuckDB
table on local disk.

### 5.2 Statement translation sketches

| Overpass | Plan |
| --- | --- |
| `[bbox:s,w,n,e]` / `(s,w,n,e)` filter | Compute leaf cells + ancestors intersecting the bbox from the manifest; `read_parquet` of exactly those files with `xmax >= w AND xmin <= e AND ...` (nodes: `lat/lon` between) so row groups prune. |
| `node["amenity"="cafe"]` | Tagged partition only; promoted column equality or `tags['k'] = 'v'`; `~` uses `regexp_matches`; `!=`, `!~`, `[k]`, `[!k]`, `~"k"~"v"` map straightforwardly; `,i` flag → case-insensitive regex. |
| `way(id:1,2,3)` / `node(123)` | Look up `way_idx` for cells, then read those cells filtered by `id IN (...)`. Ids already in a set carry their cell. |
| `(.a; .b;)` union, `(.a; - .b;)` difference | `UNION` / `EXCEPT` on `(type, id)` with payload carried along. |
| `.a;` item / `->.b` | Temp-table rename/copy. |
| `>` | Ways in set → node ids from `refs`; nodes are synthesized from way geometry + refs for skel output and looked up by (cells of the way, id) when tags/meta are needed. Relations in set → members via `members`, resolved by (parent cell hint → cell) lookups. |
| `>>` | `>` iterated over relation→relation until fixed point, then `>`. |
| `<` | Nodes → ways: scan ways in node cells + ancestors with `list_contains(refs, id)`. Nodes/ways/relations → relations: `member_index` lookups. |
| `<<` | Transitive `member_index` closure. |
| `node(w)`, `way(bn)`, `rel(bw)`, `node(r:"role")` ... | Same machinery as `>`/`<`, restricted to one type and optional role. |
| `(around:r)` / `(around.set:r)` | Bbox expanded by r meters for pruning, then `ST_DWithin_Spheroid` (or `ST_Distance_Sphere <= r`) against the set's geometries, including the linestring form for "around a way". |
| `(poly:"...")` | Bbox of polygon for pruning, `ST_Intersects(geometry, poly)`. |
| `area[...]`, `(area.a)`, `(area:3600...)` | Select from `area` table; `node(area)` = `ST_Within(point, polygon)`; `way(area)` per Overpass semantics (any point inside) = `ST_Intersects`; pruned by area bbox and cells. |
| `is_in` / `is_in(lat,lon)` | Point-in-polygon against `area` rows in the cells covering the point; returns area elements. |
| `(pivot.a)` | Area rows → their source way/relation by pivot id. |
| `(newer:"ts")`, `(changed:"a","b")`, `(user:"..")`, `(uid:..)` | Predicates on meta columns. |
| `(if: expr)` | Evaluator expression compiled to a SQL expression for the supported subset (tag access `t["k"]`, `id()`, `type()`, `count_tags()`, `length()`, arithmetic, string ops, `is_tag`, ternary). |
| `foreach`, `if`, `for` | Executed by the planner loop, one sub-plan per iteration/group; sets are DuckDB temp tables so this is just control flow. |
| `complete` | Iterate sub-plan until the set stops growing. |
| `out ids/skel/body/tags/meta/geom/bb/center/count/noids/qt/asc/N` | Serializer options; `qt` sorts by the stored Hilbert key (Overpass's quadtile order is a Z-order; we document the difference or store both), default order is nodes then ways then relations by id, matching Overpass. `out count` emits the `count` element. |
| `[timeout:n]`, `[maxsize:n]` | DuckDB `interrupt()` on a timer; `SET memory_limit`; return Overpass-style `runtime error` remarks. |
| `[out:json|xml|csv(...)|popup|custom]` | Serializer. JSON and XML first; CSV next; popup/custom last. |
| `make`, `convert`, `derived` | Planner-side: build synthetic elements from evaluator results. Phase 3. |
| `[date:]`, `retro`, `timeline`, `[diff:]`, `[adiff:]` | Phase 4 (attic). |

Compatibility is defined empirically: a corpus of real queries (overpass
turbo wizard output, the wiki examples, JOSM's built-in queries, Overpass
Ultra examples) is run against `overpass-api.de` and against us, and the
results are diffed. That harness is a first-class deliverable, not an
afterthought.

### 5.3 Engine choice

DuckDB is the pragmatic pick: embedded, reads remote Parquet with pruning,
has a built-in GEOMETRY type with statistics (1.5+), a spatial function
library, Python/Rust/Node bindings, Wasm build, and a large ecosystem
(DuckLake, Iceberg). Apache DataFusion (Rust) is the alternative if we end
up wanting to own the planner and physical operators (custom cell-aware
scans, streaming output); it lacks DuckDB's spatial maturity. We can keep
the planner engine-agnostic by emitting SQL plus a small set of "scan table T
in cells C" primitives.

## 6. Serving tier

- HTTP endpoints matching Overpass: `/api/interpreter` (GET `?data=` and POST
  form/raw), `/api/status`, `/api/timestamp`, `/api/kill_my_queries`.
  Same JSON/XML envelope (`version`, `generator`, `osm3s.timestamp_osm_base`,
  `copyright`, `remark` for errors).
- Each worker: HTTP server + a pool of DuckDB connections, the httpfs
  extension pointed at the bucket, optional `cache_httpfs` with a local disk
  cache (pure performance, evictable), and the local delta tables refreshed
  once a minute from the manifest.
- Workers are stateless: scale by count, run anywhere (a VPS next to R2, a
  container platform, an autoscaled pool). Cold start = fetch manifest +
  recent deltas (seconds to a minute).
- Per-client limits as Overpass has them (concurrent slots, timeout,
  maxsize), because a planet-wide `way["building"]` is as unbounded here as
  it is there.
- Optional later: a DuckDB-Wasm build of the same planner so small bbox
  queries run entirely in the browser against the public bucket with no
  server at all.

## 7. Cost model (rough, 2026 list prices)

| Item | Estimate |
| --- | --- |
| Base dataset on R2 (nodes ~100 GB, ways ~150-200 GB, relations, indexes, areas) | ~350-500 GB → ~$5-8/month at $0.015/GB-month; egress $0 |
| History dataset (phase 4) | another ~300-500 GB → ~$5-8/month |
| Read operations | Class B ~$0.36 per million; a typical query issues 10-200 range requests → roughly $0.01-0.07 per 1,000 queries |
| Updater VM | 8 vCPU / 32 GB RAM / 400 GB NVMe: ~$40-120/month depending on provider |
| Query workers | any 2-4 vCPU VM with some local disk for cache, ~$10-40/month each; scale horizontally |
| Compare: self-hosted Overpass | ~1 TB NVMe + 32-64 GB RAM *per replica*, plus the operational burden of a single-writer custom database |

On S3 the numbers are similar for storage but egress to workers outside AWS
costs real money, so workers should be in-region; R2's zero egress is the
stronger fit for a public service.

## 8. Risks and how to check them early

| Risk | Mitigation / experiment |
| --- | --- |
| Object storage latency makes interactive queries feel slow (hundreds of ms per round trip, many round trips per query) | Measure on a state extract first; issue file reads concurrently; keep row groups few and files few per query; `cache_httpfs` on workers; consider a tiny "hot cell" prefetch for popular regions. |
| Global tag queries without bbox scan too much | `tag_stats` cell index for rare tags; cap or refuse common ones without bbox, like Overpass's timeouts do today. |
| `<` from nodes to ways in dense cells is slow (list_contains scan) | Measure; add a materialized `node_way_index` if needed (~11B rows, sorted by node id, tens of GB). |
| Relation geometry assembly (multipolygon ring building, broken rings) | Reuse a proven assembler (libosmium via bindings, or a Rust port of its ring assembler); mark unresolvable relations with NULL geometry and still return their members. |
| Overpass output fidelity (ordering, `qt` order, float formatting, error remarks) | Differential test harness against overpass-api.de from milestone 0. |
| Minutely diffs touch a large fraction of cells daily, making compaction expensive | Compaction only rewrites touched cells; measure the touched-cell ratio on real diffs; adjust cell size. |
| DuckDB spatial functions on spheroid distances are slower than Overpass's integer math | Use bbox pre-filter; consider a planar approximation with correction for `around`. |
| Node id-to-cell index is large (tens of GB) | Only consulted for explicit id queries; row groups sorted by id so a lookup is one range read; could be replaced by a lighter structure (sorted id ranges per cell) if it matters. |

## 9. Implementation language and stack

Recommendation: **Rust** for the base builder and updater (`osmpbf`, `parquet`/`arrow`,
`object_store`, `rocksdb` or `heed`/LMDB, `geo`), and Rust for the query
service too (`duckdb` crate, `axum`). The pipeline has to chew through an
88 GB PBF and sustain minutely updates forever; that is where Rust pays off.
The parser and planner are plain compiler work in any language.

Pragmatic alternative for milestone 0: prototype the Overpass QL parser and
QL-to-SQL planner in **Python** (Lark grammar, `duckdb` package) against an
extract so that the language semantics and layout can be validated in days,
then port the planner to Rust once the SQL it emits is settled. The layout
and manifest format are language-neutral, so the two halves can be built in
different languages without rework.

Licensing: Overpass API is AGPL-3, ohsome-planet is GPL-3. We learn from
both but do not copy code, so this project can be Apache-2/MIT.

## 10. Roadmap

**M0: extract-scale prototype (2-4 weeks of effort)**
- Base builder for a Geofabrik extract (a US state or small country) into the
  layout of section 3, on a local MinIO bucket and on an R2 bucket.
- Parser + planner for the tier-1 language subset (see `overpass-ql-support.md`).
- `/api/interpreter` returning JSON/XML; run overpass turbo against it.
- Differential test harness vs overpass-api.de (bboxed to the extract).
- Publish latency and cost numbers for typical queries from a worker with a
  cold cache. This is the go/no-go gate for the whole idea.

**M1: full planet base build**
- Build the planet; record build time, file counts, sizes, cell depth
  distribution; tune cell split thresholds and row group sizes.

**M2: minutely updates**
- Updater with replication store, deltas, manifests, worker delta refresh;
  hourly/daily compaction; run for a month and measure lag and compaction cost.

**M3: public beta**
- Tier-2 language features (areas, around, poly, is_in, changed/newer/user,
  foreach/if), rate limits, status endpoints, monitoring, documentation.

**M4: attic**
- History dataset, `[date:]`/`retro`/`timeline`/`diff`/`adiff`.

**M5: extras** — CSV/popup/custom outputs, evaluators/`make`/`convert`,
DuckDB-Wasm browser mode, optional GOQL front end.

## 11. Open questions

1. **Language:** all-Rust, or Python prototype first? (Section 9 recommends
   Rust with a Python M0 planner if speed of iteration matters more.)
2. **R2 vs S3:** R2 for zero egress and a public bucket, or S3 to sit next to
   the OSMF `osm-planet-*` buckets for cheap in-region PBF/diff reads? (Both
   can be supported; the choice affects where the updater runs.)
3. **How strict is "Overpass compatible"?** Byte-identical output and error
   remarks (needed for overpass turbo/JOSM parity) versus "same language,
   documented differences" (e.g. `qt` ordering).
4. **Table format:** hand-rolled manifest first (recommended), or commit to
   DuckLake / Iceberg from the start?
5. **Attic scope:** full history from the 2012 license change (as Overpass
   does) or only from the day we start recording deltas?
6. **Public service or library?** A hosted endpoint has rate-limit and abuse
   concerns; a library/CLI that anyone can point at the public bucket has
   none, and the Wasm mode makes that very attractive.
