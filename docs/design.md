# Design: Overpass QL over object storage

Status: proposal, revised September 2026. Nothing here is implemented yet.

## 0. Decisions so far

| Question | Decision |
| --- | --- |
| Object store | **Cloudflare R2.** Zero egress, and the serverless compute below runs on the same network. S3 stays possible through the same `object_store`/httpfs abstraction but is not the target. |
| Query compute | **Serverless.** Queries mostly wait on I/O, so pay-per-active-time compute that sleeps when idle is the right shape. Primary target: Cloudflare Workers (front door) + Cloudflare Containers (engine). AWS Lambda is the fallback. |
| Compatibility | **"Most real queries work", not byte-identical.** Same language, same JSON/XML shape, documented differences on edge cases (ordering under `qt`, exotic evaluators, error page formatting). |
| Attic | **Full history, back to 2012 and earlier eventually.** Loaded from the full-history planet; prototyped on small/recent regional history extracts first. |
| Table format | Hand-rolled manifest for the hot query path (one small JSON per version); Iceberg on R2 Data Catalog for the history dataset and for offline/analytical access. Revisit if the manifest grows complex. |
| Language | Rust for the base builder, updater and query engine; a Python prototype of the parser/planner is acceptable for milestone 0 if it speeds up validation. |

## 1. Goal and constraints

Serve Overpass QL queries for the whole planet, current to within about a
minute, from data on R2, with:

- **No serving-side database.** Query compute may cache on ephemeral disk but
  must start from nothing, fetch a manifest, and answer correctly.
- **Pay only while a query runs.** Idle cost should be storage plus the one
  small updater process.
- **Overpass QL, not a new language.** overpass turbo, JOSM, Overpass Ultra
  and typical scripts should work for the common subset.
- **History is a real goal**, so the layout must support versioned rows from
  the start even though attic queries ship later.

What makes Overpass expensive today: its storage engine assumes random access
to hundreds of gigabytes of block files, and every instance carries the whole
thing. Object storage has the opposite profile: ~20-100 ms first-byte latency
per request, high aggregate throughput, immutable objects, near-zero cost per
GB, no egress on R2. So the design turns "many small random reads" into "few,
large, well-targeted range reads", and "in-place updates" into "append small
deltas, compact periodically". Serverless compute adds a third rule: **a cold
worker must become useful after one small fetch**, so nothing on the read path
may require preloading gigabytes.

Sizes we are designing for (September 2026): planet PBF 88 GB, history PBF
150 GB; on the order of 10 billion nodes, 1.1 billion ways, 13 million
relations; a few million element changes per day arriving in minutely
`.osc` files.

## 2. Architecture overview

```
                weekly planet PBF / history PBF          minutely .osc diffs
                          |                                     |
                   [ base builder ]                        [ updater ]   <- the one stateful process:
                    (batch job)                             small VM, local replication store
                          |                                     |
                          v                                     v
  +------------------------------------------------------------------------------------+
  |  R2 bucket                                                                         |
  |  base/<gen>/{node,way,relation,area}/cell=.../*.parquet   member_index/ node_idx/  |
  |  delta/<gen>/{hour,day,week}-<ver>.parquet  (rolling, cell-sorted, rewritten)      |
  |  manifest/<n>.json  manifest/LATEST        history/ (Iceberg via R2 Data Catalog)  |
  +------------------------------------------------------------------------------------+
                          ^                                     ^
                          | HTTP range reads                    | one GET per minute
                          |                                     |
   [ Cloudflare Worker ] --> [ Container: query engine ] x N  (DuckDB + Overpass QL planner;
     rate limit, cache,          sleeps when idle, ephemeral disk = cache)
     /api/interpreter
```

Three processes:

1. **Base builder** (batch, weekly or less): planet PBF to the partitioned
   Parquet layout of section 3, plus derived tables. Runs anywhere with a few
   hundred GB of scratch disk (a rented VM for a few hours, or the updater VM).
2. **Updater** (always on, exactly one): applies minutely diffs to a local
   replication store, re-resolves geometry for everything a change touches,
   rewrites the rolling delta files and publishes a new manifest. Also runs
   compaction and appends to the history dataset. This process is inherently
   continuous and stateful, so it is a small VM, not serverless (section 4.6
   discusses a stateless variant for later).
3. **Query engines** (serverless, N of them): a Worker receives
   `/api/interpreter`, applies caching and rate limits, and forwards to a
   container running the Overpass QL parser, planner and DuckDB. The
   container reads base and delta files from R2 with range requests, caches
   what it touched on its ephemeral disk, and goes to sleep after idling.

## 3. Storage layout

### 3.1 Two access patterns, one clustering

Overpass queries need exactly two kinds of access:

- **Spatial**: everything of some kind inside a bbox / polygon / area / radius.
- **By id**: `node(123)`, and the recursion operators `>`, `<`, `>>`, `<<`
  that follow node references and relation memberships.

Overpass keeps two indexes (quadtile block files and id block files). On
object storage we can afford one clustering per table, so we cluster
everything spatially and make id lookups *also* spatially scoped wherever
the data model allows it:

- A way's nodes lie inside the way's bbox, so `>` from ways needs node
  lookups only within the cells covering those ways.
- A way containing a node has a bbox containing that node, so `<` from nodes
  scans only the ways stored in the cells around that node.
- Relations can span a continent, so they get an explicit reverse
  membership table.
- Pure id queries (`node(123)`) use a small id-to-cell index.

### 3.2 Cells

A quadtree over plain lon/lat (quadkey-style, same family as Overpass's
quadtiles) with **adaptive depth**: a cell splits until its file falls under
a target (roughly 64-256 MB; smaller than a batch-analytics layout because a
serverless engine wants to finish in seconds, and R2 Data Catalog's own
compaction guidance says the same for latency-sensitive workloads). Dense
cities end up deep, oceans shallow. The manifest lists the leaf cells, so a
query computes "which cells intersect my bbox" without listing the bucket.

Elements with an extent (ways, relations, areas) are placed **loosely**: into
the smallest cell that fully contains their bbox, which may be an interior
cell. A bbox query reads the intersecting leaf cells *and* their ancestors up
to the root. Ancestors are few and hold long features (coastlines, rivers,
motorways, boundaries), which is what Overpass does for large objects too.

Within a file, rows are sorted by a space-filling curve (Hilbert) of the
bbox center, then id. Row groups are small (about 50k-100k rows, a few MB
compressed) so that min/max statistics on flat `xmin/ymin/xmax/ymax` (or
`lat/lon`) columns let DuckDB skip most row groups when the query bbox covers
part of a cell. Flat bbox columns are the portable way to get pruning in any
reader; DuckDB 1.5's native GEOMETRY statistics add pruning on top.

### 3.3 Tables

All tables are Parquet, zstd, dictionary-encoded strings, Hive-style
`cell=<quadkey>` directories where partitioned. Every row carries the
version metadata Overpass calls `meta`.

**`node`** (partitioned by cell, then `tagged=true|false`)

| column | type | note |
| --- | --- | --- |
| id | INT64 | |
| lat, lon | INT32 (1e-7 degrees) | delta-compresses far better than doubles; converted on read |
| tags | MAP<VARCHAR,VARCHAR> | NULL in the untagged partition |
| promoted tag columns | VARCHAR | section 3.4 |
| version, changeset, timestamp, uid, user | | Overpass `meta` |
| hilbert | UINT64 | sort key; also our `qt` order |

Roughly 96-97% of nodes are untagged way vertices. Separate partitions mean
`node[amenity=cafe](bbox)` scans only the tagged partition, while `>` and
`out skel` can still fetch untagged nodes by (cell, id).

**`way`** (partitioned by loose cell)

| column | type | note |
| --- | --- | --- |
| id | INT64 | |
| refs | LIST<INT64> | node ids in order; needed for `out body`/`skel` and `>` |
| tags, promoted tag columns, meta | | |
| xmin, ymin, xmax, ymax | INT32 | flat bbox for pruning |
| geometry | GEOMETRY | LINESTRING of node coordinates in ref order |
| is_closed, is_area | BOOLEAN | lets `area`/`is_in`/`around` build polygons on demand |
| centroid_lat, centroid_lon | INT32 | `out center` without reading geometry |
| hilbert | UINT64 | |

Because the way row carries its coordinates, `out geom`, `out center`,
`out bb`, `around`, `area` and `poly` never join to `node`. Only
`out body/meta` *after* `>` needs node rows, looked up by (cell, id).

**`relation`** (partitioned by loose cell; most land in shallow cells)

| column | type | note |
| --- | --- | --- |
| id | INT64 | |
| members | LIST<STRUCT<type: UINT8, ref: INT64, role: VARCHAR>> | |
| tags, promoted tag columns, meta | | |
| xmin..ymax | INT32 | bbox over resolved members, NULL if unresolvable |
| geometry | GEOMETRY | MULTIPOLYGON for multipolygon/boundary, GEOMETRYCOLLECTION otherwise, NULL if unresolvable |
| centroid_lat, centroid_lon | INT32 | |

**`member_index`** (sorted by member_type, member_id; ~150M rows)

`(member_type, member_id, parent_id, role, parent_cell)`. Answers
node→relation, way→relation, relation→relation for `<`, `<<` and the
`(bn|bw|br)` filters. node→way is *not* materialized at first (~11 billion
rows); `<` from nodes scans ways in the node's cell and ancestors with
`list_contains(refs, id)`. Add a `node_way_index` later if measurements say so.

**`node_idx`, `way_idx`** (sorted by id; `(id, cell)`)

Id-to-cell index for `node(123)` and id-set inputs. Node ids are nearly
monotonic in creation time, so delta encoding makes this ~3-4 bytes per row
(tens of GB for nodes, a few GB for ways). A lookup for a sorted batch of ids
touches one row group per id range. Relations need no index (13M rows).

**`area`** (partitioned by loose cell)

Derived per Overpass's `areas.osm3s` rules (multipolygon/boundary relations
with `name`, relations with `admin_level`+`name`, relations with
`postal_code` / `addr:postcode`, ways with `area=yes`+`name`, ...), with the
Overpass id convention (`way_id + 2400000000`, `relation_id + 3600000000`),
polygon geometry, copied tags, and the pivot (source type, id). Regenerated
for touched pivots at each compaction; Overpass's own area loop takes 4-12
hours per pass, so hourly/daily is no worse.

**`tag_stats`** (small): `(key, value, cell, count)` for key/value pairs
below an occurrence threshold. Global queries on a rare tag without a bbox go
straight to the cells that contain matches. Common tags without a bbox are
capped, as Overpass effectively does through timeouts.

### 3.4 Tags: map column plus promoted columns

`tags` is a `MAP<VARCHAR,VARCHAR>`. DuckDB evaluates `tags['amenity'] = 'cafe'`
but cannot push it into the Parquet reader. The writer therefore **promotes
the most-queried keys into their own nullable VARCHAR columns** (30-50 keys:
`amenity, shop, highway, building, name, natural, landuse, leisure, railway,
power, waterway, place, boundary, admin_level, type, tourism, office, craft,
public_transport, route, addr:street, addr:postcode, addr:city, ...`). Those
get dictionary encoding, min/max statistics and Parquet bloom filters, which
DuckDB uses for equality predicates. The planner rewrites `["amenity"="cafe"]`
to `amenity = 'cafe'` for promoted keys and to `tags['amenity'] = 'cafe'`
otherwise; regex filters always use the map. A `tag_keys LIST<VARCHAR>`
column serves the existence filter `["name"]`.

The promoted list is part of the manifest's schema version, so it can change
between base generations without breaking readers.

### 3.5 Manifest

A serverless engine that just woke up must learn the state of the world in
**one small fetch**. That rules out anything that needs a catalog round trip
plus several metadata files on the hot path, and it is why the current
dataset uses a hand-rolled manifest rather than Iceberg:

- `manifest/<n>.json`: schema version, promoted-key list, base generation,
  leaf-cell list per table with file sizes and bboxes, the current rolling
  delta files (section 4.3) with their versions, latest applied replication
  sequence and timestamp (this becomes `timestamp_osm_base`).
- `manifest/LATEST`: the number of the newest manifest, written last. Objects
  are immutable, so an engine that loaded manifest *n* stays consistent for
  the whole query even while *n+1* appears.
- Old base generations and delta versions stay until no live manifest
  references them, then get deleted by the updater.

For comparison, DuckDB reading an Iceberg table from R2 Data Catalog must
authenticate to the REST catalog, fetch table metadata, the manifest list and
the Avro manifests (megabytes for a table with thousands of files) before it
can prune. That is fine for analytics and for the history dataset (section
4.5), and it lets R2 SQL, Spark and PyIceberg read the same data, but it is
the wrong shape for a sub-second interactive query on a cold container.
DuckLake has the same catalog-round-trip property plus a database to host.

The manifest is a few hundred lines of code and stays explicit about the
bespoke rules (loose placement, id index).

## 4. Update pipeline

### 4.1 Replication store

The updater keeps, on local NVMe (budget ~300 GB, growing slowly):

- latest version of every node (id → lat, lon, tags-or-null, meta),
- every way (id → refs, tags, meta) and relation (id → members, tags, meta),
- reverse indexes: node→ways, node→relations, way→relations, relation→relations,
- the cell each element currently lives in.

Candidates: RocksDB (ohsome-planet's choice), LMDB via OSMExpress (already
implements the store, `.osc` application and S2 indexing in ~1,500 lines of
C++), or a purpose-built store. Recommendation: prototype with OSMExpress to
avoid writing the store, then decide.

### 4.2 Per-minute cycle

1. Fetch the next `.osc.gz` by replication sequence (pyosmium-style state
   handling, retries, gap detection).
2. Apply to the store; collect **touched elements**: every created/modified/
   deleted node, way, relation, plus every parent way of a moved/deleted node
   and every parent relation of a touched node/way/relation, transitively
   for nested relations. QLever's `osm-live-updates` and ohsome-planet do the
   same step; Freiburg reports under 7 s per minute for the planet.
3. Re-resolve geometry, bbox, centroid, `is_area` and cell for touched ways
   and relations. Re-derive area rows for touched pivots.
4. Rewrite the rolling delta files (4.3) and append the touched elements'
   previous versions to the history dataset (4.5).
5. Write manifest *n+1*, then `LATEST`.

Target latency: under 60 seconds behind planet.openstreetmap.org.

### 4.3 Rolling deltas, designed for cold readers

An earlier draft had workers preload a week of deltas into memory. That is
the wrong shape for compute that sleeps and wakes. Instead the deltas are
**a small fixed number of remote files that prune like the base**:

| file | content | rewritten |
| --- | --- | --- |
| `delta/<gen>/hour-<ver>.parquet` | every element touched since the top of the hour, newest version wins | every minute (a few MB) |
| `delta/<gen>/day-<ver>.parquet` | touched since midnight | hourly (tens to a few hundred MB) |
| `delta/<gen>/week-<ver>.parquet` | touched since the base generation | daily (~1-2 GB) |

Each is sorted by (cell, hilbert) with the same flat bbox columns and row
group sizes as the base, so a bbox query reads a few row groups from each.
Rows carry `deleted BOOLEAN`, `cell` and `prev_cell` so readers can shadow the
old row wherever it was. Rewriting a few-MB file every minute and a ~1 GB
file daily is trivial on R2 (Class A operations are ~$4.50 per million).

A query therefore touches: the base cells for its bbox, plus at most three
delta files. Read path per table:

```sql
WITH d AS (SELECT * FROM read_parquet([hour, day, week]) WHERE <bbox pruning>
           QUALIFY row_number() OVER (PARTITION BY id ORDER BY version DESC, seq DESC) = 1)
SELECT ... FROM read_parquet(<base files for cells>) b
WHERE <bbox pruning> AND <predicates>
  AND NOT EXISTS (SELECT 1 FROM d WHERE d.id = b.id)        -- shadowed by a newer version
UNION ALL
SELECT ... FROM d WHERE NOT deleted AND <predicates>
```

Elements that moved *out* of the queried cells are handled by `prev_cell`:
a delta row whose `prev_cell` is in the queried set but whose `cell` is not
still shadows the base row.

A warm container keeps the delta files it has read in its disk cache; new
versions have new names, so the cache never serves stale data.

### 4.4 Compaction

- Every minute, hour and day: the rolling files above (this *is* the
  hourly/daily compaction).
- Weekly, or when the week file exceeds a few percent of the base: rewrite
  the touched base cell files into a new generation and start a fresh set of
  rolling deltas. Untouched cells are referenced, not rewritten. Areas and
  `tag_stats` are refreshed for touched cells.

All of this runs on the updater; none of it blocks readers, because manifests
are immutable and swapped atomically.

### 4.5 History (attic)

Scope: the full history back to 2012 and, where the history planet has it,
earlier. Modelled as an append-only **`history`** dataset with one row per
*version of an element's state*, ohsome-planet style:

- `osm_type, id, version, minor_version, valid_from, valid_to, deleted,
  tags, geometry/refs/members, bbox, meta, cell`.
- **Minor versions matter.** A way's geometry changes when one of its nodes
  moves even though the way's own version does not. Overpass's attic treats
  that as a new state; so do we, by writing a minor-version row for every
  parent way/relation the updater re-resolves. This inflates the dataset
  (ohsome-planet's history is a few times the size of the current planet)
  and is the price of correct `[date:]` geometry.
- Initial load from the full-history PBF (150 GB) through the same geometry
  assembler, then continuous appends from the updater (step 4 in 4.2 writes
  the *previous* state with its `valid_to` closed and the new state with
  `valid_to = NULL`).
- Stored as an **Iceberg table in R2 Data Catalog**, partitioned by cell and
  by `valid_to` year (closed versions) with an "open" partition for current
  ones. This dataset is append-mostly and read by analytical and attic
  queries where a catalog round trip is acceptable; R2 Data Catalog's
  automatic compaction (64-512 MB target) and snapshot expiration handle
  maintenance, and DuckDB 1.4+ can both read and append to it. DuckDB cannot
  `DELETE` on partitioned Iceberg tables, which is fine for append-only.
- `[date:t]` / `retro` become `valid_from <= t AND (valid_to IS NULL OR
  valid_to > t)`; `timeline` is a per-id scan; `diff`/`adiff` are two such
  scans compared. Prototype on a regional history extract (osmium
  `extract --with-history` of a small area) before touching the planet.

### 4.6 A stateless updater, later

Once `node_idx`, `member_index` and (if added) `node_way_index` exist on R2,
the per-minute working set (changed nodes → parent ways → their node
coordinates) could in principle be fetched from R2 instead of a local store,
which would let the updater run as a scheduled container too. It costs
thousands of range reads per minute and the geometry assembler would need a
read-through cache to stay under the minute budget. Worth an experiment after
milestone 2, not before.

## 5. Query engine

### 5.1 Pipeline

```
Overpass QL text -> lexer/parser -> AST -> semantic pass (set names, types, settings)
   -> planner (statement -> SQL over manifest-resolved files; sets are temp tables)
   -> DuckDB execution (memory/timeout limits)
   -> Overpass-shaped serializer (JSON / XML / CSV, out modes)
```

Overpass semantics are set-based: each statement reads the default set `_`
or a named set and writes a set. A set is a temporary table
`set_<name>(type, id, cell, payload...)` holding full rows so `out` never
re-fetches. Sets are small in practice; large ones spill to the container's
ephemeral disk.

### 5.2 Statement translation sketches

| Overpass | Plan |
| --- | --- |
| `[bbox:s,w,n,e]` / `(s,w,n,e)` filter | Leaf cells + ancestors intersecting the bbox from the manifest; `read_parquet` of exactly those files plus the three delta files, with `xmax >= w AND xmin <= e ...` so row groups prune. |
| `node["amenity"="cafe"]` | Tagged partition only; promoted column equality or `tags['k'] = 'v'`; `~` uses `regexp_matches`; `!=`, `!~`, `[k]`, `[!k]`, `[~k~v]`, `,i` map directly. |
| `way(id:1,2,3)` / `node(123)` | `way_idx`/`node_idx` for cells, then read those cells filtered by `id IN (...)`. Ids already in a set carry their cell. |
| `(.a; .b;)` union, `(.a; - .b;)` difference, `.a.b` intersection | `UNION` / `EXCEPT` / `INTERSECT` on `(type, id)` with payload. |
| `>` | Ways → node ids from `refs`; nodes synthesized from way geometry + refs for skel output, looked up by (way's cells, id) when tags/meta are needed. Relations → members via `members`, resolved per member type. |
| `>>` | `>` iterated over relation→relation to a fixed point, then `>`. |
| `<` | Nodes → ways: scan ways in the node's cells + ancestors with `list_contains(refs, id)`. Anything → relations: `member_index`. |
| `<<` | Transitive `member_index` closure. |
| `node(w)`, `way(bn)`, `rel(bw)`, `node(r:"role")` ... | Same machinery restricted to one type and optional role. |
| `(around:r)` / `(around.set:r)` | Bbox expanded by r meters for pruning, then spheroid distance against the set's geometries. |
| `(poly:"...")` | Polygon bbox for pruning, `ST_Intersects`. |
| `area[...]`, `(area.a)`, `(area:id)` | `area` table; `node(area)` = `ST_Within`; `way(area)` = `ST_Intersects`; pruned by the area's bbox and cells. |
| `is_in` | Point-in-polygon against `area` rows in the point's cells. |
| `(pivot.a)` | Area rows → their source element by pivot id. |
| `(newer:)`, `(changed:)`, `(user:)`, `(uid:)` | Meta columns; `changed` with a range is exact only with history. |
| `(if: expr)` | Evaluator subset compiled to SQL expressions. |
| `foreach`, `if`, `for`, `complete` | Planner-side control flow over temp tables. |
| `out ids/skel/body/tags/meta/geom/bb/center/count/qt/asc/N` | Serializer; `qt` sorts by our Hilbert key (documented difference from Overpass's Z-order); default order is nodes, ways, relations by id. |
| `[timeout:n]`, `[maxsize:n]` | DuckDB `interrupt()` on a timer; `SET memory_limit`; Overpass-style `remark`. |
| `[out:json\|xml\|csv]` | JSON and XML first, CSV next, `popup`/`custom` last. |
| `make`, `convert`, `derived` | Planner-side synthetic elements; later. |
| `[date:]`, `retro`, `timeline`, `[diff:]`, `[adiff:]` | Section 4.5. |

Compatibility is checked with a corpus of real queries (overpass turbo
wizard output, wiki examples, JOSM's built-in queries, Overpass Ultra
examples) run against overpass-api.de and against us, comparing the *set* of
elements, their tags and geometry, not bytes.

### 5.3 Engine choice

DuckDB: embedded, reads remote Parquet with pruning, built-in GEOMETRY with
statistics (1.5+), spatial functions, Rust/Python bindings, a Wasm build,
Iceberg read/write. Apache DataFusion is the alternative if we later want to
own the physical operators; it lacks DuckDB's spatial maturity. The planner
emits SQL plus a small "scan table T in cells C" primitive so the engine can
be swapped.

## 6. Serverless serving tier

### 6.1 Shape

```
client --> Cloudflare Worker (/api/interpreter, /api/status, /api/timestamp)
             | parse settings only (timeout, out format), normalize query text
             | Cache API: identical query + current manifest number -> cached response (60 s)
             | rate limiting binding: per-IP/API-key slots like Overpass's
             v
           Container (Durable-Object-managed pool, sleepAfter ~ minutes)
             | Rust binary: HTTP -> parser -> planner -> DuckDB (httpfs + spatial baked in)
             | manifest cached in memory; parquet blocks cached on ephemeral disk
             v
           R2 (same network, zero egress)
```

- **Worker** does the cheap, high-volume work: request parsing, a response
  cache keyed by normalized query text plus manifest number (overpass turbo
  and polling apps re-issue identical queries constantly), rate limiting, and
  routing to a container. It never touches data.
- **Container** is the engine. Cloudflare Containers instances go up to
  `standard-4` (4 vCPU, 12 GiB, 20 GB disk) and custom shapes up to those
  maxima; `standard-3` (2 vCPU, 8 GiB, 16 GB disk) is the likely default. The
  disk is ephemeral and is used purely as a `cache_httpfs`-style block cache.
  Instances sleep after an idle timeout and are billed per 10 ms of active
  time, so a quiet service costs nothing beyond storage.
- **Cold start budget.** Container boot plus DuckDB init is on the order of a
  second or two. To keep it there: extensions are baked into the image (no
  `INSTALL` at runtime), the manifest is one GET, and nothing is preloaded.
  The first query on a cold instance pays a few extra round trips for Parquet
  footers; subsequent ones hit the disk cache.
- **Concurrency.** One instance serves a handful of queries at a time (DuckDB
  parallelizes within a query; memory is the limit). The Worker spreads load
  across instances and can spin up more; account limits are in the thousands
  of `standard-*` instances.
- **Limits that matter.** A single query is bounded by the instance's memory
  (12 GiB max) and by our own timeout. Planet-scale scans do not fit this
  shape and are refused, like Overpass refuses them with timeouts.

### 6.2 Alternatives kept open

- **AWS Lambda** (arm64, up to 10 GB memory, 15 min): DuckDB in Lambda is well
  trodden, with cold starts around 1-2 s and ~18 MB layers. Cross-cloud reads
  from R2 add tens of ms per request but no egress. Useful if Cloudflare
  Containers' beta status or limits become a problem.
- **R2 SQL** on the Iceberg history table for heavy offline scans (priced per
  TB scanned), never on the interactive path.
- **DuckDB-Wasm in the browser** with the same planner, for small bbox
  queries against the public bucket with no server at all. The most
  serverless option of all, and a good demo.

## 7. Cost model (rough, 2026 list prices)

| Item | Estimate |
| --- | --- |
| Current dataset on R2 (nodes ~100 GB, ways ~150-200 GB, relations, indexes, areas) | ~350-500 GB → ~$5-8/month at $0.015/GB-month; egress $0 |
| History dataset (Iceberg) | ~0.5-1.5 TB once complete with minor versions → ~$8-25/month |
| R2 operations | Class B (reads) ~$0.36/M, Class A (writes) ~$4.50/M. A typical query issues 10-200 range requests → roughly $0.01-0.07 per 1,000 queries. The updater's minutely rewrites are a few hundred writes per hour. |
| Container compute | `standard-3` active: 8 GiB × $0.0000025/GiB-s + 2 vCPU × $0.00002/vCPU-s + 16 GB × $0.00000007/GB-s ≈ $0.00006 per active second, i.e. ~$0.22 per active hour; a 2-second query ≈ $0.00012. Workers Paid plan $5/month includes 25 GiB-hours memory and 375 vCPU-minutes. Sleeping instances cost nothing. |
| Crossover | At sustained load (say 100k two-second queries per day ≈ 55 active hours/day) serverless is ~$12/day, and a fixed VM pool becomes cheaper. The design does not care: the same container image runs on a VM. |
| Updater VM | 8 vCPU / 32 GB RAM / 400 GB NVMe: ~$40-120/month depending on provider |
| Compare: self-hosted Overpass | ~1 TB NVMe + 32-64 GB RAM *per replica*, always on |

## 8. Risks and how to check them early

| Risk | Mitigation / experiment |
| --- | --- |
| Cold-container plus object-store latency makes interactive queries feel slow | Measure on a state extract from a real container with a cold disk; issue file reads concurrently; few files and few row groups per query; response cache in the Worker; keep a small warm pool (`sleepAfter` of several minutes) during busy hours. |
| Global tag queries without bbox scan too much | `tag_stats` cell index for rare tags; cap common ones without bbox. |
| `<` from nodes to ways in dense cells is slow | Measure; add a `node_way_index` (~11B rows, tens of GB) if needed. |
| Relation geometry assembly (multipolygon rings, broken rings) | Reuse a proven assembler (libosmium via bindings, or a Rust port of its ring builder); NULL geometry for unresolvable relations, members still returned. |
| Cloudflare Containers is still beta | Same image runs on Lambda, Fly, or a VM; nothing in the engine depends on Cloudflare APIs, only the Worker front door does. |
| Rolling delta files grow large late in a generation | Compact into a new base generation on size, not only weekly. |
| History with minor versions is large | It is append-only cold data on R2; prototype on a regional history extract to measure the multiplier before the planet load. |
| DuckDB spheroid distance is slower than Overpass's integer math | Bbox pre-filter; planar approximation with correction for `around`. |

## 9. Implementation language and stack

**Rust** for the base builder and updater (`osmpbf`, `parquet`/`arrow`,
`object_store`, `rocksdb` or `heed`/LMDB, `geo`), and for the query engine
binary that runs in the container (`duckdb` crate, `axum`). The pipeline has
to chew through an 88 GB PBF and sustain minutely updates forever, and the
container image should start fast and stay small, which Rust does well.

Pragmatic option for milestone 0: prototype the Overpass QL parser and
QL-to-SQL planner in **Python** (Lark grammar, `duckdb` package) against an
extract to validate semantics and layout quickly, then port the planner to
Rust once the SQL it emits is settled. Layout and manifest are
language-neutral, so the halves can be built in different languages.

Licensing: Overpass API is AGPL-3, ohsome-planet is GPL-3. We learn from both
and copy no code; this project can be Apache-2/MIT.

## 10. Roadmap

**M0: extract-scale prototype**
- Base builder for a Geofabrik extract (a US state) into the section 3 layout
  on an R2 bucket.
- Parser + planner for the tier-1 subset (`overpass-ql-support.md`).
- Engine container behind a Worker; `/api/interpreter` returning JSON/XML;
  run overpass turbo against it.
- Differential test harness vs overpass-api.de (bboxed to the extract),
  semantic comparison.
- Publish cold and warm latency and cost per query. **Go/no-go gate.**

**M1: full planet base build**
- Build the planet; record build time, file counts, sizes, cell depth
  distribution; tune cell split thresholds and row group sizes.

**M2: minutely updates**
- Updater with replication store, rolling deltas, manifests; run for a month
  and measure lag and compaction cost.

**M3: public beta**
- Tier-2 features (areas, around, poly, is_in, changed/newer/user,
  foreach/if), rate limits, status endpoints, monitoring, docs.

**M4: history**
- Regional history extract → Iceberg history table → `[date:]`/`retro`/
  `timeline`/`diff`/`adiff`; then the full-history planet load.

**M5: extras** — CSV/popup/custom outputs, evaluators/`make`/`convert`,
DuckDB-Wasm browser mode.

## 11. Open questions

1. **Warm pool policy.** How long should containers stay awake after a query
   (`sleepAfter`)? Longer means fewer cold starts and more idle cost; decide
   from M0 latency numbers.
2. **Public endpoint or bucket-plus-tooling?** A hosted endpoint needs abuse
   handling; publishing the bucket and the engine (including the Wasm mode)
   lets anyone run it. Both can coexist.
3. **Stateless updater** (section 4.6): worth pursuing after M2 if the VM is
   the only non-serverless piece left?
