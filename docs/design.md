# Design: Overpass QL over object storage

Status: proposal, revised September 2026. Nothing here is implemented yet.

## 0. Decisions so far

| Question | Decision |
| --- | --- |
| Object store | **Cloudflare R2.** Zero egress, and the serverless compute below runs on the same network. S3 stays possible through the same `object_store`/httpfs abstraction but is not the target. |
| Query compute | **Serverless.** Queries mostly wait on I/O, so pay-per-active-time compute that sleeps when idle is the right shape. Primary target: Cloudflare Workers (front door) + Cloudflare Containers (engine). AWS Lambda is the fallback. |
| Warm pool | **Shut down quickly at first** (one user, a short `sleepAfter`, cold starts are acceptable). Lengthen `sleepAfter` and keep a warm instance once there is real traffic. |
| Initial load | **Runs on a laptop or a rented big node**, not in the cloud. It is a one-off batch job with a few hundred GB of scratch disk; nothing about it needs to be serverless. |
| Minutely updates | **Must run in a cloud container**, which has no persistent disk. So the updater is **stateless**: every piece of state it needs lives on R2, and it keeps only an ephemeral cache. This is the biggest consequence of the decisions so far and shapes section 4. |
| Public endpoint | **Yes.** A hosted `/api/interpreter` with Overpass-style rate limits and status endpoints, ODbL attribution in every response. Publishing the bucket for people to run their own engine can come later. |
| Compatibility | **"Most real queries work", not byte-identical.** Same language, same JSON/XML shape, documented differences on edge cases (ordering under `qt`, exotic evaluators, error page formatting). |
| Attic | **Full history, back to 2012 and earlier eventually.** Loaded from the full-history planet; prototyped on small/recent regional history extracts first. |
| Table format | Hand-rolled manifest for the hot query path (one small JSON per version); Iceberg on R2 Data Catalog for the history dataset and for offline/analytical access. Revisit if the manifest grows complex. |
| Language | Rust for the two whole-planet passes (nodes, ways: `rust/osmpq-raw`); Python + DuckDB for the relational tail of the build (relations, indexes, manifest) and for the query engine until its SQL is stable. M0/M1 showed DuckDB's out-of-core SQL is fast enough for everything except touching every node. |

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
      weekly planet PBF / history PBF                       minutely .osc diffs
                |                                                  |
         [ base builder ]                                   [ updater ]  <- scheduled cloud container,
     (one-off batch on a laptop                              stateless: reads what it needs
      or a rented big node)                                  from R2, ephemeral cache only
                |                                                  |
                v                                                  v
  +------------------------------------------------------------------------------------------+
  |  R2 bucket                                                                               |
  |  spatial/<gen>/{node,way,relation,area}/cell=.../*.parquet      <- query copy            |
  |  byid/<gen>/{node,way,relation}/*.parquet  node_way_index/  member_index/  <- id copy    |
  |  delta/<gen>/{hour,day,week}-<ver>.{spatial,byid}.parquet   (rolling, rewritten)         |
  |  manifest/<n>.json  manifest/LATEST            history/ (Iceberg via R2 Data Catalog)    |
  +------------------------------------------------------------------------------------------+
                          ^                                     ^
                          | HTTP range reads                    | one GET per query
                          |                                     |
   [ Cloudflare Worker ] --> [ Container: query engine ] x N  (DuckDB + Overpass QL planner;
     rate limit, cache,          short sleepAfter at first, ephemeral disk = cache)
     /api/interpreter
```

Three processes, none of which owns a database:

1. **Base builder** (one-off batch, laptop or rented big node): planet PBF to
   the two copies of the current state described in section 3 (a spatially
   clustered copy for queries and an id-sorted copy for lookups and for the
   updater), plus the reverse-membership indexes and derived tables. Writes
   straight to R2, or locally and then syncs.
2. **Updater** (a container run every minute, exactly one at a time):
   fetches the next `.osc`, works out what it touches, reads the current
   state of those elements and their nodes from the id-sorted copy on R2,
   re-resolves geometry, rewrites the rolling delta files and publishes a
   new manifest. Everything it needs is on R2; its disk is a cache.
   Compaction is the same program run with a bigger instance and a longer
   schedule.
3. **Query engines** (serverless, N of them): a Worker receives
   `/api/interpreter`, applies caching and rate limits, and forwards to a
   container running the Overpass QL parser, planner and DuckDB, which reads
   base and delta files from R2 with range requests and sleeps after idling.

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
bbox center, then id. Row groups are sized by compressed bytes (about 1 MB
for nodes, 4 MB for ways) so that min/max statistics on flat
`xmin/ymin/xmax/ymax` (or `lat/lon`) columns let DuckDB skip most row groups
when the query bbox covers part of a cell, while each file still costs few
range requests (DuckDB reads one range per column chunk per row group).
A row-group index side file per table (bbox per row group, built from the
footers) lets the engine skip whole files without a request. Flat bbox
columns are the portable way to get pruning in any reader; DuckDB 1.5's
native GEOMETRY statistics add pruning on top.

M1 learned that loose placement at every depth costs a fixed tax of about
ten files per query, so ways and relations are placed only at leaves or at
depths {0, 3, 6, 9, 12}; and that Parquet encodings matter as much as the
schema (delta encoding on the sort key and coordinates, no dictionary on
numeric columns: 2x smaller files than the writer defaults).

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

**The id-sorted copy: `byid/{node,way,relation}`**

The same rows as the spatial tables (nodes as `(id, lat, lon, tags, meta,
cell)`, ways and relations with refs/members, tags, meta, bbox and `cell`, but
no geometry), sorted by id and split into files of a few hundred thousand
row groups' worth. Row-group min/max on `id` makes "fetch these 50k ids" a
merge over the row groups that cover them, and ids issued in one editing
session are contiguous, so real batches touch far fewer row groups than
their size suggests.

This copy exists for two reasons. It answers `node(123)` and id-set inputs
for queries (replacing a separate id-to-cell index; the `cell` column then
says where the spatial row lives). And it is what makes the updater
stateless: the replication store that Overpass, ohsome-planet and OSMExpress
keep on local disk is here a Parquet table on R2 that the updater reads by
range request. Doubling the current-state data costs roughly $4-6/month at R2
prices, which is the whole thesis of the project.

**`node_way_index`** (sorted by node_id; ~11 billion rows, tens of GB)

`(node_id, way_id)`. Needed by the updater ("which ways contain the node that
just moved") and by `<` from nodes to ways. The earlier draft deferred it in
favour of scanning ways in the node's cell; the stateless updater makes it
mandatory.

**`member_index`** (sorted by member_type, member_id; ~150M rows)

`(member_type, member_id, parent_id, role)`. Answers node→relation,
way→relation, relation→relation for the updater, for `<`, `<<` and the
`(bn|bw|br)` filters.

**`area`** (partitioned by loose cell) and **`way_areas`** index

The reference makes every closed way an area and prints it as the way
itself, so way areas are not stored: a closed way's polygon is built from
its own LINESTRING when a filter needs it, and `is_in` finds containing
closed ways in the cells covering the point. Only relation areas are
materialized, per the reference's `areas.osm3s` rules (multipolygon or
boundary relations with `name`, relations with `admin_level` and `name`,
`postal_code`, `addr:postcode`), with `id = relation_id + 3600000000`,
polygon geometry, copied tags and the pivot. A small id-sorted index of
closed ways carrying `name`/`ref`/`admin_level`/`boundary`/`place` serves
`area[...]` lookups by tag. Both are regenerated at compaction; Overpass's
own area loop takes 4-12 hours per pass, so hourly/daily is no worse.

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

### 4.1 No replication store

Overpass, ohsome-planet, OSMExpress and QLever's updater all keep a local
random-access copy of the current state (hundreds of GB on NVMe) so that a
moved node can be turned into a re-resolved way geometry. Our updater runs in
a container with ephemeral disk, so that state lives on R2 instead, in the
id-sorted copy and the two reverse indexes of section 3.3. The updater's
local disk is a read-through cache that happens to be warm across
consecutive runs while the container stays alive, and is allowed to be empty.

The trade is round trips for state. A minute of planet edits is on the order
of thousands to a few tens of thousands of node changes, a few thousand way
changes and hundreds of relation changes. Re-resolving the touched ways needs
the coordinates of all their nodes, which is typically 100k-600k node
lookups. Edits are spatially and id-locally clustered (a changeset touches
contiguous ids in one area), so those lookups collapse into hundreds to a few
thousand row groups, i.e. a few GB of range reads issued with high
concurrency. On R2 that is a few thousand Class B operations (fractions of a
cent) and tens of seconds of wall clock. If a minute turns out to be too
tight, running every 2-3 minutes is still "minutely updates" in the sense
that matters; Overpass instances routinely lag that much.

### 4.2 Per-run cycle (scheduled every minute)

1. Fetch `manifest/LATEST` and the manifest; load the current generation's
   rolling deltas in their id-sorted variant (section 4.3) into memory (a few
   MB to ~1-2 GB late in a generation; the container has 8-12 GiB). Build an
   in-memory inverted index of delta ways' refs and delta relations' members,
   because the base `node_way_index`/`member_index` are static per generation
   and do not know about ways created or re-noded since.
2. Fetch the next `.osc.gz` by replication sequence (pyosmium-style state
   file, retries, gap detection). Apply to the in-memory delta view.
3. Compute the **touched set**: every created/modified/deleted element, plus
   parent ways of moved/deleted nodes (`node_way_index` ⊕ delta index), plus
   parent relations of anything touched (`member_index` ⊕ delta index),
   transitively for nested relations.
4. Fetch current state for the touched ways/relations and coordinates for
   every node they reference, from the id-sorted copy on R2 shadowed by the
   in-memory deltas and this minute's changes. Batched, sorted, concurrent
   range reads; cached on local disk for the next run.
5. Re-resolve geometry, bbox, centroid, `is_area` and cell. Re-derive area
   rows for touched pivots. Append the previous state of every touched
   element to the history dataset (section 4.5).
6. Rewrite the rolling delta files (both sort variants) and write manifest
   *n+1*, then `LATEST`.

A single Durable Object owns the schedule: its alarm fires every minute,
starts (or wakes) the updater container, and refuses to start another run
while one is in flight. That is the "cron with a lock" the pipeline needs;
Cloudflare Cron Triggers alone do not guarantee exclusivity. If Containers'
beta limits bite, the identical image runs under any scheduler that can
promise one instance at a time.

### 4.3 Rolling deltas, in two sort orders

Deltas are **a small fixed number of remote files that prune like the base**,
each written twice: sorted by (cell, hilbert) for the query engines and
sorted by id for the updater and for id lookups.

| file | content | rewritten |
| --- | --- | --- |
| `delta/<gen>/hour-<ver>.*.parquet` | every element touched since the top of the hour, newest version wins | every run (a few MB) |
| `delta/<gen>/day-<ver>.*.parquet` | touched since midnight | hourly (tens to a few hundred MB) |
| `delta/<gen>/week-<ver>.*.parquet` | touched since the base generation | daily (~1-2 GB) |

Rows carry `deleted BOOLEAN`, `cell` and `prev_cell` so readers can shadow
the old row wherever it was. Rewriting a few-MB file every minute and a ~1 GB
file daily is trivial on R2 (Class A operations are ~$4.50 per million).

A query touches the base cells for its bbox plus at most three delta files.
Read path per table:

```sql
WITH d AS (SELECT * FROM read_parquet([hour, day, week]) WHERE <bbox pruning>
           QUALIFY row_number() OVER (PARTITION BY id ORDER BY version DESC, seq DESC) = 1)
SELECT ... FROM read_parquet(<base files for cells>) b
WHERE <bbox pruning> AND <predicates>
  AND NOT EXISTS (SELECT 1 FROM d WHERE d.id = b.id)        -- shadowed by a newer version
UNION ALL
SELECT ... FROM d WHERE NOT deleted AND <predicates>
```

Elements that moved *out* of the queried cells are handled by `prev_cell`.
A warm container keeps delta files it has read in its disk cache; new
versions have new names, so the cache never serves stale data.

### 4.4 Compaction

- Every run, hour and day: the rolling files above (this *is* the
  hourly/daily compaction).
- Weekly, or when the week file exceeds a few percent of the base: a
  compaction run rewrites the touched base cell files (both copies) and the
  affected slices of `node_way_index`/`member_index` into a new generation,
  then starts fresh rolling deltas. This is a streaming job (read old file
  plus delta rows, write new file, per cell) with no large local state, so
  it runs in a bigger scheduled container (`standard-4`) for a few hours, or
  on the big node used for the initial load if that is cheaper. Untouched
  cells are referenced, not rewritten. Areas and `tag_stats` are refreshed
  for touched cells.

All of this happens without blocking readers, because manifests are
immutable and swapped atomically.

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
- Initial load from the full-history PBF (150 GB) on the big node through
  the same geometry assembler, then continuous appends from the updater
  (step 5 in 4.2 writes the *previous* state with its `valid_to` closed and
  the new state with `valid_to = NULL`).
- Stored as an **Iceberg table in R2 Data Catalog**, partitioned by cell and
  by `valid_to` year (closed versions) with an "open" partition for current
  ones. It is append-mostly and read by analytical and attic queries where a
  catalog round trip is acceptable; R2 Data Catalog's automatic compaction
  (64-512 MB target) and snapshot expiration handle maintenance, and DuckDB
  1.4+ can both read and append to it. DuckDB cannot `DELETE` on partitioned
  Iceberg tables, which is fine for append-only.
- `[date:t]` / `retro` become `valid_from <= t AND (valid_to IS NULL OR
  valid_to > t)`; `timeline` is a per-id scan; `diff`/`adiff` are two such
  scans compared. Prototype on a regional history extract (osmium
  `extract --with-history` of a small area) before touching the planet.

**M4 amendment (September 2026).** History is stored as Parquet under the
dataset's own manifest (`history/<gen>/…`, manifest v5 `history` key,
`docs/m4-contracts.md` section 2), not as an Iceberg table: the engine
container reads it through the same `read_parquet` + manifest path as the
current tables with no catalog round trip, the M2 rolling tiers give
cheap append-only writes per minute, and DuckDB can only write Iceberg
through a REST catalog. An Iceberg export of the same rows for the
analytical copy remains an option. Two details differ from the sketch
above: `valid_to` is an optimization (tier rows leave it NULL and readers
pick the state at `t` with a window function), and cell moves write a
tombstone into the vacated cell so cell-scoped scans stay correct.

### 4.6 Initial load

Runs once on a laptop or a rented node with a few hundred GB of scratch
space. Passes over the planet PBF:

1. Nodes: stream, assign cells, write the id-sorted copy directly (PBF is
   already id-ordered); spill (cell, hilbert, row) to local scratch for the
   spatial copy.
2. Ways: stream, look up node coordinates from the local scratch node table
   (this is the one place a local random-access store is used, and it is
   thrown away afterwards), build geometry/bbox/cell, write both copies,
   emit `node_way_index` pairs to scratch.
3. Relations: same, plus `member_index`.
4. Sort and write the index tables, derive `area` and `tag_stats`, write
   the first manifest.

DuckDB can do most of the sorting and Parquet writing with spilling; the
geometry assembly and PBF decoding are Rust. Output can go straight to R2
or be synced afterwards with `rclone`.

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
- **Sleep policy.** Initially a short `sleepAfter` (on the order of a minute
  or two): with one user, paying a cold start now and then is cheaper than
  paying for an idle instance. As traffic grows, lengthen it and keep one
  instance warm during busy hours; that is a configuration change on the
  Durable Object that manages the pool, not a design change.
- **Public endpoint hygiene.** Overpass-style per-IP concurrency slots and a
  daily quota in the Worker's rate-limiting binding, hard caps on `timeout`
  and `maxsize`, the ODbL attribution line in every response, and `/api/status`
  reporting slots and the current data timestamp as clients expect.
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
| Current dataset on R2: spatial copy (nodes ~100 GB, ways ~150-200 GB, relations, areas) plus id-sorted copy plus `node_way_index`/`member_index` | ~600-900 GB → ~$9-14/month at $0.015/GB-month; egress $0 |
| History dataset (Iceberg) | ~0.5-1.5 TB once complete with minor versions → ~$8-25/month |
| R2 operations | Class B (reads) ~$0.36/M, Class A (writes) ~$4.50/M. A typical query issues 10-200 range requests → roughly $0.01-0.07 per 1,000 queries. The updater's minutely rewrites are a few hundred writes per hour. |
| Container compute | `standard-3` active: 8 GiB × $0.0000025/GiB-s + 2 vCPU × $0.00002/vCPU-s + 16 GB × $0.00000007/GB-s ≈ $0.00006 per active second, i.e. ~$0.22 per active hour; a 2-second query ≈ $0.00012. Workers Paid plan $5/month includes 25 GiB-hours memory and 375 vCPU-minutes. Sleeping instances cost nothing. |
| Crossover | At sustained load (say 100k two-second queries per day ≈ 55 active hours/day) serverless is ~$12/day, and a fixed VM pool becomes cheaper. The design does not care: the same container image runs on a VM. |
| Updater container | One run per minute, active for perhaps 15-40 s on `standard-3`: roughly $0.02-0.04 per hour active-equivalent → ~$20-45/month, plus a few thousand R2 reads per minute (~$1-2/month). Weekly compaction on `standard-4` for a few hours adds a few dollars. |
| Initial load | A rented big node for a day, or a laptop and patience; one-off. |
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
| The stateless updater cannot finish a minute's diff inside a minute | Measure on real diffs in M2 with a cold and a warm cache; raise concurrency; run every 2-3 minutes if needed; as a last resort give the updater a persistent volume on another provider (the code path is the same, the cache just never empties). |
| A generation's static indexes miss ways/relations created since | Every run builds an in-memory inverted index from the delta ways/relations before computing parents (4.2 step 1). |
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

**M1: planet-capable builder and layout v2** (done on Minnesota; see
`docs/m1-report.md`)
- Rust producer with a `dense-file` node store for the planet, layout v2
  (loose placement restricted to depths {0,3,6,9,12}, row-group index side
  files, tuned Parquet encodings and byte-sized row groups), engine caching
  and R2 credentials. The planet run itself follows `docs/m1-runbook.md`
  on the user's machine; its numbers replace the extrapolations.

**M2: minutely updates**
- Stateless updater in a scheduled container: rolling deltas, manifests,
  Durable Object scheduler; run for a month and measure per-run wall clock,
  lag behind planet.osm.org, R2 operation counts and compaction cost.

**M3: public beta**
- Tier-2 features (areas, around, poly, is_in, changed/newer/user,
  foreach/if), rate limits, status endpoints, monitoring, docs.

**M4: history**
- Regional history extract → Iceberg history table → `[date:]`/`retro`/
  `timeline`/`diff`/`adiff`; then the full-history planet load.

**M5: extras** — CSV/popup/custom outputs, evaluators/`make`/`convert`,
DuckDB-Wasm browser mode.

## 11. Open questions

1. **Run cadence for the updater.** Every minute is the goal; whether a
   stateless run fits in a minute on real diffs is the first thing M2
   measures. Every 2-3 minutes is the fallback.
2. **Where compaction runs.** A big scheduled container, or the same node
   used for the initial load, whichever is cheaper once we know the touched
   cell ratio per week.
3. **Bucket publishing.** Whether and when to make the bucket public so others
   can run the engine (including the Wasm mode) against it, alongside the
   hosted endpoint.
