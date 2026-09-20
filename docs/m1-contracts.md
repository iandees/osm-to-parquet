# M1 contracts

M1 goal: a builder that can do the planet on one big machine, plus the
layout changes the M0 report asked for, with the engine adapted and the
whole thing re-validated on Minnesota. The planet run itself happens on the
user's machine (see `docs/m1-runbook.md`); this sandbox validates on
Minnesota (55M nodes) and Bermuda.

Everything in `docs/m0-contracts.md` still holds unless overridden here.
Section numbers below are new.

## 1. Division of work

```
PBF ──► osmpq-raw (Rust) ──► raw/ (Parquet) ──► osmpq build --raw (Python + DuckDB) ──► dataset root
```

- **`osmpq-raw`** (Rust, `rust/osmpq-raw/`): everything that must touch every
  node: leaf-cell selection, node files (both copies), way geometry assembly
  and way files (both copies), relation raw parts, and the node_way index.
  Streams; memory bounded by the node location store choice (section 3).
- **`osmpq build --raw <rawdir> <root>`** (Python, DuckDB): everything that is
  small or relational: relation bbox/cell/hilbert and files, member index,
  row-group index side files, manifest v2. Also `osmpq raw-py <pbf> <rawdir>`:
  a Python/DuckDB producer of the same `raw/` layout (from the M0 builder
  code) so the Python stages can be developed and tested without the Rust
  binary; it is allowed to leave untagged-node metadata NULL.
- **Engine**: reads manifest v2 (and still v1), uses the row-group index and
  ancestor-depth rule, and takes metadata for untagged nodes from the data.

## 2. Cell rules v2

Unchanged: quadkey cells, leaf selection by node count, node placement.

**Loose placement is restricted to leaf cells and ancestor depths in
`ancestor_depths = [0, 3, 6, 9, 12]`.** Rule for a way/relation with bbox B:
descend from the root while exactly one child fully contains B and the
current cell is not a leaf; call the result C at depth d. If C is a leaf, use
C. Otherwise use the ancestor of C at the greatest depth in `ancestor_depths`
that is ≤ d (depth 7 → 6, depth 2 → 0, depth 12 → 12). The manifest records
`ancestor_depths`; the engine's `cells_for_bbox` for ways/relations is:
leaves intersecting the bbox, plus their ancestors whose depth is in
`ancestor_depths`, filtered to cells present for the table. (For manifest
v1 the engine keeps using all ancestors.)

Max leaf depth is 13 (parameter `--max-depth`, default 13).

## 3. `osmpq-raw` (Rust)

```
osmpq-raw build <input.osm.pbf> <rawdir>
    [--max-nodes-per-cell 1000000] [--max-depth 13] [--threads N]
    [--promoted-keys amenity,shop,...] [--node-store auto|sorted-mem|dense-file]
    [--flat-nodes PATH] [--tmpdir DIR] [--bbox S,W,N,E]
osmpq-raw node-way-index <rawdir>      # optional separate pass, writes rawdir/node_way/
```

Passes:
1. **Histogram**: read nodes once, count per depth-`max_depth` quadkey (dense
   `u32` array of 4^13 = 67M entries), select leaves exactly as M0 did
   (split while count > max-nodes-per-cell and depth < max-depth). Write
   `rawdir/leaves.json` (`{"max_nodes_per_cell": N, "max_depth": 13,
   "leaves": ["0213", ...]}`). With `--bbox`, nodes outside the bbox are
   ignored here and everywhere below, and ways/relations are kept with the
   same "smart" semantics as M0 (a way with at least one node in the bbox
   keeps all its nodes; a relation with at least one kept member).
2. **Nodes**: read nodes again; for each node compute `cell` (leaf key),
   `hilbert`, and write:
   - `rawdir/node/part-NNNNN.parquet` (byid copy): id-ordered as in the PBF,
     columns exactly as the M0 byid node schema **plus** metadata for every
     node (`version, changeset, timestamp, uid, "user"`), promoted columns,
     `cell`, and `hilbert UBIGINT`. Parts of at most 4,000,000 rows; row
     groups of about 64k rows.
   - the node location store (section 3.1),
   - per-cell spill files under `--tmpdir` (append-only, binary), one per
     leaf; buffered so that memory stays around 64 KB × leaves.
   Then, per leaf (parallel over leaves): sort the spill by `(hilbert, id)`
   and write `rawdir/spatial/node/cell=<cell>/tagged=true/part-0.parquet` and
   `.../tagged=false/part-0.parquet` with the M0 spatial node schema plus
   metadata columns in **both** partitions.
3. **Ways**: read ways; resolve each ref through the location store; compute
   `xmin_e7..ymax_e7`, `geometry` (LINESTRING as WKB bytes; NULL if fewer
   than 2 resolved nodes), `is_closed`, `is_area` (M0 rule), centroid, `cell`
   (section 2 rule against the leaves), `hilbert` (bbox center). Write:
   - `rawdir/way/part-NNNNN.parquet` (byid copy, id order): M0 byid way schema
     (no geometry) plus `hilbert`; parts of at most 1,000,000 rows, row
     groups about 1 MB (≈ 8k rows).
   - per-cell spill, then per cell sorted by `(hilbert, id)`:
     `rawdir/spatial/way/cell=<cell>/part-0.parquet` with the M0 spatial way
     schema, `geometry` written as Parquet **native GEOMETRY** (logical type
     `GEOMETRY` with CRS OGC:CRS84, WKB encoding; the `parquet` crate ≥ 56
     supports the Geometry logical type; if the writer cannot express it,
     write a BYTE_ARRAY column named `geometry` holding WKB and add the
     GeoParquet `geo` key-value metadata to the file footer so DuckDB reads it
     as GEOMETRY. Verify with DuckDB: `DESCRIBE SELECT * FROM read_parquet(f)`
     must show `GEOMETRY` for the column.)
   - Row groups of about 1-2 MB in spatial way files (≈ 10k rows).
4. **Relations**: write `rawdir/relation/part-NNNNN.parquet` (id order):
   `id, members STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]` (type in
   `n`/`w`/`r`), `tags, <promoted...>, <meta...>`. No bbox/cell (DuckDB stage
   computes them).
5. **`rawdir/summary.json`**: counts per type, `max_timestamp`, data bbox,
   promoted keys, timings per pass, and the location store used.

Parquet details for everything Rust writes: ZSTD, dictionary encoding,
statistics on. `tags` is an Arrow `Map<Utf8, Utf8>` (Parquet MAP) so DuckDB
reads `MAP(VARCHAR, VARCHAR)`; NULL when the element has no tags.
`timestamp` is `Timestamp(Microsecond, None)` in UTC. `version INT32,
changeset INT64, uid INT32, "user" VARCHAR`. Coordinates INT32 `*_e7`. Ids
INT64. `hilbert UINT64`. Column names and order exactly as the M0 schemas.

### 3.1 Node location store

`--node-store auto` picks `sorted-mem` when the pass-1 node count is below
`--sorted-mem-max` (default 400M); above that, it picks `dense-file` when
`max_id + 1 <= 4 * node_count` (id density >= 25%) and `sorted-file`
otherwise. The threshold compares each mode's disk footprint
(`8*(max_id+1)` vs `16*node_count`) rather than always preferring one --
see the code comment at the selection site in `main.rs` for the derivation.

- `sorted-mem`: a `Vec<(i64, i32, i32)>` appended in id order during pass 2,
  binary-searched in pass 3. 16 bytes per node; Minnesota ≈ 0.9 GB.
- `dense-file`: a file of `[i32 lat_e7, i32 lon_e7]` indexed by node id
  (`--flat-nodes`, default `<tmpdir>/nodes.flat`), written with sparse
  seeks, read through mmap in pass 3, the same approach as osm2pgsql's
  `--flat-nodes` and osmium's `dense_file_array`; needs a machine whose page
  cache can hold most of it. An unset entry (both zero, or a sentinel
  `i32::MIN`) means missing. Sized `8*(max_id+1)` bytes -- correct only
  when density is high enough that most 4 KiB blocks end up touched anyway
  (see the `sorted-file` note below for why "sparse" is not actually
  accurate at any density this project has measured).
- `sorted-file`: same sorted `(id: i64, lat_e7: i32, lon_e7: i32)` layout
  and append-in-id-order/binary-search approach as `sorted-mem`, but
  backed by an mmap'd file (`--flat-nodes`, same flag `dense-file` uses)
  sized `16 * node_count` bytes -- the pass-1 histogram's exact count, not
  the id range -- instead of a `Vec` held in process RAM. For a country
  extract (global OSM ids scattered thinly across the full id range, not
  clustered), `dense-file`'s nominal sparse-file size is dominated by
  `max_id`, which can be far larger than the actual node count; `dense-file`
  is also not meaningfully sparse *on disk* at any density found here --
  with ids landing roughly uniformly, the probability that a 4 KiB block
  (512 entries) stays untouched is `(1-density)^512`, which is
  indistinguishable from zero at both whole-US density (~11.2%: 1.6B
  nodes, max id 14.2B) and planet density (~70%: ~10B nodes, similar max
  id) -- i.e. `dense-file` ends up fully realized on disk either way, and
  its "sparse" framing in this doc and `docs/m1-runbook.md` is inaccurate;
  it is still the right planet-scale choice because its *nominal* size
  wins outright at that density (no id stored per entry), not because it
  stays sparse. Found and fixed after `--node-store auto` picked
  `dense-file` for a real whole-US build (Geofabrik's combined US extract,
  1.596B nodes, max id 14.2B) and tried to allocate a ~113.6 GB file,
  filling a 107 GB-free disk in ~14 minutes; `sorted-file` needs ~25.6 GB
  for the same input. See `docs/progress.md` for the full incident and
  validation notes.

### 3.2 Spill and sort

Spill records are fixed-size binary rows; ways additionally carry their WKB
and tag payload, so way spills are variable-length (length-prefixed). Per
cell, the sort happens in memory (a leaf holds ≤ max-nodes-per-cell nodes);
if a way cell exceeds a few GB (root-level cells can), sort in chunks and
merge. Use `rayon` for the per-cell stage with a `--threads` bound.

Implementation note (not a deviation, just specifics the contract leaves
open): the chunked path triggers per cell once its spill file is ≥ 512 MiB
on disk (`ways.rs`'s `WAY_CHUNK_SORT_THRESHOLD_BYTES`, chosen with headroom
for decoded `WaySpillRow`s running several times larger than their encoded
bytes -- see the comment there), below which the simple in-memory path
above still runs unchanged. Above it, the spill file is streamed into
sorted "run" files (same length-prefixed `WaySpillRow` encoding, ~512 MiB
of encoded rows per run) written alongside it in `<tmpdir>/spill/way/`,
then merged with a `BinaryHeap`-based k-way merge that holds one decoded
row per run at a time -- bounded by run count, not total row count. This is
still per-cell, sequential work inside the existing `rayon` per-cell
closure, so it adds no parallelism beyond the `--threads` bound already in
place for the per-cell stage.

### 3.3 node_way index

`osmpq-raw node-way-index <rawdir>`: reads `rawdir/way/part-*.parquet`,
emits `(node_id, way_id)` pairs into 256 buckets by `node_id >> 26`
(covering ids up to 2^34), sorts each bucket, writes
`rawdir/node_way/part-NNNNN.parquet` sorted by `(node_id, way_id)` with parts
of at most 8,000,000 rows and `min_id/max_id` recorded in
`rawdir/node_way/parts.json`. Planet: ~13B rows, ~200 GB of temp; the run
book warns about it. Optional in M1; the engine no longer needs it on the
query path and the updater (M2) will.

## 4. Layout v2 (dataset root)

Same paths as M0 with these changes:

- `byid/<gen>/node/` = the raw node parts, moved or copied as is (they
  already carry `cell` and `hilbert`).
- `byid/<gen>/way/` = raw way parts as is. `byid/<gen>/relation/` rewritten
  by the Python stage with bbox and cell.
- `spatial/<gen>/node|way/` = raw spatial files as is; `spatial/<gen>/relation/`
  from the Python stage.
- `index/<gen>/node_way/` from `osmpq-raw node-way-index` when present;
  `index/<gen>/member/` from the Python stage.
- **New**: `index/<gen>/rowgroups/{node,way,relation}.parquet`, one row per
  Parquet row group of the spatial files: `path VARCHAR, cell VARCHAR,
  tagged BOOLEAN (nodes only, NULL otherwise), rg INTEGER, rows INTEGER,
  xmin_e7 INTEGER, ymin_e7 INTEGER, xmax_e7 INTEGER, ymax_e7 INTEGER` (for
  nodes the min/max of `lon_e7`/`lat_e7`). Built by the Python stage from the
  Parquet footers (pyarrow). Sorted by `path, rg`.
- Untagged node partition has metadata columns (NULL allowed when the
  producer had none).

## 5. Manifest v2

As v1 plus:

```json
"manifest_version": 2,
"ancestor_depths": [0, 3, 6, 9, 12],
"max_depth": 13,
"rowgroup_index": {"node": "index/g0001/rowgroups/node.parquet",
                   "way": "index/g0001/rowgroups/way.parquet",
                   "relation": "index/g0001/rowgroups/relation.parquet"},
"producer": {"raw": "osmpq-raw 0.1.0" | "osmpq raw-py", "build": "osmpq 0.1.0"},
"stats": {"nodes": 0, "tagged_nodes": 0, "ways": 0, "relations": 0, "leaf_cells": 0,
          "bytes": {"spatial": 0, "byid": 0, "index": 0}}
```

`index.node_way` may be an empty list when the index was not built.

## 6. Engine changes

- Accept `manifest_version` 1 and 2. For 2: `cells_for_bbox` for ways and
  relations uses `ancestor_depths` (section 2); the row-group index, loaded
  once per Engine (one small Parquet read per table, cached in memory as a
  DuckDB table or pandas frame), filters the candidate files to those with at
  least one row group intersecting the query bbox before any `read_parquet`
  call; `Result.stats` reports files considered vs files read.
- Untagged node rows may carry metadata; `out meta` after `>` must output it
  when present.
- Nothing else in the query semantics changes. The harness must stay at
  54/55 gradable entries on Minnesota built through both producers.

## 7. Builder v2 CLI

```
osmpq raw-py <input.osm.pbf> <rawdir> [--bbox ...] [--max-nodes-per-cell N] [--max-depth 13] [--promoted-keys ...] [--threads N] [--memory-limit ...] [--tmpdir DIR]
osmpq build --raw <rawdir> <root> [--generation g0001] [--timestamp ...] [--replication-sequence N] [--threads N] [--memory-limit ...] [--tmpdir DIR] [--copy|--move|--link]
osmpq manifest <root>
osmpq validate <root>       # checks every manifest path exists, row counts match, sorted-ness, rowgroup index coverage
```

`osmpq build <pbf> <root>` (M0 form) keeps working by running `raw-py` into
`--tmpdir` and then `build --raw`. `--link` hardlinks raw files into the
root (default when on the same filesystem), `--copy` copies, `--move` moves.

## 8. Validation targets on Minnesota

- Harness: 54/55 gradable entries, same set as M0.
- Profiler (downtown bbox): files read per simple bbox query ≤ 6 (was 9),
  ways `out geom` ≤ 30 requests (was 40), wizard queries ≤ 60 requests
  (was 43-107), no regression in seconds.
- `osmpq-raw` throughput on Minnesota reported per pass (nodes/s, ways/s),
  peak RSS, temp bytes, and the extrapolation to the planet (10B nodes,
  1.1B ways) written into `docs/m1-report.md`.
