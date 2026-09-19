# M2 contracts: minutely updates

M2 goal: keep a dataset current from minutely replication diffs without a
local replication store, exactly as `docs/design.md` section 4 describes,
validated on Minnesota against the reference Overpass at the updated
timestamp. M0/M1 contracts still hold. Everything here is developed and
tested at Minnesota scale; the scheduler that runs it in a Cloudflare
container is M3.

## 1. Replication source

Osmosis-style directories: `<source>/state.txt` (latest) and
`<source>/AAA/BBB/CCC.state.txt` + `.osc.gz` for sequence `AAABBBCCC`.
For Minnesota use `https://download.openstreetmap.fr/replication/north-america/us-midwest/minute`
(regional diffs of the extract the data came from, planet-aligned sequence
numbers; the dataset manifest says `replication_sequence: 7292744`). The
planet source (`https://planet.openstreetmap.org/replication/minute`) has the
same layout. The manifest gains `replication_source` (URL string) and the
existing `replication_sequence` / `timestamp_osm_base` advance with every
applied diff.

Diff semantics (OsmChange): `create`/`modify`/`delete` blocks; pyosmium
exposes them with `obj.deleted` (True in delete blocks) and full element
data otherwise. Within one file the same id can appear more than once; the
last occurrence wins. Element versions only increase.

## 2. Extent filter

A regional dataset keeps only what is inside its `extent` (manifest bbox):

- a node change is kept if the node is inside the extent, or the node id
  already exists in the dataset (base ⊕ deltas) — a known node moving out of
  the extent is kept (and stays queryable), a node entering is created;
- a way change is kept if any of its nodes is known or inside the extent
  after applying this batch's node changes, or the way id already exists;
- a relation change is kept if any member is known or the relation id
  exists; members that are unknown stay in the member list and contribute
  nothing to geometry (M0 rule);
- deletions are kept if the id exists.

A planet dataset has the world as its extent, so the filter is a no-op.

## 3. Delta files

Under `delta/<gen>/<tier>/<ver>/`, tiers `hour`, `day`, `week`, version an
increasing integer per tier. Each tier version holds, per element type,
one **spatial** file sorted by `(cell, hilbert, id)` and one **byid** file
sorted by `id`, plus one **tombstone** file:

```
delta/g0001/hour/17/node.spatial.parquet   node.byid.parquet
delta/g0001/hour/17/way.spatial.parquet    way.byid.parquet
delta/g0001/hour/17/relation.spatial.parquet relation.byid.parquet
delta/g0001/hour/17/tombstones.parquet
```

Row schema of the spatial/byid delta files = the corresponding base
spatial/byid schema (M1 versions: metadata on all nodes, `hilbert` on byid
rows) **plus**:

| column | type | meaning |
| --- | --- | --- |
| `deleted` | BOOLEAN | true for a tombstone row; payload columns NULL |
| `prev_cell` | VARCHAR | the cell the element was stored in before this change (base or older delta); NULL if the element is new |
| `seq` | BIGINT | replication sequence that last touched the element |

For a deleted element the spatial row's `cell` = `prev_cell` (so it is
found when reading the old cell) and bbox/lat/lon columns are NULL. Every
delta file holds **at most one row per (type, id)** (newest wins when a
tier is rewritten). Untouched columns are never NULL-filled: a delta row is
the element's complete current state.

Conventions fixed during implementation: delta spatial files carry a
**stored `cell` column** for every type (base node spatial files get it from
the hive directory; delta files are flat). byid delta rows for node and way
carry `hilbert` like the M1 base parts; byid relation rows do not. Deleted
rows have every payload column NULL except `id`, and `cell = prev_cell`, in
both the spatial and the byid file. Tombstone `type` values are the full
names `node`/`way`/`relation`.

`tombstones.parquet`: `type VARCHAR, id BIGINT, prev_cell VARCHAR, seq BIGINT`
for every row in the tier whose `prev_cell` is not NULL and differs from
`cell`, or which is deleted. Sorted by `(prev_cell, type, id)`. This is what
lets a query on the *old* cell shadow a moved or deleted element without
scanning the whole delta.

Delta files use the same Parquet settings as the base (M1 contract 3), row
groups of about 1 MB; no row-group index side file (tiers are small).

Manifest v3 (`manifest_version: 3`) adds:

```json
"replication_source": "https://download.openstreetmap.fr/replication/north-america/us-midwest/minute",
"deltas": {
  "hour": {"version": 17, "seq_from": 7293001, "seq_to": 7293042, "timestamp": "...", "rows": {"node": 0, "way": 0, "relation": 0},
           "cells": {"node": ["0213", "..."], "way": ["021", "root"], "relation": ["root"]},
           "files": {"node": {"spatial": "delta/g0001/hour/17/node.spatial.parquet", "byid": "..."}, "way": {...}, "relation": {...}, "tombstones": "delta/g0001/hour/17/tombstones.parquet"}},
  "day": {...}, "week": {...}
}
```

A tier that is empty is absent from `deltas`. Engines must accept manifest
versions 1, 2 and 3 (`deltas` absent or empty means base only).

## 4. Engine read path with deltas

Tier precedence: `hour` > `day` > `week` > base. For a table T and query
cells C (from `cells_for_bbox`):

1. **Delta candidates**: rows of T's spatial delta file of each present
   tier with `cell IN C`, where C for the delta side is computed over the
   union of the base's cells and the tier's `cells` list (a new element may
   live in a cell the base has no file for) (Parquet pruning on `cell` min/max works because
   the file is sorted by cell; the engine may also prune by bbox columns).
   Rank tiers, keep one row per `(type, id)` by highest rank
   (`QUALIFY row_number() OVER (PARTITION BY id ORDER BY rank DESC) = 1`).
2. **Shadow set**: ids of all delta candidates ∪ ids from each tier's
   tombstones with `prev_cell IN C`. (An id shadowed by a tombstone in a
   newer tier but present with a payload in an older tier is still
   shadowed: newest tier wins.)
3. **Result**: base rows from C `WHERE id NOT IN shadow` ∪ delta candidates
   `WHERE NOT deleted`, both with the query's predicates applied.

By-id lookups (`node(123)`, recursion hydration via byid, updater fetches):
the same precedence using the byid delta files (`id` ranges are in the
Parquet stats) over the byid base parts. Cell-scoped hydration paths
(`>`, `<`, relation member ways) treat the delta spatial rows for the
chosen cells as additional inputs, with the shadow set applied to base.

Recursion inputs from deltas: the `node_way` and `member` base indexes do
not know about ways/relations changed since the base, so `<`-style lookups
that use the indexes must also scan the delta way/relation byid files
(small) for refs/members containing the ids. The cell-scoped `<` path
already scans way rows and only needs the delta rows added.

`Result.stats` gains `delta_rows` (delta candidates considered) and
`shadowed` (ids removed from base).

## 5. Updater

```
osmpq update <root> [--source URL] [--once | --follow] [--max-diffs N]
             [--tmpdir DIR] [--threads N] [--memory-limit ...]
```

One run (`--once`, default) applies up to `--max-diffs` (default 60)
consecutive diffs after the manifest's `replication_sequence`, as one
batch, and writes one new manifest. `--follow` loops: sleep until the
source's `state.txt` advances, then run again. The run is **stateless**:
everything it needs comes from the root and the source; `--tmpdir` holds a
scratch DuckDB database that is deleted at the end.

Algorithm per run:

1. Load the manifest; load the current delta tiers' byid files and
   tombstones into the scratch database (they are small); build an
   in-memory inverted index of delta ways' refs and delta relations'
   members (`delta_node_way(node_id, way_id)`, `delta_member(member_type,
   member_id, parent_id)`).
2. Fetch diffs `seq+1 ...` (stop at the source's current sequence or
   `--max-diffs`); parse with pyosmium into three change tables
   `(id, deleted, version, timestamp, changeset, uid, user, tags, lat/lon |
   refs | members)`, last occurrence per id wins; record the last diff's
   timestamp and sequence.
3. Apply the extent filter (section 2) using byid ⊕ delta lookups for
   "known" ids (batch semi-joins, never per-id queries).
4. Touched set: changed elements; plus parent ways of changed nodes
   (`node_way` base index parts covering the ids, ∪ `delta_node_way`);
   plus parent relations of changed nodes/ways/relations (`member` index ∪
   `delta_member`), iterated to a fixed point for nested relations.
5. Current state of touched ways/relations that are not themselves in the
   batch: from byid ⊕ deltas (cell-scoped reads are not available here, so
   byid parts by id range; this is the cost the design accepts). Node
   coordinates for all refs of touched ways: byid ⊕ deltas ⊕ this batch.
6. Re-resolve every touched way (bbox, LINESTRING, is_closed, is_area,
   centroid, cell v2, hilbert) and relation (bbox from member nodes/ways
   and nested relations, cell v2, hilbert) with the same rules and code as
   the builder (reuse `osmpq.layout.cells` and the builder's SQL where
   possible; ways with fewer than 2 known nodes get NULL geometry).
   `prev_cell` = the cell the element had (byid ⊕ deltas), NULL if new.
   Tombstones for deletions carry `prev_cell` and `cell = prev_cell`.
7. Rolling tiers: `hour' = merge(hour, batch)` (newest wins per id, keep
   the batch's `prev_cell` unless the older row's `prev_cell` is older
   still: the first `prev_cell` recorded for an id within a tier is kept,
   since the shadow must point at where the *base or lower tier* has the
   element). If the batch's last timestamp crosses an hour boundary (UTC)
   relative to the tier's `timestamp`, first fold `hour` into `day`
   (`day' = merge(day, hour)`, same prev_cell rule) and start `hour` from
   the batch alone; likewise `day` into `week` at a day boundary. Write
   new tier versions (never modify a published file), then the manifest.
8. Manifest: new `deltas`, `replication_sequence`, `timestamp_osm_base`
   (= the last applied diff's timestamp), `replication_source`; write
   `manifest/<n+1>.json` then `LATEST`. Old tier versions stay on disk until
   `osmpq compact` or `osmpq gc` removes files no manifest references.

Correctness rules: an element version already present (same version and
not newer) is not re-applied; a `create` for an existing id is treated as a
modify; a delete of an unknown id is ignored; a way whose refs include
nodes deleted in the same batch drops those nodes from its geometry (not
from `refs`, which stay as the diff says).

## 6. Compaction into the base

```
osmpq compact <root> [--generation gNNNN] [--tmpdir DIR] [--threads N] [--memory-limit ...]
```

Folds every tier into a new base generation:

- For each spatial table and each cell touched by any delta row or
  tombstone: rewrite the cell file as `base rows WHERE id NOT IN shadow ∪
  delta rows (newest tier) WHERE NOT deleted`, sorted by `(hilbert, id)`,
  same writer settings. Untouched cell files are hardlinked into the new
  generation.
- byid: rewrite only the parts whose `[min_id, max_id]` covers a touched
  id; hardlink the others. Deleted ids disappear.
- `node_way` and `member` indexes: rebuild the parts covering touched
  node ids / rebuild the member index (small) from the new state.
- Row-group index and manifest v3 with `deltas: {}`; the previous
  generation's files stay until `osmpq gc <root>` deletes files that no
  manifest among the last `--keep` (default 2) references.

## 7. Validation on Minnesota

1. `tools/diffcheck.py --root <root> --reference <overpass url> --date <T>`:
   for a sample of ids touched since the base (from the delta byid files;
   default 200 per type, all if fewer), query `node(id); out meta;` (way,
   relation likewise) locally and at the reference with `[date:"T"]` where
   T = `timestamp_osm_base`, and compare existence, version, tags, coords
   / refs / members. Deleted elements must be absent on both sides.
   Exit non-zero on mismatch; print a table.
2. The harness (`tools/difftest.py --date <timestamp_osm_base>`) still
   passes at the same 54/55 after applying diffs, and the profiler shows
   the delta overhead per query (extra files, requests, MB).
3. After `osmpq compact`, `osmpq validate` passes, the harness and
   `diffcheck` give the same results, and deltas are empty.

Target for the M2 report: apply at least 6 hours of Midwest diffs (≥ 360
files) to the Minnesota dataset in `--follow`-like batches, report per-run
wall time, rows per tier, delta bytes, diffcheck result, harness result,
profiler delta overhead, and compaction time.
