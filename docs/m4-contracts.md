# M4 contracts: history (attic)

M4 goal: answer Overpass attic queries — `[date:]`, `retro`, `timeline`,
`[diff:]`/`[adiff:]`, an exact `(changed:a,b)` — from a **history dataset**
that the base builder creates and the minutely updater keeps appending to.
Validated on Minnesota against the public reference, for dates on or after
the extract's base timestamp (the only history a base extract plus diffs
can know).

M0–M3 contracts still hold. The current-state layout (base, deltas, areas)
is untouched; history is an additive dataset under `history/` and a new
manifest field. Queries without attic settings must emit byte-identical
SQL to M3 (no extra scans, no extra files).

### Design decision: Parquet under our manifest, not Iceberg (for now)

`docs/design.md` 4.5 planned an Iceberg table in R2 Data Catalog. M4
stores history as Parquet under the dataset's own manifest instead:

- the engine container reads history through the same `read_parquet` +
  manifest path as the current tables, with no catalog round trip;
- the M2 rolling-tier machinery (hour/day/week files, folded at
  compaction) already gives append-only writes that are cheap per minute;
- DuckDB can only write Iceberg through a REST catalog, which the
  development sandbox cannot exercise, so an Iceberg export stays a
  documented option for the analytical copy, not the query path.

`design.md` is updated at the end of M4 to say so.

## 1. Division of work and file ownership

Four workstreams run concurrently in separate worktrees. Each owns the
files listed; touching another workstream's file is allowed only for the
integration points named in its section.

| workstream | owns |
| --- | --- |
| W1 history builder | `src/osmpq/history/build.py` (new), `src/osmpq/history/ingest.py` (new), `src/osmpq/history/writer.py` (new), `src/osmpq/history/schema.py` (coordinator wrote it; W1 may extend), `src/osmpq/layout/manifest.py` (v5 `history` field, `all_paths`), `src/osmpq/cli.py` (`history build`), `src/osmpq/build/validate.py` (history checks), `tests/test_history_build.py`, `tests/test_history_ingest.py` |
| W2 engine attic | `src/osmpq/engine/attic.py` (new), `src/osmpq/engine/sources.py` (snapshot branches), `src/osmpq/engine/catalog.py` (history accessors, `SNAPSHOT` contextvar), `src/osmpq/engine/recurse.py` (snapshot backward path), `src/osmpq/engine/planner.py` (`check_settings`, statement dispatch for `Retro`/`Timeline`), `src/osmpq/engine/executor.py` (diff/adiff two-pass), `src/osmpq/engine/render.py`, `src/osmpq/engine/result.py` (actions, timeline), `src/osmpq/engine/metafilters.py` (`changed` with history), `src/osmpq/ql/parser.py` + `ast.py` (`retro`, `timeline`), `tests/fixtures/make_history_fixture.py` (new), `tests/test_attic_*.py` |
| W3 updater + compaction | `src/osmpq/update/updater.py` (history tier append), `src/osmpq/update/osc.py` (keep every version), `src/osmpq/build/compact.py` (fold history tiers), `src/osmpq/build/gc.py`, `tests/test_history_update.py`, `tests/test_history_compact.py` |
| W4 harness + corpus + data | `tools/difftest.py` (XML diff/adiff and timeline comparison), `tests/corpus/50–58_*.overpassql`, `tests/corpus/bboxes.json`, `tools/m4_dataset.sh` (new: builds the Minnesota history dataset), `docs/m4-report.md` (data and harness sections) |

The coordinator owns `docs/m4-contracts.md`, `src/osmpq/history/schema.py`,
`docs/overpass-ql-support.md`, `docs/design.md`, and the merge.

Common rules as in M3: Python 3.12, DuckDB 1.5.5, pytest under `tests/`,
no new dependencies. Commit in the worktree; do not push. Read the M2
updater contract (`docs/m2-contracts.md`) before touching the updater,
and `docs/m3-contracts.md` section 2 for planner hooks.

Reference probes taken by the coordinator live in
`/tmp/claude-0/-home-user-osm-to-parquet/d9d8896c-c4a1-5798-bd34-4be59080fbe3/scratchpad/m4probe/`
(`probe.py`/`probe2.py` are the queries; `*.xml`/`*.json` the answers).
Section 6 summarizes what they show; read the files before rendering.

## 2. History dataset

### 2.1 Rows

One row per **state** of an element: every version, plus a *minor
version* whenever a way's or relation's geometry changed because a member
moved while the element's own version did not. Nodes have no minor
versions.

A history row has exactly the columns of the same element type's
current-state row in the same copy (spatial or byid — see
`src/osmpq/update/updater.py` `SPATIAL_COLUMNS`/`BYID_COLUMNS`, i.e. the
base schema with promoted columns, bbox, `geometry` for spatial ways,
`is_closed`/`is_area`, `cell`, `hilbert`, meta columns) plus four columns
defined in `src/osmpq/history/schema.py`:

| column | type | meaning |
| --- | --- | --- |
| `minor` | INTEGER | 0 for the element's own version; 1, 2, … for geometry-only states of the same version, in time order |
| `valid_from` | TIMESTAMP | when this state began: the version's `timestamp`, or for a minor version the `timestamp` of the member change that caused it |
| `valid_to` | TIMESTAMP | when the next state began, or NULL when this is the latest known state **or the writer did not know** (tier files always NULL) |
| `visible` | BOOLEAN | false for a deletion or a "moved out of this cell" tombstone |

`version`, `timestamp`, `changeset`, `uid`, `user` are the element
version's own (a minor row repeats them). `valid_to` is an optimization
only: readers must never rely on it for correctness (section 3.1's QUALIFY
does the real work).

Tombstones: a deletion writes a row with `visible = false`, `version` of
the deleting version, `minor = 0`, `valid_from` = its timestamp, `cell` =
the cell of the previous state, everything else NULL (tags, refs, members,
geometry, bbox). When a state moves to a different cell than the previous
state, the previous cell additionally gets a `visible = false` row with
the new state's `version`/`minor`/`valid_from` — so a scan of the old cell
alone sees the element leave. The byid copy has no move tombstones (it is
not cell-scoped) but does have deletion rows.

Ordering rule for "the state at t" (section 3.1): `valid_from DESC,
visible DESC, version DESC, minor DESC`. `visible DESC` makes the new
state win over its move tombstone at the same instant when both cells are
scanned.

### 2.2 Files

```
history/<gen>/spatial/<type>/cell=<key>/part-<n>.parquet   # base history, cell-partitioned
history/<gen>/byid/<type>/part-<n>.parquet                   # base history, id-sorted
history/<gen>/tier/<hour|day|week>/<version>/<type>.spatial.parquet
history/<gen>/tier/<hour|day|week>/<version>/<type>.byid.parquet
```

- Base spatial files hold the rows whose `cell` is that cell, sorted by
  `hilbert, id, valid_from`; a cell may have several parts (≤ 64 MB
  each). Base byid files are sorted by `id, valid_from` and split at ≈
  4M rows like the current byid copy.
- Tier files hold the rows the updater appended since the last fold, all
  cells in one file (a `cell` column, like M2 delta tiers), sorted by
  `cell, id, valid_from` (spatial) and `id, valid_from` (byid). Tiers are
  **append-only**: the hour tier's new version = old hour rows + this
  run's rows; hour folds into day, day into week at the same boundaries
  as M2's deltas; compaction folds week/day/hour into base. Nothing is
  ever merged by id.
- No row-group index side files for history in M4.

### 2.3 Manifest v5

`manifest_version: 5` adds one key (absent = no history; every reader
treats absence as "attic unsupported"):

```json
"history": {
  "generation": "g0001",
  "since": "2026-09-19T00:21:52Z",
  "minor_versions": true,
  "spatial": {"node": {"<cell>": [{"path": "...", "rows": n, "bytes": n}]},
              "way": {...}, "relation": {...}},
  "byid": {"node": [{"path": "...", "min_id": n, "max_id": n, "rows": n, "bytes": n}],
           "way": [...], "relation": [...]},
  "tiers": {
    "hour": {"version": 3, "seq_from": n, "seq_to": n, "timestamp": "...",
             "rows": {"node": n, "way": n, "relation": n},
             "cells": {"node": [...], "way": [...], "relation": [...]},
             "files": {"node": {"spatial": "...", "byid": "..."}, "way": {...}, "relation": {...}}},
    "day": {...}, "week": {...}
  },
  "stats": {"rows": {"node": n, "way": n, "relation": n}, "minor_rows": {...}, "bytes": n}
}
```

`since` is the earliest instant the history is complete from: queries at
an earlier `t` are answered from what exists (the state current at
`since` looks like it always existed) and get a `remark`. Every path is
root-relative like the rest of the manifest; `Manifest.all_paths()` and
`gc._referenced_paths` include all of them. `history.generation` follows
the base generation at compaction and is otherwise independent.

## 3. W2: engine

### 3.1 Snapshot reads

`catalog.SNAPSHOT: ContextVar[Optional[datetime]]` (like `FILE_STATS`).
When set, `sources.current_rows` and `sources.byid_current_rows` ignore
their `base_files`/delta arguments and read history instead:

```sql
SELECT {project(cols)} FROM (
  SELECT * FROM (
    SELECT * FROM read_parquet([base history spatial files of `cells`], hive_partitioning=true, union_by_name=true)
     WHERE valid_from <= $t AND (valid_to IS NULL OR valid_to > $t)
    UNION ALL BY NAME
    SELECT * FROM read_parquet([tier spatial files]) WHERE cell IN (...) AND valid_from <= $t
  ) __v
  QUALIFY row_number() OVER (PARTITION BY id ORDER BY valid_from DESC, visible DESC, version DESC, minor DESC) = 1
) __h
WHERE visible AND ({where_sql})
```

(byid: the byid files, `id_pred_sql` inside the inner select, tag
predicate outside). The predicate must stay **outside** the QUALIFY: the
state at `t` is chosen first, then filtered. Everything above
`current_rows` (spatial builders, hydration, `>`, `out geom`, areas'
candidate reads via `current_base_table`) then works at `t` without
change. File lists for a cell come from `Manifest.history_spatial_files
(type, cells)` and `history_byid_parts(type)` (new accessors in
`catalog.py`; tier files are always included). `ctx.files_read` counts
them.

Backward recursion (`<`, `<<`, `(bn)`, `(bw)`, `(br)`) under a snapshot:
the `node_way`/`member` indexes are current-only, so `recurse.py` takes
the bbox-semijoin path unconditionally for ways (it already reads through
`current_rows`), and for relations scans relation history rows in the
ancestors-and-self cells of the source elements' cells with a
`list_contains`/`list_filter` on `members`. `delta_*_ids_by_*` helpers are
skipped under a snapshot.

Areas are not versioned: `area[...]`, `(area)`, `(pivot)`, `is_in`,
`map_to_area` use the current area tables under a snapshot (the reference
regenerates areas from current data too). Say so in a `remark`? No —
silently, as the reference does; document it.

`t` earlier than `history.since`: answer from what exists and add
`remark: "history starts at <since>; earlier dates return the earliest known state"`.
`t` later than `timestamp_osm_base`: same as now. Malformed date: the
reference's static error text (probe `date_bad_json`; `parse` error →
HTTP 400 like other parse errors).

### 3.2 Settings and statements

- `[date:"t"]`: `SNAPSHOT = t` for the whole program.
- `retro("t") { ... }` (parser + `ast.Retro(time_expr, body)`): body runs
  with `SNAPSHOT = t`, restored afterwards; sets assigned inside remain
  visible outside (Overpass sets are global). The argument is an
  evaluator expression in the reference; M4 accepts a string literal and
  `_.val`-style evaluator expressions the M3 evaluator already supports
  (evaluate with the M3 evaluator against the current default set when
  not a literal).
- `timeline(type, id[, version])` → `ast.Timeline`; result set of derived
  elements `{"type": "timeline", "id": k, "tags": {"reftype", "ref",
  "refversion", "created", "expired"}}` — one per **state** (own version
  and minor versions each get an entry, `refversion` repeats for minor
  ones); `expired` absent for the latest state. Confirm the exact tag
  set, id numbering and ordering against `m4probe/timeline_*.json`
  (probe2, may still be running when you start; check again before
  rendering). Rendered in JSON as an element of type `timeline`, in XML
  as `<timeline id=".."><tag .../></timeline>`. `out` of such a set prints
  them; `out count` counts them under `total` only.
- `(changed:"a"[,"b"])`, `(newer:"t")` with history present: an element
  qualifies if any history row for it has `a < valid_from <= b` (`b` =
  now when absent; deletions and minor versions count — the reference
  counts geometry changes). Implemented as a predicate over a per-query
  temp table of changed ids in the query's cells (spatial history files
  + tiers, `cell IN` the effective cells). Without history: M3 behavior.
- `[diff:"a"[,"b"]]` / `[adiff:"a"[,"b"]]`: the executor runs the whole
  program twice, `SNAPSHOT = a` then `= b` (`b` = now when absent),
  collecting the elements every `out` produced in each run, keyed by
  `(type, id)`; then emits actions: in `b` only → `create`; in `a` only →
  `delete`; in both with a different `(version, minor)` → `modify`;
  identical → omitted. The rendering is the reference's (section 6):
  XML `<action type="...">` with `<old>`/`<new>` wrappers, ordered as
  `out` orders elements at `b` (creates/modifies) with deletes in the
  order of `a`. JSON is **not** supported by the reference for diff
  modes (static error, probe `diff_json`); we render JSON anyway as
  elements `{"action": "create|modify|delete", "type", "id", "old":
  {element}, "new": {element}}` (`old` absent for create, `new` absent
  for delete) and document it as an extension. `out count` under diff:
  follow the probe `adiff_count_xml`. Differences between `diff` and
  `adiff` in the reference (probes `diff_xml` vs `adiff_xml` differ by
  one action; `diff_way_xml` vs `adiff_way_xml`): determine the rule from
  the files (hypothesis: `diff` ignores minor versions, `adiff` includes
  them) and implement whichever the data shows.
- `compare` is out of scope for M4 (parser keeps rejecting it).

### 3.3 Fixture and tests

`tests/fixtures/make_history_fixture.py`: extends the M0 fixture root
with a v5 `history` section built directly with DuckDB (like the M0
fixture): at least one node with 3 versions (tag change, move to another
cell, deletion), one way with 2 own versions and 2 minor versions (a
node move, a node move that changes the way's cell), one relation with a
minor version, and one element created after `since`. Tier files for the
"hour" tier holding the newest state of one element so the tier path is
exercised. Tests cover: `[date:]` at each boundary (`valid_from` exactly
equal to `t` → the new state), the move tombstone, `out geom` at `t`,
`>` and `<` at `t`, `retro`, `timeline`, `changed` ranges, `diff`/`adiff`
XML and JSON, dates before `since` (remark), no-history manifest
(unsupported error text unchanged), and the no-regression check: a
program without attic settings emits the same SQL as before (assert
`SNAPSHOT` is never set and no history file is read).

## 4. W1: history builder

`osmpq history build <root> --pbf <base.osm.pbf> [--osc <file|dir>]... |
--osh <full-history.osh.pbf>` adds `history/<gen>/` and a v5 manifest to
an existing dataset root whose current state is at the *end* of the
input stream (the caller guarantees that; `validate` checks it, section
4.3). Inputs:

- `--pbf` + `--osc`: the base PBF is the first state of every element
  (its `timestamp`/`version` are that state's own); the `.osc`/`.osc.gz`
  files (sorted by sequence number as in their names) supply later
  versions and deletions. This is how Minnesota is built (base extract +
  the regional minutely diffs since it).
- `--osh`: a full-history file (`osmium extract --with-history` output or
  Geofabrik's `-internal.osh.pbf`); every object carries `version`,
  `visible`, `timestamp`.

Both feed one ingest layer (`history/ingest.py`) producing three DuckDB
tables `hist_node_raw(id, version, timestamp, visible, changeset, uid,
user, tags, lat_e7, lon_e7)`, `hist_way_raw(..., refs)`,
`hist_relation_raw(..., members)`: **every version**, not deduplicated.
Read the base PBF the way `build/raw.py` does (`ST_ReadOSM` +
`osmium_read`, fast) and `.osc`/`.osh` with pyosmium (`osc.py` shows the
pattern; keep every version instead of last-wins).

Extent filter: the M2 rule (`docs/m2-contracts.md` section 2) applied per
version in time order: a node version is kept if inside the manifest
extent or the node id is already known; a way version if any ref is
known or the way is known; a relation version if any member is known or
the relation is known; a deletion if the id is known. Implement it in
SQL over the raw tables (no per-id loops).

### 4.1 States and minor versions

Computed in DuckDB SQL (`history/build.py`):

1. Nodes: one state per version. `valid_from = timestamp`, `valid_to` =
   next version's `timestamp`, `visible` from the input, `cell`/`hilbert`
   from the version's own position (the M1 v2 placement code in
   `layout/cells.py` / `build/common.py`).
2. Ways: own versions as above. Minor versions: for each way version
   `(id, version, valid_from, valid_to)` and each ref, every node version
   with `valid_from < ts < valid_to` (strictly inside the way version's
   window) is an event; distinct event timestamps, ordered, become
   `minor = 1, 2, …` with `valid_from = ts`. Every state's geometry,
   bbox, `is_closed`, `is_area`, centroid, cell, hilbert are computed from
   the node states valid at that `valid_from` (the node with the greatest
   `valid_from <= ts`; a node not yet existing or deleted contributes
   nothing, as in M0's "missing node" rule). A way state with fewer than
   two resolvable nodes keeps the row with NULL geometry/bbox and the
   previous state's cell (never dropped; `[date:]` must still find it).
3. Relations: own versions; minor versions from member way *states*
   (own and minor) and member node versions inside the window, bbox from
   member states valid at `valid_from`, nested relations one level as
   the base builder does. `members` is unchanged in a minor row.
4. Tombstones per section 2.1 (deletions from `visible = false` input
   versions; move tombstones by comparing each state's `cell` to the
   previous state's).
5. `valid_to` filled for every state that has a successor; NULL for the
   last.

Memory: the Minnesota base has 25M nodes; the way-minor-version join
must stream through DuckDB (`SET memory_limit`, `temp_directory` under
`--tmpdir`) rather than Python. Nodes without version history (only the
base PBF state) are the vast majority — make sure their rows cost one
pass, not a join per ref.

### 4.2 Writer

`history/writer.py` writes the layout of section 2.2 from a DuckDB table
per type with the section 2.1 columns, returning the manifest fragments
of section 2.3. It is also used by compaction (W3) to rewrite touched
cells, so its interface is `write_spatial(con, root, gen, type, table,
cells: Optional[set])` and `write_byid(con, root, gen, type, table)`.
The manifest v5 dataclass field (`Manifest.history: Optional[dict]`)
round-trips through `to_dict`/`from_dict`; `CURRENT_MANIFEST_VERSION`
becomes 5 only for manifests that carry history (a v4 manifest without
history stays v4 — the M2/M3 tests asserting versions must not change).

### 4.3 Validate and CLI

`osmpq validate` with history: every path exists; per type, the latest
visible state of every id present in the current byid copy matches the
current row's `version` (sample 10k ids, not all); no row has
`valid_from > valid_to`; `minor` rows share `version`/`timestamp` with
their `minor = 0` row; the byid and spatial copies have the same visible
row count. `osmpq manifest` prints the history summary.

Tests build tiny inputs with pyosmium's writer (`osmium.SimpleWriter`
can write `.osm.pbf`/`.osc` and history files) so the ingest paths are
real: a node moved twice, a way whose node moves (minor version), a way
whose node move changes its cell (move tombstone), a deletion, an object
outside the extent that must be dropped, and a `--osh` input equivalent
to a `--pbf --osc` input producing identical history rows.

## 5. W3: updater and compaction

### 5.1 Updater

After `_resolve` (m2 contract step 6), when the manifest has `history`,
the run also writes history rows to the **hour** history tier (folding
hour → day → week at the same boundaries as the delta tiers, appending
rather than merging):

- every resolved row (batch elements and touched parents) becomes a
  history row; a batch element's own version gets `minor = 0`,
  `valid_from = timestamp`; a touched parent whose version did not change
  gets `minor` = previous known `minor` + 1 (look it up in the tiers and
  the base byid history for that id; unknown → 1) and `valid_from` = the
  greatest `timestamp` among this batch's changed elements it depends on
  (member nodes/ways in the batch), falling back to the batch's max
  element timestamp;
- deletions and cell moves get the section 2.1 tombstones (`prev_cell`
  is already computed for the deltas);
- `valid_to` stays NULL in tier rows;
- catch-up batches (several diffs per run) currently keep only the last
  version per id (`osc.parse_batch`); change `parse_batch` to also return
  the full version list per type (`BatchResult.node_all` etc.) so the
  history gets every version's meta row — geometry for a superseded
  intermediate version is that of the final state (document this as the
  one approximation; it only matters when the updater is behind).

Tier file layout and manifest fields per sections 2.2–2.3; a run that
touches nothing writes no tier file. The `s3://` root path (M3 W4) must
work: tier files go through the store like delta tier files.

### 5.2 Compaction and gc

`osmpq compact` with history: fold every history tier into the base
history — rewrite the spatial parts of the cells the tiers touch (old
rows + tier rows, sorted; fill `valid_to` for rows that now have a
successor), rewrite the byid parts whose id range the tiers touch,
hardlink/copy the rest into the new generation (as the current tables
do), clear `history.tiers`, bump `history.generation`. `gc` keeps every
path in `history`. Use W1's `history/writer.py` (agree on its interface
through the contract, not by editing it; if it is not merged yet, write
against the section 4.2 signatures and a local stub).

Tests: the existing updater e2e (Bermuda PBF + hand-written `.osc`)
extended with a v5 root: after three runs crossing an hour boundary the
history tiers hold the expected rows (versions, a minor version for a
way whose node moved, a deletion tombstone, a move tombstone), then
`compact` folds them and `[date:]` at three instants (through the W2
engine when merged; until then assert on the rows directly) returns the
expected states.

## 6. Reference facts (from the probes)

1. `[date:]`, `retro`, `timeline`, `(changed:)` work with `[out:json]`;
   `[diff:]`/`[adiff:]` are XML-only on the reference ("static error:
   The selected output format does not support the diff or adiff mode").
2. XML diff shape (`diff_xml`, `adiff_xml`): after `<meta>`, a sequence
   of `<action type="create">` wrapping the element directly, `<action
   type="modify">` wrapping `<old>` and `<new>`, `<action type="delete">`
   wrapping `<old>` and (check the file) `<new>` with a `visible="false"`
   stub. Tag order inside elements is the reference's storage order —
   ours is alphabetical; the harness compares as sets.
3. In the probe both modes list nearly the same actions (258 vs 259
   `modify`); establish the rule from the files (section 3.2).
4. `[date:]` output carries the same envelope as a normal query (the
   `osm3s.timestamp_osm_base` is the server's current base). `out meta`
   prints the version's own `version`/`timestamp` (a minor state repeats
   its version's).
5. `(changed:a,b)` on a large bbox runs out of memory on the reference
   (2 GB); grade it on small bboxes only.
6. `retro` accepts a string literal and prints elements like a `[date:]`
   query (`retro_json`).

## 7. W4: harness, corpus and the Minnesota history dataset

- `tools/difftest.py`: run diff/adiff queries as `[out:xml]` on both
  sides and compare the action list semantically (type, id, action,
  old/new element sets, tags as sets, node refs, geometry within 1e-7);
  compare `timeline` results as sets of tag dicts. Everything else
  already compares as JSON.
- Corpus (bbox names in `tests/corpus/bboxes.json`; the reference must
  be asked with dates the Minnesota history covers, i.e. from
  `2026-09-19T00:21:52Z`): 50 `[date:]` node tags in a small bbox; 51
  `[date:]` ways `out geom`; 52 `retro` block with a difference against
  now; 53 `timeline` of a node changed since base; 54 `timeline` of a way
  with a minor version; 55 `(changed:a,b)` small bbox; 56 `[adiff:]`
  nodes `out meta`; 57 `[diff:]` ways `out geom`; 58 `[adiff:]` with
  recursion `(._;>;)`. Pick ids from the diffs applied (W4 finds real
  ids that changed by querying the reference with `adiff` on Minneapolis
  and Duluth bboxes).
- `tools/m4_dataset.sh`: hardlink-copy `minnesota-rs3-areas2` to
  `minnesota-h1`; fetch the osm.fr `us-midwest/minute` diffs from
  sequence 7292745 to the latest (the replication client in
  `src/osmpq/update/replication.py`; cache them under the scratchpad);
  `osmpq history build --pbf data/minnesota.osm.pbf --osc <dir>`; then
  `osmpq update --max-diffs 240` in a loop until the current state is at
  the same sequence (M2's updater, so `[date:now]` equals the current
  tables); record timings, row counts and bytes for the report.
- Grade corpus 01–58 on the history dataset (`--date` for the reference
  side stays the base timestamp for 01–49) and write the data/harness
  sections of `docs/m4-report.md` with the same tables as
  `docs/m3-report.md`.

## 8. Validation targets

- All existing tests pass unchanged (563), plus the new suites.
- `[date:t]` on Minnesota at `t` = base timestamp equals the M3 answer
  for corpus 01–49 (same elements), and at `t` = latest equals the
  current tables.
- Corpus 50–58 match the reference for elements, tags and geometry.
- A `[date:]` cafés-in-a-bbox query costs ≤ 2× the non-attic query's
  files and time on Minnesota; `adiff` over a day on a Minneapolis bbox
  finishes under 5 s cold.
- The history build for Minnesota (base + ~20 h of diffs) finishes in
  under 30 minutes in the sandbox (4 cores, 15 GB) and the history is
  ≤ 1.5× the base dataset's bytes.
