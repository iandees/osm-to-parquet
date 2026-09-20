# M4 report: history (attic)

Contract: `docs/m4-contracts.md`. Base dataset: the M3 Minnesota areas
dataset (`minnesota-rs3-areas2` in the shared scratchpad; 55.0M nodes,
4.6M ways, 59,798 relations; `timestamp_osm_base`
`2026-09-19T00:21:52Z`; `replication_sequence` 7292744; manifest v4).
Reference: `maps.mail.ru` (Overpass API 0.7.62.4).

Sections 2, 3 and the fetch/catch-up parts of 4 were written by W4 while
the other workstreams were in flight; the coordinator completed the rest
after integration. Everything below was measured on the merged branch
(683 tests passing).

## 1. What was built

| workstream | delivered |
| --- | --- |
| W1 history builder | `src/osmpq/history/{ingest,build,writer,schema}.py`: every version of a raw object stream (base PBF + `.osc`, or a full-history `.osh.pbf`, both through pyosmium) becomes states with minor versions, tombstones and `valid_to`; `osmpq history build`; manifest v5 `history` field; `osmpq validate` history checks; 24 tests on pyosmium-written fixtures. Its full-size Minnesota run did not fit the sandbox (section 4) |
| W2 engine attic | `src/osmpq/engine/attic.py` plus a `catalog.SNAPSHOT` context variable that turns `sources.current_rows`/`byid_current_rows` into history reads (state at `t` picked by a window function, then the query's own predicate); `[date:]`, `retro` (block-local sets, as the reference), `timeline` (own versions only, as the reference), exact `(changed:)`/`(newer:)`, `[diff:]`/`[adiff:]` as two snapshot passes with the reference's XML actions and a JSON extension; snapshot-aware backward recursion; 67 tests on a v5 fixture, including a no-regression test that ordinary queries read no history file |
| W3 updater + compaction | the updater appends every applied version, a minor row for every re-resolved parent, and deletion/move tombstones to append-only hour/day/week history tiers (folded at the same boundaries as the delta tiers, uploaded through the store for `s3://` roots); `osmpq compact` folds the tiers into the base history and fills `valid_to`; `gc` keeps history paths; 20 tests |
| W4 harness + corpus + data | `tools/difftest.py`: XML action-list comparator for `[diff:]`/`[adiff:]`, multiset-of-tags comparator for `timeline`, `--date` no longer injected into attic queries (section 2 below); corpus 50-58 (section 3); `tools/m4_dataset.sh` + `tools/m4_fetch_diffs.py` dataset recipe, diffs fetched, M2 catch-up timing measured (section 4) |
| coordinator | contract and shared `history/schema.py`; `osmpq history init` (section 4); history compaction rewritten to fold one byid part and one cell at a time (the first version pooled every byid part into one table and filled the disk); the exact relation member test made history-aware under a snapshot; the updater now keeps the manifest's `areas` section (an M3 bug: the first update run dropped it); batched `adiff` stub lookups; per-engine DuckDB temp directories; `tools/upload_root.py`; harness `--local-date`/`--history-since`; docs |

## 2. Harness: `tools/difftest.py` (docs/m4-contracts.md section 7)

### Diff/adiff comparison

A corpus entry containing `[diff:` or `[adiff:` is now graded differently
from every other entry: the reference rejects `[out:json]` in diff/adiff
mode with a static error (confirmed by the coordinator's probes,
`m4probe/diff_json.json` etc. return the error body), so both the
reference and the local server are asked for `[out:xml]`
(`force_out_xml()`), overriding whatever the entry declared.

The two XML bodies are parsed (`parse_diff_actions()`) into a list of
`{"action": "create"|"modify"|"delete", "type", "id", "old", "new"}`
dicts -- `old`/`new` are element dicts in the same shape
`parse_elements()` produces from JSON (tags as a dict, node `lat`/`lon`,
way `nodes` id list and, when the query asked for geometry, a `geometry`
point list), built directly from the XML by `_xml_child_to_element()`.
Probing real reference XML confirmed the exact shapes documented in
`docs/m4-contracts.md` section 6.2 and filled in the one thing that
section left an open question ("check the file"): a `delete` action has
**no** `<new>` at all when the element is genuinely gone (deleted from
OSM), but **does** carry a `<new>` stub (id/version only, no tags or `nd`
children, still `visible`) when the reason it dropped out of the result
is that a `(._;>;)` recursion pulled it in only as a referenced member
that no longer matches the outer filter at the later timestamp. Both
shapes round-trip through `parse_diff_actions()`/`compare_diff_actions()`
correctly (`tests/corpus/selftest.py` has a case for each), because a
stub's empty tags/`nodes=None` compare the same way "no data on this
side" already does in `compare_elements()`.

`compare_diff_actions()` compares the *set* of `(action, type, id)`
triples (missing/extra, same as the element-key comparator), then for
every triple present on both sides, compares its `old` and `new` element
pair with the exact same tolerances the JSON path uses
(`compare_elements()`, factored out via a new `_classify_element_problems()`
helper so both paths share one implementation).

Sanity check against the coordinator's cached reference responses in
`m4probe/`: `parse_diff_actions()` parses `diff_xml.xml` (410 actions: 133
create + 258 modify + 19 delete) and `adiff_xml.xml` (410: 132 + 259 + 19)
without error, `compare_diff_actions(actions, actions)` is `PASS` for
every probe file tried, and comparing `diff_xml.xml` against
`adiff_xml.xml` reproduces exactly the one-action difference
`docs/m4-contracts.md` section 6 calls out: node 11897640668 is a
`create` under `diff` and a `modify` under `adiff` (nothing else
differs).

### Timeline comparison

A `timeline(...)` query's results are compared as a **multiset of `tags`
dicts, ignoring the synthetic per-query `id`** (`compare_timeline()`,
dispatched from `compare()` whenever either side has a `"type":
"timeline"` element): the reference numbers timeline entries 1, 2, 3, ...
in emission order, which is not a stable identity to key on the way
`(type, id)` is for ordinary elements, but each entry's tags
(`reftype`/`ref`/`refversion`/`created`/`expired`) fully describe the
state.

### `--date` and attic queries

`--date` (inserted on the reference side to pin corpus 01-49, which know
nothing about attic settings, to the dataset's base timestamp) is now
skipped for any query that already carries its own attic time setting --
`[date:]`, `retro(...)`, `timeline(...)`, `[diff:]`/`[adiff:]`
(`should_apply_date()`); a plain `(changed:a,b)` query (corpus 43 and the
new 55) is unaffected and still gets `--date`, exactly as in M0-M3.

### `tests/corpus/selftest.py`

Extended with checks for `should_apply_date`, `is_diff_variant`,
`force_out_xml`, the timeline multiset comparison (same states under
different synthetic ids PASS, a missing state FAILs), and
`parse_diff_actions`/`compare_diff_actions` against hand-written XML
fixtures covering `create`/`modify`/`delete` and the `delete`-with-a-stub
case. All checks pass (`python tests/corpus/selftest.py`).

## 3. Corpus 50-58

All new entries hardcode their bbox as literal coordinates (not
`{{bbox}}`) because the harness's default bbox selection (first entry in
`bboxes.json` unless the file is in `ALL_BBOX_PATTERNS`) does not let a
single corpus file request a *specific* bbox -- several of these entries
depend on a bbox actually containing the real edit(s) they were built
around, so a silent fallback to the wrong bbox at grading time would be
a bug, not just a missed opportunity. Each names the bbox it uses (from
`tests/corpus/bboxes.json`, three of them new: `minneapolis_wide`,
`duluth_wide`, `minneapolis_eatery_cluster`) in a leading comment.

Real ids/bboxes for 53-58 were found by querying the reference directly
with `[adiff:"2026-09-19T00:21:52Z","2026-09-19T20:00:00Z"]` (the
Minnesota history's base timestamp `A` to the contract's upper bound `B`)
over `downtown_minneapolis`, `duluth_harbor`, and two wider boxes
(`minneapolis_wide`, `duluth_wide`) -- reusing the coordinator's cached
probe answers in `m4probe/findids_*.xml` where the range already matched,
one fresh call apiece otherwise.

| # | query | bbox | id(s) chosen and why |
| --- | --- | --- | --- |
| 50 | `[date:"2026-09-19T06:00:00Z"]` node tags | `tiny` | no specific id needed; a snapshot read |
| 51 | `[date:"2026-09-19T06:00:00Z"]` ways `out geom` | `downtown_minneapolis` | no specific id needed |
| 52 | `retro("2026-09-19T06:00:00Z")` block | `minneapolis_eatery_cluster` (new, `[44.9470,-93.2545,44.9490,-93.2520]`) | a cluster of restaurants around (44.948,-93.253): node 11899155031/11899155032 modified and several nodes (14197301589, 14197341210, 14197386267, ...) created between 19:34 and 19:51 UTC on 2026-09-19, all well after the retro date, so the snapshot is guaranteed to differ from now -- see the finding below about how this entry's first draft was wrong |
| 53 | `timeline(node,...)` | none (static id) | node **973257881**: version 2 -> 3 inside the window (found via `findids_minneapolis_wide.xml`) |
| 54 | `timeline(way,...)` | none (static id) | way **69963816**: own version stayed 4 (`modify` action, same version, in `findids_minneapolis_wide.xml`) while member node 7851834327 moved by about 3e-6 deg -- a real minor-version case |
| 55 | `(changed:a,b)` | `tiny` | reference unavailable (section 6.5); query kept, no reference call made |
| 56 | `[adiff:]` nodes `out meta` | `minneapolis_wide` | unfiltered `node(bbox)`, needed for real node creates/modifies/deletes in range -- `downtown_minneapolis` alone has *zero* node actions in this window |
| 57 | `[diff:]` ways `out geom` | `downtown_minneapolis` | unfiltered `way(bbox)`: exactly the 1 modify (27346594, v18->19) + 1 delete (1558225576) already visible in `findids_downtown_minneapolis.xml` |
| 58 | `[adiff:]` with `(._;>;)` | `downtown_minneapolis` | `way["building"](bbox)` -- way 27346594 (`building=parking`, "Marquette Parking Ramp") is tagged `building` and has real changes in range, so this exercises the recursion path adiff needs member ids for |

### Finding: `retro()`'s named-set assignments do not leak outside the block

The contract's first design for corpus 52 followed
`docs/m4-contracts.md`'s own probe pattern (`retro_mixed_json`):

```
node(bbox)[amenity]->.now;
retro("t"){ node(bbox)[amenity]->.then; }
(.now; - .then;);
out meta;
```

Section 3.2 says retro's "sets assigned inside remain visible outside
(Overpass sets are global)". Testing this directly against the
reference:

```
[out:json];retro("2026-06-01T00:00:00Z"){ node(B)[amenity=cafe]->.then; } .then; out count;
```

returns `count: 0` for every element type -- `.then` is empty once read
*outside* the `retro()` block, on the live reference. That means the
`(.now; - .then;)` pattern above silently degenerates to `.now - {} =
.now`: the coordinator's own `retro_mixed_json` probe (29 elements) and
`date_json` probe (also 29, the plain "now" set) are identical in count,
and my first draft of corpus 52 against `minneapolis_wide` returned
11,049 elements -- essentially the full "now" set, not a small
difference. **This probably means the contract's "sets assigned inside
remain visible outside" claim does not hold for `retro()`
specifically** (it may hold for other blocks like `foreach`/`if`, which
this probe didn't test) -- W2 should confirm before relying on it, and
should not implement `retro()` to leak its interior sets if it wants
`out`side reads of them to match the reference. Corpus 52 was rewritten
to a plain `retro(t){ ...; out meta; }` snapshot at a date proven (from
the adiff probe) to actually differ from "now", which is a real,
narrower test of the same feature.

### Finding: the reference's `timeline()` does not split minor versions (confirmed; the engine lists own versions only)

Section 3.2's hypothesis is that a way's minor (geometry-only) states get
their own `timeline()` entries with a repeated `refversion`. Way
69963816's own version stayed 4 across the window while a member node
moved (a `modify` action with equal old/new `version`, confirmed via
`findids_minneapolis_wide.xml`) -- exactly the case that should produce a
minor entry. Querying the live reference directly
(`timeline(way,69963816)`, cached as
`m4probe/w4_timeline_way_69963816.json`) returns only 3 entries
(refversion 2, 3, 4), with refversion 4 shown as a single ongoing state
(`created` only, no `expired`) -- no second entry for the node move.
Corpus 54 still exercises this exact case (per the contract's
instruction), but W2 should be aware the reference may simply not split
`timeline()` by minor version at all (only `diff`/`adiff` might, per
section 3.2's other hypothesis) before deciding what the engine's
`timeline()` should emit for a way like this.

## 4. Data: the Minnesota history dataset (docs/m4-contracts.md section 7)

### Diff fetch

`tools/m4_dataset.sh fetch` (`tools/m4_fetch_diffs.py`, using
`ReplicationClient` with retries/backoff) against
`https://download.openstreetmap.fr/replication/north-america/us-midwest/minute`:

| | |
| --- | --- |
| range fetched | sequence 7292745 .. 7294047 (the source's latest at fetch time) |
| diffs fetched | 1,303 (`.osc.gz` + `.state.txt` pairs), 0 already cached |
| total size | 12 MB (`osc-midwest/`) |
| wall time | 324.8 s (about 5.4 minutes) for all 1,303, i.e. ~4 diffs/s including retries |
| destination | `$SCRATCHPAD/osc-midwest/` (kept outside the repo per the contract's scratch rule) |

The fetch is safe to re-run (skips files already on disk) to top up to a
later "latest" before the history build runs.

### History build: what worked and what did not

**`osmpq history build --pbf --osc` (W1) does not fit this sandbox at
Minnesota scale.** Six attempts, each after a real memory fix (views
instead of tables, dropping the 55M-row raw node table early, a narrow
node-state table for the way joins, a window frame DuckDB 1.5.5 does not
spill replaced by `LAG`), all died at the same 14 GB cgroup ceiling
while materializing every node version for cell assignment through
Python. The ingest itself works (59.8M object versions from the base PBF
+ 1,200 diffs in 20-25 minutes, extent filter to 55.0M nodes / 4.47M
ways / 53.6k relations). The code is correct on its fixtures and remains
the path for a full-history `.osh.pbf`; it needs the node pass done in
bounded id ranges before a planet-sized run.

**`osmpq history init` + the M2 updater is what built the dataset.** The
current tables at the base timestamp *are* the first state of every
element, so `history init` streams them file by file into history rows
(`minor = 0`, `valid_from = timestamp`), and every later `osmpq update`
run appends the versions, minor versions and tombstones the contract
asks for. No PBF is re-read and memory stays flat.

| step | result |
| --- | --- |
| `osmpq history init minnesota-h2 --threads 4 --memory-limit 6GB` | 70 s; 54,996,422 node / 4,630,269 way / 59,798 relation states; 2.79 GB (history/base is 1.2x the base dataset's 2.3 GB) |
| catch-up: `osmpq update --max-diffs 60` x 22 batches, sequences 7292745..7294064 (1,320 diffs, 22.4 hours of edits) | 192 s in total, 6-12 s per batch; history tiers 7.3 MB |
| `osmpq validate` (history checks included) | PASS, 4 m 17 s |
| `osmpq compact` (copy `minnesota-h3`) | 3 m 52 s, of which the history fold 131 s for 59.7M rows; result: 55,008,768 / 4,631,670 / 59,840 states, minor rows way 88 / relation 40, 2.72 GB |
| `osmpq gc --keep 1` | 5.0 GB, 1,335 files for base + areas + history |
| `osmpq validate` after compaction | PASS, 6 m 38 s |

The whole recipe is `tools/m4_dataset.sh` (`history` stage = `history
init`, `update` stage = the 60-diff batches).

### M2 updater catch-up timing (`minnesota-cur1`)

Per the task, a *plain* hardlink copy of the base dataset (no history --
`minnesota-cur1`, via `tools/m4_dataset.sh curcheck`) was caught up with
the existing M2 `osmpq update` in `--max-diffs 240` batches over the same
diff range, purely to give the coordinator a timing baseline for how long
the M2 updater alone takes over this range (history-tier writing, W3's
work, is not part of this run). Full log:
`$SCRATCHPAD/m4-dataset-logs/curcheck-stdout.log`.

| batch | sequences | diffs | touched node/way/relation | dropped by extent filter | tiers after | wall time |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 7292745-7292984 | 240 | 999 / 239 / 30 | 13,466 / 2,278 / 38 | hour v1 | 79.3 s |
| 2 | 7292985-7293224 | 240 | 45 / 11 / 0 | 2,758 / 692 / 42 | day v1, hour v2 | 69.2 s |
| 3 | 7293225-7293464 | 240 | 265 / 72 / 2 | 846 / 371 / 7 | day v2, hour v3 | 70.1 s |
| 4 | 7293465-7293704 | 240 | 5,321 / 280 / 7 | 16,165 / 2,016 / 38 | day v3, hour v4 | 70.4 s |
| 5 | 7293705-7293944 | 240 | 2,895 / 394 / 3 | 17,647 / 5,245 / 148 | day v4, hour v5 | 77.4 s |
| 6 | 7293945-7294059 | 115 | 2,503 / 304 / 0 | 14,072 / 1,634 / 24 | day v5, hour v6 | 41.8 s |
| **total** | 7292745-7294059 (1,315 diffs) | | | | | **412 s** (≈ 6.9 min) |

(Batch 6 fetched 115 diffs, not the 103 expected from the sequence the
`fetch` stage had cached at 7294047 -- `osmpq update` talks to the live
`--source` directly rather than only the pre-fetched cache, and the
source's own "latest" had advanced by 12 more minutes while the earlier
batches ran. This is expected minutely-replication behavior, not a bug in
the catch-up loop.)

`osmpq validate` on `minnesota-cur1` after catch-up: **PASS**, no
problems found (615 manifest paths, byid ordering, spatial hilbert/id
ordering, 151 way + 150 relation spatial files' cell placement, rowgroup
index coverage all checked). No `osmpq compact` was run on `minnesota-cur1`,
so all 1,315 diffs' worth of hour/day tiers (through day v5, hour v6)
are what a same-range history run would additionally need to append
history rows for.

## 5. Validation targets (docs/m4-contracts.md section 8)

Graded with `python tools/difftest.py --local-only --date 2026-09-19T00:21:52Z --local-date 2026-09-19T00:21:52Z --history-since 2026-09-19T00:21:52Z`
against `osmpq serve` on `minnesota-h2` (history tiers, not yet
compacted). `--local-date` asks the local side for the state at the
reference cache's date, since the local dataset has moved 22 hours past
it; `--history-since` drops reference `timeline` entries that expired
before our history begins.

| target | status |
| --- | --- |
| all existing tests pass unchanged, plus new suites | 683 passed (563 before M4 + 120 new) |
| `[date:t]` at `t` = base equals the M3 answer for corpus 01-49 | yes: 69 of the 80 M3 rows pass exactly as in M3; the 11 that do not are the same 11 as in M3 (04 six rows, 26, 28: reference unavailable; 06 Duluth: Lake Superior extent; 39: five out-of-extent relation areas; 43: `changed` range starting before the history) |
| `[date:t]` at `t` = latest equals the current tables | yes by construction (`test_history_init`, `test_history_update`), and the fixture-level `[date:]` tests over updater tiers and compacted history |
| corpus 50-58 match the reference | 8 of 8 that have a reference answer pass (50, 51, 52, 53, 54, 56, 57, 58); 55 has no reference answer |
| `[date:]` cafes query costs <= 2x the non-attic query | downtown cafes: 1 file both ways, 0.2 s vs < 0.1 s warm; on R2 cold 2.1 s vs 2.7 s. A wide unfiltered node count over 2.1M nodes costs 4x (7.9 s vs 2.0 s): history files carry no row-group index, so the snapshot scan reads the cells' files in full |
| `adiff` over a day on a Minneapolis bbox under 5 s cold | 1.7 s for corpus 58 (buildings with `(._;>;)`, 34 actions), 1.0 s for 57, 1.0 s on R2 for downtown nodes; the unfiltered wide-bbox 56 (2.1M nodes per pass, 190 actions) takes 58 s because both passes are rendered before the diff (open issue) |
| history build under 30 min, history <= 1.5x base bytes | `history init` 70 s and 1.2x; the catch-up 3.2 min; the raw-stream builder does not fit the sandbox (section 4) |

### On R2

With the credentials provided for the `osm-parquet` bucket:
`tools/upload_root.py minnesota-h3 s3://osm-parquet/minnesota` uploaded
5.29 GB / 1,333 files in 102 s. The engine on `s3://osm-parquet/minnesota`
answered downtown cafes in 2.7 s cold (1 file) and 0.0 s warm, the same
at `[date:]` in 2.1 s cold, `timeline` in 4.8 s, an `adiff` of downtown
nodes in 1.0 s. One `osmpq update s3://osm-parquet/minnesota --max-diffs 30`
run applied sequences 7294065..7294094 in 224 s (touched 525 nodes / 144
ways / 23 relations, dropped the out-of-extent rest) and left a manifest
28 with an hour tier for both deltas and history on the bucket. 224 s
for 30 diffs is the first real number for the updater over object
storage; the M2 report's local runs were 6-12 s, so the by-id reads of
touched parents over HTTP dominate and are the next thing to measure
(section 6).

### Reference-only cache status for corpus 50-58

`python tools/difftest.py --reference https://maps.mail.ru/osm/tools/overpass/api/interpreter --corpus tests/corpus --only '5[0-46-8]_*' --reference-only` (55 deliberately excluded from the glob, per the task, to avoid spending retries on a query the reference cannot answer):

| # | query | status | size | count |
| --- | --- | --- | --- | --- |
| 50 | node tags at `[date:]` | cached | 9.7 KB | 24 elements |
| 51 | ways `out geom` at `[date:]` | cached | 2.4 MB | 3,452 elements |
| 52 | `retro` snapshot | cached | 11.4 KB | 30 elements |
| 53 | `timeline(node,973257881)` | cached | 848 B | 3 states |
| 54 | `timeline(way,69963816)` | cached | 842 B | 3 states |
| 55 | `(changed:a,b)` | **not queried** (reference unavailable, OOMs even on tiny bboxes -- section 6.5) | -- | -- |
| 56 | `[adiff:]` nodes `out meta` | cached | 62.3 KB | 190 actions |
| 57 | `[diff:]` ways `out geom` | cached | 6.1 KB | 2 actions |
| 58 | `[adiff:]` with `(._;>;)` | cached | 12.6 KB | 34 actions |

All cached under `tests/corpus/.cache/` (gitignored, not committed);
re-running `--reference-only` for these entries will reuse the cache
rather than re-querying the reference.

## 6. Open issues

- **Raw-stream history builder at scale.** `osmpq history build` needs
  its node pass rewritten to work in bounded id ranges (and the way
  minor-version join to run per range) before a full-history planet
  load; the Minnesota-from-PBF run needs more than the sandbox's 14 GB.
  `history init` + updater covers regional datasets from their base
  timestamp.
- **`adiff` over very large result sets** renders both passes before
  diffing (58 s for 2.1M nodes per pass). Diffing the two passes' set
  rows in SQL and rendering only the changed ids would cut that to the
  two scans (about 8 s each here).
- **Updater over R2**: 224 s for a 30-diff batch against the bucket
  versus 6-12 s locally. Profile the by-id reads of touched parents
  (DuckDB reads whole row groups over HTTP) before the minutely
  schedule is switched on; caching the byid parts in the container or
  an id-to-row-group index are the candidates.
- **Snapshot scans have no row-group index**, so a wide `[date:]` bbox
  scan reads its cells' history files in full (4x the current-table
  cost on 2.1M nodes). Writing the M1 row-group index for history files
  at compaction is straightforward.
- **`(changed:a,b)` with `a` before the history's start** counts only
  the versions the history knows (corpus 43: 16 vs the reference's 19);
  the reference itself cannot answer `changed` on this mirror (out of
  memory), so corpus 55 has no reference answer.
- **Minor versions in catch-up batches**: an element edited twice inside
  one updater batch keeps every version's meta row but the final
  geometry (contract 5.1); the 60-diff batches used here make that
  window one hour.
- **Areas are not versioned**: area filters under `[date:]` use the
  current areas, as the reference does. The updater dropped the `areas`
  manifest section on every run before this milestone (fixed; a test
  now guards it).
- **`retro`'s argument** accepts a string literal or the M3 evaluator's
  expressions, not `_.val` (which needs the unimplemented `for`).
- **Malformed `[date:]`** is a runtime error with a remark here; the
  reference silently ignores it.
