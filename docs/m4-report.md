# M4 report: history (attic)

Contract: `docs/m4-contracts.md`. Base dataset: the M3 Minnesota areas
dataset (`minnesota-rs3-areas2` in the shared scratchpad; 55.0M nodes,
4.6M ways, 59,798 relations; `timestamp_osm_base`
`2026-09-19T00:21:52Z`; `replication_sequence` 7292744; manifest v4).
Reference: `maps.mail.ru` (Overpass API 0.7.62.4).

This report's data and harness sections (4 and 5, and the corpus part of
3) were written by W4; everything marked **[coordinator/Wn: fill in]**
below is a placeholder for the coordinator to complete once the other
workstreams land.

## 1. What was built

| workstream | delivered |
| --- | --- |
| W1 history builder | **[coordinator/W1: fill in]** -- `src/osmpq/history/{build,ingest,writer,schema}.py`, `osmpq history build`, `osmpq validate` history checks |
| W2 engine attic | **[coordinator/W2: fill in]** -- `src/osmpq/engine/attic.py`, snapshot reads, `retro`/`timeline`/`diff`/`adiff`/`changed` |
| W3 updater + compaction | **[coordinator/W3: fill in]** -- history tier append in the updater, history fold in `osmpq compact`/`gc` |
| W4 harness + corpus + data | `tools/difftest.py`: XML action-list comparator for `[diff:]`/`[adiff:]`, multiset-of-tags comparator for `timeline`, `--date` no longer injected into attic queries (section 2 below); corpus 50-58 (section 3); `tools/m4_dataset.sh` + `tools/m4_fetch_diffs.py` dataset recipe, diffs fetched, M2 catch-up timing measured (section 4) |
| coordinator | **[coordinator: fill in]** |

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

### Finding: the reference's `timeline()` may not split minor versions

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

### History build

**Not run.** `osmpq history build` (`src/osmpq/history/build.py`) is
W1's deliverable and does not exist in this worktree as of this report;
`tools/m4_dataset.sh history` is written against the exact invocation
`docs/m4-contracts.md` section 7 specifies
(`osmpq history build minnesota-h1 --pbf data/minnesota.osm.pbf --osc
osc-midwest`) and is ready to run once it lands. `minnesota-h1` itself
(the hardlink copy of `minnesota-rs3-areas2` the history build will run
against) **was** created by `tools/m4_dataset.sh copy`.

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

Most of section 8's targets need the built history dataset and the W2
engine, neither of which exists in this worktree yet -- **[coordinator:
fill in once W1/W2/W3 land]**:

| target | status |
| --- | --- |
| all existing tests pass unchanged (563), plus new suites | `python -m pytest tests -q`: 499 passed, 64 skipped in this worktree (W4's changes only: `tools/difftest.py`, `tests/corpus/*`) -- no regressions from this workstream. The 563/new-suite count from the contract depends on W1/W2/W3's tests, not yet merged here |
| `[date:t]` at `t`=base equals the M3 answer for corpus 01-49 | **[coordinator/W2: fill in]** |
| `[date:t]` at `t`=latest equals the current tables | **[coordinator/W2: fill in]** |
| corpus 50-58 match the reference | reference side ready (section 3); **[coordinator: fill in local-side grading once the engine and history dataset exist]** -- 7 of 9 entries have a cached reference answer (see the table below), 55 is intentionally reference-unavailable, 52 was rewritten after a finding invalidated its first form (section 3) |
| `[date:]` cafés query costs <= 2x files/time of the non-attic query | **[coordinator/W2: fill in]** |
| `adiff` over a day on a Minneapolis bbox finishes under 5s cold | **[coordinator/W2: fill in]** |
| history build finishes under 30 min, history <= 1.5x base bytes | **[coordinator/W1: fill in]** |

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

- **History build and engine not run against corpus 50-58 yet** -- this
  workstream could only validate the harness machinery (section 2,
  `tests/corpus/selftest.py`) and the reference side of the corpus
  (section 3/5); grading the local engine's answers is blocked on W1's
  `osmpq history build` and W2's attic engine landing.
- **Two findings for W2** (detailed in section 3): `retro()`'s
  named-set-assignment scoping does not match section 3.2's "sets
  assigned inside remain visible outside" claim on the live reference;
  and the reference's `timeline()` may not split minor versions the way
  section 3.2 hypothesized. Both are direct probe observations, not
  inferences -- see the commands and cached responses referenced in
  section 3.
- **`tools/m4_dataset.sh update`/`history` stages are untested** since
  they depend on `osmpq history build`, which does not exist in this
  worktree. They are written against the exact syntax
  `docs/m4-contracts.md` section 7 specifies; the coordinator should
  smoke-test them once W1's command lands, before relying on them for
  the real build.
- **Disk**: `minnesota-h1` and `minnesota-cur1` are hardlink copies of
  `minnesota-rs3-areas2` (2.4 GB), so they cost near-zero extra disk
  until compaction/history-build rewrites files; `osc-midwest/` is 12 MB.
  About 19 GB was free before this workstream's copies; unaffected so
  far since nothing has been rewritten yet.
