# M2 report: minutely updates on Minnesota

Date: 2026-09-19. Same sandbox as M0/M1. Source of diffs: the
openstreetmap.fr minutely replication of the US Midwest extract that the
Minnesota dataset was cut from (planet-aligned sequence numbers).

## What exists now

| Piece | Where | State |
| --- | --- | --- |
| Delta tiers (hour/day/week) with tombstones, manifest v3 | `docs/m2-contracts.md` section 3 | Written by the updater, read by the engine, folded by compaction |
| Engine read path base ⊕ deltas | `src/osmpq/engine/sources.py` (`current_rows`, `byid_current_rows`) | Every spatial and by-id read goes through one helper; identical SQL to before when no deltas exist; tiers loaded once per run |
| Stateless updater | `src/osmpq/update/`, `osmpq update` | Fetch, parse (pyosmium), extent filter, touched set through indexes, re-resolution, rolling tiers, manifest; `--once` / `--follow` |
| Compaction and gc | `src/osmpq/build/compact.py`, `gc.py`, `osmpq compact`, `osmpq gc` | New generation with hardlinks for untouched files |
| Validation tool | `tools/diffcheck.py` | Compares changed elements by id with the reference at the dataset's timestamp |

Tests: 381 (`python -m pytest -q`).

## The real run

Base: the M1 Rust-produced Minnesota dataset (55.0M nodes, 4.6M ways,
sequence 7292744, 2026-09-19T00:21:52Z). Each `osmpq update` run applied 60
consecutive minute diffs as one batch.

| run | sequences | touched node / way / relation | dropped by extent filter | wall time | tiers after |
| --- | --- | --- | --- | --- | --- |
| 1 | 7292745-7292804 | 633 / 86 / 1 | 4,082 / 720 / 4 | 27 s | hour v1 |
| 2 | 7292805-7292864 | 16 / 8 / 0 | 6,947 / 816 / 16 | 22 s | day v1, hour v2 |
| 3 | 7292865-7292924 | 187 / 140 / 29 | 1,040 / 518 / 15 | 25 s | day v2, hour v3 |
| compaction | | 17 node cells, 14 way cells, 4 relation cells rewritten; all 14 node byid parts, 5 way parts, 8 node_way parts | | 56 s | none (generation g0005) |
| 4 | 7292925-7292984 | 163 / 5 / 0 | 1,397 / 240 / 3 | 23 s | hour v1 |
| 5 | 7292985-7293044 | 2 / 0 / 0 | 1,399 / 493 / 1 | 21 s | day v1, hour v2 |
| 6 | 7293045-7293104 | 0 / 0 / 0 | 315 / 137 / 14 | 21 s | day v2 |

Six hours of diffs, one compaction in the middle, `osmpq validate` clean
after every step. Delta tiers are tens of KB: `hour` v3 was 99 KB and `day`
v2 91 KB against a 2.4 GB base. The ~20 s floor per run is dominated by
fetching 60 diff files sequentially and by loading the index parts; a
`--follow` loop applying one diff per minute has a much smaller fetch cost.
Most dropped elements are Midwest edits outside Minnesota, which is the
extent filter doing its job.

## Correctness

- `tools/diffcheck.py` after the first three hours: **430 of 430** sampled
  changed elements (200 nodes, 200 ways, 30 relations, including 11
  deletions) match the reference Overpass at `[date:"2026-09-19T03:25:00Z"]`
  on existence, version, tags, coordinates, refs and members.
- _post-compaction diffcheck and second-round diffcheck: pending_
- _harness at the updated timestamp: pending_

## Delta overhead per query

Cold Engine per query, HTTP range reads, downtown Minneapolis bbox, same
machine state for both columns. "With deltas" = base plus `hour` v3 (187
nodes, 140 ways, 29 relations) and `day` v2 (649 / 94 / 1).

| query | no deltas: s / requests / files | with two tiers: s / requests / files |
| --- | --- | --- |
| `way[building]` any `out` | 0.10 / 27 / 5 | 0.15 / 36 / 9 |
| wizard `amenity=cafe` | 0.18 / 38 / 14 | 0.30 / 50 / 22 |
| wizard `building` | 0.39 / 85 / 19 | 0.52 / 108 / 27 |
| wizard `natural=water` | 0.67 / 392 / 31 | 0.82 / 415 / 39 |
| `<` / `<<` from nodes | 0.35 / 82 / 12, 0.45 / 77 / 12 | 0.51 / 99 / 22, 0.65 / 94 / 22 |
| `>>` from relations | 0.46 / 114 / 23 | 0.65 / 136 / 33 |
| `node(id)` | 0.04 / 5 / 1 | 0.06 / 7 / 3 |

Each present tier costs one spatial file plus the tombstone file per table
touched (4 extra files for a way query with two tiers) and about 10 extra
range requests; 50-150 ms per query cold, nothing once an Engine has loaded
the tiers (they are cached per run and by DuckDB's object cache across runs).
This is the fixed price of freshness the design accepted; compaction resets
it.

## Bugs the real run found (all fixed, with tests)

1. **Cell placement in leaf-tree gaps.** An extract's leaves do not tile the
   world; the vectorized v2 placement trusted the nearest leaf by sort order
   and could emit a cell at a non-allowed depth for an element in a gap.
   Now it checks containment and snaps to an allowed depth.
2. **In-place manifest writes corrupted hardlinked snapshots.** `cp -al`
   copies share the `manifest/LATEST` inode; writing it in place changed
   every snapshot. Manifests are now written to a temp file and renamed.
3. **Regional extent too wide.** The builder recorded the data bbox, which
   includes the far nodes of boundary-crossing ways, so new elements far
   outside Minnesota could be kept. `osmpq build --raw --extent S,W,N,E`
   records the intended coverage instead.
4. **Delta files re-read per hydration hop.** One query could re-read and
   re-rank the same hundred-row tier files up to 56 times; they are now
   loaded once per run into temp tables (with a size guard that falls back
   to filtered reads for very large tiers).

## Findings that matter for the planet

- **Compaction rewrites every byid part that contains a touched id.** Ids
  are scattered, so on Minnesota all 14 node parts were rewritten for 836
  touched nodes. At planet scale that is the whole 450 GB byid copy per
  compaction. Options: hash-partition byid parts by id so a touched set hits
  few parts, or accept weekly full rewrites (they stream, so it is time and
  Class A operations, not memory).
- **The updater's per-run cost is index reads, not diff size.** The
  node_way and member index parts covering the touched ids are read by
  range; at planet scale a minute's scattered node ids will touch most
  node_way parts. The design's "few thousand row groups" estimate holds
  only if the parts are id-range pruned at row-group granularity, which
  they are (sorted by node_id, 1 MB row groups).
- **Empty tiers vanish.** A run that touches nothing leaves no hour tier;
  the engine and compaction handle an absent tier.
- The updater needs the `node_way` index and metadata on untagged nodes,
  so a base built with the pure-Python `osmpq raw-py` path is not enough;
  use `osmpq-raw build` + `osmpq-raw node-way-index`.

## What M3 needs from this

- The update loop is a plain process (`osmpq update --follow`); the
  Cloudflare Durable Object scheduler from the design only has to start it
  with `--once` every minute and guarantee a single runner.
- Delta tiers and compaction are generation-aware, so the manifest swap and
  `gc --keep` give readers a consistent view while files change underneath.
