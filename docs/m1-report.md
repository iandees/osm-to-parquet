# M1 report: planet-capable builder and layout v2

Date: 2026-09-19. Same sandbox as M0 (4 cores, 15 GB, local disk standing
in for object storage). The planet run itself is yours to do with
`docs/m1-runbook.md`; the numbers here are Minnesota measurements and
extrapolations.

## What changed since M0

| Piece | Change |
| --- | --- |
| `rust/osmpq-raw` (new) | Rust PBF producer: two-pass leaf selection, node files in both copies with metadata for every node, way geometry through a `sorted-mem` or `dense-file` node store, relation parts, optional node_way index. Streams with bounded memory. |
| `osmpq build --raw` | The Python/DuckDB stage now consumes the raw layout from either producer: links the node/way files, computes relations, member index, row-group index side files, manifest v2. `osmpq raw-py` is the pure-Python producer for machines without Rust. `osmpq validate` checks a root. |
| Layout v2 | Loose placement restricted to leaves plus ancestor depths {0, 3, 6, 9, 12}; row-group index (`index/<gen>/rowgroups/*.parquet`) built from Parquet footers; byte-sized row groups; metadata on untagged nodes. |
| Engine | Reads manifest v1 and v2; prunes files by the row-group index before any read; one DuckDB database per Engine with a cursor per run (object and HTTP metadata caches survive between queries); concurrency-safe stats; R2 credentials from `OSMPQ_S3_*`. |

Test suite: 278 tests, including a Rust end-to-end test that skips when cargo is absent.

## Producer throughput on Minnesota (55.0M nodes, 4.6M ways, 60k relations)

| producer | total | per pass |
| --- | --- | --- |
| `osmpq raw-py` (Python + DuckDB, 4 threads) | 556 s | read 36 s, leaves 79 s, node cell/hilbert 158 s, way geometry 104 s, way cells 38 s, node files 87 s, way files 53 s |
| `osmpq-raw` (Rust, 4 threads, sorted-mem store) | 159 s | histogram 13.5 s (4.1M nodes/s), nodes 88 s (624k nodes/s incl. per-cell sort and write), ways 47.5 s (98k ways/s), relations 9.5 s |
| `osmpq-raw node-way-index` | 14 s | 63.8M pairs, 245 MB |
| `osmpq build --raw` (both producers) | 10 s | relations 9 s, row-group index 0.2 s, everything else is hardlinks |

Peak RSS of the Rust producer: 1.9 GB (0.9 GB of it the sorted-mem node
store); temp spill peaked at 3.7 GB and is deleted per cell.

**Planet extrapolation** (10B nodes, 1.1B ways, ~14M relations) at the same
per-element cost: histogram 41 min, nodes 4.5 h, ways 3.1 h, relations 40
min, node_way index 50 min: about 10 hours on 4 cores, and PBF decoding and
per-cell sorting parallelize, so 16 cores should land well under that. Two
caveats: the `dense-file` node store (a 110 GB sparse file, since node ids
already reach 14.2 billion) is slower than the in-memory store unless the
page cache holds most of it, and a root-level way cell at planet scale may
need the chunked sort the contract allows for and the Rust code does not yet
implement. The way pass, not the node pass, is where a 32 GB machine will
hurt.

## Layout v2 effect on queries (downtown Minneapolis bbox, cold DuckDB, HTTP range reads)

| query | M0 (v1) files / requests / MB / s | v2 files / requests / MB / s |
| --- | --- | --- |
| `way[building]` any `out` mode | 9 / 39 / 6.6 / 0.25 | 5 / 23 / 6.5 / 0.11 |
| `way[highway] out geom` | 9 / 40 / 6.8 / 0.39 | 5 / 26 / 9.1 / 0.24 |
| wizard `amenity=cafe` | 20 / 43 / 8.8 / 0.34 | 14 / 38 / 8.8 / 0.20 |
| wizard `building` | 22 / 95 / 15.9 / 0.57 | 19 / 73 / 14.2 / 0.41 |
| wizard `natural=water` | 37 / 247 / 60 / 0.84 | 31 / 209 / 69 / 0.71 |
| `<` from nodes | 17 / 87 / 14 / 0.53 | 12 / 74 / 15 / 0.40 |
| `>>` from relations | 26 / 107 / 28 / 0.69 | 23 / 97 / 28 / 0.61 |
| `node(id)` | 1 / 5 / 1.0 / 0.18 | 1 / 5 / 0.7 / 0.04 |

(v2 numbers from the `osmpq raw-py` build; the engine-side caching change is
not included in either column: both use a fresh Engine per query.)

Files per simple bbox query went from 9 to 5 (the ancestor tax is now the
leaf plus at most three ancestors at depths 9, 6, 3 and root, and the
row-group index skips the ones with nothing in range). Way files dropped
from 186 to 151 as ways at odd depths folded into allowed ancestors.

## Engine caching effect (same process, repeated query)

| query | run 1 requests / s | run 2 requests / s |
| --- | --- | --- |
| cafe (34 elements) | 8 / 0.045 | 0 / 0.029 |
| wizard building (6,893 elements) | 110 / 0.44 | 0 / 0.30 |

A warm container serving repeat queries over the same cells does no I/O at
all; the cold-start cost per process is now the manifest, the three
row-group index files and the footers of the touched files.

## Writer settings matter as much as layout

The first Rust output was 1.6-2x larger than DuckDB's for identical columns:
parquet-rs tried dictionary encoding on numeric columns and fell back to
PLAIN mid-page, and its row groups were 3-10x smaller, which multiplied the
range requests DuckDB issues (one per column chunk). Per-row cost of the
untagged node file:

| column | DuckDB | parquet-rs default | parquet-rs tuned |
| --- | --- | --- | --- |
| id | 2.0 B | 3.9 B | 2.0 B |
| lat_e7 / lon_e7 | 3.0 / 3.2 B | 4.8 / 4.9 B | 1.9 / 2.0 B |
| hilbert (sort key) | 0.7 B | 2.2 B | 0.5 B |
| timestamp / changeset / uid / version | NULL in the DuckDB build | 1.6 / 0.6 / 0.3 / 0.1 B | 1.2 / 0.5 / 0.3 / 0.1 B |
| whole row | 8.9 B | 18.5 B | 8.6 B |

Tuned settings (now part of the contract): Parquet 2.0 writer, ZSTD level 3,
dictionary encoding only on strings, DELTA_BINARY_PACKED on the sort key and
on every coordinate/bbox column, PLAIN on refs, changeset, timestamp and
uid (deltas measured larger there), row groups sized by compressed bytes
(nodes 1 MB, byid ways 2 MB, spatial ways 4 MB, relations 8k rows). The
Rust output is now smaller than DuckDB's while carrying metadata for every
node.

## Dataset size and the planet

Minnesota, Rust producer:

| | before tuning | after tuning |
| --- | --- | --- |
| spatial (node + way + relation) | 2.29 GB | 1.50 GB |
| byid | 1.40 GB | 0.69 GB |
| index (node_way + member + row groups) | 0.26 GB | 0.24 GB |
| **total** | **3.95 GB** | **2.42 GB** |
| spatial way row groups | 541 (8.6k rows) | 334 (13.9k rows) |
| spatial node row groups | 1,062 (52k rows) | 691 (80k rows) |

That is 44 bytes per node-equivalent including full metadata, the id-sorted
copy and the node_way index; roughly 440 GB for the planet, of which the
spatial copy the queries read is about 270 GB. Metadata on all nodes costs
about 2.1 bytes per node and is required for JOSM-style `out meta`.

## Final query profile (Rust-produced, tuned dataset; cold Engine per query, HTTP range reads, downtown Minneapolis)

| query | elements | seconds | requests | files | MB | M0 requests / MB |
| --- | --- | --- | --- | --- | --- | --- |
| wizard `amenity=cafe` | 72 | 0.18 | 38 | 14 | 6.5 | 43 / 8.8 |
| wizard `building` | 6,893 | 0.37 | 85 | 19 | 12.3 | 95 / 15.9 |
| wizard `highway=residential` | 159 | 0.19 | 46 | 14 | 7.1 | 59 / 9.5 |
| wizard `natural=water` | 2,645 | 0.71 | 394 | 31 | 55 | 247 / 60 |
| wizard `leisure=park` | 1,320 | 0.35 | 119 | 22 | 23 | 116 / 24 |
| `nwr[amenity] out center` | 1,311 | 0.24 | 84 | 18 | 8.0 | 84 / 9.5 |
| `way[highway] out geom` | 3,452 | 0.23 | 30 | 5 | 7.0 | 40 / 6.8 |
| `way[building]`, any `out` | 614 | 0.08-0.11 | 27 | 5 | 4.4 | 39 / 6.6 |
| `[bbox:]` + `nwr[shop]` | 103 | 0.12 | 41 | 13 | 5.0 | 55 / 7.3 |
| regex `["name"~"^Lake",i]` | 12 | 0.13 | 51 | 13 | 10.5 | 65 / 10 |
| `node(w)` | 5,878 | 0.17 | 38 | 8 | 7.0 | 49 / 9.4 |
| `way(bn)` / `<` / `<<` | 422-654 | 0.22-0.43 | 45-82 | 8-12 | 12-15 | 44-87 / 12-14 |
| `>>` from relations | 1,244 | 0.49 | 115 | 23 | 27 | 107 / 28 |
| difference / intersection | 2,831 / 966 | 0.25 / 0.20 | 30 / 33 | 5 | 7.0 / 9.5 | 40 / 42 |
| `node(id)` / `way(id:...)` | 1 / 2 | 0.03 / 0.09 | 5 / 9 | 1 / 2 | 1.0 / 5.0 | 5 / 7 |
| relation `out geom` | 6 | 0.22 | 92 | 17 | 18 | 84 / 10 |

Harness on this dataset: 54 of 55 gradable entries match, same set as M0.
The `natural=water` wizard query is the one that still fans out (31 files,
394 requests) because lake multipolygons pull member ways and their nodes
from many cells; it is also the one where a warm Engine helps most.

## Findings

1. **Rust is the right tool for the two whole-planet passes** (nodes, ways);
   DuckDB is the right tool for the relational tail (relations, indexes) and
   for reading. The split at the raw layout keeps both testable alone.
2. **Encodings and row-group sizes are a first-class part of the layout
   contract**, not an implementation detail: they changed size by 2x and
   request count by 6x with no schema change. The contract now pins them.
3. **Warm engines are cheap**: DuckDB's object cache makes a container that
   stays up between queries behave like a local database for repeated
   regions. The `sleepAfter` decision from the design is therefore a real
   cost/latency knob.
4. **Boundary semantics**: the vectorized v2 placement in Python and the
   literal descent rule in Rust disagree only when a bbox corner sits
   exactly on a cell edge; both produce a containing cell at an allowed
   depth, and the engine's selection is a superset of either, so results are
   unaffected. `osmpq validate` accepts both.
5. **`--bbox` in the Rust producer is not reference-complete** (it drops
   out-of-bbox nodes before way assembly). Use `tools/bbox_extract.py` or
   `osmium extract` to cut extracts; the flag is for quick experiments.

## What M2 needs from this

- `rawdir/node_way/` and `index/member` exist for the updater; the node
  store abstraction (`store.rs`) is where a persistent-volume or R2-backed
  variant plugs in.
- The manifest already carries `replication_sequence` and
  `timestamp_osm_base`; the delta files of design section 4.3 slot in next
  to the base generation without changing the engine's file selection.
