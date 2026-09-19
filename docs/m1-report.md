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
| id | 1.7 B | 3.5 B | _pending_ |
| lat_e7 / lon_e7 | 3.2 B | 5.0 B | _pending_ |
| hilbert | 0.9 B | 2.3 B | _pending_ |

_The tuned numbers and the resulting dataset size are filled in below once
the writer change lands._

## Dataset size and the planet

Minnesota, Rust producer, before writer tuning: spatial 2.29 GB, byid 1.40
GB, index 0.26 GB (3.7 GB total, 6.8 GB at planet scale per 100M nodes).
Metadata on all nodes costs about 120 MB per 55M nodes (2.2 bytes per
node) and is required for JOSM-style `out meta`.

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
