# M0 report: Minnesota prototype

Date: 2026-09-19. Everything below ran in a 4-core, 15 GB sandbox with local
disk standing in for object storage (see "Remote reads" for how range
requests were measured). Nothing has touched a real R2 bucket yet.

## What exists now

| Piece | Where | State |
| --- | --- | --- |
| Builder: PBF → cell-partitioned Parquet layout, id-sorted copy, indexes, manifest | `src/osmpq/build`, `src/osmpq/layout`, `osmpq build` | Works on Bermuda (6 s) and Minnesota (~10 min). Python + DuckDB; M1 ports it to Rust. |
| Overpass QL lexer/parser → AST | `src/osmpq/ql` | Tier-1 subset fully; tier-2/3 syntax parsed and rejected by the planner. 140 tests. |
| Planner + DuckDB executor + JSON/XML output + FastAPI server | `src/osmpq/engine`, `src/osmpq/server.py` | Tier-1 statements. Reads a local dir, an `http(s)://` root or `s3://` through DuckDB httpfs. |
| Differential harness + corpus | `tools/difftest.py`, `tests/corpus` | 32 queries × 6 Minnesota bboxes against a public Overpass with `[date:]` pinned to the extract timestamp. |
| Range-request profiler | `tools/range_server.py`, `tools/remote_profile.py` | Counts requests, files and bytes per query over HTTP range reads. |
| Extract cutter | `tools/bbox_extract.py` | Cuts a reference-complete bbox extract from a larger PBF (Geofabrik is unreachable from the sandbox). |

Test suite: 222 tests (`python -m pytest -q`). One test (`test_timeout_produces_remark`) is timing-flaky and should use a slower query.

## Dataset

Minnesota, cut from the openstreetmap.fr US Midwest extract of 2026-09-19 00:21 UTC (replication sequence 7292744), bbox (43.45, -97.30, 49.40, -89.45).

| | count |
| --- | --- |
| nodes | 54,996,422 (1,191,794 tagged, 2.2%) |
| ways | 4,630,269 |
| relations | 59,798 |
| leaf cells (split at 1M nodes) | 143, depth 6-12 |
| way cells (leaves + ancestors holding loose-placed ways) | 186, depth 1-12 |

Sizes on disk (Parquet, zstd):

| table | files | rows | size |
| --- | --- | --- | --- |
| spatial node (tagged + untagged partitions) | 286 | 55.0M | 506 MB (median 2.9 MB per cell, max 9.8 MB) |
| spatial way (with LINESTRING geometry) | 186 | 4.6M | 1,029 MB (median 4.7 MB per cell, max 18 MB) |
| spatial relation | 185 | 60k | 5.8 MB |
| byid node | 14 parts | 55.0M | 454 MB |
| byid way (refs, no geometry) | 2 parts | 4.6M | 240 MB |
| byid relation | 1 | 60k | 3.7 MB |
| node_way index | 16 parts | 63.8M | 140 MB |
| member index | 1 | 627k | 2.3 MB |
| **total** | | | **2.3 GB** (5.2 bytes per node-equivalent; the 439 MB PBF is 5.3x smaller) |

Scaled naively to the planet (10B nodes, 1.1B ways) that is roughly 420 GB for
the current state, in line with the design's estimate. Way geometry is the
largest item (about 220 bytes per way); shredded GEOMETRY encoding and
smaller coordinate types are the obvious levers.

Build (4 threads, 9 GB memory limit): read PBF 20 s, way geometry 93 s,
relation bbox 1 s, leaf selection 50 s, node cell/Hilbert 135 s, way/relation
cell assignment 95 s, node files 57 s, way files 47 s, relation files 7 s,
byid 41 s, indexes 9 s: about 9.5 minutes. Three memory blowups were fixed on
the way (unbounded ordered `list()` aggregation, a lateral `UNNEST` join
planned as a cross product, and numbering 55M rows with a window function);
the pipeline now streams within the memory limit.

## Correctness against Overpass

`tools/difftest.py` ran all 32 corpus queries against `maps.mail.ru`'s
Overpass (`[date:"2026-09-19T00:21:52Z"]`) and against our server, comparing
element sets, tags, coordinates and node lists.

| | entries |
| --- | --- |
| total (query × bbox) | 63 |
| reference could not answer (its dispatcher timed out or ran out of memory; retried over several hours) | 8 |
| pass | 54 |
| fail | 1 |

The one failure is `natural=water` at Duluth harbor: Lake Superior is a
multipolygon relation with 4,612 member ways, only 1,120 of which exist in
the Minnesota extract; the reference returns members in Wisconsin and
Michigan that our extract never contained. Not an engine issue.

Bugs the harness found and that were fixed before reaching this state:
promoted tag columns held only the first character of each value (DuckDB
1.5 map subscripts return the value, not a one-element list); `>` from
relations omitted the nodes of member ways; `>>` dropped input relations;
`<` was missing the relations that contain the found ways; and ways and
relations were selected on bounding-box overlap rather than geometry
intersection, which produced spurious extras. Each of these now has a test.

## Latency and remote-read profile

Measured through the engine directly, dataset served over HTTP with byte
ranges from the same machine, a fresh DuckDB per query (so no in-process
cache; DuckDB's own HTTP metadata cache is per connection and cold). Downtown
Minneapolis bbox (44.970, -93.280, 44.985, -93.255). Network latency is
essentially zero here; the request counts are what matter for object storage.

| query | elements | seconds | range requests | files | MB read |
| --- | --- | --- | --- | --- | --- |
| wizard `amenity=cafe` (nwr + `>` + skel) | 72 | 0.35 | 43 | 20 | 8.8 |
| wizard `building` | 6,893 | 0.80 | 107 | 22 | 57 |
| wizard `highway=residential` | 159 | 0.36 | 59 | 20 | 9.5 |
| wizard `natural=water` | 2,645 | 1.7 | 302 | 32 | 258 |
| wizard `leisure=park` | 1,320 | 0.76 | 129 | 25 | 75 |
| `nwr[amenity] out center` | 1,311 | 0.40 | 84 | 21 | 9.5 |
| `way[highway] out geom` | 3,452 | 0.40 | 40 | 9 | 6.8 |
| `way[building]` with `out tags/ids/skel/meta/count/5/qt` | 614 | 0.25-0.29 | 39 | 9 | 6.6 |
| `[bbox:]` global + `nwr[shop]` | 103 | 0.28 | 55 | 19 | 7.3 |
| regex `["name"~"^Lake",i]` | 12 | 0.29 | 65 | 19 | 10 |
| `node(w)` after a way query | 5,878 | 0.33 | 49 | 11 | 9.4 |
| `way(bn)` / `<` / `<<` from nodes | 422-654 | 3.0-3.4 | ~1,070 | 15-16 | 322-328 |
| `>>` from relations | 1,244 | 4.5 | 1,678 | 29 | 496 |
| `rel(bw)` after a way query | 251 | 0.52 | 73 | 11 | 12.6 |
| difference / intersection of sets | 2,831 / 966 | 0.43 / 0.38 | 40 / 42 | 9 | 6.8 / 9.3 |
| `node(id)` / `way(id:...)` by id | 1 / 2 | 0.18 / 0.30 | 5 / 7 | 1 | 1.0 / 12.7 |
| relation `out geom` | 6 | 0.33 | 50 | 14 | 10 |

Reading the table:

- **A typical bbox query touches 9-25 files and 40-110 range requests.** Each
  Parquet file costs 2-3 requests before any data (footer, metadata), and the
  loose-placement scheme means a query reads the leaf cell plus every
  ancestor cell that has a file (up to 12 here), most of them for nothing.
  This baseline, not the data volume, will dominate on R2, where each request
  is 20-80 ms of latency. With DuckDB issuing them concurrently, a cold
  simple query should land around 0.5-1.5 s on R2; the numbers must be
  confirmed on a real bucket in M1.
- **Data volumes are small** for the common queries (6-10 MB), and the byte
  reads are almost entirely row groups that survive bbox pruning. The
  `building` wizard query reads 57 MB because the untagged node partition of
  the downtown cell is read to resolve 5.9k way nodes; that is one file.
- **Reverse lookups were the outlier** (`<`, `way(bn)`, `>>`): 1,000+ requests
  and 300-500 MB, because parent ways were resolved through the id-sorted
  `node_way` index and byid way parts, and scattered ids defeat row-group
  pruning. The same mechanism was fixed for `>` during M0 by resolving nodes
  from the spatial cells covering the source ways (5.6 s → 0.65 s); the
  reverse direction is being changed to scan the way files of the nodes'
  cells with a `refs` semi-join, and relation member ways to the spatial
  files covering the relation bbox. Numbers will be updated below.
- **By-id lookups are cheap** when the row is in one byid part (5-7 requests);
  the way lookup read 12.7 MB because a whole row group of the byid way
  parts (refs and tags of ~100k ways) is fetched for two ids. Smaller row
  groups on byid tables would help.

## Findings that change the design

1. **Ancestor-cell cost.** Loose placement is right for large features, but
   reading every ancestor file per query is a fixed tax of ~10 files. Options
   for M1: store per-cell way files only at leaves and at a few coarse
   levels (say depth 0, 4, 8), splitting long ways' bboxes upward only to
   those levels; or record in the manifest which ancestor files actually
   have rows intersecting each leaf (a tiny bitmap), so empty ancestors are
   skipped.
2. **Parquet footer round trips.** Two to three requests per file before
   data. M1 should either embed each file's row-group index (bbox min/max
   per row group and byte offsets) in the manifest, or keep files fewer and
   larger with small row groups, so that a query is one request per file.
3. **Id-sorted copies are for the updater, not for queries.** Every query
   path that used byid or the node_way index for scattered ids was slow;
   every one that used cells was fast. The engine should touch byid only for
   explicit id queries and for ids whose bbox is unknown.
4. **Untagged nodes are 98% of rows** but only needed for `>`/`out skel`
   after ways; splitting them into their own partition paid off (tag queries
   read the small partition).
5. **Metadata gap.** The two DuckDB extensions available cannot both give raw
   structure and metadata, so untagged nodes have NULL version/timestamp/user
   in M0. The Rust builder in M1 fixes this.
6. **Row-group size.** 100k rows per row group is too coarse for byid parts
   (a two-id lookup reads 12.7 MB). 20-50k rows would cut that 2-5x at a
   modest footer cost.

## Verdict

The layout and translation approach work: 54 of 55 gradable corpus entries
match a real Overpass instance, and the common queries cost tens of range
requests and under 10 MB, which is compatible with an object-store-backed,
serverless engine. The remaining risk is per-request latency on R2, which
scales with the file count per query (fixable in M1 by the findings above),
and the reverse-lookup paths, which are being moved onto the same cell-scoped
mechanism that fixed `>`.

Go for M1, with these changes to the plan: implement finding 1-3 in the
layout before the planet build; port the builder to Rust with pyosmium-free
metadata; keep the Python engine until the SQL it emits is stable, then port.

## Reproducing

```
# data
python tools/bbox_extract.py data/us-midwest-latest.osm.pbf data/minnesota.osm.pbf 43.45,-97.30,49.40,-89.45
osmpq build data/minnesota.osm.pbf /path/to/root --threads 4 --memory-limit 9GB --tmpdir /path/to/tmp \
    --timestamp 2026-09-19T00:21:52Z --replication-sequence 7292744
# serve and test
OSMPQ_ROOT=/path/to/root uvicorn osmpq.server:app --port 8080
python tools/difftest.py --reference https://maps.mail.ru/osm/tools/overpass/api/interpreter \
    --local http://127.0.0.1:8080/api/interpreter --corpus tests/corpus --date 2026-09-19T00:21:52Z
# remote-read profile
python tools/range_server.py /path/to/root 8090 > range.log &
python tools/remote_profile.py --root http://127.0.0.1:8090 --log range.log
```
