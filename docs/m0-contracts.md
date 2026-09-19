# M0 contracts

Interfaces that the M0 prototype's parts are built against, so they can be
developed in parallel. `design.md` explains *why*; this file says *exactly
what*. If you need to change something here, change this file first.

M0 stack: Python 3.11, DuckDB 1.5.x (`spatial`, `httpfs`, community
`osmium` extensions), pyarrow, pyosmium, FastAPI. The planet-scale builder
and the container binary get ported to Rust in M1+; M0 exists to validate the
layout and the query semantics quickly. Package name: `osmpq` (in `src/`).

Environment notes for this repo: `data/` is git-ignored and holds PBFs.
`data/bermuda-latest.osm.pbf` (2 MB) is the development extract;
Minnesota is cut from `data/us-midwest-latest.osm.pbf` with the builder's
`--bbox` option. `download.geofabrik.de` and `overpass-api.de` are not
reachable from the development sandbox; `https://maps.mail.ru/osm/tools/overpass/api/interpreter`
is a full Overpass instance with attic support and is the reference endpoint.

## 1. Dataset root and paths

A dataset lives under a *root*, which is a local directory or an
`s3://bucket/prefix` URL (R2 through DuckDB `httpfs`). All paths inside the
manifest are relative to the root, forward slashes, no leading slash.

```
<root>/manifest/LATEST                         text: the newest manifest number, e.g. "3"
<root>/manifest/<n>.json                       manifest (section 3)
<root>/spatial/<gen>/node/cell=<cell>/tagged=true/part-0.parquet
<root>/spatial/<gen>/node/cell=<cell>/tagged=false/part-0.parquet
<root>/spatial/<gen>/way/cell=<cell>/part-0.parquet
<root>/spatial/<gen>/relation/cell=<cell>/part-0.parquet
<root>/byid/<gen>/node/part-<k>.parquet        k = 00000, 00001, ...; sorted by id, non-overlapping id ranges
<root>/byid/<gen>/way/part-<k>.parquet
<root>/byid/<gen>/relation/part-<k>.parquet
<root>/index/<gen>/node_way/part-<k>.parquet   sorted by node_id
<root>/index/<gen>/member/part-<k>.parquet     sorted by (member_type, member_id)
```

`<gen>` is a generation label like `g0001`. Deltas (`delta/`), `area/`,
`tag_stats/` and `history/` are out of scope for M0.

## 2. Cells

Quadtree over plain lon/lat. The root cell covers lon [-180, 180], lat
[-90, 90]. Each split produces four children in Bing quadkey order:
`0` = NW, `1` = NE, `2` = SW, `3` = SE. A cell key is the string of digits
from the root; the root itself is the key `root`. Depth = number of digits
(root = 0). Max depth 20.

- **Leaf cells** are chosen by the builder: starting at the root, split
  while the cell contains more than `--max-nodes-per-cell` nodes (default
  1,000,000) and depth < 20. The manifest lists the leaves.
- **Nodes** live in the leaf cell containing their point. A point on a
  boundary belongs to the cell where `lon < east` and `lat < north` are
  strict on the high side (i.e. half-open intervals, with the world's
  eastern and northern edges included in the last cell).
- **Ways, relations** live in the *smallest cell (leaf or ancestor,
  including root) whose bbox fully contains the element's bbox*: descend
  from the root while exactly one child contains the whole bbox and that
  child is not below the leaf level for that branch. Cells that end up with
  no elements for a table are simply absent from the manifest for that table.
- **Cells to read for a query bbox** (planner rule): every leaf cell that
  intersects the bbox, plus every ancestor of those leaves up to and
  including `root`, filtered to the cells that exist for the table.

Coordinates are stored as `INT32` in units of 1e-7 degrees (column suffix
`_e7`). `lat_e7 = round(lat * 1e7)`. Conversion back is `lat_e7 / 1e7`.

**Hilbert key** (`hilbert UBIGINT`): map (lon, lat) to a 2^20 × 2^20 grid
(`x = floor((lon + 180) / 360 * 2^20)`, `y = floor((lat + 90) / 180 * 2^20)`,
clamped to [0, 2^20 - 1]) and take the Hilbert curve index of (x, y) at
order 20. For ways/relations use the bbox center. Reference implementation:
`osmpq.layout.hilbert.hilbert_xy2d(order=20, x, y)`, the classic
Wikipedia `xy2d` algorithm. It is the `qt` sort order.

## 3. Manifest (`manifest/<n>.json`)

```json
{
  "manifest_version": 1,
  "generation": "g0001",
  "schema_version": 1,
  "coordinate_scale": 10000000,
  "promoted_keys": ["amenity", "shop", "highway", "building", "name", "natural",
                    "landuse", "leisure", "railway", "waterway", "place", "tourism"],
  "timestamp_osm_base": "2026-09-19T00:21:52Z",
  "replication_sequence": 7292744,
  "source": "us-midwest-latest.osm.pbf bbox=(43.4,-97.3,49.4,-89.4)",
  "extent": [43.4, -97.3, 49.4, -89.4],
  "leaf_cells": ["0213", "0230", "..."],
  "tables": {
    "node": {
      "cells": {
        "0213": {
          "tagged":   {"path": "spatial/g0001/node/cell=0213/tagged=true/part-0.parquet",  "rows": 12345, "bytes": 456789},
          "untagged": {"path": "spatial/g0001/node/cell=0213/tagged=false/part-0.parquet", "rows": 812345, "bytes": 4567890}
        }
      }
    },
    "way":      {"cells": {"021": {"path": "spatial/g0001/way/cell=021/part-0.parquet", "rows": 1, "bytes": 1, "bbox": [43.4, -97.3, 49.4, -89.4]}}},
    "relation": {"cells": {"root": {"path": "spatial/g0001/relation/cell=root/part-0.parquet", "rows": 1, "bytes": 1, "bbox": [43.4, -97.3, 49.4, -89.4]}}}
  },
  "byid": {
    "node":     [{"path": "byid/g0001/node/part-00000.parquet", "min_id": 1, "max_id": 99999999, "rows": 1, "bytes": 1}],
    "way":      [{"path": "byid/g0001/way/part-00000.parquet", "min_id": 1, "max_id": 1, "rows": 1, "bytes": 1}],
    "relation": [{"path": "byid/g0001/relation/part-00000.parquet", "min_id": 1, "max_id": 1, "rows": 1, "bytes": 1}]
  },
  "index": {
    "node_way": [{"path": "index/g0001/node_way/part-00000.parquet", "min_id": 1, "max_id": 1, "rows": 1, "bytes": 1}],
    "member":   [{"path": "index/g0001/member/part-00000.parquet", "rows": 1, "bytes": 1}]
  }
}
```

`extent` and all bboxes are `[south, west, north, east]` in degrees. Either
node partition (`tagged`/`untagged`) may be absent for a cell. `timestamp_osm_base`
is the extract's replication timestamp (from the mirror's `.state.txt`
when available, else the newest `timestamp` in the data) and becomes the
`osm3s.timestamp_osm_base` in responses.

## 4. Parquet schemas

All files: Parquet, ZSTD, dictionary encoding on, row groups of at most
100,000 rows. Types are DuckDB names.

Common **meta** columns on node/way/relation rows:
`version INTEGER, changeset BIGINT, timestamp TIMESTAMP (UTC, no tz), uid INTEGER, "user" VARCHAR`.

Common **tags** columns: `tags MAP(VARCHAR, VARCHAR)` (NULL when the element
has no tags) plus one nullable `VARCHAR` column per promoted key, named
exactly like the key (e.g. `amenity`). Keys with characters that are not
`[a-z0-9_]` are never promoted, so column names need no quoting; `name` is
promoted and must be quoted in SQL as `"name"`.

**`spatial/.../node`** (sorted by `hilbert, id`):
`id BIGINT, lat_e7 INTEGER, lon_e7 INTEGER, tags, <promoted...>, <meta...>, hilbert UBIGINT`.
The `tagged=false` partition has `tags` all NULL and all promoted columns NULL.

**`spatial/.../way`** (sorted by `hilbert, id`):
`id BIGINT, refs BIGINT[], tags, <promoted...>, <meta...>,
xmin_e7 INTEGER, ymin_e7 INTEGER, xmax_e7 INTEGER, ymax_e7 INTEGER,
geometry GEOMETRY, is_closed BOOLEAN, is_area BOOLEAN,
centroid_lat_e7 INTEGER, centroid_lon_e7 INTEGER, cell VARCHAR, hilbert UBIGINT`.
- `geometry` is a LINESTRING (WGS84 lon/lat) of the referenced node
  coordinates in `refs` order, written with DuckDB's native Parquet GEOMETRY
  type. Ways with fewer than 2 resolvable nodes get NULL geometry, and their
  bbox from whatever nodes resolved (NULL if none).
- `is_closed` = first ref == last ref and len(refs) >= 4.
- `is_area` = is_closed AND tags['area'] != 'no' AND NOT (tags has `highway`
  or `barrier` without `area=yes`). This is the osm2pgsql-ish default; it
  only steers `area`/`is_in` behaviour later, so keep it simple.
- `centroid_*` = bbox center in M0.

**`spatial/.../relation`** (sorted by `hilbert, id`):
`id BIGINT, members STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[], tags, <promoted...>, <meta...>,
xmin_e7 INTEGER, ymin_e7 INTEGER, xmax_e7 INTEGER, ymax_e7 INTEGER,
geometry GEOMETRY, centroid_lat_e7 INTEGER, centroid_lon_e7 INTEGER, cell VARCHAR, hilbert UBIGINT`.
- `members[].type` is one of `n`, `w`, `r`.
- bbox = union of member node points, member way bboxes and member relation
  bboxes (one level; nested relation bboxes computed in a second pass, or
  left NULL if the member relation is missing). Relations whose members are
  all missing from the extract get NULL bbox and go to `root`.
- `geometry` is NULL in M0 (relation geometry assembly is later).

**`byid/.../node`** (sorted by `id`):
`id BIGINT, lat_e7 INTEGER, lon_e7 INTEGER, tags, <promoted...>, <meta...>, cell VARCHAR`.
(Includes untagged nodes; there is no tagged split here.)

**`byid/.../way`** (sorted by `id`):
`id BIGINT, refs BIGINT[], tags, <promoted...>, <meta...>, xmin_e7, ymin_e7, xmax_e7, ymax_e7, is_closed, is_area, cell VARCHAR`.
(No geometry: the updater and id lookups rebuild it from the spatial row or from nodes.)

**`byid/.../relation`** (sorted by `id`):
`id BIGINT, members ..., tags, <promoted...>, <meta...>, xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell VARCHAR`.

**`index/.../node_way`** (sorted by `node_id, way_id`): `node_id BIGINT, way_id BIGINT`.

**`index/.../member`** (sorted by `member_type, member_id, parent_id`):
`member_type VARCHAR ('n'|'w'|'r'), member_id BIGINT, parent_id BIGINT, role VARCHAR, parent_cell VARCHAR`.

byid/index files are split into parts of at most 5,000,000 rows.

## 5. Builder CLI

```
osmpq build <input.osm.pbf> <root> [--bbox S,W,N,E] [--generation g0001]
            [--max-nodes-per-cell 1000000] [--promoted-keys k1,k2,...]
            [--timestamp 2026-09-19T00:21:52Z] [--replication-sequence N]
            [--threads N] [--memory-limit 8GB] [--tmpdir DIR]
```

`--bbox` cuts an extract with Osmium "smart" semantics before building:
keep nodes inside the bbox, ways with at least one kept node (plus *all* of
their nodes, even outside the bbox), relations with at least one kept member
(members that are missing stay in the member list but contribute nothing).
The builder works on `data/bermuda-latest.osm.pbf` in under a minute and on
the Minnesota bbox of the Midwest extract (about 60 million nodes) within the
sandbox's 4 cores / 15 GB RAM / ~25 GB scratch, spilling to `--tmpdir`.
Writes `manifest/1.json` and `manifest/LATEST` last.

Reading the PBF: use DuckDB's community `osmium` extension
(`INSTALL osmium FROM community; LOAD osmium; SELECT ... FROM osmium_read('file.pbf')`),
which yields `kind ('node'|'way'|'relation'), id, tags MAP, geometry
(POINT for nodes), version, timestamp, changeset, uid, username, refs
BIGINT[], ref_roles VARCHAR[], ref_types VARCHAR[]`. pyosmium is the fallback
if the extension misbehaves.

## 6. Engine API (Python) and HTTP API

```python
from osmpq.engine import Engine
eng = Engine(root="/path/or/s3://bucket/prefix", duckdb_config={...})   # loads manifest/LATEST
result = eng.run("[out:json];node(44.9,-93.3,45.0,-93.2)[amenity=cafe];out;", timeout=None)
# result: osmpq.engine.result.Result with .elements (list[dict]), .settings, .remark (str|None),
#         .timestamp_osm_base, .stats (dict: files_read, bytes_read, seconds, ...)
body, content_type = result.render()      # JSON or XML per settings.out_format
```

Element dicts follow the Overpass JSON element shape exactly
(`type, id, lat, lon, tags, nodes, members, geometry, bounds, center,
version, timestamp, changeset, user, uid`), so the XML and JSON renderers
are pure formatting over the same list. `out count` yields one element of
type `count` with `tags: {"nodes": "..", "ways": "..", "relations": "..", "total": ".."}`
as strings, like Overpass.

HTTP (FastAPI app in `osmpq.server:app`, run with `uvicorn osmpq.server:app`,
root from env `OSMPQ_ROOT`):

- `GET|POST /api/interpreter` with `data=<query>` (query string or form
  field; raw POST body also accepted when no `data` field). Response
  `Content-Type: application/json` or `application/osm3s+xml`. Parse errors and
  unsupported features: HTTP 400 with the Overpass-style HTML error body
  (`<p><strong style="color:#FF0000">Error</strong>: line N: parse error: ...</p>`).
  Runtime errors (timeout, memory): HTTP 200 with a `remark` in the envelope.
- `GET /api/status`: plain text like Overpass (`Connected as: ...`, `Current time: ...`,
  `Announced endpoint: ...`, `Rate limit: 0`, `Currently running queries ...`).
- `GET /api/timestamp`: the `timestamp_osm_base`.
- CORS `*` on everything (overpass turbo runs in the browser).

JSON envelope:

```json
{"version": 0.6, "generator": "osmpq 0.0.1",
 "osm3s": {"timestamp_osm_base": "2026-09-19T00:21:52Z",
           "copyright": "The data included in this document is from www.openstreetmap.org. The data is made available under ODbL."},
 "elements": [...], "remark": "..."}
```

XML envelope: `<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" generator="osmpq 0.0.1"><note>The data included in this document is from www.openstreetmap.org. The data is made available under ODbL.</note><meta osm_base="2026-09-19T00:21:52Z"/> ... </osm>`
with `<node id lat lon [version timestamp changeset uid user]><tag k v/></node>`,
`<way id ...><bounds .../><nd ref [lat lon]/>...<tag/></way>`,
`<relation id ...><bounds/><member type ref role [lat lon | <nd lat lon/>...]/><tag/></relation>`,
and `<remark>` for runtime errors.

## 7. Output semantics (tier 1)

- Default element order: all nodes, then ways, then relations, each by id
  ascending. `out qt`: by `hilbert` (spatial rows carry it; for byid-only
  rows compute it from the coordinates), ties by id. `out N` limits after
  sorting.
- `out ids`: `type, id` only. `out skel`: plus `lat/lon` for nodes, `nodes`
  for ways, `members` for relations. `out body` (default): skel plus `tags`.
  `out tags`: ids plus tags, no coordinates. `out meta`: body plus
  `version, timestamp, changeset, user, uid`.
- `out geom`: ways get `geometry: [{"lat","lon"}, ...]` in `refs` order (from
  the stored LINESTRING) and `bounds`; relations get per-member geometry
  (`lat/lon` on node members, `geometry` list on way members) by resolving
  members, plus `bounds`; nodes are unchanged. `out bb`: `bounds` only.
  `out center`: `center: {"lat","lon"}` for ways/relations (bbox center).
- `out geom` on a way whose stored geometry is NULL: omit `geometry`.
- Timestamps in output are `YYYY-MM-DDTHH:MM:SSZ`.

## 8. Tier-1 language subset (parser and planner must agree)

Settings `[out:json|xml]`, `[timeout:n]`, `[maxsize:n]`, `[bbox:s,w,n,e]`;
`[date:]`/`[diff:]`/`[adiff:]` parse to `Settings` fields, planner rejects.

Statements: `node|way|rel|relation|nwr|nw|nr|wr` queries with any mix of
tag filters (`[k=v]`, `[k!=v]`, `[k]`, `[!k]`, `[k~v]`, `[k!~v]`, `[~k~v]`,
`,i` flag; keys/values quoted with `"` or `'` or bare identifiers), bbox
`(s,w,n,e)`, `(id)`, `(id:1,2,3)`, input sets `.a` (several = intersection),
recurse filters `(w)`, `(r)`, `(bn)`, `(bw)`, `(br)` with optional `.set` and
`:"role"`; `->.name` on any statement; union `( ... )`, difference
`( a; - b; )`; `>`, `>>`, `<`, `<<`; `.a;` and `.a -> .b;`; `out` with any
combination of `ids|skel|body|tags|meta`, `geom|bb|center`, `qt|asc`, a limit
integer, `noids`, and `out count`. Comments `//` and `/* */`. The
`{{bbox}}`-style overpass turbo shortcuts never reach the server.

Parsed but planner-rejected in M0 (`UnsupportedError`): `area` queries and
`(area...)`, `(around...)`, `(poly...)`, `(pivot...)`, `(newer:)`,
`(changed:)`, `(user:)`, `(uid:)`, `(if:)`, `is_in`, `map_to_area`,
`foreach`, `if`, `for`, `complete`, `retro`, `compare`, `make`, `convert`,
`timeline`, `local`, `[out:csv]`.

## 9. Differential test harness

`tools/difftest.py --reference https://maps.mail.ru/osm/tools/overpass/api/interpreter --local http://127.0.0.1:8080/api/interpreter --corpus tests/corpus --date 2026-09-19T00:21:52Z`

- Corpus files `tests/corpus/*.overpassql` contain a query with `{{bbox}}`
  placeholders; the harness substitutes a bbox from `tests/corpus/bboxes.json`
  (several small Minnesota bboxes: downtown Minneapolis, a suburb, farmland,
  a lake) and prepends `[date:"<date>"]` **only on the reference side** so
  the reference answers as of the extract's timestamp.
- Compare per query: the set of `(type, id)`; for common elements, tags
  equality; node coordinates within 1e-7; way `nodes` lists equality;
  `geometry` point lists within 1e-6 when both sides have them; `count`
  values. Report per query: `PASS`, or `FAIL` with counts of missing/extra
  elements and up to 5 example ids per category, plus wall-clock times for
  both sides. Exit non-zero if any FAIL. `--json report.json` writes the
  full report. Rate-limit reference calls (one at a time, short sleep) and
  retry on 429/504.
