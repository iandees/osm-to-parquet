"""Builds a tiny synthetic dataset that follows docs/m0-contracts.md
sections 1-4 literally, using DuckDB directly (the real builder is being
developed concurrently and is not used here).

Layout produced under `root`:
  - 3 leaf cells ("000", "001", "002"), all children of ancestor cell "00".
  - ~36 nodes spread across the 3 leaves, both tagged=true/false partitions.
  - 10 ways (one closed), 9 confined to a single leaf, 1 (way 110) spanning
    leaves "000" and "002" and therefore placed at ancestor cell "00".
  - 3 relations: one confined to a leaf, one spanning leaves (ancestor "00"),
    one with a node + way member for `out geom` coverage.
  - byid copies (node split into 2 parts on purpose), node_way and member
    indexes, and manifest/1.json + manifest/LATEST.

Everything needed by tests to make assertions is returned by `build()` as a
`FixtureInfo` (well-known ids/tags), so tests don't have to re-derive them.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from osmpq.engine.catalog import cell_bbox  # noqa: E402
from osmpq.engine.hilbert import bbox_e7_center_hilbert, lonlat_to_hilbert  # noqa: E402

GEN = "g0001"
PROMOTED_KEYS = [
    "amenity", "shop", "highway", "building", "name", "natural",
    "landuse", "leisure", "railway", "waterway", "place", "tourism",
]
TIMESTAMP_OSM_BASE = "2026-09-19T00:21:52Z"

LEAVES = ["000", "001", "002"]
ANCESTOR = "00"

# -- manifest v2 (docs/m1-contracts.md sections 2/4/5) --------------------
# `manifest_version=2` reuses every v1 node/way/relation verbatim (same
# ids, tags, geometry -- see build()'s "v2 cell-placement adjustments"
# block below) so the same overpassQL queries return the same elements
# against either mode (see test_engine_v2.py's v1-vs-v2 equivalence test);
# only *where things are filed* (cell placement) and what side files exist
# (row-group index) differ.
V2_ANCESTOR_DEPTHS = [0, 3, 6, 9, 12]
V2_MAX_DEPTH = 13
# A brand-new, disjoint branch of the quadtree (nothing under "3" exists in
# the v1 topology) with one deliberately deep split, purely to exercise the
# ancestor-depth placement rule (m1-contracts.md section 2): a way whose
# smallest *containing* cell (root-descend algorithm, no depth
# restriction) is "3000" (depth 4) -- because it straddles leaves "30000"
# and "30001", two children of "3000" -- lands, once v2 restricts loose
# placement to leaves and to depths in `V2_ANCESTOR_DEPTHS`, at "300"
# (depth 3, the greatest allowed depth <= 4). See `TRAP_WAY_CELL` below.
V2_TRAP_LEAVES = ["30000", "30001", "30002", "30003"]
TRAP_WAY_CELL = "300"  # V2_TRAP_LEAVES[0][:3] == V2_TRAP_LEAVES[1][:3]
TRAP_WAY_ID = 120


def to_e7(deg: float) -> int:
    return int(round(deg * 1e7))


def _inset_point(bbox, frac_lat: float, frac_lon: float):
    s, w, n, e = bbox
    lat = s + (n - s) * frac_lat
    lon = w + (e - w) * frac_lon
    return lon, lat


def _column_stats(row_group, column_name: str):
    """A row group's statistics for the scalar (non-nested) column named
    `column_name`, found by `path_in_schema` rather than a positional index
    -- a MAP column (``tags``) flattens into several physical leaf columns
    in the Parquet footer, which would otherwise throw off any fixed
    column-index arithmetic for the plain INTEGER columns after it."""
    for k in range(row_group.num_columns):
        col = row_group.column(k)
        if col.path_in_schema == column_name:
            return col.statistics
    return None


def _node_rowgroup_rows(path: Path, rel_path: str, cell: str, tagged: bool) -> list[dict]:
    """Row-group index rows (m1-contracts.md section 4) for one node
    spatial file, read from its own Parquet footer: for nodes, xmin_e7/
    ymax_e7/etc. are the min/max of lon_e7/lat_e7."""
    md = pq.ParquetFile(str(path)).metadata
    rows = []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        lon_st, lat_st = _column_stats(rg, "lon_e7"), _column_stats(rg, "lat_e7")
        if lon_st is None or lat_st is None or not lon_st.has_min_max or not lat_st.has_min_max:
            continue
        rows.append({
            "path": rel_path, "cell": cell, "tagged": tagged, "rg": i, "rows": rg.num_rows,
            "xmin_e7": int(lon_st.min), "ymin_e7": int(lat_st.min),
            "xmax_e7": int(lon_st.max), "ymax_e7": int(lat_st.max),
        })
    return rows


def _bbox_rowgroup_rows(path: Path, rel_path: str, cell: str) -> list[dict]:
    """Row-group index rows for one way/relation spatial file: the envelope
    (min of xmin_e7/ymin_e7, max of xmax_e7/ymax_e7) of every row in the
    group. A row group whose rows all have NULL bbox (no resolvable
    geometry) contributes no row -- `catalog.prune_files_by_bbox` treats a
    file with no index entries as "keep", the same safe default as v1."""
    md = pq.ParquetFile(str(path)).metadata
    rows = []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        stats = {c: _column_stats(rg, c) for c in ("xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7")}
        if any(s is None or not s.has_min_max for s in stats.values()):
            continue
        rows.append({
            "path": rel_path, "cell": cell, "tagged": None, "rg": i, "rows": rg.num_rows,
            "xmin_e7": int(stats["xmin_e7"].min), "ymin_e7": int(stats["ymin_e7"].min),
            "xmax_e7": int(stats["xmax_e7"].max), "ymax_e7": int(stats["ymax_e7"].max),
        })
    return rows


@dataclass
class FixtureInfo:
    root: str
    leaf_bbox: dict = field(default_factory=dict)
    ancestor_bbox: tuple = None
    # well-known ids for tests
    cafe_node_id: int = 1
    cafe_node2_id: int = 2
    restaurant_node_id: int = 3
    untagged_node_in_cafe_cell: int = 4
    bakery_node_id: int = 5  # promoted key ("shop")
    craft_bakery_node_id: int = 6  # non-promoted key ("craft"), same semantics
    mixed_case_name_node_id: int = 7  # for case-insensitive regex test
    closed_way_id: int = 101
    open_way_ids: list = field(default_factory=list)
    spanning_way_id: int = 110
    leaf_relation_id: int = 201
    spanning_relation_id: int = 202
    node_way_relation_id: int = 203
    # far_way_relation_id (204): stored at ancestor "00" (like
    # spanning_relation_id) but its *way* member (far_way_id, 111) and that
    # way's nodes all live entirely in leaf "002" -- a different leaf than
    # some of the relation's own members. Exercises `>`/`>>` resolving a
    # relation's member way to its nodes via byid regardless of which cell
    # anything is stored in (recurse.py never consults `cell`).
    far_way_relation_id: int = 204
    far_way_id: int = 111
    far_way_node_ids: list = field(default_factory=list)
    far_way_relation_label_node_id: int = 0  # the relation's other (node) member
    # nested_relation_id (205): a relation whose only member is
    # far_way_relation_id (204) -- a relation-of-a-relation, for `>>`.
    nested_relation_id: int = 205
    # way_only_relation_id (206): a relation whose only member is
    # spanning_way_id (110), itself with no direct member of any node --
    # only reachable via "relations that have a *found way* as a member",
    # the second hop `<` was missing.
    way_only_relation_id: int = 206
    # diagonal_way_id (112): a single straight segment across leaf "000"
    # from its SW corner to its NE corner, i.e. its stored bbox covers the
    # whole leaf even though the line itself never visits most of it --
    # for the exact (ST_Intersects) bbox test vs. the flat-bbox prune.
    diagonal_way_id: int = 112
    diagonal_node_ids: list = field(default_factory=list)  # [sw, ne]
    # A bbox in leaf "000"'s NW quadrant: the diagonal way's flat bbox
    # overlaps it, but its actual geometry (the SW->NE segment) never does.
    diagonal_bbox_miss: tuple = None
    # A bbox straddling the middle of the diagonal: the geometry does
    # intersect this one.
    diagonal_bbox_hit: tuple = None
    # diagonal_relation_id (207): a relation whose only member is
    # diagonal_way_id -- for the same exact-bbox test, but for relations
    # (member way resolution instead of the relation's own geometry, which
    # is always NULL).
    diagonal_relation_id: int = 207
    all_node_ids: list = field(default_factory=list)
    all_way_ids: list = field(default_factory=list)
    all_relation_ids: list = field(default_factory=list)
    total_bbox: tuple = None  # (s, w, n, e) covering everything
    # -- manifest_version=2 only (all None/empty in v1 mode) --------------
    manifest_version: int = 1
    trap_way_id: int = 0  # placed at TRAP_WAY_CELL ("300"), not "3000" (v2 rule)
    trap_leaf_bbox: dict = field(default_factory=dict)  # V2_TRAP_LEAVES -> bbox
    # An untagged node with real metadata (version/changeset/timestamp/uid/
    # user all non-NULL) and one with all of it NULL (mirrors `osmpq
    # raw-py`'s allowed gap, m1-contracts.md section 1) -- for `out meta`'s
    # "omit when NULL, emit when present" rule (section 6).
    trap_node_with_meta_id: int = 0
    trap_node_without_meta_id: int = 0
    # way110 (spanning_way_id) and every relation at old-v1 ancestor "00"
    # (depth 2, not in V2_ANCESTOR_DEPTHS) move to "root" under the v2 rule
    # -- see build()'s v2 cell-placement adjustments. Same ids as v1.
    root_promoted_way_ids: list = field(default_factory=list)
    root_promoted_relation_ids: list = field(default_factory=list)
    # -- manifest_version=3 only (delta tiers, docs/m2-contracts.md) -------
    manifest_replication_source: str = ""
    delta_week_version: int = 0
    delta_day_version: int = 0
    delta_hour_version: int = 0
    # week modifies this tagged node's tags; day re-modifies it (day wins).
    delta_modified_node_id: int = 0
    delta_modified_node_week_tags: dict = field(default_factory=dict)
    delta_modified_node_day_tags: dict = field(default_factory=dict)
    # week moves this untagged node to a different leaf; hour then deletes
    # it outright (hour tombstone beats week's payload).
    delta_moved_deleted_node_id: int = 0
    delta_moved_deleted_node_from_cell: str = ""
    delta_moved_deleted_node_to_cell: str = ""
    # week moves this *other* untagged node to a different leaf and nothing
    # touches it again -- for "moved node shows only in the new cell".
    delta_moved_only_node_id: int = 0
    delta_moved_only_node_from_cell: str = ""
    delta_moved_only_node_to_cell: str = ""
    # week deletes this way; `delta_deleted_way_surviving_partner_way_id`
    # shares one node (ref) with it, to prove the node itself survives.
    delta_deleted_way_id: int = 0
    delta_deleted_way_cell: str = ""
    delta_deleted_way_surviving_partner_way_id: int = 0
    delta_deleted_way_shared_node_id: int = 0
    # week creates this new way, spanning two leaves -> placed at an
    # allowed ancestor cell, same promotion rule as spanning_way_id.
    delta_new_way_id: int = 0
    delta_new_way_cell: str = ""
    delta_new_way_refs: list = field(default_factory=list)
    # day creates this new relation, whose member way is delta_new_way_id.
    delta_new_relation_id: int = 0
    delta_new_relation_cell: str = ""
    delta_new_relation_way_member_id: int = 0
    delta_new_relation_node_member_id: int = 0
    # hour modifies this way's refs (geometry changes) and bumps version.
    delta_modified_refs_way_id: int = 0
    delta_modified_refs_way_new_refs: list = field(default_factory=list)
    delta_modified_refs_way_new_version: int = 0
    # week creates this way at an *allowed ancestor cell the base has no
    # file for at all* (depth 3, a sibling of TRAP_WAY_CELL with no leaves
    # declared beneath it) -- docs/m2-contracts.md follow-up: the updater
    # can place a new element somewhere the base has never written to, and
    # a bbox query must still find it via `deltas.<tier>.cells`.
    delta_new_way_no_base_cell_id: int = 0
    delta_new_way_no_base_cell: str = ""
    delta_new_way_no_base_cell_bbox: tuple = None
    delta_new_way_no_base_cell_refs: list = field(default_factory=list)


def build(
    root_dir: str,
    con: duckdb.DuckDBPyConnection | None = None,
    manifest_version: int = 1,
) -> FixtureInfo:
    """`manifest_version=1` (default): exactly the original fixture,
    unchanged, for every existing test. `manifest_version=2`: the same
    logical nodes/ways/relations (same ids/tags/geometry -- see the "v2
    cell-placement adjustments" block below), a brand-new disjoint trap
    region for the ancestor-depth rule, metadata on untagged nodes
    (including one deliberately all-NULL), and the row-group index side
    files, per docs/m1-contracts.md sections 2/4/5."""
    if manifest_version not in (1, 2, 3):
        raise ValueError(f"unsupported manifest_version {manifest_version!r}")
    root = Path(root_dir)
    root.mkdir(parents=True, exist_ok=True)
    own_con = con is None
    if con is None:
        con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    leaf_bbox = {c: cell_bbox(c) for c in LEAVES}
    ancestor_bbox = cell_bbox(ANCESTOR)
    info = FixtureInfo(root=str(root), leaf_bbox=leaf_bbox, ancestor_bbox=ancestor_bbox,
                        manifest_version=manifest_version)
    null_meta_node_ids: set[int] = set()

    # ---------------------------------------------------------------- nodes
    # Deterministic placement: for the i-th node in a cell, spread it inside
    # the cell's bbox on a small inset grid (never on an edge).
    nodes = []  # list of dict(id, lon, lat, cell, tags)

    def add_node(node_id: int, cell: str, idx: int, count: int, tags: dict | None):
        frac = 0.15 + 0.7 * (idx / max(1, count - 1)) if count > 1 else 0.5
        lon, lat = _inset_point(leaf_bbox[cell], frac_lat=frac, frac_lon=1 - frac)
        nodes.append({"id": node_id, "lon": lon, "lat": lat, "cell": cell, "tags": tags})

    next_id = 1

    def alloc() -> int:
        nonlocal next_id
        v = next_id
        next_id += 1
        return v

    # Well-known tagged nodes in leaf "000" (ids assigned in this order: 1..7)
    assert alloc() == 1
    add_node(1, "000", 0, 12, {"amenity": "cafe", "name": "Aroma Cafe"})
    assert alloc() == 2
    add_node(2, "000", 1, 12, {"amenity": "cafe", "cuisine": "coffee_shop"})
    assert alloc() == 3
    add_node(3, "000", 2, 12, {"amenity": "restaurant"})
    assert alloc() == 4
    add_node(4, "000", 3, 12, None)  # untagged, used for the [amenity!=cafe] test
    assert alloc() == 5
    add_node(5, "000", 4, 12, {"shop": "bakery"})  # promoted key
    assert alloc() == 6
    add_node(6, "000", 5, 12, {"craft": "bakery"})  # equivalent, non-promoted key
    assert alloc() == 7
    add_node(7, "000", 6, 12, {"name": "Coffee HOUSE"})  # case-insensitive regex target

    # Remaining nodes in leaf "000" to round it out (some tagged, mostly not)
    for i in range(7, 12):
        node_id = alloc()
        tags = {"natural": "tree"} if i == 7 else None
        add_node(node_id, "000", i, 12, tags)

    # A ring of 4 more nodes dedicated to the closed way (building corners)
    ring_ids = [alloc() for _ in range(4)]
    ring_bbox = leaf_bbox["000"]
    ring_coords = [(0.30, 0.30), (0.30, 0.40), (0.40, 0.40), (0.40, 0.30)]
    for nid, (flat, flon) in zip(ring_ids, ring_coords):
        lon, lat = _inset_point(ring_bbox, frac_lat=flat, frac_lon=flon)
        nodes.append({"id": nid, "lon": lon, "lat": lat, "cell": "000", "tags": None})

    leaf000_extra_ids = [alloc() for _ in range(3)]
    for j, nid in enumerate(leaf000_extra_ids):
        add_node(nid, "000", 8 + j, 12, None)

    # Leaf "001": 12 nodes, mostly untagged plus a couple tagged
    leaf001_ids = [alloc() for _ in range(12)]
    for i, nid in enumerate(leaf001_ids):
        tags = None
        if i == 0:
            tags = {"public_transport": "platform"}
        if i == 1:
            tags = {"highway": "bus_stop"}
        add_node(nid, "001", i, len(leaf001_ids), tags)

    # Leaf "002": 10 nodes, mostly untagged plus one tagged
    leaf002_ids = [alloc() for _ in range(10)]
    for i, nid in enumerate(leaf002_ids):
        tags = {"amenity": "cafe"} if i == 0 else None
        add_node(nid, "002", i, len(leaf002_ids), tags)

    # Two untagged nodes in leaf "002", dedicated to `far_way_id` (111):
    # a way member of `far_way_relation_id` (204), which is stored at
    # ancestor "00" -- a different cell than these nodes' own leaf.
    far_way_node_ids = [alloc() for _ in range(2)]
    for i, nid in enumerate(far_way_node_ids):
        add_node(nid, "002", i + len(leaf002_ids), len(leaf002_ids) + 2, None)
    info.far_way_node_ids = far_way_node_ids

    # Two untagged nodes at the SW and NE corners of leaf "000", dedicated
    # to `diagonal_way_id` (112): a single segment whose flat bbox covers
    # the whole leaf but whose actual line only ever visits the diagonal.
    diagonal_bbox = leaf_bbox["000"]
    sw_lon, sw_lat = _inset_point(diagonal_bbox, frac_lat=0.1, frac_lon=0.1)
    ne_lon, ne_lat = _inset_point(diagonal_bbox, frac_lat=0.9, frac_lon=0.9)
    diagonal_sw_id, diagonal_ne_id = alloc(), alloc()
    nodes.append({"id": diagonal_sw_id, "lon": sw_lon, "lat": sw_lat, "cell": "000", "tags": None})
    nodes.append({"id": diagonal_ne_id, "lon": ne_lon, "lat": ne_lat, "cell": "000", "tags": None})
    info.diagonal_node_ids = [diagonal_sw_id, diagonal_ne_id]

    def _sub_bbox(frac_s: float, frac_w: float, frac_n: float, frac_e: float):
        s, w, n, e = diagonal_bbox
        return (
            s + (n - s) * frac_s,
            w + (e - w) * frac_w,
            s + (n - s) * frac_n,
            w + (e - w) * frac_e,
        )

    # NW quadrant of the leaf: overlaps the diagonal way's flat bbox
    # (lat/lon both span [0.1, 0.9]) but the SW->NE segment (lat_frac ==
    # lon_frac throughout) never enters a region where lat is high and lon
    # is low.
    info.diagonal_bbox_miss = _sub_bbox(0.6, 0.1, 0.9, 0.4)
    # Straddles the middle of the diagonal (around the (0.5, 0.5) point).
    info.diagonal_bbox_hit = _sub_bbox(0.4, 0.4, 0.6, 0.6)

    info.all_node_ids = [n["id"] for n in nodes]
    info.untagged_node_in_cafe_cell = leaf000_extra_ids[0]

    # ---------------------------------------------- v2 trap region (nodes)
    # Deliberately not folded into `info.all_node_ids`/the byid id-range
    # tests above: this whole region only exists in manifest_version=2, in
    # a part of the quadtree v1 never uses, precisely so the same queries
    # against v1 and v2 fixtures built from everything above stay
    # equivalent (test_engine_v2.py).
    if manifest_version >= 2:
        trap_leaf_bbox = {c: cell_bbox(c) for c in V2_TRAP_LEAVES}
        info.trap_leaf_bbox = trap_leaf_bbox
        trap_with_meta_id = alloc()
        lon, lat = _inset_point(trap_leaf_bbox["30000"], frac_lat=0.5, frac_lon=0.5)
        nodes.append({"id": trap_with_meta_id, "lon": lon, "lat": lat, "cell": "30000", "tags": None})
        trap_without_meta_id = alloc()
        lon, lat = _inset_point(trap_leaf_bbox["30001"], frac_lat=0.5, frac_lon=0.5)
        nodes.append({"id": trap_without_meta_id, "lon": lon, "lat": lat, "cell": "30001", "tags": None})
        null_meta_node_ids.add(trap_without_meta_id)
        info.trap_node_with_meta_id = trap_with_meta_id
        info.trap_node_without_meta_id = trap_without_meta_id

    # ----------------------------------------------------------------- ways
    ways = []  # dict(id, refs, tags, cell)

    # way 101: closed (building), ring of 4 nodes + repeat first
    closed_refs = ring_ids + [ring_ids[0]]
    ways.append({"id": 101, "refs": closed_refs, "tags": {"building": "yes"}, "cell": "000"})

    # ways 102-104: open ways within leaf "000"
    ways.append({"id": 102, "refs": [1, 4, 8], "tags": {"highway": "residential"}, "cell": "000"})
    ways.append({"id": 103, "refs": [2, 5], "tags": {}, "cell": "000"})  # untagged way (skel test)
    ways.append({"id": 104, "refs": [3, 6, 9], "tags": {"highway": "footway"}, "cell": "000"})

    # ways 105-107: leaf "001"
    ways.append({"id": 105, "refs": leaf001_ids[0:3], "tags": {"highway": "footway"}, "cell": "001"})
    ways.append({"id": 106, "refs": leaf001_ids[2:5], "tags": {"waterway": "stream"}, "cell": "001"})
    ways.append({"id": 107, "refs": leaf001_ids[4:6], "tags": {}, "cell": "001"})

    # ways 108-109: leaf "002"
    ways.append({"id": 108, "refs": leaf002_ids[0:3], "tags": {"landuse": "grass"}, "cell": "002"})
    ways.append({"id": 109, "refs": leaf002_ids[3:5], "tags": {}, "cell": "002"})

    # way 110: spans leaf "000" and leaf "002" -> ancestor cell "00"
    ways.append({"id": 110, "refs": [1, leaf002_ids[0]], "tags": {"railway": "rail"}, "cell": "00"})

    # way 111: far_way_id, untagged, entirely within leaf "002" -- the way
    # member of far_way_relation_id (204), which itself lives at ancestor "00".
    ways.append({"id": 111, "refs": far_way_node_ids, "tags": {}, "cell": "002"})

    # way 112: diagonal_way_id, a single segment across leaf "000" from its
    # SW corner to its NE corner (see the node comment above).
    ways.append({"id": 112, "refs": [diagonal_sw_id, diagonal_ne_id], "tags": {}, "cell": "000"})

    # way TRAP_WAY_ID (v2 only): straddles trap leaves "30000"/"30001" (two
    # children of "3000", depth 4) -- see the V2_TRAP_LEAVES comment above.
    if manifest_version >= 2:
        ways.append({
            "id": TRAP_WAY_ID,
            "refs": [info.trap_node_with_meta_id, info.trap_node_without_meta_id],
            "tags": {"highway": "track"},
            "cell": TRAP_WAY_CELL,
        })
        info.trap_way_id = TRAP_WAY_ID

    info.open_way_ids = [w["id"] for w in ways if w["id"] != 101]
    info.all_way_ids = [w["id"] for w in ways]

    node_by_id = {n["id"]: n for n in nodes}

    def way_geometry_and_bbox(refs: list[int]):
        pts = [(node_by_id[r]["lon"], node_by_id[r]["lat"]) for r in refs if r in node_by_id]
        if len(pts) < 2:
            return None, (None, None, None, None)
        xmin = min(p[0] for p in pts)
        xmax = max(p[0] for p in pts)
        ymin = min(p[1] for p in pts)
        ymax = max(p[1] for p in pts)
        wkt = "LINESTRING (" + ", ".join(f"{lon} {lat}" for lon, lat in pts) + ")"
        return wkt, (xmin, ymin, xmax, ymax)

    for w in ways:
        wkt, (xmin, ymin, xmax, ymax) = way_geometry_and_bbox(w["refs"])
        w["geometry_wkt"] = wkt
        w["xmin"], w["ymin"], w["xmax"], w["ymax"] = xmin, ymin, xmax, ymax
        refs = w["refs"]
        w["is_closed"] = len(refs) >= 4 and refs[0] == refs[-1]
        has_area_no = w["tags"].get("area") == "no"
        has_linear_tag = ("highway" in w["tags"] or "barrier" in w["tags"]) and w["tags"].get("area") != "yes"
        w["is_area"] = bool(w["is_closed"] and not has_area_no and not has_linear_tag)

    # ------------------------------------------------------------ relations
    relations = []
    # rel 201: fully inside leaf "000" (way + node member)
    relations.append({
        "id": 201, "cell": "000",
        "members": [{"type": "w", "ref": 101, "role": "outer"}, {"type": "n", "ref": 1, "role": "label"}],
        "tags": {"type": "multipolygon", "building": "yes"},
    })
    # rel 202: spans leaf "000" (way 101) and leaf "002" (node) -> ancestor "00"
    relations.append({
        "id": 202, "cell": "00",
        "members": [{"type": "w", "ref": 101, "role": "outer"}, {"type": "n", "ref": leaf002_ids[0], "role": "label"}],
        "tags": {"route": "bicycle", "name": "Cross-town Loop"},
    })
    # rel 203: fully inside leaf "001" (node + way member), tests member roles
    relations.append({
        "id": 203, "cell": "001",
        "members": [{"type": "n", "ref": leaf001_ids[0], "role": "stop"}, {"type": "w", "ref": 105, "role": "platform"}],
        "tags": {"public_transport": "stop_area", "name": "Downtown Stop"},
    })
    # rel 204: far_way_relation_id -- way 111 (leaf "002") + a node in leaf
    # "000" -> ancestor "00", like rel 202, but the way's nodes (111's refs)
    # are only reachable via byid, not via the relation's own cell.
    relations.append({
        "id": 204, "cell": "00",
        "members": [{"type": "w", "ref": 111, "role": "outer"}, {"type": "n", "ref": leaf000_extra_ids[1], "role": "label"}],
        "tags": {"type": "multipolygon", "natural": "water"},
    })
    info.far_way_relation_label_node_id = leaf000_extra_ids[1]
    # rel 205: nested_relation_id -- a relation whose only member is
    # relation 204 (a relation-of-a-relation, for `>>`).
    relations.append({
        "id": 205, "cell": "00",
        "members": [{"type": "r", "ref": 204, "role": "outer"}],
        "tags": {"type": "multipolygon", "leisure": "park"},
    })
    # rel 206: way_only_relation_id -- its only member is way 110 (which
    # itself has node 1 as a ref); it has no direct node/way member of
    # node 1, so it's only reachable from node 1 via the second `<` hop
    # ("relations that have a *found way* as a member").
    relations.append({
        "id": 206, "cell": "00",
        "members": [{"type": "w", "ref": 110, "role": "outer"}],
        "tags": {"type": "multipolygon", "railway": "rail"},
    })
    # rel 207: diagonal_relation_id -- its only member is the diagonal way
    # 112, for the same exact-bbox test as the way itself, but through
    # relation member resolution (relation rows carry no geometry of
    # their own in M0).
    relations.append({
        "id": 207, "cell": "000",
        "members": [{"type": "w", "ref": 112, "role": "outer"}],
        "tags": {"type": "multipolygon", "leisure": "park"},
    })
    info.all_relation_ids = [r["id"] for r in relations]

    def relation_bbox(members: list[dict]):
        xs, ys = [], []
        for m in members:
            if m["type"] == "n" and m["ref"] in node_by_id:
                n = node_by_id[m["ref"]]
                xs.append(n["lon"]); ys.append(n["lat"])
            elif m["type"] == "w":
                w = next((w for w in ways if w["id"] == m["ref"]), None)
                if w and w["xmin"] is not None:
                    xs.extend([w["xmin"], w["xmax"]]); ys.extend([w["ymin"], w["ymax"]])
            elif m["type"] == "r":
                # One-level nested relation bbox (contract section 4): the
                # referenced relation must already have its own bbox
                # computed -- true as long as it appears earlier in
                # `relations` than this one, which it does here.
                rr = next((rr for rr in relations if rr["id"] == m["ref"]), None)
                if rr is not None and rr.get("xmin") is not None:
                    xs.extend([rr["xmin"], rr["xmax"]]); ys.extend([rr["ymin"], rr["ymax"]])
        if not xs:
            return (None, None, None, None)
        return (min(xs), min(ys), max(xs), max(ys))

    for r in relations:
        r["xmin"], r["ymin"], r["xmax"], r["ymax"] = relation_bbox(r["members"])

    # ------------------------------------- v2 cell-placement adjustments
    # Everything above is identical to v1 (same ids/tags/geometry/bbox).
    # The only thing manifest_version=2 changes for the *existing* v1
    # elements is where loose placement files them: ancestor "00" is depth
    # 2, which is not in V2_ANCESTOR_DEPTHS, so anything the v1 rule placed
    # there (way 110, relations 202/204/205/206 -- all "descend from root
    # while exactly one child fully contains the bbox" stopped at "00"
    # because their members straddle two of "00"'s own children) promotes
    # to the greatest allowed depth <= 2, which is 0 ("root"). This is the
    # "one long way at root" case the m1-contracts.md task calls for --
    # way 110 already spans two leaves, hence "00" under v1 in the first
    # place. TRAP_WAY_CELL ("300") above is the other case: a way whose v1
    # cell (a depth-4 ancestor with no equivalent under v1's 3-leaf-deep
    # topology) doesn't exist in this fixture at all, only in the v2 trap
    # region.
    if manifest_version >= 2:
        for w in ways:
            if w["cell"] == "00":
                w["cell"] = "root"
                info.root_promoted_way_ids.append(w["id"])
        for r in relations:
            if r["cell"] == "00":
                r["cell"] = "root"
                info.root_promoted_relation_ids.append(r["id"])

    all_lons = [n["lon"] for n in nodes]
    all_lats = [n["lat"] for n in nodes]
    info.total_bbox = (min(all_lats), min(all_lons), max(all_lats), max(all_lons))

    # ---------------------------------------------------------- write files
    manifest_leaf_cells = list(LEAVES) + (list(V2_TRAP_LEAVES) if manifest_version >= 2 else [])
    manifest: dict = {
        "manifest_version": manifest_version,
        "generation": GEN,
        "schema_version": 1,
        "coordinate_scale": 10000000,
        "promoted_keys": PROMOTED_KEYS,
        "timestamp_osm_base": TIMESTAMP_OSM_BASE,
        "replication_sequence": 1,
        "source": "synthetic engine-test fixture (tests/fixtures/make_fixture.py)",
        "extent": list(info.total_bbox),
        "leaf_cells": manifest_leaf_cells,
        "tables": {"node": {"cells": {}}, "way": {"cells": {}}, "relation": {"cells": {}}},
        "byid": {"node": [], "way": [], "relation": []},
        "index": {"node_way": [], "member": []},
    }
    if manifest_version >= 2:
        manifest["ancestor_depths"] = list(V2_ANCESTOR_DEPTHS)
        manifest["max_depth"] = V2_MAX_DEPTH
        manifest["producer"] = {"raw": "osmpq raw-py", "build": "osmpq 0.0.1 (tests/fixtures/make_fixture.py)"}
        # rowgroup_index paths are filled in once the spatial files (and
        # their row-group footers) exist -- see the bottom of this function.

    def meta_cols_sql(i: int, force_null: bool = False) -> str:
        if force_null:
            return (
                'NULL::INTEGER AS version, NULL::BIGINT AS changeset, '
                'NULL::TIMESTAMP AS "timestamp", NULL::INTEGER AS uid, NULL::VARCHAR AS "user"'
            )
        return (
            f"{2 + (i % 5)} AS version, {9000 + i} AS changeset, "
            f"TIMESTAMP '2026-09-01 12:00:00' + INTERVAL ({i}) MINUTE AS \"timestamp\", "
            f"{500 + (i % 3)} AS uid, 'tester{i % 3}' AS \"user\""
        )

    def tags_literal(tags: dict | None) -> str:
        if not tags:
            return "NULL::MAP(VARCHAR, VARCHAR)"
        pairs = ", ".join(f"'{k}': '{v}'" for k, v in tags.items())
        return f"MAP {{{pairs}}}"

    def promoted_cols_sql(tags: dict | None) -> str:
        tags = tags or {}
        return ", ".join(
            (f"'{tags[k]}'::VARCHAR AS \"{k}\"" if k in tags else f"NULL::VARCHAR AS \"{k}\"")
            for k in PROMOTED_KEYS
        )

    # -- node spatial partitions (tagged / untagged), per cell actually used
    # (LEAVES for v1; LEAVES + V2_TRAP_LEAVES for v2, but derived from the
    # data rather than hardcoded so this needs no separate v1/v2 branch).
    node_cells = sorted({n["cell"] for n in nodes})
    for cell in node_cells:
        cell_nodes = [n for n in nodes if n["cell"] == cell]
        for tagged in (True, False):
            part = [n for n in cell_nodes if bool(n["tags"]) == tagged]
            if not part:
                continue
            rows_sql = []
            for i, n in enumerate(part):
                lat_e7, lon_e7 = to_e7(n["lat"]), to_e7(n["lon"])
                h = lonlat_to_hilbert(n["lon"], n["lat"])
                rows_sql.append(
                    f"SELECT {n['id']} AS id, {lat_e7} AS lat_e7, {lon_e7} AS lon_e7, "
                    f"{tags_literal(n['tags'])} AS tags, {promoted_cols_sql(n['tags'])}, "
                    f"{meta_cols_sql(n['id'], force_null=n['id'] in null_meta_node_ids)}, {h}::UBIGINT AS hilbert"
                )
            sql = " UNION ALL ".join(rows_sql)
            rel_path = f"spatial/{GEN}/node/cell={cell}/tagged={'true' if tagged else 'false'}/part-0.parquet"
            out_path = root / rel_path
            out_path.parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT PARQUET)")
            manifest["tables"]["node"]["cells"].setdefault(cell, {})[
                "tagged" if tagged else "untagged"
            ] = {"path": rel_path, "rows": len(part), "bytes": out_path.stat().st_size}

    # -- way spatial files, one per cell that has ways (leaves + ancestor)
    way_cells = sorted({w["cell"] for w in ways})
    for cell in way_cells:
        part = [w for w in ways if w["cell"] == cell]
        rows_sql = []
        for w in part:
            geom_expr = f"ST_GeomFromText('{w['geometry_wkt']}')" if w["geometry_wkt"] else "NULL::GEOMETRY"
            if w["xmin"] is not None:
                xmin_e7 = to_e7(w["xmin"])
                ymin_e7 = to_e7(w["ymin"])
                xmax_e7 = to_e7(w["xmax"])
                ymax_e7 = to_e7(w["ymax"])
                centroid_lat_e7 = to_e7((w["ymin"] + w["ymax"]) / 2)
                centroid_lon_e7 = to_e7((w["xmin"] + w["xmax"]) / 2)
                h = bbox_e7_center_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7)
            else:
                xmin_e7 = ymin_e7 = xmax_e7 = ymax_e7 = "NULL::INTEGER"
                centroid_lat_e7 = centroid_lon_e7 = "NULL::INTEGER"
                h = 0
            refs_literal = "[" + ", ".join(str(r) for r in w["refs"]) + "]::BIGINT[]"
            rows_sql.append(
                f"SELECT {w['id']} AS id, {refs_literal} AS refs, "
                f"{tags_literal(w['tags'])} AS tags, {promoted_cols_sql(w['tags'])}, "
                f"{meta_cols_sql(w['id'])}, "
                f"{xmin_e7} AS xmin_e7, {ymin_e7} AS ymin_e7, {xmax_e7} AS xmax_e7, {ymax_e7} AS ymax_e7, "
                f"{geom_expr} AS geometry, {str(w['is_closed']).upper()} AS is_closed, "
                f"{str(w['is_area']).upper()} AS is_area, "
                f"{centroid_lat_e7} AS centroid_lat_e7, {centroid_lon_e7} AS centroid_lon_e7, "
                f"{h}::UBIGINT AS hilbert"
            )
        sql = " UNION ALL ".join(rows_sql)
        rel_path = f"spatial/{GEN}/way/cell={cell}/part-0.parquet"
        out_path = root / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT PARQUET)")
        bbox = None
        xs = [w["xmin"] for w in part if w["xmin"] is not None]
        if xs:
            bbox = [
                min(w["ymin"] for w in part if w["ymin"] is not None),
                min(w["xmin"] for w in part if w["xmin"] is not None),
                max(w["ymax"] for w in part if w["ymax"] is not None),
                max(w["xmax"] for w in part if w["xmax"] is not None),
            ]
        manifest["tables"]["way"]["cells"][cell] = {
            "path": rel_path, "rows": len(part), "bytes": out_path.stat().st_size, "bbox": bbox,
        }

    # -- relation spatial files
    relation_cells = sorted({r["cell"] for r in relations})
    for cell in relation_cells:
        part = [r for r in relations if r["cell"] == cell]
        rows_sql = []
        for r in part:
            members_literal = (
                "["
                + ", ".join(
                    f"{{'type': '{m['type']}', 'ref': {m['ref']}, 'role': '{m['role']}'}}" for m in r["members"]
                )
                + "]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]"
            )
            if r["xmin"] is not None:
                xmin_e7, ymin_e7, xmax_e7, ymax_e7 = (
                    to_e7(r["xmin"]), to_e7(r["ymin"]), to_e7(r["xmax"]), to_e7(r["ymax"]),
                )
                centroid_lat_e7 = to_e7((r["ymin"] + r["ymax"]) / 2)
                centroid_lon_e7 = to_e7((r["xmin"] + r["xmax"]) / 2)
                h = bbox_e7_center_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7)
            else:
                xmin_e7 = ymin_e7 = xmax_e7 = ymax_e7 = "NULL::INTEGER"
                centroid_lat_e7 = centroid_lon_e7 = "NULL::INTEGER"
                h = 0
            rows_sql.append(
                f"SELECT {r['id']} AS id, {members_literal} AS members, "
                f"{tags_literal(r['tags'])} AS tags, {promoted_cols_sql(r['tags'])}, "
                f"{meta_cols_sql(r['id'])}, "
                f"{xmin_e7} AS xmin_e7, {ymin_e7} AS ymin_e7, {xmax_e7} AS xmax_e7, {ymax_e7} AS ymax_e7, "
                f"NULL::GEOMETRY AS geometry, "
                f"{centroid_lat_e7} AS centroid_lat_e7, {centroid_lon_e7} AS centroid_lon_e7, "
                f"{h}::UBIGINT AS hilbert"
            )
        sql = " UNION ALL ".join(rows_sql)
        rel_path = f"spatial/{GEN}/relation/cell={cell}/part-0.parquet"
        out_path = root / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT PARQUET)")
        bbox = None
        xs = [r["xmin"] for r in part if r["xmin"] is not None]
        if xs:
            bbox = [
                min(r["ymin"] for r in part if r["ymin"] is not None),
                min(r["xmin"] for r in part if r["xmin"] is not None),
                max(r["ymax"] for r in part if r["ymax"] is not None),
                max(r["xmax"] for r in part if r["xmax"] is not None),
            ]
        manifest["tables"]["relation"]["cells"][cell] = {
            "path": rel_path, "rows": len(part), "bytes": out_path.stat().st_size, "bbox": bbox,
        }

    # ---------------------------------------------------------------- byid
    node_to_cell = {n["id"]: n["cell"] for n in nodes}
    sorted_nodes = sorted(nodes, key=lambda n: n["id"])
    mid = len(sorted_nodes) // 2
    node_parts = [sorted_nodes[:mid], sorted_nodes[mid:]]
    for k, part in enumerate(node_parts):
        if not part:
            continue
        rows_sql = []
        for n in part:
            lat_e7, lon_e7 = to_e7(n["lat"]), to_e7(n["lon"])
            rows_sql.append(
                f"SELECT {n['id']} AS id, {lat_e7} AS lat_e7, {lon_e7} AS lon_e7, "
                f"{tags_literal(n['tags'])} AS tags, {promoted_cols_sql(n['tags'])}, "
                f"{meta_cols_sql(n['id'], force_null=n['id'] in null_meta_node_ids)}, '{n['cell']}' AS cell"
            )
        sql = " UNION ALL ".join(rows_sql)
        rel_path = f"byid/{GEN}/node/part-{k:05d}.parquet"
        out_path = root / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT PARQUET)")
        ids = [n["id"] for n in part]
        manifest["byid"]["node"].append(
            {"path": rel_path, "min_id": min(ids), "max_id": max(ids), "rows": len(part),
             "bytes": out_path.stat().st_size}
        )

    def write_byid_way_or_relation(kind: str, items: list[dict]):
        rows_sql = []
        for it in items:
            if kind == "way":
                refs_literal = "[" + ", ".join(str(r) for r in it["refs"]) + "]::BIGINT[]"
                extra = f"{refs_literal} AS refs"
            else:
                members_literal = (
                    "["
                    + ", ".join(
                        f"{{'type': '{m['type']}', 'ref': {m['ref']}, 'role': '{m['role']}'}}"
                        for m in it["members"]
                    )
                    + "]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]"
                )
                extra = f"{members_literal} AS members"
            xmin_e7 = to_e7(it["xmin"]) if it["xmin"] is not None else "NULL::INTEGER"
            ymin_e7 = to_e7(it["ymin"]) if it["ymin"] is not None else "NULL::INTEGER"
            xmax_e7 = to_e7(it["xmax"]) if it["xmax"] is not None else "NULL::INTEGER"
            ymax_e7 = to_e7(it["ymax"]) if it["ymax"] is not None else "NULL::INTEGER"
            extra_flags = ""
            if kind == "way":
                extra_flags = f", {str(it['is_closed']).upper()} AS is_closed, {str(it['is_area']).upper()} AS is_area"
            rows_sql.append(
                f"SELECT {it['id']} AS id, {extra}, "
                f"{tags_literal(it['tags'])} AS tags, {promoted_cols_sql(it['tags'])}, "
                f"{meta_cols_sql(it['id'])}, "
                f"{xmin_e7} AS xmin_e7, {ymin_e7} AS ymin_e7, {xmax_e7} AS xmax_e7, {ymax_e7} AS ymax_e7"
                f"{extra_flags}, '{it['cell']}' AS cell"
            )
        sql = " UNION ALL ".join(rows_sql)
        rel_path = f"byid/{GEN}/{kind}/part-00000.parquet"
        out_path = root / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT PARQUET)")
        ids = [it["id"] for it in items]
        manifest["byid"][kind].append(
            {"path": rel_path, "min_id": min(ids), "max_id": max(ids), "rows": len(items),
             "bytes": out_path.stat().st_size}
        )

    write_byid_way_or_relation("way", ways)
    write_byid_way_or_relation("relation", relations)

    # ------------------------------------------------------------- indexes
    node_way_rows = []
    for w in ways:
        for r in w["refs"]:
            node_way_rows.append((r, w["id"]))
    node_way_rows.sort()
    if node_way_rows:
        rows_sql = " UNION ALL ".join(f"SELECT {nid} AS node_id, {wid} AS way_id" for nid, wid in node_way_rows)
        rel_path = f"index/{GEN}/node_way/part-00000.parquet"
        out_path = root / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({rows_sql}) TO '{out_path}' (FORMAT PARQUET)")
        manifest["index"]["node_way"].append(
            {"path": rel_path, "min_id": min(n for n, _ in node_way_rows),
             "max_id": max(n for n, _ in node_way_rows), "rows": len(node_way_rows),
             "bytes": out_path.stat().st_size}
        )

    member_rows = []
    for r in relations:
        for m in r["members"]:
            member_rows.append((m["type"], m["ref"], r["id"], m["role"], r["cell"]))
    member_rows.sort()
    if member_rows:
        rows_sql = " UNION ALL ".join(
            f"SELECT '{mt}' AS member_type, {mid} AS member_id, {pid} AS parent_id, "
            f"'{role}' AS role, '{pcell}' AS parent_cell"
            for mt, mid, pid, role, pcell in member_rows
        )
        rel_path = f"index/{GEN}/member/part-00000.parquet"
        out_path = root / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({rows_sql}) TO '{out_path}' (FORMAT PARQUET)")
        manifest["index"]["member"].append(
            {"path": rel_path, "rows": len(member_rows), "bytes": out_path.stat().st_size}
        )

    # ------------------------------------------- row-group index (v2 only)
    # index/<gen>/rowgroups/{node,way,relation}.parquet (m1-contracts.md
    # section 4): one row per Parquet row group of the *spatial* files,
    # computed from the files' own footers with pyarrow (not recomputed
    # from the Python row lists above), exactly as the real Python build
    # stage is expected to do it.
    if manifest_version >= 2:
        rg_rows: dict[str, list[dict]] = {"node": [], "way": [], "relation": []}
        for cell, parts in manifest["tables"]["node"]["cells"].items():
            for tag_key, entry in parts.items():
                rg_rows["node"].extend(
                    _node_rowgroup_rows(root / entry["path"], entry["path"], cell, tag_key == "tagged")
                )
        for cell, entry in manifest["tables"]["way"]["cells"].items():
            rg_rows["way"].extend(_bbox_rowgroup_rows(root / entry["path"], entry["path"], cell))
        for cell, entry in manifest["tables"]["relation"]["cells"].items():
            rg_rows["relation"].extend(_bbox_rowgroup_rows(root / entry["path"], entry["path"], cell))

        manifest["rowgroup_index"] = {}
        for table, rows in rg_rows.items():
            rel_path = f"index/{GEN}/rowgroups/{table}.parquet"
            out_path = root / rel_path
            out_path.parent.mkdir(parents=True, exist_ok=True)
            rows = sorted(rows, key=lambda r: (r["path"], r["rg"]))
            if rows:
                rows_sql = " UNION ALL ".join(
                    f"SELECT '{r['path']}' AS path, '{r['cell']}' AS cell, "
                    f"{'NULL::BOOLEAN' if r['tagged'] is None else str(r['tagged']).upper()} AS tagged, "
                    f"{r['rg']} AS rg, {r['rows']} AS rows, "
                    f"{r['xmin_e7']} AS xmin_e7, {r['ymin_e7']} AS ymin_e7, "
                    f"{r['xmax_e7']} AS xmax_e7, {r['ymax_e7']} AS ymax_e7"
                    for r in rows
                )
            else:
                rows_sql = (
                    "SELECT NULL::VARCHAR AS path, NULL::VARCHAR AS cell, NULL::BOOLEAN AS tagged, "
                    "NULL::INTEGER AS rg, NULL::INTEGER AS rows, NULL::INTEGER AS xmin_e7, "
                    "NULL::INTEGER AS ymin_e7, NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7 WHERE FALSE"
                )
            con.execute(f"COPY ({rows_sql}) TO '{out_path}' (FORMAT PARQUET)")
            manifest["rowgroup_index"][table] = rel_path

        manifest["stats"] = {
            "nodes": len(nodes),
            "tagged_nodes": sum(1 for n in nodes if n["tags"]),
            "ways": len(ways),
            "relations": len(relations),
            "leaf_cells": len(manifest_leaf_cells),
            "bytes": {"spatial": 0, "byid": 0, "index": 0},
        }

    # ---------------------------------------------------- delta tiers (v3)
    # docs/m2-contracts.md sections 3-4: three tiers (week, day, hour) on
    # top of the v2 base above (same node/way/relation ids/tags/geometry),
    # exercising exactly the scenarios M2 asks for: a plain tag modify with
    # tier precedence (day beats week), a moved element (prev_cell != cell,
    # tombstone) later fully deleted (hour tombstone beats week's payload),
    # a separately-moved element that stays live (moved node visible only
    # in its new cell), a deleted way (tombstone, spatial row at
    # cell=prev_cell with NULL payload) whose surviving partner way still
    # reaches their shared node via `>`, a brand-new way spanning two
    # leaves (placed at an allowed ancestor, like `spanning_way_id`), a
    # brand-new relation whose member is that new way, and a way whose
    # refs change (geometry change + version bump).
    if manifest_version == 3:
        REPLICATION_SOURCE = "https://download.openstreetmap.fr/replication/north-america/us-midwest/minute"
        info.manifest_replication_source = REPLICATION_SOURCE

        info.delta_week_version = 1
        info.delta_day_version = 1
        info.delta_hour_version = 1

        info.delta_modified_node_id = info.cafe_node_id  # node 1
        info.delta_modified_node_week_tags = {
            "amenity": "cafe", "name": "Aroma Cafe", "note": "renovating (week)",
        }
        info.delta_modified_node_day_tags = {
            "amenity": "cafe", "name": "Aroma Cafe", "note": "reopened (day)",
        }

        info.delta_moved_deleted_node_id = info.untagged_node_in_cafe_cell  # leaf000_extra_ids[0]
        info.delta_moved_deleted_node_from_cell = "000"
        info.delta_moved_deleted_node_to_cell = "001"

        move_only_node_id = leaf001_ids[6]
        info.delta_moved_only_node_id = move_only_node_id
        info.delta_moved_only_node_from_cell = "001"
        info.delta_moved_only_node_to_cell = "002"

        info.delta_deleted_way_id = 107
        info.delta_deleted_way_cell = "001"
        info.delta_deleted_way_surviving_partner_way_id = 106  # shares leaf001_ids[4]
        info.delta_deleted_way_shared_node_id = leaf001_ids[4]

        info.delta_new_way_id = 9001
        info.delta_new_way_cell = "root"  # spans leaf "000" and leaf "002", like way 110
        info.delta_new_way_refs = [info.cafe_node_id, leaf002_ids[0]]

        info.delta_new_relation_id = 9002
        info.delta_new_relation_cell = "root"
        info.delta_new_relation_way_member_id = info.delta_new_way_id
        info.delta_new_relation_node_member_id = info.cafe_node_id

        info.delta_modified_refs_way_id = 102
        modified_way_extra_node = leaf000_extra_ids[2]  # unused elsewhere
        info.delta_modified_refs_way_new_refs = [1, 4, 8, modified_way_extra_node]
        info.delta_modified_refs_way_new_version = 99

        # A depth-3 ancestor cell with *no* declared leaves beneath it at
        # all (unlike TRAP_WAY_CELL="300", which sits above the real
        # V2_TRAP_LEAVES) and no base way file either -- the base has
        # never written anything there. Two synthetic node ids (not part
        # of the base fixture, only used here to give this one delta way a
        # real, resolvable bbox/geometry) sit inside it.
        NO_BASE_CELL = "301"
        no_base_cell_bbox = cell_bbox(NO_BASE_CELL)
        no_base_node_a_id, no_base_node_b_id = 9201, 9202
        na_lon, na_lat = _inset_point(no_base_cell_bbox, 0.3, 0.3)
        nb_lon, nb_lat = _inset_point(no_base_cell_bbox, 0.7, 0.7)
        node_by_id[no_base_node_a_id] = {"id": no_base_node_a_id, "lon": na_lon, "lat": na_lat,
                                          "cell": NO_BASE_CELL, "tags": None}
        node_by_id[no_base_node_b_id] = {"id": no_base_node_b_id, "lon": nb_lon, "lat": nb_lat,
                                          "cell": NO_BASE_CELL, "tags": None}
        info.delta_new_way_no_base_cell_id = 9203
        info.delta_new_way_no_base_cell = NO_BASE_CELL
        info.delta_new_way_no_base_cell_bbox = no_base_cell_bbox
        info.delta_new_way_no_base_cell_refs = [no_base_node_a_id, no_base_node_b_id]
        assert NO_BASE_CELL not in manifest["tables"]["way"]["cells"], (
            "fixture bug: NO_BASE_CELL must have no base way file"
        )

        # -- generic row -> SQL builders --------------------------------
        def meta_literal_sql(version, changeset, ts, uid, user):
            return (
                f"{version} AS version, {changeset} AS changeset, "
                f"TIMESTAMP '{ts}' AS \"timestamp\", {uid} AS uid, '{user}' AS \"user\""
            )

        def meta_literal_null_sql():
            return (
                'NULL::INTEGER AS version, NULL::BIGINT AS changeset, '
                'NULL::TIMESTAMP AS "timestamp", NULL::INTEGER AS uid, NULL::VARCHAR AS "user"'
            )

        def cell_sql_of(c):
            return f"'{c}'" if c else "NULL::VARCHAR"

        def node_spatial_row_sql(row):
            if row["deleted"]:
                payload = (
                    "NULL::INTEGER AS lat_e7, NULL::INTEGER AS lon_e7, "
                    f"NULL::MAP(VARCHAR, VARCHAR) AS tags, {promoted_cols_sql(None)}, "
                    f"{meta_literal_null_sql()}, NULL::UBIGINT AS hilbert"
                )
            else:
                lat_e7, lon_e7 = to_e7(row["lat"]), to_e7(row["lon"])
                h = lonlat_to_hilbert(row["lon"], row["lat"])
                payload = (
                    f"{lat_e7} AS lat_e7, {lon_e7} AS lon_e7, {tags_literal(row['tags'])} AS tags, "
                    f"{promoted_cols_sql(row['tags'])}, "
                    f"{meta_literal_sql(row['version'], row['changeset'], row['timestamp'], row['uid'], row['user'])}, "
                    f"{h}::UBIGINT AS hilbert"
                )
            return (
                f"SELECT {row['id']} AS id, {cell_sql_of(row['cell'])} AS cell, {payload}, "
                f"{str(row['deleted']).upper()} AS deleted, {cell_sql_of(row['prev_cell'])} AS prev_cell, "
                f"{row['seq']} AS seq"
            )

        def node_byid_row_sql(row):
            # M1's real byid node parts carry a stored `hilbert` (docs/
            # m1-contracts.md section 3), unlike this engine's own byid
            # reads, which recompute it via the `opq_node_hilbert` UDF
            # (`sources._byid_cols`) and so never look at a stored column
            # -- write it anyway, for fixture realism and so a reader that
            # *does* use the stored column also works.
            if row["deleted"]:
                payload = (
                    "NULL::INTEGER AS lat_e7, NULL::INTEGER AS lon_e7, "
                    f"NULL::MAP(VARCHAR, VARCHAR) AS tags, {promoted_cols_sql(None)}, {meta_literal_null_sql()}, "
                    "NULL::UBIGINT AS hilbert"
                )
            else:
                lat_e7, lon_e7 = to_e7(row["lat"]), to_e7(row["lon"])
                h = lonlat_to_hilbert(row["lon"], row["lat"])
                payload = (
                    f"{lat_e7} AS lat_e7, {lon_e7} AS lon_e7, {tags_literal(row['tags'])} AS tags, "
                    f"{promoted_cols_sql(row['tags'])}, "
                    f"{meta_literal_sql(row['version'], row['changeset'], row['timestamp'], row['uid'], row['user'])}, "
                    f"{h}::UBIGINT AS hilbert"
                )
            return (
                f"SELECT {row['id']} AS id, {payload}, {cell_sql_of(row['cell'])} AS cell, "
                f"{str(row['deleted']).upper()} AS deleted, {cell_sql_of(row['prev_cell'])} AS prev_cell, "
                f"{row['seq']} AS seq"
            )

        def _way_is_closed_area(refs, tags):
            is_closed = len(refs) >= 4 and refs[0] == refs[-1]
            tags = tags or {}
            has_area_no = tags.get("area") == "no"
            has_linear_tag = ("highway" in tags or "barrier" in tags) and tags.get("area") != "yes"
            is_area = bool(is_closed and not has_area_no and not has_linear_tag)
            return is_closed, is_area

        def way_spatial_row_sql(row):
            if row["deleted"]:
                payload = (
                    "NULL::BIGINT[] AS refs, NULL::MAP(VARCHAR, VARCHAR) AS tags, "
                    f"{promoted_cols_sql(None)}, {meta_literal_null_sql()}, "
                    "NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, "
                    "NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, "
                    "NULL::GEOMETRY AS geometry, NULL::BOOLEAN AS is_closed, NULL::BOOLEAN AS is_area, "
                    "NULL::INTEGER AS centroid_lat_e7, NULL::INTEGER AS centroid_lon_e7, NULL::UBIGINT AS hilbert"
                )
            else:
                wkt, (xmin, ymin, xmax, ymax) = way_geometry_and_bbox(row["refs"])
                geom_expr = f"ST_GeomFromText('{wkt}')" if wkt else "NULL::GEOMETRY"
                if xmin is not None:
                    xmin_e7, ymin_e7, xmax_e7, ymax_e7 = to_e7(xmin), to_e7(ymin), to_e7(xmax), to_e7(ymax)
                    centroid_lat_e7 = to_e7((ymin + ymax) / 2)
                    centroid_lon_e7 = to_e7((xmin + xmax) / 2)
                    h = bbox_e7_center_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7)
                else:
                    xmin_e7 = ymin_e7 = xmax_e7 = ymax_e7 = "NULL::INTEGER"
                    centroid_lat_e7 = centroid_lon_e7 = "NULL::INTEGER"
                    h = 0
                refs_literal = "[" + ", ".join(str(r) for r in row["refs"]) + "]::BIGINT[]"
                is_closed, is_area = _way_is_closed_area(row["refs"], row["tags"])
                payload = (
                    f"{refs_literal} AS refs, {tags_literal(row['tags'])} AS tags, "
                    f"{promoted_cols_sql(row['tags'])}, "
                    f"{meta_literal_sql(row['version'], row['changeset'], row['timestamp'], row['uid'], row['user'])}, "
                    f"{xmin_e7} AS xmin_e7, {ymin_e7} AS ymin_e7, {xmax_e7} AS xmax_e7, {ymax_e7} AS ymax_e7, "
                    f"{geom_expr} AS geometry, {str(is_closed).upper()} AS is_closed, "
                    f"{str(is_area).upper()} AS is_area, "
                    f"{centroid_lat_e7} AS centroid_lat_e7, {centroid_lon_e7} AS centroid_lon_e7, "
                    f"{h}::UBIGINT AS hilbert"
                )
            return (
                f"SELECT {row['id']} AS id, {cell_sql_of(row['cell'])} AS cell, {payload}, "
                f"{str(row['deleted']).upper()} AS deleted, {cell_sql_of(row['prev_cell'])} AS prev_cell, "
                f"{row['seq']} AS seq"
            )

        def way_byid_row_sql(row):
            # See node_byid_row_sql's comment: M1's real byid way parts
            # carry a stored `hilbert` too; this engine recomputes it via
            # `opq_bbox_hilbert` on read (`sources._byid_cols`) and never
            # looks at the stored column, but write it for realism anyway.
            if row["deleted"]:
                payload = (
                    "NULL::BIGINT[] AS refs, NULL::MAP(VARCHAR, VARCHAR) AS tags, "
                    f"{promoted_cols_sql(None)}, {meta_literal_null_sql()}, "
                    "NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, "
                    "NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, "
                    "NULL::BOOLEAN AS is_closed, NULL::BOOLEAN AS is_area, NULL::UBIGINT AS hilbert"
                )
            else:
                _wkt, (xmin, ymin, xmax, ymax) = way_geometry_and_bbox(row["refs"])
                if xmin is not None:
                    xmin_e7, ymin_e7, xmax_e7, ymax_e7 = to_e7(xmin), to_e7(ymin), to_e7(xmax), to_e7(ymax)
                    h = bbox_e7_center_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7)
                else:
                    xmin_e7 = ymin_e7 = xmax_e7 = ymax_e7 = "NULL::INTEGER"
                    h = 0
                refs_literal = "[" + ", ".join(str(r) for r in row["refs"]) + "]::BIGINT[]"
                is_closed, is_area = _way_is_closed_area(row["refs"], row["tags"])
                payload = (
                    f"{refs_literal} AS refs, {tags_literal(row['tags'])} AS tags, "
                    f"{promoted_cols_sql(row['tags'])}, "
                    f"{meta_literal_sql(row['version'], row['changeset'], row['timestamp'], row['uid'], row['user'])}, "
                    f"{xmin_e7} AS xmin_e7, {ymin_e7} AS ymin_e7, {xmax_e7} AS xmax_e7, {ymax_e7} AS ymax_e7, "
                    f"{str(is_closed).upper()} AS is_closed, {str(is_area).upper()} AS is_area, "
                    f"{h}::UBIGINT AS hilbert"
                )
            return (
                f"SELECT {row['id']} AS id, {payload}, {cell_sql_of(row['cell'])} AS cell, "
                f"{str(row['deleted']).upper()} AS deleted, {cell_sql_of(row['prev_cell'])} AS prev_cell, "
                f"{row['seq']} AS seq"
            )

        def relation_spatial_row_sql(row):
            if row["deleted"]:
                payload = (
                    "NULL::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[] AS members, "
                    "NULL::MAP(VARCHAR, VARCHAR) AS tags, "
                    f"{promoted_cols_sql(None)}, {meta_literal_null_sql()}, "
                    "NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, "
                    "NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, "
                    "NULL::GEOMETRY AS geometry, NULL::INTEGER AS centroid_lat_e7, "
                    "NULL::INTEGER AS centroid_lon_e7, NULL::UBIGINT AS hilbert"
                )
            else:
                members_literal = (
                    "[" + ", ".join(
                        f"{{'type': '{m['type']}', 'ref': {m['ref']}, 'role': '{m['role']}'}}"
                        for m in row["members"]
                    ) + "]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]"
                )
                xmin, ymin, xmax, ymax = row["bbox"]
                if xmin is not None:
                    xmin_e7, ymin_e7, xmax_e7, ymax_e7 = to_e7(xmin), to_e7(ymin), to_e7(xmax), to_e7(ymax)
                    centroid_lat_e7 = to_e7((ymin + ymax) / 2)
                    centroid_lon_e7 = to_e7((xmin + xmax) / 2)
                    h = bbox_e7_center_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7)
                else:
                    xmin_e7 = ymin_e7 = xmax_e7 = ymax_e7 = "NULL::INTEGER"
                    centroid_lat_e7 = centroid_lon_e7 = "NULL::INTEGER"
                    h = 0
                payload = (
                    f"{members_literal} AS members, {tags_literal(row['tags'])} AS tags, "
                    f"{promoted_cols_sql(row['tags'])}, "
                    f"{meta_literal_sql(row['version'], row['changeset'], row['timestamp'], row['uid'], row['user'])}, "
                    f"{xmin_e7} AS xmin_e7, {ymin_e7} AS ymin_e7, {xmax_e7} AS xmax_e7, {ymax_e7} AS ymax_e7, "
                    f"NULL::GEOMETRY AS geometry, {centroid_lat_e7} AS centroid_lat_e7, "
                    f"{centroid_lon_e7} AS centroid_lon_e7, {h}::UBIGINT AS hilbert"
                )
            return (
                f"SELECT {row['id']} AS id, {cell_sql_of(row['cell'])} AS cell, {payload}, "
                f"{str(row['deleted']).upper()} AS deleted, {cell_sql_of(row['prev_cell'])} AS prev_cell, "
                f"{row['seq']} AS seq"
            )

        def relation_byid_row_sql(row):
            if row["deleted"]:
                payload = (
                    "NULL::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[] AS members, "
                    "NULL::MAP(VARCHAR, VARCHAR) AS tags, "
                    f"{promoted_cols_sql(None)}, {meta_literal_null_sql()}, "
                    "NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, "
                    "NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7"
                )
            else:
                members_literal = (
                    "[" + ", ".join(
                        f"{{'type': '{m['type']}', 'ref': {m['ref']}, 'role': '{m['role']}'}}"
                        for m in row["members"]
                    ) + "]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]"
                )
                xmin, ymin, xmax, ymax = row["bbox"]
                if xmin is not None:
                    xmin_e7, ymin_e7, xmax_e7, ymax_e7 = to_e7(xmin), to_e7(ymin), to_e7(xmax), to_e7(ymax)
                else:
                    xmin_e7 = ymin_e7 = xmax_e7 = ymax_e7 = "NULL::INTEGER"
                payload = (
                    f"{members_literal} AS members, {tags_literal(row['tags'])} AS tags, "
                    f"{promoted_cols_sql(row['tags'])}, "
                    f"{meta_literal_sql(row['version'], row['changeset'], row['timestamp'], row['uid'], row['user'])}, "
                    f"{xmin_e7} AS xmin_e7, {ymin_e7} AS ymin_e7, {xmax_e7} AS xmax_e7, {ymax_e7} AS ymax_e7"
                )
            return (
                f"SELECT {row['id']} AS id, {payload}, {cell_sql_of(row['cell'])} AS cell, "
                f"{str(row['deleted']).upper()} AS deleted, {cell_sql_of(row['prev_cell'])} AS prev_cell, "
                f"{row['seq']} AS seq"
            )

        def tombstone_row_sql(t, id_, prev_cell, seq):
            return f"SELECT '{t}' AS type, {id_} AS id, '{prev_cell}' AS prev_cell, {seq} AS seq"

        _EMPTY_NODE_SPATIAL = (
            "SELECT NULL::BIGINT AS id, NULL::VARCHAR AS cell, NULL::INTEGER AS lat_e7, "
            "NULL::INTEGER AS lon_e7, NULL::MAP(VARCHAR, VARCHAR) AS tags, "
            + promoted_cols_sql(None) + ", " + meta_literal_null_sql()
            + ", NULL::UBIGINT AS hilbert, NULL::BOOLEAN AS deleted, "
            "NULL::VARCHAR AS prev_cell, NULL::BIGINT AS seq WHERE FALSE"
        )
        _EMPTY_NODE_BYID = (
            "SELECT NULL::BIGINT AS id, NULL::INTEGER AS lat_e7, NULL::INTEGER AS lon_e7, "
            "NULL::MAP(VARCHAR, VARCHAR) AS tags, " + promoted_cols_sql(None) + ", "
            + meta_literal_null_sql() + ", NULL::UBIGINT AS hilbert, NULL::VARCHAR AS cell, "
            "NULL::BOOLEAN AS deleted, NULL::VARCHAR AS prev_cell, NULL::BIGINT AS seq WHERE FALSE"
        )
        _EMPTY_WAY_SPATIAL = (
            "SELECT NULL::BIGINT AS id, NULL::VARCHAR AS cell, NULL::BIGINT[] AS refs, "
            "NULL::MAP(VARCHAR, VARCHAR) AS tags, " + promoted_cols_sql(None) + ", "
            + meta_literal_null_sql() + ", NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, "
            "NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, NULL::GEOMETRY AS geometry, "
            "NULL::BOOLEAN AS is_closed, NULL::BOOLEAN AS is_area, NULL::INTEGER AS centroid_lat_e7, "
            "NULL::INTEGER AS centroid_lon_e7, NULL::UBIGINT AS hilbert, NULL::BOOLEAN AS deleted, "
            "NULL::VARCHAR AS prev_cell, NULL::BIGINT AS seq WHERE FALSE"
        )
        _EMPTY_WAY_BYID = (
            "SELECT NULL::BIGINT AS id, NULL::BIGINT[] AS refs, NULL::MAP(VARCHAR, VARCHAR) AS tags, "
            + promoted_cols_sql(None) + ", " + meta_literal_null_sql()
            + ", NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, NULL::INTEGER AS xmax_e7, "
            "NULL::INTEGER AS ymax_e7, NULL::BOOLEAN AS is_closed, NULL::BOOLEAN AS is_area, "
            "NULL::UBIGINT AS hilbert, NULL::VARCHAR AS cell, NULL::BOOLEAN AS deleted, "
            "NULL::VARCHAR AS prev_cell, NULL::BIGINT AS seq WHERE FALSE"
        )
        _EMPTY_REL_SPATIAL = (
            "SELECT NULL::BIGINT AS id, NULL::VARCHAR AS cell, "
            "NULL::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[] AS members, "
            "NULL::MAP(VARCHAR, VARCHAR) AS tags, " + promoted_cols_sql(None) + ", "
            + meta_literal_null_sql() + ", NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, "
            "NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, NULL::GEOMETRY AS geometry, "
            "NULL::INTEGER AS centroid_lat_e7, NULL::INTEGER AS centroid_lon_e7, NULL::UBIGINT AS hilbert, "
            "NULL::BOOLEAN AS deleted, NULL::VARCHAR AS prev_cell, NULL::BIGINT AS seq WHERE FALSE"
        )
        _EMPTY_REL_BYID = (
            "SELECT NULL::BIGINT AS id, NULL::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[] AS members, "
            "NULL::MAP(VARCHAR, VARCHAR) AS tags, " + promoted_cols_sql(None) + ", "
            + meta_literal_null_sql() + ", NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, "
            "NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, NULL::VARCHAR AS cell, "
            "NULL::BOOLEAN AS deleted, NULL::VARCHAR AS prev_cell, NULL::BIGINT AS seq WHERE FALSE"
        )
        _EMPTY_TOMBSTONES = (
            "SELECT NULL::VARCHAR AS type, NULL::BIGINT AS id, NULL::VARCHAR AS prev_cell, "
            "NULL::BIGINT AS seq WHERE FALSE"
        )

        def write_delta_tier(tier_name, version, node_rows, way_rows, relation_rows, tombstones):
            tier_dir = f"delta/{GEN}/{tier_name}/{version}"

            def write(rel_path, row_sqls, empty_sql):
                out_path = root / rel_path
                out_path.parent.mkdir(parents=True, exist_ok=True)
                sql = " UNION ALL ".join(row_sqls) if row_sqls else empty_sql
                con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT PARQUET)")

            paths = {
                "node": {
                    "spatial": f"{tier_dir}/node.spatial.parquet",
                    "byid": f"{tier_dir}/node.byid.parquet",
                },
                "way": {
                    "spatial": f"{tier_dir}/way.spatial.parquet",
                    "byid": f"{tier_dir}/way.byid.parquet",
                },
                "relation": {
                    "spatial": f"{tier_dir}/relation.spatial.parquet",
                    "byid": f"{tier_dir}/relation.byid.parquet",
                },
                "tombstones": f"{tier_dir}/tombstones.parquet",
            }
            write(paths["node"]["spatial"], [node_spatial_row_sql(r) for r in node_rows], _EMPTY_NODE_SPATIAL)
            write(paths["node"]["byid"], [node_byid_row_sql(r) for r in node_rows], _EMPTY_NODE_BYID)
            write(paths["way"]["spatial"], [way_spatial_row_sql(r) for r in way_rows], _EMPTY_WAY_SPATIAL)
            write(paths["way"]["byid"], [way_byid_row_sql(r) for r in way_rows], _EMPTY_WAY_BYID)
            write(paths["relation"]["spatial"], [relation_spatial_row_sql(r) for r in relation_rows], _EMPTY_REL_SPATIAL)
            write(paths["relation"]["byid"], [relation_byid_row_sql(r) for r in relation_rows], _EMPTY_REL_BYID)
            write(
                paths["tombstones"],
                [tombstone_row_sql(t["type"], t["id"], t["prev_cell"], t["seq"]) for t in tombstones],
                _EMPTY_TOMBSTONES,
            )
            cells = {
                "node": sorted({r["cell"] for r in node_rows if r.get("cell")}),
                "way": sorted({r["cell"] for r in way_rows if r.get("cell")}),
                "relation": sorted({r["cell"] for r in relation_rows if r.get("cell")}),
            }
            return paths, {
                "node": len(node_rows), "way": len(way_rows), "relation": len(relation_rows),
            }, cells

        # -- week tier ----------------------------------------------------
        week_seq_from, week_seq_to = 2, 100
        week_ts = "2026-09-19T06:00:00Z"
        week_node_rows = [
            {
                "id": info.delta_modified_node_id, "deleted": False, "cell": "000", "prev_cell": "000",
                "seq": week_seq_to, "lat": node_by_id[info.delta_modified_node_id]["lat"],
                "lon": node_by_id[info.delta_modified_node_id]["lon"],
                "tags": info.delta_modified_node_week_tags,
                "version": 3, "changeset": 90001, "timestamp": "2026-09-19 06:00:00", "uid": 501, "user": "tester1",
            },
            {
                "id": info.delta_moved_deleted_node_id, "deleted": False,
                "cell": info.delta_moved_deleted_node_to_cell, "prev_cell": info.delta_moved_deleted_node_from_cell,
                "seq": week_seq_to, "lat": _inset_point(leaf_bbox[info.delta_moved_deleted_node_to_cell], 0.5, 0.5)[1],
                "lon": _inset_point(leaf_bbox[info.delta_moved_deleted_node_to_cell], 0.5, 0.5)[0],
                "tags": None, "version": 2, "changeset": 90002, "timestamp": "2026-09-19 06:00:00",
                "uid": 501, "user": "tester1",
            },
            {
                "id": info.delta_moved_only_node_id, "deleted": False,
                "cell": info.delta_moved_only_node_to_cell, "prev_cell": info.delta_moved_only_node_from_cell,
                "seq": week_seq_to, "lat": _inset_point(leaf_bbox[info.delta_moved_only_node_to_cell], 0.6, 0.4)[1],
                "lon": _inset_point(leaf_bbox[info.delta_moved_only_node_to_cell], 0.6, 0.4)[0],
                "tags": None, "version": 2, "changeset": 90003, "timestamp": "2026-09-19 06:00:00",
                "uid": 501, "user": "tester1",
            },
            {
                "id": no_base_node_a_id, "deleted": False, "cell": NO_BASE_CELL, "prev_cell": None,
                "seq": week_seq_to, "lat": node_by_id[no_base_node_a_id]["lat"],
                "lon": node_by_id[no_base_node_a_id]["lon"],
                "tags": None, "version": 1, "changeset": 90005, "timestamp": "2026-09-19 06:00:00",
                "uid": 501, "user": "tester1",
            },
            {
                "id": no_base_node_b_id, "deleted": False, "cell": NO_BASE_CELL, "prev_cell": None,
                "seq": week_seq_to, "lat": node_by_id[no_base_node_b_id]["lat"],
                "lon": node_by_id[no_base_node_b_id]["lon"],
                "tags": None, "version": 1, "changeset": 90005, "timestamp": "2026-09-19 06:00:00",
                "uid": 501, "user": "tester1",
            },
        ]
        week_way_rows = [
            {
                "id": info.delta_deleted_way_id, "deleted": True, "cell": info.delta_deleted_way_cell,
                "prev_cell": info.delta_deleted_way_cell, "seq": week_seq_to,
            },
            {
                "id": info.delta_new_way_id, "deleted": False, "cell": info.delta_new_way_cell, "prev_cell": None,
                "seq": week_seq_to, "refs": info.delta_new_way_refs, "tags": {"highway": "path"},
                "version": 1, "changeset": 90004, "timestamp": "2026-09-19 06:00:00", "uid": 501, "user": "tester1",
            },
            {
                # Placed at NO_BASE_CELL ("301"): an allowed ancestor depth
                # (3) the base has never written a way file for -- proves
                # `cells_for_bbox`/`delta_present_cells` can discover a
                # cell that exists only via `deltas.<tier>.cells`.
                "id": info.delta_new_way_no_base_cell_id, "deleted": False, "cell": NO_BASE_CELL, "prev_cell": None,
                "seq": week_seq_to, "refs": info.delta_new_way_no_base_cell_refs, "tags": {"building": "yes"},
                "version": 1, "changeset": 90006, "timestamp": "2026-09-19 06:00:00", "uid": 501, "user": "tester1",
            },
        ]
        week_tombstones = [
            {"type": "node", "id": info.delta_moved_deleted_node_id,
             "prev_cell": info.delta_moved_deleted_node_from_cell, "seq": week_seq_to},
            {"type": "node", "id": info.delta_moved_only_node_id,
             "prev_cell": info.delta_moved_only_node_from_cell, "seq": week_seq_to},
            {"type": "way", "id": info.delta_deleted_way_id,
             "prev_cell": info.delta_deleted_way_cell, "seq": week_seq_to},
        ]
        week_paths, week_rows_count, week_cells = write_delta_tier(
            "week", info.delta_week_version, week_node_rows, week_way_rows, [], week_tombstones
        )

        # -- day tier -------------------------------------------------------
        day_seq_from, day_seq_to = week_seq_to + 1, 500
        day_ts = "2026-09-19T12:00:00Z"
        new_way_wkt, new_way_bbox = way_geometry_and_bbox(info.delta_new_way_refs)
        day_node_rows = [
            {
                "id": info.delta_modified_node_id, "deleted": False, "cell": "000", "prev_cell": "000",
                "seq": day_seq_to, "lat": node_by_id[info.delta_modified_node_id]["lat"],
                "lon": node_by_id[info.delta_modified_node_id]["lon"],
                "tags": info.delta_modified_node_day_tags,
                "version": 4, "changeset": 90101, "timestamp": "2026-09-19 12:00:00", "uid": 502, "user": "tester2",
            },
        ]
        day_relation_rows = [
            {
                "id": info.delta_new_relation_id, "deleted": False, "cell": info.delta_new_relation_cell,
                "prev_cell": None, "seq": day_seq_to,
                "members": [
                    {"type": "w", "ref": info.delta_new_relation_way_member_id, "role": "outer"},
                    {"type": "n", "ref": info.delta_new_relation_node_member_id, "role": "label"},
                ],
                "tags": {"type": "multipolygon", "leisure": "park", "name": "New Park (day)"},
                "version": 1, "changeset": 90102, "timestamp": "2026-09-19 12:00:00", "uid": 502, "user": "tester2",
                "bbox": (
                    min(new_way_bbox[0], node_by_id[info.delta_new_relation_node_member_id]["lon"]),
                    min(new_way_bbox[1], node_by_id[info.delta_new_relation_node_member_id]["lat"]),
                    max(new_way_bbox[2], node_by_id[info.delta_new_relation_node_member_id]["lon"]),
                    max(new_way_bbox[3], node_by_id[info.delta_new_relation_node_member_id]["lat"]),
                ) if new_way_bbox[0] is not None else (None, None, None, None),
            },
        ]
        day_paths, day_rows_count, day_cells = write_delta_tier(
            "day", info.delta_day_version, day_node_rows, [], day_relation_rows, []
        )

        # -- hour tier ------------------------------------------------------
        hour_seq_from, hour_seq_to = day_seq_to + 1, 520
        hour_ts = "2026-09-19T12:45:00Z"
        hour_node_rows = [
            {
                "id": info.delta_moved_deleted_node_id, "deleted": True,
                "cell": info.delta_moved_deleted_node_to_cell, "prev_cell": info.delta_moved_deleted_node_to_cell,
                "seq": hour_seq_to,
            },
        ]
        hour_way_rows = [
            {
                "id": info.delta_modified_refs_way_id, "deleted": False, "cell": "000", "prev_cell": "000",
                "seq": hour_seq_to, "refs": info.delta_modified_refs_way_new_refs,
                "tags": next(w for w in ways if w["id"] == info.delta_modified_refs_way_id)["tags"],
                "version": info.delta_modified_refs_way_new_version, "changeset": 90201,
                "timestamp": "2026-09-19 12:45:00", "uid": 503, "user": "tester3",
            },
        ]
        hour_tombstones = [
            {"type": "node", "id": info.delta_moved_deleted_node_id,
             "prev_cell": info.delta_moved_deleted_node_to_cell, "seq": hour_seq_to},
        ]
        hour_paths, hour_rows_count, hour_cells = write_delta_tier(
            "hour", info.delta_hour_version, hour_node_rows, hour_way_rows, [], hour_tombstones
        )

        manifest["manifest_version"] = 3
        manifest["replication_source"] = REPLICATION_SOURCE
        manifest["replication_sequence"] = hour_seq_to
        manifest["timestamp_osm_base"] = hour_ts
        manifest["deltas"] = {
            "week": {
                "version": info.delta_week_version, "seq_from": week_seq_from, "seq_to": week_seq_to,
                "timestamp": week_ts, "rows": week_rows_count, "files": week_paths, "cells": week_cells,
            },
            "day": {
                "version": info.delta_day_version, "seq_from": day_seq_from, "seq_to": day_seq_to,
                "timestamp": day_ts, "rows": day_rows_count, "files": day_paths, "cells": day_cells,
            },
            "hour": {
                "version": info.delta_hour_version, "seq_from": hour_seq_from, "seq_to": hour_seq_to,
                "timestamp": hour_ts, "rows": hour_rows_count, "files": hour_paths, "cells": hour_cells,
            },
        }

    # -------------------------------------------------------------- manifest
    manifest_dir = root / "manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "1.json").write_text(json.dumps(manifest, indent=2))
    (manifest_dir / "LATEST").write_text("1")

    if own_con:
        con.close()
    return info


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        info = build(tmp)
        print(json.dumps(json.loads((Path(tmp) / "manifest" / "1.json").read_text()), indent=2)[:2000])
        print("built at", tmp)
