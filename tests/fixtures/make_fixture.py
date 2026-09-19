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


def to_e7(deg: float) -> int:
    return int(round(deg * 1e7))


def _inset_point(bbox, frac_lat: float, frac_lon: float):
    s, w, n, e = bbox
    lat = s + (n - s) * frac_lat
    lon = w + (e - w) * frac_lon
    return lon, lat


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
    all_node_ids: list = field(default_factory=list)
    all_way_ids: list = field(default_factory=list)
    all_relation_ids: list = field(default_factory=list)
    total_bbox: tuple = None  # (s, w, n, e) covering everything


def build(root_dir: str, con: duckdb.DuckDBPyConnection | None = None) -> FixtureInfo:
    root = Path(root_dir)
    root.mkdir(parents=True, exist_ok=True)
    own_con = con is None
    if con is None:
        con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    leaf_bbox = {c: cell_bbox(c) for c in LEAVES}
    ancestor_bbox = cell_bbox(ANCESTOR)
    info = FixtureInfo(root=str(root), leaf_bbox=leaf_bbox, ancestor_bbox=ancestor_bbox)

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

    info.all_node_ids = [n["id"] for n in nodes]
    info.untagged_node_in_cafe_cell = leaf000_extra_ids[0]

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
        if not xs:
            return (None, None, None, None)
        return (min(xs), min(ys), max(xs), max(ys))

    for r in relations:
        r["xmin"], r["ymin"], r["xmax"], r["ymax"] = relation_bbox(r["members"])

    all_lons = [n["lon"] for n in nodes]
    all_lats = [n["lat"] for n in nodes]
    info.total_bbox = (min(all_lats), min(all_lons), max(all_lats), max(all_lons))

    # ---------------------------------------------------------- write files
    manifest = {
        "manifest_version": 1,
        "generation": GEN,
        "schema_version": 1,
        "coordinate_scale": 10000000,
        "promoted_keys": PROMOTED_KEYS,
        "timestamp_osm_base": TIMESTAMP_OSM_BASE,
        "replication_sequence": 1,
        "source": "synthetic engine-test fixture (tests/fixtures/make_fixture.py)",
        "extent": list(info.total_bbox),
        "leaf_cells": LEAVES,
        "tables": {"node": {"cells": {}}, "way": {"cells": {}}, "relation": {"cells": {}}},
        "byid": {"node": [], "way": [], "relation": []},
        "index": {"node_way": [], "member": []},
    }

    def meta_cols_sql(i: int) -> str:
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

    # -- node spatial partitions (tagged / untagged), per leaf cell
    for cell in LEAVES:
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
                    f"{meta_cols_sql(n['id'])}, {h}::UBIGINT AS hilbert"
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
                f"{meta_cols_sql(n['id'])}, '{n['cell']}' AS cell"
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
