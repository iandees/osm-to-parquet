"""Builds a v5 (history) fixture on top of the M0 fixture root
(docs/m4-contracts.md section 3.3), for the engine's attic tests
(`tests/test_attic_*.py`).

`build()` first calls `make_fixture.build(root, con, manifest_version=4)`
to get a normal, self-consistent current-state dataset (nodes/ways/
relations/areas), then layers a `history/` tree directly with DuckDB
(like the M0 fixture itself -- the real history builder is being
developed concurrently in a different worktree, W1), and rewrites the
manifest as v5 with a `history` section (docs/m4-contracts.md section
2.3). The history-only elements below are *not* required to also exist
in the current-state tables (the engine's snapshot read path never
touches current-state files when a snapshot is active), except where a
test explicitly wants to compare `[date:"latest"]` against the plain
current-state answer.

Fixture contents (docs/m4-contracts.md section 3.3's checklist):

- `hist_node_id` (a node): 4 versions -- created, a tag change, a move to
  a different leaf cell (exercising the move tombstone in the old cell),
  and a deletion.
- `hist_way_id` (a way): 2 own versions and 2 minor versions -- a node
  move that stays in the same cell (own version 1's minor), and a node
  move whose way now sits in a different cell (own version 2's minor,
  exercising a way-side move tombstone).
- `hist_relation_id` (a relation): 1 minor version (a member move) inside
  its first own version's window.
- `hist_new_after_since_id` (a node): its only version's `valid_from` is
  after `history.since`.
- `hist_tier_node_id` (a node): the *base* files only know its first
  version; a newer version-2 state lives in the `hour` tier only, so
  reading at/after that tier's `valid_from` must fall through to it.

`HISTORY_SINCE` is the manifest's `history.since`; every id/timestamp a
test needs is on the returned `HistoryFixtureInfo`.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import make_fixture  # noqa: E402

from osmpq.engine.catalog import cell_bbox  # noqa: E402
from osmpq.history.schema import HISTORY_EXTRA_NAMES  # noqa: E402
from osmpq.update.updater import BYID_COLUMNS, SPATIAL_COLUMNS  # noqa: E402

HIST_GEN = "h0001"
HISTORY_SINCE = "2024-06-01T00:00:00Z"
PROMOTED_KEYS = make_fixture.PROMOTED_KEYS

_TYPES = {
    "id": "BIGINT",
    "lat_e7": "INTEGER",
    "lon_e7": "INTEGER",
    "tags": "MAP(VARCHAR, VARCHAR)",
    "version": "INTEGER",
    "changeset": "BIGINT",
    "timestamp": "TIMESTAMP",
    "uid": "INTEGER",
    "user": "VARCHAR",
    "hilbert": "UBIGINT",
    "cell": "VARCHAR",
    "refs": "BIGINT[]",
    "members": "STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]",
    "geometry": "GEOMETRY",
    "is_closed": "BOOLEAN",
    "is_area": "BOOLEAN",
    "xmin_e7": "INTEGER",
    "ymin_e7": "INTEGER",
    "xmax_e7": "INTEGER",
    "ymax_e7": "INTEGER",
    "centroid_lat_e7": "INTEGER",
    "centroid_lon_e7": "INTEGER",
    "minor": "INTEGER",
    "valid_from": "TIMESTAMP",
    "valid_to": "TIMESTAMP",
    "visible": "BOOLEAN",
}


def _logical(col: str) -> str:
    return col.strip('"')


def _val_sql(col: str, value) -> str:
    ty = _TYPES.get(col, "VARCHAR")
    if value is None:
        return f"NULL::{ty}"
    if col == "tags":
        if not value:
            return "NULL::MAP(VARCHAR, VARCHAR)"
        pairs = ", ".join(f"'{k}': '{v}'" for k, v in value.items())
        return f"MAP {{{pairs}}}"
    if col == "refs":
        return "[" + ", ".join(str(r) for r in value) + "]::BIGINT[]"
    if col == "members":
        items = ", ".join(
            f"{{'type': '{m['type']}', 'ref': {m['ref']}, 'role': '{m.get('role', '')}'}}" for m in value
        )
        return f"[{items}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]"
    if col == "geometry":
        return f"ST_GeomFromText('{value}')"
    if col in ("timestamp", "valid_from", "valid_to"):
        return f"TIMESTAMP '{value}'"
    if col in ("is_closed", "is_area", "visible"):
        return "TRUE" if value else "FALSE"
    if ty == "VARCHAR":
        return "'" + str(value).replace("'", "''") + "'"
    return str(value)


def _row_select(columns: list[str], values: dict) -> str:
    parts = []
    for c in columns:
        name = _logical(c)
        parts.append(f'{_val_sql(name, values.get(name))} AS "{name}"')
    return "SELECT " + ", ".join(parts)


def _write_parquet(con, root: Path, rel_path: str, selects: list[str]) -> dict:
    out_path = root / rel_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sql = " UNION ALL BY NAME ".join(selects)
    con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT PARQUET)")
    return {"path": rel_path, "rows": len(selects), "bytes": out_path.stat().st_size}


@dataclass
class HistoryFixtureInfo:
    root: str
    since: str = HISTORY_SINCE
    generation: str = HIST_GEN
    base: "make_fixture.FixtureInfo" = None
    leaf_bbox: dict = field(default_factory=dict)

    hist_node_id: int = 9001
    hist_node_cell_before: str = "000"
    hist_node_cell_after: str = "001"
    hist_node_v1_from: str = "2020-01-01T00:00:00Z"
    hist_node_v2_from: str = "2021-06-15T00:00:00Z"  # tag change
    hist_node_v3_from: str = "2022-01-01T00:00:00Z"  # moved
    hist_node_v4_from: str = "2023-01-01T00:00:00Z"  # deleted
    hist_node_v1_tags: dict = field(default_factory=lambda: {"amenity": "cafe", "name": "Old Cafe"})
    hist_node_v2_tags: dict = field(default_factory=lambda: {"amenity": "cafe", "name": "New Cafe"})

    hist_way_id: int = 9002
    hist_way_node_a: int = 9011
    hist_way_node_b: int = 9012
    hist_way_cell_before: str = "000"
    hist_way_cell_after: str = "001"
    hist_way_v1_from: str = "2020-06-01T00:00:00Z"
    hist_way_v1_minor1_from: str = "2020-09-01T00:00:00Z"
    hist_way_v2_from: str = "2021-01-01T00:00:00Z"
    hist_way_v2_minor1_from: str = "2021-06-01T00:00:00Z"  # moves to a new cell

    hist_relation_id: int = 9003
    # "00" (not "000"): the loose-placement invariant a relation's cell
    # must satisfy an ancestor-or-self of every member's cell -- its one
    # node member (`hist_node_id`) moves from leaf "000" to leaf "001"
    # partway through this relation's own history, so the relation itself
    # must live at their shared ancestor, not either leaf.
    hist_relation_cell: str = "00"
    hist_relation_v1_from: str = "2022-01-01T00:00:00Z"
    hist_relation_v1_minor1_from: str = "2022-03-01T00:00:00Z"
    hist_relation_v2_from: str = "2022-06-01T00:00:00Z"

    hist_new_after_since_id: int = 9010
    hist_new_after_since_from: str = "2025-01-01T00:00:00Z"
    hist_new_after_since_cell: str = "000"

    hist_tier_node_id: int = 9020
    hist_tier_node_cell: str = "000"
    hist_tier_node_v1_from: str = "2020-01-01T00:00:00Z"
    hist_tier_node_v2_from: str = "2026-08-01T00:00:00Z"
    hist_tier_node_v1_tags: dict = field(default_factory=lambda: {"leisure": "park_bench"})
    hist_tier_node_v2_tags: dict = field(default_factory=lambda: {"leisure": "park_bench", "amenity": "bench"})

    # `(changed:)`/`(newer:)` need an id the *current* tables also know
    # about (the filter runs over a normal current-state candidate row,
    # then checks its history) -- reuse the base fixture's cafe node (id
    # 1, leaf "000") and give it one extra history state.
    changed_test_node_id: int = 0
    changed_test_node_from: str = "2026-07-01T00:00:00Z"


def build(root_dir: str, con: "duckdb.DuckDBPyConnection | None" = None) -> HistoryFixtureInfo:
    root = Path(root_dir)
    root.mkdir(parents=True, exist_ok=True)
    own_con = con is None
    if con is None:
        con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    base_info = make_fixture.build(root_dir, con=con, manifest_version=4)
    leaf_bbox = {c: cell_bbox(c) for c in make_fixture.LEAVES}
    info = HistoryFixtureInfo(root=str(root), base=base_info, leaf_bbox=leaf_bbox)
    info.changed_test_node_id = base_info.cafe_node_id

    def point(cell: str, frac_lat: float, frac_lon: float):
        s, w, n, e = leaf_bbox[cell]
        lat = s + (n - s) * frac_lat
        lon = w + (e - w) * frac_lon
        return lat, lon

    def to_e7(deg: float) -> int:
        return int(round(deg * 1e7))

    node_spatial_cols = SPATIAL_COLUMNS["node"](PROMOTED_KEYS) + HISTORY_EXTRA_NAMES
    node_byid_cols = BYID_COLUMNS["node"](PROMOTED_KEYS) + HISTORY_EXTRA_NAMES
    way_spatial_cols = SPATIAL_COLUMNS["way"](PROMOTED_KEYS) + HISTORY_EXTRA_NAMES
    way_byid_cols = BYID_COLUMNS["way"](PROMOTED_KEYS) + HISTORY_EXTRA_NAMES
    rel_spatial_cols = SPATIAL_COLUMNS["relation"](PROMOTED_KEYS) + HISTORY_EXTRA_NAMES
    rel_byid_cols = BYID_COLUMNS["relation"](PROMOTED_KEYS) + HISTORY_EXTRA_NAMES

    # ---------------------------------------------------------------- node
    lat_a, lon_a = point(info.hist_node_cell_before, 0.5, 0.5)
    lat_b, lon_b = point(info.hist_node_cell_after, 0.5, 0.5)

    def node_row(version, valid_from, valid_to, cell, tags, lat, lon, minor=0, visible=True, uid=501, user="tester0", changeset=9500):
        return {
            "id": info.hist_node_id, "lat_e7": to_e7(lat) if visible else None, "lon_e7": to_e7(lon) if visible else None,
            "tags": tags if visible else None, "version": version if visible or minor == 0 else version,
            "changeset": changeset if visible else None, "timestamp": valid_from.replace("T", " ").rstrip("Z") if visible else None,
            "uid": uid if visible else None, "user": user if visible else None, "hilbert": 0 if visible else None,
            "cell": cell, "minor": minor, "valid_from": valid_from.replace("T", " ").rstrip("Z"),
            "valid_to": valid_to.replace("T", " ").rstrip("Z") if valid_to else None, "visible": visible,
        }

    node_v1 = node_row(1, info.hist_node_v1_from, info.hist_node_v2_from, "000", info.hist_node_v1_tags, lat_a, lon_a)
    node_v2 = node_row(2, info.hist_node_v2_from, info.hist_node_v3_from, "000", info.hist_node_v2_tags, lat_a, lon_a)
    node_v3 = node_row(3, info.hist_node_v3_from, info.hist_node_v4_from, "001", info.hist_node_v2_tags, lat_b, lon_b)
    node_v3_tombstone = dict(node_v3)
    node_v3_tombstone.update({"cell": "000", "lat_e7": None, "lon_e7": None, "tags": None, "changeset": None,
                              "timestamp": None, "uid": None, "user": None, "hilbert": None, "visible": False})
    node_v4_delete = {
        "id": info.hist_node_id, "lat_e7": None, "lon_e7": None, "tags": None, "version": 4,
        "changeset": None, "timestamp": None, "uid": None, "user": None, "hilbert": None,
        "cell": "001", "minor": 0, "valid_from": info.hist_node_v4_from.replace("T", " ").rstrip("Z"),
        "valid_to": None, "visible": False,
    }

    node_spatial_000 = _write_parquet(
        con, root, f"history/{HIST_GEN}/spatial/node/cell=000/part-0.parquet",
        [_row_select(node_spatial_cols, node_v1), _row_select(node_spatial_cols, node_v2),
         _row_select(node_spatial_cols, node_v3_tombstone)],
    )
    node_spatial_001 = _write_parquet(
        con, root, f"history/{HIST_GEN}/spatial/node/cell=001/part-0.parquet",
        [_row_select(node_spatial_cols, node_v3), _row_select(node_spatial_cols, node_v4_delete)],
    )
    node_byid_rows = [node_v1, node_v2, node_v3, node_v4_delete]

    # -- hist_new_after_since_id: single version, after `since` -----------
    lat_n, lon_n = point(info.hist_new_after_since_cell, 0.2, 0.8)
    new_after = {
        "id": info.hist_new_after_since_id, "lat_e7": to_e7(lat_n), "lon_e7": to_e7(lon_n),
        "tags": {"shop": "new_shop"}, "version": 1, "changeset": 9600,
        "timestamp": info.hist_new_after_since_from.replace("T", " ").rstrip("Z"), "uid": 501, "user": "tester0",
        "hilbert": 0, "cell": info.hist_new_after_since_cell, "minor": 0,
        "valid_from": info.hist_new_after_since_from.replace("T", " ").rstrip("Z"), "valid_to": None, "visible": True,
    }
    node_spatial_000_sel = [_row_select(node_spatial_cols, node_v1), _row_select(node_spatial_cols, node_v2),
                            _row_select(node_spatial_cols, node_v3_tombstone), _row_select(node_spatial_cols, new_after)]
    node_spatial_000 = _write_parquet(con, root, f"history/{HIST_GEN}/spatial/node/cell=000/part-0.parquet", node_spatial_000_sel)
    node_byid_rows.append(new_after)

    # -- hist_tier_node_id: base v1 only, hour tier holds v2 --------------
    lat_t, lon_t = point(info.hist_tier_node_cell, 0.65, 0.15)
    tier_node_v1 = {
        "id": info.hist_tier_node_id, "lat_e7": to_e7(lat_t), "lon_e7": to_e7(lon_t),
        "tags": info.hist_tier_node_v1_tags, "version": 1, "changeset": 9700,
        "timestamp": info.hist_tier_node_v1_from.replace("T", " ").rstrip("Z"), "uid": 501, "user": "tester0",
        "hilbert": 0, "cell": info.hist_tier_node_cell, "minor": 0,
        "valid_from": info.hist_tier_node_v1_from.replace("T", " ").rstrip("Z"), "valid_to": None, "visible": True,
    }
    tier_node_v2 = dict(tier_node_v1)
    tier_node_v2.update({
        "tags": info.hist_tier_node_v2_tags, "version": 2, "changeset": 9701,
        "timestamp": info.hist_tier_node_v2_from.replace("T", " ").rstrip("Z"),
        "valid_from": info.hist_tier_node_v2_from.replace("T", " ").rstrip("Z"), "valid_to": None,
    })
    node_spatial_000_sel.append(_row_select(node_spatial_cols, tier_node_v1))
    node_byid_rows.append(tier_node_v1)

    # -- changed_test_node_id: an extra history state for a node the
    # *current* tables already know about (see the field's docstring).
    lat_c, lon_c = point("000", 0.15, 0.5)
    changed_test_row = {
        "id": info.changed_test_node_id, "lat_e7": to_e7(lat_c), "lon_e7": to_e7(lon_c),
        "tags": {"amenity": "cafe", "name": "Aroma Cafe"}, "version": 1, "changeset": 9550,
        "timestamp": info.changed_test_node_from.replace("T", " ").rstrip("Z"), "uid": 501, "user": "tester0",
        "hilbert": 0, "cell": "000", "minor": 0,
        "valid_from": info.changed_test_node_from.replace("T", " ").rstrip("Z"), "valid_to": None, "visible": True,
    }
    node_spatial_000_sel.append(_row_select(node_spatial_cols, changed_test_row))
    node_byid_rows.append(changed_test_row)

    node_spatial_000 = _write_parquet(con, root, f"history/{HIST_GEN}/spatial/node/cell=000/part-0.parquet", node_spatial_000_sel)

    node_byid_part = _write_parquet(
        con, root, f"history/{HIST_GEN}/byid/node/part-0.parquet",
        [_row_select(node_byid_cols, r) for r in node_byid_rows],
    )
    node_byid_meta = {**node_byid_part, "min_id": min(r["id"] for r in node_byid_rows),
                      "max_id": max(r["id"] for r in node_byid_rows)}

    # hour tier: node v2 (spatial + byid), a `cell` column (tier files
    # cover every cell in one file, section 2.2).
    tier_node_v2_spatial = dict(tier_node_v2)
    tier_hour_node_spatial = _write_parquet(
        con, root, f"history/{HIST_GEN}/tier/hour/1/node.spatial.parquet",
        [_row_select(["cell"] + node_spatial_cols, {**tier_node_v2_spatial, "cell": info.hist_tier_node_cell})],
    )
    tier_hour_node_byid = _write_parquet(
        con, root, f"history/{HIST_GEN}/tier/hour/1/node.byid.parquet",
        [_row_select(node_byid_cols, tier_node_v2)],
    )

    # ----------------------------------------------------------------- way
    def way_geom(lat1, lon1, lat2, lon2) -> tuple[str, int, int, int, int]:
        wkt = f"LINESTRING ({lon1} {lat1}, {lon2} {lat2})"
        xmin, xmax = sorted((to_e7(lon1), to_e7(lon2)))
        ymin, ymax = sorted((to_e7(lat1), to_e7(lat2)))
        return wkt, xmin, ymin, xmax, ymax

    lat_wa1, lon_wa1 = point("000", 0.2, 0.2)
    lat_wb1, lon_wb1 = point("000", 0.3, 0.3)
    wkt1, xmin1, ymin1, xmax1, ymax1 = way_geom(lat_wa1, lon_wa1, lat_wb1, lon_wb1)

    lat_wa2, lon_wa2 = point("000", 0.22, 0.22)  # node A moved slightly, still leaf "000"
    wkt2, xmin2, ymin2, xmax2, ymax2 = way_geom(lat_wa2, lon_wa2, lat_wb1, lon_wb1)

    lat_wb3, lon_wb3 = point("000", 0.35, 0.35)  # node B moved, own version 2's own edit
    wkt3, xmin3, ymin3, xmax3, ymax3 = way_geom(lat_wa2, lon_wa2, lat_wb3, lon_wb3)

    lat_wb4, lon_wb4 = point("001", 0.5, 0.5)  # node B moved into leaf "001": way's own bbox follows
    wkt4, xmin4, ymin4, xmax4, ymax4 = way_geom(lat_wa2, lon_wa2, lat_wb4, lon_wb4)

    def way_row(version, valid_from, valid_to, cell, wkt, xmin, ymin, xmax, ymax, minor=0, visible=True):
        return {
            "id": info.hist_way_id, "refs": [info.hist_way_node_a, info.hist_way_node_b],
            "tags": {"highway": "path"} if visible else None, "version": version, "changeset": 9800,
            "timestamp": valid_from.replace("T", " ").rstrip("Z") if visible else None, "uid": 501, "user": "tester0",
            "xmin_e7": xmin if visible else None, "ymin_e7": ymin if visible else None,
            "xmax_e7": xmax if visible else None, "ymax_e7": ymax if visible else None,
            "geometry": wkt if visible else None, "is_closed": False, "is_area": False,
            "centroid_lat_e7": (ymin + ymax) // 2 if visible else None,
            "centroid_lon_e7": (xmin + xmax) // 2 if visible else None, "hilbert": 0 if visible else None,
            "cell": cell, "minor": minor, "valid_from": valid_from.replace("T", " ").rstrip("Z"),
            "valid_to": valid_to.replace("T", " ").rstrip("Z") if valid_to else None, "visible": visible,
        }

    way_v1 = way_row(1, info.hist_way_v1_from, info.hist_way_v1_minor1_from, "000", wkt1, xmin1, ymin1, xmax1, ymax1)
    way_v1_minor1 = way_row(1, info.hist_way_v1_minor1_from, info.hist_way_v2_from, "000", wkt2, xmin2, ymin2, xmax2, ymax2, minor=1)
    way_v2 = way_row(2, info.hist_way_v2_from, info.hist_way_v2_minor1_from, "000", wkt3, xmin3, ymin3, xmax3, ymax3)
    way_v2_minor1 = way_row(2, info.hist_way_v2_minor1_from, None, "001", wkt4, xmin4, ymin4, xmax4, ymax4, minor=1)
    way_v2_minor1_tombstone = dict(way_v2_minor1)
    way_v2_minor1_tombstone.update({"cell": "000", "refs": None, "tags": None, "changeset": None, "timestamp": None,
                                     "xmin_e7": None, "ymin_e7": None, "xmax_e7": None, "ymax_e7": None,
                                     "geometry": None, "centroid_lat_e7": None, "centroid_lon_e7": None,
                                     "hilbert": None, "visible": False})

    _write_parquet(
        con, root, f"history/{HIST_GEN}/spatial/way/cell=000/part-0.parquet",
        [_row_select(way_spatial_cols, way_v1), _row_select(way_spatial_cols, way_v1_minor1),
         _row_select(way_spatial_cols, way_v2), _row_select(way_spatial_cols, way_v2_minor1_tombstone)],
    )
    _write_parquet(
        con, root, f"history/{HIST_GEN}/spatial/way/cell=001/part-0.parquet",
        [_row_select(way_spatial_cols, way_v2_minor1)],
    )
    way_byid_rows = [way_v1, way_v1_minor1, way_v2, way_v2_minor1]
    way_byid_part = _write_parquet(
        con, root, f"history/{HIST_GEN}/byid/way/part-0.parquet",
        [_row_select(way_byid_cols, r) for r in way_byid_rows],
    )
    way_byid_meta = {**way_byid_part, "min_id": info.hist_way_id, "max_id": info.hist_way_id}

    # ------------------------------------------------------------ relation
    def rel_bbox(frac: float) -> tuple[int, int, int, int]:
        s, w, n, e = leaf_bbox["000"]
        lat, lon = s + (n - s) * frac, w + (e - w) * frac
        e7 = to_e7(lat), to_e7(lon)
        return e7[1] - 100, e7[0] - 100, e7[1] + 100, e7[0] + 100

    def rel_row(version, valid_from, valid_to, bbox, minor=0):
        xmin, ymin, xmax, ymax = bbox
        return {
            "id": info.hist_relation_id, "members": [{"type": "n", "ref": info.hist_node_id, "role": "label"}],
            "tags": {"type": "multipolygon", "landuse": "forest"}, "version": version, "changeset": 9900,
            "timestamp": valid_from.replace("T", " ").rstrip("Z"), "uid": 501, "user": "tester0",
            "xmin_e7": xmin, "ymin_e7": ymin, "xmax_e7": xmax, "ymax_e7": ymax,
            "geometry": None, "centroid_lat_e7": (ymin + ymax) // 2, "centroid_lon_e7": (xmin + xmax) // 2,
            "hilbert": 0, "cell": info.hist_relation_cell, "minor": minor,
            "valid_from": valid_from.replace("T", " ").rstrip("Z"),
            "valid_to": valid_to.replace("T", " ").rstrip("Z") if valid_to else None, "visible": True,
        }

    rel_v1 = rel_row(1, info.hist_relation_v1_from, info.hist_relation_v1_minor1_from, rel_bbox(0.3))
    rel_v1_minor1 = rel_row(1, info.hist_relation_v1_minor1_from, info.hist_relation_v2_from, rel_bbox(0.32), minor=1)
    rel_v2 = rel_row(2, info.hist_relation_v2_from, None, rel_bbox(0.34))

    def _rel_spatial_row(r):
        row = dict(r)
        row.pop("geometry", None)
        return row

    _write_parquet(
        con, root, f"history/{HIST_GEN}/spatial/relation/cell=00/part-0.parquet",
        [_row_select(rel_spatial_cols, r) for r in (rel_v1, rel_v1_minor1, rel_v2)],
    )
    rel_byid_rows = [rel_v1, rel_v1_minor1, rel_v2]
    rel_byid_part = _write_parquet(
        con, root, f"history/{HIST_GEN}/byid/relation/part-0.parquet",
        [_row_select(rel_byid_cols, r) for r in rel_byid_rows],
    )
    rel_byid_meta = {**rel_byid_part, "min_id": info.hist_relation_id, "max_id": info.hist_relation_id}

    # ------------------------------------------------------------ manifest
    manifest_path = root / "manifest" / "1.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["manifest_version"] = 5
    manifest["history"] = {
        "generation": HIST_GEN,
        "since": HISTORY_SINCE,
        "minor_versions": True,
        "spatial": {
            "node": {"000": [node_spatial_000], "001": [node_spatial_001]},
            "way": {
                "000": [{"path": f"history/{HIST_GEN}/spatial/way/cell=000/part-0.parquet", "rows": 4,
                         "bytes": (root / f"history/{HIST_GEN}/spatial/way/cell=000/part-0.parquet").stat().st_size}],
                "001": [{"path": f"history/{HIST_GEN}/spatial/way/cell=001/part-0.parquet", "rows": 1,
                         "bytes": (root / f"history/{HIST_GEN}/spatial/way/cell=001/part-0.parquet").stat().st_size}],
            },
            "relation": {
                "00": [{"path": f"history/{HIST_GEN}/spatial/relation/cell=00/part-0.parquet", "rows": 3,
                        "bytes": (root / f"history/{HIST_GEN}/spatial/relation/cell=00/part-0.parquet").stat().st_size}],
            },
        },
        "byid": {
            "node": [node_byid_meta],
            "way": [way_byid_meta],
            "relation": [rel_byid_meta],
        },
        "tiers": {
            "hour": {
                "version": 1, "seq_from": 1, "seq_to": 1, "timestamp": info.hist_tier_node_v2_from,
                "rows": {"node": 1, "way": 0, "relation": 0},
                "cells": {"node": [info.hist_tier_node_cell], "way": [], "relation": []},
                "files": {
                    "node": {"spatial": tier_hour_node_spatial["path"], "byid": tier_hour_node_byid["path"]},
                    "way": {}, "relation": {},
                },
            },
        },
        "stats": {
            "rows": {"node": len(node_byid_rows), "way": len(way_byid_rows), "relation": len(rel_byid_rows)},
            "minor_rows": {"node": 0, "way": 2, "relation": 1},
            "bytes": sum(
                (root / p).stat().st_size
                for p in [
                    node_spatial_000["path"], node_spatial_001["path"], node_byid_part["path"],
                    way_byid_part["path"], rel_byid_part["path"],
                ]
            ),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    # manifest/LATEST already points at "1" (make_fixture.build's own
    # write); the v5 history section lives in the same numbered file.

    if own_con:
        con.close()
    return info


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        i = build(tmp)
        print(json.dumps(json.loads((Path(tmp) / "manifest" / "1.json").read_text())["history"], indent=2)[:2000])
