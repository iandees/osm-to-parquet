"""``osmpq compact`` area re-derivation (docs/m3-contracts.md section 9.2):
relation areas are re-derived for touched pivots and merged into the
previous generation's area cell files like before; the way index has no
per-pivot merge at all -- it is rebuilt in full from the compacted way
byid table. Self-contained (not `make_fixture.py`, whose delta-tier
machinery is only exercised at `manifest_version=3` and doesn't yet know
about areas): builds a minimal two-leaf root by hand (one way that is
both a way-area on its own and the sole member of a relation-area), runs
the real `osmpq.build.areas` derivation once to get a "before" state, then
applies one hand-written `hour` delta tier that moves the pivot way (and
re-emits the relation that references it, as the updater's touched-set
fixed point would) to the other leaf, compacts, and checks both the
relation area and the way-index entry followed it.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from osmpq.build.areas import RELATION_ID_OFFSET, build_areas_for_manifest  # noqa: E402
from osmpq.build.compact import CompactOptions, compact  # noqa: E402
from osmpq.engine.catalog import Manifest as EngineManifest  # noqa: E402
from osmpq.engine.catalog import cell_bbox  # noqa: E402

PROMOTED = ["leisure", "name"]
GEN = "g0001"
WAY_ID = 9001
REL_ID = 9101
TAGS = {"leisure": "park", "name": "Movable Park"}
REL_TAGS = {"type": "multipolygon", "name": "Movable Park Relation"}


def _to_e7(deg: float) -> int:
    return int(round(deg * 1e7))


def _promoted_sql(tags: dict) -> str:
    return ", ".join(
        f"'{tags[k]}'::VARCHAR AS \"{k}\"" if k in tags else f"NULL::VARCHAR AS \"{k}\"" for k in PROMOTED
    )


def _square(bbox, frac_lo: float, frac_hi: float) -> list[tuple[float, float]]:
    s, w, n, e = bbox
    lat_lo, lat_hi = s + (n - s) * frac_lo, s + (n - s) * frac_hi
    lon_lo, lon_hi = w + (e - w) * frac_lo, w + (e - w) * frac_hi
    return [(lon_lo, lat_lo), (lon_lo, lat_hi), (lon_hi, lat_hi), (lon_hi, lat_lo)]


def _bbox_of(pts: list[tuple[float, float]]) -> tuple[float, float, float, float]:
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    return min(xs), min(ys), max(xs), max(ys)


def _way_row_sql(pts: list[tuple[float, float]], version: int, cell: str) -> str:
    wkt = "LINESTRING (" + ", ".join(f"{lon} {lat}" for lon, lat in pts + [pts[0]]) + ")"
    xmin, ymin, xmax, ymax = _bbox_of(pts)
    return (
        f"SELECT {WAY_ID} AS id, [1,2,3,4,1]::BIGINT[] AS refs, "
        f"MAP {{'leisure': '{TAGS['leisure']}', 'name': '{TAGS['name']}'}} AS tags, "
        f"{_promoted_sql(TAGS)}, "
        f"{version} AS version, 1001 AS changeset, TIMESTAMP '2026-09-19 00:00:00' AS \"timestamp\", "
        f"501 AS uid, 'tester' AS \"user\", "
        f"{_to_e7(xmin)} AS xmin_e7, {_to_e7(ymin)} AS ymin_e7, {_to_e7(xmax)} AS xmax_e7, {_to_e7(ymax)} AS ymax_e7, "
        f"ST_GeomFromText('{wkt}') AS geometry, TRUE AS is_closed, TRUE AS is_area, "
        f"{_to_e7((ymin + ymax) / 2)} AS centroid_lat_e7, {_to_e7((xmin + xmax) / 2)} AS centroid_lon_e7, "
        f"0::UBIGINT AS hilbert, '{cell}' AS cell"
    )


def _relation_row_sql(way_bbox: tuple[float, float, float, float], version: int, cell: str) -> str:
    xmin, ymin, xmax, ymax = way_bbox
    members_sql = f"[{{'type': 'w', 'ref': {WAY_ID}, 'role': 'outer'}}]"
    return (
        f"SELECT {REL_ID} AS id, {members_sql}::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[] AS members, "
        f"MAP {{'type': '{REL_TAGS['type']}', 'name': '{REL_TAGS['name']}'}} AS tags, "
        f"{_promoted_sql(REL_TAGS)}, "
        f"{version} AS version, 1002 AS changeset, TIMESTAMP '2026-09-19 00:00:00' AS \"timestamp\", "
        f"502 AS uid, 'tester' AS \"user\", "
        f"{_to_e7(xmin)} AS xmin_e7, {_to_e7(ymin)} AS ymin_e7, {_to_e7(xmax)} AS xmax_e7, {_to_e7(ymax)} AS ymax_e7, "
        f"NULL::GEOMETRY AS geometry, "
        f"{_to_e7((ymin + ymax) / 2)} AS centroid_lat_e7, {_to_e7((xmin + xmax) / 2)} AS centroid_lon_e7, "
        f"0::UBIGINT AS hilbert, '{cell}' AS cell"
    )


@pytest.fixture()
def moved_pivot_root(tmp_path):
    root = tmp_path
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET preserve_insertion_order=false")

    leaf0_bbox = cell_bbox("0")
    pts0 = _square(leaf0_bbox, 0.3, 0.4)
    way_bbox0 = _bbox_of(pts0)

    way_spatial_path = root / f"spatial/{GEN}/way/cell=0/part-0.parquet"
    way_spatial_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * EXCLUDE (cell) FROM ({_way_row_sql(pts0, 1, '0')})) TO '{way_spatial_path}' (FORMAT PARQUET)")
    way_byid_path = root / f"byid/{GEN}/way/part-00000.parquet"
    way_byid_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({_way_row_sql(pts0, 1, '0')}) TO '{way_byid_path}' (FORMAT PARQUET)")

    rel_spatial_path = root / f"spatial/{GEN}/relation/cell=0/part-0.parquet"
    rel_spatial_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * EXCLUDE (cell) FROM ({_relation_row_sql(way_bbox0, 1, '0')})) TO '{rel_spatial_path}' (FORMAT PARQUET)")
    rel_byid_path = root / f"byid/{GEN}/relation/part-00000.parquet"
    rel_byid_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({_relation_row_sql(way_bbox0, 1, '0')}) TO '{rel_byid_path}' (FORMAT PARQUET)")

    man = {
        "manifest_version": 2,
        "generation": GEN,
        "schema_version": 1,
        "coordinate_scale": 10_000_000,
        "promoted_keys": PROMOTED,
        "timestamp_osm_base": "2026-09-19T00:00:00Z",
        "replication_sequence": 1,
        "source": "test_areas_compact.py",
        "extent": [-90.0, -180.0, 90.0, 180.0],
        "leaf_cells": ["0", "1"],
        "ancestor_depths": [0],
        "max_depth": 1,
        "tables": {
            "node": {"cells": {}},
            "way": {"cells": {"0": {
                "path": f"spatial/{GEN}/way/cell=0/part-0.parquet", "rows": 1,
                "bytes": way_spatial_path.stat().st_size, "bbox": [None, None, None, None],
            }}},
            "relation": {"cells": {"0": {
                "path": f"spatial/{GEN}/relation/cell=0/part-0.parquet", "rows": 1,
                "bytes": rel_spatial_path.stat().st_size, "bbox": [None, None, None, None],
            }}},
        },
        "byid": {
            "node": [],
            "way": [{"path": f"byid/{GEN}/way/part-00000.parquet", "min_id": WAY_ID, "max_id": WAY_ID,
                     "rows": 1, "bytes": way_byid_path.stat().st_size}],
            "relation": [{"path": f"byid/{GEN}/relation/part-00000.parquet", "min_id": REL_ID, "max_id": REL_ID,
                          "rows": 1, "bytes": rel_byid_path.stat().st_size}],
        },
        "index": {"node_way": [], "member": []},
        "rowgroup_index": {},
        "producer": {},
        "stats": {},
    }
    cat_man = EngineManifest(root=str(root), data=man)
    man["areas"] = build_areas_for_manifest(con, root, cat_man, PROMOTED)
    man["manifest_version"] = 4

    manifest_dir = root / "manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "1.json").write_text(json.dumps(man))
    (manifest_dir / "LATEST").write_text("1")

    # -- an `hour` delta that moves way WAY_ID from leaf "0" to leaf "1",
    # re-emitting relation REL_ID too (same members, new bbox) -- exactly
    # what the updater's touched-set fixed point does for a relation that
    # lists a touched way as a member (docs/m2-contracts.md). ------------
    leaf1_bbox = cell_bbox("1")
    pts1 = _square(leaf1_bbox, 0.3, 0.4)
    way_bbox1 = _bbox_of(pts1)
    delta_dir = root / f"delta/{GEN}/hour/1"
    delta_dir.mkdir(parents=True, exist_ok=True)

    way_spatial_delta = delta_dir / "way.spatial.parquet"
    con.execute(
        f"COPY ({_way_row_sql(pts1, 2, '1')}, FALSE AS deleted, '0' AS prev_cell, 2 AS seq) "
        f"TO '{way_spatial_delta}' (FORMAT PARQUET)"
    )
    way_byid_delta = delta_dir / "way.byid.parquet"
    con.execute(
        f"COPY ({_way_row_sql(pts1, 2, '1')}, FALSE AS deleted, '0' AS prev_cell, 2 AS seq) "
        f"TO '{way_byid_delta}' (FORMAT PARQUET)"
    )
    rel_spatial_delta = delta_dir / "relation.spatial.parquet"
    con.execute(
        f"COPY ({_relation_row_sql(way_bbox1, 2, '1')}, FALSE AS deleted, '0' AS prev_cell, 2 AS seq) "
        f"TO '{rel_spatial_delta}' (FORMAT PARQUET)"
    )
    rel_byid_delta = delta_dir / "relation.byid.parquet"
    con.execute(
        f"COPY ({_relation_row_sql(way_bbox1, 2, '1')}, FALSE AS deleted, '0' AS prev_cell, 2 AS seq) "
        f"TO '{rel_byid_delta}' (FORMAT PARQUET)"
    )
    con.close()

    man2 = dict(man)
    man2["deltas"] = {
        "hour": {
            "version": 1, "seq_from": 1, "seq_to": 2, "timestamp": "2026-09-19T01:00:00Z",
            "rows": {"node": 0, "way": 1, "relation": 1},
            "files": {
                "way": {"spatial": str(way_spatial_delta.relative_to(root)), "byid": str(way_byid_delta.relative_to(root))},
                "relation": {"spatial": str(rel_spatial_delta.relative_to(root)), "byid": str(rel_byid_delta.relative_to(root))},
            },
            "cells": {"way": ["1"], "relation": ["1"]},
        },
    }
    (manifest_dir / "2.json").write_text(json.dumps(man2))
    (manifest_dir / "LATEST").write_text("2")

    return root, way_bbox1


def test_compact_rederives_relation_area_and_rebuilds_way_index(moved_pivot_root):
    root, way_bbox1 = moved_pivot_root
    rel_area_id = REL_ID + RELATION_ID_OFFSET

    new_man = compact(CompactOptions(root=str(root), tmpdir=str(root / "compact-tmp")))

    assert new_man["manifest_version"] == 4
    assert new_man["deltas"] == {}
    areas = new_man["areas"]

    # -- relation area followed the moved way (9.2: re-derived for the
    # touched relation, merged into the area cell files like before). ----
    assert set(areas["cells"].keys()) == {"1"}  # cell "0" dropped entirely

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    index_path = root / areas["index"]["path"]
    rows = con.execute(f"SELECT id, cell, xmin_e7, ymin_e7 FROM read_parquet('{index_path}')").fetchall()
    xmin1, ymin1, _xmax1, _ymax1 = way_bbox1
    assert rows == [(rel_area_id, "1", _to_e7(xmin1), _to_e7(ymin1))]

    cell_path = root / areas["cells"]["1"]["path"]
    n = con.execute(f"SELECT count(*) FROM read_parquet('{cell_path}') WHERE id = {rel_area_id}").fetchone()[0]
    assert n == 1

    # -- way index rebuilt in full from the compacted way byid table: the
    # way's own entry follows its new cell (9.2: no per-pivot merge, a
    # straight rescan). --------------------------------------------------
    way_index = areas["way_index"]
    way_index_path = root / way_index["path"]
    way_rows = con.execute(f"SELECT id, cell FROM read_parquet('{way_index_path}')").fetchall()
    assert way_rows == [(WAY_ID, "1")]
    con.close()
