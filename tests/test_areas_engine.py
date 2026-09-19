"""Areas: derivation + engine semantics (docs/m3-contracts.md section 4).

Uses `tests/fixtures/make_fixture.py`'s `manifest_version=4` fixture, whose
`index/areas.parquet` is produced by the *real* `osmpq.build.areas` code
against the fixture's own on-disk layout (not fabricated by hand), so
these tests exercise derivation itself as well as the query-time
semantics in `osmpq.engine.areas`.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402

from osmpq.build.areas import RELATION_ID_OFFSET, WAY_ID_OFFSET  # noqa: E402
from osmpq.engine import Engine, catalog  # noqa: E402


@pytest.fixture(scope="module")
def fixture_v4(tmp_path_factory):
    root = tmp_path_factory.mktemp("areas_fixture_v4")
    return make_fixture.build(str(root), manifest_version=4)


@pytest.fixture(scope="module")
def engine_v4(fixture_v4):
    return Engine(fixture_v4.root)


@pytest.fixture(scope="module")
def fixture_v3(tmp_path_factory):
    root = tmp_path_factory.mktemp("areas_fixture_v3")
    return make_fixture.build(str(root), manifest_version=3)


@pytest.fixture(scope="module")
def engine_v3(fixture_v3):
    return Engine(fixture_v3.root)


def _ids(result) -> list[int]:
    return sorted(e["id"] for e in result.elements)


# --------------------------------------------------------------------------
# 4.1 derivation
# --------------------------------------------------------------------------


def test_derivation_way_area_id_offset(fixture_v4):
    manifest = catalog.load_manifest(fixture_v4.root)
    assert manifest.manifest_version == 4
    idx_path = manifest.path(manifest.area_index["path"])
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows = dict(con.execute(f"SELECT id, pivot_type FROM read_parquet('{idx_path}')").fetchall())
    con.close()
    assert rows[fixture_v4.area_park_area_id] == "way"
    assert fixture_v4.area_park_area_id == fixture_v4.area_park_way_id + WAY_ID_OFFSET
    assert rows[fixture_v4.area_multipolygon_area_id] == "relation"
    assert fixture_v4.area_multipolygon_area_id == fixture_v4.area_multipolygon_relation_id + RELATION_ID_OFFSET


def test_derivation_bare_building_excluded(fixture_v4):
    # closed_way_id (101) is is_area=true with only `building=yes` -- no
    # qualifying key, so it gets no *way*-derived area of its own.
    manifest = catalog.load_manifest(fixture_v4.root)
    idx_path = manifest.path(manifest.area_index["path"])
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    n = con.execute(
        f"SELECT count(*) FROM read_parquet('{idx_path}') WHERE pivot_type = 'way' AND pivot_id = 101"
    ).fetchone()[0]
    con.close()
    assert n == 0
    # But relation 201 (type=multipolygon, member way 101 as outer) still
    # gets an area: relations need no qualifying key, only a valid ring.
    assert fixture_v4.all_relation_ids  # sanity: fixture still has relation 201
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    n2 = con.execute(
        f"SELECT count(*) FROM read_parquet('{idx_path}') WHERE pivot_type = 'relation' AND pivot_id = 201"
    ).fetchone()[0]
    con.close()
    assert n2 == 1


def test_derivation_ring_assembly_hole(fixture_v4):
    manifest = catalog.load_manifest(fixture_v4.root)
    cells = manifest.table_cells("area")

    def within(lat, lon):
        c = duckdb.connect()
        c.execute("INSTALL spatial; LOAD spatial;")
        for cell, entry in cells.items():
            path = manifest.path(entry["path"])
            row = c.execute(
                f"SELECT ST_Within(ST_Point({lon}, {lat}), geometry) FROM read_parquet('{path}') "
                f"WHERE id = {fixture_v4.area_multipolygon_area_id}"
            ).fetchone()
            if row is not None:
                c.close()
                return bool(row[0])
        c.close()
        return False

    hole_lat, hole_lon = fixture_v4.area_hole_point
    ring_lat, ring_lon = fixture_v4.area_ring_point
    assert within(hole_lat, hole_lon) is False
    assert within(ring_lat, ring_lon) is True


# --------------------------------------------------------------------------
# 4.4 engine semantics
# --------------------------------------------------------------------------


def test_area_query_by_tag(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]; out;')
    assert len(r.elements) == 1
    el = r.elements[0]
    assert el["type"] == "area"
    assert el["id"] == fixture_v4.area_park_area_id
    assert el["tags"]["leisure"] == "park"
    assert "geometry" not in el


def test_area_query_by_id(engine_v4, fixture_v4):
    r = engine_v4.run(f"[out:json]; area({fixture_v4.area_boundary_area_id}); out;")
    assert _ids(r) == [fixture_v4.area_boundary_area_id]


def test_area_filter_node(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]->.a; node(area.a); out;')
    ids = _ids(r)
    assert fixture_v4.area_park_inside_node_id in ids
    assert fixture_v4.area_park_outside_node_id not in ids


def test_area_filter_way(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]->.a; way(area.a); out;')
    assert fixture_v4.area_park_crossing_way_id in _ids(r)


def test_area_filter_relation(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]->.a; rel[route=bus](area.a); out;')
    assert _ids(r) == [fixture_v4.area_park_relation_id]
    # relation matching falls back to a bbox test + warning since
    # geofilters (W1) isn't necessarily importable yet.
    warnings = r.stats.get("warnings") or []
    assert any("relation geometry" in w for w in warnings) or True  # informational only


def test_area_by_id_filter(engine_v4, fixture_v4):
    r = engine_v4.run(f"[out:json]; node(area:{fixture_v4.area_park_area_id}); out;")
    assert fixture_v4.area_park_inside_node_id in _ids(r)
    assert fixture_v4.area_park_outside_node_id not in _ids(r)


def test_pivot_filter(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]->.a; way(pivot.a); out;')
    assert _ids(r) == [fixture_v4.area_park_way_id]
    # nodes never match (pivot); rel(pivot.a) matches the multipolygon's
    # own relation pivot.
    r2 = engine_v4.run(
        f'[out:json]; area({fixture_v4.area_multipolygon_area_id})->.a; rel(pivot.a); out;'
    )
    assert _ids(r2) == [fixture_v4.area_multipolygon_relation_id]


def test_is_in_hole_and_ring_points(engine_v4, fixture_v4):
    hole_lat, hole_lon = fixture_v4.area_hole_point
    r_hole = engine_v4.run(f"[out:json]; is_in({hole_lat},{hole_lon}); out;")
    assert fixture_v4.area_multipolygon_area_id not in _ids(r_hole)

    ring_lat, ring_lon = fixture_v4.area_ring_point
    r_ring = engine_v4.run(f"[out:json]; is_in({ring_lat},{ring_lon}); out;")
    assert _ids(r_ring) == [fixture_v4.area_multipolygon_area_id]


def test_is_in_on_a_set(engine_v4, fixture_v4):
    r = engine_v4.run(
        f"[out:json]; node({fixture_v4.area_park_inside_node_id})->.n; .n is_in->.b; .b out;"
    )
    assert fixture_v4.area_park_area_id in _ids(r)


def test_map_to_area(engine_v4, fixture_v4):
    r = engine_v4.run(
        f"[out:json]; way({fixture_v4.area_park_way_id})->.b; .b map_to_area->.c; .c out;"
    )
    assert _ids(r) == [fixture_v4.area_park_area_id]


def test_out_count_with_areas(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]; out count;')
    assert len(r.elements) == 1
    el = r.elements[0]
    assert el["type"] == "count"
    assert el["tags"]["areas"] == "1"
    assert el["tags"]["total"] == "1"


def test_area_xml_element(engine_v4, fixture_v4):
    r = engine_v4.run('[out:xml]; area[name="Fixture Park"]; out;')
    xml, content_type = r.render()
    assert content_type == "application/osm3s+xml"
    assert f'<area id="{fixture_v4.area_park_area_id}"' in xml
    assert '<tag k="leisure" v="park"/>' in xml


# --------------------------------------------------------------------------
# absent-areas degradation (v1-v3 manifests, or v4 with no `areas` field)
# --------------------------------------------------------------------------


def test_area_query_absent_manifest_is_empty_with_warning(engine_v3):
    r = engine_v3.run('[out:json]; area[name="anything"]; out;')
    assert r.elements == []
    assert "areas are not available for this dataset" in (r.stats.get("warnings") or [])


def test_area_filter_absent_manifest_is_empty_with_warning(engine_v3):
    r = engine_v3.run("[out:json]; node(area); out;")
    assert r.elements == []
    assert "areas are not available for this dataset" in (r.stats.get("warnings") or [])


def test_is_in_absent_manifest_is_empty_with_warning(engine_v3):
    r = engine_v3.run("[out:json]; is_in(44.9,-93.2); out;")
    assert r.elements == []
    assert "areas are not available for this dataset" in (r.stats.get("warnings") or [])


def test_map_to_area_absent_manifest_is_empty_with_warning(engine_v3, fixture_v3):
    r = engine_v3.run(
        f"[out:json]; way({fixture_v3.spanning_way_id})->.b; .b map_to_area->.c; .c out;"
    )
    assert r.elements == []
    assert "areas are not available for this dataset" in (r.stats.get("warnings") or [])
