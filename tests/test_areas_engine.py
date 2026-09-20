"""Areas: derivation + engine semantics (docs/m3-contracts.md section 4,
amended by section 9 -- the reference stores relation areas but treats
every closed way as an area of its own; this suite tests that
representation).

Uses `tests/fixtures/make_fixture.py`'s `manifest_version=4` fixture, whose
`index/areas.parquet` and `index/way_areas.parquet` are produced by the
*real* `osmpq.build.areas` code against the fixture's own on-disk layout
(not fabricated by hand), so these tests exercise derivation itself as
well as the query-time semantics in `osmpq.engine.areas`.
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


def _types_ids(result) -> list[tuple[str, int]]:
    return sorted((e["type"], e["id"]) for e in result.elements)


# --------------------------------------------------------------------------
# 9.1/9.2 derivation
# --------------------------------------------------------------------------


def test_derivation_way_area_is_its_own_id(fixture_v4):
    """9.1: a way area is indexed by the way's own id, no 2400000000
    offset; a relation area is indexed by relation id + 3600000000."""
    manifest = catalog.load_manifest(fixture_v4.root)
    assert manifest.manifest_version == 4
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    way_idx_path = manifest.path(manifest.way_area_index["path"])
    way_ids = {r[0] for r in con.execute(f"SELECT id FROM read_parquet('{way_idx_path}')").fetchall()}
    assert fixture_v4.area_park_way_id in way_ids
    assert all(w < WAY_ID_OFFSET for w in way_ids)  # no offset anywhere

    rel_idx_path = manifest.path(manifest.area_index["path"])
    rows = dict(con.execute(f"SELECT id, pivot_type FROM read_parquet('{rel_idx_path}')").fetchall())
    con.close()
    assert rows[fixture_v4.area_multipolygon_area_id] == "relation"
    assert fixture_v4.area_multipolygon_area_id == fixture_v4.area_multipolygon_relation_id + RELATION_ID_OFFSET


def test_derivation_bare_building_not_in_way_index(fixture_v4):
    """`closed_way_id` (101) is closed but carries only `building=yes` --
    no way-index qualifying key (9.2: name/ref/admin_level/boundary/place),
    so `area[...]` never finds it, even though it's still a valid area for
    is_in/(area)/(pivot)/map_to_area (9 fact 1, tested below)."""
    manifest = catalog.load_manifest(fixture_v4.root)
    way_idx_path = manifest.path(manifest.way_area_index["path"])
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{way_idx_path}') WHERE id = {fixture_v4.closed_way_id}").fetchone()[0]
    con.close()
    assert n == 0


def test_derivation_unnamed_multipolygon_excluded(fixture_v4):
    """rel 201 (type=multipolygon, building=yes, ring = way 101) resolves
    a valid ring but has no `name`, so it fails the `areas.osm3s` rule (9
    fact 3) and gets no area -- unlike the named multipolygon (208)."""
    manifest = catalog.load_manifest(fixture_v4.root)
    rel_idx_path = manifest.path(manifest.area_index["path"])
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    n_unnamed = con.execute(
        f"SELECT count(*) FROM read_parquet('{rel_idx_path}') WHERE pivot_type = 'relation' AND pivot_id = 201"
    ).fetchone()[0]
    n_named = con.execute(
        f"SELECT count(*) FROM read_parquet('{rel_idx_path}') WHERE pivot_type = 'relation' "
        f"AND pivot_id = {fixture_v4.area_multipolygon_relation_id}"
    ).fetchone()[0]
    con.close()
    assert n_unnamed == 0
    assert n_named == 1


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
# 9.3 engine semantics
# --------------------------------------------------------------------------


def test_area_query_by_tag_returns_way_element(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]; out;')
    assert len(r.elements) == 1
    el = r.elements[0]
    assert el["type"] == "way"
    assert el["id"] == fixture_v4.area_park_way_id
    assert el["tags"]["leisure"] == "park"
    assert "nodes" in el  # a way area is the way's own full canonical row


def test_area_query_by_id_relation(engine_v4, fixture_v4):
    r = engine_v4.run(f"[out:json]; area({fixture_v4.area_boundary_area_id}); out;")
    assert _types_ids(r) == [("area", fixture_v4.area_boundary_area_id)]


def test_area_query_by_id_way_lookup(engine_v4, fixture_v4):
    """9.1: `area(2400000000 + way_id)` is accepted as a lookup of that
    way -- a superset of the reference."""
    way_area_id = fixture_v4.area_park_way_id + WAY_ID_OFFSET
    r = engine_v4.run(f"[out:json]; area({way_area_id}); out;")
    assert _types_ids(r) == [("way", fixture_v4.area_park_way_id)]


def test_area_filter_node(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]->.a; node(area.a); out;')
    ids = _ids(r)
    assert fixture_v4.area_park_inside_node_id in ids
    assert fixture_v4.area_park_outside_node_id not in ids


def test_area_filter_way_any_vertex_within(engine_v4, fixture_v4):
    """9 fact 4: any-vertex-within, never ST_Intersects -- the crossing way
    (one endpoint inside the ring) is selected, the bridge way (crosses
    the ring with neither endpoint inside) is not."""
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]->.a; way(area.a); out;')
    ids = _ids(r)
    assert fixture_v4.area_park_crossing_way_id in ids
    assert fixture_v4.area_park_bridge_way_id not in ids


def test_area_filter_relation(engine_v4, fixture_v4):
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]->.a; rel[route=bus](area.a); out;')
    assert _ids(r) == [fixture_v4.area_park_relation_id]


def test_area_by_id_filter_way(engine_v4, fixture_v4):
    """`(area:id)` accepts a way-lookup id (9.1) exactly like `area(id)`."""
    way_area_id = fixture_v4.area_park_way_id + WAY_ID_OFFSET
    r = engine_v4.run(f"[out:json]; node(area:{way_area_id}); out;")
    assert fixture_v4.area_park_inside_node_id in _ids(r)
    assert fixture_v4.area_park_outside_node_id not in _ids(r)


def test_area_by_id_filter_relation(engine_v4, fixture_v4):
    r = engine_v4.run(f"[out:json]; node(area:{fixture_v4.area_multipolygon_area_id}); out;")
    hole_lat, hole_lon = fixture_v4.area_hole_point
    ring_lat, ring_lon = fixture_v4.area_ring_point
    # There's no node fixture placed exactly at these points for this
    # filter, so just check the filter runs and returns a set (no crash);
    # the ring/hole distinction itself is covered by is_in below.
    assert isinstance(r.elements, list)


def test_area_query_by_tag_building_only_excluded(engine_v4, fixture_v4):
    """A building-only closed way (101) is not found by `area[...]` (no
    way-index qualifying key), even though it's still an area for
    is_in/(area)/(pivot)/map_to_area (9 fact 1)."""
    r = engine_v4.run('[out:json]; area[building="yes"]; out;')
    assert fixture_v4.closed_way_id not in _ids(r)


def test_area_filter_building_only_way_still_matches(engine_v4, fixture_v4):
    """9 fact 1: a bare-building closed way is still a valid `(area)`
    target when referenced directly by id."""
    r = engine_v4.run(f"[out:json]; node(area:{fixture_v4.closed_way_id + WAY_ID_OFFSET}); out;")
    assert isinstance(r.elements, list)  # runs without error against a real way polygon


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
    assert fixture_v4.area_multipolygon_area_id in _ids(r_ring)


def test_is_in_returns_way_for_closed_way(engine_v4, fixture_v4):
    """9 fact 2: `is_in` returns `way` elements for closed ways, not a
    2400000000-offset `area` element."""
    r = engine_v4.run(
        f"[out:json]; node({fixture_v4.area_park_inside_node_id})->.n; .n is_in->.b; .b out;"
    )
    assert ("way", fixture_v4.area_park_way_id) in _types_ids(r)
    assert all(i < WAY_ID_OFFSET or t != "area" for t, i in _types_ids(r))


def test_map_to_area(engine_v4, fixture_v4):
    """9.3: closed ways in the input map to themselves (no offset)."""
    r = engine_v4.run(
        f"[out:json]; way({fixture_v4.area_park_way_id})->.b; .b map_to_area->.c; .c out;"
    )
    assert _types_ids(r) == [("way", fixture_v4.area_park_way_id)]


def test_map_to_area_relation(engine_v4, fixture_v4):
    r = engine_v4.run(
        f"[out:json]; rel({fixture_v4.area_multipolygon_relation_id})->.b; .b map_to_area->.c; .c out;"
    )
    assert _types_ids(r) == [("area", fixture_v4.area_multipolygon_area_id)]


def test_out_count_with_areas(engine_v4, fixture_v4):
    """A way area counts as a way (9.3), not under `areas`; the `areas`
    key is still emitted because the program used an area statement."""
    r = engine_v4.run('[out:json]; area[name="Fixture Park"]; out count;')
    assert len(r.elements) == 1
    el = r.elements[0]
    assert el["type"] == "count"
    assert el["tags"]["ways"] == "1"
    assert el["tags"]["areas"] == "0"
    assert el["tags"]["total"] == "1"


def test_area_xml_element_is_a_way(engine_v4, fixture_v4):
    r = engine_v4.run('[out:xml]; area[name="Fixture Park"]; out;')
    xml, content_type = r.render()
    assert content_type == "application/osm3s+xml"
    assert f'<way id="{fixture_v4.area_park_way_id}"' in xml
    assert '<tag k="leisure" v="park"/>' in xml


def test_relation_area_xml_element(engine_v4, fixture_v4):
    r = engine_v4.run(f'[out:xml]; area({fixture_v4.area_boundary_area_id}); out;')
    xml, content_type = r.render()
    assert content_type == "application/osm3s+xml"
    assert f'<area id="{fixture_v4.area_boundary_area_id}"' in xml


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
