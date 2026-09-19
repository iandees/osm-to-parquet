"""Tests for docs/m3-contracts.md section 3: `(around:...)`,
`(around.set:...)`, literal-coordinate `(around:r,lat,lon,...)`, and
`(poly:"...")`, against the synthetic fixture
(tests/fixtures/make_fixture.py) plus a standalone check of the distance
approach itself (section 3.1).
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import duckdb
import pytest

from osmpq.engine import Engine, geofilters

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("geofilters_fixture")
    return make_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def _node_latlon(engine, node_id: int) -> tuple[float, float]:
    r = engine.run(f"[out:json];node({node_id});out;")
    el = r.elements[0]
    return el["lat"], el["lon"]


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Reference implementation for the 3.1 verification test only --
    independent of anything in geofilters.py."""
    r = 6371008.8  # WGS84 mean radius, meters
    p1, p2, l1, l2 = map(math.radians, [lat1, lat2, lon1, lon2])
    dlat, dlon = p2 - p1, l2 - l1
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# --------------------------------------------------------------------- 3.1


@pytest.fixture(scope="module")
def spatial_con():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    yield con
    con.close()


def _engine_point_distance_m(con, lat1, lon1, lat2, lon2) -> float:
    """The exact projection geofilters.py's predicates use, applied
    directly to two points -- point-to-point sanity/accuracy check
    against haversine (contract 3.1: within 0.2%)."""
    lat_ts = geofilters._lat_ts((min(lat1, lat2), min(lon1, lon2), max(lat1, lat2), max(lon1, lon2)))
    p1 = geofilters._project_sql(f"ST_Point({lon1}, {lat1})", lat_ts)
    p2 = geofilters._project_sql(f"ST_Point({lon2}, {lat2})", lat_ts)
    return con.execute(f"SELECT ST_Distance({p1}, {p2})").fetchone()[0]


MINNEAPOLIS_PAIRS = [
    # (lat1, lon1, lat2, lon2) -- real Minnesota-area coordinates.
    (44.9778, -93.2650, 44.9400, -93.0900),  # ~14.4 km, downtown -> St. Paul-ish
    (44.9749861, -93.2598737, 44.9483483, -93.2388449),  # two light-rail stations
    (46.7867, -92.1005, 44.9778, -93.2650),  # Duluth -> Minneapolis, ~220 km
    (43.9500, -93.3000, 44.9778, -93.2650),  # southern MN -> Minneapolis
]


@pytest.mark.parametrize("lat1,lon1,lat2,lon2", MINNEAPOLIS_PAIRS)
def test_distance_matches_haversine_reference(spatial_con, lat1, lon1, lat2, lon2):
    got = _engine_point_distance_m(spatial_con, lat1, lon1, lat2, lon2)
    want = _haversine_m(lat1, lon1, lat2, lon2)
    assert got == pytest.approx(want, rel=0.002)  # contract 3.1: within 0.2%


def test_distance_point_to_linestring_geometric(spatial_con):
    """Asserted geometrically (contract 3.1), not against haversine: a
    point sitting on a west-east segment is 0 m away; a point 0.1 degrees
    of latitude off that segment is close to the geodesic meridian
    distance for 0.1 degrees (~11.1 km), not to some unrelated value."""
    lat_ts = 45.0
    line = geofilters._project_sql("ST_GeomFromText('LINESTRING (-93.0 45.0, -92.9 45.0)')", lat_ts)
    on_line = geofilters._project_sql("ST_GeomFromText('POINT (-92.95 45.0)')", lat_ts)
    off_line = geofilters._project_sql("ST_GeomFromText('POINT (-92.95 45.1)')", lat_ts)

    d_on = spatial_con.execute(f"SELECT ST_Distance({line}, {on_line})").fetchone()[0]
    d_off = spatial_con.execute(f"SELECT ST_Distance({line}, {off_line})").fetchone()[0]

    assert d_on == pytest.approx(0.0, abs=1.0)
    assert d_off == pytest.approx(11119.5, rel=0.01)


# ------------------------------------------------------------- around: node


def test_around_named_set_finds_nearby_node_excludes_far_one(engine, fixture):
    lat1, lon1 = _node_latlon(engine, fixture.cafe_node_id)
    lat2, lon2 = _node_latlon(engine, fixture.cafe_node2_id)
    lat3, lon3 = _node_latlon(engine, fixture.restaurant_node_id)
    d12 = _haversine_m(lat1, lon1, lat2, lon2)
    d13 = _haversine_m(lat1, lon1, lat3, lon3)
    assert d12 < d13  # sanity: the fixture's own geometry, not our choice
    radius = (d12 + d13) / 2

    r = engine.run(
        f"[out:json];node({fixture.cafe_node_id})->.a;node(around.a:{radius});out ids;"
    )
    ids = {e["id"] for e in r.elements}
    assert fixture.cafe_node_id in ids  # distance 0 to itself
    assert fixture.cafe_node2_id in ids
    assert fixture.restaurant_node_id not in ids


def test_around_literal_coords_point(engine, fixture):
    """(around:r,lat,lon) -- a literal point source, no named set."""
    lat1, lon1 = _node_latlon(engine, fixture.cafe_node_id)
    lat2, lon2 = _node_latlon(engine, fixture.cafe_node2_id)
    d12 = _haversine_m(lat1, lon1, lat2, lon2)

    r = engine.run(f"[out:json];node(around:{d12 * 1.2},{lat1},{lon1});out ids;")
    ids = {e["id"] for e in r.elements}
    assert fixture.cafe_node2_id in ids


# -------------------------------------------------------------- around: way


def test_around_way_candidate(engine, fixture):
    """A literal point on the diagonal way's own line matches it (way
    LINESTRING candidate geometry, not just its flat bbox), and does not
    pull in a way from a completely different part of the fixture."""
    sw_lat, sw_lon = _node_latlon(engine, fixture.diagonal_node_ids[0])
    ne_lat, ne_lon = _node_latlon(engine, fixture.diagonal_node_ids[1])
    mid_lat, mid_lon = (sw_lat + ne_lat) / 2, (sw_lon + ne_lon) / 2

    r = engine.run(f"[out:json];way(around:50000,{mid_lat},{mid_lon});out ids;")
    ids = {e["id"] for e in r.elements}
    assert fixture.diagonal_way_id in ids
    # A way stored in a different leaf, far from the diagonal's midpoint.
    assert 108 not in ids


# --------------------------------------------------------------- poly: way


def test_poly_way_crossing_matches_exact_geometry_not_flat_bbox(engine, fixture):
    """The diagonal way's flat bbox overlaps `diagonal_bbox_miss`, but its
    actual line never does (same fixture the bbox exact-match tests use,
    tests/test_engine_basic.py); `(poly:...)` must use the real geometry,
    not the flat-bbox prune."""

    def poly_str(bbox):
        s, w, n, e = bbox
        return f"{s} {w} {s} {e} {n} {e} {n} {w}"

    hit = poly_str(fixture.diagonal_bbox_hit)
    miss = poly_str(fixture.diagonal_bbox_miss)

    r_hit = engine.run(f'[out:json];way(id:{fixture.diagonal_way_id})(poly:"{hit}");out ids;')
    assert {e["id"] for e in r_hit.elements} == {fixture.diagonal_way_id}

    r_miss = engine.run(f'[out:json];way(id:{fixture.diagonal_way_id})(poly:"{miss}");out ids;')
    assert r_miss.elements == []


# ---------------------------------------------------------- poly: relation


def test_poly_relation_matched_via_member_way(engine, fixture):
    """diagonal_relation_id's only member is the diagonal way; matching it
    requires resolving that member's exact geometry via
    relation_geometry_table (3.5), not just the relation's own stored
    (flat) bbox."""

    def poly_str(bbox):
        s, w, n, e = bbox
        return f"{s} {w} {s} {e} {n} {e} {n} {w}"

    hit = poly_str(fixture.diagonal_bbox_hit)
    miss = poly_str(fixture.diagonal_bbox_miss)

    r_hit = engine.run(f'[out:json];relation(id:{fixture.diagonal_relation_id})(poly:"{hit}");out ids;')
    assert {e["id"] for e in r_hit.elements} == {fixture.diagonal_relation_id}

    r_miss = engine.run(f'[out:json];relation(id:{fixture.diagonal_relation_id})(poly:"{miss}");out ids;')
    assert r_miss.elements == []


# -------------------------------------------------------- empty source set


def test_around_default_empty_set_is_empty_result(engine, fixture):
    # Default source ".": nothing has ever populated it in this program,
    # so it exists (ensure_empty_set) but has zero rows -- contract 3.2:
    # "Empty source set -> empty result", not an error.
    r = engine.run("[out:json];node(around:1000);out;")
    assert r.elements == []
    assert r.remark is None


def test_around_unset_named_source_is_a_runtime_error(engine, fixture):
    r = engine.run("[out:json];node(around.nope:1000);out;")
    assert r.remark is not None
    assert "nope" in r.remark


# --------------------------------------------------- implied bbox narrows

def test_around_implied_bbox_avoids_full_scan_warning(engine, fixture):
    """A query with an (around:) filter and no explicit bbox still gets a
    real (non-None) effective bbox from `implied_bbox`, so it must not
    trigger the "no bbox and no ids; scanning all cells" warning a truly
    unbounded query gets."""
    r = engine.run(
        f"[out:json];node({fixture.cafe_node_id})->.a;node(around.a:10);out;"
    )
    warnings = r.stats.get("warnings") or []
    assert not any("scanning all cells" in w for w in warnings)
    assert r.stats["files_read"] >= 1
