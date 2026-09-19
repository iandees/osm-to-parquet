"""Engine tests against the synthetic fixture (tests/fixtures/make_fixture.py).

Covers docs/m0-contracts.md sections 6-8: bbox+tag queries, id lookups,
recursion, set algebra, `out` verbosity/geometry/order/count, tag-filter
semantics (!=, regex, promoted vs. non-promoted keys), timeouts, and
JSON/XML rendering.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine
from osmpq.engine.result import Result
from osmpq.ql.ast import Settings

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("engine_fixture")
    info = make_fixture.build(str(root))
    return info


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def bbox_args(bbox):
    s, w, n, e = bbox
    return f"{s},{w},{n},{e}"


# --------------------------------------------------------------------- bbox


def test_bbox_tagged_node_query(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(f"[out:json];node[amenity=cafe]({b});out;")
    ids = sorted(e["id"] for e in r.elements)
    assert ids == [fixture.cafe_node_id, fixture.cafe_node2_id]
    for e in r.elements:
        assert e["type"] == "node"
        assert "lat" in e and "lon" in e
        assert e["tags"]["amenity"] == "cafe"


def test_bbox_query_no_matches_outside_bbox(engine, fixture):
    # leaf "001" bbox has no amenity=cafe nodes.
    b = bbox_args(fixture.leaf_bbox["001"])
    r = engine.run(f"[out:json];node[amenity=cafe]({b});out;")
    assert r.elements == []


# ------------------------------------------------------------------- recurse


def test_forward_recurse_from_way_gets_untagged_nodes(engine, fixture):
    r = engine.run(f"[out:json];way({fixture.closed_way_id});out;>;out skel;")
    node_elements = [e for e in r.elements if e["type"] == "node"]
    assert len(node_elements) == 4
    for e in node_elements:
        assert "tags" not in e  # ring nodes are untagged
        assert "lat" in e and "lon" in e


def test_backward_recurse_from_node_gets_parent_ways(engine, fixture):
    r = engine.run(f"[out:json];node({fixture.cafe_node_id});<;out ids;")
    got = sorted((e["type"], e["id"]) for e in r.elements)
    # node 1 is referenced by way 102 and way 110, and is itself a member of
    # relation 201.
    assert got == [("relation", 201), ("way", 102), ("way", 110)]


def test_transitive_forward_recurse(engine, fixture):
    r = engine.run(f"[out:json];relation({fixture.node_way_relation_id});>>;out ids;")
    got = sorted((e["type"], e["id"]) for e in r.elements)
    assert ("way", 105) in got
    assert all(t in ("node", "way") for t, _ in got)


def test_transitive_backward_recurse(engine, fixture):
    r = engine.run(f"[out:json];node({fixture.cafe_node_id});<<;out ids;")
    got = {(e["type"], e["id"]) for e in r.elements}
    assert ("way", 102) in got
    assert ("way", 110) in got
    assert ("relation", 201) in got


def test_inline_recurse_filter_w(engine, fixture):
    r = engine.run(f"[out:json];way({fixture.closed_way_id});node(w);out ids;")
    ids = sorted(e["id"] for e in r.elements)
    assert len(ids) == 4
    assert all(e["type"] == "node" for e in r.elements)


# --------------------------------------------------------------- out geom


def test_way_out_geom_refs_order_and_bounds(engine, fixture):
    r = engine.run(f"[out:json];way({fixture.closed_way_id});out geom;")
    assert len(r.elements) == 1
    el = r.elements[0]
    assert el["type"] == "way"
    assert "bounds" in el
    assert len(el["geometry"]) == len(el["nodes"]) == 5
    assert el["nodes"][0] == el["nodes"][-1]  # closed ring
    # geometry follows refs order: first and last point coincide too.
    assert el["geometry"][0] == el["geometry"][-1]


def test_way_out_geom_via_byid_hydrates_geometry(engine, fixture):
    # id lookup (no bbox) goes through byid, which has no geometry column;
    # `out geom` must hydrate it from the spatial file via (cell, id).
    r = engine.run(f"[out:json];way(id:{fixture.closed_way_id});out geom;")
    assert len(r.elements[0]["geometry"]) == 5


def test_relation_out_geom_resolves_members(engine, fixture):
    r = engine.run(f"[out:json];relation({fixture.spanning_relation_id});out geom;")
    assert len(r.elements) == 1
    el = r.elements[0]
    assert "bounds" in el
    members = {m["ref"]: m for m in el["members"]}
    way_member = members[101]
    assert way_member["type"] == "way"
    assert len(way_member["geometry"]) == 5
    node_member = [m for m in el["members"] if m["type"] == "node"][0]
    assert "lat" in node_member and "lon" in node_member


# --------------------------------------------------------------- set algebra


def test_union(engine, fixture):
    r = engine.run(
        f"[out:json];(node({fixture.cafe_node_id});node({fixture.restaurant_node_id}););out ids;"
    )
    assert sorted(e["id"] for e in r.elements) == sorted(
        [fixture.cafe_node_id, fixture.restaurant_node_id]
    )


def test_difference(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(
        f"[out:json];(node[amenity=cafe]({b}); - node({fixture.cafe_node_id}); );out ids;"
    )
    assert [e["id"] for e in r.elements] == [fixture.cafe_node2_id]


def test_input_set_intersection(engine, fixture):
    r = engine.run(
        f"[out:json];node({fixture.cafe_node_id})->.a;node.a[amenity=cafe];out ids;"
    )
    assert [e["id"] for e in r.elements] == [fixture.cafe_node_id]
    r2 = engine.run(
        f"[out:json];node({fixture.cafe_node_id})->.a;node.a[amenity=restaurant];out ids;"
    )
    assert r2.elements == []


# ------------------------------------------------------------------ byid


def test_id_lookup_byid(engine, fixture):
    r = engine.run(
        f"[out:json];node(id:{fixture.bakery_node_id},{fixture.craft_bakery_node_id});out;"
    )
    assert sorted(e["id"] for e in r.elements) == sorted(
        [fixture.bakery_node_id, fixture.craft_bakery_node_id]
    )


def test_id_lookup_spans_byid_parts(engine, fixture):
    # byid/node is split into 2 parts; pick one low and one high id.
    lo, hi = fixture.all_node_ids[0], fixture.all_node_ids[-1]
    r = engine.run(f"[out:json];node(id:{lo},{hi});out ids;")
    assert sorted(e["id"] for e in r.elements) == sorted([lo, hi])


# -------------------------------------------------------------------- out


def test_out_count(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f"[out:json];node[amenity=cafe]({b});out count;")
    assert len(r.elements) == 1
    el = r.elements[0]
    assert el["type"] == "count"
    assert el["tags"]["nodes"] == "3"
    assert el["tags"]["ways"] == "0"
    assert el["tags"]["total"] == "3"


def test_out_qt_orders_by_hilbert_then_id(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r_qt = engine.run(f"[out:json];node({b});out qt;")
    r_all = engine.run(f"[out:json];node({b});out;")
    assert {e["id"] for e in r_qt.elements} == {e["id"] for e in r_all.elements}
    ids_qt = [e["id"] for e in r_qt.elements]
    # Recompute expected order directly from the hilbert function.
    from osmpq.engine.hilbert import lonlat_to_hilbert

    by_id = {n["id"]: n for n in _all_nodes(fixture)}
    expected = sorted(ids_qt, key=lambda i: (lonlat_to_hilbert(by_id[i]["lon"], by_id[i]["lat"]), i))
    assert ids_qt == expected


def _all_nodes(fixture):
    # Recreate node placement deterministically the same way make_fixture does,
    # just to get (lon, lat) for the hilbert-order assertion above.
    import duckdb

    con = duckdb.connect()
    # Rebuild is expensive; instead read node coordinates straight back out
    # of the fixture's own byid parquet files.
    import glob

    nodes = []
    for path in glob.glob(f"{fixture.root}/byid/g0001/node/part-*.parquet"):
        rows = con.execute(f"SELECT id, lat_e7, lon_e7 FROM read_parquet('{path}')").fetchall()
        for nid, lat_e7, lon_e7 in rows:
            nodes.append({"id": nid, "lat": lat_e7 / 1e7, "lon": lon_e7 / 1e7})
    con.close()
    return nodes


def test_out_meta_fields(engine, fixture):
    r = engine.run(f"[out:json];node({fixture.cafe_node_id});out meta;")
    el = r.elements[0]
    for key in ("version", "timestamp", "changeset", "user", "uid"):
        assert key in el
    assert el["timestamp"].endswith("Z")


def test_out_ids_tags_skel_bb_center(engine, fixture):
    r = engine.run(f"[out:json];node({fixture.cafe_node_id});out ids;")
    assert r.elements == [{"type": "node", "id": fixture.cafe_node_id}]

    r = engine.run(f"[out:json];node({fixture.cafe_node_id});out tags;")
    assert "lat" not in r.elements[0]
    assert r.elements[0]["tags"]["amenity"] == "cafe"

    r = engine.run(f"[out:json];way({fixture.closed_way_id});out skel;")
    assert "tags" not in r.elements[0]
    assert "nodes" in r.elements[0]

    r = engine.run(f"[out:json];way({fixture.closed_way_id});out bb;")
    assert "bounds" in r.elements[0]
    assert "geometry" not in r.elements[0]

    r = engine.run(f"[out:json];way({fixture.closed_way_id});out center;")
    assert "center" in r.elements[0]


def test_out_noids_omits_way_node_list(engine, fixture):
    r = engine.run(f"[out:json];way({fixture.closed_way_id});out geom noids;")
    assert "nodes" not in r.elements[0]
    assert "geometry" in r.elements[0]


def test_out_limit(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f"[out:json];node({b});out 3;")
    assert len(r.elements) == 3


# ---------------------------------------------------------- tag semantics


def test_not_equal_matches_absent_or_different(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(f"[out:json];node[amenity!=cafe]({b});out ids;")
    ids = {e["id"] for e in r.elements}
    assert fixture.cafe_node_id not in ids
    assert fixture.cafe_node2_id not in ids
    assert fixture.restaurant_node_id in ids  # amenity=restaurant: different value
    assert fixture.untagged_node_in_cafe_cell in ids  # no amenity tag at all


def test_case_insensitive_regex(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(f'[out:json];node[name~"coffee",i]({b});out ids;')
    assert [e["id"] for e in r.elements] == [fixture.mixed_case_name_node_id]
    r2 = engine.run(f'[out:json];node[name~"coffee"]({b});out ids;')
    assert r2.elements == []  # case-sensitive: no match


def test_promoted_and_nonpromoted_keys_behave_identically(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r_promoted = engine.run(f"[out:json];node[shop=bakery]({b});out ids;")
    r_map = engine.run(f"[out:json];node[craft=bakery]({b});out ids;")
    assert [e["id"] for e in r_promoted.elements] == [fixture.bakery_node_id]
    assert [e["id"] for e in r_map.elements] == [fixture.craft_bakery_node_id]


def test_exists_and_not_exists(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(f"[out:json];node[amenity]({b});out ids;")
    ids = {e["id"] for e in r.elements}
    assert {fixture.cafe_node_id, fixture.cafe_node2_id, fixture.restaurant_node_id} <= ids

    r = engine.run(f"[out:json];node[!amenity]({b});out ids;")
    ids = {e["id"] for e in r.elements}
    assert fixture.cafe_node_id not in ids
    assert fixture.untagged_node_in_cafe_cell in ids


# ---------------------------------------------------------------- timeout


def test_timeout_produces_remark(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f"[out:json];node({b});out;", timeout=1e-9)
    assert r.remark is not None
    assert "timed out" in r.remark
    assert r.elements == []
    # Rendered body/content-type still come back cleanly (HTTP 200 case).
    body, content_type = r.render()
    assert content_type == "application/json"
    assert "remark" in json.loads(body)


# --------------------------------------------------------------- rendering


def test_json_render_shape():
    result = Result(
        elements=[{"type": "node", "id": 1, "lat": 1.0, "lon": 2.0, "tags": {"amenity": "cafe"}}],
        settings=Settings(out_format="json"),
        timestamp_osm_base="2026-09-19T00:21:52Z",
    )
    body, content_type = result.render()
    assert content_type == "application/json"
    envelope = json.loads(body)
    assert envelope["version"] == 0.6
    assert envelope["generator"] == "osmpq 0.0.1"
    assert envelope["osm3s"]["timestamp_osm_base"] == "2026-09-19T00:21:52Z"
    assert envelope["elements"] == result.elements
    assert "remark" not in envelope


def test_xml_render_shape():
    result = Result(
        elements=[
            {"type": "node", "id": 1, "lat": 1.0, "lon": 2.0, "tags": {"amenity": "cafe"}},
            {"type": "way", "id": 2, "nodes": [1, 3], "tags": {"highway": "residential"}},
        ],
        settings=Settings(out_format="xml"),
        timestamp_osm_base="2026-09-19T00:21:52Z",
    )
    body, content_type = result.render()
    assert content_type == "application/osm3s+xml"
    assert body.startswith('<?xml version="1.0" encoding="UTF-8"?>')
    assert '<meta osm_base="2026-09-19T00:21:52Z"/>' in body
    assert '<node id="1" lat="1.0" lon="2.0"><tag k="amenity" v="cafe"/></node>' in body
    assert '<way id="2"><nd ref="1"/><nd ref="3"/><tag k="highway" v="residential"/></way>' in body
    assert body.endswith("</osm>")


def test_xml_render_remark():
    result = Result(elements=[], settings=Settings(out_format="xml"), remark="runtime error: boom")
    body, _ = result.render()
    assert "<remark>runtime error: boom</remark>" in body
