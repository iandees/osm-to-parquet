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
    # relation 201. relation 206 has no direct node/way member of node 1 --
    # its only member is way 110 -- so it's only reachable via `<`'s second
    # hop ("relations that have a *found way* as a member").
    assert got == [("relation", 201), ("relation", fixture.way_only_relation_id), ("way", 102), ("way", 110)]


def test_transitive_forward_recurse(engine, fixture):
    r = engine.run(f"[out:json];relation({fixture.node_way_relation_id});>>;out ids;")
    got = sorted((e["type"], e["id"]) for e in r.elements)
    assert ("way", 105) in got
    # `>>` keeps relations of the original input set in its result even
    # though nothing "discovers" them via recursion (a real Overpass quirk;
    # see recurse.py's recurse_transitive docstring and the
    # `27_down_transitive_from_relation` corpus entry). `<<` has no such
    # exception (test_transitive_backward_recurse below).
    assert ("relation", fixture.node_way_relation_id) in got
    assert all(t in ("node", "way", "relation") for t, _ in got)


def test_transitive_backward_recurse(engine, fixture):
    r = engine.run(f"[out:json];node({fixture.cafe_node_id});<<;out ids;")
    got = {(e["type"], e["id"]) for e in r.elements}
    assert ("way", 102) in got
    assert ("way", 110) in got
    assert ("relation", 201) in got


def test_forward_recurse_from_relation_includes_member_way_nodes(engine, fixture):
    # Overpass's `>` from a relation: the way's own row, PLUS the nodes of
    # that member way (not just the way itself) -- and the relation's
    # direct node member too. The way and its nodes live in a different
    # leaf than the relation's own storage cell, and are only resolvable
    # via byid, which is exactly what was missing before this fix.
    r = engine.run(f"[out:json];relation({fixture.far_way_relation_id});>;out ids;")
    got = {(e["type"], e["id"]) for e in r.elements}
    assert got == {
        ("way", fixture.far_way_id),
        ("node", fixture.far_way_node_ids[0]),
        ("node", fixture.far_way_node_ids[1]),
        ("node", fixture.far_way_relation_label_node_id),
    }


def test_forward_recurse_from_relation_excludes_relation_members(engine, fixture):
    # Plain `>` (one hop) must NOT follow a relation-type member -- that's
    # `>>`'s job. relation 205's only member is relation 204.
    r = engine.run(f"[out:json];relation({fixture.nested_relation_id});>;out ids;")
    assert r.elements == []


def test_transitive_forward_recurse_follows_nested_relation(engine, fixture):
    # `>>` from relation 205 (whose only member is relation 204) must
    # follow the relation-type member down into 204's own members too.
    r = engine.run(f"[out:json];relation({fixture.nested_relation_id});>>;out ids;")
    got = {(e["type"], e["id"]) for e in r.elements}
    assert ("relation", fixture.far_way_relation_id) in got
    assert ("way", fixture.far_way_id) in got
    assert ("node", fixture.far_way_node_ids[0]) in got
    assert ("node", fixture.far_way_node_ids[1]) in got
    assert ("node", fixture.far_way_relation_label_node_id) in got
    # The nested relation is *discovered* via recursion (unlike
    # test_transitive_forward_recurse's relation 203, which has no relation
    # members and is only kept by the >>-retains-input-relations rule).
    assert ("relation", fixture.nested_relation_id) in got


def test_backward_recurse_finds_relation_via_found_way(engine, fixture):
    # `<` from way 110: relation 206's only member is way 110 itself, so
    # it's found directly (first hop), same as relation 201 (which
    # references node 1 directly, not way 110).
    r = engine.run(f"[out:json];way({fixture.spanning_way_id});<;out ids;")
    got = {(e["type"], e["id"]) for e in r.elements}
    assert ("relation", fixture.way_only_relation_id) in got


def test_backward_recurse_from_node_finds_way_in_ancestor_cell(engine, fixture):
    # design.md 3.1 item 1: `<` resolves a node's parent ways via the
    # bbox-scoped way lookup (a way containing a node has a bbox
    # containing that node, so it lives in the node's own leaf cell or one
    # of its ancestors), not the node_way index. `spanning_way_id` (110)
    # is stored at ancestor cell "00" because its refs span leaf "000"
    # (node 1, covered by test_backward_recurse_from_node_gets_parent_ways)
    # and leaf "002" -- exercised here from that *other* endpoint, whose
    # leaf ("002") is neither "00" nor "000".
    b = bbox_args(fixture.leaf_bbox["002"])
    r = engine.run(f"[out:json];node[amenity=cafe]({b});<;out ids;")
    got = {(e["type"], e["id"]) for e in r.elements}
    assert ("way", fixture.spanning_way_id) in got


def test_inline_recurse_filter_bn_restricted_to_way(engine, fixture):
    # design.md 3.1 item 1: `way(bn)` restricts the backward hop's result
    # to ways, resolved the same bbox-scoped way as `<` (not the
    # node_way index directly followed by a byid hydrate). Node 1 is
    # referenced by way 102 and (via the ancestor cell) way 110; it is
    # also a member of relation 201, which `way(bn)` must exclude.
    r = engine.run(f"[out:json];node({fixture.cafe_node_id});way(bn);out ids;")
    ids = sorted(e["id"] for e in r.elements)
    assert ids == [102, fixture.spanning_way_id]
    assert all(e["type"] == "way" for e in r.elements)


def test_transitive_forward_recurse_resolves_relation_member_way_geometry(engine, fixture):
    # design.md 3.1 item 3: `>>` (like `>`) resolves a relation's member
    # way from the spatial way files of the cells covering the relation's
    # own bbox, not byid -- so the way's geometry comes back directly,
    # with no second hydration pass. `spanning_relation_id` (202) is
    # stored at ancestor cell "00" (its members span leaf "000" and leaf
    # "002"); its way member (101) is stored at leaf "000", a cell the
    # relation itself is not stored at.
    r = engine.run(f"[out:json];relation({fixture.spanning_relation_id});>>;out geom;")
    way_el = next(e for e in r.elements if e["type"] == "way" and e["id"] == fixture.closed_way_id)
    assert len(way_el["geometry"]) == 5
    assert way_el["geometry"][0] == way_el["geometry"][-1]  # closed ring, refs order preserved


def test_way_bbox_exact_geometry_test(engine, fixture):
    # The diagonal way's flat bbox covers the whole leaf, but its actual
    # line only ever visits the SW->NE diagonal.
    miss = bbox_args(fixture.diagonal_bbox_miss)
    r_miss = engine.run(f"[out:json];way({fixture.diagonal_way_id})({miss});out ids;")
    assert r_miss.elements == []

    hit = bbox_args(fixture.diagonal_bbox_hit)
    r_hit = engine.run(f"[out:json];way({fixture.diagonal_way_id})({hit});out ids;")
    ids = [e["id"] for e in r_hit.elements]
    assert ids == [fixture.diagonal_way_id]


def test_relation_bbox_exact_member_test(engine, fixture):
    # Same exact-bbox test, but for a relation: its only member is the
    # diagonal way, and relation rows carry no geometry of their own, so
    # this must resolve the member way's geometry to decide.
    miss = bbox_args(fixture.diagonal_bbox_miss)
    r_miss = engine.run(f"[out:json];relation({fixture.diagonal_relation_id})({miss});out ids;")
    assert r_miss.elements == []

    hit = bbox_args(fixture.diagonal_bbox_hit)
    r_hit = engine.run(f"[out:json];relation({fixture.diagonal_relation_id})({hit});out ids;")
    ids = [e["id"] for e in r_hit.elements]
    assert ids == [fixture.diagonal_relation_id]


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


def test_forward_recurse_way_spans_byid_parts(engine, fixture):
    # `spanning_way_id` (fixture: refs include node 1 and a leaf "002" node)
    # has endpoints in different byid/node parts: the fixture splits byid
    # node into 2 parts by contiguous id, low half / high half. `>` must
    # hydrate both ends via a single query, not miss the one in the other
    # part.
    r = engine.run(f"[out:json];way({fixture.spanning_way_id});>;out ids;")
    ids = sorted(e["id"] for e in r.elements)
    assert len(ids) == 2
    mid = len(fixture.all_node_ids) // 2
    assert ids[0] == fixture.cafe_node_id  # ref 1: in the low byid part
    assert ids[0] <= mid < ids[1]  # the other ref: in the high byid part


def test_forward_recurse_way_ancestor_cell_resolves_nodes_in_two_leaves(engine, fixture):
    # `spanning_way_id` (110) is stored at ancestor cell "00" because its
    # own bbox spans leaf "000" (node 1) and leaf "002" (the other ref).
    # design.md 3.1: `>` must resolve those nodes from the *leaf* cells
    # intersecting the way's own bbox (which is on its row), not from the
    # way's own storage cell -- exercising the spatially-scoped node
    # hydration in recurse.build_forward_one_hop across two distinct
    # leaves within a single hop.
    r = engine.run(f"[out:json];way({fixture.spanning_way_id});>;out;")
    nodes = [e for e in r.elements if e["type"] == "node"]
    assert len(nodes) == 2

    def in_bbox(lat, lon, bbox):
        s, w, n, e = bbox
        return s <= lat <= n and w <= lon <= e

    leaves_hit = set()
    for el in nodes:
        for leaf, bbox in fixture.leaf_bbox.items():
            if in_bbox(el["lat"], el["lon"], bbox):
                leaves_hit.add(leaf)
    assert leaves_hit == {"000", "002"}


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


# ------------------------------------------------ id lookups: no literal IN-lists
#
# id-based lookups (`>`, `<`, `>>`, `<<`, recurse filters, `(id:...)`, and
# render.py's lazy way-geometry hydration) used to inline every id as a SQL
# literal (`WHERE id IN (1,2,3,...)`); past a few tens of thousands of ids
# that alone took tens of seconds just to parse/plan (see the module
# docstrings in osmpq.engine.idset/recurse/hilbert). These tests aren't
# timing-sensitive: they check the *shape* of what gets executed (a bounded
# predicate backed by a temp table) rather than a wall-clock budget, so they
# stay meaningful on any machine.


def test_id_predicate_uses_temp_table_not_literal_list_for_many_ids():
    import duckdb

    from osmpq.engine import idset

    con = duckdb.connect()
    try:
        ids = list(range(1, 501))  # comfortably above idset.INLINE_ID_LIMIT
        pred = idset.id_predicate(con, "id", ids)
        assert len(ids) > idset.INLINE_ID_LIMIT
        # Bounded in size regardless of how many ids there are: a handful of
        # SQL keywords plus one generated table name, not 500 literals.
        assert len(pred) < 200
        assert "IN (SELECT id FROM" in pred
        table_name = pred[pred.index("FROM ") + len("FROM ") : pred.rindex(")")]
        # The temp table really holds every id, not a truncated sample.
        n = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        assert n == len(ids)
    finally:
        con.close()


def test_id_predicate_stays_inline_for_a_handful_of_ids():
    import duckdb

    from osmpq.engine import idset

    con = duckdb.connect()
    try:
        pred = idset.id_predicate(con, "id", [1, 2, 3])
        assert pred == "id IN (1,2,3)"
    finally:
        con.close()


def test_forward_recurse_hop_uses_temp_table_for_many_ids():
    import duckdb

    from osmpq.engine import idset, recurse

    con = duckdb.connect()
    try:
        n_ways = 150  # -> 300 distinct referenced node ids, past INLINE_ID_LIMIT
        rows_sql = " UNION ALL ".join(
            f"SELECT 'way' AS type, {i} AS id, "
            f"[{1000 + 2 * i}, {1001 + 2 * i}]::BIGINT[] AS refs, "
            f"NULL::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[] AS members"
            for i in range(n_ways)
        )
        con.execute(f"CREATE TEMP TABLE set_src AS {rows_sql}")

        table = recurse.forward_new_ids_table(con, "set_src", restrict_source_types={"way"})
        assert table is not None
        assert 2 * n_ways > idset.INLINE_ID_LIMIT
        n = con.execute(f"SELECT count(*) FROM {table} WHERE type = 'node'").fetchone()[0]
        assert n == 2 * n_ways  # every ref here is distinct

        # A real materialized TEMP TABLE (what a join/semi-join can hash),
        # not a stand-in that still hides an inlined literal list somewhere.
        is_temp = con.execute(
            "SELECT temporary FROM duckdb_tables() WHERE table_name = ?", [table]
        ).fetchone()[0]
        assert is_temp is True
    finally:
        con.close()
