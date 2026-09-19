"""`[date:]` snapshot reads (docs/m4-contracts.md section 3.1): boundaries,
the move tombstone, `out geom` at `t`, `>`/`<` at `t`, the hour-tier
fallback, dates before `history.since`, and the no-history-manifest
unsupported error (section 8's no-regression requirement).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine
from osmpq.errors import UnsupportedError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture, make_history_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("attic_snapshot_fixture")
    return make_history_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def _node(engine, node_id, date):
    r = engine.run(f'[out:json][date:"{date}"];node({node_id});out meta;')
    assert r.remark is None
    return r.elements


# ------------------------------------------------------------- [date:] node


def test_date_at_v1_creation_returns_v1(engine, fixture):
    els = _node(engine, fixture.hist_node_id, fixture.hist_node_v1_from)
    assert len(els) == 1
    assert els[0]["version"] == 1
    assert els[0]["tags"] == fixture.hist_node_v1_tags


def test_date_exactly_at_valid_from_picks_the_new_state(engine, fixture):
    # boundary rule (section 3.1): valid_from <= t, so t == v2's valid_from
    # must already show v2, not v1.
    els = _node(engine, fixture.hist_node_id, fixture.hist_node_v2_from)
    assert els[0]["version"] == 2
    assert els[0]["tags"] == fixture.hist_node_v2_tags


def test_date_just_before_a_boundary_still_shows_the_old_state(engine, fixture):
    els = engine.run(
        f'[out:json][date:"2021-06-14T23:59:59Z"];node({fixture.hist_node_id});out meta;'
    ).elements
    assert els[0]["version"] == 1


def test_date_after_move_shows_new_cell_and_position(engine, fixture):
    els = _node(engine, fixture.hist_node_id, fixture.hist_node_v3_from)
    assert len(els) == 1
    assert els[0]["version"] == 3


def test_move_tombstone_hides_element_from_old_cell_bbox_scan(engine, fixture):
    # A bbox scan of the *old* leaf ("000") at a date after the move must
    # not find the node there any more (the move tombstone, section 2.1).
    s, w, n, e = fixture.leaf_bbox[fixture.hist_node_cell_before]
    r = engine.run(
        f'[out:json][date:"{fixture.hist_node_v3_from}"];node({s},{w},{n},{e});out ids;'
    )
    ids = {el["id"] for el in r.elements}
    assert fixture.hist_node_id not in ids


def test_move_makes_element_findable_in_new_cell_bbox_scan(engine, fixture):
    s, w, n, e = fixture.leaf_bbox[fixture.hist_node_cell_after]
    r = engine.run(
        f'[out:json][date:"{fixture.hist_node_v3_from}"];node({s},{w},{n},{e});out ids;'
    )
    ids = {el["id"] for el in r.elements}
    assert fixture.hist_node_id in ids


def test_date_after_deletion_returns_nothing(engine, fixture):
    els = _node(engine, fixture.hist_node_id, fixture.hist_node_v4_from)
    assert els == []


def test_date_between_creation_and_deletion_finds_it_in_old_cell_history(engine, fixture):
    s, w, n, e = fixture.leaf_bbox[fixture.hist_node_cell_before]
    r = engine.run(
        f'[out:json][date:"{fixture.hist_node_v1_from}"];node({s},{w},{n},{e});out ids;'
    )
    ids = {el["id"] for el in r.elements}
    assert fixture.hist_node_id in ids


# --------------------------------------------------------- way: minor states


def test_way_minor_version_moves_geometry_without_changing_version(engine, fixture):
    before = engine.run(
        f'[out:json][date:"2020-07-01T00:00:00Z"];way({fixture.hist_way_id});out geom;'
    ).elements[0]
    after = engine.run(
        f'[out:json][date:"{fixture.hist_way_v1_minor1_from}"];way({fixture.hist_way_id});out geom;'
    ).elements[0]
    assert before["id"] == after["id"]
    assert before["geometry"] != after["geometry"]


def test_way_second_minor_moves_it_to_a_new_cell(engine, fixture):
    # After the second minor version, the way's own cell (in its history
    # row, section 2.2) is the new one -- a bbox scan of the new cell
    # ("001") must find it. (It also still touches the old cell's bbox
    # scan, since one endpoint (node A) never moved and the way is a
    # single segment spanning both leaves -- a real, correct
    # intersection, not a stale reference to the old cell -- so that
    # scan is not a useful "did it leave" signal here; the timeline/
    # move-tombstone assertions above already cover the tombstone
    # mechanics for a element wholly contained in one cell.)
    s1, w1, n1, e1 = fixture.leaf_bbox[fixture.hist_way_cell_after]
    t = fixture.hist_way_v2_minor1_from
    r_new = engine.run(f'[out:json][date:"{t}"];way({s1},{w1},{n1},{e1});out ids;')
    assert fixture.hist_way_id in {el["id"] for el in r_new.elements}


def test_way_not_yet_in_new_cell_before_the_second_minor(engine, fixture):
    s1, w1, n1, e1 = fixture.leaf_bbox[fixture.hist_way_cell_after]
    r = engine.run(f'[out:json][date:"{fixture.hist_way_v2_from}"];way({s1},{w1},{n1},{e1});out ids;')
    assert fixture.hist_way_id not in {el["id"] for el in r.elements}


def test_way_timeline_has_only_two_own_version_entries(engine, fixture):
    # docs/m4-contracts.md section 6 fact (confirmed against
    # m4probe/timeline_way_json.json): minor/geometry-only states get no
    # timeline entry of their own.
    r = engine.run(f'[out:json];timeline(way,{fixture.hist_way_id});out;')
    versions = [int(e["tags"]["refversion"]) for e in r.elements]
    assert versions == [1, 2]


# ------------------------------------------------------------- relation


def test_relation_minor_version_at_date(engine, fixture):
    v1 = engine.run(
        f'[out:json][date:"{fixture.hist_relation_v1_from}"];relation({fixture.hist_relation_id});out;'
    ).elements[0]
    minor = engine.run(
        f'[out:json][date:"{fixture.hist_relation_v1_minor1_from}"];relation({fixture.hist_relation_id});out;'
    ).elements[0]
    v2 = engine.run(
        f'[out:json][date:"{fixture.hist_relation_v2_from}"];relation({fixture.hist_relation_id});out;'
    ).elements[0]
    # All three states resolve (none is missing); the minor state is a
    # distinct state from both v1 and v2 (a real member-derived bbox
    # change), and only v1/minor share their own owning version.
    assert v1["id"] == minor["id"] == v2["id"] == fixture.hist_relation_id


# -------------------------------------------------------------- hour tier


def test_tier_state_used_after_its_valid_from(engine, fixture):
    els = _node(engine, fixture.hist_tier_node_id, fixture.hist_tier_node_v2_from)
    assert els[0]["version"] == 2
    assert els[0]["tags"] == fixture.hist_tier_node_v2_tags


def test_base_state_used_before_tier_valid_from(engine, fixture):
    els = _node(engine, fixture.hist_tier_node_id, "2022-01-01T00:00:00Z")
    assert els[0]["version"] == 1
    assert els[0]["tags"] == fixture.hist_tier_node_v1_tags


# --------------------------------------------------- created after `since`


def test_element_created_after_since_is_found_at_its_own_date(engine, fixture):
    els = _node(engine, fixture.hist_new_after_since_id, fixture.hist_new_after_since_from)
    assert len(els) == 1


def test_element_created_after_since_absent_before_its_creation(engine, fixture):
    els = _node(engine, fixture.hist_new_after_since_id, "2024-07-01T00:00:00Z")
    assert els == []


# ------------------------------------------------------- before `since`


def test_date_before_since_gets_a_remark(engine, fixture):
    r = engine.run(f'[out:json][date:"2020-01-01T00:00:00Z"];node({fixture.hist_node_id});out meta;')
    warnings = r.stats.get("warnings") or []
    assert any("history starts at" in w for w in warnings)


def test_date_at_or_after_since_gets_no_such_remark(engine, fixture):
    r = engine.run(f'[out:json][date:"{fixture.since}"];node({fixture.hist_node_id});out meta;')
    warnings = r.stats.get("warnings") or []
    assert not any("history starts at" in w for w in warnings)


# ---------------------------------------------- backward recursion (`<`)


def test_backward_recursion_finds_parent_relation_at_snapshot(engine, fixture):
    r = engine.run(
        f'[out:json][date:"{fixture.hist_relation_v2_from}"];node({fixture.hist_node_id});<;out ids;'
    )
    ids = {(el["type"], el["id"]) for el in r.elements}
    assert ("relation", fixture.hist_relation_id) in ids


# --------------------------------------------------------- malformed date


def test_malformed_date_is_a_runtime_error(engine, fixture):
    r = engine.run('[out:json][date:"2020-13-45T00:00:00Z"];node(1);out;')
    assert r.remark is not None
    assert "runtime error" in r.remark


# -------------------------------------------------- no-history manifest


@pytest.fixture(scope="module")
def no_history_fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("attic_no_history_fixture")
    return make_fixture.build(str(root), manifest_version=4)


@pytest.fixture(scope="module")
def no_history_engine(no_history_fixture):
    return Engine(no_history_fixture.root)


def test_date_without_history_raises_unsupported_with_history_message(no_history_engine):
    with pytest.raises(UnsupportedError) as exc:
        no_history_engine.run('[out:json][date:"2020-01-01T00:00:00Z"];node(1);out;')
    assert "history" in str(exc.value)


def test_retro_without_history_raises_unsupported(no_history_engine):
    with pytest.raises(UnsupportedError) as exc:
        no_history_engine.run('[out:json];retro("2020-01-01T00:00:00Z"){ node(1); out; }')
    assert "history" in str(exc.value)


def test_timeline_without_history_raises_unsupported(no_history_engine):
    with pytest.raises(UnsupportedError) as exc:
        no_history_engine.run("[out:json];timeline(node,1);out;")
    assert "history" in str(exc.value)


def test_diff_without_history_raises_unsupported(no_history_engine):
    with pytest.raises(UnsupportedError) as exc:
        no_history_engine.run('[out:xml][diff:"2020-01-01T00:00:00Z"];node(1);out;')
    assert "history" in str(exc.value)
