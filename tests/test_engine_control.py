"""Engine tests for control flow (docs/m3-contracts.md section 5.3):
``foreach``, ``if``/``else``, and the ``(if:)`` filter. Uses the same
synthetic fixture as tests/test_engine_basic.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine
from osmpq.errors import UnsupportedError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("control_fixture")
    return make_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def bbox_args(bbox):
    s, w, n, e = bbox
    return f"{s},{w},{n},{e}"


# ------------------------------------------------------------------- foreach


def test_foreach_out_per_element(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(
        f"[out:json];node[amenity=cafe]({b})->.a;foreach.a->.b{{ .b out; }}"
    )
    ids = sorted(e["id"] for e in r.elements)
    assert ids == sorted([fixture.cafe_node_id, fixture.cafe_node2_id])
    for e in r.elements:
        assert e["tags"]["amenity"] == "cafe"


def test_foreach_default_set_names_are_underscore(engine, fixture):
    # `foreach { ... }` with no `.a`/`->.b` defaults both to `_`.
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(f"[out:json];node[amenity=cafe]({b});foreach{{ out; }}")
    ids = sorted(e["id"] for e in r.elements)
    assert ids == sorted([fixture.cafe_node_id, fixture.cafe_node2_id])


def test_foreach_body_runs_once_per_element_not_once_total(engine, fixture):
    # Each iteration's `out count;` emits its own count element (always 1
    # node), so the number of `count` elements equals the number of inputs.
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(f"[out:json];node[amenity=cafe]({b})->.a;foreach.a->.b{{ .b out count; }}")
    counts = [e for e in r.elements if e["type"] == "count"]
    assert len(counts) == 2
    assert all(c["tags"]["nodes"] == "1" for c in counts)


def test_foreach_body_can_target_a_different_set_name(engine, fixture):
    # `.b` inside the body must be independently addressable from `.a`
    # (the snapshot of the input) and from `_`.
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(
        f"[out:json];node[amenity=cafe]({b})->.a;"
        "foreach.a->.item{ way(bn.item)->.w; .w out ids; }"
    )
    # cafe_node_id (1) is referenced by way 102 and way 110
    # (spanning_way_id); cafe_node2_id (2) is referenced by way 103.
    ids = {e["id"] for e in r.elements}
    assert ids == {102, 103, fixture.spanning_way_id}


def test_foreach_over_empty_set_runs_body_zero_times(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["001"])  # no cafes here
    r = engine.run(f"[out:json];node[amenity=cafe]({b})->.a;foreach.a->.b{{ .b out; }}")
    assert r.elements == []


# --------------------------------------------------------------------- if/else


def test_if_true_branch_on_nonzero_count(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    q = f'[out:json];way["building"]({b});if (count(ways) > 0) {{ out count; }} else {{ out ids; }}'
    r = engine.run(q)
    assert len(r.elements) == 1
    assert r.elements[0]["type"] == "count"
    assert r.elements[0]["tags"]["ways"] == "1"


def test_if_false_branch_on_zero_count(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    q = f'[out:json];way["building"="nope"]({b});if (count(ways) > 0) {{ out count; }} else {{ out count; }}'
    r = engine.run(q)
    assert len(r.elements) == 1
    assert r.elements[0]["tags"]["ways"] == "0"


def test_if_with_no_else_and_false_condition_runs_nothing(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    q = f'[out:json];way["building"="nope"]({b});if (count(ways) > 0) {{ out count; }}'
    r = engine.run(q)
    assert r.elements == []


def test_if_condition_always_evaluates_against_underscore(engine, fixture):
    # Per contract 5.3, `if`'s condition is always evaluated on `_`, even
    # if the query result was stored to a named set instead.
    b = bbox_args(fixture.leaf_bbox["000"])
    q = f'[out:json];way["building"]({b})->.a;if (count(ways) > 0) {{ out count; }} else {{ out count; }}'
    r = engine.run(q)
    # `_` is still the empty set ensure_empty_set() seeded at program
    # start, so count(ways) on it is 0 -> the else branch runs, and its
    # plain `out count;` (default set `_`) reports zero, not one.
    assert r.elements[0]["tags"]["ways"] == "0"


# ------------------------------------------------------------------- (if:)


def test_if_filter_numeric_comparison(engine, fixture):
    # Fixture ways have no "lanes" tag at all: t["lanes"] reads as "", and
    # "" > "2" is a *lexical* comparison (neither side is numeric), which
    # is false -- so no way should match either direction here.
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(if: t["lanes"] > 2)({b});out ids;')
    assert r.elements == []


def test_if_filter_numeric_comparison_true_and_false_with_a_real_tag(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    # way 101's `building` tag is "yes" (non-numeric), so a numeric
    # comparison against it must be lexical. Prove the numeric branch
    # itself works with a fixture value we control: fixture way ids serve
    # as a comparable "tag" via id() (always numeric).
    r_gt = engine.run(f"[out:json];way(if: id() > 105)({b});out ids;")
    ids_gt = {e["id"] for e in r_gt.elements}
    assert ids_gt == {w for w in fixture.all_way_ids if w > 105}

    r_le = engine.run(f"[out:json];way(if: id() <= 105)({b});out ids;")
    ids_le = {e["id"] for e in r_le.elements}
    assert ids_le == {w for w in fixture.all_way_ids if w <= 105}


def test_if_filter_lexical_comparison(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    # "highway" values compared lexically against a non-numeric string.
    r = engine.run(f'[out:json];way[highway](if: t["highway"] == "footway")({b});out ids;')
    ids = {e["id"] for e in r.elements}
    assert ids == {104, 105}


def test_if_filter_lanes_gt_2_corpus_style(engine, fixture):
    # Mirrors corpus 48 (way[highway](if:t["lanes"] > 2)): give one way a
    # numeric "lanes" tag directly via the tag filter machinery isn't
    # possible on this read-only fixture, so this exercises the exact
    # boolean/typing path against the fixture's real (tagless) data,
    # confirming it degrades to "no match" rather than raising.
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way[highway](if: t["lanes"] > 2)({b});out ids;')
    assert r.elements == []


# --------------------------------------------------------------------- errors


def test_set_scoped_function_inside_if_filter_is_unsupported(engine, fixture):
    # Like any other syntactically-valid-but-unsupported construct (e.g. an
    # unhandled filter class), this is a hard UnsupportedError raised out
    # of Engine.run(), not a Result.remark -- RuntimeQueryError is reserved
    # for well-formed runtime failures Overpass itself would report as a
    # 200-with-remark (bad date string, missing set, ...).
    b = bbox_args(fixture.total_bbox)
    with pytest.raises(UnsupportedError, match="set-scoped"):
        engine.run(f"[out:json];way(if: count(ways) > 0)({b});out ids;")


def test_bare_element_function_inside_if_statement_is_runtime_error(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    q = f'[out:json];way["building"]({b});if (t["building"] == "yes") {{ out count; }}'
    r = engine.run(q)
    assert r.remark is not None
    assert "runtime error" in r.remark
    assert "aggregator" in r.remark or "u(" in r.remark


def test_bare_element_function_wrapped_in_u_is_allowed(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    # No `->.a` here, so the query's own output lands in `_` (the default
    # output set) -- unlike the other tests above, which use `->.a` to keep
    # `_` empty on purpose. `u(...)` over the one matching way ("yes") is
    # unique, so the condition is true.
    q = f'[out:json];way["building"]({b});if (u(t["building"]) == "yes") {{ out count; }} else {{ out ids; }}'
    r = engine.run(q)
    assert r.remark is None
    assert r.elements[0]["type"] == "count"
    assert r.elements[0]["tags"]["ways"] == "1"
