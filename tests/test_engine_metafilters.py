"""Engine tests for the meta filters (docs/m3-contracts.md section 5.1):
``(newer:)``, ``(changed:)``, ``(user:)``, ``(uid:)``. Uses the same
synthetic fixture as tests/test_engine_basic.py.

The fixture's meta columns (tests/fixtures/make_fixture.py: `meta_cols_sql`)
assign every element a deterministic version/changeset/timestamp/uid/user
from its id: ``timestamp = 2026-09-01T12:00:00Z + i minutes``, ``uid = 500 +
(i % 3)``, ``user = "tester<i % 3>"`` where ``i`` is the element's id.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("metafilters_fixture")
    return make_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def bbox_args(bbox):
    s, w, n, e = bbox
    return f"{s},{w},{n},{e}"


# ------------------------------------------------------------------- newer


def test_newer_matches_everything_before_a_past_timestamp(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(newer:"2000-01-01T00:00:00Z")({b});out ids;')
    assert sorted(e["id"] for e in r.elements) == sorted(fixture.all_way_ids)


def test_newer_matches_nothing_after_the_last_timestamp(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(newer:"2099-01-01T00:00:00Z")({b});out ids;')
    assert r.elements == []


def test_newer_partitions_by_id_derived_timestamp(engine, fixture):
    # way <id>'s timestamp is 2026-09-01T12:00:00Z + <id> minutes: id 101 ->
    # 13:41:00, id 110 -> 13:50:00. A cutoff strictly between the two must
    # exclude the earlier one and keep the later one.
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(newer:"2026-09-01T13:45:30Z")({b});out ids;')
    ids = {e["id"] for e in r.elements}
    assert fixture.closed_way_id not in ids  # id 101 -> 13:41:00, before cutoff
    assert fixture.spanning_way_id in ids  # id 110 -> 13:50:00, at/after cutoff


def test_newer_invalid_timestamp_is_runtime_error(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(newer:"not-a-date")({b});out ids;')
    assert r.remark is not None
    assert "runtime error" in r.remark


# ----------------------------------------------------------------- changed


def test_changed_since_only_is_at_or_after(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(changed:"2026-09-01T13:50:00Z")({b});out ids;')
    ids = {e["id"] for e in r.elements}
    assert fixture.spanning_way_id in ids  # exactly at the cutoff
    assert fixture.closed_way_id not in ids


def test_changed_since_and_until_is_inclusive_range(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(
        f'[out:json];way(changed:"2026-09-01T13:42:00Z","2026-09-01T13:44:00Z")({b});out ids;'
    )
    ids = sorted(e["id"] for e in r.elements)
    assert ids == [102, 103, 104]  # timestamps 13:42, 13:43, 13:44


# --------------------------------------------------------------------- user


def test_user_single_name(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(user:"tester0")({b});out ids;')
    ids = {e["id"] for e in r.elements}
    assert ids == {w for w in fixture.all_way_ids if w % 3 == 0}


def test_user_multiple_names(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(user:"tester0","tester1")({b});out ids;')
    ids = {e["id"] for e in r.elements}
    assert ids == {w for w in fixture.all_way_ids if w % 3 in (0, 1)}


def test_user_no_match(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(user:"nobody")({b});out ids;')
    assert r.elements == []


# ---------------------------------------------------------------------- uid


def test_uid_single(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(uid:500)({b});out ids;')
    ids = {e["id"] for e in r.elements}
    assert ids == {w for w in fixture.all_way_ids if w % 3 == 0}


def test_uid_multiple(engine, fixture):
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way(uid:500,501)({b});out ids;')
    ids = {e["id"] for e in r.elements}
    assert ids == {w for w in fixture.all_way_ids if w % 3 in (0, 1)}


# ---------------------------------------------------------- combined with tags


def test_meta_filter_combines_with_tag_filter(engine, fixture):
    # Fixture ways tagged "highway": 102 (residential), 104 and 105 (footway).
    b = bbox_args(fixture.total_bbox)
    r = engine.run(f'[out:json];way["highway"](user:"tester0")({b});out ids;')
    ids = {e["id"] for e in r.elements}
    expected = {w for w in fixture.all_way_ids if w % 3 == 0} & {102, 104, 105}
    assert ids == expected
    assert expected  # sanity: the intersection isn't accidentally empty


def test_meta_filter_implied_bbox_is_none_still_needs_explicit_bbox_or_id(engine, fixture):
    # No implied bbox: without one, the query scans every cell (a warning,
    # not an error -- same as any other bbox-less query, contract 5.1).
    r = engine.run('[out:json];way(user:"tester0");out count;')
    assert r.elements[0]["type"] == "count"
    assert int(r.elements[0]["tags"]["ways"]) == sum(1 for w in fixture.all_way_ids if w % 3 == 0)
