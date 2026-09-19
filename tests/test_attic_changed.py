"""`(changed:"a"[,"b"])`/`(newer:"t")` with history present (docs/m4-
contracts.md section 3.2): exact, from any history row (minor versions
and deletions count), not just the element's own last-edit timestamp.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_history_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("attic_changed_fixture")
    return make_history_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def test_changed_since_only_matches_when_history_has_a_row_after_it(engine, fixture):
    r = engine.run(
        f'[out:json];node({fixture.changed_test_node_id})'
        f'(changed:"2026-01-01T00:00:00Z");out ids;'
    )
    assert {el["id"] for el in r.elements} == {fixture.changed_test_node_id}


def test_changed_since_only_excludes_when_nothing_happened_after_it(engine, fixture):
    r = engine.run(
        f'[out:json];node({fixture.changed_test_node_id})'
        f'(changed:"2026-09-19T00:00:00Z");out ids;'
    )
    assert r.elements == []


def test_changed_range_is_exclusive_since_inclusive_until(engine, fixture):
    exactly_at = fixture.changed_test_node_from
    r_hit = engine.run(
        f'[out:json];node({fixture.changed_test_node_id})'
        f'(changed:"2026-06-01T00:00:00Z","{exactly_at}");out ids;'
    )
    assert {el["id"] for el in r_hit.elements} == {fixture.changed_test_node_id}
    r_miss = engine.run(
        f'[out:json];node({fixture.changed_test_node_id})'
        f'(changed:"{exactly_at}","{exactly_at}");out ids;'
    )
    # `since < valid_from <= until`: `since == valid_from` does not count.
    assert r_miss.elements == []


def test_newer_with_history_behaves_like_changed_with_no_upper_bound(engine, fixture):
    r = engine.run(
        f'[out:json];node({fixture.changed_test_node_id})'
        f'(newer:"2026-01-01T00:00:00Z");out ids;'
    )
    assert {el["id"] for el in r.elements} == {fixture.changed_test_node_id}


def test_newer_with_history_excludes_after_the_last_change(engine, fixture):
    r = engine.run(
        f'[out:json];node({fixture.changed_test_node_id})'
        f'(newer:"2026-09-19T00:00:00Z");out ids;'
    )
    assert r.elements == []


def test_changed_bbox_scoped_scan_finds_it_too(engine, fixture):
    s, w, n, e = fixture.leaf_bbox["000"]
    r = engine.run(
        f'[out:json];node({s},{w},{n},{e})(changed:"2026-01-01T00:00:00Z");out ids;'
    )
    assert fixture.changed_test_node_id in {el["id"] for el in r.elements}
