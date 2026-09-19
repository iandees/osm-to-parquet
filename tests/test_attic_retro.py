"""`retro("t") { ... }` (docs/m4-contracts.md section 3.2): runs its body
at a snapshot, accepts a string literal, and has block-local set scope
(confirmed against a live reference probe: a set the block assigns --
including the default set `_` -- is not visible after it)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_history_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("attic_retro_fixture")
    return make_history_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def test_retro_runs_body_at_the_given_time(engine, fixture):
    r = engine.run(
        f'[out:json];retro("{fixture.hist_node_v1_from}"){{ '
        f'node({fixture.hist_node_id}); out meta; }}'
    )
    assert len(r.elements) == 1
    assert r.elements[0]["version"] == 1


def test_retro_restores_snapshot_after_the_block(engine, fixture):
    # Immediately after the block (no `out` inside it this time), a plain
    # (non-attic) query must behave exactly as if `retro` had never run --
    # the current tables, not the retro'd date.
    r = engine.run(
        f'[out:json];retro("{fixture.hist_node_v1_from}"){{ '
        f'node({fixture.hist_node_id}); }} '
        f'node({fixture.changed_test_node_id});out ids;'
    )
    assert {el["id"] for el in r.elements} == {fixture.changed_test_node_id}


def test_retro_named_set_assigned_inside_is_not_visible_outside(engine, fixture):
    r = engine.run(
        f'[out:json];retro("{fixture.hist_node_v1_from}"){{ '
        f'node({fixture.hist_node_id})->.then; }} '
        f".then; out count;"
    )
    assert r.remark is None or r.remark.startswith("history starts at ")
    assert r.elements == [{"type": "count", "id": 0,
                            "tags": {"nodes": "0", "ways": "0", "relations": "0", "total": "0"}}]


def test_retro_default_set_is_not_visible_outside_either(engine, fixture):
    r = engine.run(
        f'[out:json];retro("{fixture.hist_node_v1_from}"){{ '
        f'node({fixture.hist_node_id}); }} '
        f"out count;"
    )
    assert int(r.elements[0]["tags"]["total"]) == 0


def test_retro_preserves_a_set_that_existed_before_the_block(engine, fixture):
    r = engine.run(
        f'[out:json];node({fixture.changed_test_node_id})->.keep;'
        f'retro("{fixture.hist_node_v1_from}"){{ node({fixture.hist_node_id})->.keep; }} '
        f".keep;out ids;"
    )
    assert {el["id"] for el in r.elements} == {fixture.changed_test_node_id}


def test_retro_out_inside_the_block_is_visible_outside(engine, fixture):
    # Only the body's own `out` statements escape the block.
    r = engine.run(
        f'[out:json];retro("{fixture.hist_node_v1_from}"){{ '
        f'node({fixture.hist_node_id}); out ids; }}'
    )
    assert {el["id"] for el in r.elements} == {fixture.hist_node_id}
