"""Relation-area ring assembly parallelism: `_relation_area_rows_from_tasks`
fans the per-relation shapely work in `osmpq.build.areas._relation_area_rows`
out across a `ProcessPoolExecutor` for large inputs, and stays exactly on
the original in-process serial loop for small ones (`AREA_PARALLEL_MIN_
CANDIDATES`). This suite covers both branches directly plus exception
propagation, since the small fixtures used elsewhere never have enough
candidate relations to reach the parallel branch on their own.

Also covers `_subtract_holes` (the batched-union-then-single-difference
replacement for the old one-`.difference()`-call-per-hole loop -- see its
own docstring in `areas.py` for why: real relations with thousands of
intersecting holes, e.g. a Great Lake's shoreline, made each successive
`.difference()` call operate on an increasingly complex polygon, which
dominated real build time -- one relation alone took 27+ minutes
single-threaded before this fix, 6.9s after, found benchmarking a
~10x-Minnesota-scale region).

Also includes a manual sanity check of `derive_relation_areas_for_pivots`
(compact's touched-pivot re-derive integration, docs/m3-contracts.md 9.2):
`tests/test_areas_compact.py` normally covers this via a real `osmpq
compact` run, but can't be collected in this sandbox -- it transitively
imports `osmpq.build.compact` -> `osmpq.update.updater` -> `osmpq.update.osc`,
which imports the `osmium` package that isn't installed here (a
pre-existing, unrelated environment gap; see docs/development.md)."""
from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest
from shapely.geometry import Polygon
from shapely.validation import make_valid

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402

from osmpq.build import areas as areas_mod  # noqa: E402
from osmpq.engine import catalog  # noqa: E402


@pytest.fixture(scope="module")
def fixture_v4(tmp_path_factory):
    root = tmp_path_factory.mktemp("areas_parallel_fixture_v4")
    return make_fixture.build(str(root), manifest_version=4)


def _row_key(r: dict) -> tuple:
    return (r["id"], r["xmin_e7"], r["ymin_e7"], r["xmax_e7"], r["ymax_e7"], r["wkb"])


# --------------------------------------------------------------------------
# serial branch (small input, the default for every existing fixture/test)
# --------------------------------------------------------------------------


def test_small_input_stays_serial(fixture_v4, tmp_path, monkeypatch):
    """Below `AREA_PARALLEL_MIN_CANDIDATES`, no `ProcessPoolExecutor` is
    ever constructed, even when `threads` asks for several workers --
    patch it to blow up if instantiated, to prove the serial branch (not
    just "the same output") is actually what ran."""
    def _boom(*a, **k):
        raise AssertionError("ProcessPoolExecutor should not be constructed for a small input")
    monkeypatch.setattr(areas_mod, "ProcessPoolExecutor", _boom)

    manifest = catalog.load_manifest(fixture_v4.root)
    con = areas_mod._connect(None, None, tmp_path)
    rows, _files = areas_mod._relation_area_rows(con, manifest, None, threads=8)
    con.close()
    assert rows  # the fixture has qualifying relation areas
    assert any(r["pivot_id"] == fixture_v4.area_multipolygon_relation_id for r in rows)


# --------------------------------------------------------------------------
# parallel branch (forced via AREA_PARALLEL_MIN_CANDIDATES, since the
# fixtures are far too small to reach it on their own)
# --------------------------------------------------------------------------


def test_parallel_path_matches_serial(fixture_v4, tmp_path, monkeypatch):
    """With the threshold forced down to 1, `threads=2` actually spins up
    a real (spawn-based) process pool for this fixture's handful of
    relations; its output must match the serial (`threads=1`) run
    row-for-row (id, bbox, and wkb bytes)."""
    monkeypatch.setattr(areas_mod, "AREA_PARALLEL_MIN_CANDIDATES", 1)

    manifest = catalog.load_manifest(fixture_v4.root)
    con = areas_mod._connect(None, None, tmp_path)
    serial_rows, _ = areas_mod._relation_area_rows(con, manifest, None, threads=1)
    parallel_rows, _ = areas_mod._relation_area_rows(con, manifest, None, threads=2)
    con.close()

    assert len(parallel_rows) >= 1
    assert sorted(map(_row_key, serial_rows)) == sorted(map(_row_key, parallel_rows))


# --------------------------------------------------------------------------
# exception propagation (docs the task: `_assemble_relation_geometry`
# throwing for a pathological relation must still kill the whole build,
# on both branches -- no new blanket exception-swallowing)
# --------------------------------------------------------------------------


def test_exception_propagates_serial(monkeypatch):
    def _boom(outer_ids, inner_ids, way_wkt):
        raise RuntimeError("boom from a pathological relation")
    monkeypatch.setattr(areas_mod, "_assemble_relation_geometry", _boom)

    tasks = [(1, {}, 1, 1, None, 1, "u", 0, 0, 0, 0, [10], [])]
    with pytest.raises(RuntimeError, match="boom"):
        areas_mod._relation_area_rows_from_tasks(tasks, {}, None)


def test_exception_propagates_parallel(monkeypatch):
    """Real (unpatched) code running for real in a spawned worker process:
    a monkeypatched function on the parent's in-memory module wouldn't be
    visible there anyway, since macOS's `spawn` start method re-imports
    this module fresh in each worker. `outer_way_ids` here is an int, not
    a list, so `_rings_from_way_ids`'s `for wid in way_ids` raises
    TypeError immediately -- a genuinely malformed task, exercised through
    the real ProcessPoolExecutor.map() path to confirm it doesn't get
    swallowed."""
    monkeypatch.setattr(areas_mod, "AREA_PARALLEL_MIN_CANDIDATES", 1)
    tasks = [(1, {}, 1, 1, None, 1, "u", 0, 0, 0, 0, 12345, [])]
    with pytest.raises(TypeError):
        areas_mod._relation_area_rows_from_tasks(tasks, {}, 2)


# --------------------------------------------------------------------------
# _subtract_holes: batched union+difference must match the old
# one-hole-at-a-time loop exactly, across many randomized hole
# configurations (overlapping holes, non-intersecting holes, zero holes)
# --------------------------------------------------------------------------


def _old_subtract_holes_one_at_a_time(op, matching_inner: list) -> object:
    """The pre-fix algorithm, reimplemented here only as a reference
    oracle for the equivalence test below -- not exercised by production
    code any more."""
    poly = op
    for ip in matching_inner:
        try:
            poly = poly.difference(ip)
        except Exception:
            continue
    return poly


def test_subtract_holes_matches_old_one_at_a_time_algorithm():
    random.seed(42)
    mismatches = []
    for trial in range(200):
        outer = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        holes = []
        for _ in range(random.randint(0, 8)):
            x, y = random.uniform(0, 9), random.uniform(0, 9)
            s = random.uniform(0.2, 2.5)
            holes.append(Polygon([(x, y), (x + s, y), (x + s, y + s), (x, y + s)]))
        if random.random() < 0.3:
            # A hole nowhere near `outer` -- `_assemble_relation_geometry`
            # only ever calls `_subtract_holes` with the `intersects`-
            # filtered subset, but a non-intersecting entry here should
            # still behave identically between the two algorithms if one
            # ever were passed (defense in depth for the equivalence claim).
            holes.append(Polygon([(20, 20), (21, 20), (21, 21), (20, 21)]))

        matching = [h for h in holes if outer.intersects(h)]
        old_result = make_valid(_old_subtract_holes_one_at_a_time(outer, matching))
        new_result = make_valid(areas_mod._subtract_holes(outer, matching))

        if abs(old_result.area - new_result.area) > 1e-9 or not old_result.equals(new_result):
            mismatches.append((trial, len(matching), old_result.area, new_result.area))

    assert not mismatches, f"{len(mismatches)}/200 trials disagreed: {mismatches[:5]}"


def test_subtract_holes_no_holes_returns_outer_unchanged():
    outer = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    assert areas_mod._subtract_holes(outer, []) is outer


def test_subtract_holes_falls_back_when_batched_path_raises(monkeypatch):
    """If the batched union/difference itself raises for any reason, the
    result must still come from the same per-hole, exception-tolerant loop
    the old algorithm used (so one malformed hole degrades the same way it
    always did, instead of losing every hole)."""
    def _boom(*a, **k):
        raise RuntimeError("simulated GEOS failure")
    # `_subtract_holes` does `from shapely.ops import unary_union` as a
    # local import inside the function body on every call, so patching
    # the `shapely.ops` module's own attribute (looked up fresh each call)
    # is what actually takes effect here.
    import shapely.ops as shapely_ops_mod
    monkeypatch.setattr(shapely_ops_mod, "unary_union", _boom)

    outer = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    h1 = Polygon([(1, 1), (2, 1), (2, 2), (1, 2)])
    h2 = Polygon([(5, 5), (6, 5), (6, 6), (5, 6)])
    result = areas_mod._subtract_holes(outer, [h1, h2])
    # The fallback loop's plain `.difference()` calls aren't patched, so
    # this must still have actually subtracted both holes.
    expected = _old_subtract_holes_one_at_a_time(outer, [h1, h2])
    assert make_valid(result).equals(make_valid(expected))


# --------------------------------------------------------------------------
# manual coverage of the compact touched-pivot integration path (9.2) --
# tests/test_areas_compact.py covers this end-to-end via `osmpq compact`
# but can't be collected here (see module docstring)
# --------------------------------------------------------------------------


def test_derive_relation_areas_for_pivots_manual_sanity(fixture_v4, tmp_path):
    manifest = catalog.load_manifest(fixture_v4.root)
    con = areas_mod._connect(None, None, tmp_path)

    touched_id = fixture_v4.area_multipolygon_relation_id
    placed_table, _files = areas_mod.derive_relation_areas_for_pivots(
        con, manifest, list(make_fixture.PROMOTED_KEYS), [touched_id],
    )
    rows = con.execute(f"SELECT pivot_id, pivot_type FROM {placed_table}").fetchall()
    assert rows == [(touched_id, "relation")]

    # An id that doesn't qualify (or doesn't exist) yields no row at all --
    # exactly what a relation that lost its ring/tags/membership on the
    # next delta would look like to a real `osmpq compact` re-derive.
    placed_table2, _files2 = areas_mod.derive_relation_areas_for_pivots(
        con, manifest, list(make_fixture.PROMOTED_KEYS), [999_999_999],
    )
    rows2 = con.execute(f"SELECT pivot_id FROM {placed_table2}").fetchall()
    assert rows2 == []
    con.close()
