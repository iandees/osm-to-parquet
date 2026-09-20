"""End-to-end test of ``osmpq.history.build``/``osmpq.history.writer``
against a tiny dataset, docs/m4-contracts.md section 4.

The story (chosen so each element lands in a cell precisely, verified with
``osmpq.layout.cells`` directly before writing this test -- see the
workstream report): extent (40, -100, 50, -80), leaf cells ``00``/``01``
selected with ``max_nodes_per_cell=1, max_depth=4``.

- node 101: base v1 at (45.0, -93.0) [leaf ``00``]; diff1 moves it a little
  to (45.0005, -93.0005) [still ``00``, no move tombstone -- a way whose
  node moves]; diff2 moves it far to (49.0, -82.0) [leaf ``01``, a node
  move tombstone] which also drags way 301's bbox out to cell ``root``
  (a way move tombstone).
- node 102: base v1 only, a way 301 ref, never touched again (the
  overwhelming "single version" case the minor-version join must not cost
  a join per ref).
- node 103: base v1, deleted in diff3 (a deletion tombstone).
- node 104: base v1 **outside** the extent -- dropped entirely.
- way 301: refs [101, 102], own version 1 throughout; gets two minor
  versions (diff1's move, diff2's move) with no own-version bump.
- relation 401: member way 301 (role "outer"); gets minor versions
  whenever way 301 does (member way *states*, section 4.1.3).
- the *current* dataset root (built from a plain PBF of the story's final
  state -- no history) is what `osmpq history build` attaches to.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import pytest

from osmpq.build.builder import BuildOptions, build
from osmpq.build.validate import validate
from osmpq.history.build import HistoryBuildOptions, history_build
from osmpq.layout import manifest as manifest_mod

osmium = pytest.importorskip("osmium")

EXTENT = (40.0, -100.0, 50.0, -80.0)
MAX_NODES_PER_CELL = 1
MAX_DEPTH = 4


def _node(id, lat, lon, version, ts, tags=None, visible=True):
    return osmium.osm.mutable.Node(
        id=id, location=(lon, lat), version=version, timestamp=ts,
        tags=tags or {}, visible=visible, changeset=1, uid=1, user="tester",
    )


def _way(id, refs, version, ts, tags=None, visible=True):
    return osmium.osm.mutable.Way(
        id=id, nodes=refs, version=version, timestamp=ts,
        tags=tags or {}, visible=visible, changeset=1, uid=1, user="tester",
    )


def _relation(id, members, version, ts, tags=None, visible=True):
    return osmium.osm.mutable.Relation(
        id=id, members=members, version=version, timestamp=ts,
        tags=tags or {}, visible=visible, changeset=1, uid=1, user="tester",
    )


N101_BASE = (45.0, -93.0)
N101_SMALL_MOVE = (45.0005, -93.0005)
N101_FAR_MOVE = (49.0, -82.0)
N102 = (45.001, -93.001)
N103 = (45.5, -93.5)
N104_OUTSIDE = (0.0, 0.0)


@pytest.fixture(scope="module")
def fixture_dir(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("history-build-fixtures")


@pytest.fixture(scope="module")
def base_pbf(fixture_dir: Path) -> Path:
    p = fixture_dir / "base.osm.pbf"
    w = osmium.SimpleWriter(str(p))
    w.add_node(_node(101, *N101_BASE, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(102, *N102, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(103, *N103, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(104, *N104_OUTSIDE, 1, dt.datetime(2020, 1, 1)))
    w.add_way(_way(301, [101, 102], 1, dt.datetime(2020, 1, 1), tags={"highway": "residential"}))
    w.add_relation(_relation(401, [("w", 301, "outer")], 1, dt.datetime(2020, 1, 1), tags={"type": "multipolygon"}))
    w.close()
    return p


@pytest.fixture(scope="module")
def osc_dir(fixture_dir: Path) -> Path:
    d = fixture_dir / "osc"
    d.mkdir()
    w1 = osmium.SimpleWriter(str(d / "0000001.osc"))
    w1.add_node(_node(101, *N101_SMALL_MOVE, 2, dt.datetime(2020, 2, 1)))
    w1.close()
    w2 = osmium.SimpleWriter(str(d / "0000002.osc"))
    w2.add_node(_node(101, *N101_FAR_MOVE, 3, dt.datetime(2020, 3, 1)))
    w2.close()
    w3 = osmium.SimpleWriter(str(d / "0000003.osc"))
    w3.add_node(_node(103, *N103, 2, dt.datetime(2020, 4, 1), visible=False))
    w3.close()
    return d


@pytest.fixture(scope="module")
def final_pbf(fixture_dir: Path) -> Path:
    """The current-state snapshot after every diff: node 101 at its final
    position, node 102 unchanged, node 103 gone (deleted), node 104 never
    admitted (outside the extent)."""
    p = fixture_dir / "final.osm.pbf"
    w = osmium.SimpleWriter(str(p))
    w.add_node(_node(101, *N101_FAR_MOVE, 3, dt.datetime(2020, 3, 1)))
    w.add_node(_node(102, *N102, 1, dt.datetime(2020, 1, 1)))
    w.add_way(_way(301, [101, 102], 1, dt.datetime(2020, 1, 1), tags={"highway": "residential"}))
    w.add_relation(_relation(401, [("w", 301, "outer")], 1, dt.datetime(2020, 1, 1), tags={"type": "multipolygon"}))
    w.close()
    return p


@pytest.fixture(scope="module")
def current_root(tmp_path_factory, final_pbf) -> Path:
    root = tmp_path_factory.mktemp("history-build-root")
    tmp = tmp_path_factory.mktemp("history-build-current-tmp")
    build(BuildOptions(
        pbf_path=str(final_pbf), root=str(root), bbox=EXTENT,
        max_nodes_per_cell=MAX_NODES_PER_CELL, max_depth=MAX_DEPTH,
        run_areas=True, tmpdir=str(tmp),
    ))
    return root


@pytest.fixture(scope="module")
def history_root(tmp_path_factory, current_root, base_pbf, osc_dir):
    """``cp -al`` the current-state root (as the contract's own workflow
    does) so the base-build fixture above stays untouched, then attach
    history to the copy."""
    import shutil

    root = tmp_path_factory.mktemp("history-build-hroot") / "root"
    shutil.copytree(current_root, root, copy_function=__import__("os").link)
    tmp = tmp_path_factory.mktemp("history-build-history-tmp")
    summary = history_build(HistoryBuildOptions(
        root=str(root), pbf_path=str(base_pbf), osc_paths=[str(osc_dir)], tmpdir=str(tmp),
    ))
    return root, summary


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    return con


def test_current_root_is_v4_without_history(current_root):
    man = manifest_mod.load_latest(str(current_root))
    assert man.manifest_version == 4
    assert man.history is None


def test_manifest_becomes_v5_with_history(history_root):
    root, summary = history_root
    man = manifest_mod.load_latest(str(root))
    assert man.manifest_version == 5
    assert man.history is not None
    assert man.history["generation"] == man.generation
    assert man.history["minor_versions"] is True
    assert man.history["since"] == "2020-01-01T00:00:00Z"


def test_history_stats_row_counts(history_root):
    _root, summary = history_root
    stats = summary["stats"]
    assert stats["rows"]["node"] == 6  # 101(v1,v2,v3) + 102(v1) + 103(v1,v2-delete); 104 excluded (outside extent)
    assert stats["minor_rows"]["way"] == 2  # diff1 + diff2 minor versions
    assert stats["minor_rows"]["node"] == 0  # nodes never have minor versions


def test_byid_rows_node101_all_three_versions(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    paths = [str(root / p["path"]) for p in man.history["byid"]["node"]]
    rows = con.execute(
        f"SELECT version, visible, lat_e7, lon_e7, cell FROM read_parquet({paths!r}) "
        "WHERE id = 101 ORDER BY version"
    ).fetchall()
    assert [r[0] for r in rows] == [1, 2, 3]
    assert all(r[1] for r in rows)  # all visible (no delete for 101)
    assert rows[0][4] == "00" and rows[1][4] == "00" and rows[2][4] == "01"


def test_byid_deletion_row_node103(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    paths = [str(root / p["path"]) for p in man.history["byid"]["node"]]
    rows = con.execute(
        f"SELECT version, visible, lat_e7, tags FROM read_parquet({paths!r}) WHERE id = 103 ORDER BY version"
    ).fetchall()
    assert [r[0] for r in rows] == [1, 2]
    assert rows[0][1] is True
    assert rows[1][1] is False
    assert rows[1][2] is None and rows[1][3] is None


def test_node104_outside_extent_absent(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    paths = [str(root / p["path"]) for p in man.history["byid"]["node"]]
    rows = con.execute(f"SELECT count(*) FROM read_parquet({paths!r}) WHERE id = 104").fetchone()[0]
    assert rows == 0


def test_node_byid_has_no_move_tombstones(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    paths = [str(root / p["path"]) for p in man.history["byid"]["node"]]
    # every byid row for 101 has non-null meta (a move tombstone would have
    # everything but id/cell/version/minor/valid_from/visible NULL)
    rows = con.execute(f"SELECT changeset FROM read_parquet({paths!r}) WHERE id = 101").fetchall()
    assert all(r[0] is not None for r in rows)


def test_spatial_move_tombstone_for_node101_in_old_cell(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    cell00_parts = man.history["spatial"]["node"].get("00", [])
    paths = [str(root / p["path"]) for p in cell00_parts]
    rows = con.execute(
        f"SELECT version, visible, changeset FROM read_parquet({paths!r}) WHERE id = 101 ORDER BY version, visible"
    ).fetchall()
    # v1 (visible), v2 (visible, still cell 00), and a v3 move-tombstone
    # (invisible, meta NULL) once node 101 leaves cell 00 for good.
    assert (1, True) in [(r[0], r[1]) for r in rows]
    assert (2, True) in [(r[0], r[1]) for r in rows]
    tomb = [r for r in rows if r[0] == 3]
    assert len(tomb) == 1
    assert tomb[0][1] is False and tomb[0][2] is None
    # and cell 01 (the new home) holds the real, visible v3 state
    cell01_parts = man.history["spatial"]["node"].get("01", [])
    paths01 = [str(root / p["path"]) for p in cell01_parts]
    real = con.execute(f"SELECT version, visible, lat_e7 FROM read_parquet({paths01!r}) WHERE id = 101").fetchall()
    assert real == [(3, True, 490000000)]


def test_way_minor_versions_and_move_tombstone(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    paths = [str(root / p["path"]) for p in man.history["byid"]["way"]]
    rows = con.execute(
        f"SELECT version, minor, visible, cell, xmin_e7 IS NOT NULL AS has_bbox FROM read_parquet({paths!r}) "
        "WHERE id = 301 ORDER BY minor"
    ).fetchall()
    assert [r[:2] for r in rows] == [(1, 0), (1, 1), (1, 2)]  # own version never bumps
    assert [r[3] for r in rows] == ["00", "00", "root"]  # v1, diff1-minor stay in 00; diff2-minor moves to root
    assert all(r[2] for r in rows)  # all visible, all with resolvable bbox (both refs known throughout)
    assert all(r[4] for r in rows)

    # a move tombstone for the way in cell "00" once its minor=2 state moved to "root"
    cell00_parts = man.history["spatial"]["way"].get("00", [])
    spatial_paths = [str(root / p["path"]) for p in cell00_parts]
    tomb = con.execute(
        f"SELECT minor, visible FROM read_parquet({spatial_paths!r}) WHERE id = 301 AND visible = FALSE"
    ).fetchall()
    assert tomb == [(2, False)]


def test_way_geometry_resolves_at_each_state(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    parts = []
    for cell_parts in man.history["spatial"]["way"].values():
        parts.extend(cell_parts)
    paths = [str(root / p["path"]) for p in parts]
    wkts = con.execute(
        f"SELECT minor, ST_AsText(geometry) FROM read_parquet({paths!r}) WHERE id = 301 AND visible ORDER BY minor"
    ).fetchall()
    assert len(wkts) == 3
    for _minor, wkt in wkts:
        assert wkt.startswith("LINESTRING")


def test_relation_gets_minor_versions_from_member_way_states(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    paths = [str(root / p["path"]) for p in man.history["byid"]["relation"]]
    rows = con.execute(
        f"SELECT version, minor, xmin_e7, xmax_e7 FROM read_parquet({paths!r}) WHERE id = 401 ORDER BY minor"
    ).fetchall()
    assert [r[:2] for r in rows] == [(1, 0), (1, 1), (1, 2)]
    # the relation's bbox must have grown by the final (minor=2) state to
    # cover node 101's far-away position.
    assert rows[-1][3] > rows[0][3]


def test_valid_from_valid_to_chain_for_node101(history_root):
    root, _summary = history_root
    man = manifest_mod.load_latest(str(root))
    con = _con()
    paths = [str(root / p["path"]) for p in man.history["byid"]["node"]]
    rows = con.execute(
        f"SELECT version, valid_from, valid_to FROM read_parquet({paths!r}) WHERE id = 101 ORDER BY version"
    ).fetchall()
    assert rows[0][2] == rows[1][1]  # v1's valid_to == v2's valid_from
    assert rows[1][2] == rows[2][1]  # v2's valid_to == v3's valid_from
    assert rows[2][2] is None  # latest state: valid_to NULL


def test_validate_passes_with_history(history_root):
    root, _summary = history_root
    ok, summary = validate(str(root))
    assert ok, "\n".join(summary)
    assert any("checked history" in line for line in summary)


def test_cli_history_build_and_manifest(tmp_path_factory, current_root, base_pbf, osc_dir):
    from osmpq.cli import main

    import shutil

    root = tmp_path_factory.mktemp("history-build-cliroot") / "root"
    shutil.copytree(current_root, root, copy_function=__import__("os").link)
    tmp = tmp_path_factory.mktemp("history-build-cli-tmp")
    rc = main([
        "history", "build", str(root), "--pbf", str(base_pbf), "--osc", str(osc_dir), "--tmpdir", str(tmp),
    ])
    assert rc == 0
    man = manifest_mod.load_latest(str(root))
    assert man.manifest_version == 5

    rc2 = main(["manifest", str(root)])
    assert rc2 == 0

    rc3 = main(["validate", str(root)])
    assert rc3 == 0


def test_osh_input_gives_identical_history_rows(tmp_path_factory, current_root, base_pbf, osc_dir):
    """--osh (a full-history file equivalent to --pbf + --osc) must produce
    the same history rows (docs/m4-contracts.md section 4.3)."""
    import shutil

    osh_path = base_pbf.parent / "story.osh.pbf"
    w = osmium.SimpleWriter(str(osh_path))
    w.add_node(_node(101, *N101_BASE, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(102, *N102, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(103, *N103, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(104, *N104_OUTSIDE, 1, dt.datetime(2020, 1, 1)))
    w.add_way(_way(301, [101, 102], 1, dt.datetime(2020, 1, 1), tags={"highway": "residential"}))
    w.add_relation(_relation(401, [("w", 301, "outer")], 1, dt.datetime(2020, 1, 1), tags={"type": "multipolygon"}))
    w.add_node(_node(101, *N101_SMALL_MOVE, 2, dt.datetime(2020, 2, 1)))
    w.add_node(_node(101, *N101_FAR_MOVE, 3, dt.datetime(2020, 3, 1)))
    w.add_node(_node(103, *N103, 2, dt.datetime(2020, 4, 1), visible=False))
    w.close()

    root_pbf = tmp_path_factory.mktemp("history-osh-root-pbf") / "root"
    shutil.copytree(current_root, root_pbf, copy_function=__import__("os").link)
    tmp1 = tmp_path_factory.mktemp("history-osh-tmp-pbf")
    history_build(HistoryBuildOptions(root=str(root_pbf), pbf_path=str(base_pbf), osc_paths=[str(osc_dir)], tmpdir=str(tmp1)))

    root_osh = tmp_path_factory.mktemp("history-osh-root-osh") / "root"
    shutil.copytree(current_root, root_osh, copy_function=__import__("os").link)
    tmp2 = tmp_path_factory.mktemp("history-osh-tmp-osh")
    history_build(HistoryBuildOptions(root=str(root_osh), osh_path=str(osh_path), tmpdir=str(tmp2)))

    con = _con()
    man_pbf = manifest_mod.load_latest(str(root_pbf))
    man_osh = manifest_mod.load_latest(str(root_osh))
    for typ in ("node", "way", "relation"):
        paths_pbf = [str(root_pbf / p["path"]) for p in man_pbf.history["byid"][typ]]
        paths_osh = [str(root_osh / p["path"]) for p in man_osh.history["byid"][typ]]
        rows_pbf = con.execute(f"SELECT * FROM read_parquet({paths_pbf!r}) ORDER BY id, version, minor").fetchall()
        rows_osh = con.execute(f"SELECT * FROM read_parquet({paths_osh!r}) ORDER BY id, version, minor").fetchall()
        assert rows_pbf == rows_osh, typ
