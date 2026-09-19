"""End-to-end test: build data/bermuda-latest.osm.pbf and validate the
result against docs/m0-contracts.md sections 3-4.

Skipped if the (git-ignored) development fixture isn't present.
"""
from __future__ import annotations

import glob
from pathlib import Path

import duckdb
import pytest

from osmpq.build.builder import BuildOptions, build
from osmpq.layout import cells as cells_mod
from osmpq.layout import manifest as manifest_mod

REPO_ROOT = Path(__file__).resolve().parents[1]
PBF_PATH = REPO_ROOT / "data" / "bermuda-latest.osm.pbf"

pytestmark = pytest.mark.skipif(not PBF_PATH.exists(), reason="data/bermuda-latest.osm.pbf not present")


@pytest.fixture(scope="module")
def built_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("bermuda-root")
    tmpdir = tmp_path_factory.mktemp("bermuda-build-tmp")
    opts = BuildOptions(
        pbf_path=str(PBF_PATH),
        root=str(root),
        max_nodes_per_cell=20_000,
        tmpdir=str(tmpdir),
    )
    build(opts)
    return root


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute("INSTALL spatial")
    c.execute("LOAD spatial")
    return c


@pytest.fixture(scope="module")
def man(built_root) -> manifest_mod.Manifest:
    return manifest_mod.load_latest(str(built_root))


def test_manifest_loads_and_has_multiple_leaves(man):
    # --max-nodes-per-cell 20000 on ~262k Bermuda nodes must force splitting.
    assert len(man.leaf_cells) > 1
    assert man.generation
    assert man.tables.get("node", {}).get("cells")
    assert man.tables.get("way", {}).get("cells")


def test_all_manifest_paths_exist_with_matching_row_counts(built_root, man, con):
    paths_and_rows: list[tuple[str, int]] = []
    for spec in man.tables.values():
        for entry in spec.get("cells", {}).values():
            if "path" in entry:
                paths_and_rows.append((entry["path"], entry["rows"]))
            else:
                for part in ("tagged", "untagged"):
                    if entry.get(part):
                        paths_and_rows.append((entry[part]["path"], entry[part]["rows"]))
    for parts in man.byid.values():
        for p in parts:
            paths_and_rows.append((p["path"], p["rows"]))
    for parts in man.index.values():
        for p in parts:
            paths_and_rows.append((p["path"], p["rows"]))

    assert paths_and_rows, "manifest should reference at least one file"
    for rel_path, expected_rows in paths_and_rows:
        full = built_root / rel_path
        assert full.exists(), full
        n = con.execute(f"SELECT count(*) FROM read_parquet('{full.as_posix()}')").fetchone()[0]
        assert n == expected_rows, (rel_path, n, expected_rows)


def _node_files(built_root) -> list[str]:
    return glob.glob(str(built_root / "spatial" / "*" / "node" / "cell=*" / "tagged=*" / "part-0.parquet"))


def _way_files(built_root) -> list[str]:
    return glob.glob(str(built_root / "spatial" / "*" / "way" / "cell=*" / "part-0.parquet"))


def _relation_files(built_root) -> list[str]:
    return glob.glob(str(built_root / "spatial" / "*" / "relation" / "cell=*" / "part-0.parquet"))


def test_every_node_in_exactly_one_file_and_in_byid(built_root, man, con):
    node_files = _node_files(built_root)
    total = con.execute(f"SELECT count(*) FROM read_parquet({node_files!r})").fetchone()[0]
    distinct = con.execute(f"SELECT count(DISTINCT id) FROM read_parquet({node_files!r})").fetchone()[0]
    assert total == distinct, "a node id appears in more than one spatial file"

    byid_paths = [str(built_root / p["path"]) for p in man.byid["node"]]
    byid_total = con.execute(f"SELECT count(*) FROM read_parquet({byid_paths!r})").fetchone()[0]
    assert byid_total == total

    missing = con.execute(f"""
        SELECT count(*) FROM read_parquet({node_files!r}) s
        LEFT JOIN read_parquet({byid_paths!r}) b USING (id)
        WHERE b.id IS NULL
    """).fetchone()[0]
    assert missing == 0


def test_every_way_cell_fully_contains_its_bbox(built_root, man, con):
    way_files = _way_files(built_root)
    rows = con.execute(
        f"SELECT cell, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM read_parquet({way_files!r}) "
        "WHERE xmin_e7 IS NOT NULL"
    ).fetchall()
    assert rows, "expected at least one way with a resolvable bbox"
    for cell, ymin, xmin, ymax, xmax in rows:
        south, west, north, east = ymin / 1e7, xmin / 1e7, ymax / 1e7, xmax / 1e7
        c_south, c_west, c_north, c_east = cells_mod.cell_bbox(cell)
        assert south >= c_south and west >= c_west and north <= c_north and east <= c_east, (
            cell, (c_south, c_west, c_north, c_east), (south, west, north, east)
        )


def test_node_way_index_row_count_equals_total_refs(built_root, man, con):
    way_files = _way_files(built_root)
    total_refs = con.execute(f"SELECT sum(len(refs)) FROM read_parquet({way_files!r})").fetchone()[0]
    nw_paths = [str(built_root / p["path"]) for p in man.index["node_way"]]
    nw_count = con.execute(f"SELECT count(*) FROM read_parquet({nw_paths!r})").fetchone()[0]
    assert nw_count == total_refs


def test_member_index_row_count_equals_total_members(built_root, man, con):
    rel_files = _relation_files(built_root)
    total_members = con.execute(f"SELECT sum(len(members)) FROM read_parquet({rel_files!r})").fetchone()[0]
    mem_paths = [str(built_root / p["path"]) for p in man.index["member"]]
    mem_count = con.execute(f"SELECT count(*) FROM read_parquet({mem_paths!r})").fetchone()[0]
    assert mem_count == total_members


def test_sample_way_geometry_matches_its_refs_node_coordinates(built_root, man, con):
    way_files = _way_files(built_root)
    byid_node_paths = [str(built_root / p["path"]) for p in man.byid["node"]]
    way_id, refs, wkt = con.execute(
        f"SELECT id, refs, ST_AsText(geometry) FROM read_parquet({way_files!r}) "
        "WHERE geometry IS NOT NULL ORDER BY id LIMIT 1"
    ).fetchone()
    assert wkt.startswith("LINESTRING")

    rows = con.execute(f"""
        SELECT n.lon_e7 / 1e7, n.lat_e7 / 1e7
        FROM (SELECT unnest(?) AS ref, generate_subscripts(?, 1) AS ord) t
        JOIN read_parquet({byid_node_paths!r}) n ON n.id = t.ref
        ORDER BY t.ord
    """, [refs, refs]).fetchall()
    expected_points = ", ".join(f"{lon} {lat}" for lon, lat in rows)
    match = con.execute(
        f"SELECT ST_Equals(geometry, ST_GeomFromText('LINESTRING ({expected_points})')) "
        f"FROM read_parquet({way_files!r}) WHERE id = {way_id}"
    ).fetchone()[0]
    assert match


def test_promoted_amenity_matches_tags_map(built_root, con):
    for files in (_node_files(built_root), _way_files(built_root), _relation_files(built_root)):
        if not files:
            continue
        mismatches = con.execute(
            f"SELECT count(*) FROM read_parquet({files!r}) WHERE amenity IS DISTINCT FROM tags['amenity']"
        ).fetchone()[0]
        assert mismatches == 0


def test_way_geometry_column_is_native_geometry_type(built_root, con):
    way_files = _way_files(built_root)
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{way_files[0]}')").fetchall()
    geom_type = next(t for name, t, *_ in desc if name == "geometry")
    assert geom_type.startswith("GEOMETRY"), geom_type


def test_cli_manifest_command_runs(built_root, capsys):
    from osmpq.cli import main

    rc = main(["manifest", str(built_root)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "generation:" in out
    assert "leaf_cells:" in out
