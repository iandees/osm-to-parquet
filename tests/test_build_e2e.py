"""End-to-end test: build data/bermuda-latest.osm.pbf through the M1 two
stage pipeline (``osmpq raw-py`` then ``osmpq build --raw``) and validate the
result against docs/m0-contracts.md sections 3-4 and docs/m1-contracts.md
sections 2, 4, 5, 7.

Skipped if the (git-ignored) development fixture isn't present.
"""
from __future__ import annotations

import glob
import json
from pathlib import Path

import duckdb
import pytest

from osmpq.build.builder import BuildFromRawOptions, BuildOptions, build, build_from_raw
from osmpq.build.raw import RawBuildOptions, raw_build
from osmpq.build.validate import validate
from osmpq.layout import cells as cells_mod
from osmpq.layout import manifest as manifest_mod

REPO_ROOT = Path(__file__).resolve().parents[1]
PBF_PATH = REPO_ROOT / "data" / "bermuda-latest.osm.pbf"

pytestmark = pytest.mark.skipif(not PBF_PATH.exists(), reason="data/bermuda-latest.osm.pbf not present")


@pytest.fixture(scope="module")
def rawdir(tmp_path_factory) -> Path:
    d = tmp_path_factory.mktemp("bermuda-raw")
    opts = RawBuildOptions(
        pbf_path=str(PBF_PATH),
        rawdir=str(d),
        max_nodes_per_cell=20_000,
        tmpdir=str(tmp_path_factory.mktemp("bermuda-raw-tmp")),
    )
    raw_build(opts)
    return d


@pytest.fixture(scope="module")
def built_root(tmp_path_factory, rawdir) -> Path:
    root = tmp_path_factory.mktemp("bermuda-root")
    opts = BuildFromRawOptions(
        rawdir=str(rawdir),
        root=str(root),
        tmpdir=str(tmp_path_factory.mktemp("bermuda-build-tmp")),
    )
    build_from_raw(opts)
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


# --------------------------------------------------------------------------
# raw-py output (docs/m1-contracts.md section 3)
# --------------------------------------------------------------------------


def test_raw_leaves_json_has_v2_fields(rawdir):
    leaves = json.loads((rawdir / "leaves.json").read_text())
    assert leaves["max_nodes_per_cell"] == 20_000
    assert leaves["max_depth"] == 13
    assert leaves["leaves"]
    assert len(leaves["leaves"]) > 1  # 20k cap on ~262k bermuda nodes must force splitting


def test_raw_summary_json_has_counts_and_timings(rawdir):
    summary = json.loads((rawdir / "summary.json").read_text())
    assert summary["counts"]["nodes"] > 0
    assert summary["counts"]["ways"] > 0
    assert summary["producer"] == "osmpq raw-py"
    assert summary["timings_seconds"]


def test_raw_untagged_node_partition_has_metadata_columns(rawdir, con):
    files = glob.glob(str(rawdir / "spatial" / "node" / "cell=*" / "tagged=false" / "part-0.parquet"))
    assert files
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{files[0]}')").fetchall()
    names = {row[0] for row in desc}
    # Present (raw-py is allowed to leave the values NULL: osmium_read only
    # returns metadata for tagged nodes).
    assert {"version", "changeset", "timestamp", "uid", "user"} <= names


def test_raw_way_geometry_is_native_geometry_type(rawdir, con):
    files = glob.glob(str(rawdir / "spatial" / "way" / "cell=*" / "part-0.parquet"))
    assert files
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{files[0]}')").fetchall()
    geom_type = next(t for name, t, *_ in desc if name == "geometry")
    assert geom_type.startswith("GEOMETRY"), geom_type


def test_raw_way_cells_obey_v2_placement_rule(rawdir, con):
    """Every raw way's cell must be a leaf, or an ancestor at one of the
    allowed ancestor depths (docs/m1-contracts.md section 2)."""
    leaves = json.loads((rawdir / "leaves.json").read_text())
    leaf_set = set(leaves["leaves"])
    ancestor_depths = set(leaves["ancestor_depths"])
    files = glob.glob(str(rawdir / "spatial" / "way" / "cell=*" / "part-0.parquet"))
    cells = {row[0] for row in con.execute(f"SELECT DISTINCT cell FROM read_parquet({files!r})").fetchall()}
    assert cells
    for cell in cells:
        depth = 0 if cell == cells_mod.ROOT else len(cell)
        assert cell in leaf_set or depth in ancestor_depths, (cell, depth)


# --------------------------------------------------------------------------
# build --raw output / manifest v2 (docs/m1-contracts.md sections 4-5)
# --------------------------------------------------------------------------


def test_manifest_is_v2_with_expected_fields(man):
    assert man.manifest_version == 4  # v4 since M3: areas derived at the end of build
    assert man.ancestor_depths == [0, 3, 6, 9, 12]
    assert man.max_depth == 13
    assert set(man.rowgroup_index) == {"node", "way", "relation"}
    assert man.producer.get("raw") == "osmpq raw-py"
    assert man.producer.get("build")
    for key in ("nodes", "tagged_nodes", "ways", "relations", "leaf_cells", "bytes"):
        assert key in man.stats
    assert len(man.leaf_cells) > 1


def test_manifest_loads_and_has_multiple_leaves(man):
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


def test_node_way_index_absent_is_an_empty_list(man):
    # raw-py doesn't build the node_way index (docs/m1-contracts.md: "index.node_way
    # may be an empty list when the index was not built").
    assert man.index["node_way"] == []


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


def test_every_way_and_relation_cell_obeys_v2_placement_rule(built_root, man, con):
    leaf_set = set(man.leaf_cells)
    ancestor_depths = set(man.ancestor_depths)
    for label, files in (("way", _way_files(built_root)), ("relation", _relation_files(built_root))):
        rows = con.execute(
            f"SELECT cell, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM read_parquet({files!r}) "
            "WHERE xmin_e7 IS NOT NULL"
        ).fetchall()
        assert rows, f"expected at least one {label} with a resolvable bbox"
        for cell, ymin, xmin, ymax, xmax in rows:
            south, west, north, east = ymin / 1e7, xmin / 1e7, ymax / 1e7, xmax / 1e7
            c_south, c_west, c_north, c_east = cells_mod.cell_bbox(cell)
            assert south >= c_south and west >= c_west and north <= c_north and east <= c_east, (
                label, cell, (c_south, c_west, c_north, c_east), (south, west, north, east)
            )
            depth = 0 if cell == cells_mod.ROOT else len(cell)
            assert cell in leaf_set or depth in ancestor_depths, (label, cell, depth)


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


def test_untagged_node_partition_has_metadata_columns_in_built_root(built_root, con):
    files = glob.glob(str(built_root / "spatial" / "*" / "node" / "cell=*" / "tagged=false" / "part-0.parquet"))
    assert files
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{files[0]}')").fetchall()
    names = {row[0] for row in desc}
    assert {"version", "changeset", "timestamp", "uid", "user"} <= names


def test_rowgroup_index_covers_every_spatial_file(built_root, man, con):
    import pyarrow.parquet as pq

    for table_name, files in (
        ("node", _node_files(built_root)),
        ("way", _way_files(built_root)),
        ("relation", _relation_files(built_root)),
    ):
        rg_path = built_root / man.rowgroup_index[table_name]
        assert rg_path.exists()
        counts = dict(con.execute(f"SELECT path, count(*) FROM read_parquet('{rg_path.as_posix()}') GROUP BY path").fetchall())
        for f in files:
            rel = str(Path(f).relative_to(built_root)).replace("\\", "/")
            actual = pq.ParquetFile(f).metadata.num_row_groups
            assert counts.get(rel) == actual, (table_name, rel, counts.get(rel), actual)


def test_cli_manifest_command_runs(built_root, capsys):
    from osmpq.cli import main

    rc = main(["manifest", str(built_root)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "generation:" in out
    assert "leaf_cells:" in out
    assert "manifest_version: 4" in out


def test_cli_validate_command_passes(built_root):
    from osmpq.cli import main

    rc = main(["validate", str(built_root)])
    assert rc == 0


def test_validate_function_reports_pass(built_root):
    ok, summary = validate(str(built_root))
    assert ok, "\n".join(summary)
    assert any("PASS" in line for line in summary)


# --------------------------------------------------------------------------
# M0-compatible `osmpq build <pbf> <root>` still works (runs raw-py into a
# tmpdir, then build --raw), per docs/m1-contracts.md section 7.
# --------------------------------------------------------------------------


def test_m0_form_build_still_works(tmp_path_factory):
    root = tmp_path_factory.mktemp("bermuda-root-m0form")
    tmpdir = tmp_path_factory.mktemp("bermuda-m0form-tmp")
    opts = BuildOptions(
        pbf_path=str(PBF_PATH),
        root=str(root),
        max_nodes_per_cell=20_000,
        tmpdir=str(tmpdir),
    )
    build(opts)
    man = manifest_mod.load_latest(str(root))
    assert man.manifest_version == 4  # v4 since M3: areas derived at the end of build
    assert man.producer.get("raw") == "osmpq raw-py"
    ok, summary = validate(str(root))
    assert ok, "\n".join(summary)
