"""``osmpq compact`` end-to-end test on the Bermuda extract, per
docs/m2-contracts.md section 6.

Builds Bermuda through the M0-compatible pipeline (``osmpq build
data/bermuda-latest.osm.pbf <tmp> --max-nodes-per-cell 20000``, i.e.
``osmpq.build.builder.build``, which runs ``raw-py`` then ``build --raw``),
adds a node_way index (``raw-py`` does not build one -- see the
``_add_node_way_index`` helper below, a test-only equivalent of ``osmpq-raw
node-way-index`` built directly with DuckDB, so compaction's node_way
rebuild path is actually exercised), writes synthetic delta tiers with
``tests/fixtures/deltas.py`` covering a retagged node, a moved node, a
deleted way, a new way and a modified relation across two tiers (with one
id present in both, to check newest-tier-wins), compacts, and checks the
result against ``osmpq validate`` plus direct DuckDB assertions.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fixtures import deltas as deltas_fixture  # noqa: E402

from osmpq.build.builder import BuildOptions, build  # noqa: E402
from osmpq.build.compact import CompactOptions, compact  # noqa: E402
from osmpq.build.validate import validate  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
PBF_PATH = REPO_ROOT / "data" / "bermuda-latest.osm.pbf"

pytestmark = pytest.mark.skipif(not PBF_PATH.exists(), reason="data/bermuda-latest.osm.pbf not present")


def _add_node_way_index(root: Path) -> None:
    """Test-only equivalent of ``osmpq-raw node-way-index``: builds
    ``index/<gen>/node_way/`` from the base's byid/way parts with plain
    DuckDB and patches the manifest in place (same manifest number/
    generation), so a base built through ``raw-py`` -- which never builds
    this index (m1-contracts.md section 3.3 is Rust-only) -- still has one
    for this test to exercise ``compact``'s node_way rebuild path against."""
    latest = int((root / "manifest" / "LATEST").read_text().strip())
    man = json.loads((root / "manifest" / f"{latest}.json").read_text())
    generation = man["generation"]
    way_paths = [str(root / p["path"]) for p in man["byid"]["way"]]
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE _nw AS
        SELECT unnest(refs) AS node_id, id AS way_id
        FROM read_parquet({way_paths!r}) WHERE refs IS NOT NULL
        ORDER BY node_id, way_id
    """)
    rel = f"index/{generation}/node_way/part-00000.parquet"
    out_path = root / rel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM _nw) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    rows, min_id, max_id = con.execute("SELECT count(*), min(node_id), max(node_id) FROM _nw").fetchone()
    con.close()
    man["index"]["node_way"] = [{
        "path": rel, "min_id": int(min_id), "max_id": int(max_id),
        "rows": int(rows), "bytes": out_path.stat().st_size,
    }]
    (root / "manifest" / f"{latest}.json").write_text(json.dumps(man, indent=2))


@pytest.fixture(scope="module")
def built_root(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("bermuda-compact-root")
    opts = BuildOptions(
        pbf_path=str(PBF_PATH),
        root=str(root),
        max_nodes_per_cell=20_000,
        tmpdir=str(tmp_path_factory.mktemp("bermuda-compact-buildtmp")),
    )
    build(opts)
    _add_node_way_index(root)
    return root


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("INSTALL spatial")
    c.execute("LOAD spatial")
    return c


@pytest.fixture(scope="module")
def scenario_ids(built_root, con) -> dict:
    node_ids = deltas_fixture.sample_ids(str(built_root), "node", 6, con=con)
    way_ids = deltas_fixture.sample_ids(str(built_root), "way", 4, con=con)
    relation_ids = deltas_fixture.sample_ids(str(built_root), "relation", 2, con=con)
    assert len(node_ids) >= 4 and len(way_ids) >= 2 and len(relation_ids) >= 1

    latest = int((built_root / "manifest" / "LATEST").read_text().strip())
    man = json.loads((built_root / "manifest" / f"{latest}.json").read_text())
    node_paths = [str(built_root / p["path"]) for p in man["byid"]["node"]]
    way_paths = [str(built_root / p["path"]) for p in man["byid"]["way"]]

    retag_id, move_id = node_ids[0], node_ids[1]
    new_way_ref_a, new_way_ref_b = node_ids[2], node_ids[3]
    del_way_id = way_ids[0]
    mod_relation_id = relation_ids[0]
    new_way_id = 900_000_001

    move_lat_e7, move_lon_e7 = con.execute(
        f"SELECT lat_e7, lon_e7 FROM read_parquet({node_paths!r}) WHERE id = {move_id}"
    ).fetchone()
    del_way_refs = con.execute(
        f"SELECT refs FROM read_parquet({way_paths!r}) WHERE id = {del_way_id}"
    ).fetchone()[0]

    return {
        "retag_id": retag_id,
        "move_id": move_id,
        "move_new_lat": move_lat_e7 / 1e7 + 0.05,
        "move_new_lon": move_lon_e7 / 1e7 + 0.05,
        "del_way_id": del_way_id,
        "del_way_refs": del_way_refs,
        "new_way_id": new_way_id,
        "new_way_refs": [new_way_ref_a, new_way_ref_b],
        "mod_relation_id": mod_relation_id,
    }


@pytest.fixture(scope="module")
def deltas_written(built_root, scenario_ids) -> dict:
    s = scenario_ids
    tiers = {
        "week": [
            ("node", s["retag_id"], {"tags": {"amenity": "changed_in_week"}}),
            ("way", s["del_way_id"], "delete"),
        ],
        "day": [
            # same id as week -> newest tier (day) must win at compaction.
            ("node", s["retag_id"], {"tags": {"amenity": "changed_in_day"}}),
            ("node", s["move_id"], {"lat": s["move_new_lat"], "lon": s["move_new_lon"]}),
            ("way", s["new_way_id"], {"tags": {"highway": "residential"}, "refs": s["new_way_refs"]}),
            ("relation", s["mod_relation_id"], {"tags": {"name": "modified-by-deltas-fixture"}}),
        ],
    }
    return deltas_fixture.write_deltas(str(built_root), tiers)


@pytest.fixture(scope="module")
def pre_compact_manifest(built_root, deltas_written) -> dict:
    # Captured eagerly (as soon as deltas_written is realized) rather than
    # re-read from manifest/LATEST lazily: another test in this module may
    # have already realized `compacted` by the time a *later* test first
    # asks for this fixture, which would otherwise read the post-compact
    # manifest instead of the pre-compact one.
    latest = int((built_root / "manifest" / "LATEST").read_text().strip())
    return json.loads((built_root / "manifest" / f"{latest}.json").read_text())


@pytest.fixture(scope="module")
def compacted(built_root, pre_compact_manifest, tmp_path_factory) -> dict:
    return compact(CompactOptions(root=str(built_root), tmpdir=str(tmp_path_factory.mktemp("bermuda-compact-tmp"))))


def _byid_paths(man: dict, root: Path, table: str) -> list[str]:
    return [str(root / p["path"]) for p in man["byid"][table]]


def _spatial_way_or_relation_paths(man: dict, root: Path, table: str) -> list[str]:
    return [str(root / e["path"]) for e in man["tables"][table]["cells"].values()]


def _spatial_node_paths(man: dict, root: Path) -> list[str]:
    out = []
    for entry in man["tables"]["node"]["cells"].values():
        for part in ("tagged", "untagged"):
            if part in entry:
                out.append(str(root / entry[part]["path"]))
    return out


# --------------------------------------------------------------------------


def test_compact_writes_new_generation_with_empty_deltas(built_root, compacted):
    assert compacted["generation"] != "g0001"
    assert compacted["deltas"] == {}
    assert compacted["manifest_version"] == 4  # base is v4 (areas) since M3


def test_validate_passes_on_compacted_generation(built_root, compacted):
    ok, summary = validate(str(built_root))
    assert ok, "\n".join(summary)


def test_retagged_node_has_newest_tier_tags(built_root, compacted, scenario_ids, con):
    paths = _byid_paths(compacted, built_root, "node")
    tags = con.execute(f"SELECT tags FROM read_parquet({paths!r}) WHERE id = {scenario_ids['retag_id']}").fetchone()[0]
    assert tags == {"amenity": "changed_in_day"}, tags


def test_moved_node_is_in_new_cell_only(built_root, compacted, scenario_ids, con):
    move_id = scenario_ids["move_id"]
    found_cells = []
    for cell, entry in compacted["tables"]["node"]["cells"].items():
        for part in ("tagged", "untagged"):
            if part in entry:
                p = str(built_root / entry[part]["path"])
                n = con.execute(f"SELECT count(*) FROM read_parquet('{p}') WHERE id = {move_id}").fetchone()[0]
                if n:
                    found_cells.append(cell)
    assert len(found_cells) == 1, found_cells

    byid_paths = _byid_paths(compacted, built_root, "node")
    new_cell, lat_e7, lon_e7 = con.execute(
        f"SELECT cell, lat_e7, lon_e7 FROM read_parquet({byid_paths!r}) WHERE id = {move_id}"
    ).fetchone()
    assert new_cell == found_cells[0]
    assert abs(lat_e7 / 1e7 - scenario_ids["move_new_lat"]) < 1e-6
    assert abs(lon_e7 / 1e7 - scenario_ids["move_new_lon"]) < 1e-6


def test_deleted_way_absent_from_spatial_and_byid(built_root, compacted, scenario_ids, con):
    del_way_id = scenario_ids["del_way_id"]
    byid_paths = _byid_paths(compacted, built_root, "way")
    assert con.execute(f"SELECT count(*) FROM read_parquet({byid_paths!r}) WHERE id = {del_way_id}").fetchone()[0] == 0
    spatial_paths = _spatial_way_or_relation_paths(compacted, built_root, "way")
    assert con.execute(f"SELECT count(*) FROM read_parquet({spatial_paths!r}) WHERE id = {del_way_id}").fetchone()[0] == 0


def test_deleted_way_node_way_entries_gone(built_root, compacted, scenario_ids, con):
    nw_paths = [str(built_root / p["path"]) for p in compacted["index"]["node_way"]]
    assert nw_paths, "expected a node_way index (patched onto the base by _add_node_way_index)"
    del_way_id = scenario_ids["del_way_id"]
    n = con.execute(f"SELECT count(*) FROM read_parquet({nw_paths!r}) WHERE way_id = {del_way_id}").fetchone()[0]
    assert n == 0


def test_new_way_present_with_geometry(built_root, compacted, scenario_ids, con):
    new_way_id = scenario_ids["new_way_id"]
    byid_paths = _byid_paths(compacted, built_root, "way")
    row = con.execute(f"SELECT refs, is_closed FROM read_parquet({byid_paths!r}) WHERE id = {new_way_id}").fetchone()
    assert row is not None
    assert list(row[0]) == scenario_ids["new_way_refs"]

    spatial_paths = _spatial_way_or_relation_paths(compacted, built_root, "way")
    wkt = con.execute(
        f"SELECT ST_AsText(geometry) FROM read_parquet({spatial_paths!r}) WHERE id = {new_way_id}"
    ).fetchone()[0]
    assert wkt is not None and wkt.startswith("LINESTRING")

    nw_paths = [str(built_root / p["path"]) for p in compacted["index"]["node_way"]]
    for ref in scenario_ids["new_way_refs"]:
        n = con.execute(f"SELECT count(*) FROM read_parquet({nw_paths!r}) WHERE node_id = {ref} AND way_id = {new_way_id}").fetchone()[0]
        assert n == 1, (ref, new_way_id)


def test_member_index_reflects_modified_relation(built_root, compacted, scenario_ids, con):
    mod_relation_id = scenario_ids["mod_relation_id"]
    byid_paths = _byid_paths(compacted, built_root, "relation")
    members, tags = con.execute(
        f"SELECT members, tags FROM read_parquet({byid_paths!r}) WHERE id = {mod_relation_id}"
    ).fetchone()
    assert tags == {"name": "modified-by-deltas-fixture"}

    member_paths = [str(built_root / p["path"]) for p in compacted["index"]["member"]]
    for m in members:
        n = con.execute(
            f"SELECT count(*) FROM read_parquet({member_paths!r}) "
            f"WHERE member_type = '{m['type']}' AND member_id = {m['ref']} AND parent_id = {mod_relation_id}"
        ).fetchone()[0]
        assert n == 1, m


def test_deltas_empty_in_manifest(compacted):
    assert compacted["deltas"] == {}


def test_untouched_files_are_hardlinks_of_previous_generation(built_root, pre_compact_manifest, compacted):
    old_way_cells = pre_compact_manifest["tables"]["way"]["cells"]
    new_way_cells = compacted["tables"]["way"]["cells"]
    # Every entry's "path" differs across generations regardless of whether
    # it was hardlinked or rewritten (the generation label is in the path),
    # so identical rows+bytes -- not a differing path -- is what marks a
    # cell as untouched (hardlinked) rather than rewritten.
    untouched = [
        c for c in old_way_cells
        if c in new_way_cells
        and old_way_cells[c]["rows"] == new_way_cells[c]["rows"]
        and old_way_cells[c]["bytes"] == new_way_cells[c]["bytes"]
    ]
    assert untouched, "expected at least one untouched way cell"
    checked = 0
    for c in untouched:
        old_path = built_root / old_way_cells[c]["path"]
        new_path = built_root / new_way_cells[c]["path"]
        assert old_path.exists() and new_path.exists()
        assert os.stat(old_path).st_ino == os.stat(new_path).st_ino, (c, old_path, new_path)
        checked += 1
    assert checked > 0
    # Bermuda's node/way/relation byid tables are each a single part, and
    # the scenario always touches an id somewhere inside it, so every byid
    # part is rewritten here; test_gc.py separately checks that gc keeps
    # exactly the files a manifest references (including untouched byid
    # parts on a root with more than one part per table).
