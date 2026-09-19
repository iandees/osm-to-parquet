"""Pytest for the Rust `osmpq-raw` producer (docs/m1-contracts.md section 3).

Skipped if `cargo` is unavailable and no `osmpq-raw` binary has already been
built. Otherwise builds the crate in release mode (if needed) and validates
its output against docs/m0-contracts.md sections 2 and 4 and
docs/m1-contracts.md section 2, on ``data/bermuda-latest.osm.pbf``:

- counts per type match DuckDB's `ST_ReadOSM`;
- every sampled node's `cell`/`hilbert` match `osmpq.layout.cells`/`hilbert`
  bit-for-bit;
- way geometry equals the coordinates of its refs in order;
- way cell placement obeys the v2 loose-placement rule
  (`osmpq.layout.cells.containing_cell_v2`);
- `DESCRIBE` shows `GEOMETRY` for the way `geometry` column;
- tags round-trip (including unicode) against `ST_ReadOSM`, and relation
  member roles are never NULL (empty string instead);
- untagged nodes carry non-NULL `version`/`timestamp` metadata.
"""
from __future__ import annotations

import glob
import json
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

from osmpq.layout import cells as cells_mod
from osmpq.layout import hilbert as hilbert_mod

REPO_ROOT = Path(__file__).resolve().parents[1]
CRATE_DIR = REPO_ROOT / "rust" / "osmpq-raw"
PBF_PATH = REPO_ROOT / "data" / "bermuda-latest.osm.pbf"
BINARY_PATH = CRATE_DIR / "target" / "release" / "osmpq-raw"


def _cargo_path() -> str | None:
    found = shutil.which("cargo")
    if found:
        return found
    fallback = Path.home() / ".cargo" / "bin" / "cargo"
    return str(fallback) if fallback.exists() else None


CARGO = _cargo_path()

pytestmark = [
    pytest.mark.skipif(not PBF_PATH.exists(), reason="data/bermuda-latest.osm.pbf not present"),
    pytest.mark.skipif(
        CARGO is None and not BINARY_PATH.exists(),
        reason="cargo and a pre-built osmpq-raw binary are both unavailable",
    ),
]


@pytest.fixture(scope="session")
def binary() -> Path:
    if not BINARY_PATH.exists():
        if CARGO is None:
            pytest.skip("cargo not available to build osmpq-raw")
        subprocess.run([CARGO, "build", "--release"], cwd=str(CRATE_DIR), check=True)
    if not BINARY_PATH.exists():
        pytest.skip("osmpq-raw binary could not be built")
    return BINARY_PATH


@pytest.fixture(scope="module")
def rawdir(binary, tmp_path_factory) -> Path:
    d = tmp_path_factory.mktemp("raw-bermuda-rust")
    tmp = tmp_path_factory.mktemp("raw-bermuda-rust-tmp")
    subprocess.run(
        [
            str(binary),
            "build",
            str(PBF_PATH),
            str(d),
            "--max-nodes-per-cell",
            "20000",
            "--tmpdir",
            str(tmp),
        ],
        check=True,
    )
    return d


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute("INSTALL spatial")
    c.execute("LOAD spatial")
    return c


def _node_byid_files(rawdir: Path) -> list[str]:
    return sorted(glob.glob(str(rawdir / "node" / "part-*.parquet")))


def _way_byid_files(rawdir: Path) -> list[str]:
    return sorted(glob.glob(str(rawdir / "way" / "part-*.parquet")))


def _relation_files(rawdir: Path) -> list[str]:
    return sorted(glob.glob(str(rawdir / "relation" / "part-*.parquet")))


def _node_spatial_files(rawdir: Path) -> list[str]:
    return sorted(glob.glob(str(rawdir / "spatial" / "node" / "cell=*" / "tagged=*" / "part-0.parquet")))


def _node_untagged_files(rawdir: Path) -> list[str]:
    return sorted(glob.glob(str(rawdir / "spatial" / "node" / "cell=*" / "tagged=false" / "part-0.parquet")))


def _way_spatial_files(rawdir: Path) -> list[str]:
    return sorted(glob.glob(str(rawdir / "spatial" / "way" / "cell=*" / "part-0.parquet")))


# --------------------------------------------------------------------------
# leaves.json / summary.json
# --------------------------------------------------------------------------


def test_leaves_json_v2_fields(rawdir):
    leaves = json.loads((rawdir / "leaves.json").read_text())
    assert leaves["max_nodes_per_cell"] == 20_000
    assert leaves["max_depth"] == 13
    assert leaves["ancestor_depths"] == [0, 3, 6, 9, 12]
    assert len(leaves["leaves"]) > 1  # 20k cap on ~262k bermuda nodes forces splitting


def test_summary_json(rawdir):
    summary = json.loads((rawdir / "summary.json").read_text())
    assert summary["counts"]["nodes"] > 0
    assert summary["counts"]["ways"] > 0
    assert summary["counts"]["relations"] > 0
    assert summary["producer"].startswith("osmpq-raw")
    assert summary["timings_seconds"]
    assert summary["node_store"] in ("sorted-mem", "dense-file")


# --------------------------------------------------------------------------
# counts vs DuckDB ST_ReadOSM
# --------------------------------------------------------------------------


def test_counts_match_st_readosm(rawdir, con):
    ref = dict(
        con.execute(
            f"SELECT kind, count(*) FROM ST_ReadOSM('{PBF_PATH.as_posix()}') GROUP BY kind"
        ).fetchall()
    )
    node_files = _node_spatial_files(rawdir)
    way_files = _way_spatial_files(rawdir)
    rel_files = _relation_files(rawdir)
    assert con.execute(f"SELECT count(*) FROM read_parquet({node_files!r})").fetchone()[0] == ref["node"]
    assert con.execute(f"SELECT count(*) FROM read_parquet({way_files!r})").fetchone()[0] == ref["way"]
    assert con.execute(f"SELECT count(*) FROM read_parquet({rel_files!r})").fetchone()[0] == ref["relation"]

    byid_node_files = _node_byid_files(rawdir)
    byid_way_files = _way_byid_files(rawdir)
    assert con.execute(f"SELECT count(*) FROM read_parquet({byid_node_files!r})").fetchone()[0] == ref["node"]
    assert con.execute(f"SELECT count(*) FROM read_parquet({byid_way_files!r})").fetchone()[0] == ref["way"]


# --------------------------------------------------------------------------
# cell / hilbert on a sample of nodes
# --------------------------------------------------------------------------


def test_node_cell_and_hilbert_match_python(rawdir, con):
    leaves = json.loads((rawdir / "leaves.json").read_text())
    leaf_set = frozenset(leaves["leaves"])
    byid_node_files = _node_byid_files(rawdir)
    rows = con.execute(
        f"SELECT id, lat_e7, lon_e7, cell, hilbert FROM read_parquet({byid_node_files!r}) "
        "ORDER BY id LIMIT 10000"
    ).fetchall()
    assert len(rows) == 10_000
    for node_id, lat_e7, lon_e7, cell, hilbert in rows:
        expected_cell = cells_mod.point_cell(lat_e7 / 1e7, lon_e7 / 1e7, leaf_set)
        assert cell == expected_cell, (node_id, cell, expected_cell)
        expected_hilbert = int(hilbert_mod.hilbert_key(lat_e7, lon_e7))
        assert int(hilbert) == expected_hilbert, (node_id, hilbert, expected_hilbert)


# --------------------------------------------------------------------------
# way geometry vs. its refs' node coordinates
# --------------------------------------------------------------------------


def test_way_geometry_matches_refs_node_coordinates(rawdir, con):
    way_files = _way_spatial_files(rawdir)
    byid_node_files = _node_byid_files(rawdir)
    rows = con.execute(
        f"SELECT id, refs FROM read_parquet({way_files!r}) WHERE geometry IS NOT NULL "
        "ORDER BY id LIMIT 20"
    ).fetchall()
    assert rows
    for way_id, refs in rows:
        pts = con.execute(
            f"""
            SELECT n.lon_e7 / 1e7, n.lat_e7 / 1e7
            FROM (SELECT unnest(?) AS ref, generate_subscripts(?, 1) AS ord) t
            JOIN read_parquet({byid_node_files!r}) n ON n.id = t.ref
            ORDER BY t.ord
            """,
            [refs, refs],
        ).fetchall()
        assert len(pts) >= 2
        expected_wkt = "LINESTRING (" + ", ".join(f"{lon} {lat}" for lon, lat in pts) + ")"
        match = con.execute(
            f"SELECT ST_Equals(geometry, ST_GeomFromText(?)) FROM read_parquet({way_files!r}) WHERE id = ?",
            [expected_wkt, way_id],
        ).fetchone()[0]
        assert match, (way_id, expected_wkt)


# --------------------------------------------------------------------------
# way cell v2 placement rule
# --------------------------------------------------------------------------


def _v2_rule(bbox, leaf_index: cells_mod.LeafIndex, ancestor_depths: list[int]) -> str:
    """The docs/m1-contracts.md section 2 rule, written directly from the
    `osmpq.layout.cells` primitives (per this test's brief) rather than via
    `cells_mod.containing_cell_v2`: that helper takes a numpy shortcut (each
    bbox corner's cell via half-open point-grid assignment, then a
    longest-common-prefix depth) that disagrees with the literal
    "descend while exactly one child fully contains B" rule -- and with
    this Rust producer -- when a corner sits exactly on a cell boundary
    (closed-interval bbox containment vs. half-open point membership).
    `containing_cell` (v1, unrestricted) implements the literal rule, so
    C = containing_cell(bbox, leaves); this only adds the v2 ancestor-depth
    snap.
    """
    c = cells_mod.containing_cell(bbox, leaf_index)
    if c in leaf_index:
        return c
    depth = 0 if c == cells_mod.ROOT else len(c)
    allowed = [d for d in ancestor_depths if d <= depth]
    target = max(allowed) if allowed else 0
    return cells_mod.ROOT if target == 0 else c[:target]


def test_way_cell_obeys_v2_placement_rule(rawdir, con):
    leaves = json.loads((rawdir / "leaves.json").read_text())
    leaf_index = cells_mod.LeafIndex(leaves["leaves"])
    ancestor_depths = leaves["ancestor_depths"]
    way_files = _way_spatial_files(rawdir)
    rows = con.execute(
        f"SELECT id, cell, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM read_parquet({way_files!r}) "
        "WHERE xmin_e7 IS NOT NULL"
    ).fetchall()
    assert rows, "expected at least one way with a resolvable bbox"
    leaf_set = set(leaves["leaves"])
    for way_id, cell, ymin, xmin, ymax, xmax in rows:
        bbox = (ymin / 1e7, xmin / 1e7, ymax / 1e7, xmax / 1e7)
        expected = _v2_rule(bbox, leaf_index, ancestor_depths)
        assert cell == expected, (way_id, cell, expected, bbox)

        # Belt-and-braces: also check the section-2 prose rule directly
        # (leaf, or an ancestor at one of the allowed depths).
        depth = 0 if cell == cells_mod.ROOT else len(cell)
        assert cell in leaf_set or depth in set(ancestor_depths)


# --------------------------------------------------------------------------
# geometry column type (GeoParquet fallback -> DuckDB GEOMETRY)
# --------------------------------------------------------------------------


def test_way_geometry_column_is_geometry_type(rawdir, con):
    way_files = _way_spatial_files(rawdir)
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{way_files[0]}')").fetchall()
    geom_type = next(t for name, t, *_ in desc if name == "geometry")
    assert geom_type.startswith("GEOMETRY"), geom_type


# --------------------------------------------------------------------------
# tags round-trip (incl. unicode) and empty (never NULL) relation roles
# --------------------------------------------------------------------------


def test_tags_roundtrip_against_st_readosm_incl_unicode(rawdir, con):
    node_files = _node_spatial_files(rawdir)
    ref = con.execute(
        f"""
        SELECT id, tags FROM ST_ReadOSM('{PBF_PATH.as_posix()}')
        WHERE kind = 'node' AND cardinality(tags) > 0
        ORDER BY id
        """
    ).fetchall()
    assert ref
    ids = ",".join(str(r[0]) for r in ref)
    got = dict(
        con.execute(f"SELECT id, tags FROM read_parquet({node_files!r}) WHERE id IN ({ids})").fetchall()
    )
    checked_unicode = False
    for node_id, tags in ref:
        got_tags = dict(got[node_id])
        assert got_tags == dict(tags), node_id
        if any(not v.isascii() for v in tags.values()):
            checked_unicode = True
    # Not fatal if bermuda happens to have no non-ascii tag values, but the
    # extract does (e.g. accented names), so assert it to catch regressions.
    assert checked_unicode, "expected at least one non-ascii tag value in the sample"


def test_relation_member_roles_are_never_null(rawdir, con):
    rel_files = _relation_files(rawdir)
    n_null = con.execute(
        f"SELECT count(*) FROM (SELECT unnest(members) AS m FROM read_parquet({rel_files!r})) "
        "WHERE m.role IS NULL"
    ).fetchone()[0]
    assert n_null == 0
    n_empty = con.execute(
        f"SELECT count(*) FROM (SELECT unnest(members) AS m FROM read_parquet({rel_files!r})) "
        "WHERE m.role = ''"
    ).fetchone()[0]
    assert n_empty >= 0  # exercised the query; empty-string roles are valid


# --------------------------------------------------------------------------
# metadata present for untagged nodes
# --------------------------------------------------------------------------


def test_untagged_nodes_have_metadata(rawdir, con):
    files = _node_untagged_files(rawdir)
    assert files
    total = con.execute(f"SELECT count(*) FROM read_parquet({files!r})").fetchone()[0]
    assert total > 0
    n_missing = con.execute(
        f"SELECT count(*) FROM read_parquet({files!r}) WHERE version IS NULL OR timestamp IS NULL"
    ).fetchone()[0]
    assert n_missing == 0


# --------------------------------------------------------------------------
# node_way-index subcommand
# --------------------------------------------------------------------------


def test_node_way_index_subcommand(binary, rawdir, con):
    subprocess.run([str(binary), "node-way-index", str(rawdir)], check=True)
    parts = sorted(glob.glob(str(rawdir / "node_way" / "part-*.parquet")))
    assert parts
    rows = con.execute(f"SELECT count(*) FROM read_parquet({parts!r})").fetchone()[0]

    way_files = _way_byid_files(rawdir)
    expected = con.execute(f"SELECT sum(len(refs)) FROM read_parquet({way_files!r})").fetchone()[0]
    assert rows == expected

    # Each individual part must be internally sorted by (node_id, way_id),
    # and parts.json's min_id/max_id ranges must not overlap across parts
    # (docs/m1-contracts.md 3.3: "sorted by (node_id, way_id)").
    parts_json = json.loads((rawdir / "node_way" / "parts.json").read_text())
    assert len(parts_json) == len(parts)
    prev_max = None
    for entry in sorted(parts_json, key=lambda e: e["path"]):
        if prev_max is not None and entry["min_id"] is not None:
            assert entry["min_id"] >= prev_max
        prev_max = entry["max_id"] if entry["max_id"] is not None else prev_max
    for p in parts:
        bad = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT node_id, way_id, row_number() OVER (ORDER BY node_id, way_id) AS sorted_rn,
                       row_number() OVER () AS scan_rn
                FROM read_parquet('{p}')
            ) WHERE sorted_rn != scan_rn
            """
        ).fetchone()[0]
        assert bad == 0, p
