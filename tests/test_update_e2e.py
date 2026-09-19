"""End-to-end test of the M2 updater against the Bermuda dataset
(docs/m2-contracts.md sections 2, 3, 5).

Builds Bermuda with ``--max-nodes-per-cell 20000`` through the Rust
``osmpq-raw`` producer (``build`` then the separate ``node-way-index``
pass) and ``osmpq build --raw`` (``build_from_raw``), i.e. what plain
``osmpq build <pbf> <root>`` does except for two things this test needs
that the Python ``raw-py`` producer doesn't give it:

- a real ``node_way`` index, which the updater's touched-set computation
  (docs/m2-contracts.md section 5 step 4) needs -- docs/m1-contracts.md
  section 3.3 explicitly calls it "optional in M1... the updater (M2)
  will [need it]" and only the Rust producer's separate pass builds one;
- real metadata (in particular ``version``, which a hand-written ``.osc``
  must exceed) on *untagged* nodes -- ``raw-py`` leaves it NULL by design
  (docs/m0-contracts.md section 5, "a known gap fixed by the Rust reader
  in M1"), and several of the ids this test moves/retags/deletes are
  untagged way vertices.

Applies a hand-written ``.osc`` batch (via a fake replication client
pointing at local files -- no network) covering every extent-filter and
re-resolution case from section 2, then a second batch that crosses an
UTC hour boundary to exercise the hour->day tier fold (section 5 step 7).
Delta files are read directly with DuckDB in the assertions, since the
engine's delta read support is a different agent's concurrent work.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Optional

import duckdb
import pytest

from osmpq.build.builder import BuildFromRawOptions, build_from_raw
from osmpq.layout import manifest as manifest_mod
from osmpq.update import updater as updater_mod
from osmpq.update.replication import FetchResult

REPO_ROOT = Path(__file__).resolve().parents[1]
PBF_PATH = REPO_ROOT / "data" / "bermuda-latest.osm.pbf"
BINARY_PATH = REPO_ROOT / "rust" / "osmpq-raw" / "target" / "release" / "osmpq-raw"

pytestmark = [
    pytest.mark.skipif(not PBF_PATH.exists(), reason="data/bermuda-latest.osm.pbf not present"),
    pytest.mark.skipif(not BINARY_PATH.exists(), reason="osmpq-raw binary not built (needed for the node_way index)"),
]

# ids picked from the real Bermuda extract (stable across rebuilds: they're
# OSM element ids, independent of --max-nodes-per-cell).
MOVE_NODE = 6947471281       # untagged, member of way WAY_AFFECTED_BY_MOVE
RETAG_NODE = 242149605       # untagged, member of several ways
DELETE_NODE = 242149609      # untagged, standalone-ish
EXISTING_NODE_A = 242149606
EXISTING_NODE_B = 242149607
WAY_AFFECTED_BY_MOVE = 22577455
WAY_TO_DELETE = 22576623
NEW_NODE = 900000000000101
NEW_WAY_TWO_KNOWN = 900000000000001
OUTSIDE_NODE = 900000000000200
UNKNOWN_REF_NODE = 900000000000999
NEW_WAY_PARTIAL = 900000000000002
MODIFY_REL = 1652402
MODIFY_REL_WAY_MEMBER = 120474935
DELETE_REL = 1993208


def _osmpq_raw(*args: str) -> None:
    subprocess.run([str(BINARY_PATH), *args], check=True, capture_output=True)


@pytest.fixture(scope="module")
def rawdir(tmp_path_factory) -> Path:
    d = tmp_path_factory.mktemp("bermuda-upd-raw")
    tmp = tmp_path_factory.mktemp("bermuda-upd-raw-tmp")
    _osmpq_raw("build", str(PBF_PATH), str(d), "--max-nodes-per-cell", "20000", "--tmpdir", str(tmp))
    _osmpq_raw("node-way-index", str(d))
    return d


@pytest.fixture(scope="module")
def base_versions(rawdir) -> dict[str, dict]:
    """Current version/coords/bbox of every id the synthetic batches touch,
    read directly from the raw byid parts, so the ``.osc`` we hand-write
    always carries a version greater than what's on disk."""
    con = duckdb.connect()
    node_parts = sorted(str(p) for p in (rawdir / "node").glob("part-*.parquet"))
    way_parts = sorted(str(p) for p in (rawdir / "way").glob("part-*.parquet"))
    rel_parts = sorted(str(p) for p in (rawdir / "relation").glob("part-*.parquet"))
    out: dict[str, dict] = {}
    for nid in (MOVE_NODE, RETAG_NODE, DELETE_NODE, EXISTING_NODE_A, EXISTING_NODE_B):
        row = con.execute(
            f"SELECT version, lat_e7, lon_e7 FROM read_parquet({node_parts!r}) WHERE id = {nid}"
        ).fetchone()
        out[f"node:{nid}"] = {"version": row[0], "lat_e7": row[1], "lon_e7": row[2]}
    for wid in (WAY_AFFECTED_BY_MOVE, WAY_TO_DELETE):
        row = con.execute(f"SELECT version FROM read_parquet({way_parts!r}) WHERE id = {wid}").fetchone()
        out[f"way:{wid}"] = {"version": row[0]}
    for rid in (MODIFY_REL, DELETE_REL):
        row = con.execute(f"SELECT version FROM read_parquet({rel_parts!r}) WHERE id = {rid}").fetchone()
        out[f"rel:{rid}"] = {"version": row[0]}
    return out


@pytest.fixture(scope="module")
def root(tmp_path_factory, rawdir) -> Path:
    d = tmp_path_factory.mktemp("bermuda-upd-root")
    tmp = tmp_path_factory.mktemp("bermuda-upd-build-tmp")
    build_from_raw(BuildFromRawOptions(rawdir=str(rawdir), root=str(d), tmpdir=str(tmp)))
    # Seed replication_sequence=0 and a source so the first `update` starts
    # at sequence 1; manifest stays v2 until the first update writes v3.
    man = manifest_mod.load_latest(str(d))
    man.replication_sequence = 0
    man.replication_source = "https://fake.example/repl"
    manifest_mod.write_manifest(str(d), man, 1)
    return d


def _osc(body: str) -> str:
    return f'<osmChange version="0.6" generator="test">\n{body}\n</osmChange>\n'


@pytest.fixture(scope="module")
def batch1_osc(tmp_path_factory, base_versions) -> Path:
    v = base_versions
    body = f"""
<create>
<node id="{NEW_NODE}" version="1" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001" lat="32.3000000" lon="-64.7000000"/>
<node id="{OUTSIDE_NODE}" version="1" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001" lat="0.0000000" lon="0.0000000"/>
<way id="{NEW_WAY_TWO_KNOWN}" version="1" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001">
<nd ref="{EXISTING_NODE_A}"/>
<nd ref="{NEW_NODE}"/>
<nd ref="{EXISTING_NODE_B}"/>
<tag k="name" v="NewWay"/>
</way>
<way id="{NEW_WAY_PARTIAL}" version="1" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001">
<nd ref="{UNKNOWN_REF_NODE}"/>
<nd ref="{EXISTING_NODE_B}"/>
<tag k="name" v="PartialWay"/>
</way>
</create>
<modify>
<node id="{MOVE_NODE}" version="{v[f'node:{MOVE_NODE}']['version'] + 1}" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001" lat="25.0000000" lon="-70.0000000"/>
<node id="{RETAG_NODE}" version="{v[f'node:{RETAG_NODE}']['version'] + 1}" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001" lat="{v[f'node:{RETAG_NODE}']['lat_e7'] / 1e7:.7f}" lon="{v[f'node:{RETAG_NODE}']['lon_e7'] / 1e7:.7f}">
<tag k="amenity" v="cafe"/>
</node>
<relation id="{MODIFY_REL}" version="{v[f'rel:{MODIFY_REL}']['version'] + 1}" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001">
<member type="way" ref="{MODIFY_REL_WAY_MEMBER}" role="forward"/>
<member type="node" ref="{EXISTING_NODE_B}" role="label"/>
</relation>
</modify>
<delete>
<node id="{DELETE_NODE}" version="{v[f'node:{DELETE_NODE}']['version'] + 1}" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001" lat="{v[f'node:{DELETE_NODE}']['lat_e7'] / 1e7:.7f}" lon="{v[f'node:{DELETE_NODE}']['lon_e7'] / 1e7:.7f}"/>
<way id="{WAY_TO_DELETE}" version="{v[f'way:{WAY_TO_DELETE}']['version'] + 1}" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001"/>
<relation id="{DELETE_REL}" version="{v[f'rel:{DELETE_REL}']['version'] + 1}" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001"/>
</delete>
"""
    p = tmp_path_factory.mktemp("osc-fixtures") / "batch1.osc"
    p.write_text(_osc(body))
    return p


@pytest.fixture(scope="module")
def batch2_osc(tmp_path_factory) -> Path:
    body = """
<modify>
<node id="242149608" version="6" timestamp="2026-01-01T11:05:00Z" uid="1" user="tester" changeset="1002" lat="32.2614118" lon="-64.8259646">
<tag k="amenity" v="bench"/>
</node>
</modify>
"""
    p = tmp_path_factory.mktemp("osc-fixtures2") / "batch2.osc"
    p.write_text(_osc(body))
    return p


class _FakeClient:
    """Stands in for ``ReplicationClient``: serves local ``.osc`` files by
    sequence number instead of an HTTP source (docs/m2-contracts.md section 5
    is silent on the source's transport, and this test has none)."""

    files: dict[int, tuple[Path, str]] = {}

    def __init__(self, source: str, **kwargs) -> None:
        self.source = source

    def __enter__(self) -> "_FakeClient":
        return self

    def __exit__(self, *exc) -> None:
        return None

    def fetch_range(self, from_seq: int, max_diffs: int, dest_dir: Path) -> list[FetchResult]:
        out = []
        seq = from_seq
        while seq in self.files and len(out) < max_diffs:
            path, ts = self.files[seq]
            out.append(FetchResult(seq=seq, osc_path=path, state_path=Path("/dev/null"), timestamp=ts))
            seq += 1
        return out


@pytest.fixture(autouse=True, scope="module")
def _patch_replication_client(batch1_osc, batch2_osc):
    _FakeClient.files = {
        1: (batch1_osc, "2026-01-01T10:30:00Z"),
        2: (batch2_osc, "2026-01-01T11:05:00Z"),
    }
    original = updater_mod.ReplicationClient
    updater_mod.ReplicationClient = _FakeClient
    yield
    updater_mod.ReplicationClient = original


@pytest.fixture(scope="module")
def run1(tmp_path_factory, root):
    opts = updater_mod.UpdateOptions(
        root=str(root), source="https://fake.example/repl", max_diffs=1,
        tmpdir=str(tmp_path_factory.mktemp("update-tmp-1")),
    )
    return updater_mod.run_once(opts)


@pytest.fixture(scope="module")
def run2(tmp_path_factory, root, run1):
    opts = updater_mod.UpdateOptions(
        root=str(root), source="https://fake.example/repl", max_diffs=1,
        tmpdir=str(tmp_path_factory.mktemp("update-tmp-2")),
    )
    return updater_mod.run_once(opts)


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    return con


def _rows(con, path: str, cols: str = "*", where: Optional[str] = None) -> list[dict]:
    sql = f"SELECT {cols} FROM read_parquet('{path}')"
    if where:
        sql += f" WHERE {where}"
    r = con.execute(sql)
    names = [c[0] for c in r.description]
    return [dict(zip(names, row)) for row in r.fetchall()]


# --------------------------------------------------------------------------
# manifest v3 / run summary
# --------------------------------------------------------------------------


def test_first_run_upgrades_manifest_to_v3(root, run1):
    man = manifest_mod.load_latest(str(root))
    assert man.manifest_version == 4  # built roots are v4 (areas) since M3; deltas are additive
    assert man.replication_source == "https://fake.example/repl"
    assert man.replication_sequence == 1
    assert man.timestamp_osm_base == "2026-01-01T10:30:00Z"
    assert "hour" in man.deltas
    assert man.deltas["hour"]["version"] == 1
    assert man.deltas["hour"]["seq_from"] == 1
    assert man.deltas["hour"]["seq_to"] == 1
    cells = man.deltas["hour"]["cells"]
    assert set(cells.keys()) == {"node", "way", "relation"}
    assert cells["node"] == sorted(cells["node"])  # sorted cell keys
    assert len(cells["node"]) > 0 and len(cells["way"]) > 0 and len(cells["relation"]) > 0


def test_run1_summary(run1):
    assert run1.applied == 1
    assert run1.first_seq == 1 and run1.last_seq == 1
    assert run1.dropped["node"] == 1  # OUTSIDE_NODE


# --------------------------------------------------------------------------
# extent filter (docs/m2-contracts.md section 2)
# --------------------------------------------------------------------------


def _hour1_dir(root: Path) -> Path:
    return root / "delta" / "g0001" / "hour" / "1"


def test_outside_extent_node_dropped(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "node.byid.parquet"), where=f"id = {OUTSIDE_NODE}")
    assert rows == []


def test_new_way_two_known_one_new_node_kept_with_full_geometry(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "way.byid.parquet"), where=f"id = {NEW_WAY_TWO_KNOWN}")
    assert len(rows) == 1
    row = rows[0]
    assert row["deleted"] is False
    assert row["prev_cell"] is None  # brand new
    assert row["refs"] == [EXISTING_NODE_A, NEW_NODE, EXISTING_NODE_B]
    spatial = _rows(con, str(d / "way.spatial.parquet"), cols="ST_AsText(geometry) AS g", where=f"id = {NEW_WAY_TWO_KNOWN}")
    assert spatial[0]["g"] is not None  # 3 resolved nodes -> a real LINESTRING


def test_way_with_unknown_and_known_node_kept_with_partial_geometry(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "way.byid.parquet"), where=f"id = {NEW_WAY_PARTIAL}")
    assert len(rows) == 1
    assert rows[0]["refs"] == [UNKNOWN_REF_NODE, EXISTING_NODE_B]
    # only 1 of 2 refs resolves -> bbox from that one node, geometry NULL
    assert rows[0]["xmin_e7"] == rows[0]["xmax_e7"]
    assert rows[0]["ymin_e7"] == rows[0]["ymax_e7"]
    spatial = _rows(con, str(d / "way.spatial.parquet"), cols="ST_AsText(geometry) AS g", where=f"id = {NEW_WAY_PARTIAL}")
    assert spatial[0]["g"] is None


# --------------------------------------------------------------------------
# re-resolution: moved node, retag, deletes (docs/m2-contracts.md section 5)
# --------------------------------------------------------------------------


def test_moved_node_row_and_tombstone(root, run1, base_versions):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "node.byid.parquet"), where=f"id = {MOVE_NODE}")
    assert len(rows) == 1
    row = rows[0]
    assert row["deleted"] is False
    assert row["lat_e7"] == 250000000 and row["lon_e7"] == -700000000
    old = base_versions[f"node:{MOVE_NODE}"]
    assert row["prev_cell"] is not None
    # the node actually changed cell (moved far away): a tombstone must
    # shadow the old cell.
    tomb = _rows(con, str(d / "tombstones.parquet"), where=f"type='node' AND id = {MOVE_NODE}")
    assert len(tomb) == 1
    assert tomb[0]["prev_cell"] == row["prev_cell"]


def test_moved_node_parent_way_bbox_changed(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "way.byid.parquet"), where=f"id = {WAY_AFFECTED_BY_MOVE}")
    assert len(rows) == 1
    row = rows[0]
    assert row["deleted"] is False
    # the way's bbox must now cover the node's new (far away) location
    assert row["xmin_e7"] <= -700000000 <= row["xmax_e7"]
    assert row["ymin_e7"] <= 250000000 <= row["ymax_e7"]
    # its cell changed too (bbox grew a lot) -> tombstoned
    tomb = _rows(con, str(d / "tombstones.parquet"), where=f"type='way' AND id = {WAY_AFFECTED_BY_MOVE}")
    assert len(tomb) == 1


def test_retagged_node_kept_cell_unchanged_no_tombstone(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "node.byid.parquet"), where=f"id = {RETAG_NODE}")
    assert len(rows) == 1
    row = rows[0]
    assert row["amenity"] == "cafe"
    assert row["prev_cell"] == row["cell"]  # didn't move
    tomb = _rows(con, str(d / "tombstones.parquet"), where=f"type='node' AND id = {RETAG_NODE}")
    assert tomb == []  # unchanged cell -> no tombstone needed


def test_deleted_way_tombstoned_cell_equals_prev_cell_payload_null(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "way.byid.parquet"), where=f"id = {WAY_TO_DELETE}")
    assert len(rows) == 1
    row = rows[0]
    assert row["deleted"] is True
    assert row["prev_cell"] is not None
    for col in ("refs", "tags", "version", "xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7", "is_closed", "is_area"):
        assert row[col] is None, f"{col} should be NULL on a deleted row"
    spatial = _rows(con, str(d / "way.spatial.parquet"), where=f"id = {WAY_TO_DELETE}")
    assert spatial[0]["cell"] == row["prev_cell"]
    assert spatial[0]["hilbert"] is None
    tomb = _rows(con, str(d / "tombstones.parquet"), where=f"type='way' AND id = {WAY_TO_DELETE}")
    assert len(tomb) == 1 and tomb[0]["prev_cell"] == row["prev_cell"]


def test_deleted_node_row(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "node.byid.parquet"), where=f"id = {DELETE_NODE}")
    assert len(rows) == 1
    assert rows[0]["deleted"] is True
    assert rows[0]["lat_e7"] is None and rows[0]["tags"] is None


def test_deleted_relation_row(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "relation.byid.parquet"), where=f"id = {DELETE_REL}")
    assert len(rows) == 1
    assert rows[0]["deleted"] is True
    assert rows[0]["members"] is None
    tomb = _rows(con, str(d / "tombstones.parquet"), where=f"type='relation' AND id = {DELETE_REL}")
    assert len(tomb) == 1


def test_modified_relation_members_updated(root, run1):
    con = _con()
    d = _hour1_dir(root)
    rows = _rows(con, str(d / "relation.byid.parquet"), where=f"id = {MODIFY_REL}")
    assert len(rows) == 1
    members = rows[0]["members"]
    assert {"type": "w", "ref": MODIFY_REL_WAY_MEMBER, "role": "forward"} in members
    assert {"type": "n", "ref": EXISTING_NODE_B, "role": "label"} in members


# --------------------------------------------------------------------------
# one row per (type, id); files sorted correctly
# --------------------------------------------------------------------------


def test_one_row_per_id_in_delta_files(root, run1):
    con = _con()
    d = _hour1_dir(root)
    for typ in ("node", "way", "relation"):
        n_total = con.execute(f"SELECT count(*) FROM read_parquet('{d / f'{typ}.byid.parquet'}')").fetchone()[0]
        n_distinct = con.execute(f"SELECT count(DISTINCT id) FROM read_parquet('{d / f'{typ}.byid.parquet'}')").fetchone()[0]
        assert n_total == n_distinct, typ


def test_spatial_files_sorted_cell_hilbert_id(root, run1):
    con = _con()
    d = _hour1_dir(root)
    for typ in ("node", "way", "relation"):
        path = str(d / f"{typ}.spatial.parquet")
        ordered = con.execute(f"SELECT cell, hilbert, id FROM read_parquet('{path}') ORDER BY cell, hilbert, id").fetchall()
        natural = con.execute(f"SELECT cell, hilbert, id FROM read_parquet('{path}')").fetchall()
        assert ordered == natural, typ


def test_byid_files_sorted_by_id(root, run1):
    con = _con()
    d = _hour1_dir(root)
    for typ in ("node", "way", "relation"):
        path = str(d / f"{typ}.byid.parquet")
        ordered = con.execute(f"SELECT id FROM read_parquet('{path}') ORDER BY id").fetchall()
        natural = con.execute(f"SELECT id FROM read_parquet('{path}')").fetchall()
        assert ordered == natural, typ


def test_tombstones_sorted_by_prev_cell_type_id(root, run1):
    con = _con()
    d = _hour1_dir(root)
    path = str(d / "tombstones.parquet")
    ordered = con.execute(f"SELECT prev_cell, type, id FROM read_parquet('{path}') ORDER BY prev_cell, type, id").fetchall()
    natural = con.execute(f"SELECT prev_cell, type, id FROM read_parquet('{path}')").fetchall()
    assert ordered == natural


def test_way_native_geometry_type(root, run1):
    con = _con()
    d = _hour1_dir(root)
    described = dict((c[0], c[1]) for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{d / 'way.spatial.parquet'}')").fetchall())
    assert "GEOMETRY" in described["geometry"]


# --------------------------------------------------------------------------
# second batch: hour -> day fold on a UTC hour boundary crossing
# --------------------------------------------------------------------------


def test_second_batch_folds_hour_into_day(root, run1, run2):
    man = manifest_mod.load_latest(str(root))
    assert man.replication_sequence == 2
    assert man.timestamp_osm_base == "2026-01-01T11:05:00Z"
    assert "day" in man.deltas
    assert man.deltas["day"]["version"] == 1
    assert man.deltas["day"]["seq_from"] == 1 and man.deltas["day"]["seq_to"] == 1
    assert man.deltas["hour"]["version"] == 2
    assert man.deltas["hour"]["seq_from"] == 2 and man.deltas["hour"]["seq_to"] == 2

    con = _con()
    day_dir = root / "delta" / "g0001" / "day" / "1"
    hour_dir = root / "delta" / "g0001" / "hour" / "2"
    # everything from the first hour is now reachable through `day`
    day_node_ids = {r["id"] for r in _rows(con, str(day_dir / "node.byid.parquet"))}
    assert MOVE_NODE in day_node_ids and RETAG_NODE in day_node_ids and NEW_NODE in day_node_ids
    # the new hour tier holds only the second batch's node
    hour_node_ids = {r["id"] for r in _rows(con, str(hour_dir / "node.byid.parquet"))}
    assert hour_node_ids == {242149608}


def test_day_tier_populated_and_hour_tier_reset(run2):
    assert run2.tier_versions.get("day") == 1
    assert run2.tier_versions.get("hour") == 2
