"""M4 updater history-tier append, docs/m4-contracts.md section 5.1.

Builds a v5 (history-carrying) root from the M0 fixture
(``tests/fixtures/history_v5.py``: the fixture's current rows as their
``minor=0`` state at ``since``), then applies three hand-written ``.osc``
batches through the real updater (``osmpq.update.updater.run_once``, a fake
``ReplicationClient`` serving local files -- same pattern as
``tests/test_update_e2e.py``/``tests/test_updater_s3.py``):

- run 1: a node that owns no way (``move_node_id``) moves to a different
  leaf cell (own-version history row + move tombstone in the old cell); a
  way's *member* node (``way_member_node_id``) moves to a different leaf
  cell without the way's own version changing (a minor version on
  ``minor_way_id``, with its own move tombstone since the way's bbox now
  spans two cells); a standalone node and an unrelated way are deleted
  (deletion tombstones).
- run 2: a further, position-preserving tag edit on a different node,
  within the same UTC hour as run 1 -- the hour tier must simply grow
  (append), not fold.
- run 3: a timestamp in the next UTC hour -- the hour tier folds into
  `day` (the same boundary the current-state delta tiers fold at), and a
  fresh hour tier holds only run 3's own row.

Tier files are read directly with DuckDB (docs/m4-contracts.md section 3's
engine read path is a different, concurrent workstream).
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import history_v5  # noqa: E402

from osmpq.layout import manifest as manifest_mod  # noqa: E402
from osmpq.update import updater as updater_mod  # noqa: E402
from osmpq.update.replication import FetchResult  # noqa: E402


def _osc(body: str) -> str:
    return f'<osmChange version="0.6" generator="test">\n{body}\n</osmChange>\n'


class _FakeClient:
    files: dict[int, tuple[Path, str]] = {}

    def __init__(self, source: str, **kwargs) -> None:
        self.source = source

    def __enter__(self) -> "_FakeClient":
        return self

    def __exit__(self, *exc) -> None:
        return None

    def fetch_range(self, from_seq: int, max_diffs: int, dest_dir: Path):
        out = []
        seq = from_seq
        while seq in self.files and len(out) < max_diffs:
            path, ts = self.files[seq]
            out.append(FetchResult(seq=seq, osc_path=path, state_path=Path("/dev/null"), timestamp=ts))
            seq += 1
        return out


@pytest.fixture(scope="module")
def hinfo(tmp_path_factory) -> history_v5.HistoryFixtureInfo:
    root = tmp_path_factory.mktemp("history-update-root") / "root"
    return history_v5.build(str(root))


@pytest.fixture(scope="module")
def batch1_osc(tmp_path_factory, hinfo) -> Path:
    lat_a, lon_a = history_v5.HistoryFixtureInfo.leaf001_point_a
    lat_b, lon_b = history_v5.HistoryFixtureInfo.leaf001_point_b
    body = f"""
<modify>
<node id="{hinfo.way_member_node_id}" version="{hinfo.way_member_node_version + 1}" timestamp="2026-09-19T01:15:00Z" uid="1" user="tester" changeset="1001" lat="{lat_a:.7f}" lon="{lon_a:.7f}"/>
<node id="{hinfo.move_node_id}" version="{hinfo.move_node_version + 1}" timestamp="2026-09-19T01:15:00Z" uid="1" user="tester" changeset="1001" lat="{lat_b:.7f}" lon="{lon_b:.7f}">
<tag k="name" v="Coffee HOUSE moved"/>
</node>
</modify>
<delete>
<node id="{hinfo.delete_node_id}" version="{hinfo.delete_node_version + 1}" timestamp="2026-09-19T01:15:00Z" uid="1" user="tester" changeset="1001"/>
<way id="{hinfo.delete_way_id}" version="{hinfo.delete_way_version + 1}" timestamp="2026-09-19T01:15:00Z" uid="1" user="tester" changeset="1001"/>
</delete>
"""
    p = tmp_path_factory.mktemp("history-osc") / "batch1.osc"
    p.write_text(_osc(body))
    return p


@pytest.fixture(scope="module")
def batch2_osc(tmp_path_factory, hinfo) -> Path:
    body = """
<modify>
<node id="1" version="99001" timestamp="2026-09-19T01:45:00Z" uid="1" user="tester" changeset="1002" lat="99.0" lon="99.0">
<tag k="amenity" v="cafe"/>
<tag k="name" v="Aroma Cafe (renamed run2)"/>
</node>
</modify>
"""
    p = tmp_path_factory.mktemp("history-osc2") / "batch2.osc"
    p.write_text(_osc(body))
    return p


@pytest.fixture(scope="module")
def batch3_osc(tmp_path_factory, hinfo) -> Path:
    body = """
<modify>
<node id="1" version="99002" timestamp="2026-09-19T02:05:00Z" uid="1" user="tester" changeset="1003" lat="99.0" lon="99.0">
<tag k="amenity" v="cafe"/>
<tag k="name" v="Aroma Cafe (renamed run3)"/>
</node>
</modify>
"""
    p = tmp_path_factory.mktemp("history-osc3") / "batch3.osc"
    p.write_text(_osc(body))
    return p


@pytest.fixture(autouse=True, scope="module")
def _fix_node1_versions(hinfo, batch2_osc, batch3_osc):
    """The literal ``version="99001"``/``"99002"`` and the placeholder
    ``lat="99.0" lon="99.0"`` above stand in for ``node 1``'s real current
    version/position, patched in once the fixture (and its real values)
    exist -- keeps ``node 1`` in the *same place* across runs 2-3 (a
    tag-only edit) so ``minor_way_id`` (which lists it as a member) is not
    also given a spurious minor version by these two runs."""
    con = duckdb.connect()
    root = Path(hinfo.root)
    man = manifest_mod.load_latest(str(root))
    node_byid_paths = [str(root / p["path"]) for p in man.byid["node"]]
    v, lat_e7, lon_e7 = con.execute(
        f"SELECT version, lat_e7, lon_e7 FROM read_parquet({node_byid_paths!r}) WHERE id = 1"
    ).fetchone()
    lat, lon = lat_e7 / 1e7, lon_e7 / 1e7
    for p, dv in ((batch2_osc, 1), (batch3_osc, 2)):
        text = p.read_text()
        text = text.replace('version="99001"' if dv == 1 else 'version="99002"', f'version="{v + dv}"')
        text = text.replace('lat="99.0" lon="99.0"', f'lat="{lat:.7f}" lon="{lon:.7f}"')
        p.write_text(text)
    con.close()


@pytest.fixture(autouse=True, scope="module")
def _patch_replication_client(batch1_osc, batch2_osc, batch3_osc):
    _FakeClient.files = {
        1: (batch1_osc, "2026-09-19T01:15:00Z"),
        2: (batch2_osc, "2026-09-19T01:45:00Z"),
        3: (batch3_osc, "2026-09-19T02:05:00Z"),
    }
    original = updater_mod.ReplicationClient
    updater_mod.ReplicationClient = _FakeClient
    yield
    updater_mod.ReplicationClient = original


@pytest.fixture(scope="module")
def run1(tmp_path_factory, hinfo):
    opts = updater_mod.UpdateOptions(
        root=hinfo.root, source="https://fake.example/repl", max_diffs=1,
        tmpdir=str(tmp_path_factory.mktemp("history-update-tmp-1")),
    )
    return updater_mod.run_once(opts)


@pytest.fixture(scope="module")
def run2(tmp_path_factory, hinfo, run1):
    opts = updater_mod.UpdateOptions(
        root=hinfo.root, source="https://fake.example/repl", max_diffs=1,
        tmpdir=str(tmp_path_factory.mktemp("history-update-tmp-2")),
    )
    return updater_mod.run_once(opts)


@pytest.fixture(scope="module")
def run3(tmp_path_factory, hinfo, run2):
    opts = updater_mod.UpdateOptions(
        root=hinfo.root, source="https://fake.example/repl", max_diffs=1,
        tmpdir=str(tmp_path_factory.mktemp("history-update-tmp-3")),
    )
    return updater_mod.run_once(opts)


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


def _rows(con, path: str, cols: str = "*", where: str | None = None, order: str | None = None) -> list[dict]:
    sql = f"SELECT {cols} FROM read_parquet('{path}')"
    if where:
        sql += f" WHERE {where}"
    if order:
        sql += f" ORDER BY {order}"
    r = con.execute(sql)
    names = [c[0] for c in r.description]
    return [dict(zip(names, row)) for row in r.fetchall()]


def _hour1_files(hinfo) -> dict:
    man = manifest_mod.load_latest(hinfo.root)
    return man.history["tiers"]["hour"]["files"]


# --------------------------------------------------------------------------
# manifest / tier bookkeeping
# --------------------------------------------------------------------------


def test_run1_writes_history_manifest_v5_and_hour_tier(hinfo, run1):
    man = manifest_mod.load_latest(hinfo.root)
    assert man.manifest_version == 5
    assert man.history is not None
    assert man.history["since"] == hinfo.since
    hour = man.history["tiers"]["hour"]
    assert hour["version"] == 1
    assert hour["seq_from"] == 1 and hour["seq_to"] == 1
    assert "day" not in man.history["tiers"]
    assert hour["rows"]["node"] == 3  # move_node, way_member_node, delete_node
    assert hour["rows"]["way"] == 2   # minor_way_id, delete_way_id
    assert run1.history_tier_versions == {"hour": 1}


def test_run1_summary_reports_history_tiers(run1):
    assert run1.history_tier_versions.get("hour") == 1
    assert run1.history_tier_bytes.get("hour", 0) > 0


# --------------------------------------------------------------------------
# own-version move: history row + move tombstone (section 2.1)
# --------------------------------------------------------------------------


def test_move_node_own_version_row_and_tombstone(hinfo, run1):
    con = _con()
    files = _hour1_files(hinfo)
    byid_root = files["node"]["byid"]
    spatial_root = files["node"]["spatial"]

    byid_rows = _rows(con, str(Path(hinfo.root) / byid_root), where=f"id = {hinfo.move_node_id}")
    assert len(byid_rows) == 1
    row = byid_rows[0]
    assert row["version"] == hinfo.move_node_version + 1
    assert row["minor"] == 0
    assert row["visible"] is True
    assert row["valid_to"] is None
    assert row["tags"]["name"] == "Coffee HOUSE moved"

    spatial_rows = _rows(con, str(Path(hinfo.root) / spatial_root), where=f"id = {hinfo.move_node_id}", order="valid_from")
    assert len(spatial_rows) == 2  # the new state + the old cell's move tombstone
    visible_row = next(r for r in spatial_rows if r["visible"])
    tomb_row = next(r for r in spatial_rows if not r["visible"])
    assert visible_row["cell"] != tomb_row["cell"]
    assert tomb_row["version"] == row["version"]
    assert tomb_row["minor"] == 0
    # a move tombstone is a pure marker: no payload columns.
    assert tomb_row["tags"] is None and tomb_row["lat_e7"] is None and tomb_row["lon_e7"] is None


# --------------------------------------------------------------------------
# minor version: a way whose member node moved (section 4.1/5.1)
# --------------------------------------------------------------------------


def test_minor_way_gets_minor_version_and_move_tombstone(hinfo, run1):
    con = _con()
    files = _hour1_files(hinfo)
    byid_rows = _rows(con, str(Path(hinfo.root) / files["way"]["byid"]), where=f"id = {hinfo.minor_way_id}")
    assert len(byid_rows) == 1
    row = byid_rows[0]
    # the way's OWN version is unchanged; only a minor state was added.
    assert row["version"] == hinfo.minor_way_version
    assert row["minor"] == 1
    assert row["visible"] is True
    assert row["valid_to"] is None

    spatial_rows = _rows(con, str(Path(hinfo.root) / files["way"]["spatial"]), where=f"id = {hinfo.minor_way_id}")
    assert len(spatial_rows) == 2  # the new (minor) state + a move tombstone at the old cell
    visible_row = next(r for r in spatial_rows if r["visible"])
    tomb_row = next(r for r in spatial_rows if not r["visible"])
    assert visible_row["minor"] == 1 and visible_row["version"] == hinfo.minor_way_version
    assert tomb_row["minor"] == 1 and tomb_row["version"] == hinfo.minor_way_version
    assert visible_row["cell"] != tomb_row["cell"]


# --------------------------------------------------------------------------
# deletion tombstones (section 2.1)
# --------------------------------------------------------------------------


def test_deleted_node_tombstone(hinfo, run1):
    con = _con()
    files = _hour1_files(hinfo)
    rows = _rows(con, str(Path(hinfo.root) / files["node"]["byid"]), where=f"id = {hinfo.delete_node_id}")
    assert len(rows) == 1
    row = rows[0]
    assert row["visible"] is False
    assert row["version"] == hinfo.delete_node_version + 1
    assert row["minor"] == 0
    assert row["lat_e7"] is None and row["tags"] is None
    assert row["cell"] is not None  # the cell of the previous (pre-delete) state

    spatial_rows = _rows(con, str(Path(hinfo.root) / files["node"]["spatial"]), where=f"id = {hinfo.delete_node_id}")
    # a deletion's cell already equals its own prev_cell -- no *extra* move
    # tombstone row on top of the one state row.
    assert len(spatial_rows) == 1
    assert spatial_rows[0]["visible"] is False


def test_deleted_way_tombstone(hinfo, run1):
    con = _con()
    files = _hour1_files(hinfo)
    rows = _rows(con, str(Path(hinfo.root) / files["way"]["byid"]), where=f"id = {hinfo.delete_way_id}")
    assert len(rows) == 1
    row = rows[0]
    assert row["visible"] is False
    assert row["version"] == hinfo.delete_way_version + 1
    for col in ("refs", "tags", "xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7"):
        assert row[col] is None, col


# --------------------------------------------------------------------------
# run 2: same-hour append (no fold)
# --------------------------------------------------------------------------


def test_run2_appends_to_the_same_hour_tier(hinfo, run1, run2):
    man = manifest_mod.load_latest(hinfo.root)
    hour = man.history["tiers"]["hour"]
    assert hour["version"] == 2
    assert hour["seq_from"] == 1 and hour["seq_to"] == 2  # first run's rows are still in the tier
    assert "day" not in man.history["tiers"]

    con = _con()
    byid_rows = _rows(con, str(Path(hinfo.root) / hour["files"]["node"]["byid"]), where=f"id IN ({hinfo.move_node_id}, 1)")
    ids_present = {r["id"] for r in byid_rows}
    assert hinfo.move_node_id in ids_present and 1 in ids_present  # run1's row is still here (appended, not replaced)


# --------------------------------------------------------------------------
# run 3: crosses the UTC hour boundary -> fold hour into day
# --------------------------------------------------------------------------


def test_run3_folds_hour_into_day(hinfo, run1, run2, run3):
    man = manifest_mod.load_latest(hinfo.root)
    assert "day" in man.history["tiers"]
    day = man.history["tiers"]["day"]
    hour = man.history["tiers"]["hour"]
    assert day["version"] == 1
    assert hour["version"] == 3

    con = _con()
    # everything from runs 1-2 is now reachable through `day`.
    day_node_ids = {r["id"] for r in _rows(con, str(Path(hinfo.root) / day["files"]["node"]["byid"]))}
    assert hinfo.move_node_id in day_node_ids
    assert hinfo.way_member_node_id in day_node_ids
    assert 1 in day_node_ids

    # the new hour tier holds only run 3's own row (node 1's third state).
    hour_node_rows = _rows(con, str(Path(hinfo.root) / hour["files"]["node"]["byid"]))
    assert {r["id"] for r in hour_node_rows} == {1}
    assert run3.history_tier_versions == {"hour": 3, "day": 1}


def test_history_byid_never_carries_move_tombstones(hinfo, run1, run2, run3):
    """Section 2.1: "the byid copy has no move tombstones (it is not
    cell-scoped) but does have deletion rows." -- every byid row for the
    moved node/way is visible=True except the genuine deletions, across
    every tier this run touched."""
    man = manifest_mod.load_latest(hinfo.root)
    con = _con()
    for tier_name in ("hour", "day"):
        tier = man.history["tiers"].get(tier_name)
        if not tier:
            continue
        node_rows = _rows(con, str(Path(hinfo.root) / tier["files"]["node"]["byid"]),
                           where=f"id IN ({hinfo.move_node_id}, {hinfo.way_member_node_id})")
        assert all(r["visible"] for r in node_rows)


# --------------------------------------------------------------------------
# s3:// root (docs/m4-contracts.md section 5.1: "the s3:// root path must
# work for tier files -- tier files go through the store like delta tier
# files"), same style as tests/test_updater_s3.py.
# --------------------------------------------------------------------------


def test_upload_new_history_tier_files_pushes_every_file_of_a_written_tier(tmp_path):
    write_root = tmp_path / "staging"
    tier_dir = write_root / "history" / "g0001" / "tier" / "hour" / "1"
    tier_dir.mkdir(parents=True)
    for name in ("node.byid.parquet", "node.spatial.parquet"):
        (tier_dir / name).write_bytes(b"x")

    new_history = {
        "tiers": {
            "hour": {
                "files": {
                    "node": {
                        "byid": "history/g0001/tier/hour/1/node.byid.parquet",
                        "spatial": "history/g0001/tier/hour/1/node.spatial.parquet",
                    },
                    "way": {}, "relation": {},
                }
            },
            # an untouched tier this run didn't (re)write -- must NOT upload.
            "day": {"files": {"node": {"byid": "history/g0001/tier/day/3/node.byid.parquet"}}},
        }
    }
    tier_versions = {"hour": 1}

    class _RecordingStore:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def upload_file(self, local_path: str, rel: str) -> None:
            self.calls.append((local_path, rel))

    store = _RecordingStore()
    updater_mod._upload_new_history_tier_files(store, write_root, new_history, tier_versions)

    uploaded = {rel for _local, rel in store.calls}
    assert uploaded == {
        "history/g0001/tier/hour/1/node.byid.parquet",
        "history/g0001/tier/hour/1/node.spatial.parquet",
    }


def test_run_once_through_s3_root_writes_history_tiers(tmp_path, monkeypatch):
    """Same trick as ``test_updater_s3.py``'s ``test_run_once_picks_a_local_
    staging_dir_for_an_s3_root``: patch ``store_mod.for_root`` to hand back a
    ``LocalStore`` over a real (freshly built, independent of the module's
    shared ``hinfo``) fixture root -- so DuckDB can actually read it -- while
    ``opts.root`` is still an ``s3://`` URL string, and confirm the run
    stages history tier files under ``--tmpdir`` and then uploads (here:
    ``LocalStore.upload_file``, a plain copy) them to the real root."""
    from osmpq import store as store_mod

    root2 = history_v5.build(str(tmp_path / "s3-root"))
    lat_b, lon_b = history_v5.HistoryFixtureInfo.leaf001_point_b
    body = f"""
<modify>
<node id="{root2.move_node_id}" version="{root2.move_node_version + 1}" timestamp="2026-09-19T01:15:00Z" uid="1" user="tester" changeset="2001" lat="{lat_b:.7f}" lon="{lon_b:.7f}"/>
</modify>
"""
    osc_path = tmp_path / "s3-batch1.osc"
    osc_path.write_text(_osc(body))
    _FakeClient.files = {1: (osc_path, "2026-09-19T01:15:00Z")}

    local_store_over_root = store_mod.LocalStore(root2.root)
    monkeypatch.setattr(store_mod, "for_root", lambda root_str: local_store_over_root)

    tmpdir = tmp_path / "s3-update-tmp"
    opts = updater_mod.UpdateOptions(
        root="s3://fake-bucket/does-not-matter", source="https://fake.example/repl", max_diffs=1,
        tmpdir=str(tmpdir),
    )
    summary = updater_mod.run_once(opts)
    assert summary is not None and summary.applied == 1
    assert summary.history_tier_versions == {"hour": 1}

    staging = tmpdir / "staging"
    staged_parquet = list(staging.rglob("*.parquet"))
    assert any("history" in p.parts for p in staged_parquet)

    man = manifest_mod.load_latest(root2.root)
    hour = man.history["tiers"]["hour"]
    assert (Path(root2.root) / hour["files"]["node"]["byid"]).exists()


# --------------------------------------------------------------------------
# integration: the engine's attic reads over the tiers the updater wrote
# (docs/m4-contracts.md sections 3.1-3.2 on section 5.1's output)
# --------------------------------------------------------------------------


def _engine(hinfo):
    from osmpq.engine.executor import Engine

    return Engine(hinfo.root)


def _names_at(engine, node_id: int, date: str) -> list[str]:
    r = engine.run(f'[out:json][date:"{date}"];node({node_id});out meta;')
    assert r.remark is None or r.remark.startswith("history starts at ")
    return [e.get("tags", {}).get("name") for e in r.elements]


def test_engine_date_reads_updater_tiers(hinfo, run3):
    eng = _engine(hinfo)
    before = _names_at(eng, 1, "2026-09-19T01:00:00Z")
    at_run2 = _names_at(eng, 1, "2026-09-19T01:50:00Z")
    at_run3 = _names_at(eng, 1, "2026-09-19T02:10:00Z")
    assert len(before) == 1 and "renamed" not in (before[0] or "")
    assert at_run2 == ["Aroma Cafe (renamed run2)"]
    assert at_run3 == ["Aroma Cafe (renamed run3)"]
    # exactly at valid_from the new state is already visible
    assert _names_at(eng, 1, "2026-09-19T01:45:00Z") == ["Aroma Cafe (renamed run2)"]


def test_engine_date_sees_move_and_deletion(hinfo, run3):
    eng = _engine(hinfo)
    lat_b, lon_b = history_v5.HistoryFixtureInfo.leaf001_point_b
    r_old = eng.run(f'[out:json][date:"2026-09-19T01:00:00Z"];node({hinfo.move_node_id});out;')
    r_new = eng.run(f'[out:json][date:"2026-09-19T01:20:00Z"];node({hinfo.move_node_id});out;')
    assert len(r_old.elements) == 1 and len(r_new.elements) == 1
    assert abs(r_new.elements[0]["lat"] - lat_b) < 1e-6 and abs(r_new.elements[0]["lon"] - lon_b) < 1e-6
    assert (r_old.elements[0]["lat"], r_old.elements[0]["lon"]) != (r_new.elements[0]["lat"], r_new.elements[0]["lon"])
    # the moved node is found by a bbox scan of its NEW position at t, and not at its old one
    r_bbox = eng.run(
        f'[out:json][date:"2026-09-19T01:20:00Z"];'
        f"node({lat_b - 0.0005},{lon_b - 0.0005},{lat_b + 0.0005},{lon_b + 0.0005});out ids;"
    )
    assert hinfo.move_node_id in [e["id"] for e in r_bbox.elements]
    gone = eng.run(f'[out:json][date:"2026-09-19T01:20:00Z"];node({hinfo.delete_node_id});out ids;')
    still = eng.run(f'[out:json][date:"2026-09-19T01:00:00Z"];node({hinfo.delete_node_id});out ids;')
    assert gone.elements == [] and len(still.elements) == 1


def test_engine_timeline_and_adiff_over_tiers(hinfo, run3):
    eng = _engine(hinfo)
    tl = eng.run("[out:json];timeline(node,1);out;")
    versions = [e["tags"]["refversion"] for e in tl.elements]
    assert len(versions) == 3 and versions == sorted(versions, key=int)
    assert "expired" not in tl.elements[-1]["tags"] and all("expired" in e["tags"] for e in tl.elements[:-1])
    d = eng.run('[out:xml][adiff:"2026-09-19T01:00:00Z","2026-09-19T02:10:00Z"];node(1);out meta;')
    body, _ctype = d.render()
    assert '<action type="modify">' in body and "renamed run3" in body and "<old>" in body
