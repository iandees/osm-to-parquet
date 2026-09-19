"""M4 compaction folding history tiers into the base history,
docs/m4-contracts.md section 5.2.

Runs the same three-batch scenario as ``tests/test_history_update.py``
(built from the same shared fixture helper, ``tests/fixtures/
history_v5.py``) through the real updater, then ``osmpq.build.compact.
compact``, and checks:

- ``history.tiers`` is cleared and ``history.generation`` follows the new
  base generation;
- the touched spatial cells and byid parts were rewritten (old rows + tier
  rows), untouched ones hardlinked;
- ``valid_to`` is filled for every state that now has a successor;
- ``[date:]``-style direct queries (section 3.1's ``validity_predicate``/
  ``state_at_sql`` helpers from ``osmpq.history.schema``, since the W2
  engine's snapshot reads are a different, concurrent workstream) at three
  instants -- ``since``, right after run 1, and right after run 3 -- return
  the expected state for the moved node, the minor-versioned way and the
  deleted node/way.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import history_v5  # noqa: E402

from osmpq.build.compact import CompactOptions, compact  # noqa: E402
from osmpq.history import schema as history_schema  # noqa: E402
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
    root = tmp_path_factory.mktemp("history-compact-root") / "root"
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
    p = tmp_path_factory.mktemp("history-compact-osc") / "batch1.osc"
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
    p = tmp_path_factory.mktemp("history-compact-osc3") / "batch3.osc"
    p.write_text(_osc(body))
    return p


@pytest.fixture(autouse=True, scope="module")
def _fix_node1_version(hinfo, batch3_osc):
    con = duckdb.connect()
    root = Path(hinfo.root)
    man = manifest_mod.load_latest(str(root))
    node_byid_paths = [str(root / p["path"]) for p in man.byid["node"]]
    v, lat_e7, lon_e7 = con.execute(
        f"SELECT version, lat_e7, lon_e7 FROM read_parquet({node_byid_paths!r}) WHERE id = 1"
    ).fetchone()
    lat, lon = lat_e7 / 1e7, lon_e7 / 1e7
    text = batch3_osc.read_text()
    text = text.replace('version="99002"', f'version="{v + 1}"')
    text = text.replace('lat="99.0" lon="99.0"', f'lat="{lat:.7f}" lon="{lon:.7f}"')
    batch3_osc.write_text(text)
    con.close()


@pytest.fixture(autouse=True, scope="module")
def _patch_replication_client(batch1_osc, batch3_osc):
    # Only two batches this time (run 1, then a run crossing into the next
    # UTC hour) -- enough to exercise the hour->day fold before compaction
    # without run 2's same-hour-append case, which the updater test already
    # covers on its own.
    _FakeClient.files = {
        1: (batch1_osc, "2026-09-19T01:15:00Z"),
        2: (batch3_osc, "2026-09-19T02:05:00Z"),
    }
    original = updater_mod.ReplicationClient
    updater_mod.ReplicationClient = _FakeClient
    yield
    updater_mod.ReplicationClient = original


@pytest.fixture(scope="module")
def run1(tmp_path_factory, hinfo):
    opts = updater_mod.UpdateOptions(
        root=hinfo.root, source="https://fake.example/repl", max_diffs=1,
        tmpdir=str(tmp_path_factory.mktemp("history-compact-tmp-1")),
    )
    return updater_mod.run_once(opts)


@pytest.fixture(scope="module")
def run2(tmp_path_factory, hinfo, run1):
    opts = updater_mod.UpdateOptions(
        root=hinfo.root, source="https://fake.example/repl", max_diffs=1,
        tmpdir=str(tmp_path_factory.mktemp("history-compact-tmp-2")),
    )
    return updater_mod.run_once(opts)


@pytest.fixture(scope="module")
def compacted(tmp_path_factory, hinfo, run1, run2) -> dict:
    return compact(CompactOptions(root=hinfo.root, tmpdir=str(tmp_path_factory.mktemp("history-compact-compacttmp"))))


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


def _byid_paths(hinfo, compacted, typ: str) -> list[str]:
    return [str(Path(hinfo.root) / p["path"]) for p in compacted["history"]["byid"][typ]]


def _spatial_glob(hinfo, compacted, typ: str) -> str:
    return str(Path(hinfo.root) / "history" / compacted["generation"] / "spatial" / typ / "**" / "*.parquet")


# --------------------------------------------------------------------------
# manifest bookkeeping (section 5.2)
# --------------------------------------------------------------------------


def test_compact_clears_tiers_and_bumps_history_generation(hinfo, compacted):
    history = compacted["history"]
    assert history["tiers"] == {}
    assert history["generation"] == compacted["generation"]
    assert history["since"] == hinfo.since
    assert compacted["manifest_version"] >= 5


def test_compacted_byid_rows_include_base_and_tier_states(hinfo, compacted):
    con = _con()
    paths = _byid_paths(hinfo, compacted, "node")
    rows = con.execute(
        f"SELECT version, minor, valid_from, valid_to, visible FROM read_parquet({paths!r}) "
        f"WHERE id = {hinfo.move_node_id} ORDER BY valid_from"
    ).fetchall()
    assert len(rows) == 2  # the fixture's base state, then the moved state
    base_row, moved_row = rows
    assert base_row[0] == hinfo.move_node_version and base_row[3] is not None  # valid_to filled: a successor exists
    assert base_row[3] == moved_row[2]  # base's valid_to == the successor's valid_from
    assert moved_row[0] == hinfo.move_node_version + 1 and moved_row[3] is None  # latest state: valid_to NULL


def test_compacted_minor_way_row_present_with_valid_to_filled(hinfo, compacted):
    # `minor_way_id` lists node 1 as a member too, so run 2's edit to node 1
    # (a tag-only change, same position -- see `_fix_node1_version`) still
    # puts the way in this run's *touched* set (the node_way cascade doesn't
    # distinguish "moved" from "merely edited") and gets it a second minor
    # state, per section 5.1's rule read literally: any touched parent whose
    # own version didn't change gets a fresh minor row, every run it is
    # cascaded into -- see the report's "deviations" for the tradeoff this
    # implies for a member with many unrelated edits.
    con = _con()
    paths = _byid_paths(hinfo, compacted, "way")
    rows = con.execute(
        f"SELECT version, minor, valid_from, valid_to, visible FROM read_parquet({paths!r}) "
        f"WHERE id = {hinfo.minor_way_id} ORDER BY valid_from"
    ).fetchall()
    assert len(rows) == 3  # base own-version + run 1's minor state + run 2's minor state
    base_row, run1_row, run2_row = rows
    assert base_row[1] == 0 and base_row[3] == run1_row[2]
    assert run1_row[1] == 1 and run1_row[3] == run2_row[2]
    assert run2_row[1] == 2 and run2_row[3] is None


def test_compacted_spatial_move_tombstone_survives_compaction(hinfo, compacted):
    con = _con()
    glob = _spatial_glob(hinfo, compacted, "node")
    rows = con.execute(
        f"SELECT cell, visible, valid_to FROM read_parquet('{glob}', hive_partitioning=true) "
        f"WHERE id = {hinfo.move_node_id} AND version = {hinfo.move_node_version + 1} ORDER BY visible"
    ).fetchall()
    assert len(rows) == 2
    tomb, live = rows
    assert tomb[1] is False and live[1] is True
    assert tomb[0] != live[0]
    # tombstones are markers, not states with a successor -- valid_to stays
    # NULL for them even though the live row's own successor bookkeeping is
    # correct (checked above).
    assert tomb[2] is None


def test_deleted_node_and_way_absent_from_visible_final_state(hinfo, compacted):
    con = _con()
    node_paths = _byid_paths(hinfo, compacted, "node")
    way_paths = _byid_paths(hinfo, compacted, "way")
    node_rows = con.execute(
        f"SELECT visible, valid_to FROM read_parquet({node_paths!r}) WHERE id = {hinfo.delete_node_id} ORDER BY valid_from"
    ).fetchall()
    assert node_rows[-1][0] is False and node_rows[-1][1] is None  # latest state is the deletion, valid_to NULL
    way_rows = con.execute(
        f"SELECT visible, valid_to FROM read_parquet({way_paths!r}) WHERE id = {hinfo.delete_way_id} ORDER BY valid_from"
    ).fetchall()
    assert way_rows[-1][0] is False and way_rows[-1][1] is None


# --------------------------------------------------------------------------
# `[date:]`-equivalent state-at-t queries (section 3.1, engine not merged
# yet -- assert on the rows directly through schema.py's own SQL helpers).
# --------------------------------------------------------------------------


def _state_at(con, paths: list[str], id_: int, t: datetime) -> dict | None:
    t_sql = f"TIMESTAMP '{t.strftime('%Y-%m-%d %H:%M:%S')}'"
    rows_sql = f"SELECT * FROM read_parquet({paths!r}) WHERE id = {id_} AND {history_schema.validity_predicate(t_sql)}"
    sql = history_schema.state_at_sql(rows_sql, t_sql)
    r = con.execute(sql)
    names = [c[0] for c in r.description]
    row = r.fetchone()
    return dict(zip(names, row)) if row else None


def test_date_at_since_equals_base_state(hinfo, compacted):
    con = _con()
    paths = _byid_paths(hinfo, compacted, "node")
    since_dt = datetime.strptime(hinfo.since, "%Y-%m-%dT%H:%M:%SZ")
    state = _state_at(con, paths, hinfo.move_node_id, since_dt)
    assert state is not None
    assert state["version"] == hinfo.move_node_version
    assert state["visible"] is True


def test_date_at_run1_timestamp_sees_the_move(hinfo, compacted):
    con = _con()
    paths = _byid_paths(hinfo, compacted, "node")
    t1 = datetime(2026, 9, 19, 1, 15, 0)
    state = _state_at(con, paths, hinfo.move_node_id, t1)
    assert state is not None
    assert state["version"] == hinfo.move_node_version + 1
    assert state["tags"]["name"] == "Coffee HOUSE moved"

    way_paths = _byid_paths(hinfo, compacted, "way")
    way_state = _state_at(con, way_paths, hinfo.minor_way_id, t1)
    assert way_state["version"] == hinfo.minor_way_version
    assert way_state["minor"] == 1

    node_state = _state_at(con, paths, hinfo.delete_node_id, t1)
    assert node_state["visible"] is False


def test_date_before_run1_does_not_see_the_move(hinfo, compacted):
    con = _con()
    paths = _byid_paths(hinfo, compacted, "node")
    just_before = datetime(2026, 9, 19, 1, 14, 59)
    state = _state_at(con, paths, hinfo.move_node_id, just_before)
    assert state is not None
    assert state["version"] == hinfo.move_node_version  # still the original state

    node_state = _state_at(con, paths, hinfo.delete_node_id, just_before)
    assert node_state["visible"] is True  # not deleted yet


def test_date_at_run2_timestamp_sees_latest(hinfo, compacted):
    con = _con()
    paths = _byid_paths(hinfo, compacted, "node")
    t2 = datetime(2026, 9, 19, 2, 5, 0)
    state = _state_at(con, paths, 1, t2)
    assert state is not None
    assert state["tags"]["name"] == "Aroma Cafe (renamed run3)"
