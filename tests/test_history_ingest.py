"""Tests for ``osmpq.history.ingest``: docs/m4-contracts.md section 4 (the
ingest layer, above "4.1"), against tiny inputs written with pyosmium's own
writer (``osmium.SimpleWriter``) so the real PBF/OsmChange/full-history
parsing paths are exercised, not a mock.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import osmium
import pytest

from osmpq.history import ingest as ingest_mod

EXTENT = (40.0, -100.0, 50.0, -80.0)  # south, west, north, east
INSIDE = (45.0, -93.0)  # lat, lon
OUTSIDE = (0.0, 0.0)


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


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    return con


@pytest.fixture(scope="module")
def fixture_dir(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("history-ingest-fixtures")


@pytest.fixture(scope="module")
def base_and_osc(fixture_dir: Path) -> tuple[Path, list[Path]]:
    """A tiny base extract + two `.osc` diffs:

    - node 101 (inside extent): base v1, then v2 (retag, in `diff1`).
    - node 102 (inside extent): base v1 only, a way ref, never touched again.
    - node 103 (inside extent): base v1, then deleted in `diff2`.
    - node 104 (OUTSIDE the extent): base v1 -- must be dropped entirely.
    - way 201: base v1, refs [101, 102].
    """
    base_path = fixture_dir / "base.osm.pbf"
    w = osmium.SimpleWriter(str(base_path))
    w.add_node(_node(101, *INSIDE, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(102, 45.001, -93.001, 1, dt.datetime(2020, 1, 2)))
    w.add_node(_node(103, 45.002, -93.002, 1, dt.datetime(2020, 1, 3)))
    w.add_node(_node(104, *OUTSIDE, 1, dt.datetime(2020, 1, 4)))
    w.add_way(_way(201, [101, 102], 1, dt.datetime(2020, 1, 5), tags={"highway": "residential"}))
    w.close()

    diff1 = fixture_dir / "diff1.osc"
    w1 = osmium.SimpleWriter(str(diff1))
    w1.add_node(_node(101, *INSIDE, 2, dt.datetime(2020, 2, 1), tags={"amenity": "cafe"}))
    w1.close()

    diff2 = fixture_dir / "diff2.osc"
    w2 = osmium.SimpleWriter(str(diff2))
    w2.add_node(_node(103, 45.002, -93.002, 2, dt.datetime(2020, 3, 1), visible=False))
    w2.close()

    return base_path, [diff1, diff2]


def _run_ingest(base_path: Path, osc_paths: list[Path]) -> tuple[duckdb.DuckDBPyConnection, dict, object]:
    con = _con()
    opts = ingest_mod.IngestOptions(root="unused", pbf_path=str(base_path), osc_paths=[str(p) for p in osc_paths])
    counts, since = ingest_mod.ingest_history(con, opts, EXTENT)
    return con, counts, since


def test_extent_filter_drops_outside_node(base_and_osc):
    base_path, oscs = base_and_osc
    con, counts, since = _run_ingest(base_path, oscs)
    ids = {r[0] for r in con.execute("SELECT DISTINCT id FROM hist_node_raw").fetchall()}
    assert 104 not in ids
    assert {101, 102, 103} <= ids


def test_every_version_kept_not_deduplicated(base_and_osc):
    base_path, oscs = base_and_osc
    con, counts, since = _run_ingest(base_path, oscs)
    versions = sorted(r[0] for r in con.execute("SELECT version FROM hist_node_raw WHERE id = 101").fetchall())
    assert versions == [1, 2]


def test_deletion_row_present_and_marked_invisible(base_and_osc):
    base_path, oscs = base_and_osc
    con, counts, since = _run_ingest(base_path, oscs)
    rows = con.execute("SELECT version, visible, lat_e7, tags FROM hist_node_raw WHERE id = 103 ORDER BY version").fetchall()
    assert rows[0] == (1, True, 450020000, None)
    assert rows[1][0:2] == (2, False)
    assert rows[1][2] is None and rows[1][3] is None  # payload NULLed on delete


def test_way_kept_because_both_refs_known(base_and_osc):
    base_path, oscs = base_and_osc
    con, counts, since = _run_ingest(base_path, oscs)
    rows = con.execute("SELECT id, refs FROM hist_way_raw").fetchall()
    assert rows == [(201, [101, 102])]


def test_since_is_the_base_files_own_max_timestamp(base_and_osc):
    base_path, oscs = base_and_osc
    con, counts, since = _run_ingest(base_path, oscs)
    assert since == dt.datetime(2020, 1, 5)  # the way's base timestamp, the latest in the base file


def test_way_dropped_when_every_ref_outside_extent(fixture_dir):
    base_path = fixture_dir / "base_isolated_way.osm.pbf"
    w = osmium.SimpleWriter(str(base_path))
    w.add_node(_node(501, *OUTSIDE, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(502, 0.001, 0.001, 1, dt.datetime(2020, 1, 1)))
    w.add_way(_way(601, [501, 502], 1, dt.datetime(2020, 1, 1)))
    w.close()
    con = _con()
    opts = ingest_mod.IngestOptions(root="unused", pbf_path=str(base_path), osc_paths=None)
    counts, since = ingest_mod.ingest_history(con, opts, EXTENT)
    assert counts["node"] == 0
    assert counts["way"] == 0


def test_relation_kept_via_member_way(fixture_dir):
    base_path = fixture_dir / "base_with_relation.osm.pbf"
    w = osmium.SimpleWriter(str(base_path))
    w.add_node(_node(701, *INSIDE, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(702, 45.001, -93.001, 1, dt.datetime(2020, 1, 1)))
    w.add_way(_way(801, [701, 702], 1, dt.datetime(2020, 1, 1)))
    w.add_relation(_relation(901, [("w", 801, "outer")], 1, dt.datetime(2020, 1, 1)))
    w.close()
    con = _con()
    opts = ingest_mod.IngestOptions(root="unused", pbf_path=str(base_path), osc_paths=None)
    counts, since = ingest_mod.ingest_history(con, opts, EXTENT)
    assert counts["relation"] == 1


def test_osc_dedup_keeps_last_occurrence_within_a_file(fixture_dir):
    """A version genuinely repeated within one `.osc` (an accepted real-world
    oddity per docs/m2-contracts.md section 1) must not produce duplicate
    history rows."""
    base_path = fixture_dir / "base_dedup.osm.pbf"
    w = osmium.SimpleWriter(str(base_path))
    w.add_node(_node(111, *INSIDE, 1, dt.datetime(2020, 1, 1)))
    w.close()
    osc_path = fixture_dir / "dedup.osc"
    wo = osmium.SimpleWriter(str(osc_path))
    wo.add_node(_node(111, *INSIDE, 2, dt.datetime(2020, 2, 1), tags={"a": "1"}))
    wo.add_node(_node(111, *INSIDE, 2, dt.datetime(2020, 2, 1), tags={"a": "2"}))
    wo.close()
    con = _con()
    opts = ingest_mod.IngestOptions(root="unused", pbf_path=str(base_path), osc_paths=[str(osc_path)])
    counts, since = ingest_mod.ingest_history(con, opts, EXTENT)
    rows = con.execute("SELECT version, tags FROM hist_node_raw WHERE id = 111 ORDER BY version").fetchall()
    assert [v for v, _ in rows] == [1, 2]
    assert rows[1][1] == {"a": "2"}  # last occurrence in the file wins


def test_osh_and_pbf_plus_osc_give_identical_raw_rows(fixture_dir):
    """The same story told as `--pbf` + `--osc` and as a single `--osh` full
    history file must ingest to the same raw rows (docs/m4-contracts.md
    section 4.3)."""
    base_path = fixture_dir / "story_base.osm.pbf"
    w = osmium.SimpleWriter(str(base_path))
    w.add_node(_node(201, *INSIDE, 1, dt.datetime(2020, 1, 1)))
    w.add_node(_node(202, 45.001, -93.001, 1, dt.datetime(2020, 1, 1)))
    w.add_way(_way(301, [201, 202], 1, dt.datetime(2020, 1, 1), tags={"highway": "track"}))
    w.close()

    osc_path = fixture_dir / "story_diff.osc"
    wo = osmium.SimpleWriter(str(osc_path))
    wo.add_node(_node(201, 45.5, -93.5, 2, dt.datetime(2020, 2, 1), tags={"amenity": "bench"}))
    wo.close()

    osh_path = fixture_dir / "story.osh.pbf"
    wh = osmium.SimpleWriter(str(osh_path))
    wh.add_node(_node(201, *INSIDE, 1, dt.datetime(2020, 1, 1)))
    wh.add_node(_node(202, 45.001, -93.001, 1, dt.datetime(2020, 1, 1)))
    wh.add_way(_way(301, [201, 202], 1, dt.datetime(2020, 1, 1), tags={"highway": "track"}))
    wh.add_node(_node(201, 45.5, -93.5, 2, dt.datetime(2020, 2, 1), tags={"amenity": "bench"}))
    wh.close()

    con_pbf = _con()
    opts_pbf = ingest_mod.IngestOptions(root="unused", pbf_path=str(base_path), osc_paths=[str(osc_path)])
    counts_pbf, since_pbf = ingest_mod.ingest_history(con_pbf, opts_pbf, EXTENT)

    con_osh = _con()
    opts_osh = ingest_mod.IngestOptions(root="unused", osh_path=str(osh_path))
    counts_osh, since_osh = ingest_mod.ingest_history(con_osh, opts_osh, EXTENT)

    assert counts_pbf == counts_osh
    node_pbf = con_pbf.execute(
        "SELECT id, version, timestamp, visible, tags, lat_e7, lon_e7 FROM hist_node_raw ORDER BY id, version"
    ).fetchall()
    node_osh = con_osh.execute(
        "SELECT id, version, timestamp, visible, tags, lat_e7, lon_e7 FROM hist_node_raw ORDER BY id, version"
    ).fetchall()
    assert node_pbf == node_osh
    way_pbf = con_pbf.execute("SELECT id, version, refs, tags FROM hist_way_raw ORDER BY id, version").fetchall()
    way_osh = con_osh.execute("SELECT id, version, refs, tags FROM hist_way_raw ORDER BY id, version").fetchall()
    assert way_pbf == way_osh
