"""Focused, isolated test of ``osmpq.update.updater._extent_filter``
(docs/m2-contracts.md section 2), against a synthetic manifest with a
simple axis-aligned extent and no base data -- so the result is purely a
function of the inside-extent bounds check and can't be muddied by a real
dataset's own (possibly much wider than expected) recorded extent.

Added after a real-run report of a way placed outside Wisconsin's real
Minnesota boundary but technically inside the Minnesota dataset's own
(wide, "smart"-extraction-widened) recorded ``extent``: that turned out to
be correct per docs/m2-contracts.md section 2 ("keeps only what is inside
its extent (manifest bbox)"), not an axis bug -- this test exists to prove
the bounds check itself (south/north vs. lat, west/east vs. lon) is right,
independent of what any particular dataset's manifest happens to record.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from osmpq.layout import manifest as manifest_mod
from osmpq.update import updater as updater_mod

# extent = [south, west, north, east]; lat in [0, 10], lon in [0, 5]
EXTENT = [0.0, 0.0, 10.0, 5.0]
PROMOTED_KEYS = ["amenity"]


@pytest.fixture
def man() -> manifest_mod.Manifest:
    return manifest_mod.Manifest(
        generation="g0001",
        timestamp_osm_base="2026-01-01T00:00:00Z",
        source="synthetic",
        extent=EXTENT,
        leaf_cells=["root"],
        byid={"node": [], "way": [], "relation": []},
        index={"node_way": [], "member": []},
        promoted_keys=PROMOTED_KEYS,
        manifest_version=2,
    )


@pytest.fixture
def con(tmp_path):
    c = duckdb.connect()
    c.execute("INSTALL spatial")
    c.execute("LOAD spatial")
    return c


def _empty_batch_tables(con) -> None:
    con.execute("""
        CREATE TABLE batch_node AS
        SELECT NULL::BIGINT AS id, NULL::BOOLEAN AS deleted, NULL::INTEGER AS version,
               NULL::TIMESTAMP AS "timestamp", NULL::BIGINT AS changeset, NULL::INTEGER AS uid,
               NULL::VARCHAR AS "user", NULL::MAP(VARCHAR, VARCHAR) AS tags,
               NULL::INTEGER AS lat_e7, NULL::INTEGER AS lon_e7, NULL::BIGINT AS seq
        WHERE FALSE
    """)
    con.execute("""
        CREATE TABLE batch_way AS
        SELECT NULL::BIGINT AS id, NULL::BOOLEAN AS deleted, NULL::INTEGER AS version,
               NULL::TIMESTAMP AS "timestamp", NULL::BIGINT AS changeset, NULL::INTEGER AS uid,
               NULL::VARCHAR AS "user", NULL::MAP(VARCHAR, VARCHAR) AS tags,
               NULL::BIGINT[] AS refs, NULL::BIGINT AS seq
        WHERE FALSE
    """)
    con.execute("""
        CREATE TABLE batch_relation AS
        SELECT NULL::BIGINT AS id, NULL::BOOLEAN AS deleted, NULL::INTEGER AS version,
               NULL::TIMESTAMP AS "timestamp", NULL::BIGINT AS changeset, NULL::INTEGER AS uid,
               NULL::VARCHAR AS "user", NULL::MAP(VARCHAR, VARCHAR) AS tags,
               NULL::STRUCT("type" VARCHAR, ref BIGINT, role VARCHAR)[] AS members, NULL::BIGINT AS seq
        WHERE FALSE
    """)


def _node_row(id_: int, lat: float, lon: float) -> str:
    return (
        f"SELECT {id_}::BIGINT AS id, FALSE AS deleted, 1::INTEGER AS version, "
        f"TIMESTAMP '2026-01-01 00:00:00' AS \"timestamp\", 1::BIGINT AS changeset, 1::INTEGER AS uid, "
        f"'t'::VARCHAR AS \"user\", NULL::MAP(VARCHAR, VARCHAR) AS tags, "
        f"{round(lat * 1e7)}::INTEGER AS lat_e7, {round(lon * 1e7)}::INTEGER AS lon_e7, 1::BIGINT AS seq"
    )


def _way_row(id_: int, refs: list[int]) -> str:
    refs_sql = "[" + ", ".join(str(r) for r in refs) + "]::BIGINT[]"
    return (
        f"SELECT {id_}::BIGINT AS id, FALSE AS deleted, 1::INTEGER AS version, "
        f"TIMESTAMP '2026-01-01 00:00:00' AS \"timestamp\", 1::BIGINT AS changeset, 1::INTEGER AS uid, "
        f"'t'::VARCHAR AS \"user\", NULL::MAP(VARCHAR, VARCHAR) AS tags, {refs_sql} AS refs, 1::BIGINT AS seq"
    )


def test_node_outside_in_longitude_only_is_dropped_node_inside_is_kept(con, man):
    _empty_batch_tables(con)
    con.execute("DROP TABLE batch_node")
    con.execute(
        "CREATE TABLE batch_node AS "
        + " UNION ALL ".join([
            _node_row(1, lat=5.0, lon=2.0),    # inside both axes -> kept
            _node_row(2, lat=5.0, lon=99.0),   # inside latitude, WAY outside longitude -> dropped
            _node_row(3, lat=5.0, lon=-99.0),  # inside latitude, outside longitude the other way -> dropped
        ])
    )
    state = updater_mod._load_tiers(con, Path("/nonexistent"), man, PROMOTED_KEYS)
    updater_mod._build_delta_indexes(con, state)
    south_e7, west_e7, north_e7, east_e7 = (round(x * 1e7) for x in EXTENT)
    kept, dropped = updater_mod._extent_filter(
        con, state, Path("/nonexistent"), man, PROMOTED_KEYS, south_e7, west_e7, north_e7, east_e7
    )
    kept_ids = {r[0] for r in con.execute(f"SELECT id FROM {kept['node']}").fetchall()}
    assert kept_ids == {1}
    assert dropped["node"] == 2


def test_node_outside_in_latitude_only_is_dropped(con, man):
    """The mirror case: inside longitude, outside latitude -- catches an
    axis swap the other way (lat compared against west/east, or lon
    against south/north)."""
    _empty_batch_tables(con)
    con.execute("DROP TABLE batch_node")
    con.execute(
        "CREATE TABLE batch_node AS "
        + " UNION ALL ".join([
            _node_row(1, lat=5.0, lon=2.0),     # inside both -> kept
            _node_row(2, lat=99.0, lon=2.0),    # inside longitude, outside latitude -> dropped
        ])
    )
    state = updater_mod._load_tiers(con, Path("/nonexistent"), man, PROMOTED_KEYS)
    updater_mod._build_delta_indexes(con, state)
    south_e7, west_e7, north_e7, east_e7 = (round(x * 1e7) for x in EXTENT)
    kept, dropped = updater_mod._extent_filter(
        con, state, Path("/nonexistent"), man, PROMOTED_KEYS, south_e7, west_e7, north_e7, east_e7
    )
    kept_ids = {r[0] for r in con.execute(f"SELECT id FROM {kept['node']}").fetchall()}
    assert kept_ids == {1}
    assert dropped["node"] == 1


def test_way_referencing_only_a_longitude_outside_node_is_dropped_way_with_inside_node_is_kept(con, man):
    _empty_batch_tables(con)
    con.execute("DROP TABLE batch_node")
    con.execute("DROP TABLE batch_way")
    con.execute(
        "CREATE TABLE batch_node AS "
        + " UNION ALL ".join([
            _node_row(1, lat=5.0, lon=2.0),   # inside -> kept
            _node_row(2, lat=5.0, lon=99.0),  # outside in longitude only -> dropped
        ])
    )
    con.execute(
        "CREATE TABLE batch_way AS "
        + " UNION ALL ".join([
            _way_row(100, [1]),        # references only the inside node -> kept
            _way_row(200, [2]),        # references only the outside-in-lon node -> dropped
        ])
    )
    state = updater_mod._load_tiers(con, Path("/nonexistent"), man, PROMOTED_KEYS)
    updater_mod._build_delta_indexes(con, state)
    south_e7, west_e7, north_e7, east_e7 = (round(x * 1e7) for x in EXTENT)
    kept, dropped = updater_mod._extent_filter(
        con, state, Path("/nonexistent"), man, PROMOTED_KEYS, south_e7, west_e7, north_e7, east_e7
    )
    kept_way_ids = {r[0] for r in con.execute(f"SELECT id FROM {kept['way']}").fetchall()}
    assert kept_way_ids == {100}
    assert dropped["way"] == 1
