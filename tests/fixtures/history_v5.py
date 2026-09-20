"""Builds a v5 (history-carrying) root for W3's own history tests
(``tests/test_history_update.py``, ``tests/test_history_compact.py``),
docs/m4-contracts.md section 2.

Starts from the existing M0 fixture (``tests/fixtures/make_fixture.py``,
``manifest_version=2`` -- real ``node_way``/``member`` indexes, real
metadata on untagged nodes, v2 cell placement), patches in the ``hilbert``
byid column the M2 updater's schema needs (the same patch
``tests/test_updater_s3.py`` applies -- the M0 fixture predates it), then
adds a v5 ``history`` section built directly with DuckDB from the fixture's
*current* rows: every row as its ``minor=0`` state, ``valid_from =
timestamp_osm_base``, ``valid_to = NULL``, ``visible = true`` -- exactly
what a fresh history looks like at ``since`` (docs/m4-contracts.md's own
description of the practical test setup).

The base history is written with ``osmpq.build.compact``'s
``_history_write_spatial``/``_history_write_byid`` -- the local stub for
W1's ``history/writer.py`` (section 4.2) compaction is built against; using
it here too means both the fixture and compaction agree on exactly the
same on-disk shape, and the coordinator can swap in the real writer for
both call sites at once.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import make_fixture  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from osmpq.build import compact as compact_mod  # noqa: E402
from osmpq.layout import hilbert as hilbert_mod  # noqa: E402
from osmpq.layout import manifest as manifest_mod  # noqa: E402
from osmpq.update import updater as updater_mod  # noqa: E402


@dataclass
class HistoryFixtureInfo:
    root: str
    info: make_fixture.FixtureInfo
    since: str
    # ids/versions the update scenarios use (docs/m4-contracts.md section
    # 5.1's own test list: "a minor version for a way whose node moved, a
    # deletion tombstone, a move tombstone").
    move_node_id: int = 7          # tagged, unreferenced by any way -> own-version move + tombstone
    move_node_version: int = 0
    way_member_node_id: int = 4    # untagged, member of `minor_way_id` -> moving it gives a minor version
    way_member_node_version: int = 0
    minor_way_id: int = 102        # refs = [1, way_member_node_id, 8]
    minor_way_version: int = 0
    delete_node_id: int = 10       # untagged, referenced by no way -> clean deletion
    delete_node_version: int = 0
    delete_way_id: int = 107       # refs entirely in leaf "001", referenced by no relation
    delete_way_version: int = 0
    leaf000_far_point: tuple = (75.0, -176.0)   # stays in leaf "000"
    leaf001_point_a: tuple = (75.0, -100.0)     # leaf "001" -- crosses from "000"
    leaf001_point_b: tuple = (79.0, -100.5)


def _add_updater_hilbert_columns(root_dir: Path) -> None:
    con = duckdb.connect()
    try:
        for typ in ("node", "way"):
            for part in sorted((root_dir / "byid" / "g0001" / typ).glob("part-*.parquet")):
                cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{part}')").fetchall()]
                if "hilbert" in cols:
                    continue
                tbl = con.execute(f"SELECT * FROM read_parquet('{part}')").to_arrow_table()
                if typ == "node":
                    lat = np.asarray(tbl.column("lat_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    lon = np.asarray(tbl.column("lon_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                else:
                    ymin = np.asarray(tbl.column("ymin_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    ymax = np.asarray(tbl.column("ymax_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    xmin = np.asarray(tbl.column("xmin_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    xmax = np.asarray(tbl.column("xmax_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    lat = np.round((ymin + ymax) / 2.0).astype(np.int64)
                    lon = np.round((xmin + xmax) / 2.0).astype(np.int64)
                hb = hilbert_mod.hilbert_keys(lat, lon).astype(np.uint64)
                tbl2 = tbl.append_column("hilbert", pa.array(hb, type=pa.uint64()))
                pq.write_table(tbl2, str(part))
    finally:
        con.close()


def _history_from_current(con, root: Path, gen: str, typ: str, promoted_keys: list[str],
                           since_sql: str, byid_paths: list[str], spatial_glob: str) -> tuple[dict, list]:
    byid_cols = updater_mod.BYID_COLUMNS[typ](promoted_keys)
    spatial_cols = updater_mod.SPATIAL_COLUMNS[typ](promoted_keys)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _init_byid_{typ} AS
        SELECT {', '.join(byid_cols)}, 0 AS minor, {since_sql} AS valid_from,
               CAST(NULL AS TIMESTAMP) AS valid_to, TRUE AS visible
        FROM read_parquet({byid_paths!r})
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _init_spatial_{typ} AS
        SELECT {', '.join(spatial_cols)}, 0 AS minor, {since_sql} AS valid_from,
               CAST(NULL AS TIMESTAMP) AS valid_to, TRUE AS visible
        FROM read_parquet('{spatial_glob}', hive_partitioning=true)
    """)
    spatial_fragment = compact_mod._history_write_spatial(con, root, gen, typ, f"_init_spatial_{typ}")
    byid_fragment = compact_mod._history_write_byid(con, root, gen, typ, f"_init_byid_{typ}")
    con.execute(f"DROP TABLE _init_byid_{typ}")
    con.execute(f"DROP TABLE _init_spatial_{typ}")
    return spatial_fragment, byid_fragment


def build(root_dir: str) -> HistoryFixtureInfo:
    root = Path(root_dir)
    info = make_fixture.build(str(root), manifest_version=2)
    _add_updater_hilbert_columns(root)

    man = manifest_mod.load_latest(str(root))
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET preserve_insertion_order=false")

    since = man.timestamp_osm_base
    since_sql = f"TIMESTAMP '{since.replace('T', ' ').replace('Z', '')}'"
    gen = man.generation

    spatial: dict = {}
    byid: dict = {}
    for typ in ("node", "way", "relation"):
        byid_paths = [str(root / p["path"]) for p in man.byid[typ]]
        spatial_glob = str(root / "spatial" / gen / typ / "**" / "*.parquet")
        sp, by = _history_from_current(con, root, gen, typ, man.promoted_keys, since_sql, byid_paths, spatial_glob)
        spatial[typ] = sp
        byid[typ] = by

    man.history = {
        "generation": gen,
        "since": since,
        "minor_versions": True,
        "spatial": spatial,
        "byid": byid,
        "tiers": {},
        "stats": {
            "rows": {t: sum(p["rows"] for p in byid[t]) for t in byid},
            "minor_rows": {t: 0 for t in byid},
            "bytes": sum(p["bytes"] for parts in byid.values() for p in parts)
            + sum(p["bytes"] for cells in spatial.values() for parts in cells.values() for p in parts),
        },
    }
    man.manifest_version = 5
    man.replication_sequence = 0
    man.replication_source = "https://fake.example/repl"
    manifest_mod.write_manifest(str(root), man, 2)

    node_byid_paths = [str(root / p["path"]) for p in man.byid["node"]]
    way_byid_paths = [str(root / p["path"]) for p in man.byid["way"]]

    def _version(paths: list[str], id_: int) -> int:
        return con.execute(f"SELECT version FROM read_parquet({paths!r}) WHERE id = {id_}").fetchone()[0]

    hinfo = HistoryFixtureInfo(root=str(root), info=info, since=since)
    hinfo.move_node_version = _version(node_byid_paths, hinfo.move_node_id)
    hinfo.way_member_node_version = _version(node_byid_paths, hinfo.way_member_node_id)
    hinfo.minor_way_version = _version(way_byid_paths, hinfo.minor_way_id)
    hinfo.delete_node_version = _version(node_byid_paths, hinfo.delete_node_id)
    hinfo.delete_way_version = _version(way_byid_paths, hinfo.delete_way_id)

    con.close()
    return hinfo
