"""M0 builder: PBF -> the layout of docs/m0-contracts.md sections 1-4.

Reads the PBF with DuckDB's ``spatial`` extension (``ST_ReadOSM``, which
returns raw node/way/relation rows with refs but no version metadata) and
the community ``osmium`` extension (``osmium_read``, which returns version
metadata for ways, relations and *tagged* nodes, joined back in by
``(type, id)``). Untagged nodes therefore have NULL meta columns in M0 --
see the "known gaps" note in ``build()``'s docstring.

Everything that must touch every node (cell assignment, Hilbert keys) is
vectorized with numpy; everything counted in the tens-of-thousands to low
millions (ways, relations, per-cell/per-part file writes) is a plain Python
loop calling into DuckDB per iteration, which is fine at that scale and much
simpler than trying to express loose-cell assignment as one query.
"""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

from osmpq.layout import cells as cells_mod
from osmpq.layout import hilbert as hilbert_mod
from osmpq.layout import manifest as manifest_mod

BYID_PART_ROWS = 5_000_000
ROW_GROUP_SIZE = 100_000


def _log(msg: str) -> None:
    print(f"[osmpq build] {msg}", file=sys.stderr, flush=True)


@dataclass
class BuildOptions:
    pbf_path: str
    root: str
    bbox: Optional[tuple[float, float, float, float]] = None  # s,w,n,e
    generation: Optional[str] = None
    max_nodes_per_cell: int = 1_000_000
    promoted_keys: Optional[list[str]] = None
    timestamp: Optional[str] = None
    replication_sequence: Optional[int] = None
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    tmpdir: Optional[str] = None


def build(opts: BuildOptions) -> manifest_mod.Manifest:
    t_start = time.time()
    tmpdir = Path(opts.tmpdir) if opts.tmpdir else Path.cwd() / ".osmpq-tmp"
    tmpdir.mkdir(parents=True, exist_ok=True)
    root_is_local = "://" not in opts.root
    if root_is_local:
        Path(opts.root).mkdir(parents=True, exist_ok=True)

    promoted_keys = list(opts.promoted_keys or manifest_mod.DEFAULT_PROMOTED_KEYS)

    import duckdb

    db_path = tmpdir / "build.duckdb"
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    con.execute("SET TimeZone='UTC'")
    if opts.threads:
        con.execute(f"SET threads={int(opts.threads)}")
    if opts.memory_limit:
        con.execute(f"SET memory_limit='{opts.memory_limit}'")
    con.execute(f"SET temp_directory='{tmpdir.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    con.execute("INSTALL osmium FROM community")
    con.execute("LOAD osmium")
    if not root_is_local:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

    pbf = opts.pbf_path.replace("'", "''")
    _log(f"reading {opts.pbf_path} ...")
    t0 = time.time()
    con.execute(f"""
        CREATE TABLE raw_osm AS
        SELECT kind::VARCHAR AS kind, id, tags, refs, lat, lon, ref_roles,
               list_transform(ref_types, x -> CASE x
                   WHEN 'node' THEN 'n' WHEN 'way' THEN 'w' WHEN 'relation' THEN 'r' END) AS ref_types
        FROM ST_ReadOSM('{pbf}')
    """)
    con.execute(f"""
        CREATE TABLE raw_meta AS
        SELECT DISTINCT ON (type, id) type, id,
               CAST(version AS INTEGER) AS version,
               CAST(changeset AS BIGINT) AS changeset,
               CAST(timestamp AS TIMESTAMP) AS timestamp,
               CAST(uid AS INTEGER) AS uid,
               username AS "user"
        FROM osmium_read('{pbf}')
    """)
    counts = dict(con.execute("SELECT kind, count(*) FROM raw_osm GROUP BY kind").fetchall())
    _log(f"read {sum(counts.values())} raw rows {counts} in {time.time()-t0:.1f}s")

    con.execute("""
        CREATE TABLE node0 AS
        SELECT r.id,
               CAST(round(r.lat*1e7) AS INTEGER) AS lat_e7,
               CAST(round(r.lon*1e7) AS INTEGER) AS lon_e7,
               r.tags,
               m.version, m.changeset, m.timestamp, m.uid, m."user"
        FROM raw_osm r LEFT JOIN raw_meta m ON m.type='node' AND m.id=r.id
        WHERE r.kind='node'
    """)
    con.execute("""
        CREATE TABLE way0 AS
        SELECT r.id, r.refs, r.tags,
               m.version, m.changeset, m.timestamp, m.uid, m."user"
        FROM raw_osm r LEFT JOIN raw_meta m ON m.type='way' AND m.id=r.id
        WHERE r.kind='way'
    """)
    con.execute("""
        CREATE TABLE relation0 AS
        SELECT r.id, r.tags,
               list_transform(range(1, len(coalesce(r.refs, [])) + 1),
                   i -> struct_pack("type" := r.ref_types[i], ref := r.refs[i],
                                    role := coalesce(r.ref_roles[i], ''))) AS members,
               m.version, m.changeset, m.timestamp, m.uid, m."user"
        FROM raw_osm r LEFT JOIN raw_meta m ON m.type='relation' AND m.id=r.id
        WHERE r.kind='relation'
    """)
    con.execute("DROP TABLE raw_osm")
    con.execute("DROP TABLE raw_meta")

    if opts.bbox is not None:
        _apply_bbox_extract(con, opts.bbox)

    n_nodes, n_ways, n_relations = (
        con.execute("SELECT count(*) FROM node0").fetchone()[0],
        con.execute("SELECT count(*) FROM way0").fetchone()[0],
        con.execute("SELECT count(*) FROM relation0").fetchone()[0],
    )
    _log(f"nodes={n_nodes} ways={n_ways} relations={n_relations}")

    # ---- way geometry / bbox --------------------------------------------------
    t0 = time.time()
    # Materialize the way->node join first (hash join, spills to disk), then
    # aggregate in id-range batches: an ordered list() over tens of millions
    # of rows in one go cannot spill and runs out of memory at state scale.
    con.execute("""
        CREATE TABLE way_pts AS
        SELECT w.id AS way_id, t.ordinal, n.lat_e7, n.lon_e7
        FROM way0 w, UNNEST(w.refs) WITH ORDINALITY AS t(ref, ordinal)
        LEFT JOIN node0 n ON n.id = t.ref
    """)
    con.execute("""
        CREATE TABLE way_geom (
            id BIGINT, ymin_e7 INTEGER, ymax_e7 INTEGER, xmin_e7 INTEGER, xmax_e7 INTEGER,
            geometry GEOMETRY
        )
    """)
    batch_rows = 250_000
    bounds = [r[0] for r in con.execute(f"""
        SELECT id FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM way0)
        WHERE rn % {batch_rows} = 1 ORDER BY id
    """).fetchall()]
    bounds.append(None)
    for lo, hi in zip(bounds[:-1], bounds[1:]):
        cond = f"way_id >= {lo}" + (f" AND way_id < {hi}" if hi is not None else "")
        con.execute(f"""
            INSERT INTO way_geom
            SELECT way_id AS id,
                   CASE WHEN count(lat_e7) > 0 THEN min(lat_e7) END AS ymin_e7,
                   CASE WHEN count(lat_e7) > 0 THEN max(lat_e7) END AS ymax_e7,
                   CASE WHEN count(lat_e7) > 0 THEN min(lon_e7) END AS xmin_e7,
                   CASE WHEN count(lat_e7) > 0 THEN max(lon_e7) END AS xmax_e7,
                   CASE WHEN count(lat_e7) >= 2 THEN
                       ST_MakeLine(list(ST_Point(lon_e7 / 1e7, lat_e7 / 1e7) ORDER BY ordinal)
                                   FILTER (WHERE lat_e7 IS NOT NULL))
                   END AS geometry
            FROM way_pts WHERE {cond}
            GROUP BY way_id
        """)
    con.execute("DROP TABLE way_pts")
    con.execute("""
        CREATE TABLE way1 AS
        SELECT w.id, w.refs, w.tags, w.version, w.changeset, w.timestamp, w.uid, w."user",
               g.xmin_e7, g.ymin_e7, g.xmax_e7, g.ymax_e7, g.geometry,
               (coalesce(len(w.refs), 0) >= 4 AND w.refs[1] = w.refs[len(w.refs)]) AS is_closed,
               CASE WHEN g.xmin_e7 IS NULL THEN NULL
                    ELSE CAST(round((g.ymin_e7 + g.ymax_e7) / 2.0) AS INTEGER) END AS centroid_lat_e7,
               CASE WHEN g.xmin_e7 IS NULL THEN NULL
                    ELSE CAST(round((g.xmin_e7 + g.xmax_e7) / 2.0) AS INTEGER) END AS centroid_lon_e7
        FROM way0 w JOIN way_geom g ON g.id = w.id
    """)
    con.execute("""
        ALTER TABLE way1 ADD COLUMN is_area BOOLEAN
    """)
    con.execute("""
        UPDATE way1 SET is_area = (
            is_closed
            AND coalesce(tags['area'][1], '') != 'no'
            AND NOT (
                (tags['highway'][1] IS NOT NULL OR tags['barrier'][1] IS NOT NULL)
                AND coalesce(tags['area'][1], '') != 'yes'
            )
        )
    """)
    con.execute("DROP TABLE way_geom")
    _log(f"way geometry/bbox built in {time.time()-t0:.1f}s")

    # ---- relation bbox (member nodes/ways, then one nested pass) -------------
    t0 = time.time()
    # Flatten members first so every lookup is a plain equi-join; a lateral
    # UNNEST joined straight against the node table planned as a near
    # cross product at state scale.
    con.execute("""
        CREATE TABLE rel_members AS
        SELECT r.id AS rel_id, m.type AS mtype, m.ref AS mref
        FROM relation0 r, UNNEST(r.members) AS t(m)
    """)
    con.execute("""
        CREATE TABLE rel_member_bbox AS
        SELECT rm.rel_id, nn.lat_e7 AS ymin_e7, nn.lat_e7 AS ymax_e7,
               nn.lon_e7 AS xmin_e7, nn.lon_e7 AS xmax_e7
        FROM rel_members rm JOIN node0 nn ON nn.id = rm.mref
        WHERE rm.mtype = 'n'
        UNION ALL
        SELECT rm.rel_id, ww.ymin_e7, ww.ymax_e7, ww.xmin_e7, ww.xmax_e7
        FROM rel_members rm JOIN way1 ww ON ww.id = rm.mref
        WHERE rm.mtype = 'w' AND ww.xmin_e7 IS NOT NULL
    """)
    con.execute("""
        CREATE TABLE relation_bbox0 AS
        SELECT r.id,
               min(b.ymin_e7) AS ymin_e7, max(b.ymax_e7) AS ymax_e7,
               min(b.xmin_e7) AS xmin_e7, max(b.xmax_e7) AS xmax_e7
        FROM relation0 r LEFT JOIN rel_member_bbox b ON b.rel_id = r.id
        GROUP BY r.id
    """)
    # second pass: fold in bboxes of member relations resolved in pass 0
    con.execute("""
        CREATE TABLE relation_bbox1 AS
        SELECT r0.id,
               LEAST(r0.ymin_e7, min(rb.ymin_e7)) AS ymin_e7,
               GREATEST(r0.ymax_e7, max(rb.ymax_e7)) AS ymax_e7,
               LEAST(r0.xmin_e7, min(rb.xmin_e7)) AS xmin_e7,
               GREATEST(r0.xmax_e7, max(rb.xmax_e7)) AS xmax_e7
        FROM relation_bbox0 r0
        LEFT JOIN rel_members rm ON rm.rel_id = r0.id AND rm.mtype = 'r'
        LEFT JOIN relation_bbox0 rb ON rb.id = rm.mref
        GROUP BY r0.id, r0.ymin_e7, r0.ymax_e7, r0.xmin_e7, r0.xmax_e7
    """)
    con.execute("DROP TABLE rel_member_bbox")
    con.execute("DROP TABLE rel_members")
    con.execute("""
        CREATE TABLE relation1 AS
        SELECT r.id, r.tags, r.members, r.version, r.changeset, r.timestamp, r.uid, r."user",
               b.xmin_e7, b.ymin_e7, b.xmax_e7, b.ymax_e7,
               CASE WHEN b.xmin_e7 IS NULL THEN NULL
                    ELSE CAST(round((b.ymin_e7 + b.ymax_e7) / 2.0) AS INTEGER) END AS centroid_lat_e7,
               CASE WHEN b.xmin_e7 IS NULL THEN NULL
                    ELSE CAST(round((b.xmin_e7 + b.xmax_e7) / 2.0) AS INTEGER) END AS centroid_lon_e7
        FROM relation0 r JOIN relation_bbox1 b ON b.id = r.id
    """)
    con.execute("DROP TABLE relation_bbox0")
    con.execute("DROP TABLE relation_bbox1")
    _log(f"relation bbox built in {time.time()-t0:.1f}s")

    # ---- leaf cell selection (aggregated counts, numpy) -----------------------
    t0 = time.time()
    leaf_cells = _select_leaf_cells(con, opts.max_nodes_per_cell)
    leaf_index = cells_mod.LeafIndex(leaf_cells)
    _log(f"selected {len(leaf_cells)} leaf cells in {time.time()-t0:.1f}s")

    # ---- node cell + hilbert (vectorized) --------------------------------------
    t0 = time.time()
    ids, lat_e7, lon_e7 = con.execute("SELECT id, lat_e7, lon_e7 FROM node0").fetchnumpy().values()
    qk = cells_mod.point_to_qk_np(lat_e7, lon_e7)
    leaf_idx = leaf_index.leaf_index_for_qk(qk)
    leaf_keys_arr = np.array(leaf_index.leaves_by_lo, dtype=object)
    node_cell = leaf_keys_arr[leaf_idx]
    node_hilbert = hilbert_mod.hilbert_keys(lat_e7, lon_e7)
    _register_assignment(con, "node_assign", ids, node_cell, node_hilbert)
    con.execute("""
        CREATE TABLE node1 AS
        SELECT n.*, a.cell, a.hilbert FROM node0 n JOIN node_assign a USING (id)
    """)
    _log(f"node cell/hilbert assigned in {time.time()-t0:.1f}s")

    # ---- way / relation cell + hilbert (bbox descent, python loop) ------------
    t0 = time.time()
    _assign_loose_cells(con, "way1", leaf_index)
    _assign_loose_cells(con, "relation1", leaf_index)
    _log(f"way/relation cells assigned in {time.time()-t0:.1f}s")

    # ---- write everything -------------------------------------------------------
    gen_number = manifest_mod.next_manifest_number(opts.root)
    generation = opts.generation or f"g{gen_number:04d}"
    _log(f"writing generation {generation} to {opts.root} ...")

    tables_manifest: dict = {}
    t0 = time.time()
    tables_manifest["node"] = _write_node_spatial(con, opts.root, generation, promoted_keys)
    _log(f"wrote node spatial files in {time.time()-t0:.1f}s")

    t0 = time.time()
    tables_manifest["way"] = _write_loose_spatial(con, opts.root, generation, "way", "way1", promoted_keys)
    _log(f"wrote way spatial files in {time.time()-t0:.1f}s")

    t0 = time.time()
    tables_manifest["relation"] = _write_loose_spatial(
        con, opts.root, generation, "relation", "relation1", promoted_keys
    )
    _log(f"wrote relation spatial files in {time.time()-t0:.1f}s")

    t0 = time.time()
    byid_manifest = {
        "node": _write_byid(con, opts.root, generation, "node", promoted_keys),
        "way": _write_byid(con, opts.root, generation, "way", promoted_keys),
        "relation": _write_byid(con, opts.root, generation, "relation", promoted_keys),
    }
    _log(f"wrote byid files in {time.time()-t0:.1f}s")

    t0 = time.time()
    index_manifest = {
        "node_way": _write_node_way_index(con, opts.root, generation),
        "member": _write_member_index(con, opts.root, generation),
    }
    _log(f"wrote index files in {time.time()-t0:.1f}s")

    # ---- manifest ---------------------------------------------------------------
    if opts.timestamp:
        timestamp_osm_base = opts.timestamp
    else:
        max_ts = con.execute(
            "SELECT max(x) FROM (SELECT max(timestamp) AS x FROM node1 "
            "UNION ALL SELECT max(timestamp) FROM way1 UNION ALL SELECT max(timestamp) FROM relation1)"
        ).fetchone()[0]
        timestamp_osm_base = (
            max_ts.strftime("%Y-%m-%dT%H:%M:%SZ") if max_ts is not None else "1970-01-01T00:00:00Z"
        )

    if opts.bbox is not None:
        extent = list(opts.bbox)
        source = f"{Path(opts.pbf_path).name} bbox=({opts.bbox[0]},{opts.bbox[1]},{opts.bbox[2]},{opts.bbox[3]})"
    else:
        ext = con.execute(
            "SELECT min(lat_e7)/1e7, min(lon_e7)/1e7, max(lat_e7)/1e7, max(lon_e7)/1e7 FROM node1"
        ).fetchone()
        extent = [float(x) if x is not None else 0.0 for x in ext]
        source = Path(opts.pbf_path).name

    man = manifest_mod.Manifest(
        generation=generation,
        timestamp_osm_base=timestamp_osm_base,
        source=source,
        extent=extent,
        leaf_cells=sorted(leaf_cells),
        tables=tables_manifest,
        byid=byid_manifest,
        index=index_manifest,
        promoted_keys=promoted_keys,
        replication_sequence=opts.replication_sequence,
    )
    manifest_mod.write_manifest(opts.root, man, gen_number)
    _log(f"wrote manifest/{gen_number}.json and manifest/LATEST")
    _log(f"done in {time.time()-t_start:.1f}s total")
    con.close()
    return man


# --------------------------------------------------------------------------
# bbox extract (Osmium "smart" semantics)
# --------------------------------------------------------------------------


def _apply_bbox_extract(con, bbox: tuple[float, float, float, float]) -> None:
    south, west, north, east = bbox
    south_e7, west_e7, north_e7, east_e7 = (
        round(south * 1e7),
        round(west * 1e7),
        round(north * 1e7),
        round(east * 1e7),
    )
    con.execute(f"""
        CREATE TEMP TABLE node_in_bbox AS
        SELECT id FROM node0
        WHERE lat_e7 BETWEEN {south_e7} AND {north_e7}
          AND lon_e7 BETWEEN {west_e7} AND {east_e7}
    """)
    con.execute("""
        CREATE TEMP TABLE way_keep AS
        SELECT w.id FROM way0 w
        WHERE EXISTS (
            SELECT 1 FROM UNNEST(w.refs) AS t(ref)
            WHERE t.ref IN (SELECT id FROM node_in_bbox)
        )
    """)
    con.execute("""
        CREATE TEMP TABLE node_keep AS
        SELECT id FROM node_in_bbox
        UNION
        SELECT DISTINCT unnest(refs) AS id FROM way0 WHERE id IN (SELECT id FROM way_keep)
    """)
    con.execute("""
        CREATE TEMP TABLE relation_keep AS
        SELECT DISTINCT r.id FROM relation0 r, UNNEST(r.members) AS t(m)
        WHERE (m.type = 'n' AND m.ref IN (SELECT id FROM node_keep))
           OR (m.type = 'w' AND m.ref IN (SELECT id FROM way_keep))
           OR (m.type = 'r' AND m.ref IN (SELECT id FROM relation0))
    """)
    con.execute("CREATE TEMP TABLE node0_f AS SELECT * FROM node0 WHERE id IN (SELECT id FROM node_keep)")
    con.execute("CREATE TEMP TABLE way0_f AS SELECT * FROM way0 WHERE id IN (SELECT id FROM way_keep)")
    con.execute(
        "CREATE TEMP TABLE relation0_f AS SELECT * FROM relation0 WHERE id IN (SELECT id FROM relation_keep)"
    )
    con.execute("DROP TABLE node0")
    con.execute("DROP TABLE way0")
    con.execute("DROP TABLE relation0")
    con.execute("ALTER TABLE node0_f RENAME TO node0")
    con.execute("ALTER TABLE way0_f RENAME TO way0")
    con.execute("ALTER TABLE relation0_f RENAME TO relation0")


# --------------------------------------------------------------------------
# leaf cell selection
# --------------------------------------------------------------------------


def _select_leaf_cells(con, max_nodes_per_cell: int) -> list[str]:
    lat_e7, lon_e7 = con.execute("SELECT lat_e7, lon_e7 FROM node0").fetchnumpy().values()
    if len(lat_e7) == 0:
        return [cells_mod.ROOT]
    qk = cells_mod.point_to_qk_np(lat_e7, lon_e7)
    codes, counts = np.unique(qk, return_counts=True)
    order = np.argsort(codes)
    codes = codes[order]
    counts = counts[order]
    cum = np.concatenate(([0], np.cumsum(counts)))

    def range_count(lo: int, hi: int) -> int:
        # sum of counts for codes in [lo, hi], via searchsorted on the sorted codes
        i0 = int(np.searchsorted(codes, np.uint64(lo), side="left"))
        i1 = int(np.searchsorted(codes, np.uint64(hi), side="right"))
        return int(cum[i1] - cum[i0])

    leaves: list[str] = []

    def recurse(key: str, depth: int) -> None:
        lo, hi = cells_mod.qk_range(key)
        n = range_count(lo, hi)
        if n <= max_nodes_per_cell or depth >= cells_mod.MAX_DEPTH:
            if n > 0 or key == cells_mod.ROOT:
                leaves.append(key)
            return
        for child in cells_mod.children(key):
            recurse(child, depth + 1)

    recurse(cells_mod.ROOT, 0)
    return leaves


def _assign_loose_cells(con, table: str, leaf_index: cells_mod.LeafIndex) -> None:
    rows = con.execute(f"SELECT id, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM {table}").fetchall()
    ids = []
    cell_vals = []
    hilbert_vals = []
    for rid, ymin, xmin, ymax, xmax in rows:
        ids.append(rid)
        if ymin is None:
            cell_vals.append(cells_mod.ROOT)
            hilbert_vals.append(0)
            continue
        south, west, north, east = ymin / 1e7, xmin / 1e7, ymax / 1e7, xmax / 1e7
        cell_vals.append(cells_mod.containing_cell((south, west, north, east), leaf_index))
        clat_e7 = round((ymin + ymax) / 2.0)
        clon_e7 = round((xmin + xmax) / 2.0)
        hilbert_vals.append(int(hilbert_mod.hilbert_key(clat_e7, clon_e7)))
    ids_arr = np.array(ids, dtype=np.int64)
    cell_arr = np.array(cell_vals, dtype=object)
    hilbert_arr = np.array(hilbert_vals, dtype=np.uint64)
    _register_assignment(con, f"{table}_assign", ids_arr, cell_arr, hilbert_arr)
    con.execute(f"""
        CREATE TABLE {table}_c AS
        SELECT t.*, a.cell, a.hilbert FROM {table} t JOIN {table}_assign a USING (id)
    """)
    con.execute(f"DROP TABLE {table}")
    con.execute(f"ALTER TABLE {table}_c RENAME TO {table}")


def _register_assignment(con, name: str, ids: np.ndarray, cell: np.ndarray, hilbert: np.ndarray) -> None:
    import pyarrow as pa

    tbl = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "cell": pa.array([str(c) for c in cell], type=pa.string()),
            "hilbert": pa.array(hilbert, type=pa.uint64()),
        }
    )
    con.register(f"_{name}_arrow", tbl)
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _{name}_arrow")
    con.unregister(f"_{name}_arrow")


# --------------------------------------------------------------------------
# writers
# --------------------------------------------------------------------------


def _promoted_select(promoted_keys: list[str], tags_expr: str = "tags") -> str:
    return ", ".join(f'{tags_expr}[\'{key}\'][1] AS "{key}"' for key in promoted_keys)


def _copy_to_parquet(con, select_sql: str, path: Path) -> tuple[int, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    posix = str(path).replace("'", "''")
    con.execute(f"""
        COPY ({select_sql}) TO '{posix}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
    """)
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{posix}')").fetchone()[0]
    size = path.stat().st_size
    return rows, size


def _rel_path(generation: str, *parts: str) -> str:
    return "/".join([p.strip("/") for p in ("spatial", generation, *parts)])


def _write_node_spatial(con, root: str, generation: str, promoted_keys: list[str]) -> dict:
    promoted_sql = _promoted_select(promoted_keys)
    cells_rows = con.execute("SELECT DISTINCT cell FROM node1").fetchall()
    out: dict = {"cells": {}}
    for (cell,) in cells_rows:
        entry: dict = {}
        untagged_promoted_sql = ", ".join(f'NULL::VARCHAR AS "{k}"' for k in promoted_keys)
        for tagged, suffix in ((True, "true"), (False, "false")):
            tag_cond = "tags IS NOT NULL" if tagged else "tags IS NULL"
            tags_col = "tags" if tagged else "NULL::MAP(VARCHAR,VARCHAR)"
            this_promoted_sql = promoted_sql if tagged else untagged_promoted_sql
            select_cols = (
                f"id, lat_e7, lon_e7, "
                f"{tags_col} AS tags, "
                f"{this_promoted_sql}, "
                f"version, changeset, timestamp, uid, \"user\", hilbert"
            )
            select_sql = (
                f"SELECT {select_cols} FROM node1 WHERE cell = '{cell}' AND ({tag_cond}) "
                f"ORDER BY hilbert, id"
            )
            path = Path(root) / _rel_path(generation, "node", f"cell={cell}", f"tagged={suffix}", "part-0.parquet")
            rows, size = _copy_to_parquet(con, select_sql, path)
            if rows > 0:
                entry[("tagged" if tagged else "untagged")] = {
                    "path": _rel_path(generation, "node", f"cell={cell}", f"tagged={suffix}", "part-0.parquet"),
                    "rows": rows,
                    "bytes": size,
                }
        if entry:
            out["cells"][cell] = entry
    return out


def _way_relation_select(promoted_keys: list[str], table: str) -> str:
    promoted_sql = _promoted_select(promoted_keys)
    if table == "way1":
        return (
            f"id, refs, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
            f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, geometry, is_closed, is_area, "
            f"centroid_lat_e7, centroid_lon_e7, cell, hilbert"
        )
    return (
        f"id, members, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, NULL::GEOMETRY AS geometry, "
        f"centroid_lat_e7, centroid_lon_e7, cell, hilbert"
    )


def _write_loose_spatial(con, root: str, generation: str, table_name: str, src_table: str, promoted_keys: list[str]) -> dict:
    select_cols = _way_relation_select(promoted_keys, src_table)
    cells_rows = con.execute(f"SELECT DISTINCT cell FROM {src_table}").fetchall()
    out: dict = {"cells": {}}
    for (cell,) in cells_rows:
        select_sql = f"SELECT {select_cols} FROM {src_table} WHERE cell = '{cell}' ORDER BY hilbert, id"
        path = Path(root) / _rel_path(generation, table_name, f"cell={cell}", "part-0.parquet")
        rows, size = _copy_to_parquet(con, select_sql, path)
        if rows == 0:
            continue
        bbox = con.execute(
            f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
            f"FROM {src_table} WHERE cell = '{cell}'"
        ).fetchone()
        out["cells"][cell] = {
            "path": _rel_path(generation, table_name, f"cell={cell}", "part-0.parquet"),
            "rows": rows,
            "bytes": size,
            "bbox": [float(x) if x is not None else None for x in bbox],
        }
    return out


def _write_byid(con, root: str, generation: str, table_name: str, promoted_keys: list[str]) -> list[dict]:
    promoted_sql = _promoted_select(promoted_keys)
    if table_name == "node":
        src = "node1"
        select_cols = (
            f"id, lat_e7, lon_e7, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", cell"
        )
    elif table_name == "way":
        src = "way1"
        select_cols = (
            f"id, refs, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
            f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, is_closed, is_area, cell"
        )
    else:
        src = "relation1"
        select_cols = (
            f"id, members, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
            f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell"
        )

    total = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    if total == 0:
        return []
    con.execute(f"CREATE TEMP TABLE _byid_numbered AS SELECT {select_cols}, "
                f"row_number() OVER (ORDER BY id) AS rn FROM {src}")
    parts = []
    n_parts = (total + BYID_PART_ROWS - 1) // BYID_PART_ROWS
    out_cols = select_cols  # same columns minus rn
    for k in range(n_parts):
        lo = k * BYID_PART_ROWS + 1
        hi = min((k + 1) * BYID_PART_ROWS, total)
        select_sql = f"SELECT {out_cols} FROM _byid_numbered WHERE rn BETWEEN {lo} AND {hi} ORDER BY id"
        path = Path(root) / "byid" / generation / table_name / f"part-{k:05d}.parquet"
        rows, size = _copy_to_parquet(con, select_sql, path)
        min_id, max_id = con.execute(
            f"SELECT min(id), max(id) FROM _byid_numbered WHERE rn BETWEEN {lo} AND {hi}"
        ).fetchone()
        parts.append({
            "path": f"byid/{generation}/{table_name}/part-{k:05d}.parquet",
            "min_id": min_id,
            "max_id": max_id,
            "rows": rows,
            "bytes": size,
        })
    con.execute("DROP TABLE _byid_numbered")
    return parts


def _write_node_way_index(con, root: str, generation: str) -> list[dict]:
    con.execute("""
        CREATE TEMP TABLE _nw_index AS
        SELECT t.ref AS node_id, w.id AS way_id
        FROM way1 w, UNNEST(w.refs) AS t(ref)
        ORDER BY node_id, way_id
    """)
    total = con.execute("SELECT count(*) FROM _nw_index").fetchone()[0]
    parts = []
    if total > 0:
        con.execute("CREATE TEMP TABLE _nw_numbered AS SELECT *, row_number() OVER (ORDER BY node_id, way_id) AS rn FROM _nw_index")
        n_parts = (total + BYID_PART_ROWS - 1) // BYID_PART_ROWS
        for k in range(n_parts):
            lo = k * BYID_PART_ROWS + 1
            hi = min((k + 1) * BYID_PART_ROWS, total)
            select_sql = f"SELECT node_id, way_id FROM _nw_numbered WHERE rn BETWEEN {lo} AND {hi} ORDER BY node_id, way_id"
            path = Path(root) / "index" / generation / "node_way" / f"part-{k:05d}.parquet"
            rows, size = _copy_to_parquet(con, select_sql, path)
            min_id, max_id = con.execute(
                f"SELECT min(node_id), max(node_id) FROM _nw_numbered WHERE rn BETWEEN {lo} AND {hi}"
            ).fetchone()
            parts.append({
                "path": f"index/{generation}/node_way/part-{k:05d}.parquet",
                "min_id": min_id,
                "max_id": max_id,
                "rows": rows,
                "bytes": size,
            })
        con.execute("DROP TABLE _nw_numbered")
    con.execute("DROP TABLE _nw_index")
    return parts


def _write_member_index(con, root: str, generation: str) -> list[dict]:
    con.execute("""
        CREATE TEMP TABLE _mem_index AS
        SELECT m.type AS member_type, m.ref AS member_id, r.id AS parent_id, m.role AS role, r.cell AS parent_cell
        FROM relation1 r, UNNEST(r.members) AS t(m)
        ORDER BY member_type, member_id, parent_id
    """)
    total = con.execute("SELECT count(*) FROM _mem_index").fetchone()[0]
    parts = []
    if total > 0:
        con.execute("CREATE TEMP TABLE _mem_numbered AS SELECT *, row_number() OVER (ORDER BY member_type, member_id, parent_id) AS rn FROM _mem_index")
        n_parts = (total + BYID_PART_ROWS - 1) // BYID_PART_ROWS
        for k in range(n_parts):
            lo = k * BYID_PART_ROWS + 1
            hi = min((k + 1) * BYID_PART_ROWS, total)
            select_sql = (
                f"SELECT member_type, member_id, parent_id, role, parent_cell FROM _mem_numbered "
                f"WHERE rn BETWEEN {lo} AND {hi} ORDER BY member_type, member_id, parent_id"
            )
            path = Path(root) / "index" / generation / "member" / f"part-{k:05d}.parquet"
            rows, size = _copy_to_parquet(con, select_sql, path)
            parts.append({
                "path": f"index/{generation}/member/part-{k:05d}.parquet",
                "rows": rows,
                "bytes": size,
            })
        con.execute("DROP TABLE _mem_numbered")
    con.execute("DROP TABLE _mem_index")
    return parts
