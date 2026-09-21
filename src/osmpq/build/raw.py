"""``osmpq raw-py``: a Python/DuckDB producer of the ``raw/`` layout from
docs/m1-contracts.md sections 1 and 3, so the Python build stages
(``osmpq build --raw``) can be developed and tested without the Rust
``osmpq-raw`` binary.

Reuses the M0 reading approach (``ST_ReadOSM`` for structure, community
``osmium_read`` for metadata joined back by ``(type, id)``) -- see
``docs/m0-contracts.md`` section 5 and the module docstring of the original
``osmpq.build.builder``. As allowed by the M1 contract, this producer leaves
untagged-node metadata NULL (``osmium_read`` only returns tagged nodes).

Ways and relations use the v2 loose-placement rule
(``osmpq.layout.cells.containing_cells_v2_np``), vectorized over numpy so
that millions of ways are placed in well under a minute instead of the M0
per-row Python loop.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

from osmpq.build import common
from osmpq.layout import cells as cells_mod
from osmpq.layout import hilbert as hilbert_mod

PRODUCER_NAME = "osmpq raw-py"

NODE_BYID_PART_ROWS = 4_000_000
NODE_BYID_ROW_GROUP = 64_000
NODE_SPATIAL_ROW_GROUP = 100_000  # unchanged M0 default

WAY_BYID_PART_ROWS = 1_000_000
WAY_BYID_ROW_GROUP_BYTES = 1_000_000
WAY_SPATIAL_ROW_GROUP_BYTES = 1_500_000

RELATION_PART_ROWS = common.RANGE_TARGET_ROWS


def _log(msg: str) -> None:
    common.log("osmpq raw-py", msg)


@dataclass
class RawBuildOptions:
    pbf_path: str
    rawdir: str
    bbox: Optional[tuple[float, float, float, float]] = None  # s,w,n,e
    max_nodes_per_cell: int = 1_000_000
    max_depth: int = cells_mod.DEFAULT_MAX_DEPTH_V2
    ancestor_depths: list[int] = None  # default set in raw_build
    promoted_keys: Optional[list[str]] = None
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    tmpdir: Optional[str] = None


def raw_build(opts: RawBuildOptions) -> dict:
    t_start = time.time()
    timer = common.Timer()
    ancestor_depths = list(opts.ancestor_depths or cells_mod.DEFAULT_ANCESTOR_DEPTHS)
    from osmpq.layout import manifest as manifest_mod

    promoted_keys = list(opts.promoted_keys or manifest_mod.DEFAULT_PROMOTED_KEYS)

    rawdir = Path(opts.rawdir)
    rawdir.mkdir(parents=True, exist_ok=True)
    tmpdir = Path(opts.tmpdir) if opts.tmpdir else rawdir / ".osmpq-raw-tmp"
    tmpdir.mkdir(parents=True, exist_ok=True)

    import duckdb

    db_path = tmpdir / "raw-build.duckdb"
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

    pbf = opts.pbf_path.replace("'", "''")
    _log(f"reading {opts.pbf_path} ...")
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
    _log(f"read {sum(counts.values())} raw rows {counts} in {timer.lap('read_pbf'):.1f}s")

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

    # ---- leaf cell selection --------------------------------------------------
    leaves = common.select_leaf_cells(con, "node0", opts.max_nodes_per_cell, opts.max_depth)
    leaf_index = cells_mod.LeafIndex(leaves)
    _log(f"selected {len(leaves)} leaf cells in {timer.lap('leaves'):.1f}s")

    # ---- node cell + hilbert (vectorized) --------------------------------------
    ids, lat_e7, lon_e7 = con.execute("SELECT id, lat_e7, lon_e7 FROM node0").fetchnumpy().values()
    if len(ids) > 0:
        qk = cells_mod.point_to_qk_np(lat_e7, lon_e7)
        leaf_idx_arr = leaf_index.leaf_index_for_qk(qk)
        leaf_keys_arr = np.array(leaf_index.leaves_by_lo, dtype=object)
        node_cell = leaf_keys_arr[leaf_idx_arr]
        node_hilbert = hilbert_mod.hilbert_keys(lat_e7, lon_e7)
        common.register_assignment(con, "node_assign", ids, node_cell, node_hilbert)
        con.execute("""
            CREATE TABLE node1 AS
            SELECT n.*, a.cell, a.hilbert FROM node0 n JOIN node_assign a USING (id)
        """)
    else:
        con.execute("""
            CREATE TABLE node1 AS
            SELECT *, NULL::VARCHAR AS cell, NULL::UBIGINT AS hilbert FROM node0 WHERE FALSE
        """)
    _log(f"node cell/hilbert assigned in {timer.lap('node_cell'):.1f}s")

    # ---- way geometry / bbox ----------------------------------------------------
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
                    ELSE CAST(round((g.ymin_e7::BIGINT + g.ymax_e7) / 2.0) AS INTEGER) END AS centroid_lat_e7,
               CASE WHEN g.xmin_e7 IS NULL THEN NULL
                    ELSE CAST(round((g.xmin_e7::BIGINT + g.xmax_e7) / 2.0) AS INTEGER) END AS centroid_lon_e7
        FROM way0 w JOIN way_geom g ON g.id = w.id
    """)
    con.execute("ALTER TABLE way1 ADD COLUMN is_area BOOLEAN")
    con.execute("""
        UPDATE way1 SET is_area = (
            is_closed
            AND coalesce(tags['area'], '') != 'no'
            AND NOT (
                (tags['highway'] IS NOT NULL OR tags['barrier'] IS NOT NULL)
                AND coalesce(tags['area'], '') != 'yes'
            )
        )
    """)
    con.execute("DROP TABLE way_geom")
    _log(f"way geometry/bbox built in {timer.lap('way_geom'):.1f}s")

    # ---- way cell (v2 rule, vectorized) + hilbert -------------------------------
    way_ids, ymin, xmin, ymax, xmax = con.execute(
        "SELECT id, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM way1"
    ).fetchnumpy().values()
    def _filled_f64(col: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        # fetchnumpy gives a MaskedArray for a nullable INTEGER column;
        # .filled(nan) (not a bare cast, which would silently substitute 0
        # for NULL) plus the mask is how we detect "no resolvable geometry".
        if isinstance(col, np.ma.MaskedArray):
            return col.filled(np.nan).astype("float64"), np.ma.getmaskarray(col)
        arr = np.asarray(col, dtype="float64")
        return arr, np.zeros(len(arr), dtype=bool)

    if len(way_ids) > 0:
        ymin_f, ymin_null = _filled_f64(ymin)
        xmin_f, xmin_null = _filled_f64(xmin)
        ymax_f, ymax_null = _filled_f64(ymax)
        xmax_f, xmax_null = _filled_f64(xmax)
        has_bbox = ~(ymin_null | xmin_null | ymax_null | xmax_null)

        way_cell = np.full(len(way_ids), cells_mod.ROOT, dtype=object)
        way_hilbert = np.zeros(len(way_ids), dtype=np.uint64)
        if has_bbox.any():
            sub_cell = cells_mod.containing_cells_v2_np(
                ymin_f[has_bbox].astype(np.int64),
                xmin_f[has_bbox].astype(np.int64),
                ymax_f[has_bbox].astype(np.int64),
                xmax_f[has_bbox].astype(np.int64),
                leaf_index,
                ancestor_depths,
                opts.max_depth,
            )
            clat_e7 = np.round((ymin_f[has_bbox] + ymax_f[has_bbox]) / 2.0).astype(np.int64)
            clon_e7 = np.round((xmin_f[has_bbox] + xmax_f[has_bbox]) / 2.0).astype(np.int64)
            sub_hilbert = hilbert_mod.hilbert_keys(clat_e7, clon_e7)
            way_cell[has_bbox] = sub_cell
            way_hilbert[has_bbox] = sub_hilbert
        common.register_assignment(con, "way_assign", np.asarray(way_ids, dtype=np.int64), way_cell, way_hilbert)
        con.execute("""
            CREATE TABLE way1_c AS
            SELECT w.*, a.cell, a.hilbert FROM way1 w JOIN way_assign a USING (id)
        """)
    else:
        con.execute("""
            CREATE TABLE way1_c AS
            SELECT *, NULL::VARCHAR AS cell, NULL::UBIGINT AS hilbert FROM way1 WHERE FALSE
        """)
    con.execute("DROP TABLE way1")
    con.execute("ALTER TABLE way1_c RENAME TO way1")
    _log(f"way cells (v2) assigned in {timer.lap('way_cell'):.1f}s")

    # ---- write everything --------------------------------------------------------
    (rawdir / "spatial").mkdir(parents=True, exist_ok=True)
    leaves_json = {
        "max_nodes_per_cell": opts.max_nodes_per_cell,
        "max_depth": opts.max_depth,
        "ancestor_depths": ancestor_depths,
        "leaves": sorted(leaves),
    }
    (rawdir / "leaves.json").write_text(json.dumps(leaves_json, indent=2))

    node_stats = _write_node_raw(con, rawdir, promoted_keys)
    _log(f"wrote node/ + spatial/node/ in {timer.lap('write_node'):.1f}s")

    way_stats = _write_way_raw(con, rawdir, promoted_keys)
    _log(f"wrote way/ + spatial/way/ in {timer.lap('write_way'):.1f}s")

    relation_stats = _write_relation_raw(con, rawdir, promoted_keys)
    _log(f"wrote relation/ in {timer.lap('write_relation'):.1f}s")

    max_ts = con.execute(
        "SELECT max(x) FROM (SELECT max(timestamp) AS x FROM node1 "
        "UNION ALL SELECT max(timestamp) FROM way1 UNION ALL SELECT max(timestamp) FROM relation0)"
    ).fetchone()[0]
    max_timestamp = max_ts.strftime("%Y-%m-%dT%H:%M:%SZ") if max_ts is not None else None

    if opts.bbox is not None:
        bbox_extent = list(opts.bbox)
    else:
        ext = con.execute(
            "SELECT min(lat_e7)/1e7, min(lon_e7)/1e7, max(lat_e7)/1e7, max(lon_e7)/1e7 FROM node1"
        ).fetchone()
        bbox_extent = [float(x) if x is not None else 0.0 for x in ext]

    summary = {
        "producer": PRODUCER_NAME,
        "source": Path(opts.pbf_path).name,
        "bbox": opts.bbox is not None and list(opts.bbox) or None,
        "counts": {"nodes": n_nodes, "ways": n_ways, "relations": n_relations},
        "max_timestamp": max_timestamp,
        "extent": bbox_extent,
        "promoted_keys": promoted_keys,
        "max_nodes_per_cell": opts.max_nodes_per_cell,
        "max_depth": opts.max_depth,
        "ancestor_depths": ancestor_depths,
        "leaf_cells": len(leaves),
        "node_store": "duckdb-in-memory",
        "timings_seconds": timer.stages,
        "total_seconds": time.time() - t_start,
        "node_stats": node_stats,
        "way_stats": way_stats,
        "relation_stats": relation_stats,
    }
    (rawdir / "summary.json").write_text(json.dumps(summary, indent=2))
    _log(f"done in {time.time()-t_start:.1f}s total")
    con.close()
    if db_path.exists():
        db_path.unlink()
    return summary


# --------------------------------------------------------------------------
# bbox extract (Osmium "smart" semantics) -- identical to the M0 builder
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
# writers
# --------------------------------------------------------------------------


def _write_node_raw(con, rawdir: Path, promoted_keys: list[str]) -> dict:
    promoted_sql = common.promoted_select(promoted_keys)
    untagged_promoted_sql = ", ".join(f'NULL::VARCHAR AS "{k}"' for k in promoted_keys)

    total = con.execute("SELECT count(*) FROM node1").fetchone()[0]
    byid_parts = []
    if total > 0:
        select_cols = (
            f"id, lat_e7, lon_e7, tags, {promoted_sql}, "
            f"version, changeset, timestamp, uid, \"user\", cell, hilbert"
        )
        for k, (lo, hi) in enumerate(common.range_bounds(con, "node1", "id", total, NODE_BYID_PART_ROWS)):
            cond = common.range_cond("id", lo, hi)
            select_sql = f"SELECT {select_cols} FROM node1 WHERE {cond} ORDER BY id"
            path = rawdir / "node" / f"part-{k:05d}.parquet"
            rows, size = common.copy_to_parquet(con, select_sql, path, row_group_size=NODE_BYID_ROW_GROUP)
            min_id, max_id = con.execute(f"SELECT min(id), max(id) FROM node1 WHERE {cond}").fetchone()
            byid_parts.append({"path": str(path.relative_to(rawdir)), "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size})

    cells_rows = con.execute("SELECT DISTINCT cell FROM node1").fetchall()
    spatial_stats = {"cells": 0, "rows": 0}
    for (cell,) in cells_rows:
        for tagged, suffix in ((True, "true"), (False, "false")):
            tag_cond = "tags IS NOT NULL" if tagged else "tags IS NULL"
            tags_col = "tags" if tagged else "NULL::MAP(VARCHAR,VARCHAR)"
            this_promoted_sql = promoted_sql if tagged else untagged_promoted_sql
            select_cols = (
                f"id, lat_e7, lon_e7, {tags_col} AS tags, {this_promoted_sql}, "
                f"version, changeset, timestamp, uid, \"user\", hilbert"
            )
            select_sql = (
                f"SELECT {select_cols} FROM node1 WHERE cell = '{cell}' AND ({tag_cond}) "
                f"ORDER BY hilbert, id"
            )
            path = rawdir / "spatial" / "node" / f"cell={cell}" / f"tagged={suffix}" / "part-0.parquet"
            rows, _size = common.copy_to_parquet(con, select_sql, path, row_group_size=NODE_SPATIAL_ROW_GROUP)
            if rows > 0:
                spatial_stats["cells"] += 1
                spatial_stats["rows"] += rows
            elif path.exists():
                path.unlink()
    return {"byid_parts": len(byid_parts), "byid_rows": total, "spatial": spatial_stats, "byid": byid_parts}


def _write_way_raw(con, rawdir: Path, promoted_keys: list[str]) -> dict:
    promoted_sql = common.promoted_select(promoted_keys)
    total = con.execute("SELECT count(*) FROM way1").fetchone()[0]
    byid_parts = []
    if total > 0:
        select_cols = (
            f"id, refs, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
            f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, is_closed, is_area, cell, hilbert"
        )
        for k, (lo, hi) in enumerate(common.range_bounds(con, "way1", "id", total, WAY_BYID_PART_ROWS)):
            cond = common.range_cond("id", lo, hi)
            select_sql = f"SELECT {select_cols} FROM way1 WHERE {cond} ORDER BY id"
            path = rawdir / "way" / f"part-{k:05d}.parquet"
            rows, size = common.copy_to_parquet(
                con, select_sql, path, row_group_size_bytes=WAY_BYID_ROW_GROUP_BYTES
            )
            min_id, max_id = con.execute(f"SELECT min(id), max(id) FROM way1 WHERE {cond}").fetchone()
            byid_parts.append({"path": str(path.relative_to(rawdir)), "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size})

    select_cols_spatial = (
        f"id, refs, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, geometry, is_closed, is_area, "
        f"centroid_lat_e7, centroid_lon_e7, cell, hilbert"
    )
    cells_rows = con.execute("SELECT DISTINCT cell FROM way1").fetchall()
    spatial_stats = {"cells": 0, "rows": 0}
    for (cell,) in cells_rows:
        select_sql = f"SELECT {select_cols_spatial} FROM way1 WHERE cell = '{cell}' ORDER BY hilbert, id"
        path = rawdir / "spatial" / "way" / f"cell={cell}" / "part-0.parquet"
        rows, _size = common.copy_to_parquet(
            con, select_sql, path, row_group_size_bytes=WAY_SPATIAL_ROW_GROUP_BYTES
        )
        if rows > 0:
            spatial_stats["cells"] += 1
            spatial_stats["rows"] += rows
        elif path.exists():
            path.unlink()
    return {"byid_parts": len(byid_parts), "byid_rows": total, "spatial": spatial_stats, "byid": byid_parts}


def _write_relation_raw(con, rawdir: Path, promoted_keys: list[str]) -> dict:
    promoted_sql = common.promoted_select(promoted_keys)
    total = con.execute("SELECT count(*) FROM relation0").fetchone()[0]
    parts = []
    if total > 0:
        select_cols = (
            f"id, members, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\""
        )
        for k, (lo, hi) in enumerate(common.range_bounds(con, "relation0", "id", total, RELATION_PART_ROWS)):
            cond = common.range_cond("id", lo, hi)
            select_sql = f"SELECT {select_cols} FROM relation0 WHERE {cond} ORDER BY id"
            path = rawdir / "relation" / f"part-{k:05d}.parquet"
            rows, size = common.copy_to_parquet(con, select_sql, path)
            min_id, max_id = con.execute(f"SELECT min(id), max(id) FROM relation0 WHERE {cond}").fetchone()
            parts.append({"path": str(path.relative_to(rawdir)), "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size})
    return {"parts": len(parts), "rows": total, "byid": parts}
