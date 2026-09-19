"""``osmpq areas <root>``: docs/m3-contracts.md section 4 (4.1-4.3).

Derives the `area` table from the current generation's relations
(``type=multipolygon``/``type=boundary`` whose member ways assemble into at
least one valid ring) and closed ``is_area`` ways carrying at least one
qualifying key (4.1), and writes ``spatial/<gen>/area/cell=<cell>/
part-0.parquet`` plus a single ``index/<gen>/areas.parquet`` (4.2), then a
new manifest (v4, additive: ``tables``/``byid``/``index`` for node/way/
relation are untouched, only a top-level ``areas`` field is added and the
manifest number bumped).

Way-area geometry is a single ``ST_MakePolygon`` over the way's own stored
LINESTRING -- no assembly needed. Relation-area geometry (multipolygon
ring assembly, including holes) runs in Python with ``shapely`` (never in
the engine, per 4.1): member ways are merged into rings with
``shapely.ops.linemerge``, closed rings become polygons, inner rings are
subtracted from the outer polygons that contain them, and the result is
``shapely.validation.make_valid``-ed.

Reads relations/ways through ``osmpq.engine.sources.current_rows`` (the
same M2 delta-shadowing helper the query engine uses), so this runs
correctly on a dataset that has replication deltas (4.3): deltas
themselves carry no area rows, so newly-delta'd pivots are picked up only
once ``osmpq compact`` folds them into a base generation and re-derives
areas for the touched pivots (see ``derive_areas_for_pivots`` below, used
by ``osmpq.build.compact``).

Manifest writes go through the temp-file + ``os.replace`` pattern (dataset
roots are routinely hardlinked with ``cp -al``; an in-place write would
corrupt every other snapshot sharing that inode), matching
``osmpq.layout.manifest`` and ``osmpq.build.compact``.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

from osmpq.build import common
from osmpq.engine import catalog, idset, sources
from osmpq.engine import hilbert as engine_hilbert_mod
from osmpq.layout import cells as cells_mod
from osmpq.layout import hilbert as hilbert_mod

WAY_ID_OFFSET = 2_400_000_000
RELATION_ID_OFFSET = 3_600_000_000

# docs/m3-contracts.md section 4.1: a closed `is_area` way is an area only
# if it carries at least one of these keys (bare buildings are excluded).
QUALIFYING_KEYS = [
    "name", "ref", "admin_level", "boundary", "place", "postal_code",
    "addr:postcode", "landuse", "natural", "leisure", "amenity",
    "tourism", "historic", "military", "aeroway", "water", "area",
]

AREA_SPATIAL_ROW_GROUP_BYTES = 1_000_000
AREA_INDEX_ROW_GROUP_BYTES = 4_000_000

DEFAULT_PROMOTED_KEYS = [
    "amenity", "shop", "highway", "building", "name", "natural",
    "landuse", "leisure", "railway", "waterway", "place", "tourism",
]


def _log(msg: str) -> None:
    common.log("osmpq areas", msg)


def _esc(s: str) -> str:
    return str(s).replace("'", "''")


def _quote_list(paths: list[str]) -> str:
    return "[" + ",".join("'" + _esc(p) + "'" for p in paths) + "]"


def _to_e7(deg: float) -> int:
    return int(round(deg * 1e7))


def _files_for_cells(manifest: catalog.Manifest, table: str, cells: list[str]) -> list[str]:
    tc = manifest.table_cells(table)
    return [manifest.path(tc[c]["path"]) for c in cells if c in tc]


@dataclass
class BuildAreasOptions:
    root: str
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    tmpdir: Optional[str] = None


def _connect(threads: Optional[int], memory_limit: Optional[str], tmpdir: Path):
    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    if threads:
        con.execute(f"SET threads={int(threads)}")
    if memory_limit:
        con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET temp_directory='{tmpdir.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    # `sources.py`'s byid hydration paths (the fallback `_relation_area_rows`
    # uses for member ways the bbox-scoped pass didn't resolve) reference
    # the `opq_node_hilbert`/`opq_bbox_hilbert` UDFs the engine normally
    # registers on its own connection (`executor.Engine`) -- register them
    # here too since this module runs its own standalone connection.
    engine_hilbert_mod.register_duckdb_udfs(con)
    return con


# --------------------------------------------------------------------------
# way-derived areas: pure SQL (ST_MakePolygon on the stored LINESTRING)
# --------------------------------------------------------------------------


def _way_area_candidates(con, manifest: catalog.Manifest, way_ids: Optional[list[int]]) -> tuple[str, int]:
    """TEMP TABLE of qualifying way rows (4.1: is_area + >=1 qualifying
    key), restricted to `way_ids` when given (compact's touched-pivot
    re-derive; None means every way -- the full ``osmpq areas`` derive).
    An explicit empty `way_ids` (no touched way pivots this compaction)
    short-circuits to an empty table with no file reads at all."""
    name = "_way_area_cand"
    empty_ddl = (
        f"CREATE OR REPLACE TEMP TABLE {name} ("
        "id BIGINT, tags MAP(VARCHAR, VARCHAR), version INTEGER, changeset BIGINT, "
        "timestamp TIMESTAMP, uid INTEGER, \"user\" VARCHAR, "
        "xmin_e7 INTEGER, ymin_e7 INTEGER, xmax_e7 INTEGER, ymax_e7 INTEGER, geometry GEOMETRY)"
    )
    if way_ids is not None and not way_ids:
        con.execute(empty_ddl)
        return name, 0
    cells = catalog.cells_for_bbox(manifest, "way", None)
    files = _files_for_cells(manifest, "way", cells)
    if not files and not manifest.delta_tiers():
        con.execute(empty_ddl)
        return name, 0
    cols = {
        "type": "'way'", "id": "id", "cell": "cell", "tags": "tags",
        "version": "version", "changeset": "changeset", "timestamp": "timestamp",
        "uid": "uid", "user": '"user"',
        "xmin_e7": "xmin_e7", "ymin_e7": "ymin_e7", "xmax_e7": "xmax_e7", "ymax_e7": "ymax_e7",
        "geometry": "geometry", "hilbert": "hilbert",
    }
    qualifying = " OR ".join(f"tags['{k}'] IS NOT NULL" for k in QUALIFYING_KEYS)
    where = f"is_area AND ({qualifying})"
    if way_ids is not None:
        id_pred = idset.id_predicate(con, "id", way_ids) if way_ids else "FALSE"
        where = f"({where}) AND ({id_pred})"
    sql, nfiles = sources.current_rows(con, manifest, "way", cells, files, cols, where)
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {name} AS "
        f"SELECT id, tags, version, changeset, timestamp, uid, \"user\", "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, geometry FROM ({sql}) t "
        f"WHERE geometry IS NOT NULL AND ST_NPoints(geometry) >= 4"
    )
    return name, nfiles


def _way_area_final(con, cand_table: str) -> str:
    # `is_closed` (raw.py) means refs[0] == refs[-1] by *node id*, which
    # guarantees the ring is geometrically closed -- but real-world
    # coordinate round-tripping (float storage, GeoParquet WKB) can leave
    # the stored first/last vertex a few ULPs apart, which
    # `ST_MakePolygon` rejects outright ("shell must be closed"). Force
    # exact closure by replacing the last vertex with an exact copy of the
    # first, rather than trusting the stored one -- the ring's shape is
    # unaffected (the same node's coordinates either way).
    closed_ring_sql = (
        "ST_MakeLine(list_append("
        "list_slice(list_transform(range(1, ST_NPoints(geometry)::INTEGER), "
        "i -> ST_PointN(geometry, i::INTEGER)), 1, ST_NPoints(geometry)::INTEGER - 1), "
        "ST_PointN(geometry, 1)))"
    )
    name = "_way_area_final"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {name} AS
        SELECT id + {WAY_ID_OFFSET} AS id, 'way' AS pivot_type, id AS pivot_id, tags,
               version, changeset, timestamp, uid, "user",
               xmin_e7, ymin_e7, xmax_e7, ymax_e7,
               ST_MakeValid(ST_MakePolygon({closed_ring_sql})) AS geometry
        FROM {cand_table}
    """)
    con.execute(f"DROP TABLE {cand_table}")
    return name


# --------------------------------------------------------------------------
# relation-derived areas: multipolygon/boundary ring assembly (shapely)
# --------------------------------------------------------------------------


def _rings_from_way_ids(way_ids: list[int], way_wkt: dict[int, str]):
    """`shapely.ops.linemerge` the LINESTRINGs of `way_ids` (role outer or
    inner, per caller) and return the closed rings among the merged result
    as shapely Polygons (4.1)."""
    import shapely.wkt as shapely_wkt
    from shapely.geometry import Polygon
    from shapely.ops import linemerge

    lines = []
    for wid in way_ids:
        wkt_s = way_wkt.get(wid)
        if not wkt_s:
            continue
        try:
            geom = shapely_wkt.loads(wkt_s)
        except Exception:
            continue
        if geom.geom_type == "LineString" and len(geom.coords) >= 2:
            lines.append(geom)
    if not lines:
        return []
    merged = linemerge(lines) if len(lines) > 1 else lines[0]
    parts = list(merged.geoms) if merged.geom_type == "MultiLineString" else [merged]
    rings = []
    for g in parts:
        coords = list(g.coords)
        if len(coords) >= 4 and coords[0] == coords[-1]:
            try:
                poly = Polygon(coords)
                if poly.is_valid and not poly.is_empty:
                    rings.append(poly)
            except Exception:
                continue
    return rings


def _assemble_relation_geometry(outer_way_ids: list[int], inner_way_ids: list[int], way_wkt: dict[int, str]):
    """A relation's derived (Multi)Polygon, or None if no outer ring could
    be assembled (4.1: 'a relation with no ring yields no area')."""
    from shapely.ops import unary_union
    from shapely.validation import make_valid

    outer_rings = _rings_from_way_ids(outer_way_ids, way_wkt)
    if not outer_rings:
        return None
    inner_rings = _rings_from_way_ids(inner_way_ids, way_wkt)
    polys = []
    for op in outer_rings:
        poly = op
        for ip in inner_rings:
            if op.intersects(ip):
                try:
                    poly = poly.difference(ip)
                except Exception:
                    continue
        polys.append(poly)
    geom = polys[0] if len(polys) == 1 else unary_union(polys)
    geom = make_valid(geom)
    return None if geom.is_empty else geom


def _relation_area_rows(con, manifest: catalog.Manifest, relation_ids: Optional[list[int]]) -> tuple[list[dict], int]:
    """Python list of area-row dicts for multipolygon/boundary relations
    (4.1), restricted to `relation_ids` pivots when given (compact's
    touched-pivot re-derive; None means every relation). An explicit empty
    `relation_ids` short-circuits to no rows with no file reads at all."""
    if relation_ids is not None and not relation_ids:
        return [], 0
    files_read = 0
    cells = catalog.cells_for_bbox(manifest, "relation", None)
    files = _files_for_cells(manifest, "relation", cells)
    if not files and not manifest.delta_tiers():
        return [], 0
    cols = {
        "type": "'relation'", "id": "id", "cell": "cell", "members": "members", "tags": "tags",
        "version": "version", "changeset": "changeset", "timestamp": "timestamp",
        "uid": "uid", "user": '"user"',
        "xmin_e7": "xmin_e7", "ymin_e7": "ymin_e7", "xmax_e7": "xmax_e7", "ymax_e7": "ymax_e7",
        "geometry": "NULL::GEOMETRY", "hilbert": "hilbert",
    }
    where = "(tags['type'] = 'multipolygon' OR tags['type'] = 'boundary')"
    if relation_ids is not None:
        id_pred = idset.id_predicate(con, "id", relation_ids) if relation_ids else "FALSE"
        where = f"({where}) AND ({id_pred})"
    sql, nfiles = sources.current_rows(con, manifest, "relation", cells, files, cols, where)
    files_read += nfiles
    con.execute(f"CREATE OR REPLACE TEMP TABLE _rel_area_cand AS SELECT * FROM ({sql}) t")
    n_cand = con.execute("SELECT count(*) FROM _rel_area_cand").fetchone()[0]
    if n_cand == 0:
        con.execute("DROP TABLE _rel_area_cand")
        return [], files_read

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _rel_area_members AS
        SELECT r.id AS rel_id, m.ref AS way_id,
               CASE WHEN m.role = 'inner' THEN 'inner' ELSE 'outer' END AS role
        FROM _rel_area_cand r, UNNEST(r.members) AS t(m)
        WHERE m.type = 'w'
    """)
    way_wkt: dict[int, str] = {}
    n_members = con.execute("SELECT count(*) FROM _rel_area_members").fetchone()[0]
    if n_members:
        con.execute("CREATE OR REPLACE TEMP TABLE _rel_area_way_ids AS SELECT DISTINCT way_id AS id FROM _rel_area_members")
        way_sql, nfiles_w = sources.build_way_hydrate_via_bbox_select(
            con, manifest, "_rel_area_way_ids",
            ["SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM _rel_area_cand"], set(),
        )
        files_read += nfiles_w
        missing_cell_pairs: list[tuple[str, int]] = []
        if way_sql:
            for wid, cell, wkt in con.execute(f"SELECT id, cell, ST_AsText(geometry) FROM ({way_sql}) t").fetchall():
                if wkt:
                    way_wkt[wid] = wkt
                elif cell:
                    missing_cell_pairs.append((cell, wid))
        if missing_cell_pairs:
            # Fallback for anything the bbox-scoped pass resolved via byid
            # (no geometry column there): the (cell, id) spatial join, same
            # as render.py's `hydrate_way_geometry`.
            tc = manifest.table_cells("way")
            needed = sorted({c for c, _ in missing_cell_pairs if c in tc})
            if needed:
                wfiles = [manifest.path(tc[c]["path"]) for c in needed]
                files_read += len(wfiles)
                pairs_tbl = idset.register_pairs_table(con, missing_cell_pairs)
                for wid, wkt in con.execute(
                    f"SELECT t.id, ST_AsText(t.geometry) FROM read_parquet({_quote_list(wfiles)}) t "
                    f"JOIN {pairs_tbl} p ON t.cell = p.cell AND t.id = p.id"
                ).fetchall():
                    if wkt:
                        way_wkt[wid] = wkt
        con.execute("DROP TABLE _rel_area_way_ids")

    members_by_rel: dict[int, dict[str, list[int]]] = {}
    for rel_id, way_id, role in con.execute("SELECT rel_id, way_id, role FROM _rel_area_members").fetchall():
        members_by_rel.setdefault(rel_id, {"outer": [], "inner": []})[role].append(way_id)
    con.execute("DROP TABLE _rel_area_members")

    cand_rows = con.execute(
        "SELECT id, tags, version, changeset, timestamp, uid, \"user\", xmin_e7, ymin_e7, xmax_e7, ymax_e7 "
        "FROM _rel_area_cand"
    ).fetchall()
    con.execute("DROP TABLE _rel_area_cand")

    area_rows: list[dict] = []
    for (rel_id, tags, version, changeset, timestamp, uid, user, xmin, ymin, xmax, ymax) in cand_rows:
        m = members_by_rel.get(rel_id)
        if not m:
            continue
        geom = _assemble_relation_geometry(m["outer"], m["inner"], way_wkt)
        if geom is None:
            continue
        gxmin, gymin, gxmax, gymax = geom.bounds
        area_rows.append({
            "id": rel_id + RELATION_ID_OFFSET,
            "pivot_id": rel_id,
            "tags": dict(tags) if tags else {},
            "version": version, "changeset": changeset, "timestamp": timestamp,
            "uid": uid, "user": user,
            "xmin_e7": _to_e7(gxmin), "ymin_e7": _to_e7(gymin),
            "xmax_e7": _to_e7(gxmax), "ymax_e7": _to_e7(gymax),
            "wkb": geom.wkb,
        })
    return area_rows, files_read


def _relation_area_final(con, area_rows: list[dict]) -> str:
    name = "_rel_area_final"
    con.execute(f"DROP TABLE IF EXISTS {name}")
    con.execute(f"""
        CREATE TEMP TABLE {name} (
            id BIGINT, pivot_type VARCHAR, pivot_id BIGINT, tags MAP(VARCHAR, VARCHAR),
            version INTEGER, changeset BIGINT, timestamp TIMESTAMP, uid INTEGER, "user" VARCHAR,
            xmin_e7 INTEGER, ymin_e7 INTEGER, xmax_e7 INTEGER, ymax_e7 INTEGER, wkb BLOB
        )
    """)
    if area_rows:
        con.executemany(
            f"INSERT INTO {name} VALUES (?, 'relation', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (r["id"], r["pivot_id"], r["tags"], r["version"], r["changeset"], r["timestamp"],
                 r["uid"], r["user"], r["xmin_e7"], r["ymin_e7"], r["xmax_e7"], r["ymax_e7"], r["wkb"])
                for r in area_rows
            ],
        )
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {name} AS
        SELECT id, pivot_type, pivot_id, tags, version, changeset, timestamp, uid, "user",
               xmin_e7, ymin_e7, xmax_e7, ymax_e7, ST_GeomFromWKB(wkb) AS geometry
        FROM {name}
    """)
    return name


# --------------------------------------------------------------------------
# combine + place (cell/hilbert) + write
# --------------------------------------------------------------------------


def derive_areas(
    con, manifest: catalog.Manifest, way_ids: Optional[list[int]] = None, relation_ids: Optional[list[int]] = None,
) -> tuple[str, int]:
    """TEMP TABLE of every derived area row (id, pivot_type, pivot_id, tags,
    meta cols, bbox, geometry -- no cell/hilbert yet), restricted to
    `way_ids`/`relation_ids` pivots when given. Returns (table_name,
    files_read)."""
    way_cand, nfiles_w = _way_area_candidates(con, manifest, way_ids)
    way_final = _way_area_final(con, way_cand)
    rel_rows, nfiles_r = _relation_area_rows(con, manifest, relation_ids)
    rel_final = _relation_area_final(con, rel_rows)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _area_derived AS
        SELECT * FROM {way_final}
        UNION ALL
        SELECT * FROM {rel_final}
    """)
    con.execute(f"DROP TABLE {way_final}")
    con.execute(f"DROP TABLE {rel_final}")
    return "_area_derived", nfiles_w + nfiles_r


def place_areas(con, table_name: str, manifest: catalog.Manifest, promoted_keys: list[str]) -> str:
    """Adds promoted columns + cell/hilbert (v2 loose placement,
    docs/m1-contracts.md section 2) to `table_name`'s rows. Returns a new
    TEMP TABLE name ready to write out."""
    promoted_sql = common.promoted_select(promoted_keys)
    leaves = manifest.leaf_cells
    ancestor_depths = manifest.ancestor_depths or cells_mod.DEFAULT_ANCESTOR_DEPTHS
    max_depth = manifest.max_depth or cells_mod.DEFAULT_MAX_DEPTH_V2
    leaf_index = cells_mod.LeafIndex(leaves) if leaves else cells_mod.LeafIndex([cells_mod.ROOT])

    # `id` (way_id + WAY_ID_OFFSET / relation_id + RELATION_ID_OFFSET) is
    # *not* guaranteed unique across pivot types: a way id >= ~1.2 billion
    # (routine in modern OSM) plus WAY_ID_OFFSET can equal some relation's
    # id plus RELATION_ID_OFFSET (the same ambiguity real Overpass's own
    # `way_id+2400000000`/`relation_id+3600000000` scheme has). Joining the
    # per-row cell/hilbert assignment back by `id` would then cross-
    # multiply rows for a colliding id (each of the 2 pivots' rows joining
    # both of the 2 assignment rows), silently corrupting *other* pivots'
    # cell placement too -- so join on a synthetic unique row key instead,
    # never on `id`.
    keyed = "_area_keyed"
    con.execute(f"CREATE OR REPLACE TEMP TABLE {keyed} AS SELECT row_number() OVER () AS __rk, * FROM {table_name}")
    con.execute(f"DROP TABLE {table_name}")

    rks, ymin, xmin, ymax, xmax = con.execute(
        f"SELECT __rk, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM {keyed}"
    ).fetchnumpy().values()
    n = len(rks)
    if n == 0:
        cell = np.array([], dtype=object)
        hilbert = np.array([], dtype=np.uint64)
    else:
        ymin_a = np.asarray(ymin, dtype=np.int64)
        xmin_a = np.asarray(xmin, dtype=np.int64)
        ymax_a = np.asarray(ymax, dtype=np.int64)
        xmax_a = np.asarray(xmax, dtype=np.int64)
        cell = cells_mod.containing_cells_v2_np(ymin_a, xmin_a, ymax_a, xmax_a, leaf_index, ancestor_depths, max_depth)
        clat_e7 = np.round((ymin_a + ymax_a) / 2.0).astype(np.int64)
        clon_e7 = np.round((xmin_a + xmax_a) / 2.0).astype(np.int64)
        hilbert = hilbert_mod.hilbert_keys(clat_e7, clon_e7)
    common.register_assignment(con, "_area_assign", np.asarray(rks, dtype=np.int64), cell, hilbert)
    con.execute("ALTER TABLE _area_assign RENAME id TO __rk")
    out_name = "_area_placed"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {out_name} AS
        SELECT t.id, t.pivot_type, t.pivot_id, t.tags, {promoted_sql},
               t.version, t.changeset, t.timestamp, t.uid, t."user",
               t.xmin_e7, t.ymin_e7, t.xmax_e7, t.ymax_e7, t.geometry,
               a.cell, a.hilbert
        FROM {keyed} t JOIN _area_assign a USING (__rk)
    """)
    con.execute(f"DROP TABLE {keyed}")
    con.execute("DROP TABLE _area_assign")
    return out_name


def write_area_layout(con, root: Path, generation: str, placed_table: str, promoted_keys: list[str]) -> dict:
    """Writes spatial/<gen>/area/cell=<cell>/part-0.parquet for every cell
    present in `placed_table` and a single index/<gen>/areas.parquet
    (4.2). Returns the `areas` manifest field
    ({"index": {...}, "cells": {...}})."""
    # ROW_GROUP_SIZE_BYTES (used below for the ~1MB/4MB targets from 4.2)
    # requires insertion order not be preserved; callers may pass a
    # connection that hasn't set this (e.g. a test fixture's own
    # connection), so set it here rather than assuming it.
    con.execute("SET preserve_insertion_order=false")
    promoted_cols = ", ".join(f'"{k}"' for k in promoted_keys)
    cells_manifest: dict = {}
    for (cell,) in con.execute(f"SELECT DISTINCT cell FROM {placed_table}").fetchall():
        rel_path = f"spatial/{generation}/area/cell={cell}/part-0.parquet"
        path = root / rel_path
        select_sql = (
            f"SELECT id, pivot_type, pivot_id, tags, {promoted_cols}, "
            f'version, changeset, timestamp, uid, "user", '
            f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, geometry, cell, hilbert "
            f"FROM {placed_table} WHERE cell = '{_esc(cell)}' ORDER BY hilbert, id"
        )
        rows, size = common.copy_to_parquet(con, select_sql, path, row_group_size_bytes=AREA_SPATIAL_ROW_GROUP_BYTES)
        if rows == 0:
            path.unlink()
            continue
        bbox = con.execute(
            f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
            f"FROM {placed_table} WHERE cell = '{_esc(cell)}'"
        ).fetchone()
        cells_manifest[cell] = {
            "path": rel_path, "rows": rows, "bytes": size,
            "bbox": [float(x) if x is not None else None for x in bbox],
        }

    index_rel = f"index/{generation}/areas.parquet"
    index_path = root / index_rel
    index_sql = (
        f"SELECT id, pivot_type, pivot_id, tags, {promoted_cols}, "
        f'version, changeset, timestamp, uid, "user", '
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell, hilbert "
        f"FROM {placed_table} ORDER BY id"
    )
    index_rows, index_size = common.copy_to_parquet(con, index_sql, index_path, row_group_size_bytes=AREA_INDEX_ROW_GROUP_BYTES)
    return {"index": {"path": index_rel, "rows": index_rows, "bytes": index_size}, "cells": cells_manifest}


def build_areas_for_manifest(con, root: Path, cat_manifest: catalog.Manifest, promoted_keys: list[str]) -> dict:
    """Full derivation (every qualifying way/relation) against
    `cat_manifest`'s *current* tables, writing spatial/index files under
    `cat_manifest.generation`. Returns the new `areas` manifest field to
    merge into the caller's manifest dict; does not read or write
    `cat_manifest.data["areas"]` itself, so callers (``osmpq areas``,
    ``osmpq compact``) control how the result folds into their manifest."""
    derived, _files_read = derive_areas(con, cat_manifest)
    placed = place_areas(con, derived, cat_manifest, promoted_keys)
    return write_area_layout(con, root, cat_manifest.generation, placed, promoted_keys)


def derive_areas_for_pivots(
    con, cat_manifest: catalog.Manifest, promoted_keys: list[str],
    way_ids: list[int], relation_ids: list[int],
) -> tuple[str, int]:
    """Re-derives area rows for exactly the given touched pivots (used by
    ``osmpq compact``, 4.3): returns (TEMP TABLE of new/changed area rows
    incl. cell/hilbert, files_read). A pivot that no longer qualifies (its
    way lost its qualifying tag / stopped being closed, its relation lost
    its ring, or the pivot was deleted) simply produces no row here --
    callers remove its old area row by id."""
    derived, files_read = derive_areas(con, cat_manifest, way_ids=way_ids, relation_ids=relation_ids)
    placed = place_areas(con, derived, cat_manifest, promoted_keys)
    return placed, files_read


# --------------------------------------------------------------------------
# top-level `osmpq areas` orchestration
# --------------------------------------------------------------------------


def build_areas(opts: BuildAreasOptions) -> dict:
    t_start = time.time()
    root = Path(opts.root)
    tmpdir = Path(opts.tmpdir) if opts.tmpdir else Path.cwd() / ".osmpq-areas-tmp"
    tmpdir.mkdir(parents=True, exist_ok=True)

    manifest_dir = root / "manifest"
    latest_num = int((manifest_dir / "LATEST").read_text().strip())
    old_man = json.loads((manifest_dir / f"{latest_num}.json").read_text())
    promoted_keys = list(old_man.get("promoted_keys") or DEFAULT_PROMOTED_KEYS)

    con = _connect(opts.threads, opts.memory_limit, tmpdir)
    cat_manifest = catalog.Manifest(root=str(root), data=old_man)

    area_field = build_areas_for_manifest(con, root, cat_manifest, promoted_keys)
    con.close()

    new_man = copy.deepcopy(old_man)
    new_man["manifest_version"] = max(4, int(old_man.get("manifest_version", 1)))
    new_man["areas"] = area_field
    stats = dict(new_man.get("stats") or {})
    stats["areas"] = area_field["index"]["rows"]
    new_man["stats"] = stats

    gen_number = latest_num + 1
    manifest_dir.mkdir(parents=True, exist_ok=True)
    # Temp-file + os.replace: manifest files may be hardlinked across
    # snapshot copies (cp -al); an in-place write would corrupt every other
    # copy sharing that inode (see layout/manifest.py's _atomic_write_text
    # and build/compact.py's manifest write).
    for name, text in ((f"{gen_number}.json", json.dumps(new_man, indent=2, sort_keys=False)),
                       ("LATEST", str(gen_number))):
        tmp = manifest_dir / f".{name}.tmp"
        tmp.write_text(text)
        os.replace(tmp, manifest_dir / name)
    _log(
        f"derived {area_field['index']['rows']} area(s) across {len(area_field['cells'])} cell(s) "
        f"in {time.time() - t_start:.1f}s; wrote manifest/{gen_number}.json"
    )
    return new_man


def areas_main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="osmpq areas", description=__doc__)
    p.add_argument("root")
    p.add_argument("--threads", type=int, default=None)
    p.add_argument("--memory-limit", default=None)
    p.add_argument("--tmpdir", default=None)
    args = p.parse_args(argv if argv is not None else sys.argv[1:])
    build_areas(BuildAreasOptions(
        root=args.root, threads=args.threads, memory_limit=args.memory_limit, tmpdir=args.tmpdir,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(areas_main())
