"""``osmpq areas <root>``: docs/m3-contracts.md section 4, amended by
section 9 after probing the reference (Overpass 0.7.62) -- section 9
replaces 4.1 and changes 4.2-4.5, and is what this module implements.

Two tables, derived independently:

* **Relation areas** (`index/<gen>/areas.parquet` +
  `spatial/<gen>/area/cell=<cell>/part-0.parquet`, unchanged shape from
  4.2): relations that assemble into at least one valid ring *and* match
  the reference's own `areas.osm3s` recipe (9 fact 3): `type=multipolygon`
  with a `name`, `type=boundary` with a `name`, an `admin_level` with a
  `name`, a `postal_code`, or an `addr:postcode`. Ring assembly runs in
  Python with ``shapely`` (never in the engine): member ways are merged
  into rings with ``shapely.ops.linemerge``, closed rings become polygons,
  inner rings are subtracted from the outer polygons that contain them,
  and the result is ``shapely.validation.make_valid``-ed.
* **Way areas are not stored as a separate row at all** (9.1): every
  closed way already *is* an area, as its own canonical row, with no
  polygon geometry precomputed (the engine builds it on demand from the
  way's own LINESTRING via `ST_MakePolygon`). This module only writes a
  lightweight *index* over them, `index/<gen>/way_areas.parquet` (9.2):
  closed ways (the stored `is_closed` column -- not `is_area`, which the
  Rust producer additionally excludes highway/barrier loops and
  `area=no` ways that fact 1 says the reference still treats as areas)
  carrying at least one of a *small* set of keys area lookups use in
  practice (`name`, `ref`, `admin_level`, `boundary`, `place`) -- no
  geometry, no ring assembly, just `id`/tags/meta/bbox/cell/hilbert
  copied straight from the way's own row, sorted by `id`.

Reads relations/ways through ``osmpq.engine.sources.current_rows`` (the
same M2 delta-shadowing helper the query engine uses), so this runs
correctly on a dataset that has replication deltas: deltas themselves
carry no area rows, so newly-delta'd pivots are picked up only once
``osmpq compact`` folds them into a base generation and re-derives areas
for the touched pivots (relation areas: `derive_relation_areas_for_pivots`
below; the way index is instead rewritten in full from the compacted way
tables each time, 9.2 -- small, and simpler than merging).

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
from concurrent.futures import ProcessPoolExecutor
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

# docs/m3-contracts.md section 9.2: the keys a *way* index lookup
# (`area[key=value]`) uses in practice -- much narrower than the set of
# keys that make a way an area at all (9 fact 1: every closed way is an
# area for is_in/(area)/(pivot)/map_to_area, regardless of tags).
WAY_AREA_QUALIFYING_KEYS = ["name", "ref", "admin_level", "boundary", "place"]

AREA_SPATIAL_ROW_GROUP_BYTES = 1_000_000
AREA_INDEX_ROW_GROUP_BYTES = 4_000_000
WAY_AREA_INDEX_ROW_GROUP_BYTES = 4_000_000

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
# way areas (9.1-9.2): no stored geometry, no offset id -- just a small
# index over closed ways carrying a qualifying key, keyed by the way's own
# `id`, reusing the way's own `cell`/`hilbert` placement verbatim.
# --------------------------------------------------------------------------


def _way_qualifying_sql(tags_expr: str = "tags") -> str:
    return " OR ".join(f"{tags_expr}['{k}'] IS NOT NULL" for k in WAY_AREA_QUALIFYING_KEYS)


def _way_area_index_candidates(con, manifest: catalog.Manifest, way_ids: Optional[list[int]]) -> tuple[str, int]:
    """TEMP TABLE of closed way rows carrying a qualifying key (9.2):
    `id, tags, meta cols, bbox, cell, hilbert` -- no geometry (the engine
    builds the polygon on demand from the way's own LINESTRING).
    Restricted to `way_ids` when given (unused by the full derive; kept for
    symmetry with the relation-side helpers). An explicit empty `way_ids`
    short-circuits to an empty table with no file reads at all."""
    name = "_way_area_idx_cand"
    empty_ddl = (
        f"CREATE OR REPLACE TEMP TABLE {name} ("
        "id BIGINT, tags MAP(VARCHAR, VARCHAR), version INTEGER, changeset BIGINT, "
        "timestamp TIMESTAMP, uid INTEGER, \"user\" VARCHAR, "
        "xmin_e7 INTEGER, ymin_e7 INTEGER, xmax_e7 INTEGER, ymax_e7 INTEGER, "
        "cell VARCHAR, hilbert UBIGINT)"
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
        "hilbert": "hilbert",
    }
    # `is_closed`, not `is_area`: the Rust producer's `is_area` additionally
    # excludes highway/barrier loops (roundabouts, closed service ways) and
    # `area=no` ways -- but 9 fact 1 says the reference still treats those
    # as areas for is_in/(area)/(pivot)/map_to_area, so the way-area concept
    # here is purely topological (closed = refs[0] == refs[-1], >=4 refs).
    where = f"is_closed AND ({_way_qualifying_sql()})"
    if way_ids is not None:
        id_pred = idset.id_predicate(con, "id", way_ids) if way_ids else "FALSE"
        where = f"({where}) AND ({id_pred})"
    sql, nfiles = sources.current_rows(con, manifest, "way", cells, files, cols, where)
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {name} AS "
        f"SELECT id, tags, version, changeset, timestamp, uid, \"user\", "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell, hilbert FROM ({sql}) t"
    )
    return name, nfiles


def build_way_area_index(
    con, root: Path, generation: str, manifest: catalog.Manifest, promoted_keys: list[str],
    way_ids: Optional[list[int]] = None,
) -> tuple[dict, int]:
    """Writes `index/<gen>/way_areas.parquet` (9.2) from the current
    generation's way rows (delta-aware via `current_rows`, like the
    relation side): closed ways with a qualifying key, sorted by `id`.
    Returns ({"path", "rows", "bytes"}, files_read)."""
    cand_table, files_read = _way_area_index_candidates(con, manifest, way_ids)
    promoted_sql = common.promoted_select(promoted_keys)
    index_rel = f"index/{generation}/way_areas.parquet"
    index_path = root / index_rel
    select_sql = (
        f"SELECT id, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell, hilbert FROM {cand_table} ORDER BY id"
    )
    rows, size = common.copy_to_parquet(con, select_sql, index_path, row_group_size_bytes=WAY_AREA_INDEX_ROW_GROUP_BYTES)
    con.execute(f"DROP TABLE {cand_table}")
    return {"path": index_rel, "rows": rows, "bytes": size}, files_read


def build_way_area_index_from_files(
    con, root: Path, generation: str, way_files: list[str], promoted_keys: list[str],
) -> dict:
    """Full rebuild of `index/<gen>/way_areas.parquet` straight from a
    list of already-compacted way parquet files (no delta layering --
    ``osmpq compact`` calls this with the *new* generation's way byid
    parts, per 9.2: "the way index is rewritten in full from the compacted
    way tables"). Returns {"path", "rows", "bytes"}."""
    promoted_sql = common.promoted_select(promoted_keys)
    index_rel = f"index/{generation}/way_areas.parquet"
    index_path = root / index_rel
    if not way_files:
        empty_sql = (
            "SELECT NULL::BIGINT AS id, NULL::MAP(VARCHAR, VARCHAR) AS tags, "
            + ", ".join(f'NULL::VARCHAR AS "{k}"' for k in promoted_keys)
            + ", NULL::INTEGER AS version, NULL::BIGINT AS changeset, NULL::TIMESTAMP AS \"timestamp\", "
            "NULL::INTEGER AS uid, NULL::VARCHAR AS \"user\", NULL::INTEGER AS xmin_e7, "
            "NULL::INTEGER AS ymin_e7, NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, "
            "NULL::VARCHAR AS cell, NULL::UBIGINT AS hilbert WHERE FALSE"
        )
        rows, size = common.copy_to_parquet(con, empty_sql, index_path, row_group_size_bytes=WAY_AREA_INDEX_ROW_GROUP_BYTES)
        return {"path": index_rel, "rows": rows, "bytes": size}
    select_sql = (
        f"SELECT id, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell, hilbert "
        f"FROM read_parquet({_quote_list(way_files)}, union_by_name=true) "
        f"WHERE is_closed AND ({_way_qualifying_sql()}) ORDER BY id"
    )
    rows, size = common.copy_to_parquet(con, select_sql, index_path, row_group_size_bytes=WAY_AREA_INDEX_ROW_GROUP_BYTES)
    return {"path": index_rel, "rows": rows, "bytes": size}


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
        matching_inner = [ip for ip in inner_rings if op.intersects(ip)]
        polys.append(_subtract_holes(op, matching_inner))
    geom = polys[0] if len(polys) == 1 else unary_union(polys)
    geom = make_valid(geom)
    return None if geom.is_empty else geom


def _subtract_holes(op, matching_inner: list):
    """Subtracts every ring in `matching_inner` from `op`. Real relations
    (a Great Lake's shoreline, a large admin boundary) can have thousands
    of intersecting holes (Lake Huron: 2 outer rings, 14,105 inner --
    almost every one of its many islands is its own closed way): calling
    `.difference()` once per hole in a loop, as this used to do, makes
    each successive call operate on an increasingly complex `poly` (more
    accumulated cutouts = more vertices/rings for GEOS to carry), so cost
    grows with hole count in a way that dominated real build time (27+
    minutes single-threaded for Lake Huron alone, found benchmarking a
    ~10x-Minnesota-scale region -- see docs/progress.md). Set difference
    distributes over union (`A - (B1 union B2 union ...) == A - B1 - B2 -
    ...`), so union every intersecting hole once and subtract in a single
    call instead -- `unary_union` uses a proper spatial algorithm
    (internally an STRtree-backed sweep), not the pairwise-degrading
    pattern the old loop had. Falls back to the original one-hole-at-a-time
    loop (with the same per-hole exception tolerance as before) only if
    the batched path itself raises, so a malformed hole still degrades
    the same way it used to rather than losing every hole."""
    from shapely.ops import unary_union

    if not matching_inner:
        return op
    try:
        holes = matching_inner[0] if len(matching_inner) == 1 else unary_union(matching_inner)
        return op.difference(holes)
    except Exception:
        poly = op
        for ip in matching_inner:
            try:
                poly = poly.difference(ip)
            except Exception:
                continue
        return poly


# --------------------------------------------------------------------------
# relation-area parallelism: `_assemble_relation_geometry` is pure-Python
# shapely/GEOS work with no I/O, so it never releases the GIL usefully --
# threads buy nothing here (and DuckDB's own `--threads` only ever
# controlled *its* SQL execution, never this loop). Fan it out across
# processes instead, sized from the same `--threads` this module already
# gives DuckDB (`_area_max_workers` mirrors `_connect`'s own
# threads-or-fallback rule). `way_wkt` (potentially large: every way
# referenced by every candidate relation) is sent to each worker exactly
# once via `ProcessPoolExecutor`'s `initializer`, not per task -- macOS's
# default `spawn` start method re-imports this module fresh in every
# worker (no `fork`-inherited memory to rely on), so a plain module-level
# global set at import time wouldn't be populated; the initializer is what
# actually runs inside the freshly-spawned interpreter, once per worker.
# --------------------------------------------------------------------------

# Below this many candidate relations, a ProcessPoolExecutor's own startup
# cost (spawning N interpreters, importing duckdb/shapely/numpy in each)
# would dominate or even lose to just running in-process -- this also
# keeps the test suite's tiny fixtures (a handful of relations) fast and
# free of multiprocessing pickling/startup flakiness, and keeps compact's
# touched-pivot re-derive (normally a handful of relations) on the cheap
# serial path without any special-casing.
AREA_PARALLEL_MIN_CANDIDATES = 2000

_worker_way_wkt: dict[int, str] = {}


def _init_relation_area_worker(way_wkt: dict[int, str]) -> None:
    """`ProcessPoolExecutor(initializer=..., initargs=(way_wkt,))`: runs
    once per worker process (not per task), stashing `way_wkt` in a
    worker-local module global so `_relation_area_task_worker` never has
    to repickle it on any of the hundreds of thousands of individual
    per-relation tasks."""
    global _worker_way_wkt
    _worker_way_wkt = way_wkt


def _relation_area_task(task: tuple, way_wkt: dict[int, str]) -> Optional[dict]:
    """One relation's area row (or None) -- exactly the body of the
    original per-relation loop, unchanged, shared by the serial and
    parallel paths so both call identical code. `task` is a plain
    picklable tuple: (rel_id, tags, version, changeset, timestamp, uid,
    user, xmin, ymin, xmax, ymax, outer_way_ids, inner_way_ids); the bbox
    columns are threaded through for parity with the original row shape
    even though, as in the original loop, only the assembled geometry's
    own bounds end up used."""
    (rel_id, tags, version, changeset, timestamp, uid, user,
     _xmin, _ymin, _xmax, _ymax, outer_way_ids, inner_way_ids) = task
    geom = _assemble_relation_geometry(outer_way_ids, inner_way_ids, way_wkt)
    if geom is None:
        return None
    gxmin, gymin, gxmax, gymax = geom.bounds
    return {
        "id": rel_id + RELATION_ID_OFFSET,
        "pivot_id": rel_id,
        "tags": dict(tags) if tags else {},
        "version": version, "changeset": changeset, "timestamp": timestamp,
        "uid": uid, "user": user,
        "xmin_e7": _to_e7(gxmin), "ymin_e7": _to_e7(gymin),
        "xmax_e7": _to_e7(gxmax), "ymax_e7": _to_e7(gymax),
        "wkb": geom.wkb,
    }


def _relation_area_task_worker(task: tuple) -> Optional[dict]:
    """Top-level, picklable `ProcessPoolExecutor` task function: reads
    `way_wkt` from the worker-global `_init_relation_area_worker` set,
    instead of receiving it as a (repeatedly-repickled) argument."""
    return _relation_area_task(task, _worker_way_wkt)


def _area_max_workers(threads: Optional[int]) -> int:
    """Same threads-or-fallback rule as `_connect`'s DuckDB `--threads`
    (DuckDB itself defaults to the available core count when `SET
    threads` is never called) -- reused here so the relation-area process
    pool defaults to the same parallelism as DuckDB's own SQL phases of
    this module when `--threads` isn't given explicitly."""
    if threads:
        return int(threads)
    return os.cpu_count() or 1


def _area_chunksize(n_tasks: int, max_workers: int) -> int:
    """Relation cost is not just "hugely variable", it's *power-law*
    skewed: measured on a real ~55k-candidate region, a chunksize of 200
    (this function's previous "~8 chunks per worker" formula) left 12 of
    14 workers idle while 2 workers each sat on a chunk that happened to
    contain one of a handful of dominant relations (a big admin
    boundary/coastline-like one), for a ~1.15x wall-clock speedup instead
    of anything close to 14x. `ProcessPoolExecutor.map` only rebalances
    *between* chunks, never within one, so any chunksize above 1 risks
    the same failure mode again for a different skewed input -- and
    per-task IPC overhead here is cheap to pay per-task (measured:
    pickling one task tuple, or the whole `way_wkt` initializer payload,
    both take well under a second even at hundreds of thousands of
    tasks/hundreds of MB -- see the initializer docstring above), so
    there's no real amortization benefit to weigh against that risk.
    Always hand out one relation at a time so the pool can keep every
    worker fed until the true last relation finishes."""
    return 1


def _relation_area_rows_from_tasks(
    tasks: list[tuple], way_wkt: dict[int, str], threads: Optional[int],
) -> list[dict]:
    """Runs `_relation_area_task` over `tasks`: serially in-process for
    small inputs (exactly today's behavior -- no process-pool startup
    cost, no multiprocessing pickling edge cases) or fanned out across a
    `ProcessPoolExecutor` for large ones. Exceptions raised by a
    pathological relation's assembly are not caught anywhere in here:
    `ProcessPoolExecutor.map`'s result iterator re-raises a worker's
    exception exactly where that task's result would otherwise be, same
    as calling `_relation_area_task` directly would -- this deliberately
    adds no new exception handling around either path."""
    if not tasks:
        return []
    max_workers = _area_max_workers(threads)
    if max_workers <= 1 or len(tasks) < AREA_PARALLEL_MIN_CANDIDATES:
        rows = [_relation_area_task(t, way_wkt) for t in tasks]
    else:
        chunksize = _area_chunksize(len(tasks), max_workers)
        with ProcessPoolExecutor(
            max_workers=max_workers, initializer=_init_relation_area_worker, initargs=(way_wkt,),
        ) as ex:
            rows = list(ex.map(_relation_area_task_worker, tasks, chunksize=chunksize))
    return [r for r in rows if r is not None]


def _relation_qualifying_sql(tags_expr: str = "tags") -> str:
    """docs/m3-contracts.md section 9 fact 3: the reference's own
    `areas.osm3s` recipe -- a relation is *considered* for an area only
    when it matches one of these tag combinations (it still needs a
    resolvable ring on top of this, checked separately)."""
    return (
        f"(({tags_expr}['type'] = 'multipolygon' AND {tags_expr}['name'] IS NOT NULL) "
        f"OR ({tags_expr}['type'] = 'boundary' AND {tags_expr}['name'] IS NOT NULL) "
        f"OR ({tags_expr}['admin_level'] IS NOT NULL AND {tags_expr}['name'] IS NOT NULL) "
        f"OR {tags_expr}['postal_code'] IS NOT NULL "
        f"OR {tags_expr}['addr:postcode'] IS NOT NULL)"
    )


def _relation_area_rows(
    con, manifest: catalog.Manifest, relation_ids: Optional[list[int]], threads: Optional[int] = None,
) -> tuple[list[dict], int]:
    """Python list of area-row dicts for relations matching the
    `areas.osm3s` rule (9 fact 3) that also assemble into at least one
    valid ring, restricted to `relation_ids` pivots when given (compact's
    touched-pivot re-derive; None means every relation). An explicit empty
    `relation_ids` short-circuits to no rows with no file reads at all.
    `threads` sizes the process pool the actual ring-assembly work runs on
    (see `_relation_area_rows_from_tasks`); None uses the same fallback as
    DuckDB's own `--threads`."""
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
    where = _relation_qualifying_sql()
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

    tasks: list[tuple] = []
    for (rel_id, tags, version, changeset, timestamp, uid, user, xmin, ymin, xmax, ymax) in cand_rows:
        m = members_by_rel.get(rel_id)
        if not m:
            continue
        tasks.append((rel_id, tags, version, changeset, timestamp, uid, user, xmin, ymin, xmax, ymax, m["outer"], m["inner"]))
    area_rows = _relation_area_rows_from_tasks(tasks, way_wkt, threads)
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
# relation areas: place (cell/hilbert) + write
# --------------------------------------------------------------------------


def derive_relation_areas(
    con, manifest: catalog.Manifest, relation_ids: Optional[list[int]] = None, threads: Optional[int] = None,
) -> tuple[str, int]:
    """TEMP TABLE of every derived relation-area row (id, pivot_type,
    pivot_id, tags, meta cols, bbox, geometry -- no cell/hilbert yet),
    restricted to `relation_ids` pivots when given. Returns (table_name,
    files_read). Way areas are not stored (9.1) -- see `build_way_area_index`
    for the way side. `threads` sizes the ring-assembly process pool
    (`_relation_area_rows_from_tasks`)."""
    rel_rows, nfiles_r = _relation_area_rows(con, manifest, relation_ids, threads=threads)
    rel_final = _relation_area_final(con, rel_rows)
    return rel_final, nfiles_r


def place_areas(con, table_name: str, manifest: catalog.Manifest, promoted_keys: list[str]) -> str:
    """Adds promoted columns + cell/hilbert (v2 loose placement,
    docs/m1-contracts.md section 2) to `table_name`'s rows. Returns a new
    TEMP TABLE name ready to write out."""
    promoted_sql = common.promoted_select(promoted_keys)
    leaves = manifest.leaf_cells
    ancestor_depths = manifest.ancestor_depths or cells_mod.DEFAULT_ANCESTOR_DEPTHS
    max_depth = manifest.max_depth or cells_mod.DEFAULT_MAX_DEPTH_V2
    leaf_index = cells_mod.LeafIndex(leaves) if leaves else cells_mod.LeafIndex([cells_mod.ROOT])

    # `table_name` holds relation areas only (9.1: way areas are never
    # stored), so `id` (relation_id + RELATION_ID_OFFSET) is unique on its
    # own -- but join on a synthetic row key rather than `id` anyway, both
    # because it's cheap and because it keeps this helper correct if it's
    # ever reused for a table where that isn't true.
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


def build_areas_for_manifest(
    con, root: Path, cat_manifest: catalog.Manifest, promoted_keys: list[str], threads: Optional[int] = None,
) -> dict:
    """Full derivation (every qualifying relation, plus the way index)
    against `cat_manifest`'s *current* tables, writing spatial/index files
    under `cat_manifest.generation`. Returns the new `areas` manifest field
    ({"index", "cells", "way_index"}) to merge into the caller's manifest
    dict; does not read or write `cat_manifest.data["areas"]` itself, so
    callers (``osmpq areas``, ``osmpq compact``, the test fixture) control
    how the result folds into their manifest. `threads` sizes the
    relation-area ring-assembly process pool (see `_area_max_workers`)."""
    derived, _files_read = derive_relation_areas(con, cat_manifest, threads=threads)
    placed = place_areas(con, derived, cat_manifest, promoted_keys)
    area_field = write_area_layout(con, root, cat_manifest.generation, placed, promoted_keys)
    way_index_field, _files_read_w = build_way_area_index(con, root, cat_manifest.generation, cat_manifest, promoted_keys)
    area_field["way_index"] = way_index_field
    return area_field


def derive_relation_areas_for_pivots(
    con, cat_manifest: catalog.Manifest, promoted_keys: list[str], relation_ids: list[int],
    threads: Optional[int] = None,
) -> tuple[str, int]:
    """Re-derives relation-area rows for exactly the given touched
    relations (used by ``osmpq compact``, 9.2): returns (TEMP TABLE of
    new/changed area rows incl. cell/hilbert, files_read). A relation that
    no longer qualifies (lost its `areas.osm3s` tag combination, lost its
    ring, or was deleted) simply produces no row here -- callers remove its
    old area row by id. The way index is not touched-pivot re-derived at
    all; ``osmpq compact`` rebuilds it in full instead
    (`build_way_area_index_from_files`). `relation_ids` here is normally a
    small touched-pivot set, so this naturally lands on
    `_relation_area_rows_from_tasks`'s serial fallback without any
    special-casing."""
    derived, files_read = derive_relation_areas(con, cat_manifest, relation_ids=relation_ids, threads=threads)
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

    area_field = build_areas_for_manifest(con, root, cat_manifest, promoted_keys, threads=opts.threads)
    con.close()

    new_man = copy.deepcopy(old_man)
    new_man["manifest_version"] = max(4, int(old_man.get("manifest_version", 1)))
    new_man["areas"] = area_field
    stats = dict(new_man.get("stats") or {})
    stats["areas"] = area_field["index"]["rows"]
    stats["way_areas"] = area_field["way_index"]["rows"]
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
        f"derived {area_field['index']['rows']} relation area(s) across {len(area_field['cells'])} cell(s) "
        f"and indexed {area_field['way_index']['rows']} way area(s) "
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
