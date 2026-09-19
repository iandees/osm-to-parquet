"""Query-time area semantics (docs/m3-contracts.md section 4.4).

Registers, on import (`planner._load_hooks`, via `hooks.HOOK_MODULES`):

* `hooks.AREA_QUERY_HOOK`: a `Query` whose type is ``area``
  (`area[...]`, `area(id)`, `area.a[...]`).
* `hooks.FILTER_HOOKS[AreaFilter]`: `(area)`, `(area.a)`, `(area:id)`.
* `hooks.FILTER_HOOKS[PivotFilter]`: `(pivot.a)`.
* `hooks.STATEMENT_HOOKS[IsIn]`, `hooks.STATEMENT_HOOKS[MapToArea]`.

Area rows read from the index/spatial area files (`osmpq.build.areas`,
docs/m3-contracts.md section 4.2) are absent whenever the manifest has no
`areas` field (v1-v3, or a v4 manifest `osmpq areas` hasn't populated yet);
every entry point here degrades to an empty set plus a `ctx.warnings` entry
("areas are not available for this dataset") in that case, per contract.

Relation geometry for `(area)`/`(pivot)`/`is_in` on relations is
`osmpq.engine.geofilters.relation_geometry_table` (W1, section 3.5),
imported lazily since that module is developed concurrently; if the import
fails, matches fall back to a bbox overlap test against the relation's own
bbox columns, with a `ctx.warnings` entry, exactly as section 4.4 says.
"""
from __future__ import annotations

from typing import Optional

from osmpq.errors import RuntimeQueryError, UnsupportedError
from osmpq.ql.ast import AreaFilter, IdFilter, IsIn, MapToArea, PivotFilter, Query, TagFilter

from . import catalog, hooks, idset, setops, sources, tagsql
from .schema import empty_set_sql, project

BBox = tuple[float, float, float, float]

WAY_ID_OFFSET = 2_400_000_000
RELATION_ID_OFFSET = 3_600_000_000

_NO_AREAS_WARNING = "areas are not available for this dataset"

# The canonical projection for an area "set row" (4.4): type/id/tags/meta/
# bbox/cell/hilbert, geometry always NULL (area geometry lives only in the
# spatial area files, keyed by (cell, id), never inline in a set).
_AREA_COLS = {
    "type": "'area'",
    "id": "id",
    "cell": "cell",
    "tags": "tags",
    "version": "version",
    "changeset": "changeset",
    "timestamp": "timestamp",
    "uid": "uid",
    "user": '"user"',
    "xmin_e7": "xmin_e7",
    "ymin_e7": "ymin_e7",
    "xmax_e7": "xmax_e7",
    "ymax_e7": "ymax_e7",
    "hilbert": "hilbert",
}


def _to_e7(deg: float) -> int:
    return int(round(deg * 1e7))


def _q1(s: str) -> str:
    return s.replace("'", "''")


def _quote_list(paths: list[str]) -> str:
    return "[" + ",".join("'" + _q1(p) + "'" for p in paths) + "]"


def _require_set(ctx, name: str) -> None:
    exists = ctx.con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [f"set_{name}"]
    ).fetchone()[0]
    if not exists:
        raise RuntimeQueryError(f'runtime error: set ".{name}" has not been set before')


def _warn_no_areas(ctx) -> None:
    if _NO_AREAS_WARNING not in ctx.warnings:
        ctx.warnings.append(_NO_AREAS_WARNING)


def _warn_once(ctx, msg: str) -> None:
    if msg not in ctx.warnings:
        ctx.warnings.append(msg)


def _index_select(manifest: catalog.Manifest) -> Optional[str]:
    """SELECT over the whole area index (canonical projection, geometry
    NULL), or None when areas are absent from this manifest."""
    entry = manifest.area_index
    if not entry:
        return None
    path = manifest.path(entry["path"])
    return f"SELECT {project(_AREA_COLS)} FROM read_parquet('{_q1(path)}')"


def _area_cell_files(manifest: catalog.Manifest, cells: list[str]) -> list[str]:
    tc = manifest.table_cells("area")
    return [manifest.path(tc[c]["path"]) for c in cells if c in tc]


# --------------------------------------------------------------------------
# area[...], area(id), area.a[...]  (hooks.AREA_QUERY_HOOK)
# --------------------------------------------------------------------------


def area_query_hook(ctx, q: Query) -> None:
    manifest = ctx.manifest
    index_sql = _index_select(manifest)
    if index_sql is None:
        _warn_no_areas(ctx)
        setops.materialize(ctx.con, q.output_set, empty_set_sql())
        return

    ids: Optional[list[int]] = None
    tag_filters: list[TagFilter] = []
    for f in q.filters:
        if isinstance(f, IdFilter):
            ids = list(f.ids)
        elif isinstance(f, TagFilter):
            tag_filters.append(f)
        else:
            raise UnsupportedError(f"filter {type(f).__name__} is not supported on an area query")

    if q.input_sets:
        for name in q.input_sets:
            _require_set(ctx, name)
        base_sql = sources.build_from_set_select(ctx.con, q.input_sets, ["area"], tag_filters, ids, bbox=None)
    else:
        # Tag/id pushdown against the *raw* index file (promoted columns +
        # `tags` MAP), not the canonical projection: `tagsql` pushes a
        # promoted key (e.g. `name`) to its own physical column, which only
        # the raw file has -- the canonical row shape (4.4) never carries
        # promoted columns.
        where = []
        if ids:
            where.append(idset.id_predicate(ctx.con, "id", ids))
        where.append(tagsql.tag_filters_sql(tag_filters, ctx.promoted_keys))
        where_sql = " AND ".join(f"({w})" for w in where)
        raw_path = manifest.path(manifest.area_index["path"])
        base_sql = (
            f"SELECT {project(_AREA_COLS)} FROM read_parquet('{_q1(raw_path)}') __raw WHERE {where_sql}"
        )
        ctx.files_read += 1
    setops.materialize(ctx.con, q.output_set, base_sql)


# --------------------------------------------------------------------------
# (area), (area.a), (area:id)
# --------------------------------------------------------------------------


def _area_geometry_table(ctx, area_ids: Optional[list[int]] = None, set_name: Optional[str] = None) -> tuple[Optional[str], Optional[BBox]]:
    """Loads the geometry + bbox of the referenced areas from the spatial
    area files (keyed by cell + id) into a fresh TEMP TABLE(id, geometry,
    xmin_e7, ymin_e7, xmax_e7, ymax_e7). Returns (table_name, union bbox)
    or (None, None) when there is nothing to match (no such areas, or areas
    absent from the manifest -- callers must warn separately)."""
    manifest = ctx.manifest
    con = ctx.con
    if set_name is not None:
        _require_set(ctx, set_name)
        rows = con.execute(f"SELECT id, cell FROM set_{set_name} WHERE type = 'area'").fetchall()
    else:
        index_sql = _index_select(manifest)
        if index_sql is None or not area_ids:
            return None, None
        id_pred = idset.id_predicate(con, "id", area_ids)
        rows = con.execute(f"SELECT id, cell FROM ({index_sql}) __idx WHERE {id_pred}").fetchall()
    if not rows:
        return None, None
    pairs = [(c, i) for i, c in rows if c]
    if not pairs:
        return None, None
    needed_cells = sorted({c for c, _ in pairs})
    files = _area_cell_files(manifest, needed_cells)
    if not files:
        return None, None
    ctx.files_read += len(files)
    pairs_tbl = idset.register_pairs_table(con, pairs)
    name = ctx.fresh_name("areageom")
    con.execute(
        f"CREATE TEMP TABLE {name} AS "
        f"SELECT t.id, t.geometry, t.xmin_e7, t.ymin_e7, t.xmax_e7, t.ymax_e7 "
        f"FROM read_parquet({_quote_list(files)}) t "
        f"JOIN {pairs_tbl} p ON t.cell = p.cell AND t.id = p.id"
    )
    row = con.execute(
        f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 FROM {name}"
    ).fetchone()
    bbox = None if None in row else (row[0], row[1], row[2], row[3])
    return name, bbox


def _relation_candidate_geometry(ctx, area_bbox: Optional[BBox]) -> Optional[str]:
    """TEMP TABLE(id, geometry) of every relation intersecting
    `area_bbox`'s cells, via `geofilters.relation_geometry_table` (3.5),
    imported lazily. Returns None (and a ctx.warnings entry) when that
    module isn't importable yet, or `area_bbox` is None."""
    if area_bbox is None:
        return None
    try:
        from osmpq.engine.geofilters import relation_geometry_table
    except Exception:
        _warn_once(ctx, "relation geometry (geofilters) is not available; using a bbox test for (area)/(pivot)/is_in on relations")
        return None
    rel_sql, nfiles = sources.build_relation_spatial_select(ctx.con, ctx.manifest, area_bbox, [], None, ctx.promoted_keys)
    ctx.files_read += nfiles
    return relation_geometry_table(ctx, rel_sql)


def _node_match_sql(alias: str, geom_tbl: str) -> str:
    return (
        f"({alias}.type = 'node' AND EXISTS (SELECT 1 FROM {geom_tbl} g WHERE "
        f"ST_Within(ST_Point({alias}.lon_e7 / 1e7, {alias}.lat_e7 / 1e7), g.geometry)))"
    )


def _way_match_sql(ctx, alias: str, geom_tbl: str) -> str:
    _warn_once(ctx, "way geometry is unavailable for some ways; falling back to a bbox overlap test for (area)/(pivot)/is_in")
    return (
        f"({alias}.type = 'way' AND ("
        f"({alias}.geometry IS NOT NULL AND EXISTS (SELECT 1 FROM {geom_tbl} g WHERE ST_Intersects({alias}.geometry, g.geometry))) "
        f"OR ({alias}.geometry IS NULL AND EXISTS (SELECT 1 FROM {geom_tbl} g WHERE "
        f"{alias}.xmax_e7 >= g.xmin_e7 AND {alias}.xmin_e7 <= g.xmax_e7 AND "
        f"{alias}.ymax_e7 >= g.ymin_e7 AND {alias}.ymin_e7 <= g.ymax_e7))"
        f"))"
    )


def _relation_match_sql(ctx, alias: str, geom_tbl: str, area_bbox: Optional[BBox]) -> str:
    rel_geom_tbl = _relation_candidate_geometry(ctx, area_bbox)
    if rel_geom_tbl is not None:
        return (
            f"({alias}.type = 'relation' AND EXISTS (SELECT 1 FROM {rel_geom_tbl} rg, {geom_tbl} g "
            f"WHERE rg.id = {alias}.id AND ST_Intersects(rg.geometry, g.geometry)))"
        )
    return (
        f"({alias}.type = 'relation' AND EXISTS (SELECT 1 FROM {geom_tbl} g WHERE "
        f"{alias}.xmax_e7 >= g.xmin_e7 AND {alias}.xmin_e7 <= g.xmax_e7 AND "
        f"{alias}.ymax_e7 >= g.ymin_e7 AND {alias}.ymin_e7 <= g.ymax_e7))"
    )


class _AreaFilterHook:
    def _geom(self, ctx, f: AreaFilter):
        cache = getattr(ctx, "_area_filter_geom_cache", None)
        if cache is None:
            cache = {}
            ctx._area_filter_geom_cache = cache
        key = (f.set_name, f.area_id)
        if key not in cache:
            if f.area_id is not None:
                cache[key] = _area_geometry_table(ctx, area_ids=[f.area_id])
            else:
                cache[key] = _area_geometry_table(ctx, set_name=f.set_name or "_")
        return cache[key]

    def implied_bbox(self, ctx, q, f: AreaFilter) -> Optional[BBox]:
        if not ctx.manifest.area_index:
            _warn_no_areas(ctx)
        _geom_tbl, bbox = self._geom(ctx, f)
        return bbox

    def predicate(self, ctx, q, f: AreaFilter, alias: str) -> str:
        geom_tbl, bbox = self._geom(ctx, f)
        if geom_tbl is None:
            if not ctx.manifest.area_index:
                _warn_no_areas(ctx)
            return "FALSE"
        parts = []
        if "node" in q.types:
            parts.append(_node_match_sql(alias, geom_tbl))
        if "way" in q.types:
            parts.append(_way_match_sql(ctx, alias, geom_tbl))
        if "relation" in q.types:
            parts.append(_relation_match_sql(ctx, alias, geom_tbl, bbox))
        if not parts:
            return "FALSE"
        return "(" + " OR ".join(parts) + ")"


# --------------------------------------------------------------------------
# (pivot.a): way/rel whose derived area id is in the referenced set
# --------------------------------------------------------------------------


class _PivotFilterHook:
    def implied_bbox(self, ctx, q, f: PivotFilter) -> Optional[BBox]:
        _require_set(ctx, f.set_name)
        row = ctx.con.execute(
            f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
            f"FROM set_{f.set_name} WHERE type = 'area'"
        ).fetchone()
        return None if None in row else (row[0], row[1], row[2], row[3])

    def predicate(self, ctx, q, f: PivotFilter, alias: str) -> str:
        _require_set(ctx, f.set_name)
        return (
            f"(({alias}.type = 'way' AND ({alias}.id + {WAY_ID_OFFSET}) IN "
            f"(SELECT id FROM set_{f.set_name} WHERE type = 'area')) "
            f"OR ({alias}.type = 'relation' AND ({alias}.id + {RELATION_ID_OFFSET}) IN "
            f"(SELECT id FROM set_{f.set_name} WHERE type = 'area')))"
        )


# --------------------------------------------------------------------------
# is_in / is_in(lat, lon) / .a is_in -> .b
# --------------------------------------------------------------------------


def _way_isin_match_sql(input_set: str) -> str:
    return (
        f"SELECT a.* FROM __isin_areas a JOIN set_{input_set} s ON s.type = 'way' "
        f"WHERE (s.geometry IS NOT NULL AND ST_Intersects(s.geometry, a.geometry)) "
        f"OR (s.geometry IS NULL AND s.xmax_e7 >= a.xmin_e7 AND s.xmin_e7 <= a.xmax_e7 "
        f"AND s.ymax_e7 >= a.ymin_e7 AND s.ymin_e7 <= a.ymax_e7)"
    )


def _relation_isin_match_sql(ctx, input_set: str) -> Optional[str]:
    rel_bbox_row = ctx.con.execute(
        f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
        f"FROM set_{input_set} WHERE type = 'relation'"
    ).fetchone()
    if None in rel_bbox_row:
        return None
    rel_bbox = (rel_bbox_row[0], rel_bbox_row[1], rel_bbox_row[2], rel_bbox_row[3])
    rel_geom_tbl = _relation_candidate_geometry(ctx, rel_bbox)
    if rel_geom_tbl is not None:
        return (
            f"SELECT a.* FROM __isin_areas a "
            f"JOIN {rel_geom_tbl} rg ON TRUE "
            f"JOIN set_{input_set} s ON s.type = 'relation' AND s.id = rg.id "
            f"WHERE ST_Intersects(rg.geometry, a.geometry)"
        )
    return (
        f"SELECT a.* FROM __isin_areas a JOIN set_{input_set} s ON s.type = 'relation' "
        f"WHERE s.xmax_e7 >= a.xmin_e7 AND s.xmin_e7 <= a.xmax_e7 "
        f"AND s.ymax_e7 >= a.ymin_e7 AND s.ymin_e7 <= a.ymax_e7"
    )


def is_in_statement(ctx, stmt: IsIn) -> None:
    manifest = ctx.manifest
    con = ctx.con
    if not manifest.area_index:
        _warn_no_areas(ctx)
        setops.materialize(con, stmt.output_set, empty_set_sql())
        return

    if stmt.coords is not None:
        lat, lon = stmt.coords
        input_bbox: BBox = (lat, lon, lat, lon)
    else:
        _require_set(ctx, stmt.input_set)
        row = con.execute(
            f"SELECT min(CASE WHEN type = 'node' THEN lat_e7 ELSE ymin_e7 END), "
            f"min(CASE WHEN type = 'node' THEN lon_e7 ELSE xmin_e7 END), "
            f"max(CASE WHEN type = 'node' THEN lat_e7 ELSE ymax_e7 END), "
            f"max(CASE WHEN type = 'node' THEN lon_e7 ELSE xmax_e7 END) "
            f"FROM set_{stmt.input_set} WHERE type IN ('node', 'way', 'relation')"
        ).fetchone()
        if None in row:
            setops.materialize(con, stmt.output_set, empty_set_sql())
            return
        input_bbox = (row[0] / 1e7, row[1] / 1e7, row[2] / 1e7, row[3] / 1e7)

    cells = catalog.cells_for_bbox(manifest, "area", input_bbox)
    files = _area_cell_files(manifest, cells)
    if not files:
        setops.materialize(con, stmt.output_set, empty_set_sql())
        return
    ctx.files_read += len(files)
    con.execute(f"CREATE OR REPLACE TEMP TABLE __isin_areas AS SELECT * FROM read_parquet({_quote_list(files)})")

    if stmt.coords is not None:
        lat, lon = stmt.coords
        match_sql = f"SELECT a.* FROM __isin_areas a WHERE ST_Within(ST_Point({lon}, {lat}), a.geometry)"
    else:
        node_sql = (
            f"SELECT a.* FROM __isin_areas a JOIN set_{stmt.input_set} s ON s.type = 'node' "
            f"WHERE ST_Within(ST_Point(s.lon_e7 / 1e7, s.lat_e7 / 1e7), a.geometry)"
        )
        way_sql = _way_isin_match_sql(stmt.input_set)
        rel_sql = _relation_isin_match_sql(ctx, stmt.input_set)
        parts = [p for p in (node_sql, way_sql, rel_sql) if p]
        match_sql = "SELECT DISTINCT * FROM (\n" + "\nUNION ALL\n".join(parts) + "\n) __m"

    cols = {
        "type": "'area'", "id": "id", "cell": "cell", "tags": "tags",
        "version": "version", "changeset": "changeset", "timestamp": "timestamp",
        "uid": "uid", "user": '"user"',
        "xmin_e7": "xmin_e7", "ymin_e7": "ymin_e7", "xmax_e7": "xmax_e7", "ymax_e7": "ymax_e7",
        "hilbert": "hilbert",
    }
    final_sql = f"SELECT {project(cols)} FROM ({match_sql}) __r"
    setops.materialize(con, stmt.output_set, final_sql)


# --------------------------------------------------------------------------
# map_to_area
# --------------------------------------------------------------------------


def map_to_area_statement(ctx, stmt: MapToArea) -> None:
    _require_set(ctx, stmt.input_set)
    manifest = ctx.manifest
    index_sql = _index_select(manifest)
    if index_sql is None:
        _warn_no_areas(ctx)
        setops.materialize(ctx.con, stmt.output_set, empty_set_sql())
        return
    sql = (
        f"SELECT idx.* FROM ({index_sql}) idx WHERE idx.id IN ("
        f"SELECT id + {WAY_ID_OFFSET} FROM set_{stmt.input_set} WHERE type = 'way' "
        f"UNION ALL "
        f"SELECT id + {RELATION_ID_OFFSET} FROM set_{stmt.input_set} WHERE type = 'relation')"
    )
    ctx.files_read += 1
    setops.materialize(ctx.con, stmt.output_set, sql)


# --------------------------------------------------------------------------
# registration
# --------------------------------------------------------------------------

hooks.register_filter(AreaFilter, _AreaFilterHook())
hooks.register_filter(PivotFilter, _PivotFilterHook())
hooks.register_statement(IsIn, is_in_statement)
hooks.register_statement(MapToArea, map_to_area_statement)
hooks.set_area_query_hook(area_query_hook)
