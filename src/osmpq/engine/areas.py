"""Query-time area semantics (docs/m3-contracts.md section 4.4, amended by
section 9 after probing the reference -- the amendment replaces 4.4 and is
what this module implements).

Registers, on import (`planner._load_hooks`, via `hooks.HOOK_MODULES`):

* `hooks.AREA_QUERY_HOOK`: a `Query` whose type is ``area``
  (`area[...]`, `area(id)`, `area.a[...]`).
* `hooks.FILTER_HOOKS[AreaFilter]`: `(area)`, `(area.a)`, `(area:id)`.
* `hooks.FILTER_HOOKS[PivotFilter]`: `(pivot.a)`.
* `hooks.STATEMENT_HOOKS[IsIn]`, `hooks.STATEMENT_HOOKS[MapToArea]`.

Representation (9.1): a **way area is the closed way's own canonical row**
(``type='way'``) wherever areas appear in a set -- `area[...]` results,
`is_in` output, `map_to_area` output. No 2400000000 offset ever appears in
a set or in output; `area(N)` with ``2400000000 <= N < 3600000000`` is
accepted as a lookup of way ``N - 2400000000`` (a superset of the
reference, cheap, harmless). A **relation area is a stored `area` row**
(``type='area'``, ``id = 3600000000 + relation id``), read from
`index/<gen>/areas.parquet` / `spatial/<gen>/area/cell=<cell>/
part-0.parquet` exactly as before. A way's polygon is never stored -- it
is `ST_MakePolygon` of the way's own LINESTRING, computed on demand
wherever a filter needs it (`(area)`, `(pivot)`, `is_in`), hydrated by
cell + id from the way spatial files when the row's `geometry` is NULL
(e.g. it came from byid).

`area[...]` / `area(id)` / `area.a[...]` (9.3) read relation rows from
`areas.parquet` **union** way rows from `index/<gen>/way_areas.parquet`
(closed ways carrying one of a narrow set of keys,
`osmpq.build.areas.WAY_AREA_QUALIFYING_KEYS`), the latter hydrated to full
canonical rows (refs, geometry) from the way spatial files by cell + id --
`way_areas.parquet` itself has no geometry column.

`(area)`/`(area.a)`/`(area:id)`, `(pivot.a)`, `is_in` and `map_to_area`
never use `ST_Intersects` for a way (or a relation's member ways) against
an area polygon (9 fact 4, verified against the reference on ten
partially-inside primary ways: 8 with >=1 vertex inside were selected, the
2 with none were not): the test is "any vertex of the candidate strictly
inside the polygon" (`_any_vertex_within_sql`, built on `ST_Points` +
`ST_Dump` so it works uniformly on a way's LINESTRING and a relation's
member GEOMETRYCOLLECTION alike -- both reduce to "the set of every vertex
in the geometry").

Area rows/indexes are absent whenever the manifest has neither
`areas.index` nor `areas.way_index` (v1-v3, or a v4 manifest `osmpq areas`
hasn't populated yet); every entry point here degrades to an empty set
plus a `ctx.warnings` entry ("areas are not available for this dataset")
in that case, per contract.

Relation geometry for `(area)`/`(pivot)`/`is_in` on relations is
`osmpq.engine.geofilters.relation_geometry_table` (W1, section 3.5),
imported lazily since that module is developed concurrently; if the import
fails, matches fall back to a bbox overlap test against the relation's own
bbox columns, with a `ctx.warnings` entry, exactly as section 4.4 said.
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

# The canonical projection for a *relation*-area "set row" (9.1/9.3):
# type/id/tags/meta/bbox/cell/hilbert, geometry always NULL (area geometry
# lives only in the spatial area files, keyed by (cell, id)).
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


def _area_cols(alias: str) -> dict:
    """`_AREA_COLS`, qualified by `alias` -- needed wherever the relation-
    area row is projected out of a query that joins it against another
    table (the bare `_AREA_COLS` would otherwise collide with that other
    table's own `id`/`cell`/... columns)."""
    return {
        name: ("'area'" if name == "type" else f'{alias}."user"' if name == "user" else f"{alias}.{expr}")
        for name, expr in _AREA_COLS.items()
    }


def _way_cols(alias: str) -> dict:
    """The canonical projection for a *way*-area "set row" (9.1: a way
    area is the way's own full canonical row), columns qualified by
    `alias` so this is safe to use in a joined query."""
    return {
        "type": "'way'",
        "id": f"{alias}.id",
        "cell": f"{alias}.cell",
        "refs": f"{alias}.refs",
        "tags": f"{alias}.tags",
        "version": f"{alias}.version",
        "changeset": f"{alias}.changeset",
        "timestamp": f'{alias}."timestamp"',
        "uid": f"{alias}.uid",
        "user": f'{alias}."user"',
        "xmin_e7": f"{alias}.xmin_e7",
        "ymin_e7": f"{alias}.ymin_e7",
        "xmax_e7": f"{alias}.xmax_e7",
        "ymax_e7": f"{alias}.ymax_e7",
        "geometry": f"{alias}.geometry",
        "hilbert": f"{alias}.hilbert",
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


def _areas_available(manifest: catalog.Manifest) -> bool:
    """Whether `osmpq areas` has ever run against this dataset (either
    index present) -- absent for v1-v3, or a v4 manifest it hasn't
    populated yet."""
    return manifest.area_index is not None or manifest.way_area_index is not None


def _index_select(manifest: catalog.Manifest) -> Optional[str]:
    """SELECT over the whole *relation*-area index (canonical projection,
    geometry NULL), or None when relation areas are absent from this
    manifest."""
    entry = manifest.area_index
    if not entry:
        return None
    path = manifest.path(entry["path"])
    return f"SELECT {project(_AREA_COLS)} FROM read_parquet('{_q1(path)}')"


def _area_cell_files(manifest: catalog.Manifest, cells: list[str]) -> list[str]:
    tc = manifest.table_cells("area")
    return [manifest.path(tc[c]["path"]) for c in cells if c in tc]


def _way_cell_files(manifest: catalog.Manifest, cells: list[str]) -> list[str]:
    tc = manifest.table_cells("way")
    return [manifest.path(tc[c]["path"]) for c in cells if c in tc]


def _is_closed_way_predicate(alias: str) -> str:
    """9.1: closed means ``refs[0] = refs[-1]`` and at least 4 refs (the
    same test the stored ``is_closed`` column encodes at build time, done
    here directly against a canonical row's `refs` array)."""
    return (
        f"{alias}.refs IS NOT NULL AND len({alias}.refs) >= 4 "
        f"AND {alias}.refs[1] = {alias}.refs[len({alias}.refs)]"
    )


def _way_polygon_expr(geom_expr: str) -> str:
    """A way's polygon (9.1), computed on demand from its stored
    LINESTRING. `is_closed` (raw.py) means refs[0] == refs[-1] by *node
    id*, which guarantees the ring is geometrically closed -- but
    real-world coordinate round-tripping (float storage, GeoParquet WKB)
    can leave the stored first/last vertex a few ULPs apart, which
    `ST_MakePolygon` rejects outright ("shell must be closed"). Force
    exact closure by replacing the last vertex with an exact copy of the
    first (same idea as `osmpq.build.areas`'s relation-area assembly)."""
    closed_ring = (
        f"ST_MakeLine(list_append("
        f"list_slice(list_transform(range(1, ST_NPoints({geom_expr})::INTEGER), "
        f"i -> ST_PointN({geom_expr}, i::INTEGER)), 1, ST_NPoints({geom_expr})::INTEGER - 1), "
        f"ST_PointN({geom_expr}, 1)))"
    )
    return f"ST_MakeValid(ST_MakePolygon({closed_ring}))"


def _any_vertex_within_sql(geom_expr: str, poly_expr: str) -> str:
    """9.3 / 9 fact 4: "any vertex ST_Within", never ST_Intersects.
    `ST_Points` reduces a LINESTRING, a GEOMETRYCOLLECTION (a relation's
    member nodes + member way linestrings) or a plain POINT alike to a
    MULTIPOINT of every vertex; `ST_Dump` explodes that into rows so each
    vertex can be tested independently."""
    return (
        f"EXISTS (SELECT 1 FROM UNNEST(ST_Dump(ST_Points({geom_expr}))) AS __pv(pt) "
        f"WHERE ST_Within(__pv.pt.geom, {poly_expr}))"
    )


# --------------------------------------------------------------------------
# area[...], area(id), area.a[...]  (hooks.AREA_QUERY_HOOK)
# --------------------------------------------------------------------------


def _split_area_ids(ids: Optional[list[int]]) -> tuple[Optional[list[int]], Optional[list[int]]]:
    """Splits an `area(id, ...)` id list into (relation area ids, way ids
    to look up directly -- 9.1: `2400000000 <= N < 3600000000` is accepted
    as a lookup of way `N - 2400000000`). `None` (no id filter at all)
    maps to `(None, None)`; an id outside both ranges matches nothing."""
    if ids is None:
        return None, None
    rel_ids = [i for i in ids if i >= RELATION_ID_OFFSET]
    way_ids = [i - WAY_ID_OFFSET for i in ids if WAY_ID_OFFSET <= i < RELATION_ID_OFFSET]
    return rel_ids, way_ids


def _relation_area_raw_select(ctx, rel_ids: Optional[list[int]], tag_filters: list[TagFilter]) -> Optional[str]:
    """Tag/id pushdown against the *raw* relation-area index file
    (promoted columns + `tags` MAP), not the canonical projection:
    `tagsql` pushes a promoted key (e.g. `name`) to its own physical
    column, which only the raw file has."""
    manifest = ctx.manifest
    if manifest.area_index is None:
        return None
    if rel_ids is not None and not rel_ids:
        return None
    where = []
    if rel_ids is not None:
        where.append(idset.id_predicate(ctx.con, "id", rel_ids))
    where.append(tagsql.tag_filters_sql(tag_filters, ctx.promoted_keys))
    where_sql = " AND ".join(f"({w})" for w in where)
    raw_path = manifest.path(manifest.area_index["path"])
    ctx.files_read += 1
    return f"SELECT {project(_AREA_COLS)} FROM read_parquet('{_q1(raw_path)}') __raw WHERE {where_sql}"


def _hydrate_way_rows_select(ctx, id_cell_rows: list[tuple[int, str]]) -> Optional[str]:
    """Full canonical way-row SELECT for `id_cell_rows` ((id, cell)
    pairs), joined against the way spatial files of the involved cells
    (9.3: "way rows hydrated to full canonical rows from the way spatial
    files by cell + id")."""
    pairs = [(c, i) for i, c in id_cell_rows if c]
    if not pairs:
        return None
    needed_cells = sorted({c for c, _ in pairs})
    files = _way_cell_files(ctx.manifest, needed_cells)
    if not files:
        return None
    ctx.files_read += len(files)
    pairs_tbl = idset.register_pairs_table(ctx.con, pairs)
    return (
        f"SELECT {project(_way_cols('t'))} FROM read_parquet({_quote_list(files)}) t "
        f"JOIN {pairs_tbl} p ON t.cell = p.cell AND t.id = p.id"
    )


def _way_area_index_select(ctx, tag_filters: list[TagFilter]) -> Optional[str]:
    """area[...] / area.a[...] with no explicit id: tag-pushdown scan of
    `way_areas.parquet`, hydrated to full canonical way rows."""
    manifest = ctx.manifest
    entry = manifest.way_area_index
    if entry is None:
        return None
    path = manifest.path(entry["path"])
    where_sql = tagsql.tag_filters_sql(tag_filters, ctx.promoted_keys)
    ctx.files_read += 1
    rows = ctx.con.execute(f"SELECT id, cell FROM read_parquet('{_q1(path)}') WHERE {where_sql}").fetchall()
    return _hydrate_way_rows_select(ctx, rows)


def _way_lookup_select(ctx, way_ids: list[int], tag_filters: list[TagFilter]) -> Optional[str]:
    """`area(24xxxxxxxxx)`: a direct way byid lookup (9.1's "superset of
    the reference" convenience) -- not limited to ways in
    `way_areas.parquet`."""
    if not way_ids:
        return None
    sql, nfiles = sources.build_byid_select(ctx.con, ctx.manifest, "way", way_ids, tag_filters, ctx.promoted_keys)
    ctx.files_read += nfiles
    return sql


def _from_set_area_select(ctx, input_sets: list[str], tag_filters: list[TagFilter], ids: Optional[list[int]]) -> str:
    """`area.a[...]` filtering an already-populated set: relation-area rows
    plus closed-way rows already sitting in `input_sets` (9.1: any closed
    way in a set is an area, regardless of how it got there)."""
    base_sql = sources.build_from_set_select(ctx.con, input_sets, ["area", "way"], tag_filters, ids, bbox=None)
    return f"SELECT * FROM ({base_sql}) __aset WHERE type = 'area' OR ({_is_closed_way_predicate('__aset')})"


def area_query_hook(ctx, q: Query) -> None:
    manifest = ctx.manifest
    if not _areas_available(manifest):
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
        base_sql = _from_set_area_select(ctx, q.input_sets, tag_filters, ids)
    else:
        rel_ids, way_ids = _split_area_ids(ids)
        parts = []
        rel_sql = _relation_area_raw_select(ctx, rel_ids, tag_filters)
        if rel_sql is not None:
            parts.append(rel_sql)
        way_sql = _way_lookup_select(ctx, way_ids, tag_filters) if ids is not None else _way_area_index_select(ctx, tag_filters)
        if way_sql is not None:
            parts.append(way_sql)
        base_sql = "\nUNION ALL\n".join(parts) if parts else empty_set_sql()
    setops.materialize(ctx.con, q.output_set, base_sql)


# --------------------------------------------------------------------------
# (area), (area.a), (area:id)
# --------------------------------------------------------------------------


def _relation_area_geom_rows(ctx, rel_ids: Optional[list[int]] = None, set_name: Optional[str] = None) -> list:
    """(id, cell) pairs of relation-area rows to load geometry for."""
    con = ctx.con
    if set_name is not None:
        return con.execute(f"SELECT id, cell FROM set_{set_name} WHERE type = 'area'").fetchall()
    index_sql = _index_select(ctx.manifest)
    if index_sql is None or not rel_ids:
        return []
    id_pred = idset.id_predicate(con, "id", rel_ids)
    return con.execute(f"SELECT id, cell FROM ({index_sql}) __idx WHERE {id_pred}").fetchall()


def _way_area_geom_rows(ctx, way_ids: Optional[list[int]] = None, set_name: Optional[str] = None) -> list:
    """(id, cell) pairs of closed ways to build a polygon for."""
    con = ctx.con
    if set_name is not None:
        alias = f"set_{set_name}"
        return con.execute(
            f"SELECT id, cell FROM {alias} WHERE type = 'way' AND ({_is_closed_way_predicate(alias)})"
        ).fetchall()
    if way_ids:
        way_rows_sql, nfiles = sources.build_byid_select(con, ctx.manifest, "way", way_ids, [], ctx.promoted_keys)
        ctx.files_read += nfiles
        return con.execute(
            f"SELECT id, cell FROM ({way_rows_sql}) __w WHERE ({_is_closed_way_predicate('__w')})"
        ).fetchall()
    return []


def _area_polygon_table(ctx, area_ids: Optional[list[int]] = None, set_name: Optional[str] = None) -> tuple[Optional[str], Optional[BBox]]:
    """Loads the polygon + bbox of the referenced areas -- relation-area
    rows (from the spatial area files, keyed by cell + id) and closed-way
    rows (polygon built on demand from the way's own LINESTRING) alike --
    into a fresh TEMP TABLE(id, geometry, xmin_e7, ymin_e7, xmax_e7,
    ymax_e7). Returns (table_name, union bbox) or (None, None) when there
    is nothing to match."""
    manifest = ctx.manifest
    con = ctx.con
    if set_name is not None:
        _require_set(ctx, set_name)
        rel_rows = _relation_area_geom_rows(ctx, set_name=set_name)
        way_rows = _way_area_geom_rows(ctx, set_name=set_name)
    else:
        rel_ids, way_ids = _split_area_ids(area_ids)
        rel_rows = _relation_area_geom_rows(ctx, rel_ids=rel_ids)
        way_rows = _way_area_geom_rows(ctx, way_ids=way_ids)

    tables: list[str] = []

    rel_pairs = [(c, i) for i, c in rel_rows if c]
    if rel_pairs:
        needed_cells = sorted({c for c, _ in rel_pairs})
        files = _area_cell_files(manifest, needed_cells)
        if files:
            ctx.files_read += len(files)
            pairs_tbl = idset.register_pairs_table(con, rel_pairs)
            t = ctx.fresh_name("areapoly_rel")
            con.execute(
                f"CREATE TEMP TABLE {t} AS "
                f"SELECT t.id, t.geometry, t.xmin_e7, t.ymin_e7, t.xmax_e7, t.ymax_e7 "
                f"FROM read_parquet({_quote_list(files)}) t JOIN {pairs_tbl} p ON t.cell = p.cell AND t.id = p.id"
            )
            tables.append(t)

    way_pairs = [(c, i) for i, c in way_rows if c]
    if way_pairs:
        needed_cells = sorted({c for c, _ in way_pairs})
        files = _way_cell_files(manifest, needed_cells)
        if files:
            ctx.files_read += len(files)
            pairs_tbl = idset.register_pairs_table(con, way_pairs)
            poly_expr = _way_polygon_expr("t.geometry")
            t = ctx.fresh_name("areapoly_way")
            con.execute(
                f"CREATE TEMP TABLE {t} AS "
                f"SELECT t.id, {poly_expr} AS geometry, t.xmin_e7, t.ymin_e7, t.xmax_e7, t.ymax_e7 "
                f"FROM read_parquet({_quote_list(files)}) t JOIN {pairs_tbl} p ON t.cell = p.cell AND t.id = p.id "
                f"WHERE t.geometry IS NOT NULL AND ST_NPoints(t.geometry) >= 4"
            )
            if con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]:
                tables.append(t)
            else:
                con.execute(f"DROP TABLE {t}")

    if not tables:
        return None, None
    name = ctx.fresh_name("areapoly")
    union_sql = "\nUNION ALL\n".join(f"SELECT * FROM {t}" for t in tables)
    con.execute(f"CREATE TEMP TABLE {name} AS {union_sql}")
    for t in tables:
        con.execute(f"DROP TABLE {t}")
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
    vertex_test = _any_vertex_within_sql(f"{alias}.geometry", "g.geometry")
    return (
        f"({alias}.type = 'way' AND ("
        f"({alias}.geometry IS NOT NULL AND EXISTS (SELECT 1 FROM {geom_tbl} g WHERE {vertex_test})) "
        f"OR ({alias}.geometry IS NULL AND EXISTS (SELECT 1 FROM {geom_tbl} g WHERE "
        f"{alias}.xmax_e7 >= g.xmin_e7 AND {alias}.xmin_e7 <= g.xmax_e7 AND "
        f"{alias}.ymax_e7 >= g.ymin_e7 AND {alias}.ymin_e7 <= g.ymax_e7))"
        f"))"
    )


def _relation_match_sql(ctx, alias: str, geom_tbl: str, area_bbox: Optional[BBox]) -> str:
    rel_geom_tbl = _relation_candidate_geometry(ctx, area_bbox)
    if rel_geom_tbl is not None:
        vertex_test = _any_vertex_within_sql("rg.geometry", "g.geometry")
        return (
            f"({alias}.type = 'relation' AND EXISTS (SELECT 1 FROM {rel_geom_tbl} rg, {geom_tbl} g "
            f"WHERE rg.id = {alias}.id AND {vertex_test}))"
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
                cache[key] = _area_polygon_table(ctx, area_ids=[f.area_id])
            else:
                cache[key] = _area_polygon_table(ctx, set_name=f.set_name or "_")
        return cache[key]

    def implied_bbox(self, ctx, q, f: AreaFilter) -> Optional[BBox]:
        if not _areas_available(ctx.manifest):
            _warn_no_areas(ctx)
        _geom_tbl, bbox = self._geom(ctx, f)
        return bbox

    def predicate(self, ctx, q, f: AreaFilter, alias: str) -> str:
        geom_tbl, bbox = self._geom(ctx, f)
        if geom_tbl is None:
            if not _areas_available(ctx.manifest):
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
# (pivot.a): way(pivot.a) = the closed ways in `a` themselves;
# rel(pivot.a) = relations whose derived area id is in `a` (9.3)
# --------------------------------------------------------------------------


class _PivotFilterHook:
    def implied_bbox(self, ctx, q, f: PivotFilter) -> Optional[BBox]:
        _require_set(ctx, f.set_name)
        row = ctx.con.execute(
            f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
            f"FROM set_{f.set_name} WHERE type IN ('area', 'way')"
        ).fetchone()
        return None if None in row else (row[0], row[1], row[2], row[3])

    def predicate(self, ctx, q, f: PivotFilter, alias: str) -> str:
        _require_set(ctx, f.set_name)
        way_closed = _is_closed_way_predicate(f"set_{f.set_name}")
        return (
            f"(({alias}.type = 'way' AND {alias}.id IN "
            f"(SELECT id FROM set_{f.set_name} WHERE type = 'way' AND ({way_closed}))) "
            f"OR ({alias}.type = 'relation' AND ({alias}.id + {RELATION_ID_OFFSET}) IN "
            f"(SELECT id FROM set_{f.set_name} WHERE type = 'area')))"
        )


# --------------------------------------------------------------------------
# is_in / is_in(lat, lon) / .a is_in -> .b
# --------------------------------------------------------------------------


def _relation_isin_bbox(con, input_set: str) -> Optional[BBox]:
    row = con.execute(
        f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
        f"FROM set_{input_set} WHERE type = 'relation'"
    ).fetchone()
    return None if None in row else (row[0], row[1], row[2], row[3])


def _relation_isin_sql(cand_cols: dict, cand_table: str, input_set: str, rel_geom_tbl: Optional[str], poly_expr: str) -> str:
    if rel_geom_tbl is not None:
        vertex_test = _any_vertex_within_sql("rg.geometry", poly_expr)
        return (
            f"SELECT {project(cand_cols)} FROM {cand_table} a "
            f"JOIN {rel_geom_tbl} rg ON TRUE "
            f"JOIN set_{input_set} s ON s.type = 'relation' AND s.id = rg.id "
            f"WHERE {vertex_test}"
        )
    return (
        f"SELECT {project(cand_cols)} FROM {cand_table} a JOIN set_{input_set} s ON s.type = 'relation' "
        f"WHERE s.xmax_e7 >= a.xmin_e7 AND s.xmin_e7 <= a.xmax_e7 "
        f"AND s.ymax_e7 >= a.ymin_e7 AND s.ymin_e7 <= a.ymax_e7"
    )


def _way_isin_sql(cand_cols: dict, cand_table: str, input_set: str, poly_expr: str) -> str:
    vertex_test = _any_vertex_within_sql("s.geometry", poly_expr)
    return (
        f"SELECT {project(cand_cols)} FROM {cand_table} a JOIN set_{input_set} s ON s.type = 'way' "
        f"WHERE (s.geometry IS NOT NULL AND {vertex_test}) "
        f"OR (s.geometry IS NULL AND s.xmax_e7 >= a.xmin_e7 AND s.xmin_e7 <= a.xmax_e7 "
        f"AND s.ymax_e7 >= a.ymin_e7 AND s.ymin_e7 <= a.ymax_e7)"
    )


def is_in_statement(ctx, stmt: IsIn) -> None:
    manifest = ctx.manifest
    con = ctx.con
    if not _areas_available(manifest):
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

    rel_files: list[str] = []
    if manifest.area_index is not None:
        rel_cells = catalog.cells_for_bbox(manifest, "area", input_bbox)
        rel_files = _area_cell_files(manifest, rel_cells)
    way_files: list[str] = []
    if manifest.way_area_index is not None:
        # 9.3: closed ways whose bbox contains the point, from the way
        # spatial files of the cells covering the input -- ways are placed
        # loosely (v2), so a containing way is in a cell whose bbox
        # contains the point.
        way_cells = catalog.cells_for_bbox(manifest, "way", input_bbox)
        way_files = _way_cell_files(manifest, way_cells)

    if not rel_files and not way_files:
        setops.materialize(con, stmt.output_set, empty_set_sql())
        return

    if rel_files:
        ctx.files_read += len(rel_files)
        con.execute(f"CREATE OR REPLACE TEMP TABLE __isin_rel AS SELECT * FROM read_parquet({_quote_list(rel_files)})")
    if way_files:
        ctx.files_read += len(way_files)
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE __isin_way AS "
            f"SELECT * FROM read_parquet({_quote_list(way_files)}) "
            f"WHERE is_closed AND geometry IS NOT NULL AND ST_NPoints(geometry) >= 4"
        )
    way_poly = _way_polygon_expr("a.geometry")
    way_cols_a = _way_cols("a")
    rel_cols_a = _area_cols("a")

    if stmt.coords is not None:
        lat, lon = stmt.coords
        parts = []
        if rel_files:
            parts.append(f"SELECT {project(_AREA_COLS)} FROM __isin_rel a WHERE ST_Within(ST_Point({lon}, {lat}), a.geometry)")
        if way_files:
            parts.append(
                f"SELECT {project(way_cols_a)} FROM __isin_way a WHERE ST_Within(ST_Point({lon}, {lat}), {way_poly})"
            )
        match_sql = "\nUNION ALL\n".join(parts)
    else:
        rel_bbox = _relation_isin_bbox(con, stmt.input_set)
        rel_geom_tbl = _relation_candidate_geometry(ctx, rel_bbox) if rel_bbox is not None else None

        parts = []
        if rel_files:
            parts.append(
                f"SELECT {project(rel_cols_a)} FROM __isin_rel a JOIN set_{stmt.input_set} s ON s.type = 'node' "
                f"WHERE ST_Within(ST_Point(s.lon_e7 / 1e7, s.lat_e7 / 1e7), a.geometry)"
            )
            parts.append(_way_isin_sql(rel_cols_a, "__isin_rel", stmt.input_set, "a.geometry"))
            if rel_bbox is not None:
                parts.append(_relation_isin_sql(rel_cols_a, "__isin_rel", stmt.input_set, rel_geom_tbl, "a.geometry"))
        if way_files:
            parts.append(
                f"SELECT {project(way_cols_a)} FROM __isin_way a JOIN set_{stmt.input_set} s ON s.type = 'node' "
                f"WHERE ST_Within(ST_Point(s.lon_e7 / 1e7, s.lat_e7 / 1e7), {way_poly})"
            )
            parts.append(_way_isin_sql(way_cols_a, "__isin_way", stmt.input_set, way_poly))
            if rel_bbox is not None:
                parts.append(_relation_isin_sql(way_cols_a, "__isin_way", stmt.input_set, rel_geom_tbl, way_poly))
        match_sql = "SELECT DISTINCT * FROM (\n" + "\nUNION ALL\n".join(parts) + "\n) __m"

    setops.materialize(con, stmt.output_set, match_sql)


# --------------------------------------------------------------------------
# map_to_area: closed ways in the input -> themselves; relations -> their
# area row when one exists (9.3)
# --------------------------------------------------------------------------


def map_to_area_statement(ctx, stmt: MapToArea) -> None:
    _require_set(ctx, stmt.input_set)
    manifest = ctx.manifest
    if not _areas_available(manifest):
        _warn_no_areas(ctx)
        setops.materialize(ctx.con, stmt.output_set, empty_set_sql())
        return

    input_alias = f"set_{stmt.input_set}"
    parts = [f"SELECT * FROM {input_alias} WHERE type = 'way' AND ({_is_closed_way_predicate(input_alias)})"]
    index_sql = _index_select(manifest)
    if index_sql is not None:
        sql = (
            f"SELECT idx.* FROM ({index_sql}) idx WHERE idx.id IN ("
            f"SELECT id + {RELATION_ID_OFFSET} FROM {input_alias} WHERE type = 'relation')"
        )
        ctx.files_read += 1
        parts.append(sql)
    final_sql = "\nUNION ALL\n".join(parts)
    setops.materialize(ctx.con, stmt.output_set, final_sql)


# --------------------------------------------------------------------------
# registration
# --------------------------------------------------------------------------

hooks.register_filter(AreaFilter, _AreaFilterHook())
hooks.register_filter(PivotFilter, _PivotFilterHook())
hooks.register_statement(IsIn, is_in_statement)
hooks.register_statement(MapToArea, map_to_area_statement)
hooks.set_area_query_hook(area_query_hook)
