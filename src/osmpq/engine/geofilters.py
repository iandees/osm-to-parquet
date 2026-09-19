"""`(around:r)`, `(around.set:r)`, `(around:r,lat,lon[,lat,lon...])` and
`(poly:"lat lon ...")` (docs/m3-contracts.md section 3), registered as
`hooks.FilterHook`s, plus the public `relation_geometry_table` helper
(section 3.5) that W2's `(area)`/`is_in` reuse for relation geometry.

Distance approach (section 3.1)
--------------------------------
All distances are meters on the WGS84 spheroid, computed by projecting
into a **local equirectangular plane** (`+proj=eqc +lat_ts=<centre lat of
the query's source geometry>`, via DuckDB spatial's `ST_Transform`) and
then using plain planar `ST_Distance`/`ST_DWithin` in that projection.

This was chosen over `ST_Distance_Spheroid`/`ST_DWithin_Spheroid` for two
reasons, both confirmed empirically against this DuckDB build (1.5.5,
`spatial` extension) before picking an approach:

1. **They only accept `POINT_2D` operands** (`ST_Distance_Spheroid only
   accepts POINT geometries` at runtime) -- `around`/`poly` need
   point-to-linestring and point-to-geometrycollection distances (way and
   relation candidates/sources), which those functions cannot compute at
   all.
2. **Their coordinate order is the opposite of the WKT convention this
   codebase already uses everywhere else.** Way geometry is stored and
   parsed as `LINESTRING (lon lat, ...)` (see `render.parse_linestring_wkt`),
   i.e. X=longitude, Y=latitude, the standard GIS/WKT convention. Empirically
   (`ST_Distance_Sphere`/`ST_Distance_Spheroid` against a haversine
   reference for real Minnesota-latitude pairs):

       ST_Distance_Sphere(ST_GeomFromText('POINT (lon lat)'), ...)   -- WRONG
       ST_Distance_Sphere(ST_GeomFromText('POINT (lat lon)'), ...)   -- matches haversine

   i.e. these two functions treat a point's *first* ordinate as latitude
   and the *second* as longitude -- backwards from the X=lon/Y=lat
   geometries used everywhere else in this codebase (`ST_FlipCoordinates`
   exists in DuckDB spatial precisely to paper over this). Using them
   would mean flipping coordinates only for this one pair of calls and
   remembering why forever.

The equirectangular projection sidesteps both problems: it accepts any
geometry type, and it takes ordinary X=lon/Y=lat input (`ST_Transform(...,
'EPSG:4326', <proj4>, always_xy := true)`), matching every other geometry
built in this codebase. Verified against a haversine reference for a few
Minneapolis-area point pairs to within ~0.15% (`test_engine_geofilters.py`
-- comfortably inside the 0.2% the contract asks for), and a
point-to-linestring case is asserted geometrically (0 m for a point on the
line, ~11.1 km for a point 0.1 degrees off a west-east segment at that
latitude).

The projection is centred (`lat_ts`) on the centre latitude of whichever
bbox is at hand (the source geometry's bbox for `around`, the polygon's
bbox for `poly`), so distance error stays small near the region a query
actually touches; it is not meant to be accurate as a general-purpose
projection far from that centre.

`(poly:...)` does not need any of this: `ST_Within`/`ST_Intersects`
against the query polygon are plain topological tests in EPSG:4326
degrees, not distance measurements, so no projection is involved there.
"""
from __future__ import annotations

import math
from typing import Optional

from osmpq.errors import RuntimeQueryError
from osmpq.ql.ast import AroundFilter, BboxFilter, PolyFilter

from . import hooks, render, sources

BBox = tuple[float, float, float, float]

METERS_PER_DEGREE_LAT = 111320.0

# hooks.is_empty_bbox treats any bbox with south > north as "empty"; used
# for AroundFilter.implied_bbox when the source set has no elements
# (contract 3.2: "Empty source set -> empty result"), which the planner
# then short-circuits before ever calling `predicate`.
_EMPTY_BBOX: BBox = (1.0, 0.0, -1.0, 0.0)


# --------------------------------------------------------------------------
# small SQL/geometry helpers
# --------------------------------------------------------------------------


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _proj4(lat_ts: float) -> str:
    return (
        f"+proj=eqc +lat_ts={lat_ts:.6f} +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 "
        "+datum=WGS84 +units=m +no_defs"
    )


def _project_sql(geom_expr: str, lat_ts: float) -> str:
    return f"ST_Transform({geom_expr}, 'EPSG:4326', {_sql_str(_proj4(lat_ts))}, always_xy := true)"


def _lat_ts(bbox: BBox) -> float:
    s, _w, n, _e = bbox
    center = (s + n) / 2.0
    # Clamp away from the poles: +proj=eqc is undefined at lat_ts=+-90.
    return max(-89.0, min(89.0, center))


def _bbox_of_coords(coords: list[tuple[float, float]]) -> BBox:
    lats = [c[0] for c in coords]
    lons = [c[1] for c in coords]
    return (min(lats), min(lons), max(lats), max(lons))


def _coords_to_wkt(coords: list[tuple[float, float]]) -> str:
    """`coords` is `[(lat, lon), ...]` (contract convention); WKT wants
    `X Y` = `lon lat`."""
    if len(coords) == 1:
        lat, lon = coords[0]
        return f"POINT ({lon} {lat})"
    pts = ", ".join(f"{lon} {lat}" for lat, lon in coords)
    return f"LINESTRING ({pts})"


def _poly_to_wkt(coords: list[tuple[float, float]]) -> str:
    pts = list(coords)
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    ring = ", ".join(f"{lon} {lat}" for lat, lon in pts)
    return f"POLYGON (({ring}))"


def _bbox_envelope_sql(alias: str) -> str:
    return (
        f"ST_MakeEnvelope({alias}.xmin_e7/10000000.0, {alias}.ymin_e7/10000000.0, "
        f"{alias}.xmax_e7/10000000.0, {alias}.ymax_e7/10000000.0)"
    )


def _require_set(ctx, name: str) -> None:
    exists = ctx.con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [f"set_{name}"]
    ).fetchone()[0]
    if not exists:
        raise RuntimeQueryError(f'runtime error: set ".{name}" has not been set before')


def _bbox_of_set(ctx, name: str) -> Optional[BBox]:
    """Union bbox (degrees) of every element in `set_<name>`, using each
    row's own point (nodes) or stored bbox columns (ways/relations --
    already the union of their own members from build time), *not* full
    geometry resolution: implied_bbox only needs to bound the candidate
    cell/row-group selection, the predicate does the exact test."""
    row = ctx.con.execute(
        f"""
        SELECT
          min(CASE WHEN type = 'node' THEN lat_e7 ELSE ymin_e7 END),
          max(CASE WHEN type = 'node' THEN lat_e7 ELSE ymax_e7 END),
          min(CASE WHEN type = 'node' THEN lon_e7 ELSE xmin_e7 END),
          max(CASE WHEN type = 'node' THEN lon_e7 ELSE xmax_e7 END)
        FROM set_{name}
        """
    ).fetchone()
    lat_min_e7, lat_max_e7, lon_min_e7, lon_max_e7 = row
    if None in (lat_min_e7, lat_max_e7, lon_min_e7, lon_max_e7):
        return None
    return (lat_min_e7 / 1e7, lon_min_e7 / 1e7, lat_max_e7 / 1e7, lon_max_e7 / 1e7)


def _expand_bbox_by_radius(bbox: BBox, radius_m: float) -> BBox:
    """Contract 3.2: degrees via 111320 m/degree latitude and the cosine
    of the bbox's largest absolute latitude (the conservative choice: a
    smaller cosine near the poles means a bigger longitude buffer, so
    cell selection never undershoots)."""
    s, w, n, e = bbox
    max_abs_lat = max(abs(s), abs(n))
    lat_deg = radius_m / METERS_PER_DEGREE_LAT
    cos_lat = max(math.cos(math.radians(min(max_abs_lat, 89.9))), 1e-6)
    lon_deg = radius_m / (METERS_PER_DEGREE_LAT * cos_lat)
    return (s - lat_deg, w - lon_deg, n + lat_deg, e + lon_deg)


def _effective_bbox_for_query(ctx, q) -> Optional[BBox]:
    """Re-derive the same effective bbox `planner._effective_bbox` computed
    for this query (explicit/global bbox intersected with every hooked
    filter's own implied_bbox) -- used only to rediscover the universe of
    relation *candidates* a geo-filter's predicate needs geometry for
    (see `_relation_candidate_geom_table`); the predicate itself still does
    the exact per-row test."""
    bbox: Optional[BBox] = None
    for f in q.filters:
        if isinstance(f, BboxFilter):
            bbox = (f.south, f.west, f.north, f.east)
    if bbox is None:
        bbox = ctx.global_bbox
    for f in q.filters:
        hook = hooks.FILTER_HOOKS.get(type(f))
        if hook is not None:
            bbox = hooks.intersect_bbox(bbox, hook.implied_bbox(ctx, q, f))
    return bbox


# --------------------------------------------------------------------------
# 3.5: relation_geometry_table (public; reused by W2)
# --------------------------------------------------------------------------


def relation_geometry_table(ctx, relation_rows_sql: str) -> str:
    """Contract 3.5. `relation_rows_sql` is a `SELECT` of canonical rows of
    type "relation" (e.g. `SELECT * FROM set_x WHERE type = 'relation'`, or
    a bbox-scoped relation candidate select). Returns the name of a fresh
    TEMP TABLE `(id BIGINT, geometry GEOMETRY)`: one row per relation that
    has at least one resolvable member, `geometry` a GEOMETRYCOLLECTION
    (DuckDB may narrow it to a MULTIPOINT/MULTILINESTRING when every
    member happens to be the same shape type -- both are GEOMETRY, no
    caller-visible difference) of its member node points and member way
    linestrings. Relations with no resolvable member are absent from the
    table (contract: "callers treat absence as no match")."""
    result_tbl = ctx.fresh_name("relgeom")
    ctx.con.execute(f"CREATE TEMP TABLE {result_tbl} (id BIGINT, geometry GEOMETRY)")

    cand_tbl = ctx.fresh_name("relgeomcand")
    ctx.con.execute(f"CREATE TEMP TABLE {cand_tbl} AS {relation_rows_sql}")
    n_cand = ctx.con.execute(f"SELECT count(*) FROM {cand_tbl}").fetchone()[0]
    if not n_cand:
        return result_tbl

    # A relation's member nodes/ways lie inside the relation's own bbox
    # (same reasoning as render.py's out-geom member resolution and
    # sources.build_way_hydrate_via_bbox_select's own docstring), so the
    # union of these candidate relations' own bboxes bounds every member
    # worth resolving.
    bbox_selects = [f"SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM {cand_tbl}"]

    node_ids_tbl = ctx.fresh_name("relgeomnodeids")
    ctx.con.execute(
        f"CREATE TEMP TABLE {node_ids_tbl} AS "
        f"SELECT DISTINCT m.ref AS id FROM {cand_tbl}, UNNEST(members) AS t(m) WHERE m.type = 'n'"
    )
    way_ids_tbl = ctx.fresh_name("relgeomwayids")
    ctx.con.execute(
        f"CREATE TEMP TABLE {way_ids_tbl} AS "
        f"SELECT DISTINCT m.ref AS id FROM {cand_tbl}, UNNEST(members) AS t(m) WHERE m.type = 'w'"
    )

    member_parts: list[str] = []

    node_sql, nfiles_n = sources.build_node_hydrate_via_bbox_select(
        ctx.con, ctx.manifest, node_ids_tbl, bbox_selects, ctx.promoted_keys
    )
    ctx.files_read += nfiles_n
    if node_sql:
        node_geom_tbl = ctx.fresh_name("relgeomnodes")
        ctx.con.execute(
            f"CREATE TEMP TABLE {node_geom_tbl} AS "
            f"SELECT id, ST_Point(lon_e7/10000000.0, lat_e7/10000000.0) AS geometry FROM ({node_sql}) __n"
        )
        member_parts.append(
            f"SELECT c.id AS relation_id, ng.geometry AS geometry "
            f"FROM {cand_tbl} c, UNNEST(c.members) AS t(m) "
            f"JOIN {node_geom_tbl} ng ON m.type = 'n' AND m.ref = ng.id"
        )

    way_sql, nfiles_w = sources.build_way_hydrate_via_bbox_select(
        ctx.con, ctx.manifest, way_ids_tbl, bbox_selects, ctx.promoted_keys
    )
    ctx.files_read += nfiles_w
    if way_sql:
        way_rows = ctx.con.execute(
            f"SELECT id, cell, ST_AsText(geometry) AS geometry_wkt FROM ({way_sql}) __w"
        ).fetchall()
        rows = [{"type": "way", "id": r[0], "cell": r[1], "geometry_wkt": r[2]} for r in way_rows]
        # Second pass, same idea as render.resolve_way_geometries: the
        # bbox-scoped attempt above unions *every* candidate relation's
        # bbox into one selection, so it can miss (or trip the
        # max_cell_fraction guard and fall straight to byid, which has no
        # geometry column for ways) when candidate relations are far
        # apart. Fall back to the (cell, id) spatial join for whatever
        # came back NULL -- this doesn't add to ctx.files_read
        # (render.hydrate_way_geometry doesn't report a count either; the
        # same gap already exists in render.build_elements' own use of it
        # for top-level `out geom`).
        render.hydrate_way_geometry(ctx.con, ctx.manifest, rows)
        wkts = [(r["id"], r["geometry_wkt"]) for r in rows if r.get("geometry_wkt")]
        if wkts:
            way_geom_tbl = ctx.fresh_name("relgeomways")
            values = ", ".join(f"({wid}, ST_GeomFromText({_sql_str(wkt)}))" for wid, wkt in wkts)
            ctx.con.execute(f"CREATE TEMP TABLE {way_geom_tbl} AS SELECT * FROM (VALUES {values}) AS t(id, geometry)")
            member_parts.append(
                f"SELECT c.id AS relation_id, wg.geometry AS geometry "
                f"FROM {cand_tbl} c, UNNEST(c.members) AS t(m) "
                f"JOIN {way_geom_tbl} wg ON m.type = 'w' AND m.ref = wg.id"
            )

    if not member_parts:
        return result_tbl

    union_sql = "\nUNION ALL\n".join(member_parts)
    ctx.con.execute(
        f"INSERT INTO {result_tbl} "
        f"SELECT relation_id AS id, ST_Collect(list(geometry)) AS geometry "
        f"FROM ({union_sql}) __m GROUP BY relation_id"
    )
    return result_tbl


def _relation_candidate_geom_table(ctx, q) -> Optional[str]:
    """Geometry table (3.5) for whatever relations *could* appear as
    candidates of this query, so a geo-filter's predicate can look one up
    by id via a correlated subquery. Only meaningful when the query
    actually selects relations; rediscovers the candidate universe via the
    same bbox-scoped relation select the base query itself would use (a
    predicate has no access to the base SELECT's SQL text, only to `ctx`/
    `q`/the row alias -- see hooks.py's module docstring), which is exact
    for the common case (no `ids`/named input set on the query) and a safe
    superset otherwise (extra rows are harmless: the predicate only reads
    a relation's geometry by id if that id is actually a candidate row)."""
    if "relation" not in q.types:
        return None
    base_tbl = getattr(ctx, "current_base_table", None)
    if base_tbl is not None:
        # The planner materialized this query's filtered candidates before
        # calling predicates: resolve member geometry for those only.
        return relation_geometry_table(ctx, f"SELECT * FROM {base_tbl} WHERE type = 'relation'")
    bbox = _effective_bbox_for_query(ctx, q)
    cand_sql, nfiles = sources.build_relation_spatial_select(ctx.con, ctx.manifest, bbox, [], None, ctx.promoted_keys)
    ctx.files_read += nfiles
    return relation_geometry_table(ctx, cand_sql)


# --------------------------------------------------------------------------
# around/poly source geometry
# --------------------------------------------------------------------------


def _named_set_source_geometry_sql(ctx, name: str) -> Optional[str]:
    """A scalar SQL expression producing one combined GEOMETRY (via
    ST_Collect) for every resolvable node/way/relation geometry in
    `set_<name>` (EPSG:4326, un-projected), or None if nothing resolves."""
    table = f"set_{name}"
    parts = [
        f"SELECT ST_Point(lon_e7 / 10000000.0, lat_e7 / 10000000.0) AS geometry "
        f"FROM {table} WHERE type = 'node' AND lat_e7 IS NOT NULL AND lon_e7 IS NOT NULL"
    ]

    # Ways: hydrate any missing `geometry` (byid-sourced set rows) via the
    # same (cell, id) join `out geom` uses for byid way rows -- contract
    # 3.2 "hydrated through render.hydrate_way_geometry-style logic when
    # NULL". Source sets are small (a handful to a few hundred elements),
    # so a Python round trip here is cheap.
    way_rows = ctx.con.execute(
        f"SELECT id, cell, ST_AsText(geometry) AS geometry_wkt FROM {table} WHERE type = 'way'"
    ).fetchall()
    if way_rows:
        rows = [{"type": "way", "id": r[0], "cell": r[1], "geometry_wkt": r[2]} for r in way_rows]
        render.hydrate_way_geometry(ctx.con, ctx.manifest, rows)
        wkts = [r["geometry_wkt"] for r in rows if r.get("geometry_wkt")]
        if wkts:
            missing = len(rows) - len(wkts)
            if missing:
                ctx.warnings.append(
                    f"around/poly: {missing} way(s) in the source set '.{name}' had no "
                    "resolvable geometry and were skipped"
                )
            values = ", ".join(f"(ST_GeomFromText({_sql_str(w)}))" for w in wkts)
            parts.append(f"SELECT geometry FROM (VALUES {values}) AS t(geometry)")

    rel_count = ctx.con.execute(f"SELECT count(*) FROM {table} WHERE type = 'relation'").fetchone()[0]
    if rel_count:
        rel_tbl = relation_geometry_table(ctx, f"SELECT * FROM {table} WHERE type = 'relation'")
        parts.append(f"SELECT geometry FROM {rel_tbl}")
        missing = ctx.con.execute(
            f"SELECT count(*) FROM {table} r WHERE r.type = 'relation' "
            f"AND r.id NOT IN (SELECT id FROM {rel_tbl})"
        ).fetchone()[0]
        if missing:
            ctx.warnings.append(
                f"around/poly: {missing} relation(s) in the source set '.{name}' had no "
                "resolvable member geometry and were skipped"
            )

    union_sql = "\nUNION ALL\n".join(parts)
    n = ctx.con.execute(f"SELECT count(*) FROM ({union_sql}) __src WHERE geometry IS NOT NULL").fetchone()[0]
    if not n:
        return None
    return f"(SELECT ST_Collect(list(geometry)) FROM ({union_sql}) __src WHERE geometry IS NOT NULL)"


def _materialize_around_source(ctx, f: AroundFilter) -> tuple[Optional[str], float]:
    """Returns (TEMP TABLE name holding one row `(geometry GEOMETRY)`,
    already projected into the local eqc plane, `lat_ts`), or (None,
    <anything>) when the source is empty."""
    if f.coords:
        bbox = _bbox_of_coords(f.coords)
        lat_ts = _lat_ts(bbox)
        geom_expr = f"ST_GeomFromText({_sql_str(_coords_to_wkt(f.coords))})"
        tbl = ctx.fresh_name("aroundsrc")
        ctx.con.execute(f"CREATE TEMP TABLE {tbl} AS SELECT {_project_sql(geom_expr, lat_ts)} AS geometry")
        return tbl, lat_ts

    name = f.set_name or "_"
    _require_set(ctx, name)
    bbox = _bbox_of_set(ctx, name)
    if bbox is None:
        return None, 0.0
    lat_ts = _lat_ts(bbox)
    src_sql = _named_set_source_geometry_sql(ctx, name)
    if src_sql is None:
        return None, lat_ts
    tbl = ctx.fresh_name("aroundsrc")
    ctx.con.execute(f"CREATE TEMP TABLE {tbl} AS SELECT {_project_sql(src_sql, lat_ts)} AS geometry")
    return tbl, lat_ts


# --------------------------------------------------------------------------
# AroundFilter hook
# --------------------------------------------------------------------------


def _around_implied_bbox(ctx, f: AroundFilter) -> Optional[BBox]:
    if f.coords:
        bbox = _bbox_of_coords(f.coords)
    else:
        name = f.set_name or "_"
        _require_set(ctx, name)
        bbox = _bbox_of_set(ctx, name)
        if bbox is None:
            return _EMPTY_BBOX
    return _expand_bbox_by_radius(bbox, f.radius)


def _distance_predicate(ctx, q, alias: str, src_tbl: str, radius: float, lat_ts: float) -> str:
    src_expr = f"(SELECT geometry FROM {src_tbl})"
    node_geom = f"ST_Point({alias}.lon_e7 / 10000000.0, {alias}.lat_e7 / 10000000.0)"
    way_geom = f"COALESCE({alias}.geometry, {_bbox_envelope_sql(alias)})"

    if "way" in q.types and q.input_sets:
        # contract 3.1/3.2: a way read from a named input set (rather than
        # this query's own bbox scan) can carry a NULL `geometry` (its set
        # was built via a byid lookup, which has no geometry column at
        # all) -- the bbox-rectangle fallback above covers it, but it's
        # only an approximation, so flag it.
        ctx.warnings.append(
            "around/poly: way candidates read from a named input set may lack resolved "
            "geometry; falling back to each way's stored bounding box"
        )

    rel_tbl = _relation_candidate_geom_table(ctx, q)
    if rel_tbl is not None:
        rel_geom = f"(SELECT geometry FROM {rel_tbl} rg WHERE rg.id = {alias}.id)"
        rel_case = (
            f"WHEN 'relation' THEN ({rel_geom} IS NOT NULL AND "
            f"ST_DWithin({_project_sql(rel_geom, lat_ts)}, {src_expr}, {radius}))\n"
        )
    else:
        rel_case = "WHEN 'relation' THEN FALSE\n"

    return (
        f"CASE {alias}.type\n"
        f"WHEN 'node' THEN ST_DWithin({_project_sql(node_geom, lat_ts)}, {src_expr}, {radius})\n"
        f"WHEN 'way' THEN ST_DWithin({_project_sql(way_geom, lat_ts)}, {src_expr}, {radius})\n"
        f"{rel_case}"
        f"ELSE FALSE END"
    )


def _around_predicate(ctx, q, f: AroundFilter, alias: str) -> str:
    src_tbl, lat_ts = _materialize_around_source(ctx, f)
    if src_tbl is None:
        return "FALSE"
    return _distance_predicate(ctx, q, alias, src_tbl, f.radius, lat_ts)


class _AroundHook:
    def implied_bbox(self, ctx, q, f):
        return _around_implied_bbox(ctx, f)

    def predicate(self, ctx, q, f, alias):
        return _around_predicate(ctx, q, f, alias)


# --------------------------------------------------------------------------
# PolyFilter hook
# --------------------------------------------------------------------------


def _poly_implied_bbox(f: PolyFilter) -> BBox:
    return _bbox_of_coords(f.coords)


def _poly_predicate(ctx, q, f: PolyFilter, alias: str) -> str:
    poly_expr = f"ST_GeomFromText({_sql_str(_poly_to_wkt(f.coords))})"
    node_geom = f"ST_Point({alias}.lon_e7 / 10000000.0, {alias}.lat_e7 / 10000000.0)"
    way_geom = f"COALESCE({alias}.geometry, {_bbox_envelope_sql(alias)})"

    if "way" in q.types and q.input_sets:
        ctx.warnings.append(
            "around/poly: way candidates read from a named input set may lack resolved "
            "geometry; falling back to each way's stored bounding box"
        )

    rel_tbl = _relation_candidate_geom_table(ctx, q)
    if rel_tbl is not None:
        rel_geom = f"(SELECT geometry FROM {rel_tbl} rg WHERE rg.id = {alias}.id)"
        rel_case = f"WHEN 'relation' THEN ({rel_geom} IS NOT NULL AND ST_Intersects({rel_geom}, {poly_expr}))\n"
    else:
        rel_case = "WHEN 'relation' THEN FALSE\n"

    # contract 3.3: node -> ST_Within(point, poly); way -> ST_Intersects
    # (linestring, poly); relation -> "any member point within or any
    # member way intersects", which ST_Intersects(collection, poly)
    # already expresses (a point inside a polygon's interior intersects
    # it; a point exactly on the boundary is the one edge case where
    # ST_Intersects is slightly more permissive than ST_Within -- an
    # acceptable, documented simplification for the collection case).
    return (
        f"CASE {alias}.type\n"
        f"WHEN 'node' THEN ST_Within({node_geom}, {poly_expr})\n"
        f"WHEN 'way' THEN ST_Intersects({way_geom}, {poly_expr})\n"
        f"{rel_case}"
        f"ELSE FALSE END"
    )


class _PolyHook:
    def implied_bbox(self, ctx, q, f):
        return _poly_implied_bbox(f)

    def predicate(self, ctx, q, f, alias):
        return _poly_predicate(ctx, q, f, alias)


hooks.register_filter(AroundFilter, _AroundHook())
hooks.register_filter(PolyFilter, _PolyHook())
