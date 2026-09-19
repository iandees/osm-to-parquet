"""SQL builders that read the on-disk (or on-S3) Parquet layout into the
canonical set schema (schema.py). One function per access path from
contract section 8: spatial bbox scan, byid id lookup, and set-sourced
(input-set) queries.
"""
from __future__ import annotations

from typing import Optional

from osmpq.ql.ast import TagFilter

from . import catalog, idset, tagsql
from .schema import empty_set_sql, project

BBox = tuple[float, float, float, float]


def to_e7(deg: float) -> int:
    return int(round(deg * 1e7))


def _quote_list(paths: list[str]) -> str:
    return "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def _node_files(manifest: catalog.Manifest, cells: list[str], partition: str) -> list[str]:
    tc = manifest.table_cells("node")
    out = []
    for c in cells:
        entry = tc.get(c, {})
        part = entry.get(partition)
        if part:
            out.append(manifest.path(part["path"]))
    return out


def _way_files(manifest: catalog.Manifest, cells: list[str]) -> list[str]:
    tc = manifest.table_cells("way")
    return [manifest.path(tc[c]["path"]) for c in cells if c in tc]


def _relation_files(manifest: catalog.Manifest, cells: list[str]) -> list[str]:
    tc = manifest.table_cells("relation")
    return [manifest.path(tc[c]["path"]) for c in cells if c in tc]


# --------------------------------------------------------------------------
# Spatial (bbox) scans
# --------------------------------------------------------------------------


def build_node_spatial_select(
    con,
    manifest: catalog.Manifest,
    bbox: Optional[BBox],
    tag_filters: list[TagFilter],
    ids: Optional[list[int]],
    promoted_keys: set[str],
) -> tuple[str, int]:
    cells = catalog.cells_for_bbox(manifest, "node", bbox)
    need_untagged = tagsql.is_negative_only(tag_filters)
    files = _node_files(manifest, cells, "tagged")
    if need_untagged:
        files = files + _node_files(manifest, cells, "untagged")
    if bbox is not None:
        se, we, ne, ee = to_e7(bbox[0]), to_e7(bbox[1]), to_e7(bbox[2]), to_e7(bbox[3])
        # Row-group pruning (m1-contracts.md section 6): node row groups
        # carry the min/max of lon_e7/lat_e7, so the bbox tuple order here
        # is (xmin=lon_min, ymin=lat_min, xmax=lon_max, ymax=lat_max).
        files = catalog.prune_files_by_bbox(manifest, "node", files, (we, se, ee, ne))
    if not files:
        return empty_set_sql(), 0

    where = []
    if bbox is not None:
        s, w, n, e = bbox
        where.append(
            f"lat_e7 BETWEEN {to_e7(s)} AND {to_e7(n)} AND lon_e7 BETWEEN {to_e7(w)} AND {to_e7(e)}"
        )
    if ids:
        where.append(idset.id_predicate(con, "id", ids))
    where.append(tagsql.tag_filters_sql(tag_filters, promoted_keys))
    where_sql = " AND ".join(f"({w})" for w in where)

    cols = {
        "type": "'node'",
        "id": "id",
        "cell": "cell",
        "lat_e7": "lat_e7",
        "lon_e7": "lon_e7",
        "tags": "tags",
        "version": "version",
        "changeset": "changeset",
        "timestamp": "timestamp",
        "uid": "uid",
        "user": '"user"',
        "hilbert": "hilbert",
    }
    sql = (
        f"SELECT {project(cols)}\n"
        f"FROM read_parquet({_quote_list(files)}, hive_partitioning=true, union_by_name=true)\n"
        f"WHERE {where_sql}"
    )
    return sql, len(files)


def build_way_spatial_select(
    con,
    manifest: catalog.Manifest,
    bbox: Optional[BBox],
    tag_filters: list[TagFilter],
    ids: Optional[list[int]],
    promoted_keys: set[str],
) -> tuple[str, int]:
    cells = catalog.cells_for_bbox(manifest, "way", bbox)
    files = _way_files(manifest, cells)
    if bbox is not None:
        se, we, ne, ee = to_e7(bbox[0]), to_e7(bbox[1]), to_e7(bbox[2]), to_e7(bbox[3])
        files = catalog.prune_files_by_bbox(manifest, "way", files, (we, se, ee, ne))
    if not files:
        return empty_set_sql(), 0

    where = []
    if bbox is not None:
        s, w, n, e = bbox
        se, we, ne, ee = to_e7(s), to_e7(w), to_e7(n), to_e7(e)
        where.append(
            f"xmax_e7 >= {we} AND xmin_e7 <= {ee} AND ymax_e7 >= {se} AND ymin_e7 <= {ne}"
        )
        # The flat-bbox test above is only a prune (a way's AABB can
        # intersect the query bbox while its actual line never enters it,
        # e.g. a diagonal way whose corners straddle the box). Overpass
        # selects a way by its real geometry, so re-check exactly in the
        # same scan; a way with fewer than 2 resolvable nodes has NULL
        # geometry and ST_Intersects(NULL, ...) is NULL (excluded), which
        # matches Overpass having nothing to test against either.
        where.append(
            f"ST_Intersects(geometry, ST_MakeEnvelope({w}, {s}, {e}, {n}))"
        )
    if ids:
        where.append(idset.id_predicate(con, "id", ids))
    where.append(tagsql.tag_filters_sql(tag_filters, promoted_keys))
    where_sql = " AND ".join(f"({w})" for w in where)

    cols = {
        "type": "'way'",
        "id": "id",
        "cell": "cell",
        "refs": "refs",
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
        "geometry": "geometry",
        "hilbert": "hilbert",
    }
    sql = (
        f"SELECT {project(cols)}\n"
        f"FROM read_parquet({_quote_list(files)}, hive_partitioning=true, union_by_name=true)\n"
        f"WHERE {where_sql}"
    )
    return sql, len(files)


def _relation_bbox_exact_filter(con, manifest: catalog.Manifest, cand_sql: str, bbox: BBox) -> tuple[str, int]:
    """Overpass selects a relation for a bbox if at least one member *node*
    lies in the bbox, or at least one member *way* intersects it exactly
    (members of member relations don't count for this plain-bbox test;
    contract section 8 / m0 task item 3). Relation rows carry no geometry
    in M0, so resolve member nodes/ways the same way `render.py`'s `out
    geom` does and keep only relations with a passing member. `cand_sql`
    must already be the coarse-AABB-pruned candidate rows (id, members,
    ...) -- this only trims false positives, it never adds rows back.
    Returns (new SELECT sql, extra files read)."""
    cand = idset.fresh_table_name("relcand")
    con.execute(f"CREATE TEMP TABLE {cand} AS {cand_sql}")
    files_read = 0

    s, w, n, e = bbox
    se, we, ne, ee = to_e7(s), to_e7(w), to_e7(n), to_e7(e)

    node_ids_tbl = idset.fresh_table_name("relcandnodes")
    way_ids_tbl = idset.fresh_table_name("relcandways")
    con.execute(
        f"CREATE TEMP TABLE {node_ids_tbl} AS "
        f"SELECT DISTINCT m.ref AS id FROM {cand}, UNNEST(members) AS t(m) WHERE m.type = 'n'"
    )
    con.execute(
        f"CREATE TEMP TABLE {way_ids_tbl} AS "
        f"SELECT DISTINCT m.ref AS id FROM {cand}, UNNEST(members) AS t(m) WHERE m.type = 'w'"
    )

    node_hits = idset.fresh_table_name("relnodehits")
    lo, hi, n_nodes = con.execute(f"SELECT min(id), max(id), count(*) FROM {node_ids_tbl}").fetchone()
    node_files = [manifest.path(p["path"]) for p in catalog.byid_parts_for_range(manifest, "node", lo, hi)] if n_nodes else []
    if node_files:
        files_read += len(node_files)
        con.execute(
            f"CREATE TEMP TABLE {node_hits} AS "
            f"SELECT nb.id FROM read_parquet({_quote_list(node_files)}) nb "
            f"JOIN {node_ids_tbl} c ON nb.id = c.id "
            f"WHERE nb.lat_e7 BETWEEN {se} AND {ne} AND nb.lon_e7 BETWEEN {we} AND {ee}"
        )
    else:
        con.execute(f"CREATE TEMP TABLE {node_hits} (id BIGINT)")

    way_hits = idset.fresh_table_name("relwayhits")
    lo_w, hi_w, n_ways = con.execute(f"SELECT min(id), max(id), count(*) FROM {way_ids_tbl}").fetchone()
    way_byid_files = [manifest.path(p["path"]) for p in catalog.byid_parts_for_range(manifest, "way", lo_w, hi_w)] if n_ways else []
    if way_byid_files:
        files_read += len(way_byid_files)
        way_cells_tbl = idset.fresh_table_name("relwaycells")
        con.execute(
            f"CREATE TEMP TABLE {way_cells_tbl} AS "
            f"SELECT wb.id, wb.cell FROM read_parquet({_quote_list(way_byid_files)}) wb "
            f"JOIN {way_ids_tbl} c ON wb.id = c.id WHERE wb.cell IS NOT NULL"
        )
        way_tc = manifest.table_cells("way")
        needed_cells = [r[0] for r in con.execute(f"SELECT DISTINCT cell FROM {way_cells_tbl}").fetchall()]
        spatial_files = _way_files(manifest, needed_cells)
        spatial_files = catalog.prune_files_by_bbox(manifest, "way", spatial_files, (we, se, ee, ne))
        if spatial_files:
            files_read += len(spatial_files)
            con.execute(
                f"CREATE TEMP TABLE {way_hits} AS "
                f"SELECT ws.id FROM read_parquet({_quote_list(spatial_files)}, hive_partitioning=true, union_by_name=true) ws "
                f"JOIN {way_cells_tbl} wc ON ws.cell = wc.cell AND ws.id = wc.id "
                f"WHERE ST_Intersects(ws.geometry, ST_MakeEnvelope({w}, {s}, {e}, {n}))"
            )
        else:
            con.execute(f"CREATE TEMP TABLE {way_hits} (id BIGINT)")
    else:
        con.execute(f"CREATE TEMP TABLE {way_hits} (id BIGINT)")

    passing = idset.fresh_table_name("relpass")
    con.execute(
        f"CREATE TEMP TABLE {passing} AS "
        f"SELECT DISTINCT c.id FROM {cand} c, UNNEST(c.members) AS t(m) "
        f"WHERE (m.type = 'n' AND m.ref IN (SELECT id FROM {node_hits})) "
        f"   OR (m.type = 'w' AND m.ref IN (SELECT id FROM {way_hits}))"
    )
    return f"SELECT * FROM {cand} WHERE id IN (SELECT id FROM {passing})", files_read


def build_relation_spatial_select(
    con,
    manifest: catalog.Manifest,
    bbox: Optional[BBox],
    tag_filters: list[TagFilter],
    ids: Optional[list[int]],
    promoted_keys: set[str],
) -> tuple[str, int]:
    cells = catalog.cells_for_bbox(manifest, "relation", bbox)
    files = _relation_files(manifest, cells)
    if bbox is not None:
        se, we, ne, ee = to_e7(bbox[0]), to_e7(bbox[1]), to_e7(bbox[2]), to_e7(bbox[3])
        files = catalog.prune_files_by_bbox(manifest, "relation", files, (we, se, ee, ne))
    if not files:
        return empty_set_sql(), 0

    where = []
    if bbox is not None:
        s, w, n, e = bbox
        se, we, ne, ee = to_e7(s), to_e7(w), to_e7(n), to_e7(e)
        where.append(
            f"xmax_e7 >= {we} AND xmin_e7 <= {ee} AND ymax_e7 >= {se} AND ymin_e7 <= {ne}"
        )
    if ids:
        where.append(idset.id_predicate(con, "id", ids))
    where.append(tagsql.tag_filters_sql(tag_filters, promoted_keys))
    where_sql = " AND ".join(f"({w})" for w in where)

    cols = {
        "type": "'relation'",
        "id": "id",
        "cell": "cell",
        "members": "members",
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
        # Relation geometry is always NULL in M0 (contract section 4); the
        # builder's on-disk column may even be a BLOB of NULLs rather than
        # GEOMETRY, so we never read it, just project a typed NULL.
        "geometry": "NULL::GEOMETRY",
        "hilbert": "hilbert",
    }
    sql = (
        f"SELECT {project(cols)}\n"
        f"FROM read_parquet({_quote_list(files)}, hive_partitioning=true, union_by_name=true)\n"
        f"WHERE {where_sql}"
    )
    nfiles = len(files)
    if bbox is not None:
        # The coarse test above is the union-of-members AABB (contract
        # section 4); it can pass while no individual member actually
        # falls in the bbox (e.g. an L-shaped union of two far-apart member
        # ways). Re-check exactly, only resolving the members of whatever
        # survived the coarse prune.
        sql, extra_files = _relation_bbox_exact_filter(con, manifest, sql, bbox)
        nfiles += extra_files
    return sql, nfiles


SPATIAL_BUILDERS = {
    "node": build_node_spatial_select,
    "way": build_way_spatial_select,
    "relation": build_relation_spatial_select,
}


# --------------------------------------------------------------------------
# byid (id lookup, no bbox)
# --------------------------------------------------------------------------


def _byid_cols(element_type: str) -> dict[str, str]:
    if element_type == "node":
        return {
            "type": "'node'",
            "id": "id",
            "cell": "cell",
            "lat_e7": "lat_e7",
            "lon_e7": "lon_e7",
            "tags": "tags",
            "version": "version",
            "changeset": "changeset",
            "timestamp": "timestamp",
            "uid": "uid",
            "user": '"user"',
            "hilbert": "opq_node_hilbert(lon_e7, lat_e7)",
        }
    elif element_type == "way":
        return {
            "type": "'way'",
            "id": "id",
            "cell": "cell",
            "refs": "refs",
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
            "hilbert": "opq_bbox_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7)",
        }
    else:
        return {
            "type": "'relation'",
            "id": "id",
            "cell": "cell",
            "members": "members",
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
            "hilbert": "opq_bbox_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7)",
        }


def build_byid_select(
    con,
    manifest: catalog.Manifest,
    element_type: str,
    ids: list[int],
    tag_filters: list[TagFilter],
    promoted_keys: set[str],
) -> tuple[str, int]:
    ids = list(ids)
    if not ids:
        return empty_set_sql(), 0
    lo, hi = idset.id_range(ids)
    parts = catalog.byid_parts_for_range(manifest, element_type, lo, hi)
    files = [manifest.path(p["path"]) for p in parts]
    if not files:
        return empty_set_sql(), 0

    where = [idset.id_predicate(con, "id", ids), tagsql.tag_filters_sql(tag_filters, promoted_keys)]
    where_sql = " AND ".join(f"({w})" for w in where)
    cols = _byid_cols(element_type)
    sql = (
        f"SELECT {project(cols)}\n"
        f"FROM read_parquet({_quote_list(files)}, union_by_name=true)\n"
        f"WHERE {where_sql}"
    )
    return sql, len(files)


def _union_bbox_e7(con, selects: list[str]) -> Optional[BBox]:
    """Union bbox (south, west, north, east) in degrees over the
    xmin_e7/ymin_e7/xmax_e7/ymax_e7 columns of one or more SQL fragments
    (each ``SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM ...``), or None
    if there is nothing to union (no fragments, or every row's bbox is
    NULL, e.g. a way with fewer than 2 resolvable nodes)."""
    selects = [s for s in selects if s]
    if not selects:
        return None
    union_sql = "\nUNION ALL\n".join(selects)
    row = con.execute(
        f"SELECT min(xmin_e7), min(ymin_e7), max(xmax_e7), max(ymax_e7) FROM ({union_sql}) __bbox"
    ).fetchone()
    xmin, ymin, xmax, ymax = row
    if None in (xmin, ymin, xmax, ymax):
        return None
    return (ymin / 1e7, xmin / 1e7, ymax / 1e7, xmax / 1e7)


def bbox_from_points_e7(con, points_sql: str) -> Optional[BBox]:
    """Union bbox (south, west, north, east) in degrees over the
    lat_e7/lon_e7 columns of a SQL fragment (``SELECT lat_e7, lon_e7 FROM
    ...``), or None if there are no rows / every coordinate is NULL. Used
    to bound a set of *source nodes themselves* (design.md 3.1: a way
    containing a node has a bbox containing that node), as opposed to
    `_union_bbox_e7` which bounds a set of already-bboxed rows."""
    row = con.execute(
        f"SELECT min(lat_e7), max(lat_e7), min(lon_e7), max(lon_e7) FROM ({points_sql}) __pts"
    ).fetchone()
    lat_min, lat_max, lon_min, lon_max = row
    if None in (lat_min, lat_max, lon_min, lon_max):
        return None
    return (lat_min / 1e7, lon_min / 1e7, lat_max / 1e7, lon_max / 1e7)


def build_way_bbox_semijoin_select(
    con,
    manifest: catalog.Manifest,
    node_ids_table: str,
    bbox: BBox,
    tag_filters: list[TagFilter],
    promoted_keys: set[str],
    max_cell_fraction: float = 0.5,
) -> tuple[Optional[str], int]:
    """design.md 3.1: "a way containing a node has a bbox containing that
    node, so it is stored in the node's leaf cell or one of its
    ancestors" (contract section 2). Reads the way spatial files of
    `cells_for_bbox(manifest, "way", bbox)` with the flat-bbox prune,
    keeping rows whose `refs` contains any id in `node_ids_table` (a TEMP
    TABLE with an ``id`` column) via a semi-join over `UNNEST(refs)`.

    Exhaustive by construction as long as `bbox` truly bounds every id in
    `node_ids_table` (e.g. it is these nodes' own union bbox): no
    remainder/fallback merge is needed for *these* rows, unlike node
    hydration's bbox hint (which comes from a bounding *parent* element's
    bbox column, not the ids' own coordinates). The caller is still
    expected to fall back entirely to the node_way index when this
    returns (None, 0) because the guard below tripped.

    Returns (None, 0) when `cells_for_bbox` would touch more than
    `max_cell_fraction` of all leaves -- the planet-scale guard, the same
    one `build_node_hydrate_via_bbox_select` uses."""
    cells = catalog.cells_for_bbox(manifest, "way", bbox)
    total_leaves = len(manifest.leaf_cells) or 1
    if not cells or len(cells) > max_cell_fraction * total_leaves:
        return None, 0
    files = _way_files(manifest, cells)
    s, w, n, e = bbox
    se, we, ne, ee = to_e7(s), to_e7(w), to_e7(n), to_e7(e)
    files = catalog.prune_files_by_bbox(manifest, "way", files, (we, se, ee, ne))
    if not files:
        return empty_set_sql(), 0

    bbox_where = f"xmax_e7 >= {we} AND xmin_e7 <= {ee} AND ymax_e7 >= {se} AND ymin_e7 <= {ne}"
    tag_where = tagsql.tag_filters_sql(tag_filters, promoted_keys)

    cols = {
        "type": "'way'",
        "id": "id",
        "cell": "cell",
        "refs": "refs",
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
        "geometry": "geometry",
        "hilbert": "hilbert",
    }
    sql = (
        f"WITH __waycand AS (\n"
        f"  SELECT * FROM read_parquet({_quote_list(files)}, hive_partitioning=true, union_by_name=true)\n"
        f"  WHERE ({bbox_where}) AND ({tag_where})\n"
        f"),\n"
        f"__waymatch AS (\n"
        f"  SELECT DISTINCT c.id FROM __waycand c, UNNEST(c.refs) AS t(ref)\n"
        f"  JOIN {node_ids_table} n ON n.id = t.ref\n"
        f")\n"
        f"SELECT {project(cols)} FROM __waycand WHERE id IN (SELECT id FROM __waymatch)"
    )
    return sql, len(files)


def build_way_hydrate_via_bbox_select(
    con,
    manifest: catalog.Manifest,
    way_ids_table: str,
    bbox_source_selects: list[str],
    promoted_keys: set[str],
    tag_filters: Optional[list[TagFilter]] = None,
    max_cell_fraction: float = 0.5,
) -> tuple[Optional[str], int]:
    """design.md 3.1 item 3: resolve way ids in `way_ids_table` (a TEMP
    TABLE with at least an ``id`` column -- typically a relation's member
    way ids) from the spatial way files of the leaf cells intersecting the
    union bbox of `bbox_source_selects` (SQL fragments over rows carrying
    xmin_e7/ymin_e7/xmax_e7/ymax_e7 known to bound these ways -- a
    relation's member ways lie inside the relation's own bbox, contract
    section 4) instead of scanning the id-sorted byid way parts end to
    end. The spatial rows already carry `geometry`, so a caller resolving
    member-way geometry for `out geom` gets it in this one pass with no
    second hydration.

    Falls back to byid for whatever the spatial pass does not find
    (defensive; should be empty for consistent data), and skips the
    spatial pass entirely -- straight to byid -- when
    `bbox_source_selects` is empty, every candidate bbox is NULL, or the
    union bbox would make `cells_for_bbox` touch more than
    `max_cell_fraction` of all leaves (same guard as
    `build_node_hydrate_via_bbox_select`)."""
    total_ids = con.execute(f"SELECT count(*) FROM {way_ids_table}").fetchone()[0]
    if not total_ids:
        return None, 0

    files_total = 0
    selects: list[str] = []
    found_table: Optional[str] = None

    bbox = _union_bbox_e7(con, bbox_source_selects)
    if bbox is not None:
        cells = catalog.cells_for_bbox(manifest, "way", bbox)
        total_leaves = len(manifest.leaf_cells) or 1
        if cells and len(cells) <= max_cell_fraction * total_leaves:
            way_files = _way_files(manifest, cells)
            s, w, n, e = bbox
            se, we, ne, ee = to_e7(s), to_e7(w), to_e7(n), to_e7(e)
            way_files = catalog.prune_files_by_bbox(manifest, "way", way_files, (we, se, ee, ne))
            if way_files:
                bbox_where = (
                    f"w.xmax_e7 >= {we} AND w.xmin_e7 <= {ee} "
                    f"AND w.ymax_e7 >= {se} AND w.ymin_e7 <= {ne}"
                )
                files_total += len(way_files)
                found_table = idset.fresh_table_name("bboxwayhits")
                cols = {
                    "type": "'way'",
                    "id": "w.id",
                    "cell": "w.cell",
                    "refs": "w.refs",
                    "tags": "w.tags",
                    "version": "w.version",
                    "changeset": "w.changeset",
                    "timestamp": 'w."timestamp"',
                    "uid": "w.uid",
                    "user": 'w."user"',
                    "xmin_e7": "w.xmin_e7",
                    "ymin_e7": "w.ymin_e7",
                    "xmax_e7": "w.xmax_e7",
                    "ymax_e7": "w.ymax_e7",
                    "geometry": "w.geometry",
                    "hilbert": "w.hilbert",
                }
                tag_where = tagsql.tag_filters_sql(tag_filters or [], promoted_keys, prefix="w.")
                con.execute(
                    f"CREATE TEMP TABLE {found_table} AS\n"
                    f"SELECT {project(cols)}\n"
                    f"FROM read_parquet({_quote_list(way_files)}, hive_partitioning=true, union_by_name=true) w\n"
                    f"JOIN (SELECT DISTINCT id FROM {way_ids_table}) ids ON w.id = ids.id\n"
                    f"WHERE ({bbox_where}) AND ({tag_where})"
                )
                selects.append(f"SELECT * FROM {found_table}")

    if found_table is not None:
        remainder_sql = (
            f"SELECT DISTINCT id FROM {way_ids_table} "
            f"WHERE id NOT IN (SELECT id FROM {found_table})"
        )
    else:
        remainder_sql = f"SELECT DISTINCT id FROM {way_ids_table}"
    remainder_table = idset.fresh_table_name("bboxwayrem")
    con.execute(f"CREATE TEMP TABLE {remainder_table} AS {remainder_sql}")
    n_remaining = con.execute(f"SELECT count(*) FROM {remainder_table}").fetchone()[0]
    if n_remaining:
        lo, hi = con.execute(f"SELECT min(id), max(id) FROM {remainder_table}").fetchone()
        byid_sql, nfiles = build_byid_select_from_ids_query(
            manifest, "way", f"SELECT id FROM {remainder_table}", lo, hi, tag_filters or [], promoted_keys
        )
        files_total += nfiles
        if byid_sql:
            selects.append(byid_sql)

    if not selects:
        return None, files_total
    return "\nUNION ALL\n".join(selects), files_total


def _spatial_files_for_type(manifest: catalog.Manifest, element_type: str, cells: list[str]) -> list[str]:
    if element_type == "way":
        return _way_files(manifest, cells)
    if element_type == "relation":
        return _relation_files(manifest, cells)
    raise ValueError(f"unsupported element_type {element_type!r} for cell hydration")


def _spatial_cols_for_type(element_type: str, prefix: str) -> dict[str, str]:
    common = {
        "type": f"'{element_type}'",
        "id": f"{prefix}id",
        "cell": f"{prefix}cell",
        "tags": f"{prefix}tags",
        "version": f"{prefix}version",
        "changeset": f"{prefix}changeset",
        "timestamp": f'{prefix}"timestamp"',
        "uid": f"{prefix}uid",
        "user": f'{prefix}"user"',
        "xmin_e7": f"{prefix}xmin_e7",
        "ymin_e7": f"{prefix}ymin_e7",
        "xmax_e7": f"{prefix}xmax_e7",
        "ymax_e7": f"{prefix}ymax_e7",
        "hilbert": f"{prefix}hilbert",
    }
    if element_type == "way":
        common["refs"] = f"{prefix}refs"
        common["geometry"] = f"{prefix}geometry"
    else:
        # Relation geometry is always NULL in M0 (contract section 4).
        common["members"] = f"{prefix}members"
        common["geometry"] = "NULL::GEOMETRY"
    return common


def build_spatial_hydrate_via_cell_select(
    con,
    manifest: catalog.Manifest,
    element_type: str,
    id_cell_table: str,
    tag_filters: list[TagFilter],
    promoted_keys: set[str],
) -> tuple[Optional[str], int]:
    """design.md 3.1 item 2: hydrate `element_type` ('way' or 'relation')
    ids in `id_cell_table` (columns ``id``, ``cell`` -- `cell` may be NULL
    for some/all rows) by joining the spatial files of exactly the cells
    that appear on (cell, id), instead of scanning the id-sorted byid
    copy end to end. `cell` for relations is the member index's
    `parent_cell` column (contract section 4); for ways it is the way's
    own `cell` as found by `build_way_bbox_semijoin_select`.

    Falls back to byid for every id whose `cell` is NULL or that the
    spatial join does not find (defensive: should be empty for
    consistent data)."""
    total = con.execute(f"SELECT count(*) FROM {id_cell_table}").fetchone()[0]
    if not total:
        return None, 0

    files_total = 0
    selects: list[str] = []
    found_table: Optional[str] = None

    known_cells = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT cell FROM {id_cell_table} WHERE cell IS NOT NULL"
        ).fetchall()
    ]
    tc = manifest.table_cells(element_type)
    needed_cells = [c for c in known_cells if c in tc]
    if needed_cells:
        files = _spatial_files_for_type(manifest, element_type, needed_cells)
        if files:
            files_total += len(files)
            found_table = idset.fresh_table_name(f"{element_type}cellhits")
            tag_where = tagsql.tag_filters_sql(tag_filters, promoted_keys, prefix="t.")
            cols = _spatial_cols_for_type(element_type, prefix="t.")
            con.execute(
                f"CREATE TEMP TABLE {found_table} AS\n"
                f"SELECT {project(cols)}\n"
                f"FROM read_parquet({_quote_list(files)}, hive_partitioning=true, union_by_name=true) t\n"
                f"JOIN (SELECT DISTINCT id, cell FROM {id_cell_table} WHERE cell IS NOT NULL) ids\n"
                f"  ON t.cell = ids.cell AND t.id = ids.id\n"
                f"WHERE {tag_where}"
            )
            selects.append(f"SELECT * FROM {found_table}")

    if found_table is not None:
        remainder_sql = (
            f"SELECT DISTINCT id FROM {id_cell_table} "
            f"WHERE cell IS NULL OR id NOT IN (SELECT id FROM {found_table})"
        )
    else:
        remainder_sql = f"SELECT DISTINCT id FROM {id_cell_table}"
    remainder_table = idset.fresh_table_name(f"{element_type}cellrem")
    con.execute(f"CREATE TEMP TABLE {remainder_table} AS {remainder_sql}")
    n_remaining = con.execute(f"SELECT count(*) FROM {remainder_table}").fetchone()[0]
    if n_remaining:
        lo, hi = con.execute(f"SELECT min(id), max(id) FROM {remainder_table}").fetchone()
        byid_sql, nfiles = build_byid_select_from_ids_query(
            manifest, element_type, f"SELECT id FROM {remainder_table}", lo, hi, tag_filters, promoted_keys
        )
        files_total += nfiles
        if byid_sql:
            selects.append(byid_sql)

    if not selects:
        return None, files_total
    return "\nUNION ALL\n".join(selects), files_total


def build_node_hydrate_via_bbox_select(
    con,
    manifest: catalog.Manifest,
    node_ids_table: str,
    bbox_source_selects: list[str],
    promoted_keys: set[str],
    tag_filters: Optional[list[TagFilter]] = None,
    max_cell_fraction: float = 0.5,
) -> tuple[Optional[str], int]:
    """design.md 3.1 "spatially scoped id lookups": resolve node ids in
    `node_ids_table` (a TEMP TABLE with at least an ``id`` column) from the
    spatial node files of the leaf cells intersecting the union bbox of
    `bbox_source_selects` -- SQL fragments over rows that carry
    xmin_e7/ymin_e7/xmax_e7/ymax_e7 and are known to bound these node ids
    (a way's nodes lie inside the way's own bbox; a relation's member
    nodes lie inside the relation's own bbox, both already columns on the
    row) -- instead of scanning the id-sorted byid copy end to end, which a
    handful of scattered ids forces to read almost in full.

    Falls back to byid for whatever the spatial pass does not find, so
    correctness never depends on the bbox hint being exact or even present
    (`bbox_source_selects` empty, or every candidate bbox NULL, just skips
    straight to byid, unchanged from before this existed).

    `max_cell_fraction` is the planet-scale caveat: a way stored in an
    ancestor cell can have a bbox spanning many leaves (a continent-wide
    coastline, or -- worst case -- the root cell). Reading that many leaf
    cell files (two each, tagged/untagged) stops being cheaper than the
    byid scan it exists to avoid, so past this fraction of all leaf cells
    the spatial attempt is skipped entirely."""
    total_ids = con.execute(f"SELECT count(*) FROM {node_ids_table}").fetchone()[0]
    if not total_ids:
        return None, 0

    files_total = 0
    selects: list[str] = []
    found_table: Optional[str] = None

    bbox = _union_bbox_e7(con, bbox_source_selects)
    if bbox is not None:
        cells = catalog.cells_for_bbox(manifest, "node", bbox)
        total_leaves = len(manifest.leaf_cells) or 1
        if cells and len(cells) <= max_cell_fraction * total_leaves:
            node_files = _node_files(manifest, cells, "tagged") + _node_files(manifest, cells, "untagged")
            s, w, n, e = bbox
            se, we, ne, ee = to_e7(s), to_e7(w), to_e7(n), to_e7(e)
            node_files = catalog.prune_files_by_bbox(manifest, "node", node_files, (we, se, ee, ne))
            if node_files:
                files_total += len(node_files)
                found_table = idset.fresh_table_name("bboxnodehits")
                cols = {
                    "type": "'node'",
                    "id": "n.id",
                    "cell": "n.cell",
                    "lat_e7": "n.lat_e7",
                    "lon_e7": "n.lon_e7",
                    "tags": "n.tags",
                    "version": "n.version",
                    "changeset": "n.changeset",
                    "timestamp": 'n."timestamp"',
                    "uid": "n.uid",
                    "user": 'n."user"',
                    "hilbert": "n.hilbert",
                }
                tag_where = tagsql.tag_filters_sql(tag_filters or [], promoted_keys, prefix="n.")
                con.execute(
                    f"CREATE TEMP TABLE {found_table} AS\n"
                    f"SELECT {project(cols)}\n"
                    f"FROM read_parquet({_quote_list(node_files)}, hive_partitioning=true, union_by_name=true) n\n"
                    f"JOIN (SELECT DISTINCT id FROM {node_ids_table}) ids ON n.id = ids.id\n"
                    f"WHERE {tag_where}"
                )
                selects.append(f"SELECT * FROM {found_table}")

    if found_table is not None:
        remainder_sql = (
            f"SELECT DISTINCT id FROM {node_ids_table} "
            f"WHERE id NOT IN (SELECT id FROM {found_table})"
        )
    else:
        remainder_sql = f"SELECT DISTINCT id FROM {node_ids_table}"
    remainder_table = idset.fresh_table_name("bboxnoderem")
    con.execute(f"CREATE TEMP TABLE {remainder_table} AS {remainder_sql}")
    n_remaining = con.execute(f"SELECT count(*) FROM {remainder_table}").fetchone()[0]
    if n_remaining:
        lo, hi = con.execute(f"SELECT min(id), max(id) FROM {remainder_table}").fetchone()
        byid_sql, nfiles = build_byid_select_from_ids_query(
            manifest, "node", f"SELECT id FROM {remainder_table}", lo, hi, tag_filters or [], promoted_keys
        )
        files_total += nfiles
        if byid_sql:
            selects.append(byid_sql)

    if not selects:
        return None, files_total
    return "\nUNION ALL\n".join(selects), files_total


def build_byid_select_from_ids_query(
    manifest: catalog.Manifest,
    element_type: str,
    id_subquery_sql: str,
    lo: Optional[int],
    hi: Optional[int],
    tag_filters: list[TagFilter],
    promoted_keys: set[str],
) -> tuple[str, int]:
    """Like `build_byid_select`, but the ids are already a SQL relation
    (`id_subquery_sql`, a `SELECT id FROM ...` producing the wanted ids for
    `element_type`) instead of a Python list -- used by recurse.py so a `>`,
    `<`, `>>`, `<<` or inline recurse filter never pulls ids into Python at
    all. `lo`/`hi` (from a SQL aggregate on that same relation, not a Python
    id scan) pick which byid parts can contain them."""
    parts = catalog.byid_parts_for_range(manifest, element_type, lo, hi)
    files = [manifest.path(p["path"]) for p in parts]
    if not files:
        return empty_set_sql(), 0

    where = [f"id IN ({id_subquery_sql})", tagsql.tag_filters_sql(tag_filters, promoted_keys)]
    where_sql = " AND ".join(f"({w})" for w in where)
    cols = _byid_cols(element_type)
    sql = (
        f"SELECT {project(cols)}\n"
        f"FROM read_parquet({_quote_list(files)}, union_by_name=true)\n"
        f"WHERE {where_sql}"
    )
    return sql, len(files)


# --------------------------------------------------------------------------
# Set-sourced query (input sets: node.a[amenity=cafe])
# --------------------------------------------------------------------------


def build_from_set_select(
    con,
    set_names: list[str],
    types: list[str],
    tag_filters: list[TagFilter],
    ids: Optional[list[int]],
    bbox: Optional[BBox],
) -> str:
    base = f"set_{set_names[0]}"
    joins = "".join(f" INNER JOIN set_{s} USING (type, id)" for s in set_names[1:])

    type_list = ",".join(f"'{t}'" for t in types)
    where = [f"{base}.type IN ({type_list})"]
    if ids:
        where.append(idset.id_predicate(con, f"{base}.id", ids))
    if bbox is not None:
        s, w, n, e = bbox
        se, we, ne, ee = to_e7(s), to_e7(w), to_e7(n), to_e7(e)
        where.append(
            f"(({base}.type='node' AND {base}.lat_e7 BETWEEN {se} AND {ne} "
            f"AND {base}.lon_e7 BETWEEN {we} AND {ee}) "
            f"OR ({base}.type IN ('way','relation') AND {base}.xmax_e7 >= {we} "
            f"AND {base}.xmin_e7 <= {ee} AND {base}.ymax_e7 >= {se} AND {base}.ymin_e7 <= {ne}))"
        )
    where.append(tagsql.tag_filters_sql(tag_filters, set(), prefix=f"{base}."))
    where_sql = " AND ".join(f"({w})" for w in where)

    cols = {name: f"{base}.{name}" if name != "user" else f'{base}."user"'
            for name in ["type", "id", "cell", "lat_e7", "lon_e7", "refs", "members", "tags",
                         "version", "changeset", "timestamp", "uid", "user",
                         "xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7", "geometry", "hilbert"]}
    sql = f"SELECT {project(cols)}\nFROM {base}{joins}\nWHERE {where_sql}"
    return sql
