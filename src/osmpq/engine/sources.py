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
