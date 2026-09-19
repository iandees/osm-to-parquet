"""`>`, `>>`, `<`, `<<` and the inline recurse filters (w)/(r)/(bn)/(bw)/(br).

Contract section 8 / design.md 3.3: forward recursion follows a way's
``refs`` and a relation's ``members``; backward recursion used to lean on
the ``node_way`` and ``member`` reverse indexes for everything and then
fetch full rows from the byid copy. design.md 3.1 tightens two of those
paths to be spatially scoped instead, because scattered ids defeat
row-group pruning on the byid copy (measured: a `<`/`<<` from a handful of
downtown nodes reads almost the entire node_way index (16 parts, 140 MB)
and both way byid parts (240 MB) to resolve maybe a few dozen ways):

- A way containing a node has a bbox containing that node (contract
  section 2), so `<`'s node -> parent-way hop reads the way spatial files
  of the cells covering the union bbox of the source nodes and semi-joins
  on `UNNEST(refs)`, instead of the node_way index + byid
  (`build_way_bbox_semijoin_select`). The node_way index stays as the
  fallback when that bbox would touch too much of the dataset (the
  `max_cell_fraction` guard `backward_new_ids_table` passes through).
- Relation lookups still go through the (small) member index, but the
  relations it finds carry `parent_cell`, so they hydrate by (cell, id)
  against the spatial relation files instead of a byid scan
  (`sources.build_spatial_hydrate_via_cell_select`); `hydrate_ids_table`
  applies the same trick to way ids whenever its input table carries a
  `cell` column (populated by the bbox pass above, or by the member-way
  "extra hop" that also comes with cell already known).

Every hop here is derived with plain SQL against the source set (or the
previous hop's TEMP TABLE) -- ids are never fetched into a Python list and
never inlined as a SQL literal list. A `>` over a way set with ~80k
distinct referenced nodes used to pull those ids into Python and build a
``WHERE id IN (<80k literals>)`` string; on top of a per-row Python UDF
computing `hilbert` (fixed in hilbert.py), that made the query take upwards
of 30s. Deriving the ids as a TEMP TABLE and joining against it keeps the
whole hop inside DuckDB and O(rows), not O(ids) or O(ids^2) across
multiple hops of `>>`/`<<` (see `recurse_transitive`).
"""
from __future__ import annotations

from typing import Optional

from . import catalog, idset, sources
from .schema import empty_set_sql


def _quote_list(paths: list[str]) -> str:
    return "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


# --------------------------------------------------------------------------
# One hop, derived in SQL: produces a fresh TEMP TABLE(type, id, cell) or
# None. `cell`, when known, is a spatial hint (design.md 3.1) for
# `hydrate_ids_table`/`sources.build_spatial_hydrate_via_cell_select`; it is
# NULL wherever a hop has no such hint (forward hops never do).
# --------------------------------------------------------------------------


_MEMBER_TYPE_NAME = {"n": "node", "w": "way", "r": "relation"}


def _forward_hop_sql(source_table: str, want_way: bool, want_rel: bool, role: Optional[str],
                      rel_member_types: Optional[set[str]] = None) -> tuple[Optional[str], list]:
    parts = []
    params: list = []
    if want_way:
        parts.append(
            f"SELECT 'node' AS type, unnest(refs) AS id FROM {source_table} "
            f"WHERE type = 'way' AND refs IS NOT NULL"
        )
    if want_rel:
        wanted_chars = [c for c in ("n", "w", "r") if rel_member_types is None or c in rel_member_types]
        if wanted_chars:
            role_clause = " AND m.role = ?" if role is not None else ""
            if role is not None:
                params.append(role)
            type_in = ",".join(f"'{c}'" for c in wanted_chars)
            case_sql = " ".join(f"WHEN '{c}' THEN '{_MEMBER_TYPE_NAME[c]}'" for c in wanted_chars)
            parts.append(
                f"SELECT CASE m.type {case_sql} END AS type, "
                f"m.ref AS id FROM {source_table}, UNNEST(members) AS t(m) "
                f"WHERE type = 'relation' AND members IS NOT NULL AND m.type IN ({type_in}){role_clause}"
            )
    if not parts:
        return None, []
    return "\nUNION ALL\n".join(parts), params


def forward_new_ids_table(con, source_table: str, restrict_source_types: Optional[set[str]] = None,
                           role: Optional[str] = None, rel_member_types: Optional[set[str]] = None) -> Optional[str]:
    """One hop: way.refs -> nodes; relation.members -> ids of the member
    types in `rel_member_types` (default: all of n/w/r, e.g. for `>>`'s
    per-hop use and the `(r)` recurse filter). Returns a fresh TEMP
    TABLE(type, id, cell) name (deduplicated; `cell` always NULL -- a
    forward hop has no spatial hint of its own), or None if there is
    nothing to hop from."""
    want_way = restrict_source_types is None or "way" in restrict_source_types
    want_rel = restrict_source_types is None or "relation" in restrict_source_types
    hop_sql, params = _forward_hop_sql(source_table, want_way, want_rel, role, rel_member_types)
    if hop_sql is None:
        return None
    name = idset.fresh_table_name("fwd")
    con.execute(
        f"CREATE TEMP TABLE {name} AS "
        f"SELECT DISTINCT type, id, NULL::VARCHAR AS cell FROM ({hop_sql}) __hop",
        params,
    )
    return name


def backward_new_ids_table(con, manifest: catalog.Manifest, source_table: str,
                            restrict_source_types: Optional[set[str]] = None,
                            role: Optional[str] = None,
                            max_cell_fraction: float = 0.5) -> tuple[Optional[str], int]:
    """One hop of `<`: nodes -> parent ways, resolved spatially
    (design.md 3.1: `sources.build_way_bbox_semijoin_select` over the
    cells covering the source nodes' own union bbox), falling back to the
    node_way index (joined straight against `source_table`) only when that
    bbox would touch more than `max_cell_fraction` of all leaves or there
    is no bbox to compute; any element -> parent relations (member index,
    carrying `parent_cell` as `cell`). Returns (fresh TEMP TABLE(type, id,
    cell) name, way-spatial-files-read) or (None, 0)."""
    parts: list[str] = []
    params: list = []
    files_total = 0

    want_node = restrict_source_types is None or "node" in restrict_source_types
    if want_node:
        lo, hi, n = idset.sql_type_range(con, source_table, "node")
        if n:
            node_ids_tbl = idset.fresh_table_name("bwdnodeids")
            con.execute(
                f"CREATE TEMP TABLE {node_ids_tbl} AS "
                f"SELECT DISTINCT id, lat_e7, lon_e7 FROM {source_table} WHERE type = 'node'"
            )
            bbox = sources.bbox_from_points_e7(con, f"SELECT lat_e7, lon_e7 FROM {node_ids_tbl}")
            way_rows_sql = None
            if bbox is not None:
                way_rows_sql, nfiles_w = sources.build_way_bbox_semijoin_select(
                    con, manifest, node_ids_tbl, bbox, [], set(), max_cell_fraction
                )
                if way_rows_sql is not None:
                    files_total += nfiles_w
            if way_rows_sql is not None:
                parts.append(f"SELECT type, id, cell FROM ({way_rows_sql}) __wr")
            else:
                files = [manifest.path(p["path"]) for p in catalog.index_parts_for_range(manifest, "node_way", lo, hi)]
                if files:
                    parts.append(
                        f"SELECT 'way' AS type, idx.way_id AS id, NULL::VARCHAR AS cell "
                        f"FROM read_parquet({_quote_list(files)}) idx "
                        f"JOIN {source_table} s ON idx.node_id = s.id AND s.type = 'node'"
                    )

    member_files = [manifest.path(p["path"]) for p in manifest.index_parts("member")]
    if member_files:
        type_char = {"node": "n", "way": "w", "relation": "r"}
        wanted = {
            t: c for t, c in type_char.items()
            if (restrict_source_types is None or t in restrict_source_types)
            and idset.sql_type_range(con, source_table, t)[2]
        }
        if wanted:
            type_case = " ".join(f"WHEN '{c}' THEN '{t}'" for t, c in wanted.items())
            type_in = ",".join(f"'{c}'" for c in wanted.values())
            role_clause = " AND idx.role = ?" if role is not None else ""
            if role is not None:
                params.append(role)
            parts.append(
                f"SELECT 'relation' AS type, idx.parent_id AS id, idx.parent_cell AS cell "
                f"FROM read_parquet({_quote_list(member_files)}) idx "
                f"JOIN {source_table} s "
                f"  ON idx.member_id = s.id AND s.type = CASE idx.member_type {type_case} END "
                f"WHERE idx.member_type IN ({type_in}){role_clause}"
            )

    if not parts:
        return None, files_total
    name = idset.fresh_table_name("bwd")
    hop_sql = "\nUNION ALL\n".join(parts)
    con.execute(f"CREATE TEMP TABLE {name} AS SELECT DISTINCT type, id, cell FROM ({hop_sql}) __hop", params)
    return name, files_total


# --------------------------------------------------------------------------
# Hydrating a (type, id[, cell]) TEMP TABLE into full rows.
# --------------------------------------------------------------------------


def hydrate_ids_table(con, manifest: catalog.Manifest, id_table: str, tag_filters, promoted_keys: set[str],
                       only_types: Optional[set[str]] = None) -> tuple[Optional[str], int]:
    """Build the UNION ALL select hydrating every (type, id) row of
    `id_table`, restricted to `only_types` if given. For 'way'/'relation'
    ids, when `id_table` carries a `cell` column (design.md 3.1: the
    node-bbox way lookup and the member-index relation lookup both know
    it), hydrate by (cell, id) against the spatial copy instead of byid
    (`sources.build_spatial_hydrate_via_cell_select`, itself falling back
    to byid for anything not found there). Otherwise -- and always for
    'node' -- hydrate via byid, same as before this optimization: per type
    we only fetch a min/max/count aggregate (to pick byid parts) and pass
    the rest as a `SELECT id FROM id_table WHERE type = ...` subquery,
    which DuckDB plans as a hash join against `id_table` regardless of its
    size, so ids never leave SQL either way."""
    has_cell = idset.table_has_column(con, id_table, "cell")
    selects = []
    files_total = 0
    for t in ("node", "way", "relation"):
        if only_types is not None and t not in only_types:
            continue
        lo, hi, n = idset.sql_type_range(con, id_table, t)
        if not n:
            continue
        if t in ("way", "relation") and has_cell:
            id_cell_tbl = idset.fresh_table_name(f"{t}idcell")
            con.execute(
                f"CREATE TEMP TABLE {id_cell_tbl} AS "
                f"SELECT DISTINCT id, cell FROM {id_table} WHERE type = '{t}'"
            )
            sql, nfiles = sources.build_spatial_hydrate_via_cell_select(
                con, manifest, t, id_cell_tbl, tag_filters, promoted_keys
            )
        else:
            id_subquery = f"SELECT id FROM {id_table} WHERE type = '{t}'"
            sql, nfiles = sources.build_byid_select_from_ids_query(manifest, t, id_subquery, lo, hi, tag_filters, promoted_keys)
        files_total += nfiles
        if sql:
            selects.append(sql)
    if not selects:
        return None, files_total
    return "\nUNION ALL\n".join(selects), files_total


def build_forward_one_hop(con, manifest: catalog.Manifest, source_table: str, promoted_keys: set[str],
                           restrict_source_types: Optional[set[str]] = None,
                           role: Optional[str] = None) -> tuple[str, int]:
    """`>`: all nodes of ways in the source, plus all node and way members
    of relations in the source (relation-type members are excluded here --
    that is `>>`'s job), plus all nodes of those member ways (Overpass
    resolves a relation's member way down to its own nodes too, not just
    the way itself; see docs/m0-contracts.md and the `27_down_transitive_*`
    corpus symptom this fixes)."""
    hop_table = forward_new_ids_table(con, source_table, restrict_source_types, role, rel_member_types={"n", "w"})
    if hop_table is None:
        return empty_set_sql(), 0
    total_files = 0
    selects: list[str] = []

    # Hydrate the way-type ids (relation member ways) first -- we need
    # their `refs` both for their own row and to find their nodes below.
    # design.md 3.1 item 3: a relation's member ways lie inside the
    # relation's own bbox, so resolve them from the spatial way files of
    # the cells covering that bbox instead of byid; this also yields their
    # geometry directly (spatial rows carry it, byid rows do not).
    way_ids_table = idset.fresh_table_name("fwdwayids")
    con.execute(f"CREATE TEMP TABLE {way_ids_table} AS SELECT DISTINCT id FROM {hop_table} WHERE type = 'way'")
    n_way_ids = con.execute(f"SELECT count(*) FROM {way_ids_table}").fetchone()[0]
    way_rows_table: Optional[str] = None
    if n_way_ids:
        rel_bbox_selects = [
            f"SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM {source_table} WHERE type = 'relation'"
        ]
        way_rows_sql, nfiles_w = sources.build_way_hydrate_via_bbox_select(
            con, manifest, way_ids_table, rel_bbox_selects, promoted_keys
        )
        total_files += nfiles_w
        if way_rows_sql:
            way_rows_table = idset.fresh_table_name("fwdwayrows")
            con.execute(f"CREATE TEMP TABLE {way_rows_table} AS {way_rows_sql}")
            selects.append(f"SELECT * FROM {way_rows_table}")

    # Nodes directly in hop_table (nodes of ways-in-source, or direct node
    # members of relations-in-source) plus nodes referenced by the
    # relation-member ways above, hydrated together in a *single* pass.
    # design.md 3.1 / contract section 4: a way's nodes lie inside the
    # way's own bbox, and a relation's direct node members lie inside the
    # relation's own bbox (it is defined as the union of exactly those
    # three things: member node points, member way bboxes, member
    # relation bboxes) -- so instead of a byid scan (node ids from a `>`
    # hop are typically scattered across the whole id space, so a byid
    # part covering their min..max range usually covers most of the
    # table; a relation with both a very old and a very new node member
    # can force a scan of nearly every byid part), resolve them from the
    # spatial node files of exactly the leaf cells that intersect the
    # union bbox of the ways and relations they came from, falling back to
    # byid for anything that isn't found there (should be none for
    # consistent data).
    node_id_parts = [f"SELECT id FROM {hop_table} WHERE type = 'node'"]
    if way_rows_table is not None:
        way_node_ids = forward_new_ids_table(con, way_rows_table, restrict_source_types={"way"})
        if way_node_ids is not None:
            node_id_parts.append(f"SELECT id FROM {way_node_ids} WHERE type = 'node'")
    node_ids_table = idset.fresh_table_name("fwdnodeids")
    con.execute(
        f"CREATE TEMP TABLE {node_ids_table} AS "
        f"SELECT DISTINCT 'node' AS type, id FROM ({' UNION ALL '.join(node_id_parts)}) __u"
    )
    bbox_source_selects = [
        f"SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM {source_table} WHERE type = 'way'",
        f"SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM {source_table} WHERE type = 'relation'",
    ]
    if way_rows_table is not None:
        bbox_source_selects.append(
            f"SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM {way_rows_table}"
        )
    node_sql, nfiles_n = sources.build_node_hydrate_via_bbox_select(
        con, manifest, node_ids_table, bbox_source_selects, promoted_keys
    )
    total_files += nfiles_n
    if node_sql:
        selects.append(node_sql)

    if not selects:
        return empty_set_sql(), total_files
    return "\nUNION ALL\n".join(selects), total_files


def build_backward_one_hop(con, manifest: catalog.Manifest, source_table: str, promoted_keys: set[str],
                            restrict_source_types: Optional[set[str]] = None,
                            role: Optional[str] = None) -> tuple[str, int]:
    """`<`: all ways with a node from the source, plus all relations with a
    node/way/relation from the source as a member, plus all relations that
    have one of those *found ways* as a member (the extra hop Overpass does
    that a single `backward_new_ids_table` call misses; see the
    `25_up_from_node` corpus symptom this fixes)."""
    hop_table, total_files = backward_new_ids_table(con, manifest, source_table, restrict_source_types, role)
    if hop_table is None:
        return empty_set_sql(), total_files
    extra_rel_table, nfiles_extra = backward_new_ids_table(con, manifest, hop_table, restrict_source_types={"way"})
    total_files += nfiles_extra
    if extra_rel_table is not None:
        merged = idset.fresh_table_name("bwdmerged")
        con.execute(
            f"CREATE TEMP TABLE {merged} AS "
            f"SELECT type, id, cell FROM {hop_table} "
            f"UNION SELECT type, id, cell FROM {extra_rel_table}"
        )
        hop_table = merged
    sql, nfiles = hydrate_ids_table(con, manifest, hop_table, [], promoted_keys)
    total_files += nfiles
    return (sql or empty_set_sql()), total_files


def recurse_transitive(con, manifest: catalog.Manifest, input_set: str, promoted_keys: set[str],
                        direction: str) -> tuple[str, int]:
    """`>>` (direction='forward') or `<<` (direction='backward'): repeat one
    hop, accumulating newly discovered (type,id) pairs in a `seen` TEMP
    TABLE (an anti-join against it, not a growing Python set), until fixed
    point. Returns a SELECT over everything newly discovered, plus -- for
    `>>` only -- the relations already present in the input set itself.

    That last part is a real, verified Overpass quirk: unlike `>`, `>>`
    keeps relations of the *original* input set in its result even when
    they are not otherwise reachable by recursing down from it (confirmed
    against tests/corpus/27_down_transitive_from_relation.overpassql's
    cached reference response, whose `relation["leisure"="park"](bbox);>>;`
    output includes every one of the matched park relations, including
    ones with no relation parent or relation members at all -- so they
    cannot have been "discovered", only retained). `<<` has no such
    exception; its own input rows are never echoed back.

    Each backward hop's `hop_table` already carries a spatial `cell` hint
    for the ways/relations it found (design.md 3.1); that hint rides along
    in `new_frontier` so `hydrate_ids_table` keeps using it instead of
    byid on every iteration, not only the first. A forward hop's member
    ways get the same treatment via the *previous* frontier's relation
    bbox (item 3), since a forward hop table itself never carries a cell."""
    seen = idset.fresh_table_name("seen")
    con.execute(f"CREATE TEMP TABLE {seen} AS SELECT DISTINCT type, id FROM {input_set}")
    frontier_table = input_set
    discovered_selects: list[str] = []
    total_files = 0
    hop = 0
    while True:
        hop += 1
        if direction == "forward":
            hop_table = forward_new_ids_table(con, frontier_table)
        else:
            hop_table, nfiles_hop = backward_new_ids_table(con, manifest, frontier_table)
            total_files += nfiles_hop
        if hop_table is None:
            break
        new_frontier = idset.fresh_table_name("frontier")
        con.execute(
            f"CREATE TEMP TABLE {new_frontier} AS "
            f"SELECT h.type, h.id, h.cell FROM {hop_table} h "
            f"WHERE NOT EXISTS (SELECT 1 FROM {seen} s WHERE s.type = h.type AND s.id = h.id)"
        )
        n_new = con.execute(f"SELECT count(*) FROM {new_frontier}").fetchone()[0]
        if not n_new:
            break
        con.execute(f"INSERT INTO {seen} SELECT type, id FROM {new_frontier}")

        hop_selects: list[str] = []
        if direction == "forward":
            # design.md 3.1 items 1/3 and contract section 4: member ways
            # and direct member nodes discovered this hop lie inside the
            # bbox of whichever way/relation we just hopped from
            # (`frontier_table`, the previous iteration's rows -- a way's
            # nodes lie inside the way's own bbox, a relation's member
            # ways/nodes lie inside the relation's own bbox), so resolve
            # both from the spatial files of the cells covering that bbox
            # instead of byid (whose scattered ids -- e.g. a relation with
            # both a very old and a very new node member -- can force a
            # scan of nearly every part). Nested member relations have no
            # such cheap hint here and still hydrate via byid.
            bbox_hint_selects = [
                f"SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM {frontier_table} WHERE type IN ('way', 'relation')"
            ]

            way_ids_table = idset.fresh_table_name("transwayids")
            con.execute(
                f"CREATE TEMP TABLE {way_ids_table} AS "
                f"SELECT DISTINCT id FROM {new_frontier} WHERE type = 'way'"
            )
            n_way = con.execute(f"SELECT count(*) FROM {way_ids_table}").fetchone()[0]
            if n_way:
                way_sql, nfiles_w = sources.build_way_hydrate_via_bbox_select(
                    con, manifest, way_ids_table, bbox_hint_selects, promoted_keys
                )
                total_files += nfiles_w
                if way_sql:
                    hop_selects.append(way_sql)

            node_ids_table = idset.fresh_table_name("transnodeids")
            con.execute(
                f"CREATE TEMP TABLE {node_ids_table} AS "
                f"SELECT DISTINCT id FROM {new_frontier} WHERE type = 'node'"
            )
            n_node = con.execute(f"SELECT count(*) FROM {node_ids_table}").fetchone()[0]
            if n_node:
                node_sql, nfiles_n = sources.build_node_hydrate_via_bbox_select(
                    con, manifest, node_ids_table, bbox_hint_selects, promoted_keys
                )
                total_files += nfiles_n
                if node_sql:
                    hop_selects.append(node_sql)

            rel_sql, nfiles_r = hydrate_ids_table(
                con, manifest, new_frontier, [], promoted_keys, only_types={"relation"}
            )
            total_files += nfiles_r
            if rel_sql:
                hop_selects.append(rel_sql)
        else:
            sql, nfiles = hydrate_ids_table(con, manifest, new_frontier, [], promoted_keys)
            total_files += nfiles
            if sql:
                hop_selects.append(sql)

        if not hop_selects:
            # new_frontier is non-empty but nothing hydrated (e.g. the
            # manifest is missing byid parts for it) -- nothing usable to
            # recurse further from, so stop here rather than looping on an
            # id-only table that the next hop can't read refs/members from.
            break
        materialized = idset.fresh_table_name("hopsel")
        con.execute(f"CREATE TEMP TABLE {materialized} AS {' UNION ALL '.join(hop_selects)}")
        discovered_selects.append(f"SELECT * FROM {materialized}")
        # The next hop needs full rows (refs/members), not just (type, id).
        frontier_table = materialized
        if hop > 10000:  # safety valve against pathological cycles/bugs
            break
    if direction == "forward":
        discovered_selects.append(f"SELECT * FROM {input_set} WHERE type = 'relation'")
    if not discovered_selects:
        return empty_set_sql(), total_files
    return "\nUNION ALL\n".join(discovered_selects), total_files
