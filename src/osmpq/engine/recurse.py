"""`>`, `>>`, `<`, `<<` and the inline recurse filters (w)/(r)/(bn)/(bw)/(br).

Contract section 8 / design.md 3.3: forward recursion follows a way's
``refs`` and a relation's ``members``; backward recursion uses the
``node_way`` and ``member`` reverse indexes. Everything fetches full rows
from the byid copy (the "simplest correct M0 approach").

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
# One hop, derived in SQL: produces a fresh TEMP TABLE(type, id) or None.
# --------------------------------------------------------------------------


def _forward_hop_sql(source_table: str, want_way: bool, want_rel: bool, role: Optional[str]) -> tuple[Optional[str], list]:
    parts = []
    params: list = []
    if want_way:
        parts.append(
            f"SELECT 'node' AS type, unnest(refs) AS id FROM {source_table} "
            f"WHERE type = 'way' AND refs IS NOT NULL"
        )
    if want_rel:
        role_clause = " AND m.role = ?" if role is not None else ""
        if role is not None:
            params.append(role)
        parts.append(
            f"SELECT CASE m.type WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation' END AS type, "
            f"m.ref AS id FROM {source_table}, UNNEST(members) AS t(m) "
            f"WHERE type = 'relation' AND members IS NOT NULL{role_clause}"
        )
    if not parts:
        return None, []
    return "\nUNION ALL\n".join(parts), params


def forward_new_ids_table(con, source_table: str, restrict_source_types: Optional[set[str]] = None,
                           role: Optional[str] = None) -> Optional[str]:
    """One hop of `>`: way.refs -> nodes; relation.members -> n/w/r ids.
    Returns a fresh TEMP TABLE(type, id) name (deduplicated), or None if
    there is nothing to hop from."""
    want_way = restrict_source_types is None or "way" in restrict_source_types
    want_rel = restrict_source_types is None or "relation" in restrict_source_types
    hop_sql, params = _forward_hop_sql(source_table, want_way, want_rel, role)
    if hop_sql is None:
        return None
    name = idset.fresh_table_name("fwd")
    con.execute(f"CREATE TEMP TABLE {name} AS SELECT DISTINCT type, id FROM ({hop_sql}) __hop", params)
    return name


def backward_new_ids_table(con, manifest: catalog.Manifest, source_table: str,
                            restrict_source_types: Optional[set[str]] = None,
                            role: Optional[str] = None) -> Optional[str]:
    """One hop of `<`: nodes -> parent ways (node_way index, joined straight
    against `source_table`); any element -> parent relations (member
    index, same). Returns a fresh TEMP TABLE(type, id) name, or None."""
    parts: list[str] = []
    params: list = []

    want_node = restrict_source_types is None or "node" in restrict_source_types
    if want_node:
        lo, hi, n = idset.sql_type_range(con, source_table, "node")
        if n:
            files = [manifest.path(p["path"]) for p in catalog.index_parts_for_range(manifest, "node_way", lo, hi)]
            if files:
                parts.append(
                    f"SELECT 'way' AS type, idx.way_id AS id "
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
                f"SELECT 'relation' AS type, idx.parent_id AS id "
                f"FROM read_parquet({_quote_list(member_files)}) idx "
                f"JOIN {source_table} s "
                f"  ON idx.member_id = s.id AND s.type = CASE idx.member_type {type_case} END "
                f"WHERE idx.member_type IN ({type_in}){role_clause}"
            )

    if not parts:
        return None
    name = idset.fresh_table_name("bwd")
    hop_sql = "\nUNION ALL\n".join(parts)
    con.execute(f"CREATE TEMP TABLE {name} AS SELECT DISTINCT type, id FROM ({hop_sql}) __hop", params)
    return name


# --------------------------------------------------------------------------
# Hydrating a (type, id) TEMP TABLE into full rows via byid.
# --------------------------------------------------------------------------


def hydrate_ids_table(con, manifest: catalog.Manifest, id_table: str, tag_filters, promoted_keys: set[str],
                       only_types: Optional[set[str]] = None) -> tuple[Optional[str], int]:
    """Build the byid UNION ALL select hydrating every (type, id) row of
    `id_table`, restricted to `only_types` if given. The ids themselves
    never leave SQL: per type we only fetch a min/max/count aggregate (to
    pick byid parts) and pass the rest as a `SELECT id FROM id_table WHERE
    type = ...` subquery, which DuckDB plans as a hash join against
    `id_table` regardless of its size."""
    selects = []
    files_total = 0
    for t in ("node", "way", "relation"):
        if only_types is not None and t not in only_types:
            continue
        lo, hi, n = idset.sql_type_range(con, id_table, t)
        if not n:
            continue
        id_subquery = f"SELECT id FROM {id_table} WHERE type = '{t}'"
        sql, nfiles = sources.build_byid_select_from_ids_query(manifest, t, id_subquery, lo, hi, tag_filters, promoted_keys)
        files_total += nfiles
        selects.append(sql)
    if not selects:
        return None, files_total
    return "\nUNION ALL\n".join(selects), files_total


def build_forward_one_hop(con, manifest: catalog.Manifest, source_table: str, promoted_keys: set[str],
                           restrict_source_types: Optional[set[str]] = None,
                           role: Optional[str] = None) -> tuple[str, int]:
    table = forward_new_ids_table(con, source_table, restrict_source_types, role)
    if table is None:
        return empty_set_sql(), 0
    sql, nfiles = hydrate_ids_table(con, manifest, table, [], promoted_keys)
    return (sql or empty_set_sql()), nfiles


def build_backward_one_hop(con, manifest: catalog.Manifest, source_table: str, promoted_keys: set[str],
                            restrict_source_types: Optional[set[str]] = None,
                            role: Optional[str] = None) -> tuple[str, int]:
    table = backward_new_ids_table(con, manifest, source_table, restrict_source_types, role)
    if table is None:
        return empty_set_sql(), 0
    sql, nfiles = hydrate_ids_table(con, manifest, table, [], promoted_keys)
    return (sql or empty_set_sql()), nfiles


def recurse_transitive(con, manifest: catalog.Manifest, input_set: str, promoted_keys: set[str],
                        direction: str) -> tuple[str, int]:
    """`>>` (direction='forward') or `<<` (direction='backward'): repeat one
    hop, accumulating newly discovered (type,id) pairs in a `seen` TEMP
    TABLE (an anti-join against it, not a growing Python set), until fixed
    point. Returns a SELECT over everything newly discovered (never the
    input set's own rows)."""
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
            hop_table = backward_new_ids_table(con, manifest, frontier_table)
        if hop_table is None:
            break
        new_frontier = idset.fresh_table_name("frontier")
        con.execute(
            f"CREATE TEMP TABLE {new_frontier} AS "
            f"SELECT h.type, h.id FROM {hop_table} h "
            f"WHERE NOT EXISTS (SELECT 1 FROM {seen} s WHERE s.type = h.type AND s.id = h.id)"
        )
        n_new = con.execute(f"SELECT count(*) FROM {new_frontier}").fetchone()[0]
        if not n_new:
            break
        con.execute(f"INSERT INTO {seen} SELECT type, id FROM {new_frontier}")
        sql, nfiles = hydrate_ids_table(con, manifest, new_frontier, [], promoted_keys)
        total_files += nfiles
        if sql is None:
            # new_frontier is non-empty but nothing hydrated (e.g. the
            # manifest is missing byid parts for it) -- nothing usable to
            # recurse further from, so stop here rather than looping on an
            # id-only table that the next hop can't read refs/members from.
            break
        materialized = idset.fresh_table_name("hopsel")
        con.execute(f"CREATE TEMP TABLE {materialized} AS {sql}")
        discovered_selects.append(f"SELECT * FROM {materialized}")
        # The next hop needs full rows (refs/members), not just (type, id).
        frontier_table = materialized
        if hop > 10000:  # safety valve against pathological cycles/bugs
            break
    if not discovered_selects:
        return empty_set_sql(), total_files
    return "\nUNION ALL\n".join(discovered_selects), total_files
