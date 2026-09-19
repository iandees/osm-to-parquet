"""`>`, `>>`, `<`, `<<` and the inline recurse filters (w)/(r)/(bn)/(bw)/(br).

Contract section 8 / design.md 3.3: forward recursion follows a way's
``refs`` and a relation's ``members``; backward recursion uses the
``node_way`` and ``member`` reverse indexes. Everything fetches full rows
from the byid copy (the "simplest correct M0 approach").
"""
from __future__ import annotations

from typing import Optional

from . import catalog, sources
from .schema import empty_set_sql


def _quote_list(paths: list[str]) -> str:
    return "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def _byid_union(manifest: catalog.Manifest, ids_by_type: dict[str, list[int]], promoted_keys: set[str]) -> tuple[Optional[str], int]:
    parts = []
    files_total = 0
    for t in ("node", "way", "relation"):
        ids = sorted(set(ids_by_type.get(t) or []))
        if not ids:
            continue
        sql, nfiles = sources.build_byid_select(manifest, t, ids, [], promoted_keys)
        files_total += nfiles
        parts.append(sql)
    if not parts:
        return None, 0
    return "\nUNION ALL\n".join(parts), files_total


def forward_new_ids(con, source_table: str, restrict_source_types: Optional[set[str]] = None,
                     role: Optional[str] = None) -> dict[str, list[int]]:
    """One hop of `>`: way.refs -> nodes; relation.members -> n/w/r ids."""
    node_ids: set[int] = set()
    way_ids: set[int] = set()
    rel_ids: set[int] = set()

    want_way_source = restrict_source_types is None or "way" in restrict_source_types
    want_rel_source = restrict_source_types is None or "relation" in restrict_source_types

    if want_way_source:
        rows = con.execute(
            f"SELECT DISTINCT unnest(refs) FROM {source_table} WHERE type='way' AND refs IS NOT NULL"
        ).fetchall()
        node_ids.update(r[0] for r in rows)

    if want_rel_source:
        role_clause = ""
        if role is not None:
            role_clause = " AND m.role = ?"
        query = (
            f"SELECT DISTINCT m.type, m.ref FROM {source_table}, UNNEST(members) AS t(m) "
            f"WHERE type='relation' AND members IS NOT NULL{role_clause}"
        )
        rows = con.execute(query, [role] if role is not None else []).fetchall()
        for mtype, mref in rows:
            if mtype == "n":
                node_ids.add(mref)
            elif mtype == "w":
                way_ids.add(mref)
            elif mtype == "r":
                rel_ids.add(mref)

    return {"node": sorted(node_ids), "way": sorted(way_ids), "relation": sorted(rel_ids)}


def backward_new_ids(con, manifest: catalog.Manifest, source_table: str,
                      restrict_source_types: Optional[set[str]] = None,
                      role: Optional[str] = None) -> dict[str, list[int]]:
    """One hop of `<`: nodes -> parent ways (node_way index); any element ->
    parent relations (member index)."""
    way_ids: set[int] = set()
    rel_ids: set[int] = set()

    want_node_source = restrict_source_types is None or "node" in restrict_source_types
    if want_node_source:
        node_ids = [r[0] for r in con.execute(f"SELECT DISTINCT id FROM {source_table} WHERE type='node'").fetchall()]
        if node_ids:
            parts = catalog.index_parts_for_ids(manifest, "node_way", node_ids)
            files = [manifest.path(p["path"]) for p in parts]
            if files:
                idlist = ",".join(str(i) for i in node_ids)
                rows = con.execute(
                    f"SELECT DISTINCT way_id FROM read_parquet({_quote_list(files)}) WHERE node_id IN ({idlist})"
                ).fetchall()
                way_ids.update(r[0] for r in rows)

    member_files = [manifest.path(p["path"]) for p in manifest.index_parts("member")]
    if member_files:
        for mtype_char, osm_type in (("n", "node"), ("w", "way"), ("r", "relation")):
            if restrict_source_types is not None and osm_type not in restrict_source_types:
                continue
            ids = [r[0] for r in con.execute(f"SELECT DISTINCT id FROM {source_table} WHERE type='{osm_type}'").fetchall()]
            if not ids:
                continue
            idlist = ",".join(str(i) for i in ids)
            role_clause = " AND role = ?" if role is not None else ""
            q = (
                f"SELECT DISTINCT parent_id FROM read_parquet({_quote_list(member_files)}) "
                f"WHERE member_type = '{mtype_char}' AND member_id IN ({idlist}){role_clause}"
            )
            rows = con.execute(q, [role] if role is not None else []).fetchall()
            rel_ids.update(r[0] for r in rows)

    return {"way": sorted(way_ids), "relation": sorted(rel_ids)}


def build_forward_one_hop(con, manifest: catalog.Manifest, source_table: str, promoted_keys: set[str],
                           restrict_source_types: Optional[set[str]] = None,
                           role: Optional[str] = None) -> tuple[str, int]:
    ids = forward_new_ids(con, source_table, restrict_source_types, role)
    sql, nfiles = _byid_union(manifest, ids, promoted_keys)
    return (sql or empty_set_sql()), nfiles


def build_backward_one_hop(con, manifest: catalog.Manifest, source_table: str, promoted_keys: set[str],
                            restrict_source_types: Optional[set[str]] = None,
                            role: Optional[str] = None) -> tuple[str, int]:
    ids = backward_new_ids(con, manifest, source_table, restrict_source_types, role)
    sql, nfiles = _byid_union(manifest, ids, promoted_keys)
    return (sql or empty_set_sql()), nfiles


def recurse_transitive(con, manifest: catalog.Manifest, input_set: str, promoted_keys: set[str],
                        direction: str) -> tuple[str, int]:
    """`>>` (direction='forward') or `<<` (direction='backward'): repeat one
    hop, accumulating newly discovered (type,id) pairs, until fixed point.
    Returns a SELECT over everything newly discovered (never the input set's
    own rows)."""
    seen = {(t, i) for t, i in con.execute(f"SELECT type, id FROM {input_set}").fetchall()}
    frontier_table = input_set
    discovered_selects: list[str] = []
    total_files = 0
    hop = 0
    while True:
        hop += 1
        if direction == "forward":
            ids = forward_new_ids(con, frontier_table)
        else:
            ids = backward_new_ids(con, manifest, frontier_table)
        filtered = {t: [i for i in v if (t, i) not in seen] for t, v in ids.items()}
        for t, v in filtered.items():
            seen.update((t, i) for i in v)
        if not any(filtered.values()):
            break
        sql, nfiles = _byid_union(manifest, filtered, promoted_keys)
        total_files += nfiles
        if sql is None:
            break
        frontier_name = f"__frontier_{id(input_set)}_{hop}"
        con.execute(f"CREATE OR REPLACE TEMP TABLE {frontier_name} AS {sql}")
        discovered_selects.append(f"SELECT * FROM {frontier_name}")
        frontier_table = frontier_name
        if hop > 10000:  # safety valve against pathological cycles/bugs
            break
    if not discovered_selects:
        return empty_set_sql(), total_files
    return "\nUNION ALL\n".join(discovered_selects), total_files
