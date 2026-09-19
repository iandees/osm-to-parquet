"""Engine-side attic support (docs/m4-contracts.md section 3): snapshot
reads over the history dataset, `(changed:)`/`(newer:)` with history,
`timeline(...)`, and the `[diff:]`/`[adiff:]` two-pass executor.

This module is the one place that reads `history/` Parquet files (base
spatial/byid + tier files, see `src/osmpq/history/schema.py` for the row
shape and the ordering rule). Everything above it -- `sources.py`'s
`current_rows`/`byid_current_rows` (via `catalog.SNAPSHOT`), `recurse.py`'s
backward hop, `metafilters.py`'s `(changed:)`/`(newer:)` hooks, and
`planner.py`'s `Retro`/`Timeline` dispatch -- calls into the functions
here rather than touching `history/` paths itself, so there is exactly one
place that knows the on-disk layout of section 2.2 and the state-at-`t`
rule of section 2.1/3.1.

Listed in `hooks.HOOK_MODULES` (imported lazily on the planner's first
run, like every other tier-2 module) even though it registers no filter/
statement hooks itself -- `metafilters.py` and `planner.py` import it
directly, but keeping it in the list means a worktree that merges this
file after `metafilters.py`/`planner.py` still gets it loaded eagerly
alongside the other M3/M4 engine modules, and a future hook this module
might register (none today) would need no further wiring.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from osmpq.errors import RuntimeQueryError
from osmpq.history import schema as hschema
from osmpq.ql import evaluator as ev

from . import catalog, idset
from .schema import empty_set_sql, project

# A query time later than any real data; used for "b" in `[diff:]`/
# `[adiff:]` when omitted ("b = now") and for `(changed:)` with no upper
# bound: picking the newest known state is exactly "now" for a history
# kept up to date by the updater (docs/m4-contracts.md section 8), and it
# lets every attic read go through one history-aware code path instead of
# a separate "read the live current tables" branch.
FAR_FUTURE = datetime(9999, 12, 31, 23, 59, 59)

_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def parse_date(value: str) -> datetime:
    """Parse an Overpass `"YYYY-MM-DDThh:mm:ssZ"` timestamp strictly,
    raising `RuntimeQueryError` (rendered as a `remark`, HTTP 200 -- same
    convention as `metafilters._check_timestamp`) for anything malformed,
    including a syntactically-shaped but calendar-invalid date (e.g. month
    13). Note: the live reference server was observed to silently *ignore*
    a malformed `[date:]` value instead (probe `date_bad_json`: same
    result as no `[date:]` at all) -- we raise instead, since silently
    running an unrestricted query is a worse failure mode for a caller
    that made a typo; documented as an intentional deviation."""
    if not _TIMESTAMP_RE.match(value):
        raise RuntimeQueryError(
            f'runtime error: date "{value}" is invalid. It should look like "2000-01-01T00:00:00Z".'
        )
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise RuntimeQueryError(
            f'runtime error: date "{value}" is invalid. It should look like "2000-01-01T00:00:00Z".'
        ) from None


def resolve_optional_date(value: Optional[str]) -> datetime:
    """`b` in `[diff:"a"]`/`[diff:"a","b"]`/`(changed:"a","b"?)`: the given
    timestamp, or `FAR_FUTURE` ("now") when omitted."""
    if value is None:
        return FAR_FUTURE
    return parse_date(value)


def _t_sql(t: datetime) -> str:
    return "TIMESTAMP '" + t.strftime("%Y-%m-%d %H:%M:%S") + "'"


def _quote_list(paths: list[str]) -> str:
    return "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def _q1(s: str) -> str:
    return s.replace("'", "''")


def _quote_str_list(values: list[str]) -> str:
    if not values:
        return "(NULL)"
    return "(" + ",".join("'" + _q1(v) + "'" for v in values) + ")"


def history_remark(manifest: catalog.Manifest, t: datetime) -> Optional[str]:
    """docs/m4-contracts.md section 3.1: a `remark` when `t` is earlier
    than `history.since` -- the history answers from whatever state
    existed at `since` (that state "looks like it always existed")."""
    since = manifest.history_since
    if not since:
        return None
    try:
        since_dt = parse_date(since)
    except RuntimeQueryError:
        return None
    if t < since_dt:
        return f"history starts at {since}; earlier dates return the earliest known state"
    return None


# --------------------------------------------------------------------------
# section 3.1: the snapshot read seam `sources.current_rows`/
# `byid_current_rows` call into when `catalog.SNAPSHOT` is set.
# --------------------------------------------------------------------------


def snapshot_current_rows(
    con,
    manifest: catalog.Manifest,
    table: str,
    cells: list[str],
    cols: dict[str, str],
    where_sql: str,
    t: datetime,
) -> tuple[str, int]:
    t_sql = _t_sql(t)
    base_files = manifest.history_spatial_files(table, cells)
    tier_files = manifest.history_tier_spatial_files(table)
    if not base_files and not tier_files:
        return empty_set_sql(), 0

    parts = []
    if base_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(base_files)}, hive_partitioning=true, union_by_name=true)\n"
            f"WHERE {hschema.validity_predicate(t_sql)}"
        )
    if tier_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(tier_files)}, union_by_name=true)\n"
            f"WHERE cell IN {_quote_str_list(cells)} AND {hschema.validity_predicate(t_sql, with_valid_to=False)}"
        )
    inner_sql = "\nUNION ALL BY NAME\n".join(parts)
    state_sql = hschema.state_at_sql(inner_sql, t_sql)
    sql = f"SELECT {project(cols)} FROM (\n{state_sql}\n) __h\nWHERE __h.visible AND ({where_sql})"
    return sql, len(base_files) + len(tier_files)


def snapshot_byid_current_rows(
    con,
    manifest: catalog.Manifest,
    element_type: str,
    cols: dict[str, str],
    id_pred_sql: str,
    tag_where_sql: str,
    t: datetime,
) -> tuple[str, int]:
    t_sql = _t_sql(t)
    base_files = [manifest.path(p["path"]) for p in manifest.history_byid_parts(element_type)]
    tier_files = manifest.history_tier_byid_files(element_type)
    if not base_files and not tier_files:
        return empty_set_sql(), 0

    parts = []
    if base_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(base_files)}, union_by_name=true)\n"
            f"WHERE ({id_pred_sql}) AND {hschema.validity_predicate(t_sql)}"
        )
    if tier_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(tier_files)}, union_by_name=true)\n"
            f"WHERE ({id_pred_sql}) AND {hschema.validity_predicate(t_sql, with_valid_to=False)}"
        )
    inner_sql = "\nUNION ALL BY NAME\n".join(parts)
    state_sql = hschema.state_at_sql(inner_sql, t_sql)
    sql = f"SELECT {project(cols)} FROM (\n{state_sql}\n) __h\nWHERE __h.visible AND ({tag_where_sql})"
    return sql, len(base_files) + len(tier_files)


def raw_state_row(con, manifest: catalog.Manifest, element_type: str, element_id: int, t: datetime) -> Optional[dict]:
    """The single history row (whatever its `visible` flag) that is the
    state of `(element_type, element_id)` at `t`, ignoring every query
    filter -- used for diff/adiff's "raw" old/new stubs (section 3.2) and
    for `timeline(...)`'s single-element lookups. Returns a dict of the
    row's own columns (including `minor`/`valid_from`/`valid_to`/
    `visible`), or None when nothing is known about this id by `t`."""
    t_sql = _t_sql(t)
    base_files = [manifest.path(p["path"]) for p in manifest.history_byid_parts(element_type)]
    tier_files = manifest.history_tier_byid_files(element_type)
    if not base_files and not tier_files:
        return None
    id_pred = f"id = {int(element_id)}"
    parts = []
    if base_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(base_files)}, union_by_name=true)\n"
            f"WHERE ({id_pred}) AND {hschema.validity_predicate(t_sql)}"
        )
    if tier_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(tier_files)}, union_by_name=true)\n"
            f"WHERE ({id_pred}) AND {hschema.validity_predicate(t_sql, with_valid_to=False)}"
        )
    inner_sql = "\nUNION ALL BY NAME\n".join(parts)
    state_sql = hschema.state_at_sql(inner_sql, t_sql)
    cur = con.execute(state_sql)
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip(cols, row))


def history_way_geometry_by_id(con, manifest: catalog.Manifest, ids: list[int], t: datetime) -> dict[int, str]:
    """(way id) -> WKT geometry at `t`, resolved from the history byid
    files (which, like the current byid copy, carry no `geometry` column
    of their own) by falling back to the history *spatial* byid-adjacent
    lookup: since a byid-sourced row already knows its own state's
    `cell` (projected by `attic.snapshot_byid_current_rows`), read that
    cell's base spatial file (+ every tier) at `t` and pick out these
    ids. Used by `render.hydrate_way_geometry` under a snapshot."""
    ids = sorted(set(ids))
    if not ids:
        return {}
    t_sql = _t_sql(t)
    id_pred = f"id IN ({','.join(str(i) for i in ids)})"
    base_files = manifest.history_spatial_files("way", catalog.history_cells_for_bbox(manifest, "way", None))
    tier_files = manifest.history_tier_spatial_files("way")
    if not base_files and not tier_files:
        return {}
    parts = []
    if base_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(base_files)}, hive_partitioning=true, union_by_name=true)\n"
            f"WHERE ({id_pred}) AND {hschema.validity_predicate(t_sql)}"
        )
    if tier_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(tier_files)}, union_by_name=true)\n"
            f"WHERE ({id_pred}) AND {hschema.validity_predicate(t_sql, with_valid_to=False)}"
        )
    inner_sql = "\nUNION ALL BY NAME\n".join(parts)
    state_sql = hschema.state_at_sql(inner_sql, t_sql)
    rows = con.execute(f"SELECT id, ST_AsText(geometry) FROM ({state_sql}) __h WHERE __h.visible").fetchall()
    return {i: wkt for i, wkt in rows if wkt}


def stub_element(element_type: str, row: dict, show_visible: bool) -> dict:
    """The minimal `{id, version, timestamp, changeset, uid, user[,
    visible]}` shape observed for a diff/adiff "raw" old/new stub (section
    3.2, probes `diff_xml`/`adiff_xml`): no tags, no coordinates/geometry,
    regardless of the element's real content or the query's verbosity.
    `element_type` is passed explicitly since a history byid row (like the
    current byid copy it mirrors) carries no physical `type` column -- the
    type is implicit in which file it was read from."""
    from . import render as render_mod

    el: dict = {"type": element_type, "id": row["id"]}
    if row.get("version") is not None:
        el["version"] = row["version"]
    ts = render_mod.format_timestamp(row.get("timestamp"))
    if ts is not None:
        el["timestamp"] = ts
    if row.get("changeset") is not None:
        el["changeset"] = row["changeset"]
    if row.get("uid") is not None:
        el["uid"] = row["uid"]
    if row.get("user") is not None:
        el["user"] = row["user"]
    if show_visible:
        el["visible"] = bool(row.get("visible"))
    return el


# --------------------------------------------------------------------------
# section 3.1: backward recursion (`<`/`<<`/`(bn)`/`(bw)`/`(br)`) for
# relations under a snapshot -- recurse.py's `backward_new_ids_table`
# calls this instead of the (current-only) member index when
# `catalog.SNAPSHOT` is set.
# --------------------------------------------------------------------------


def backward_relation_ids_snapshot(
    con,
    manifest: catalog.Manifest,
    source_table: str,
    t: datetime,
    restrict_source_types: Optional[set[str]] = None,
    role: Optional[str] = None,
) -> tuple[Optional[str], int]:
    """Relation lookups (any element -> parent relations) at `t`: scan
    relation history rows (base + tier) in the ancestors-and-self cells of
    `source_table`'s own rows' cells (each source row already carries the
    cell it was resolved in), keeping a relation whose `members` list
    contains one of `source_table`'s (type, id) pairs. Returns (fresh TEMP
    TABLE(type, id, cell) name, files read) or (None, 0)."""
    cell_rows = con.execute(f"SELECT DISTINCT cell FROM {source_table} WHERE cell IS NOT NULL").fetchall()
    cell_keys: set[str] = set()
    for (c,) in cell_rows:
        cell_keys.update(catalog.ancestors_and_self(c))
    if not cell_keys:
        return None, 0
    cells = sorted(cell_keys)
    t_sql = _t_sql(t)
    base_files = manifest.history_spatial_files("relation", cells)
    tier_files = manifest.history_tier_spatial_files("relation")
    if not base_files and not tier_files:
        return None, 0

    parts = []
    if base_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(base_files)}, hive_partitioning=true, union_by_name=true)\n"
            f"WHERE {hschema.validity_predicate(t_sql)}"
        )
    if tier_files:
        parts.append(
            f"SELECT * FROM read_parquet({_quote_list(tier_files)}, union_by_name=true)\n"
            f"WHERE cell IN {_quote_str_list(cells)} AND {hschema.validity_predicate(t_sql, with_valid_to=False)}"
        )
    inner_sql = "\nUNION ALL BY NAME\n".join(parts)
    state_sql = hschema.state_at_sql(inner_sql, t_sql)

    type_char = {"node": "n", "way": "w", "relation": "r"}
    wanted = restrict_source_types or {"node", "way", "relation"}
    wanted_chars = [type_char[t2] for t2 in ("node", "way", "relation") if t2 in wanted]
    type_in = ",".join(f"'{c}'" for c in wanted_chars)
    case_sql = " ".join(f"WHEN '{c}' THEN '{t2}'" for t2, c in type_char.items())
    role_clause = " AND m.role = ?" if role is not None else ""
    params = [role] if role is not None else []

    name = idset.fresh_table_name("bwdrelsnap")
    con.execute(
        f"CREATE TEMP TABLE {name} AS "
        f"SELECT DISTINCT 'relation' AS type, h.id AS id, h.cell AS cell FROM (\n{state_sql}\n) h, "
        f"UNNEST(h.members) AS t(m) "
        f"WHERE h.visible AND m.type IN ({type_in}){role_clause} "
        f"AND EXISTS (SELECT 1 FROM {source_table} s WHERE s.id = m.ref "
        f"AND s.type = CASE m.type {case_sql} END)",
        params,
    )
    return name, len(base_files) + len(tier_files)


# --------------------------------------------------------------------------
# section 3.2: `(changed:)`/`(newer:)` with history (metafilters.py hook).
# --------------------------------------------------------------------------


def changed_ids_predicate(
    con,
    manifest: catalog.Manifest,
    bbox: Optional[tuple],
    since: datetime,
    until: datetime,
    alias: str,
) -> str:
    """A SQL boolean over `alias` (a canonical row) matching "some history
    row for this id has `since < valid_from <= until`" (section 3.2: any
    row -- minor versions and deletions count, including a deletion or a
    minor/geometry-only state), implemented as a per-query TEMP TABLE of
    changed (type, id) pairs scoped to `bbox`'s cells (spatial history
    base files of those cells + every tier, per table -- exactly the file
    list a snapshot spatial read of that bbox would use)."""
    since_sql, until_sql = _t_sql(since), _t_sql(until)
    parts = []
    for table in ("node", "way", "relation"):
        cells = catalog.history_cells_for_bbox(manifest, table, bbox)
        bfiles = manifest.history_spatial_files(table, cells)
        tfiles = manifest.history_tier_spatial_files(table)
        if not bfiles and not tfiles:
            continue
        if bfiles:
            parts.append(
                f"SELECT '{table}' AS __type, id FROM read_parquet({_quote_list(bfiles)}, hive_partitioning=true, union_by_name=true) "
                f"WHERE valid_from > {since_sql} AND valid_from <= {until_sql}"
            )
        if tfiles:
            parts.append(
                f"SELECT '{table}' AS __type, id FROM read_parquet({_quote_list(tfiles)}, union_by_name=true) "
                f"WHERE cell IN {_quote_str_list(cells)} AND valid_from > {since_sql} AND valid_from <= {until_sql}"
            )
    if not parts:
        return "FALSE"
    name = idset.fresh_table_name("changedids")
    con.execute(f"CREATE TEMP TABLE {name} AS SELECT DISTINCT __type, id FROM (\n" + "\nUNION ALL\n".join(parts) + "\n) __c")
    return f"EXISTS (SELECT 1 FROM {name} __ch WHERE __ch.__type = {alias}.type AND __ch.id = {alias}.id)"


# --------------------------------------------------------------------------
# section 3.2: `timeline(type, id[, version])`.
# --------------------------------------------------------------------------


def timeline_elements(con, manifest: catalog.Manifest, element_type: str, element_id: int, version: Optional[int]) -> list[dict]:
    """Every *own-version* state of `(element_type, element_id)`, one
    `timeline` element per state (minor/geometry-only states get no entry
    -- confirmed against `m4probe/timeline_way_json.json`: way 23125943's
    11 node-move minor states produce zero extra entries, only its 11 own
    versions 6..16 do), numbered from 1 in time order; `expired` is the
    next entry's `created`, absent on the last. `version`, when given,
    keeps only the matching entry."""
    if not manifest.has_history():
        return []
    base_files = [manifest.path(p["path"]) for p in manifest.history_byid_parts(element_type)]
    tier_files = manifest.history_tier_byid_files(element_type)
    if not base_files and not tier_files:
        return []
    id_pred = f"id = {int(element_id)}"
    parts = []
    if base_files:
        parts.append(f"SELECT * FROM read_parquet({_quote_list(base_files)}, union_by_name=true) WHERE ({id_pred})")
    if tier_files:
        parts.append(f"SELECT * FROM read_parquet({_quote_list(tier_files)}, union_by_name=true) WHERE ({id_pred})")
    inner_sql = "\nUNION ALL BY NAME\n".join(parts)
    # `valid_from` (not the meta `"timestamp"` column) is used for
    # created/expired: they agree for every live own-version state (
    # section 2.1: "valid_from = the version's timestamp" for minor=0
    # rows), but a deletion tombstone's own `"timestamp"` is NULL (section
    # 2.1's tombstone shape) while its `valid_from` is still the
    # deletion's real instant -- a deleted element's own version still
    # gets a timeline entry.
    rows = con.execute(
        f"SELECT version, valid_from FROM ({inner_sql}) __t "
        f"WHERE minor = 0 ORDER BY valid_from ASC, version ASC"
    ).fetchall()
    if not rows:
        return []
    from .render import format_timestamp

    elements = []
    for i, (ver, ts) in enumerate(rows):
        if version is not None and ver != version:
            continue
        created = format_timestamp(ts)
        expired = format_timestamp(rows[i + 1][1]) if i + 1 < len(rows) else None
        tags = {
            "reftype": element_type,
            "ref": str(element_id),
            "refversion": str(ver),
            "created": created,
        }
        if expired is not None:
            tags["expired"] = expired
        elements.append({"type": "timeline", "id": len(elements) + 1, "tags": tags})
    if version is not None:
        # Renumber to 1 when filtered to a single version, matching the
        # reference's `timeline(type,id,version)` output (a lone `id: 1`
        # entry, m4probe/timeline_ver_json.json).
        elements = [{**el, "id": i + 1} for i, el in enumerate(elements)]
    return elements


def timeline_select_sql(elements: list[dict]) -> str:
    """The canonical-set SELECT for `timeline_elements`'s result: `type` =
    'timeline', `id` = the sequence number, `tags` = the MAP shown in
    docs/m4-contracts.md section 3.2. Every other canonical column is
    NULL. `out`'s generic rendering (verbosity in body/tags/meta) already
    prints a `{"type": "timeline", "id": k, "tags": {...}}` element
    unchanged, so no render.py/result.py change is needed for the JSON
    shape; XML gets its own `<timeline id="k">` tag in result.py."""
    if not elements:
        return empty_set_sql()
    parts = []
    for el in elements:
        tags: dict = el["tags"]
        keys_sql = "[" + ",".join("'" + _q1(k) + "'" for k in tags.keys()) + "]"
        vals_sql = "[" + ",".join("'" + _q1(v) + "'" for v in tags.values()) + "]"
        cols = {"type": "'timeline'", "id": str(int(el["id"])), "tags": f"MAP({keys_sql}, {vals_sql})"}
        parts.append(f"SELECT {project(cols)}")
    return "\nUNION ALL\n".join(parts)


# --------------------------------------------------------------------------
# section 3.2: `retro`'s block-local set scope (a live reference probe
# confirmed a set a `retro` block assigns -- including the default set
# `_` -- is not visible after the block: `retro("t"){ node(...)->.then;
# } .then; out count;` gives 0 on the reference).
# --------------------------------------------------------------------------


def snapshot_sets(con) -> dict[str, str]:
    """Copy every current `set_<name>` TEMP TABLE into a fresh backup
    table, returning {original name -> backup name}. `restore_sets`
    undoes a block's set mutations with this."""
    names = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'set\\_%' ESCAPE '\\'"
        ).fetchall()
    ]
    backups: dict[str, str] = {}
    for name in names:
        backup = idset.fresh_table_name("retrobak")
        con.execute(f"CREATE TEMP TABLE {backup} AS SELECT * FROM {name}")
        backups[name] = backup
    return backups


def restore_sets(con, backups: dict[str, str]) -> None:
    """Restore every set `snapshot_sets` backed up to its saved content,
    and empty out any `set_<name>` table that didn't exist at snapshot
    time (a set a block created fresh) -- the default set `_` included.
    A probed reference behavior is *emptied*, not dropped/undefined
    (`retro("t"){ node(...)->.then; } .then; out count;` gives a `0`
    count on the reference, not a "set has not been set before" runtime
    error) -- so referencing it afterwards still works, just as if
    nothing had ever matched."""
    names = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'set\\_%' ESCAPE '\\'"
        ).fetchall()
    ]
    for name in names:
        if name not in backups:
            con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS {empty_set_sql()}")
    for name, backup in backups.items():
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM {backup}")
        con.execute(f"DROP TABLE {backup}")


# --------------------------------------------------------------------------
# section 3.2: `retro("t") { ... }` time-expression evaluation.
# --------------------------------------------------------------------------


def evaluate_retro_time(ctx, time_expr: str) -> datetime:
    """`retro`'s argument: a string literal, or an evaluator expression
    the M3 evaluator (`osmpq.ql.evaluator`) supports, evaluated against
    the ambient default set `_` (docs/m4-contracts.md section 3.2)."""
    from . import evalsql

    stripped = time_expr.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in ("'", '"'):
        # A bare literal doesn't need the default set to exist yet.
        value = stripped[1:-1]
    else:
        expr = ev.parse(time_expr)
        value = evalsql.evaluate_set(ctx, expr, "_")
    return parse_date(value)


# --------------------------------------------------------------------------
# section 3.2: `[diff:]`/`[adiff:]` two-pass execution.
# --------------------------------------------------------------------------


def _out_statements(statements) -> list:
    """Every `Out` statement anywhere in the program (including inside
    union/difference/foreach/if bodies) -- diff/adiff collects elements
    from *every* `out`, keyed by (type, id) (section 3.2)."""
    from osmpq.ql.ast import Out

    found = []
    for stmt in statements:
        if isinstance(stmt, Out):
            found.append(stmt)
        for attr in ("statements", "body", "then", "otherwise"):
            inner = getattr(stmt, attr, None)
            if inner:
                found.extend(_out_statements(inner))
        for attr in ("first", "second"):
            inner = getattr(stmt, attr, None)
            if inner is not None:
                found.extend(_out_statements([inner]))
    return found


class _PassResult:
    def __init__(self, elements: list[dict], versions: dict, files_read: int, warnings: list[str]):
        self.elements = elements
        self.versions = versions
        self.files_read = files_read
        self.warnings = warnings


def run_diff_pass(con, manifest: catalog.Manifest, program, t: datetime) -> _PassResult:
    """Run the whole program once with `catalog.SNAPSHOT = t`, returning
    the ordered list of elements every `out` statement in it produced
    (section 3.2's "collecting the elements every `out` produced in each
    run"), their (type, id) -> version map (`Context.diff_versions`,
    independent of what verbosity the `out` actually displayed), the
    files read, and any warnings. Restores `SNAPSHOT` afterwards even on
    error."""
    from . import planner as planner_mod

    token = catalog.SNAPSHOT.set(t)
    try:
        ctx = planner_mod.run_program_body(con, manifest, program, diff_mode=True)
    finally:
        catalog.SNAPSHOT.reset(token)
    return _PassResult(ctx.elements, ctx.diff_versions, ctx.files_read, ctx.warnings)


class DiffRunResult:
    def __init__(self, elements: list[dict], files_read: int, warnings: list[str]):
        self.elements = elements
        self.files_read = files_read
        self.warnings = warnings


def run_diff_program(con, manifest: catalog.Manifest, program) -> DiffRunResult:
    """The `[diff:]`/`[adiff:]` executor (section 3.2): runs `program`
    twice, `SNAPSHOT = a` then `= b` (`b` = now when absent), and turns
    the two element lists into the action list. `augmented` (adiff) does
    the extra raw-state lookups documented on `build_diff_actions`.
    Assumes `planner.check_settings` already confirmed the manifest has
    history and that exactly one of `settings.diff`/`settings.adiff` is
    set."""
    settings = program.settings
    augmented = settings.adiff is not None
    a_str, b_str = settings.adiff if augmented else settings.diff
    t_a = parse_date(a_str)
    t_b = resolve_optional_date(b_str)

    warnings: list[str] = []
    for t in (t_a, t_b):
        remark = history_remark(manifest, t)
        if remark and remark not in warnings:
            warnings.append(remark)

    pass_a = run_diff_pass(con, manifest, program, t_a)
    pass_b = run_diff_pass(con, manifest, program, t_b)
    for w in pass_a.warnings + pass_b.warnings:
        if w not in warnings:
            warnings.append(w)

    actions = build_diff_actions(con, manifest, pass_a, pass_b, t_a, t_b, augmented)
    return DiffRunResult(elements=actions, files_read=pass_a.files_read + pass_b.files_read, warnings=warnings)


def build_diff_actions(
    con,
    manifest: catalog.Manifest,
    pass_a: "_PassResult",
    pass_b: "_PassResult",
    t_a: datetime,
    t_b: datetime,
    augmented: bool,
) -> list[dict]:
    """The action list of docs/m4-contracts.md section 3.2: `{"action":
    "create"|"modify"|"delete", "type", "id", "old": el|None, "new":
    el|None}`, ordered as `out` orders elements at `b` (creates/modifies)
    with deletes in the order of `a`.

    "different (version, minor)" (section 3.2) is tested primarily via
    each pass's `versions` map (`Context.diff_versions`, populated from
    the underlying row regardless of what verbosity the query's `out`
    actually displayed -- an `out ids`/`out count` pair that both reduce
    to a bare `{"type", "id"}` must still show a real version change as a
    `modify`, confirmed against probe `adiff_count_xml`/`adiff_ids_xml`:
    every element the query still matches at both `a` and `b` appears as
    a `modify` action there, even though its id-only rendering is
    identical either side). The rendered dicts are still compared as a
    fallback/tiebreaker, so a same-version minor/geometry-only change
    that `out geom`/`out meta` actually shows still counts as a change
    even when a `version` happens to be unknown on one side (e.g. a
    verbosity that never queried it -- shouldn't happen since
    `render.build_elements` always returns it, but degrade safely).

    `augmented` (adiff): a one-sided id also gets a raw, filter-ignoring
    lookup at the *other* endpoint (section 3.2 / probe `diff_xml` vs
    `adiff_xml`): an id only in `b` whose raw state already existed at `a`
    becomes a `modify` with a minimal `old` stub instead of a `create`; an
    id only in `a` whose raw state still exists at `b` gets a minimal
    `new` stub attached to its `delete` action (showing whether it was
    truly deleted -- `visible=false` -- or just fell out of the filtered
    result, `visible=true`). Plain `diff` does neither lookup."""
    by_key_a: dict[tuple, dict] = {}
    for el in pass_a.elements:
        by_key_a[(el.get("type"), el.get("id"))] = el
    by_key_b: dict[tuple, dict] = {}
    for el in pass_b.elements:
        by_key_b[(el.get("type"), el.get("id"))] = el

    def changed(key: tuple, old: dict, new: dict) -> bool:
        va, vb = pass_a.versions.get(key), pass_b.versions.get(key)
        if va is not None or vb is not None:
            if va != vb:
                return True
        return old != new

    actions: list[dict] = []
    seen: set[tuple] = set()
    for el in pass_b.elements:
        key = (el.get("type"), el.get("id"))
        if key in seen:
            continue
        seen.add(key)
        old = by_key_a.get(key)
        if old is None:
            if augmented:
                raw = raw_state_row(con, manifest, key[0], key[1], t_a)
                if raw is not None:
                    actions.append({"action": "modify", "type": key[0], "id": key[1],
                                     "old": stub_element(key[0], raw, show_visible=False), "new": el})
                    continue
            actions.append({"action": "create", "type": key[0], "id": key[1], "new": el})
        elif changed(key, old, el):
            actions.append({"action": "modify", "type": key[0], "id": key[1], "old": old, "new": el})

    seen_a: set[tuple] = set()
    for el in pass_a.elements:
        key = (el.get("type"), el.get("id"))
        if key in seen_a:
            continue
        seen_a.add(key)
        if key in by_key_b:
            continue
        action: dict = {"action": "delete", "type": key[0], "id": key[1], "old": el}
        if augmented:
            raw = raw_state_row(con, manifest, key[0], key[1], t_b)
            if raw is not None:
                action["new"] = stub_element(key[0], raw, show_visible=True)
        actions.append(action)
    return actions
