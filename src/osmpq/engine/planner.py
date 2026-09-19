"""Translate each tier-1 statement (contract section 8) into SQL executed
against the run's DuckDB connection, materializing `set_<name>` temp tables
per docs/design.md section 5's sketches.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import count
from typing import Optional

from osmpq.errors import RuntimeQueryError, UnsupportedError
from osmpq.ql.ast import (
    AreaFilter,
    AroundFilter,
    BboxFilter,
    ChangedFilter,
    Difference,
    Foreach,
    IdFilter,
    If,
    IfFilter,
    IsIn,
    Item,
    MapToArea,
    NewerFilter,
    Out,
    PivotFilter,
    PolyFilter,
    Query,
    Recurse,
    RecurseFilter,
    Settings,
    Statement,
    TagFilter,
    UidFilter,
    Union,
    Unsupported,
    UserFilter,
)

from . import catalog, hooks, idset, recurse, render, setops, sources
from .schema import empty_set_sql

BBox = tuple[float, float, float, float]

_CORE_FILTERS = (TagFilter, BboxFilter, IdFilter, RecurseFilter)
_hooks_loaded = False


def _load_hooks() -> None:
    """Import the tier-2 modules that register filter/statement hooks
    (hooks.HOOK_MODULES). A module that does not exist yet is skipped: its
    filters/statements then raise UnsupportedError as before."""
    global _hooks_loaded
    if _hooks_loaded:
        return
    import importlib

    for name in hooks.HOOK_MODULES:
        try:
            importlib.import_module(name)
        except ModuleNotFoundError as e:
            if e.name != name:
                raise
    _hooks_loaded = True


@dataclass
class Context:
    con: object
    manifest: catalog.Manifest
    promoted_keys: set
    global_bbox: Optional[BBox]
    elements: list = field(default_factory=list)
    files_read: int = 0
    warnings: list = field(default_factory=list)
    _counter: "count" = field(default_factory=lambda: count(1))

    def fresh_name(self, prefix: str) -> str:
        return f"__{prefix}_{next(self._counter)}"


def check_settings(settings: Settings) -> None:
    if settings.out_format == "csv":
        raise UnsupportedError("[out:csv] is not supported in M0")
    if settings.date is not None:
        raise UnsupportedError("[date:] (attic) is not supported in M0")
    if settings.diff is not None:
        raise UnsupportedError("[diff:] is not supported in M0")
    if settings.adiff is not None:
        raise UnsupportedError("[adiff:] is not supported in M0")


def _bbox_for(q: Query, global_bbox: Optional[BBox]) -> Optional[BBox]:
    for f in q.filters:
        if isinstance(f, BboxFilter):
            return (f.south, f.west, f.north, f.east)
    return global_bbox


def _effective_bbox(ctx: "Context", q: Query) -> Optional[BBox]:
    """Explicit/global bbox intersected with every hooked filter's implied
    bbox (hooks.FilterHook.implied_bbox). Used for cell and row-group
    selection only; the hooks' predicates do the exact tests."""
    bbox = _bbox_for(q, ctx.global_bbox)
    for f in q.filters:
        hook = hooks.FILTER_HOOKS.get(type(f))
        if hook is None:
            continue
        bbox = hooks.intersect_bbox(bbox, hook.implied_bbox(ctx, q, f))
    return bbox


def _hook_predicates(ctx: "Context", q: Query, alias: str) -> list[str]:
    preds = []
    for f in q.filters:
        hook = hooks.FILTER_HOOKS.get(type(f))
        if hook is not None:
            preds.append(hook.predicate(ctx, q, f, alias))
    return preds


def _ids_for(q: Query) -> Optional[list[int]]:
    for f in q.filters:
        if isinstance(f, IdFilter):
            return list(f.ids)
    return None


def _tag_filters_for(q: Query) -> list[TagFilter]:
    return [f for f in q.filters if isinstance(f, TagFilter)]


def _recurse_filters_for(q: Query) -> list[RecurseFilter]:
    return [f for f in q.filters if isinstance(f, RecurseFilter)]


def _check_unsupported_filters(q: Query) -> None:
    for f in q.filters:
        if not isinstance(f, _CORE_FILTERS) and type(f) not in hooks.FILTER_HOOKS:
            raise UnsupportedError(f"filter {type(f).__name__} is not supported")


def _recurse_filter_ids_table(ctx: Context, rf: RecurseFilter) -> tuple[Optional[str], int]:
    """One hop for an inline recurse filter ((w)/(r)/(bn)/(bw)/(br)):
    returns (a fresh TEMP TABLE(type, id, cell) name, files read), or
    (None, 0) if it produced nothing. See recurse.py: this never inlines
    ids as a SQL literal list or fetches them into Python. The backward
    kinds (bn/bw/br) may read way spatial files (design.md 3.1); the
    forward kinds never read files here (any reads happen when the
    caller hydrates the ids)."""
    source_table = f"set_{rf.set_name}"
    _require_set(ctx, rf.set_name)
    if rf.kind == "w":
        return recurse.forward_new_ids_table(ctx.con, source_table, restrict_source_types={"way"}), 0
    elif rf.kind == "r":
        return recurse.forward_new_ids_table(ctx.con, source_table, restrict_source_types={"relation"}, role=rf.role), 0
    elif rf.kind == "bn":
        return recurse.backward_new_ids_table(ctx.con, ctx.manifest, source_table, restrict_source_types={"node"}, role=rf.role)
    elif rf.kind == "bw":
        return recurse.backward_new_ids_table(ctx.con, ctx.manifest, source_table, restrict_source_types={"way"}, role=rf.role)
    elif rf.kind == "br":
        return recurse.backward_new_ids_table(ctx.con, ctx.manifest, source_table, restrict_source_types={"relation"}, role=rf.role)
    else:
        raise UnsupportedError(f"recurse filter ({rf.kind}) is not supported")


def _require_set(ctx: Context, name: str) -> None:
    exists = ctx.con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [f"set_{name}"]
    ).fetchone()[0]
    if not exists:
        raise RuntimeQueryError(f'runtime error: set ".{name}" has not been set before')


def execute_query(ctx: Context, q: Query) -> None:
    if q.types == ["area"] or "area" in q.types:
        if hooks.AREA_QUERY_HOOK:
            hooks.AREA_QUERY_HOOK[0](ctx, q)
            return
        raise UnsupportedError("area queries are not supported")
    _check_unsupported_filters(q)

    types = q.types
    bbox = _effective_bbox(ctx, q)
    if hooks.is_empty_bbox(bbox):
        setops.materialize(ctx.con, q.output_set, empty_set_sql())
        return
    ids = _ids_for(q)
    tag_filters = _tag_filters_for(q)
    recurse_filters = _recurse_filters_for(q)

    if recurse_filters:
        rf = recurse_filters[0]
        id_table, nfiles_hop = _recurse_filter_ids_table(ctx, rf)
        ctx.files_read += nfiles_hop
        if id_table is None:
            base_select = empty_set_sql()
        elif rf.kind == "w" and "node" in types:
            # design.md 3.1: `(w)`'s ids are the node refs of ways in
            # `rf.set_name`, so they lie inside those ways' own bboxes --
            # the same spatially-scoped hydration as the `>` forward hop
            # (recurse.build_forward_one_hop), instead of a byid scan.
            bbox_selects = [
                f"SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM set_{rf.set_name} WHERE type = 'way'"
            ]
            base_select, nfiles = sources.build_node_hydrate_via_bbox_select(
                ctx.con, ctx.manifest, id_table, bbox_selects, ctx.promoted_keys, tag_filters=tag_filters
            )
            ctx.files_read += nfiles
            if base_select is None:
                base_select = empty_set_sql()
        elif rf.kind == "r" and "way" in types:
            # design.md 3.1 item 3: `(r)`'s way ids are a relation's member
            # ways, so they lie inside that relation's own bbox -- resolve
            # them from the spatial way files of the cells covering it
            # (same helper `>`'s forward hop uses for member ways) instead
            # of a byid scan. Other requested types (node/relation members)
            # still go through the generic byid/cell hydration below.
            way_ids_tbl = idset.fresh_table_name("rfilterwayids")
            ctx.con.execute(
                f"CREATE TEMP TABLE {way_ids_tbl} AS SELECT DISTINCT id FROM {id_table} WHERE type = 'way'"
            )
            bbox_selects = [
                f"SELECT xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM set_{rf.set_name} WHERE type = 'relation'"
            ]
            way_sql, nfiles_w = sources.build_way_hydrate_via_bbox_select(
                ctx.con, ctx.manifest, way_ids_tbl, bbox_selects, ctx.promoted_keys, tag_filters=tag_filters
            )
            ctx.files_read += nfiles_w
            parts = [way_sql] if way_sql else []
            other_types = set(types) - {"way"}
            if other_types:
                other_sql, nfiles_o = recurse.hydrate_ids_table(
                    ctx.con, ctx.manifest, id_table, tag_filters, ctx.promoted_keys, only_types=other_types
                )
                ctx.files_read += nfiles_o
                if other_sql:
                    parts.append(other_sql)
            base_select = "\nUNION ALL\n".join(parts) if parts else empty_set_sql()
        else:
            base_select, nfiles = recurse.hydrate_ids_table(
                ctx.con, ctx.manifest, id_table, tag_filters, ctx.promoted_keys, only_types=set(types)
            )
            ctx.files_read += nfiles
            if base_select is None:
                base_select = empty_set_sql()
    elif q.input_sets:
        for name in q.input_sets:
            _require_set(ctx, name)
        base_select = sources.build_from_set_select(ctx.con, q.input_sets, types, tag_filters, ids, bbox)
    else:
        if bbox is None and not ids:
            ctx.warnings.append(
                f"query on {types} has no bbox and no ids; scanning all cells for an extract-sized answer"
            )
        selects = []
        for t in types:
            if ids and bbox is None:
                sql, nfiles = sources.build_byid_select(ctx.con, ctx.manifest, t, ids, tag_filters, ctx.promoted_keys)
            else:
                builder = sources.SPATIAL_BUILDERS[t]
                sql, nfiles = builder(ctx.con, ctx.manifest, bbox, tag_filters, ids, ctx.promoted_keys)
            ctx.files_read += nfiles
            selects.append(sql)
        base_select = "\nUNION ALL\n".join(selects)

    preds = _hook_predicates(ctx, q, "__q")
    if preds:
        base_select = f"SELECT * FROM ({base_select}) __q WHERE " + " AND ".join(f"({p})" for p in preds)
    setops.materialize(ctx.con, q.output_set, base_select)


def execute_recurse(ctx: Context, r: Recurse) -> None:
    _require_set(ctx, r.input_set)
    src = f"set_{r.input_set}"
    if r.kind == ">":
        sql, nfiles = recurse.build_forward_one_hop(ctx.con, ctx.manifest, src, ctx.promoted_keys)
    elif r.kind == "<":
        sql, nfiles = recurse.build_backward_one_hop(ctx.con, ctx.manifest, src, ctx.promoted_keys)
    elif r.kind == ">>":
        sql, nfiles = recurse.recurse_transitive(ctx.con, ctx.manifest, src, ctx.promoted_keys, "forward")
    elif r.kind == "<<":
        sql, nfiles = recurse.recurse_transitive(ctx.con, ctx.manifest, src, ctx.promoted_keys, "backward")
    else:
        raise UnsupportedError(f"recurse operator {r.kind!r} is not supported")
    ctx.files_read += nfiles
    setops.materialize(ctx.con, r.output_set, sql)


def execute_item(ctx: Context, it: Item) -> None:
    _require_set(ctx, it.input_set)
    setops.materialize(ctx.con, it.output_set, f"SELECT * FROM set_{it.input_set}")


def _output_set_name(stmt: Statement) -> str:
    name = getattr(stmt, "output_set", None)
    if name is None:
        raise UnsupportedError(f"{type(stmt).__name__} cannot appear inside a union/difference block")
    return name


def _execute_and_capture(ctx: Context, stmt: Statement) -> str:
    execute_statement(ctx, stmt)
    produced = _output_set_name(stmt)
    snapshot = ctx.fresh_name("part")
    ctx.con.execute(f"CREATE OR REPLACE TEMP TABLE set_{snapshot} AS SELECT * FROM set_{produced}")
    return snapshot


def execute_union(ctx: Context, u: Union) -> None:
    parts = [_execute_and_capture(ctx, stmt) for stmt in u.statements]
    if not parts:
        setops.materialize(ctx.con, u.output_set, empty_set_sql())
        return
    setops.materialize(ctx.con, u.output_set, setops.union_sql(parts))


def execute_difference(ctx: Context, d: Difference) -> None:
    a = _execute_and_capture(ctx, d.first)
    b = _execute_and_capture(ctx, d.second)
    setops.materialize(ctx.con, d.output_set, setops.difference_sql(a, b))


def execute_out(ctx: Context, o: Out) -> None:
    _require_set(ctx, o.input_set)
    if o.geom_bbox is not None:
        raise UnsupportedError("out geom(s,w,n,e) (clipped geometry) is not supported in M0")
    elements, _extra = render.build_elements(ctx.con, ctx.manifest, o.input_set, o)
    ctx.elements.extend(elements)


def execute_statement(ctx: Context, stmt: Statement) -> None:
    if isinstance(stmt, Query):
        execute_query(ctx, stmt)
    elif isinstance(stmt, Union):
        execute_union(ctx, stmt)
    elif isinstance(stmt, Difference):
        execute_difference(ctx, stmt)
    elif isinstance(stmt, Recurse):
        execute_recurse(ctx, stmt)
    elif isinstance(stmt, Item):
        execute_item(ctx, stmt)
    elif isinstance(stmt, Out):
        execute_out(ctx, stmt)
    elif isinstance(stmt, Unsupported):
        raise UnsupportedError(f"{stmt.keyword} is not supported in M0")
    elif type(stmt) in hooks.STATEMENT_HOOKS:
        hooks.STATEMENT_HOOKS[type(stmt)](ctx, stmt)
    elif isinstance(stmt, (IsIn, MapToArea, Foreach, If)):
        raise UnsupportedError(f"{type(stmt).__name__} is not supported")
    else:
        raise UnsupportedError(f"unrecognized statement {type(stmt).__name__}")


def run_program(con, manifest: catalog.Manifest, program) -> Context:
    _load_hooks()
    check_settings(program.settings)
    promoted_keys = set(manifest.promoted_keys)
    ctx = Context(con=con, manifest=manifest, promoted_keys=promoted_keys, global_bbox=program.settings.bbox)
    setops.ensure_empty_set(con, "_")
    for stmt in program.statements:
        execute_statement(ctx, stmt)
    return ctx
