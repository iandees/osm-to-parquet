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

from . import catalog, recurse, render, setops, sources
from .schema import empty_set_sql

BBox = tuple[float, float, float, float]

_REJECTED_FILTERS = (AroundFilter, PolyFilter, AreaFilter, PivotFilter, NewerFilter, ChangedFilter, UserFilter, UidFilter, IfFilter)


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
        if isinstance(f, _REJECTED_FILTERS):
            raise UnsupportedError(f"filter {type(f).__name__} is not supported in M0")


def _recurse_filter_candidate_ids(ctx: Context, rf: RecurseFilter, types: list[str]) -> dict[str, list[int]]:
    source_table = f"set_{rf.set_name}"
    _require_set(ctx, rf.set_name)
    type_set = set(types)
    if rf.kind == "w":
        ids = recurse.forward_new_ids(ctx.con, source_table, restrict_source_types={"way"})
    elif rf.kind == "r":
        ids = recurse.forward_new_ids(ctx.con, source_table, restrict_source_types={"relation"}, role=rf.role)
    elif rf.kind == "bn":
        ids = recurse.backward_new_ids(ctx.con, ctx.manifest, source_table, restrict_source_types={"node"}, role=rf.role)
    elif rf.kind == "bw":
        ids = recurse.backward_new_ids(ctx.con, ctx.manifest, source_table, restrict_source_types={"way"}, role=rf.role)
    elif rf.kind == "br":
        ids = recurse.backward_new_ids(ctx.con, ctx.manifest, source_table, restrict_source_types={"relation"}, role=rf.role)
    else:
        raise UnsupportedError(f"recurse filter ({rf.kind}) is not supported")
    return {t: v for t, v in ids.items() if t in type_set and v}


def _require_set(ctx: Context, name: str) -> None:
    exists = ctx.con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [f"set_{name}"]
    ).fetchone()[0]
    if not exists:
        raise RuntimeQueryError(f'runtime error: set ".{name}" has not been set before')


def execute_query(ctx: Context, q: Query) -> None:
    if q.types == ["area"] or "area" in q.types:
        raise UnsupportedError("area queries are not supported in M0")
    _check_unsupported_filters(q)

    types = q.types
    bbox = _bbox_for(q, ctx.global_bbox)
    ids = _ids_for(q)
    tag_filters = _tag_filters_for(q)
    recurse_filters = _recurse_filters_for(q)

    if recurse_filters:
        rf = recurse_filters[0]
        candidates = _recurse_filter_candidate_ids(ctx, rf, types)
        selects = []
        for t in types:
            t_ids = candidates.get(t) or []
            if not t_ids:
                continue
            sql, nfiles = sources.build_byid_select(ctx.manifest, t, t_ids, tag_filters, ctx.promoted_keys)
            ctx.files_read += nfiles
            selects.append(sql)
        base_select = "\nUNION ALL\n".join(selects) if selects else empty_set_sql()
    elif q.input_sets:
        for name in q.input_sets:
            _require_set(ctx, name)
        base_select = sources.build_from_set_select(q.input_sets, types, tag_filters, ids, bbox)
    else:
        if bbox is None and not ids:
            ctx.warnings.append(
                f"query on {types} has no bbox and no ids; scanning all cells for an extract-sized answer"
            )
        selects = []
        for t in types:
            if ids and bbox is None:
                sql, nfiles = sources.build_byid_select(ctx.manifest, t, ids, tag_filters, ctx.promoted_keys)
            else:
                builder = sources.SPATIAL_BUILDERS[t]
                sql, nfiles = builder(ctx.manifest, bbox, tag_filters, ids, ctx.promoted_keys)
            ctx.files_read += nfiles
            selects.append(sql)
        base_select = "\nUNION ALL\n".join(selects)

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
    elif isinstance(stmt, (IsIn, MapToArea, Foreach, If)):
        raise UnsupportedError(f"{type(stmt).__name__} is not supported in M0")
    else:
        raise UnsupportedError(f"unrecognized statement {type(stmt).__name__}")


def run_program(con, manifest: catalog.Manifest, program) -> Context:
    check_settings(program.settings)
    promoted_keys = set(manifest.promoted_keys)
    ctx = Context(con=con, manifest=manifest, promoted_keys=promoted_keys, global_bbox=program.settings.bbox)
    setops.ensure_empty_set(con, "_")
    for stmt in program.statements:
        execute_statement(ctx, stmt)
    return ctx
