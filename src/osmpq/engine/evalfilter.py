"""``(if:)`` filter hook and the ``foreach``/``if`` statement hooks
(docs/m3-contracts.md section 5.3), wiring ``osmpq.ql.evaluator`` +
``osmpq.engine.evalsql`` into the planner's extension points.

Statement bodies recurse through ``planner.execute_statement``; ``planner``
is imported lazily inside each function (not at module level) because
``planner`` is what imports this module in the first place (via
``hooks.HOOK_MODULES``, lazily on the first run) -- a top-level import here
would be circular.
"""
from __future__ import annotations

from osmpq.errors import RuntimeQueryError
from osmpq.ql import evaluator
from osmpq.ql.ast import Foreach, If, IfFilter

from . import evalsql, hooks
from .schema import CANONICAL_COLUMNS


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _require_set(con, name: str) -> None:
    exists = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [f"set_{name}"]
    ).fetchone()[0]
    if not exists:
        raise RuntimeQueryError(f'runtime error: set ".{name}" has not been set before')


class _IfFilterHook:
    def implied_bbox(self, ctx, q, f: IfFilter):
        return None

    def predicate(self, ctx, q, f: IfFilter, alias: str) -> str:
        expr = evaluator.parse(f.expression)
        return evalsql.compile_predicate(expr, alias, ctx.promoted_keys)


def _run_foreach(ctx, stmt: Foreach) -> None:
    from . import planner  # avoid import cycle (see module docstring)
    from . import setops

    src = f"set_{stmt.input_set}"
    _require_set(ctx.con, stmt.input_set)
    # Snapshot the input set before iterating: the body may itself write to
    # `stmt.input_set` (e.g. via a nested statement targeting the same
    # name) without perturbing the elements we still have to visit.
    snapshot = ctx.fresh_name("foreach")
    ctx.con.execute(f"CREATE OR REPLACE TEMP TABLE set_{snapshot} AS SELECT * FROM {src} ORDER BY type, id")
    rows = ctx.con.execute(f"SELECT type, id FROM set_{snapshot} ORDER BY type, id").fetchall()
    cols = ", ".join(CANONICAL_COLUMNS)
    for etype, eid in rows:
        setops.materialize(
            ctx.con,
            stmt.output_set,
            f"SELECT {cols} FROM set_{snapshot} WHERE type = {_sql_str(etype)} AND id = {int(eid)}",
        )
        for body_stmt in stmt.body:
            planner.execute_statement(ctx, body_stmt)


def _run_if(ctx, stmt: If) -> None:
    from . import planner  # avoid import cycle (see module docstring)

    expr = evaluator.parse(stmt.condition)
    value = evalsql.evaluate_set(ctx, expr, "_")
    branch = stmt.then if evalsql.truthy(value) else stmt.otherwise
    for body_stmt in branch:
        planner.execute_statement(ctx, body_stmt)


hooks.register_filter(IfFilter, _IfFilterHook())
hooks.register_statement(Foreach, _run_foreach)
hooks.register_statement(If, _run_if)
