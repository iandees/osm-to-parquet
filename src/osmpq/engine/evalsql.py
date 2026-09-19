"""Compile ``osmpq.ql.evaluator`` expression ASTs against the canonical
row columns (docs/m3-contracts.md section 5.2).

Two entry points, matching the two evaluator call sites:

* ``compile_element(expr, alias, promoted_keys) -> SQL`` -- used by the
  ``(if:)`` filter (``evalfilter.py``): compiles an *element-scoped*
  expression into a single SQL text expression, evaluated once per
  candidate row inside the query's WHERE clause. Never touches
  ``ctx.con``; raises ``UnsupportedError`` if the expression contains any
  set-scoped construct (``count(...)``, ``.a.count(...)``, or an
  aggregator), since those need a whole set materialized, not a row.

* ``evaluate_set(ctx, expr, set_name) -> str`` -- used by ``if``
  (``evalfilter.py``): evaluates the *whole* expression once, in Python,
  against ``set_<set_name>``. Every ``count(...)``/aggregator subtree runs
  its own SQL query over the named set; a bare element-scoped function
  used outside an aggregator is a ``RuntimeQueryError`` (Overpass requires
  wrapping it in ``u(...)`` there).

Overpass typing rules (section 5.2): every value is a string; a comparison
is numeric when *both* sides parse as a number, else lexical; ``""`` and
``"0"`` are false, everything else is true; a missing tag reads as ``""``.
Numbers are formatted the way Overpass prints them: an integral value with
no decimal point (``"3"``, not ``"3.0"``), anything else via Python's
default ``float`` formatting. A value that fails to parse as a number
(``number()``, arithmetic on a non-numeric string) becomes IEEE-754 NaN,
formatted as the literal string ``"nan"`` -- this mirrors real Overpass's
``number()`` semantics, but note the *comparison* rule above is the one
mandated by the contract (numeric only when both sides parse; NaN never
enters a comparison unless one side already used ``number()``/arithmetic).

``length()`` (ways, meters): DuckDB's spatial extension expects
``ST_Length_Spheroid`` to be fed *(lat, lon)*-ordered points -- verified
empirically (see tests/test_evaluator.py::test_length_axis_order_empirical):
feeding it the standard *(lon, lat)* WKT order used everywhere else in this
codebase (our ``geometry`` column, GeoParquet's own convention) silently
returns NaN, while flipping the coordinates first
(``ST_FlipCoordinates``) reproduces an independent haversine reference to
well within 0.2%. So: ``ST_Length_Spheroid(ST_FlipCoordinates(geometry))``.
A NULL ``geometry`` (byid path, relations, or any non-way) reads as 0.
"""
from __future__ import annotations

from osmpq.errors import RuntimeQueryError, UnsupportedError
from osmpq.ql import evaluator as ev


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _tag_value_sql(key: str, alias: str, promoted_keys: set[str]) -> str:
    """Raw (possibly NULL) SQL for a tag's value, before the missing-tag-is-''
    coalesce -- needed by is_tag() to distinguish absent from empty.

    Unlike ``tagsql.tag_filter_sql`` (which compiles against a *source*
    file's own rows, where a promoted key is its own physical column),
    ``compile_element``/``evaluate_set`` only ever see canonical
    ``set_<name>``/query-result rows (``osmpq.engine.schema``), which carry
    every tag -- promoted or not -- solely in the ``tags`` MAP column; the
    per-key promoted columns are a source-file storage detail that never
    survives into a materialized set. So this always reads through
    ``tags``; ``promoted_keys`` is accepted (and threaded through by every
    caller) only to keep this compiler's signature symmetric with
    ``tagsql``'s, per docs/m3-contracts.md section 5.2."""
    return f"{alias}.tags[{_sql_str(key)}]"


# -- shared numeric/truthiness SQL fragments --------------------------------


def _is_num_sql(value_sql: str) -> str:
    return f"try_cast({value_sql} AS DOUBLE) IS NOT NULL"


def _num_sql(value_sql: str) -> str:
    return f"try_cast({value_sql} AS DOUBLE)"


def _format_num_sql(double_sql: str) -> str:
    """SQL: format a DOUBLE the way Overpass prints a number (no trailing
    '.0' for integral values); NULL (NaN-from-try_cast, or a NULL operand)
    becomes the literal 'nan'."""
    return (
        "(CASE "
        f"WHEN ({double_sql}) IS NULL THEN 'nan' "
        f"WHEN ({double_sql}) = floor({double_sql}) AND abs({double_sql}) < 1e15 "
        f"THEN CAST(CAST({double_sql} AS BIGINT) AS VARCHAR) "
        f"ELSE CAST({double_sql} AS VARCHAR) END)"
    )


def _truthy_sql(value_sql: str) -> str:
    return f"({value_sql} IS NOT NULL AND {value_sql} != '' AND {value_sql} != '0')"


def _bool_to_value_sql(bool_sql: str) -> str:
    return f"(CASE WHEN {bool_sql} THEN '1' ELSE '0' END)"


_CMP_OP_SQL = {"<": "<", "<=": "<=", ">": ">", ">=": ">=", "==": "=", "!=": "!="}


# -- compile_element (SQL) ---------------------------------------------------


def compile_predicate(expr, alias: str, promoted_keys: set[str]) -> str:
    """Compile an element-scoped expression to a SQL BOOLEAN (its
    truthiness, per the Overpass rule: "" and "0" are false), for use as a
    row filter predicate -- e.g. the ``(if:)`` filter hook."""
    return _truthy_sql(compile_element(expr, alias, promoted_keys))


def compile_element(expr, alias: str, promoted_keys: set[str]) -> str:
    """Compile an element-scoped evaluator expression to a SQL text
    expression (always VARCHAR-typed, an Overpass "value") over a row
    aliased ``alias``. Raises ``UnsupportedError`` for any set-scoped
    construct (count(...), .a.count(...), aggregators)."""
    if isinstance(expr, ev.Num):
        return _sql_str(_format_py_num(expr.value))
    if isinstance(expr, ev.Str):
        return _sql_str(expr.value)
    if isinstance(expr, ev.TagAccess):
        return f"COALESCE({_tag_value_sql(expr.key, alias, promoted_keys)}, '')"
    if isinstance(expr, (ev.SetCount, ev.NamedSetCount, ev.Aggregate)):
        raise UnsupportedError("set-scoped count/aggregate functions are not supported in (if:)")
    if isinstance(expr, ev.ElementCall):
        return _compile_element_call(expr, alias, promoted_keys)
    if isinstance(expr, ev.Unary):
        inner = compile_element(expr.expr, alias, promoted_keys)
        if expr.op == "!":
            return _bool_to_value_sql(f"NOT {_truthy_sql(inner)}")
        return _format_num_sql(f"-{_num_sql(inner)}")
    if isinstance(expr, ev.BinOp):
        return _compile_binop(expr, alias, promoted_keys)
    if isinstance(expr, ev.Ternary):
        cond = compile_element(expr.cond, alias, promoted_keys)
        then = compile_element(expr.then, alias, promoted_keys)
        otherwise = compile_element(expr.otherwise, alias, promoted_keys)
        return f"(CASE WHEN {_truthy_sql(cond)} THEN {then} ELSE {otherwise} END)"
    raise UnsupportedError(f"evaluator expression {type(expr).__name__} is not supported")


def _compile_binop(expr: "ev.BinOp", alias: str, promoted_keys: set[str]) -> str:
    a = compile_element(expr.left, alias, promoted_keys)
    b = compile_element(expr.right, alias, promoted_keys)
    op = expr.op
    if op in ("+", "-", "*", "/"):
        an, bn = _num_sql(a), _num_sql(b)
        if op == "+":
            return _format_num_sql(f"({an} + {bn})")
        if op == "-":
            return _format_num_sql(f"({an} - {bn})")
        if op == "*":
            return _format_num_sql(f"({an} * {bn})")
        return _format_num_sql(f"(CASE WHEN {bn} = 0 THEN NULL ELSE {an} / {bn} END)")
    if op in _CMP_OP_SQL:
        both_numeric = f"({_is_num_sql(a)} AND {_is_num_sql(b)})"
        num_cmp = f"({_num_sql(a)} {_CMP_OP_SQL[op]} {_num_sql(b)})"
        lex_cmp = f"({a} {_CMP_OP_SQL[op]} {b})"
        return _bool_to_value_sql(f"(CASE WHEN {both_numeric} THEN {num_cmp} ELSE {lex_cmp} END)")
    if op == "&&":
        return _bool_to_value_sql(f"({_truthy_sql(a)} AND {_truthy_sql(b)})")
    if op == "||":
        return _bool_to_value_sql(f"({_truthy_sql(a)} OR {_truthy_sql(b)})")
    raise UnsupportedError(f"evaluator operator {op!r} is not supported")


def _compile_element_call(expr: "ev.ElementCall", alias: str, promoted_keys: set[str]) -> str:
    name = expr.name
    if name == "id":
        return f"CAST({alias}.id AS VARCHAR)"
    if name == "type":
        return f"COALESCE({alias}.type, '')"
    if name == "version":
        return f"COALESCE(CAST({alias}.version AS VARCHAR), '')"
    if name == "timestamp":
        return f"COALESCE(strftime({alias}.\"timestamp\", '%Y-%m-%dT%H:%M:%SZ'), '')"
    if name == "changeset":
        return f"COALESCE(CAST({alias}.changeset AS VARCHAR), '')"
    if name == "uid":
        return f"COALESCE(CAST({alias}.uid AS VARCHAR), '')"
    if name == "user":
        return f"COALESCE({alias}.\"user\", '')"
    if name == "count_tags":
        return f"CAST(COALESCE(cardinality({alias}.tags), 0) AS VARCHAR)"
    if name == "count_members":
        return (
            "CAST(CASE "
            f"WHEN {alias}.type = 'way' THEN COALESCE(array_length({alias}.refs), 0) "
            f"WHEN {alias}.type = 'relation' THEN COALESCE(array_length({alias}.members), 0) "
            "ELSE 0 END AS VARCHAR)"
        )
    if name == "count_distinct_members":
        return (
            "CAST(CASE "
            f"WHEN {alias}.type = 'way' THEN COALESCE(array_length(list_distinct({alias}.refs)), 0) "
            f"WHEN {alias}.type = 'relation' THEN COALESCE(array_length(list_distinct("
            f"list_transform({alias}.members, __m -> __m.type || ':' || CAST(__m.ref AS VARCHAR)))), 0) "
            "ELSE 0 END AS VARCHAR)"
        )
    if name == "count_by_role":
        role = compile_element(expr.args[0], alias, promoted_keys)
        return (
            f"CAST(CASE WHEN {alias}.type = 'relation' THEN COALESCE(array_length("
            f"list_filter({alias}.members, __m -> __m.role = ({role}))), 0) ELSE 0 END AS VARCHAR)"
        )
    if name == "is_closed":
        return _bool_to_value_sql(
            f"({alias}.type = 'way' AND array_length({alias}.refs) >= 2 "
            f"AND list_first({alias}.refs) = list_last({alias}.refs))"
        )
    if name == "length":
        return _format_num_sql(
            f"COALESCE(CASE WHEN {alias}.type = 'way' THEN "
            f"ST_Length_Spheroid(ST_FlipCoordinates({alias}.geometry)) ELSE NULL END, 0)"
        )
    if name == "lat":
        return _format_num_sql(f"CASE WHEN {alias}.type = 'node' THEN {alias}.lat_e7 / 1e7 ELSE NULL END")
    if name == "lon":
        return _format_num_sql(f"CASE WHEN {alias}.type = 'node' THEN {alias}.lon_e7 / 1e7 ELSE NULL END")
    if name == "is_tag":
        key = expr.args[0]
        if not isinstance(key, ev.Str):
            raise UnsupportedError("is_tag(...) requires a literal string key")
        return _bool_to_value_sql(f"{_tag_value_sql(key.value, alias, promoted_keys)} IS NOT NULL")
    if name == "number":
        val = compile_element(expr.args[0], alias, promoted_keys)
        return _format_num_sql(_num_sql(val))
    if name == "is_number":
        val = compile_element(expr.args[0], alias, promoted_keys)
        return _bool_to_value_sql(_is_num_sql(val))
    raise UnsupportedError(f"evaluator function {name}() is not supported")


def _format_py_num(v: float) -> str:
    if v != v:  # NaN
        return "nan"
    if v == int(v) and abs(v) < 1e15:
        return str(int(v))
    return repr(v)


# -- evaluate_set (Python-side, executes SQL for set-scoped pieces) --------

_TYPE_FILTER_SQL = {
    "nodes": "type = 'node'",
    "ways": "type = 'way'",
    "relations": "type = 'relation'",
    "areas": "type = 'area'",
    "nwr": "type IN ('node', 'way', 'relation')",
}


def _require_set_table(con, table: str, set_name: str) -> None:
    exists = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [table]
    ).fetchone()[0]
    if not exists:
        raise RuntimeQueryError(f'runtime error: set ".{set_name}" has not been set before')


def _py_is_number(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


def _py_to_number(s: str) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return float("nan")


def truthy(s: str) -> bool:
    """Overpass truthiness of a value string: "" and "0" are false."""
    return s is not None and s != "" and s != "0"


_py_truthy = truthy  # internal alias used throughout this module


def _py_bool(b: bool) -> str:
    return "1" if b else "0"


def evaluate_set(ctx, expr, set_name: str) -> str:
    """Evaluate an evaluator expression once, against ``set_<set_name>``,
    returning the resulting Overpass value as a Python ``str``. Runs one
    SQL query per ``count(...)``/aggregator subtree; a bare element-scoped
    function (outside an aggregator) is a ``RuntimeQueryError``."""

    def run_scalar(sql: str) -> object:
        return ctx.con.execute(sql).fetchone()[0]

    def eval_node(node):
        if isinstance(node, ev.Num):
            return _format_py_num(node.value)
        if isinstance(node, ev.Str):
            return node.value
        if isinstance(node, (ev.TagAccess, ev.ElementCall)):
            # Overpass requires element-scoped functions to be wrapped in an
            # aggregator (u(...), min(...), ...) when evaluated against a
            # set rather than a single element; a bare one here has no
            # single row to read from.
            raise RuntimeQueryError(
                "runtime error: element-scoped function used outside an aggregator "
                "in an 'if'/(if:) condition; wrap it in u(...)"
            )
        if isinstance(node, ev.SetCount):
            table = f"set_{set_name}"
            _require_set_table(ctx.con, table, set_name)
            cond = _TYPE_FILTER_SQL[node.type_name]
            n = run_scalar(f"SELECT count(*) FROM {table} WHERE {cond}")
            return str(int(n))
        if isinstance(node, ev.NamedSetCount):
            table = f"set_{node.set_name}"
            _require_set_table(ctx.con, table, node.set_name)
            cond = _TYPE_FILTER_SQL[node.type_name]
            n = run_scalar(f"SELECT count(*) FROM {table} WHERE {cond}")
            return str(int(n))
        if isinstance(node, ev.Aggregate):
            table = f"set_{set_name}"
            _require_set_table(ctx.con, table, set_name)
            value_sql = compile_element(node.expr, "__e", ctx.promoted_keys)
            func = node.func
            if func == "u":
                row = run_scalar(
                    f"SELECT CASE WHEN count(DISTINCT {value_sql}) <= 1 THEN max({value_sql}) "
                    f"ELSE NULL END FROM {table} __e"
                )
                return "" if row is None else str(row)
            if func == "min":
                row = run_scalar(f"SELECT min(try_cast({value_sql} AS DOUBLE)) FROM {table} __e")
                return "" if row is None else _format_py_num(float(row))
            if func == "max":
                row = run_scalar(f"SELECT max(try_cast({value_sql} AS DOUBLE)) FROM {table} __e")
                return "" if row is None else _format_py_num(float(row))
            if func == "sum":
                row = run_scalar(f"SELECT sum(try_cast({value_sql} AS DOUBLE)) FROM {table} __e")
                return _format_py_num(float(row) if row is not None else 0.0)
            if func == "set":
                row = run_scalar(
                    f"SELECT string_agg(DISTINCT {value_sql}, ', ' ORDER BY {value_sql}) FROM {table} __e"
                )
                return "" if row is None else str(row)
            raise UnsupportedError(f"aggregator {func}(...) is not supported")
        if isinstance(node, ev.Unary):
            val = eval_node(node.expr)
            if node.op == "!":
                return _py_bool(not _py_truthy(val))
            return _format_py_num(-_py_to_number(val))
        if isinstance(node, ev.BinOp):
            return eval_binop(node)
        if isinstance(node, ev.Ternary):
            cond = eval_node(node.cond)
            return eval_node(node.then if _py_truthy(cond) else node.otherwise)
        raise UnsupportedError(f"evaluator expression {type(node).__name__} is not supported")

    def eval_binop(node: "ev.BinOp") -> str:
        a = eval_node(node.left)
        b = eval_node(node.right)
        op = node.op
        if op in ("+", "-", "*", "/"):
            an, bn = _py_to_number(a), _py_to_number(b)
            if op == "+":
                return _format_py_num(an + bn)
            if op == "-":
                return _format_py_num(an - bn)
            if op == "*":
                return _format_py_num(an * bn)
            return _format_py_num(float("nan") if bn == 0 else an / bn)
        if op in _CMP_OP_SQL:
            if _py_is_number(a) and _py_is_number(b):
                an, bn = float(a), float(b)
                result = {
                    "<": an < bn,
                    "<=": an <= bn,
                    ">": an > bn,
                    ">=": an >= bn,
                    "==": an == bn,
                    "!=": an != bn,
                }[op]
            else:
                result = {
                    "<": a < b,
                    "<=": a <= b,
                    ">": a > b,
                    ">=": a >= b,
                    "==": a == b,
                    "!=": a != b,
                }[op]
            return _py_bool(result)
        if op == "&&":
            return _py_bool(_py_truthy(a) and _py_truthy(b))
        if op == "||":
            return _py_bool(_py_truthy(a) or _py_truthy(b))
        raise UnsupportedError(f"evaluator operator {op!r} is not supported")

    return eval_node(expr)
