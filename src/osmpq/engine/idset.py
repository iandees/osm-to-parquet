"""Helpers for id-based lookups: docs/m0-contracts.md section 8's `>`, `<`,
`>>`, `<<`, recurse filters (`(w)`/`(r)`/`(bn)`/`(bw)`/`(br)`), `(id:...)`,
and render.py's lazy way-geometry / node-coordinate hydration.

All of these used to inline every id as a SQL literal (``WHERE id IN
(1,2,3,...)``); for a `>` over ~83k byid-resolved node ids that alone forces
DuckDB to parse an 80k-token expression list. Past a small threshold we
instead materialize the ids as a DuckDB TEMP TABLE and turn the lookup into
``col IN (SELECT id FROM <table>)``, which DuckDB plans as a hash (semi)
join independent of how many ids there are -- O(rows), not O(ids) parsing
plus O(ids) literal matching.

Where the ids can be derived from a source set already living in DuckDB
(the recurse hops in `recurse.py`), prefer deriving the TEMP TABLE with a
plain ``CREATE TEMP TABLE ... AS SELECT`` so the ids never round-trip
through Python at all -- important at Minnesota/planet scale where a hop
can produce millions of ids and a Python list of them would itself become
the bottleneck.
"""
from __future__ import annotations

import itertools
from typing import Iterable, Optional

import pyarrow as pa

# IN-lists at or below this size stay as SQL literals: parsing/planning a
# few dozen integers is negligible, and it avoids a TEMP TABLE round trip
# for the overwhelmingly common case (a handful of explicit ids).
INLINE_ID_LIMIT = 100

_counter = itertools.count(1)


def fresh_table_name(prefix: str = "ids") -> str:
    return f"__{prefix}_{next(_counter)}"


def register_ids_table(con, ids: Iterable[int]) -> str:
    """Materialize a Python collection of ids as a TEMP TABLE(id BIGINT),
    without emitting a per-id SQL token: build one pyarrow array and hand it
    to DuckDB via `con.register`, then copy it into a real TEMP TABLE (so
    the registered view can be dropped again)."""
    name = fresh_table_name()
    view = f"{name}__src"
    tbl = pa.table({"id": pa.array(list(ids), type=pa.int64())})
    con.register(view, tbl)
    try:
        con.execute(f"CREATE TEMP TABLE {name} AS SELECT id FROM {view}")
    finally:
        con.unregister(view)
    return name


def id_predicate(con, column_expr: str, ids: list[int]) -> str:
    """SQL boolean predicate matching `column_expr` against `ids`: a literal
    IN-list for small id sets, else a lookup against a TEMP TABLE. Either
    way DuckDB is free to plan it as a hash join/semi-join."""
    ids = list(ids)
    if not ids:
        return "FALSE"
    if len(ids) <= INLINE_ID_LIMIT:
        return f"{column_expr} IN ({','.join(str(i) for i in ids)})"
    table = register_ids_table(con, ids)
    return f"{column_expr} IN (SELECT id FROM {table})"


def id_range(ids: list[int]) -> tuple[int, int]:
    return min(ids), max(ids)


def register_pairs_table(con, pairs: Iterable[tuple[str, int]], columns: tuple[str, str] = ("cell", "id")) -> str:
    """Like `register_ids_table` but for (str, int) pairs, e.g. (cell, id):
    used to group an id lookup by cell in one query instead of one query per
    cell (render.py's way-geometry hydration)."""
    name = fresh_table_name("pairs")
    view = f"{name}__src"
    pairs = list(pairs)
    col_a, col_b = columns
    tbl = pa.table(
        {
            col_a: pa.array([p[0] for p in pairs], type=pa.string()),
            col_b: pa.array([p[1] for p in pairs], type=pa.int64()),
        }
    )
    con.register(view, tbl)
    try:
        con.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM {view}")
    finally:
        con.unregister(view)
    return name


def table_has_column(con, table: str, column: str) -> bool:
    """True if TEMP TABLE `table` has a column named `column`. Used to
    detect whether an id-hop table (recurse.py) carries a spatial `cell`
    hint (design.md 3.1 items 1-2) before deciding whether to hydrate via
    (cell, id) or fall back to a plain byid scan."""
    row = con.execute(
        "SELECT count(*) FROM duckdb_columns() WHERE table_name = ? AND column_name = ?",
        [table, column],
    ).fetchone()
    return bool(row[0])


def sql_type_range(con, table: str, type_value: str) -> tuple[Optional[int], Optional[int], int]:
    """(min id, max id, row count) for `type = type_value` rows of a
    (type, id, ...) table/set, computed in SQL (a single aggregate, not an
    id fetch) so callers can pick byid/index parts without ever pulling the
    actual ids into Python."""
    row = con.execute(
        f"SELECT min(id), max(id), count(*) FROM {table} WHERE type = ?", [type_value]
    ).fetchone()
    return row[0], row[1], row[2]
