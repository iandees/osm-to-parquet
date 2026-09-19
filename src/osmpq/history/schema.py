"""Shared definitions for history rows (docs/m4-contracts.md section 2.1).

A history row is the element's current-state row (same copy, same columns:
``osmpq.update.updater.SPATIAL_COLUMNS`` / ``BYID_COLUMNS``) plus the four
columns below. Every writer (builder, updater, compaction) and every reader
(engine snapshot path) goes through these names and this ordering rule, so
"the state at t" means the same thing everywhere.
"""
from __future__ import annotations

# (name, DuckDB type) in the order they are appended after the base columns.
HISTORY_EXTRA_COLUMNS: list[tuple[str, str]] = [
    ("minor", "INTEGER"),
    ("valid_from", "TIMESTAMP"),
    ("valid_to", "TIMESTAMP"),
    ("visible", "BOOLEAN"),
]
HISTORY_EXTRA_NAMES: list[str] = [c for c, _ in HISTORY_EXTRA_COLUMNS]

# Tier names and their fold order, identical to the M2 delta tiers.
TIERS = ("hour", "day", "week")

# ORDER BY that picks "the state at t" once rows are restricted to
# valid_from <= t: newest start wins; at the same instant a visible state
# beats its move tombstone; then the higher version / minor.
STATE_ORDER_SQL = "valid_from DESC, visible DESC, version DESC, minor DESC"


def history_columns(base_columns: list[str]) -> list[str]:
    """The full column list of a history row for a copy whose current-state
    columns are `base_columns` (a `SPATIAL_COLUMNS[typ](promoted_keys)` or
    `BYID_COLUMNS[typ](promoted_keys)` result)."""
    return list(base_columns) + HISTORY_EXTRA_NAMES


def validity_predicate(t_sql: str, with_valid_to: bool = True) -> str:
    """SQL restricting rows to those that could be the state at `t_sql`
    (a TIMESTAMP expression). `with_valid_to=False` for tier files, whose
    `valid_to` is always NULL (the predicate then costs one comparison)."""
    if with_valid_to:
        return f"valid_from <= {t_sql} AND (valid_to IS NULL OR valid_to > {t_sql})"
    return f"valid_from <= {t_sql}"


def state_at_sql(rows_sql: str, t_sql: str) -> str:
    """Wrap `rows_sql` (a SELECT over history rows already restricted with
    `validity_predicate`) so that exactly one row per id remains: the state
    at `t_sql`. Callers filter `visible` and their own predicates *outside*
    this (the state is chosen first, then filtered)."""
    return (
        f"SELECT * FROM ({rows_sql}) __hv "
        f"QUALIFY row_number() OVER (PARTITION BY id ORDER BY {STATE_ORDER_SQL}) = 1"
    )


def tier_dir(generation: str, tier: str, version: int) -> str:
    return f"history/{generation}/tier/{tier}/{version}"


def spatial_dir(generation: str, typ: str, cell: str) -> str:
    return f"history/{generation}/spatial/{typ}/cell={cell}"


def byid_dir(generation: str, typ: str) -> str:
    return f"history/{generation}/byid/{typ}"
