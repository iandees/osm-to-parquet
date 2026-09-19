"""Materializing a SELECT into a `set_<name>` temp table, and the set
algebra (union / difference) over (type, id)."""
from __future__ import annotations

from .schema import CANONICAL_COLUMNS


def materialize(con, name: str, select_sql: str) -> None:
    """CREATE OR REPLACE TEMP TABLE set_<name> AS <select_sql>, deduplicated
    by (type, id) (first row wins) so re-fetched/overlapping sources don't
    produce duplicate elements."""
    cols = ", ".join(CANONICAL_COLUMNS)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE set_{name} AS
        SELECT {cols} FROM (
          SELECT *, row_number() OVER (PARTITION BY type, id) AS __rn
          FROM ({select_sql}) __src
        ) __d
        WHERE __rn = 1
        """
    )


def ensure_empty_set(con, name: str) -> None:
    from .schema import empty_set_sql

    materialize(con, name, empty_set_sql())


def union_sql(set_names: list[str]) -> str:
    parts = [f"SELECT {', '.join(CANONICAL_COLUMNS)} FROM set_{n}" for n in set_names]
    return "\nUNION ALL\n".join(parts)


def difference_sql(first_set: str, second_set: str) -> str:
    cols = ", ".join(CANONICAL_COLUMNS)
    return (
        f"SELECT {cols} FROM set_{first_set} a "
        f"WHERE NOT EXISTS (SELECT 1 FROM set_{second_set} b WHERE b.type = a.type AND b.id = a.id)"
    )
