"""The canonical column set for an Overpass "set" (contract: TEMP tables
``set_<name>``), shared by every statement so ``out`` never re-fetches."""
from __future__ import annotations

# name -> DuckDB type
CANONICAL_TYPES: dict[str, str] = {
    "type": "VARCHAR",
    "id": "BIGINT",
    "cell": "VARCHAR",
    "lat_e7": "INTEGER",
    "lon_e7": "INTEGER",
    "refs": "BIGINT[]",
    "members": "STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]",
    "tags": "MAP(VARCHAR, VARCHAR)",
    "version": "INTEGER",
    "changeset": "BIGINT",
    "timestamp": "TIMESTAMP",
    "uid": "INTEGER",
    "user": "VARCHAR",
    "xmin_e7": "INTEGER",
    "ymin_e7": "INTEGER",
    "xmax_e7": "INTEGER",
    "ymax_e7": "INTEGER",
    "geometry": "GEOMETRY",
    "hilbert": "UBIGINT",
}

CANONICAL_COLUMNS = list(CANONICAL_TYPES.keys())


def project(cols: dict[str, str]) -> str:
    """Build the "expr AS col, ..." list in canonical order. Columns not in
    ``cols`` default to a typed NULL."""
    parts = []
    for name, ty in CANONICAL_TYPES.items():
        expr = cols.get(name, f"NULL::{ty}")
        parts.append(f'{expr} AS "{name}"')
    return ",\n  ".join(parts)


def empty_set_sql() -> str:
    return "SELECT " + project({}) + " WHERE FALSE"
