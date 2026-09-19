"""Shared helpers between ``osmpq.build.raw`` (the ``raw-py`` producer) and
``osmpq.build.builder`` (the ``build --raw`` consumer and the M0-compatible
``build`` orchestrator). Factored out of the original M0 ``builder.py`` so
both stages use identical, tested logic for the parts that didn't change in
M1 (PBF reading, leaf-cell selection, range-based part splitting, Parquet
writing).
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Optional

import numpy as np

from osmpq.layout import cells as cells_mod

RANGE_TARGET_ROWS = 4_000_000  # headroom under a 5M-row part limit for duplicate keys


def log(prefix: str, msg: str) -> None:
    print(f"[{prefix}] {msg}", file=sys.stderr, flush=True)


def promoted_select(promoted_keys: list[str], tags_expr: str = "tags") -> str:
    return ", ".join(f'{tags_expr}[\'{key}\'] AS "{key}"' for key in promoted_keys)


def copy_to_parquet(
    con,
    select_sql: str,
    path: Path,
    row_group_size: Optional[int] = None,
    row_group_size_bytes: Optional[int] = None,
) -> tuple[int, int]:
    """Run a ``COPY (...) TO parquet`` and return (rows, bytes on disk).

    ``row_group_size`` (rows) and/or ``row_group_size_bytes`` (bytes,
    DuckDB >= 1.5 with ``preserve_insertion_order=false``) control row-group
    sizing; both may be given (the writer stops a row group at whichever
    limit is hit first).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    posix = str(path).replace("'", "''")
    opts = ["FORMAT PARQUET", "COMPRESSION ZSTD"]
    if row_group_size is not None:
        opts.append(f"ROW_GROUP_SIZE {int(row_group_size)}")
    if row_group_size_bytes is not None:
        opts.append(f"ROW_GROUP_SIZE_BYTES {int(row_group_size_bytes)}")
    con.execute(f"COPY ({select_sql}) TO '{posix}' ({', '.join(opts)})")
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{posix}')").fetchone()[0]
    size = path.stat().st_size
    return rows, size


def register_assignment(con, name: str, ids: np.ndarray, cell: np.ndarray, hilbert: np.ndarray) -> None:
    """Register a numpy (id, cell, hilbert) triple as a DuckDB table ``name``,
    for joining back onto the row it was computed from."""
    import pyarrow as pa

    tbl = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "cell": pa.array([str(c) for c in cell], type=pa.string()),
            "hilbert": pa.array(hilbert, type=pa.uint64()),
        }
    )
    con.register(f"_{name}_arrow", tbl)
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _{name}_arrow")
    con.unregister(f"_{name}_arrow")


def select_leaf_cells(con, node_table: str, max_nodes_per_cell: int, max_depth: int) -> list[str]:
    """Leaf cell selection (docs/m0-contracts.md section 2), capped at
    ``max_depth`` (docs/m1-contracts.md section 2's ``--max-depth``)."""
    lat_e7, lon_e7 = con.execute(f"SELECT lat_e7, lon_e7 FROM {node_table}").fetchnumpy().values()
    if len(lat_e7) == 0:
        return [cells_mod.ROOT]
    qk = cells_mod.point_to_qk_np(lat_e7, lon_e7)
    codes, counts = np.unique(qk, return_counts=True)
    order = np.argsort(codes)
    codes = codes[order]
    counts = counts[order]
    cum = np.concatenate(([0], np.cumsum(counts)))

    def range_count(lo: int, hi: int) -> int:
        i0 = int(np.searchsorted(codes, np.uint64(lo), side="left"))
        i1 = int(np.searchsorted(codes, np.uint64(hi), side="right"))
        return int(cum[i1] - cum[i0])

    leaves: list[str] = []

    def recurse(key: str, depth: int) -> None:
        lo, hi = cells_mod.qk_range(key)
        n = range_count(lo, hi)
        if n <= max_nodes_per_cell or depth >= max_depth:
            if n > 0 or key == cells_mod.ROOT:
                leaves.append(key)
            return
        for child in cells_mod.children(key):
            recurse(child, depth + 1)

    recurse(cells_mod.ROOT, 0)
    return leaves


def range_bounds(con, table: str, col: str, total: int, target_rows: int = RANGE_TARGET_ROWS) -> list[tuple]:
    """Split ``table`` into id ranges of about ``target_rows`` rows on
    ``col`` using quantiles, so parts can be written one at a time without
    numbering every row (a window function over tens of millions of rows
    with map columns does not fit in memory)."""
    n = max(1, -(-total // target_rows))
    if n == 1:
        return [(None, None)]
    fractions = [i / n for i in range(1, n)]
    qs = con.execute(f"SELECT quantile_disc({col}, {fractions}) FROM {table}").fetchone()[0]
    qs = sorted(set(qs))
    bounds = [None] + qs + [None]
    return list(zip(bounds[:-1], bounds[1:]))


def range_cond(col: str, lo, hi) -> str:
    parts = []
    if lo is not None:
        parts.append(f"{col} >= {lo}")
    if hi is not None:
        parts.append(f"{col} < {hi}")
    return " AND ".join(parts) or "TRUE"


class Timer:
    """Small stopwatch used to build the per-stage timing dicts that go into
    ``summary.json`` / the report."""

    def __init__(self) -> None:
        self.stages: dict[str, float] = {}
        self._t0 = time.time()

    def lap(self, name: str) -> float:
        now = time.time()
        elapsed = now - self._t0
        self.stages[name] = elapsed
        self._t0 = now
        return elapsed
