"""History writer, docs/m4-contracts.md section 4.2.

Writes the layout of section 2.2 (base history: cell-partitioned spatial
files, id-sorted byid files) from a DuckDB table already shaped like a
history row (the current-state columns of
``osmpq.update.updater.SPATIAL_COLUMNS``/``BYID_COLUMNS`` plus
``osmpq.history.schema.HISTORY_EXTRA_NAMES``), and returns the manifest
fragments of section 2.3. Used by the base builder (``history/build.py``)
and, per the contract, by compaction (W3) to rewrite touched cells/byid
parts -- hence the exact signature below, agreed through the contract
rather than by direct coordination.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from osmpq.build import common
from osmpq.history import schema as schema_mod

SPATIAL_PART_MAX_BYTES = 64 * 1024 * 1024
BYID_PART_ROWS = 4_000_000


def _esc(p) -> str:
    return str(p).replace("'", "''")


def write_spatial(con, root: str, gen: str, type: str, table: str, cells: Optional[set] = None) -> dict:
    """Write ``history/<gen>/spatial/<type>/cell=<cell>/part-<n>.parquet``
    for every cell present in ``table`` (or just ``cells`` when given, for a
    compaction rewrite of touched cells only), sorted by
    ``hilbert, id, valid_from``, split at ~64MB per part. Returns
    ``{"<cell>": [{"path", "rows", "bytes"}, ...], ...}`` (section 2.3).
    """
    root_path = Path(root)
    if cells is not None:
        cell_rows = [(c,) for c in sorted(cells)]
    else:
        cell_rows = con.execute(f"SELECT DISTINCT cell FROM {table} WHERE cell IS NOT NULL ORDER BY cell").fetchall()

    out: dict[str, list[dict]] = {}
    for (cell,) in cell_rows:
        n = con.execute(f"SELECT count(*) FROM {table} WHERE cell = '{cell}'").fetchone()[0]
        if n == 0:
            continue
        cell_dir = root_path / schema_mod.spatial_dir(gen, type, cell)
        parts = _write_ordered_parts(
            con, f"SELECT * FROM {table} WHERE cell = '{cell}' ORDER BY hilbert, id, valid_from",
            cell_dir, "part", root_path,
        )
        out[cell] = parts
    return out


def write_byid(con, root: str, gen: str, type: str, table: str) -> list[dict]:
    """Write ``history/<gen>/byid/<type>/part-<n>.parquet``, sorted by
    ``id, valid_from``, split at ~4M rows per part. Returns
    ``[{"path", "min_id", "max_id", "rows", "bytes"}, ...]`` (section 2.3).
    """
    root_path = Path(root)
    out_dir = root_path / schema_mod.byid_dir(gen, type)
    total = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    parts: list[dict] = []
    if total == 0:
        return parts
    for k, (lo, hi) in enumerate(common.range_bounds(con, table, "id", total, BYID_PART_ROWS)):
        cond = common.range_cond("id", lo, hi)
        path = out_dir / f"part-{k:05d}.parquet"
        rows, size = common.copy_to_parquet(
            con, f"SELECT * FROM {table} WHERE {cond} ORDER BY id, valid_from", path,
            row_group_size_bytes=1_000_000,
        )
        min_id, max_id = con.execute(f"SELECT min(id), max(id) FROM {table} WHERE {cond}").fetchone()
        parts.append({
            "path": str(path.relative_to(root_path)).replace("\\", "/"),
            "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size,
        })
    return parts


def _write_ordered_parts(con, select_sql: str, out_dir: Path, prefix: str, root_path: Path) -> list[dict]:
    """Write ``select_sql`` (already the desired row order) to one or more
    parquet parts under ``out_dir``, splitting so each part stays under
    ``SPATIAL_PART_MAX_BYTES``. Writes once to see the actual size, and
    re-splits by row count only if that single file came out too big --
    cell files are small enough in practice that this rarely fires twice."""
    out_dir.mkdir(parents=True, exist_ok=True)
    probe_path = out_dir / f".{prefix}-probe.parquet"
    rows, size = common.copy_to_parquet(con, select_sql, probe_path, row_group_size_bytes=1_000_000)
    if rows == 0:
        probe_path.unlink(missing_ok=True)
        return []
    if size <= SPATIAL_PART_MAX_BYTES:
        final_path = out_dir / f"{prefix}-00000.parquet"
        probe_path.replace(final_path)
        return [{"path": str(final_path.relative_to(root_path)).replace("\\", "/"), "rows": rows, "bytes": size}]

    n_parts = -(-size // SPATIAL_PART_MAX_BYTES)
    target_rows = -(-rows // n_parts)
    parts: list[dict] = []
    # common.range_bounds splits on a quantile of a real column, but the
    # source rows have no single column that's both sorted and unique here
    # (hilbert/id both repeat); split by row_number() over the same order
    # the probe write already used instead.
    numbered = f"SELECT *, row_number() OVER () AS __rn FROM ({select_sql}) __src"
    for k, (lo, hi) in enumerate(_row_number_bounds(rows, target_rows)):
        cond = common.range_cond("__rn", lo, hi)
        path = out_dir / f"{prefix}-{k:05d}.parquet"
        prows, psize = common.copy_to_parquet(
            con, f"SELECT * EXCLUDE (__rn) FROM ({numbered}) __n WHERE {cond}", path, row_group_size_bytes=1_000_000,
        )
        parts.append({"path": str(path.relative_to(root_path)).replace("\\", "/"), "rows": prows, "bytes": psize})
    probe_path.unlink(missing_ok=True)
    return parts


def _row_number_bounds(total_rows: int, target_rows: int) -> list[tuple[int, int]]:
    n_parts = max(1, -(-total_rows // target_rows))
    bounds = []
    lo = 1
    for i in range(n_parts):
        hi = total_rows if i == n_parts - 1 else lo + target_rows - 1
        bounds.append((lo, hi + 1))  # range_cond uses [lo, hi) semantics on __rn
        lo = hi + 1
    return bounds
