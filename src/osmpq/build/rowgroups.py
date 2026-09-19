"""Row-group index side files (docs/m1-contracts.md section 4).

One row per Parquet row group of the spatial files, built from the Parquet
footers with pyarrow (no data read): ``path VARCHAR, cell VARCHAR, tagged
BOOLEAN (nodes only, NULL otherwise), rg INTEGER, rows INTEGER, xmin_e7
INTEGER, ymin_e7 INTEGER, xmax_e7 INTEGER, ymax_e7 INTEGER`` (for nodes the
min/max of ``lon_e7``/``lat_e7``). Sorted by ``path, rg``.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq


def _row_group_bbox_from_stats(
    metadata, rg: int, xmin_col: str, ymin_col: str, xmax_col: str, ymax_col: str
) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """Read a row group's bbox from its column statistics (footer only)."""
    rg_meta = metadata.row_group(rg)
    schema = metadata.schema
    col_index = {schema.column(i).name: i for i in range(len(schema))}
    x_lo = col_index.get(xmin_col)
    y_lo = col_index.get(ymin_col)
    x_hi = col_index.get(xmax_col)
    y_hi = col_index.get(ymax_col)

    def _min(idx: Optional[int]):
        if idx is None:
            return None
        stats = rg_meta.column(idx).statistics
        return int(stats.min) if stats is not None and stats.has_min_max and stats.min is not None else None

    def _max(idx: Optional[int]):
        if idx is None:
            return None
        stats = rg_meta.column(idx).statistics
        return int(stats.max) if stats is not None and stats.has_min_max and stats.max is not None else None

    return _min(x_lo), _min(y_lo), _max(x_hi), _max(y_hi)


def build_rowgroup_rows_for_file(
    root: Path, rel_path: str, cell: str, tagged: Optional[bool], xmin_col: str, ymin_col: str, xmax_col: str, ymax_col: str
) -> list[dict]:
    """One dict per row group of ``root/rel_path``."""
    full = root / rel_path
    pf = pq.ParquetFile(str(full))
    md = pf.metadata
    rows = []
    for rg in range(md.num_row_groups):
        xmin_e7, ymin_e7, xmax_e7, ymax_e7 = _row_group_bbox_from_stats(
            md, rg, xmin_col, ymin_col, xmax_col, ymax_col
        )
        rows.append({
            "path": rel_path,
            "cell": cell,
            "tagged": tagged,
            "rg": rg,
            "rows": md.row_group(rg).num_rows,
            "xmin_e7": xmin_e7,
            "ymin_e7": ymin_e7,
            "xmax_e7": xmax_e7,
            "ymax_e7": ymax_e7,
        })
    return rows


def write_rowgroup_index(con, root: str, generation: str, table: str, files: list[dict]) -> tuple[str, int]:
    """Build ``index/<gen>/rowgroups/<table>.parquet`` from a list of
    ``{"rel_path", "cell", "tagged"}`` dicts (one per spatial file of
    ``table``). Returns (manifest-relative path, row count)."""
    root_path = Path(root)
    if table == "node":
        xmin_col, ymin_col, xmax_col, ymax_col = "lon_e7", "lat_e7", "lon_e7", "lat_e7"
    else:
        xmin_col, ymin_col, xmax_col, ymax_col = "xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7"

    all_rows: list[dict] = []
    for f in files:
        all_rows.extend(
            build_rowgroup_rows_for_file(
                root_path, f["rel_path"], f["cell"], f.get("tagged"), xmin_col, ymin_col, xmax_col, ymax_col
            )
        )
    all_rows.sort(key=lambda r: (r["path"], r["rg"]))

    import pyarrow as pa

    arrow_table = pa.table({
        "path": pa.array([r["path"] for r in all_rows], type=pa.string()),
        "cell": pa.array([r["cell"] for r in all_rows], type=pa.string()),
        "tagged": pa.array([r["tagged"] for r in all_rows], type=pa.bool_()),
        "rg": pa.array([r["rg"] for r in all_rows], type=pa.int32()),
        "rows": pa.array([r["rows"] for r in all_rows], type=pa.int32()),
        "xmin_e7": pa.array([r["xmin_e7"] for r in all_rows], type=pa.int32()),
        "ymin_e7": pa.array([r["ymin_e7"] for r in all_rows], type=pa.int32()),
        "xmax_e7": pa.array([r["xmax_e7"] for r in all_rows], type=pa.int32()),
        "ymax_e7": pa.array([r["ymax_e7"] for r in all_rows], type=pa.int32()),
    })
    rel_path = f"index/{generation}/rowgroups/{table}.parquet"
    out_path = root_path / rel_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.register("_rg_arrow", arrow_table)
    con.execute(f"COPY (SELECT * FROM _rg_arrow) TO '{str(out_path).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.unregister("_rg_arrow")
    return rel_path, len(all_rows)
