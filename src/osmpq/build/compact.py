"""``osmpq compact <root>``: docs/m2-contracts.md section 6.

Folds every present delta tier into a new base generation: touched spatial
cells and byid parts are rewritten (base rows minus the touched-id shadow,
plus the newest tier's live delta rows), untouched files are hardlinked;
``node_way``/``member`` indexes and the row-group index are rebuilt; a new
manifest v3 is written with ``deltas: {}``.

Reads the manifest as plain JSON (``json.load``) rather than through
``osmpq.layout.manifest.Manifest`` so this module does not depend on that
dataclass gaining v3 fields, and writes the new manifest the same way
(copy the loaded dict, edit the fields that changed, dump it).

Everything streams through DuckDB (``COPY ... TO parquet`` / filtered scans,
never ``fetchall`` of a whole table) so memory stays bounded regardless of
dataset size; delta tiers themselves are assumed small (m2-contracts.md
section 3), so they are read directly with plain ``read_parquet`` calls
rather than chunked.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from osmpq.build import areas as areas_mod
from osmpq.build import common
from osmpq.build import rowgroups as rowgroups_mod
from osmpq.engine import catalog as engine_catalog
from osmpq.history import schema as history_schema
from osmpq.update.updater import BYID_COLUMNS, SPATIAL_COLUMNS

TIER_ORDER = ["hour", "day", "week"]  # precedence high (0) to low

NODE_SPATIAL_ROW_GROUP = 100_000
NODE_BYID_ROW_GROUP = 64_000
WAY_BYID_ROW_GROUP_BYTES = 1_000_000
WAY_SPATIAL_ROW_GROUP_BYTES = 1_500_000
RELATION_ROW_GROUP = 100_000
INDEX_ROW_GROUP = 100_000

DEFAULT_PROMOTED_KEYS = [
    "amenity", "shop", "highway", "building", "name", "natural",
    "landuse", "leisure", "railway", "waterway", "place", "tourism",
]

_META_COLS = 'version, changeset, timestamp, uid, "user"'


def _log(msg: str) -> None:
    common.log("osmpq compact", msg)


@dataclass
class CompactOptions:
    root: str
    generation: Optional[str] = None
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    tmpdir: Optional[str] = None


# --------------------------------------------------------------------------
# column lists (physical schemas -- see docs/m0-contracts.md section 4 and
# docs/m1-contracts.md sections 3-4 for base; docs/m2-contracts.md section 3
# for the delta augmentation)
# --------------------------------------------------------------------------


def _node_spatial_cols(promoted_sql: str) -> str:
    return f"id, lat_e7, lon_e7, tags, {promoted_sql}, {_META_COLS}, hilbert"


def _node_byid_cols(promoted_sql: str) -> str:
    return f"id, lat_e7, lon_e7, tags, {promoted_sql}, {_META_COLS}, cell, hilbert"


def _way_spatial_cols(promoted_sql: str) -> str:
    return (
        f"id, refs, tags, {promoted_sql}, {_META_COLS}, "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, geometry, is_closed, is_area, "
        f"centroid_lat_e7, centroid_lon_e7, cell, hilbert"
    )


def _way_byid_cols(promoted_sql: str) -> str:
    return (
        f"id, refs, tags, {promoted_sql}, {_META_COLS}, "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, is_closed, is_area, cell, hilbert"
    )


def _relation_spatial_cols(promoted_sql: str) -> str:
    return (
        f"id, members, tags, {promoted_sql}, {_META_COLS}, "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, geometry, "
        f"centroid_lat_e7, centroid_lon_e7, cell, hilbert"
    )


def _relation_byid_cols(promoted_sql: str) -> str:
    return f"id, members, tags, {promoted_sql}, {_META_COLS}, xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell"


# --------------------------------------------------------------------------
# small filesystem / parquet-footer helpers (mirrors builder.py's, kept
# local so this module has no coupling to another agent's file)
# --------------------------------------------------------------------------


def _esc(path: Path) -> str:
    return str(path).replace("'", "''")


def _place_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _parquet_id_range(path: Path, col: str = "id") -> tuple[Optional[int], Optional[int]]:
    import pyarrow.parquet as pq

    md = pq.ParquetFile(str(path)).metadata
    schema = md.schema
    idx = next((i for i in range(len(schema)) if schema.column(i).name == col), None)
    if idx is None:
        return None, None
    lo = hi = None
    for rg in range(md.num_row_groups):
        stats = md.row_group(rg).column(idx).statistics
        if stats is None or not stats.has_min_max:
            continue
        lo = stats.min if lo is None else min(lo, stats.min)
        hi = stats.max if hi is None else max(hi, stats.max)
    return (int(lo) if lo is not None else None), (int(hi) if hi is not None else None)


# --------------------------------------------------------------------------
# delta reading: tier-precedence winners, touched cells
# --------------------------------------------------------------------------


def _delta_paths(man_deltas: dict, table: str, kind: str) -> list[tuple[str, int]]:
    out = []
    for rank, tier in enumerate(TIER_ORDER):
        tier_info = man_deltas.get(tier)
        if not tier_info:
            continue
        files = (tier_info.get("files") or {}).get(table)
        if not files:
            continue
        path = files.get(kind)
        if path:
            out.append((path, rank))
    return out


def _tombstone_paths(man_deltas: dict) -> list[str]:
    out = []
    for tier in TIER_ORDER:
        tier_info = man_deltas.get(tier)
        if tier_info and (tier_info.get("files") or {}).get("tombstones"):
            out.append(tier_info["files"]["tombstones"])
    return out


def _build_winners(con, root: Path, man_deltas: dict, table: str, kind: str, view_name: str) -> bool:
    """Materializes ``view_name``: one row per touched (table, kind) id,
    newest tier wins, delta file columns as-is (incl. deleted/prev_cell/
    seq). Returns False (and leaves no table) if no tier has this file."""
    paths = _delta_paths(man_deltas, table, kind)
    con.execute(f"DROP TABLE IF EXISTS {view_name}")
    if not paths:
        return False
    parts = [f"SELECT *, {rank} AS __tier_rank FROM read_parquet('{_esc(root / p)}')" for p, rank in paths]
    union_sql = " UNION ALL BY NAME ".join(parts)
    con.execute(f"""
        CREATE TABLE {view_name} AS
        SELECT * EXCLUDE (__rn, __tier_rank) FROM (
            SELECT *, row_number() OVER (PARTITION BY id ORDER BY __tier_rank ASC) AS __rn
            FROM ({union_sql})
        ) WHERE __rn = 1
    """)
    return True


def _touched_cells(con, root: Path, man_deltas: dict, table: str, winners_view: str, has_spatial: bool) -> set[str]:
    cells: set[str] = set()
    if has_spatial:
        rows = con.execute(f"""
            SELECT DISTINCT cell FROM {winners_view}
            UNION SELECT DISTINCT prev_cell FROM {winners_view} WHERE prev_cell IS NOT NULL
        """).fetchall()
        cells.update(r[0] for r in rows)
    for p in _tombstone_paths(man_deltas):
        rows = con.execute(
            f"SELECT DISTINCT prev_cell FROM read_parquet('{_esc(root / p)}') "
            f"WHERE type = '{table}' AND prev_cell IS NOT NULL"
        ).fetchall()
        cells.update(r[0] for r in rows)
    cells.discard(None)
    return cells


# --------------------------------------------------------------------------
# spatial rewrite: node (tagged/untagged split) and flat (way/relation)
# --------------------------------------------------------------------------


def _compact_node_spatial(
    con, root: Path, old_cells: dict, new_generation: str, promoted_sql: str,
    winners_view: str, has_winners: bool, touched_cells: set[str],
) -> tuple[dict, int]:
    all_cells = set(old_cells) | touched_cells
    new_cells: dict = {}
    total_bytes = 0
    cols = _node_spatial_cols(promoted_sql)
    for cell in sorted(all_cells):
        old_entry = old_cells.get(cell, {})
        if cell not in touched_cells:
            new_entry = {}
            for part, suffix in (("tagged", "true"), ("untagged", "false")):
                if part in old_entry:
                    old_path = root / old_entry[part]["path"]
                    rel = f"spatial/{new_generation}/node/cell={cell}/tagged={suffix}/part-0.parquet"
                    _place_file(old_path, root / rel)
                    new_entry[part] = {"path": rel, "rows": old_entry[part]["rows"], "bytes": old_entry[part]["bytes"]}
                    total_bytes += old_entry[part]["bytes"]
            if new_entry:
                new_cells[cell] = new_entry
            continue

        base_parts = []
        for part in ("tagged", "untagged"):
            if part in old_entry:
                p = root / old_entry[part]["path"]
                base_parts.append(f"SELECT {cols} FROM read_parquet('{_esc(p)}') WHERE id NOT IN (SELECT id FROM {winners_view})")
        if has_winners:
            base_parts.append(f"SELECT {cols} FROM {winners_view} WHERE cell = '{cell}' AND NOT deleted")
        if not base_parts:
            continue
        con.execute(f"CREATE OR REPLACE TABLE _cell_combined AS {' UNION ALL '.join(base_parts)}")
        new_entry = {}
        for tagged, suffix, cond in ((True, "true", "tags IS NOT NULL"), (False, "false", "tags IS NULL")):
            sel = f"SELECT * FROM _cell_combined WHERE {cond} ORDER BY hilbert, id"
            rel = f"spatial/{new_generation}/node/cell={cell}/tagged={suffix}/part-0.parquet"
            path = root / rel
            rows, size = common.copy_to_parquet(con, sel, path, row_group_size=NODE_SPATIAL_ROW_GROUP)
            if rows == 0:
                path.unlink()
                continue
            new_entry[("tagged" if tagged else "untagged")] = {"path": rel, "rows": rows, "bytes": size}
            total_bytes += size
        con.execute("DROP TABLE _cell_combined")
        if new_entry:
            new_cells[cell] = new_entry
    return {"cells": new_cells}, total_bytes


def _compact_flat_spatial(
    con, root: Path, old_cells: dict, table: str, new_generation: str,
    spatial_cols: str, winners_view: str, has_winners: bool,
    touched_cells: set[str], row_group_kwargs: dict,
) -> tuple[dict, int]:
    all_cells = set(old_cells) | touched_cells
    new_cells: dict = {}
    total_bytes = 0
    for cell in sorted(all_cells):
        old_entry = old_cells.get(cell)
        if cell not in touched_cells:
            if old_entry:
                old_path = root / old_entry["path"]
                rel = f"spatial/{new_generation}/{table}/cell={cell}/part-0.parquet"
                _place_file(old_path, root / rel)
                new_cells[cell] = {"path": rel, "rows": old_entry["rows"], "bytes": old_entry["bytes"], "bbox": old_entry.get("bbox")}
                total_bytes += old_entry["bytes"]
            continue

        parts = []
        if old_entry:
            p = root / old_entry["path"]
            parts.append(f"SELECT {spatial_cols} FROM read_parquet('{_esc(p)}') WHERE id NOT IN (SELECT id FROM {winners_view})")
        if has_winners:
            parts.append(f"SELECT {spatial_cols} FROM {winners_view} WHERE cell = '{cell}' AND NOT deleted")
        if not parts:
            continue
        sel = " UNION ALL ".join(parts) + " ORDER BY hilbert, id"
        rel = f"spatial/{new_generation}/{table}/cell={cell}/part-0.parquet"
        path = root / rel
        rows, size = common.copy_to_parquet(con, sel, path, **row_group_kwargs)
        if rows == 0:
            path.unlink()
            continue
        bbox = con.execute(
            f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
            f"FROM read_parquet('{_esc(path)}') WHERE xmin_e7 IS NOT NULL"
        ).fetchone()
        new_cells[cell] = {"path": rel, "rows": rows, "bytes": size, "bbox": [float(x) if x is not None else None for x in bbox]}
        total_bytes += size
    return {"cells": new_cells}, total_bytes


# --------------------------------------------------------------------------
# byid rewrite (node/way/relation, all id-ranged parts)
# --------------------------------------------------------------------------


def _assign_target_parts(con, name: str, old_parts: list[dict], winners_view: str) -> str:
    """Creates and returns ``{name}_assign``: winners_view's rows plus a
    ``__target_part`` column (the part index each winner id belongs to --
    the part whose [min_id, max_id] contains it, else the nearest lower
    part, else the first part)."""
    parts_df = f"{name}_parts_df"
    con.execute(f"DROP TABLE IF EXISTS {parts_df}")
    con.execute(f"CREATE TABLE {parts_df} (idx INTEGER, min_id BIGINT, max_id BIGINT)")
    con.executemany(
        f"INSERT INTO {parts_df} VALUES (?, ?, ?)",
        [(i, p["min_id"], p["max_id"]) for i, p in enumerate(old_parts)],
    )
    assign_name = f"{name}_assign"
    con.execute(f"""
        CREATE OR REPLACE TABLE {assign_name} AS
        SELECT w.*,
          COALESCE(
            (SELECT idx FROM {parts_df} WHERE w.id BETWEEN min_id AND max_id),
            (SELECT idx FROM {parts_df} WHERE max_id < w.id ORDER BY max_id DESC LIMIT 1),
            (SELECT idx FROM {parts_df} ORDER BY idx ASC LIMIT 1)
          ) AS __target_part
        FROM {winners_view} w
    """)
    return assign_name


def _compact_byid_table(
    con, root: Path, old_parts: list[dict], table: str, new_generation: str,
    byid_cols: str, winners_view: str, has_winners: bool, row_group_kwargs: dict,
) -> tuple[list[dict], int]:
    if not has_winners:
        new_parts, total_bytes = [], 0
        for i, p in enumerate(old_parts):
            rel = f"byid/{new_generation}/{table}/part-{i:05d}.parquet"
            _place_file(root / p["path"], root / rel)
            new_parts.append({"path": rel, "min_id": p["min_id"], "max_id": p["max_id"], "rows": p["rows"], "bytes": p["bytes"]})
            total_bytes += p["bytes"]
        return new_parts, total_bytes

    if not old_parts:
        rel = f"byid/{new_generation}/{table}/part-00000.parquet"
        path = root / rel
        sel = f"SELECT {byid_cols} FROM {winners_view} WHERE NOT deleted ORDER BY id"
        rows, size = common.copy_to_parquet(con, sel, path, **row_group_kwargs)
        if rows == 0:
            path.unlink()
            return [], 0
        min_id, max_id = _parquet_id_range(path)
        return [{"path": rel, "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size}], size

    assign_name = _assign_target_parts(con, f"_{table}_byid", old_parts, winners_view)
    touched_idxs = {r[0] for r in con.execute(f"SELECT DISTINCT __target_part FROM {assign_name}").fetchall()}

    new_parts, total_bytes = [], 0
    for i, p in enumerate(old_parts):
        rel = f"byid/{new_generation}/{table}/part-{i:05d}.parquet"
        new_path = root / rel
        if i not in touched_idxs:
            _place_file(root / p["path"], new_path)
            new_parts.append({"path": rel, "min_id": p["min_id"], "max_id": p["max_id"], "rows": p["rows"], "bytes": p["bytes"]})
            total_bytes += p["bytes"]
            continue
        old_path = root / p["path"]
        sel = (
            f"SELECT {byid_cols} FROM read_parquet('{_esc(old_path)}') "
            f"WHERE id NOT IN (SELECT id FROM {winners_view}) "
            f"UNION ALL "
            f"SELECT {byid_cols} FROM {assign_name} WHERE __target_part = {i} AND NOT deleted "
            f"ORDER BY id"
        )
        rows, size = common.copy_to_parquet(con, sel, new_path, **row_group_kwargs)
        if rows == 0:
            new_path.unlink()
            continue
        min_id, max_id = _parquet_id_range(new_path)
        new_parts.append({"path": rel, "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size})
        total_bytes += size
    con.execute(f"DROP TABLE IF EXISTS {assign_name}")
    con.execute(f"DROP TABLE IF EXISTS _{table}_byid_parts_df")
    return new_parts, total_bytes


# --------------------------------------------------------------------------
# node_way index: rebuild only the parts covering touched node ids
# --------------------------------------------------------------------------


def _hardlink_index_parts(root: Path, old_parts: list[dict], new_generation: str, index_name: str) -> tuple[list[dict], int]:
    new_parts, total_bytes = [], 0
    for i, p in enumerate(old_parts):
        rel = f"index/{new_generation}/{index_name}/part-{i:05d}.parquet"
        _place_file(root / p["path"], root / rel)
        new_parts.append({"path": rel, "min_id": p.get("min_id"), "max_id": p.get("max_id"), "rows": p["rows"], "bytes": p["bytes"]})
        total_bytes += p["bytes"]
    return new_parts, total_bytes


def _compact_node_way_index(
    con, root: Path, old_node_way_parts: list[dict], old_way_byid_parts: list[dict],
    new_way_byid_manifest: list[dict], new_generation: str,
    has_way_winners: bool, way_winners_view: str, has_node_winners: bool, node_winners_view: str,
) -> tuple[list[dict], int]:
    if not old_node_way_parts:
        return [], 0  # nothing to rebuild from (m1-contracts.md: "may be an empty list")
    if not has_way_winners and not has_node_winners:
        return _hardlink_index_parts(root, old_node_way_parts, new_generation, "node_way")

    old_way_paths = [str(root / p["path"]) for p in old_way_byid_parts]
    new_way_paths = [str(root / p["path"]) for p in new_way_byid_manifest]

    con.execute("DROP TABLE IF EXISTS _touched_node_ids")
    union_parts = []
    if has_way_winners and old_way_paths:
        union_parts.append(
            f"SELECT unnest(refs) AS node_id FROM read_parquet({old_way_paths!r}) "
            f"WHERE id IN (SELECT id FROM {way_winners_view}) AND refs IS NOT NULL"
        )
    if has_way_winners:
        union_parts.append(f"SELECT unnest(refs) AS node_id FROM {way_winners_view} WHERE NOT deleted AND refs IS NOT NULL")
    if has_node_winners:
        union_parts.append(f"SELECT id AS node_id FROM {node_winners_view}")
    if not union_parts:
        return _hardlink_index_parts(root, old_node_way_parts, new_generation, "node_way")
    con.execute(f"CREATE TABLE _touched_node_ids AS SELECT DISTINCT node_id FROM ({' UNION ALL '.join(union_parts)})")
    n_touched = con.execute("SELECT count(*) FROM _touched_node_ids").fetchone()[0]
    if n_touched == 0:
        con.execute("DROP TABLE _touched_node_ids")
        return _hardlink_index_parts(root, old_node_way_parts, new_generation, "node_way")

    if new_way_paths:
        con.execute(f"""
            CREATE TABLE _new_node_way_pairs AS
            SELECT w.node_id, w.way_id FROM (
                SELECT unnest(refs) AS node_id, id AS way_id FROM read_parquet({new_way_paths!r}) WHERE refs IS NOT NULL
            ) w JOIN _touched_node_ids t ON t.node_id = w.node_id
        """)
    else:
        con.execute("CREATE TABLE _new_node_way_pairs (node_id BIGINT, way_id BIGINT)")

    parts_df = "_node_way_parts_df"
    con.execute(f"DROP TABLE IF EXISTS {parts_df}")
    con.execute(f"CREATE TABLE {parts_df} (idx INTEGER, min_id BIGINT, max_id BIGINT)")
    con.executemany(
        f"INSERT INTO {parts_df} VALUES (?, ?, ?)",
        [(i, p["min_id"], p["max_id"]) for i, p in enumerate(old_node_way_parts)],
    )
    con.execute(f"""
        CREATE TABLE _touched_node_target AS
        SELECT t.node_id,
          COALESCE(
            (SELECT idx FROM {parts_df} WHERE t.node_id BETWEEN min_id AND max_id),
            (SELECT idx FROM {parts_df} WHERE max_id < t.node_id ORDER BY max_id DESC LIMIT 1),
            (SELECT idx FROM {parts_df} ORDER BY idx ASC LIMIT 1)
          ) AS target_part
        FROM _touched_node_ids t
    """)
    touched_idxs = {r[0] for r in con.execute("SELECT DISTINCT target_part FROM _touched_node_target").fetchall()}

    new_parts, total_bytes = [], 0
    for i, p in enumerate(old_node_way_parts):
        rel = f"index/{new_generation}/node_way/part-{i:05d}.parquet"
        new_path = root / rel
        if i not in touched_idxs:
            _place_file(root / p["path"], new_path)
            new_parts.append({"path": rel, "min_id": p["min_id"], "max_id": p["max_id"], "rows": p["rows"], "bytes": p["bytes"]})
            total_bytes += p["bytes"]
            continue
        old_path = str(root / p["path"])
        sel = f"""
            SELECT node_id, way_id FROM read_parquet('{old_path.replace(chr(39), chr(39) * 2)}')
            WHERE node_id NOT IN (SELECT node_id FROM _touched_node_ids)
            UNION ALL
            SELECT p2.node_id, p2.way_id FROM _new_node_way_pairs p2
            JOIN _touched_node_target tt ON tt.node_id = p2.node_id
            WHERE tt.target_part = {i}
            ORDER BY node_id, way_id
        """
        rows, size = common.copy_to_parquet(con, sel, new_path, row_group_size=INDEX_ROW_GROUP)
        if rows == 0:
            new_path.unlink()
            continue
        min_id, max_id = _parquet_id_range(new_path, col="node_id")
        new_parts.append({"path": rel, "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size})
        total_bytes += size

    for t in ("_touched_node_ids", "_new_node_way_pairs", parts_df, "_touched_node_target"):
        con.execute(f"DROP TABLE IF EXISTS {t}")
    return new_parts, total_bytes


# --------------------------------------------------------------------------
# member index: full rebuild from the new relation state (small)
# --------------------------------------------------------------------------


def _compact_member_index(con, root: Path, relation_byid_manifest: list[dict], new_generation: str) -> tuple[list[dict], int]:
    if not relation_byid_manifest:
        return [], 0
    rel_paths = [str(root / p["path"]) for p in relation_byid_manifest]
    con.execute(f"""
        CREATE OR REPLACE TABLE _new_members AS
        SELECT m.type AS member_type, m.ref AS member_id, r.id AS parent_id, m.role AS role, r.cell AS parent_cell
        FROM read_parquet({rel_paths!r}) r, UNNEST(r.members) AS t(m)
    """)
    total = con.execute("SELECT count(*) FROM _new_members").fetchone()[0]
    member_manifest, member_bytes = [], 0
    if total > 0:
        for k, (lo, hi) in enumerate(common.range_bounds(con, "_new_members", "member_id", total)):
            cond = common.range_cond("member_id", lo, hi)
            sel = (
                f"SELECT member_type, member_id, parent_id, role, parent_cell FROM _new_members "
                f"WHERE {cond} ORDER BY member_type, member_id, parent_id"
            )
            rel = f"index/{new_generation}/member/part-{k:05d}.parquet"
            path = root / rel
            rows, size = common.copy_to_parquet(con, sel, path, row_group_size=INDEX_ROW_GROUP)
            member_manifest.append({"path": rel, "rows": rows, "bytes": size})
            member_bytes += size
    con.execute("DROP TABLE _new_members")
    return member_manifest, member_bytes


# --------------------------------------------------------------------------
# row-group index file lists (mirrors builder.py's private helpers)
# --------------------------------------------------------------------------


def _node_rg_files(node_table_manifest: dict) -> list[dict]:
    out = []
    for cell, entry in node_table_manifest["cells"].items():
        for key, tagged_bool in (("tagged", True), ("untagged", False)):
            if key in entry:
                out.append({"rel_path": entry[key]["path"], "cell": cell, "tagged": tagged_bool})
    return out


def _flat_rg_files(table_manifest: dict) -> list[dict]:
    return [{"rel_path": e["path"], "cell": cell, "tagged": None} for cell, e in table_manifest["cells"].items()]


# --------------------------------------------------------------------------
# areas (docs/m3-contracts.md section 9.2): relation areas re-derived for
# touched pivots, merged into the previous generation's area cell files;
# the way index is rewritten in full from the compacted way byid table.
# --------------------------------------------------------------------------


def _compact_areas(
    con, root: Path, old_man: dict, new_man: dict, new_generation: str,
    promoted_keys: list[str],
    way_byid_manifest: list[dict],
    has_rel_byid: bool, relation_byid_winners_view: str,
) -> tuple[Optional[dict], int]:
    """Re-derives relation-area rows for touched pivots -- winners of type
    relation, which already include every relation that lists a touched
    way as a member, since the updater puts those into the touched set too
    (docs/m2-contracts.md's touched-set fixed point) -- merges them into
    the previous generation's area cell files (only cells that actually
    gained/lost a row are rewritten; the rest are hardlinked forward), and
    rewrites the (small) relation index file in full. The way index
    (`way_areas.parquet`) has no per-pivot merge at all: it is rebuilt from
    scratch by scanning `way_byid_manifest` -- the *new* generation's
    already-fully-compacted way byid parts (9.2: "the way index is
    rewritten in full from the compacted way tables"). Returns (new
    `areas` manifest field, bytes written), or (None, 0) when the dataset
    has no `areas` table yet -- areas then simply stay absent until the
    first `osmpq areas` run, same as a brand-new v4 manifest."""
    old_areas = old_man.get("areas") or {}
    if not old_areas.get("index"):
        return None, 0

    touched_relation_ids = (
        [r[0] for r in con.execute(f"SELECT id FROM {relation_byid_winners_view}").fetchall()]
        if has_rel_byid else []
    )
    stale_ids = [r + areas_mod.RELATION_ID_OFFSET for r in touched_relation_ids]

    new_cat_manifest = engine_catalog.Manifest(root=str(root), data=new_man)
    placed_table, _files_read = areas_mod.derive_relation_areas_for_pivots(
        con, new_cat_manifest, promoted_keys, touched_relation_ids,
    )

    old_index_path = root / old_areas["index"]["path"]
    old_cells = old_areas.get("cells", {})
    promoted_cols = ", ".join(f'"{k}"' for k in promoted_keys)
    idx_cols = (
        f'id, pivot_type, pivot_id, tags, {promoted_cols}, '
        f'version, changeset, timestamp, uid, "user", xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell, hilbert'
    )
    spatial_cols = idx_cols.replace("cell, hilbert", "geometry, cell, hilbert")
    id_excl = f"id NOT IN ({','.join(str(i) for i in stale_ids)})" if stale_ids else "TRUE"

    if stale_ids:
        stale_cells = {
            r[0] for r in con.execute(
                f"SELECT DISTINCT cell FROM read_parquet('{_esc(old_index_path)}') "
                f"WHERE id IN ({','.join(str(i) for i in stale_ids)})"
            ).fetchall()
        }
    else:
        stale_cells = set()
    new_cells = {r[0] for r in con.execute(f"SELECT DISTINCT cell FROM {placed_table}").fetchall()}
    touched_cells = stale_cells | new_cells
    all_cells = set(old_cells) | touched_cells

    total_bytes = 0
    cells_manifest: dict = {}
    for cell in sorted(all_cells):
        old_entry = old_cells.get(cell)
        rel_path = f"spatial/{new_generation}/area/cell={cell}/part-0.parquet"
        if cell not in touched_cells:
            if old_entry:
                _place_file(root / old_entry["path"], root / rel_path)
                cells_manifest[cell] = {**old_entry, "path": rel_path}
                total_bytes += old_entry["bytes"]
            continue
        parts = []
        if old_entry:
            old_path = root / old_entry["path"]
            parts.append(f"SELECT {spatial_cols} FROM read_parquet('{_esc(old_path)}') WHERE {id_excl}")
        parts.append(f"SELECT {spatial_cols} FROM {placed_table} WHERE cell = '{_esc(cell)}'")
        sel = " UNION ALL ".join(parts) + " ORDER BY hilbert, id"
        path = root / rel_path
        rows, size = common.copy_to_parquet(con, sel, path, row_group_size_bytes=areas_mod.AREA_SPATIAL_ROW_GROUP_BYTES)
        if rows == 0:
            if path.exists():
                path.unlink()
            continue
        bbox = con.execute(
            f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
            f"FROM read_parquet('{_esc(path)}')"
        ).fetchone()
        cells_manifest[cell] = {
            "path": rel_path, "rows": rows, "bytes": size,
            "bbox": [float(x) if x is not None else None for x in bbox],
        }
        total_bytes += size

    index_rel = f"index/{new_generation}/areas.parquet"
    index_path = root / index_rel
    index_sel = (
        f"SELECT {idx_cols} FROM read_parquet('{_esc(old_index_path)}') WHERE {id_excl} "
        f"UNION ALL SELECT {idx_cols} FROM {placed_table} "
        f"ORDER BY id"
    )
    index_rows, index_size = common.copy_to_parquet(con, index_sel, index_path, row_group_size_bytes=areas_mod.AREA_INDEX_ROW_GROUP_BYTES)
    total_bytes += index_size

    way_files = [str(root / p["path"]) for p in way_byid_manifest]
    way_index_field = areas_mod.build_way_area_index_from_files(con, root, new_generation, way_files, promoted_keys)
    total_bytes += way_index_field["bytes"]

    return (
        {"index": {"path": index_rel, "rows": index_rows, "bytes": index_size},
         "cells": cells_manifest, "way_index": way_index_field},
        total_bytes,
    )


# --------------------------------------------------------------------------
# history (docs/m4-contracts.md section 5.2): fold every history tier into
# the base history -- rewrite the touched spatial cells and byid parts,
# fill `valid_to` for rows that now have a successor, hardlink the rest,
# clear `history.tiers`, bump `history.generation`.
#
# `_history_write_spatial`/`_history_write_byid` below are a *local stub*
# standing in for W1's `history/writer.py` (docs/m4-contracts.md section
# 4.2: "write_spatial(con, root, gen, type, table, cells: Optional[set])" /
# "write_byid(con, root, gen, type, table)", returning the section 2.3
# manifest fragments) -- at merge time the coordinator can replace these
# two functions' bodies with calls into the real module without touching
# any of their callers, since the call signature already matches.
# --------------------------------------------------------------------------


def _history_write_spatial(
    con, root: Path, gen: str, typ: str, table: str, cells: Optional[set] = None,
) -> dict:
    """Writes ``history/<gen>/spatial/<typ>/cell=<cell>/part-0.parquet`` for
    each cell in ``cells`` (or every distinct cell present in ``table`` when
    ``cells`` is None), sorted by ``(hilbert, id, valid_from)`` --
    docs/m4-contracts.md section 2.2. `table` must already carry exactly the
    rows to write (`SELECT *` is used verbatim). Returns the manifest
    fragment ``{cell: [{"path","rows","bytes"}]}`` (section 2.3's
    ``history.spatial.<type>`` shape). One part per cell, like this
    module's existing way/relation spatial compaction (`_compact_flat_
    spatial`) -- the ``<= 64MB, several parts`` split the contract allows
    for a very large cell is not implemented here (never exercised at the
    Bermuda/fixture scale this workstream tests at)."""
    if cells is None:
        cells = {r[0] for r in con.execute(f"SELECT DISTINCT cell FROM {table} WHERE cell IS NOT NULL").fetchall()}
    out: dict = {}
    for cell in sorted(cells):
        rel = f"history/{gen}/spatial/{typ}/cell={cell}/part-0.parquet"
        path = root / rel
        cell_esc = cell.replace("'", "''")
        sel = f"SELECT * FROM {table} WHERE cell = '{cell_esc}' ORDER BY hilbert, id, valid_from"
        rows, size = common.copy_to_parquet(con, sel, path, row_group_size_bytes=1_000_000)
        if rows == 0:
            if path.exists():
                path.unlink()
            continue
        out[cell] = [{"path": rel, "rows": rows, "bytes": size}]
    return out


def _history_write_byid(con, root: Path, gen: str, typ: str, table: str) -> list[dict]:
    """Writes ``history/<gen>/byid/<typ>/part-<n>.parquet`` from ``table``,
    sorted by ``(id, valid_from)``, split at ~4M rows -- section 2.2.
    Returns the manifest fragment (list of part dicts) -- section 2.3's
    ``history.byid.<type>`` shape."""
    total = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    out: list[dict] = []
    if total == 0:
        return out
    for k, (lo, hi) in enumerate(common.range_bounds(con, table, "id", total)):
        cond = common.range_cond("id", lo, hi)
        rel = f"history/{gen}/byid/{typ}/part-{k:05d}.parquet"
        path = root / rel
        sel = f"SELECT * FROM {table} WHERE {cond} ORDER BY id, valid_from"
        rows, size = common.copy_to_parquet(con, sel, path, row_group_size_bytes=1_000_000)
        if rows == 0:
            if path.exists():
                path.unlink()
            continue
        min_id, max_id = _parquet_id_range(path)
        out.append({"path": rel, "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size})
    return out


def _hardlink_history_spatial(root: Path, old_spatial: dict, new_generation: str, typ: str) -> tuple[dict, int]:
    out: dict = {}
    total_bytes = 0
    for cell, parts in (old_spatial or {}).items():
        new_parts = []
        for j, part in enumerate(parts):
            rel = f"history/{new_generation}/spatial/{typ}/cell={cell}/part-{j}.parquet"
            _place_file(root / part["path"], root / rel)
            new_parts.append({"path": rel, "rows": part["rows"], "bytes": part["bytes"]})
            total_bytes += part["bytes"]
        out[cell] = new_parts
    return out, total_bytes


def _hardlink_history_byid(root: Path, old_byid: list[dict], new_generation: str, typ: str) -> tuple[list[dict], int]:
    out: list[dict] = []
    total_bytes = 0
    for i, p in enumerate(old_byid or []):
        rel = f"history/{new_generation}/byid/{typ}/part-{i:05d}.parquet"
        _place_file(root / p["path"], root / rel)
        out.append({"path": rel, "min_id": p["min_id"], "max_id": p["max_id"], "rows": p["rows"], "bytes": p["bytes"]})
        total_bytes += p["bytes"]
    return out, total_bytes


def _compact_history_byid_parts(
    con, root: Path, old_byid: list[dict], touched_idx: set, typ: str, new_generation: str,
    pool_table: str, byid_cols: list[str],
) -> tuple[list[dict], int]:
    out: list[dict] = []
    total_bytes = 0
    next_idx = 0
    for i, p in enumerate(old_byid):
        if i in touched_idx:
            continue
        rel = f"history/{new_generation}/byid/{typ}/part-{next_idx:05d}.parquet"
        _place_file(root / p["path"], root / rel)
        out.append({"path": rel, "min_id": p["min_id"], "max_id": p["max_id"], "rows": p["rows"], "bytes": p["bytes"]})
        total_bytes += p["bytes"]
        next_idx += 1
    total = con.execute(f"SELECT count(*) FROM {pool_table}").fetchone()[0]
    for lo, hi in common.range_bounds(con, pool_table, "id", total):
        cond = common.range_cond("id", lo, hi)
        rel = f"history/{new_generation}/byid/{typ}/part-{next_idx:05d}.parquet"
        path = root / rel
        sel = f"SELECT {', '.join(byid_cols)} FROM {pool_table} WHERE {cond} ORDER BY id, valid_from"
        rows, size = common.copy_to_parquet(con, sel, path, row_group_size_bytes=1_000_000)
        next_idx += 1
        if rows == 0:
            if path.exists():
                path.unlink()
            continue
        min_id, max_id = _parquet_id_range(path)
        out.append({"path": rel, "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size})
        total_bytes += size
    return out, total_bytes


def _compact_history_type(
    con, root: Path, old_history: dict, typ: str, new_generation: str, promoted_keys: list[str],
) -> tuple[dict, list, int]:
    """Folds this type's history tiers into the base history (section 5.2):
    rewrites the touched spatial cells (old rows + tier rows, `valid_to`
    filled where a successor now exists) and the touched byid parts,
    hardlinks the rest. `valid_to` is computed *once*, globally per id, from
    the byid rows (one row per state -- no duplicate move-tombstone rows to
    confuse a LEAD-by-valid_from window function), then joined back onto
    both the byid and spatial outputs by ``(id, version, minor, valid_from,
    cell)`` -- the last, `cell`, is what keeps a move-tombstone row (same
    id/version/minor/valid_from as its state row, but at the *old* cell)
    from being mistaken for that state row and getting a spurious
    `valid_to`. Returns (spatial_fragment, byid_fragment, bytes_written)."""
    tiers = old_history.get("tiers") or {}
    old_spatial = (old_history.get("spatial") or {}).get(typ, {}) or {}
    old_byid = (old_history.get("byid") or {}).get(typ, []) or []

    tier_spatial_paths, tier_byid_paths = [], []
    touched_cells: set = set()
    for tier in history_schema.TIERS:
        entry = tiers.get(tier)
        if not entry:
            continue
        files = (entry.get("files") or {}).get(typ) or {}
        if files.get("spatial"):
            tier_spatial_paths.append(files["spatial"])
        if files.get("byid"):
            tier_byid_paths.append(files["byid"])
        touched_cells.update((entry.get("cells") or {}).get(typ, []) or [])

    if not tier_spatial_paths and not tier_byid_paths:
        spatial_fragment, sp_bytes = _hardlink_history_spatial(root, old_spatial, new_generation, typ)
        byid_fragment, by_bytes = _hardlink_history_byid(root, old_byid, new_generation, typ)
        return spatial_fragment, byid_fragment, sp_bytes + by_bytes

    byid_cols = history_schema.history_columns(BYID_COLUMNS[typ](promoted_keys))
    spatial_cols = history_schema.history_columns(SPATIAL_COLUMNS[typ](promoted_keys))

    # `_write_history_tier_version` always writes a tier's spatial and byid
    # files together (or neither), so the two path lists are either both
    # empty (handled above) or both non-empty here.
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _hist_new_spatial_{typ} AS "
        + " UNION ALL BY NAME ".join(f"SELECT * FROM read_parquet('{_esc(root / p)}')" for p in tier_spatial_paths)
    )
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _hist_new_byid_{typ} AS "
        + " UNION ALL BY NAME ".join(f"SELECT * FROM read_parquet('{_esc(root / p)}')" for p in tier_byid_paths)
    )

    lo, hi = con.execute(f"SELECT min(id), max(id) FROM _hist_new_byid_{typ}").fetchone()
    touched_idx: set = set()
    if lo is not None:
        for i, p in enumerate(old_byid):
            if p["max_id"] >= lo and p["min_id"] <= hi:
                touched_idx.add(i)
    touched_old_byid_paths = [old_byid[i]["path"] for i in sorted(touched_idx)]
    byid_pool_pieces = [f"SELECT * FROM read_parquet('{_esc(root / p)}')" for p in touched_old_byid_paths]
    byid_pool_pieces.append(f"SELECT * FROM _hist_new_byid_{typ}")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _hist_byid_pool_{typ} AS
        SELECT * EXCLUDE (valid_to) FROM ({' UNION ALL BY NAME '.join(byid_pool_pieces)}) u
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _hist_byid_filled_{typ} AS
        SELECT *, LEAD(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS valid_to
        FROM _hist_byid_pool_{typ}
    """)

    byid_fragment, byid_bytes = _compact_history_byid_parts(
        con, root, old_byid, touched_idx, typ, new_generation, f"_hist_byid_filled_{typ}", byid_cols,
    )

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _hist_valid_to_{typ} AS
        SELECT id, version, minor, valid_from, cell, valid_to FROM _hist_byid_filled_{typ}
    """)

    spatial_fragment: dict = {}
    spatial_bytes = 0
    all_cells = set(old_spatial) | touched_cells
    for cell in sorted(all_cells):
        if cell not in touched_cells:
            entry = old_spatial.get(cell)
            if entry:
                rel = f"history/{new_generation}/spatial/{typ}/cell={cell}/part-0.parquet"
                _place_file(root / entry[0]["path"], root / rel)
                spatial_fragment[cell] = [{"path": rel, "rows": entry[0]["rows"], "bytes": entry[0]["bytes"]}]
                spatial_bytes += entry[0]["bytes"]
            continue
        parts_sql = []
        old_entry = old_spatial.get(cell)
        if old_entry:
            old_path = root / old_entry[0]["path"]
            parts_sql.append(f"SELECT * FROM read_parquet('{_esc(old_path)}')")
        cell_esc = cell.replace("'", "''")
        parts_sql.append(f"SELECT * FROM _hist_new_spatial_{typ} WHERE cell = '{cell_esc}'")
        rel = f"history/{new_generation}/spatial/{typ}/cell={cell}/part-0.parquet"
        path = root / rel
        proj = ", ".join(("vt.valid_to" if c == "valid_to" else f"s.{c}") for c in spatial_cols)
        sel = f"""
            SELECT {proj}
            FROM ({' UNION ALL BY NAME '.join(parts_sql)}) s
            LEFT JOIN _hist_valid_to_{typ} vt
              ON vt.id = s.id AND vt.version = s.version AND vt.minor = s.minor
             AND vt.valid_from = s.valid_from AND vt.cell = s.cell
            ORDER BY s.hilbert, s.id, s.valid_from
        """
        rows, size = common.copy_to_parquet(con, sel, path, row_group_size_bytes=1_000_000)
        if rows == 0:
            if path.exists():
                path.unlink()
            continue
        spatial_fragment[cell] = [{"path": rel, "rows": rows, "bytes": size}]
        spatial_bytes += size

    for t in (f"_hist_new_spatial_{typ}", f"_hist_new_byid_{typ}", f"_hist_byid_pool_{typ}", f"_hist_byid_filled_{typ}"):
        con.execute(f"DROP TABLE IF EXISTS {t}")

    return spatial_fragment, byid_fragment, spatial_bytes + byid_bytes


def _history_stats(con, root: Path, byid_out: dict, spatial_out: dict) -> dict:
    rows: dict = {}
    minor_rows: dict = {}
    total_bytes = 0
    for typ, parts in byid_out.items():
        rows[typ] = sum(p["rows"] for p in parts)
        total_bytes += sum(p["bytes"] for p in parts)
        if parts:
            paths = [str(root / p["path"]) for p in parts]
            minor_rows[typ] = con.execute(f"SELECT count(*) FROM read_parquet({paths!r}) WHERE minor > 0").fetchone()[0]
        else:
            minor_rows[typ] = 0
    for _typ, cells in spatial_out.items():
        total_bytes += sum(p["bytes"] for parts in cells.values() for p in parts)
    return {"rows": rows, "minor_rows": minor_rows, "bytes": total_bytes}


def _compact_history(con, root: Path, old_man: dict, new_generation: str, promoted_keys: list[str]) -> tuple[Optional[dict], int]:
    old_history = old_man.get("history")
    if not old_history:
        return None, 0
    spatial_out: dict = {}
    byid_out: dict = {}
    total_bytes = 0
    for typ in ("node", "way", "relation"):
        sp, by, nbytes = _compact_history_type(con, root, old_history, typ, new_generation, promoted_keys)
        spatial_out[typ] = sp
        byid_out[typ] = by
        total_bytes += nbytes
    new_history = {
        "generation": new_generation,
        "since": old_history.get("since"),
        "minor_versions": old_history.get("minor_versions", True),
        "spatial": spatial_out,
        "byid": byid_out,
        "tiers": {},
        "stats": _history_stats(con, root, byid_out, spatial_out),
    }
    return new_history, total_bytes


# --------------------------------------------------------------------------
# top-level orchestration
# --------------------------------------------------------------------------


def compact(opts: CompactOptions) -> dict:
    t_start = time.time()
    timer = common.Timer()
    root = Path(opts.root)
    manifest_dir = root / "manifest"
    latest_num = int((manifest_dir / "LATEST").read_text().strip())
    old_man = json.loads((manifest_dir / f"{latest_num}.json").read_text())
    old_generation = old_man["generation"]
    man_deltas = old_man.get("deltas") or {}
    promoted_keys = list(old_man.get("promoted_keys") or DEFAULT_PROMOTED_KEYS)

    new_generation = opts.generation or f"g{(latest_num + 1):04d}"
    _log(f"compacting {root} generation {old_generation} -> {new_generation} ...")

    tmpdir = Path(opts.tmpdir) if opts.tmpdir else Path.cwd() / ".osmpq-compact-tmp"
    tmpdir.mkdir(parents=True, exist_ok=True)

    import duckdb

    db_path = tmpdir / "compact.duckdb"
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    con.execute("SET TimeZone='UTC'")
    if opts.threads:
        con.execute(f"SET threads={int(opts.threads)}")
    if opts.memory_limit:
        con.execute(f"SET memory_limit='{opts.memory_limit}'")
    con.execute(f"SET temp_directory='{tmpdir.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    # `_compact_areas` (below) calls into `osmpq.build.areas`, whose
    # relation-area member-way hydration can fall back to `sources.py`'s
    # byid path, which references the `opq_node_hilbert`/`opq_bbox_hilbert`
    # UDFs the engine normally registers on its own connection.
    from osmpq.engine import hilbert as engine_hilbert_mod

    engine_hilbert_mod.register_duckdb_udfs(con)

    promoted_sql = common.promoted_select(promoted_keys)
    bytes_by_kind = {"spatial": 0, "byid": 0, "index": 0}

    # ---- node -------------------------------------------------------------------
    has_node_spatial = _build_winners(con, root, man_deltas, "node", "spatial", "node_spatial_winners")
    has_node_byid = _build_winners(con, root, man_deltas, "node", "byid", "node_byid_winners")
    node_touched_cells = _touched_cells(con, root, man_deltas, "node", "node_spatial_winners", has_node_spatial)
    node_table_manifest, node_spatial_bytes = _compact_node_spatial(
        con, root, old_man.get("tables", {}).get("node", {}).get("cells", {}), new_generation,
        promoted_sql, "node_spatial_winners", has_node_spatial, node_touched_cells,
    )
    bytes_by_kind["spatial"] += node_spatial_bytes
    _log(f"node spatial: {len(node_touched_cells)} touched cell(s) rewritten in {timer.lap('node_spatial'):.2f}s")

    node_byid_manifest, node_byid_bytes = _compact_byid_table(
        con, root, old_man.get("byid", {}).get("node", []) or [], "node", new_generation,
        _node_byid_cols(promoted_sql), "node_byid_winners", has_node_byid, {"row_group_size": NODE_BYID_ROW_GROUP},
    )
    bytes_by_kind["byid"] += node_byid_bytes
    _log(f"node byid: {len(node_byid_manifest)} part(s) in {timer.lap('node_byid'):.2f}s")

    # ---- way ----------------------------------------------------------------------
    has_way_spatial = _build_winners(con, root, man_deltas, "way", "spatial", "way_spatial_winners")
    has_way_byid = _build_winners(con, root, man_deltas, "way", "byid", "way_byid_winners")
    way_touched_cells = _touched_cells(con, root, man_deltas, "way", "way_spatial_winners", has_way_spatial)
    way_table_manifest, way_spatial_bytes = _compact_flat_spatial(
        con, root, old_man.get("tables", {}).get("way", {}).get("cells", {}), "way", new_generation,
        _way_spatial_cols(promoted_sql), "way_spatial_winners", has_way_spatial, way_touched_cells,
        {"row_group_size_bytes": WAY_SPATIAL_ROW_GROUP_BYTES},
    )
    bytes_by_kind["spatial"] += way_spatial_bytes
    _log(f"way spatial: {len(way_touched_cells)} touched cell(s) rewritten in {timer.lap('way_spatial'):.2f}s")

    old_way_byid_parts = old_man.get("byid", {}).get("way", []) or []
    way_byid_manifest, way_byid_bytes = _compact_byid_table(
        con, root, old_way_byid_parts, "way", new_generation,
        _way_byid_cols(promoted_sql), "way_byid_winners", has_way_byid, {"row_group_size_bytes": WAY_BYID_ROW_GROUP_BYTES},
    )
    bytes_by_kind["byid"] += way_byid_bytes
    _log(f"way byid: {len(way_byid_manifest)} part(s) in {timer.lap('way_byid'):.2f}s")

    # ---- relation -------------------------------------------------------------------
    has_rel_spatial = _build_winners(con, root, man_deltas, "relation", "spatial", "relation_spatial_winners")
    has_rel_byid = _build_winners(con, root, man_deltas, "relation", "byid", "relation_byid_winners")
    rel_touched_cells = _touched_cells(con, root, man_deltas, "relation", "relation_spatial_winners", has_rel_spatial)
    relation_table_manifest, rel_spatial_bytes = _compact_flat_spatial(
        con, root, old_man.get("tables", {}).get("relation", {}).get("cells", {}), "relation", new_generation,
        _relation_spatial_cols(promoted_sql), "relation_spatial_winners", has_rel_spatial, rel_touched_cells,
        {"row_group_size": RELATION_ROW_GROUP},
    )
    bytes_by_kind["spatial"] += rel_spatial_bytes
    _log(f"relation spatial: {len(rel_touched_cells)} touched cell(s) rewritten in {timer.lap('relation_spatial'):.2f}s")

    relation_byid_manifest, rel_byid_bytes = _compact_byid_table(
        con, root, old_man.get("byid", {}).get("relation", []) or [], "relation", new_generation,
        _relation_byid_cols(promoted_sql), "relation_byid_winners", has_rel_byid, {"row_group_size": RELATION_ROW_GROUP},
    )
    bytes_by_kind["byid"] += rel_byid_bytes
    _log(f"relation byid: {len(relation_byid_manifest)} part(s) in {timer.lap('relation_byid'):.2f}s")

    # ---- node_way index ---------------------------------------------------------------
    node_way_manifest, node_way_bytes = _compact_node_way_index(
        con, root, old_man.get("index", {}).get("node_way", []) or [], old_way_byid_parts,
        way_byid_manifest, new_generation, has_way_byid, "way_byid_winners", has_node_byid, "node_byid_winners",
    )
    bytes_by_kind["index"] += node_way_bytes
    _log(f"node_way index: {len(node_way_manifest)} part(s) in {timer.lap('node_way_index'):.2f}s")

    # ---- member index (full rebuild) ---------------------------------------------------
    member_manifest, member_bytes = _compact_member_index(con, root, relation_byid_manifest, new_generation)
    bytes_by_kind["index"] += member_bytes
    _log(f"member index: {sum(p['rows'] for p in member_manifest)} row(s) in {timer.lap('member_index'):.2f}s")

    # ---- row-group index ----------------------------------------------------------------
    node_rg_path, node_rg_rows = rowgroups_mod.write_rowgroup_index(con, str(root), new_generation, "node", _node_rg_files(node_table_manifest))
    way_rg_path, way_rg_rows = rowgroups_mod.write_rowgroup_index(con, str(root), new_generation, "way", _flat_rg_files(way_table_manifest))
    relation_rg_path, relation_rg_rows = rowgroups_mod.write_rowgroup_index(con, str(root), new_generation, "relation", _flat_rg_files(relation_table_manifest))
    for p in (node_rg_path, way_rg_path, relation_rg_path):
        bytes_by_kind["index"] += (root / p).stat().st_size
    _log(
        f"rowgroup index ({node_rg_rows} node, {way_rg_rows} way, {relation_rg_rows} relation rows) "
        f"in {timer.lap('rowgroup_index'):.2f}s"
    )

    # ---- manifest -------------------------------------------------------------------------
    new_man = copy.deepcopy(old_man)
    new_man["generation"] = new_generation
    new_man["manifest_version"] = 3
    new_man["deltas"] = {}
    new_man["tables"] = {"node": node_table_manifest, "way": way_table_manifest, "relation": relation_table_manifest}
    new_man["byid"] = {"node": node_byid_manifest, "way": way_byid_manifest, "relation": relation_byid_manifest}
    new_man["index"] = {"node_way": node_way_manifest, "member": member_manifest}
    new_man["rowgroup_index"] = {"node": node_rg_path, "way": way_rg_path, "relation": relation_rg_path}

    # ---- areas (docs/m3-contracts.md section 9.2) --------------------------------------
    areas_field, areas_bytes = _compact_areas(
        con, root, old_man, new_man, new_generation, promoted_keys,
        way_byid_manifest, has_rel_byid, "relation_byid_winners",
    )
    if areas_field is not None:
        new_man["areas"] = areas_field
        new_man["manifest_version"] = 4
        bytes_by_kind["index"] += areas_bytes
        _log(
            f"areas: {areas_field['index']['rows']} relation area(s) across {len(areas_field['cells'])} cell(s), "
            f"{areas_field['way_index']['rows']} way area(s) indexed, in {timer.lap('areas'):.2f}s"
        )

    # ---- history (docs/m4-contracts.md section 5.2) ------------------------------------
    history_field, history_bytes = _compact_history(con, root, old_man, new_generation, promoted_keys)
    if history_field is not None:
        new_man["history"] = history_field
        new_man["manifest_version"] = max(int(new_man["manifest_version"]), 5)
        bytes_by_kind["history"] = history_bytes
        _log(
            f"history: folded {sum(history_field['stats']['rows'].values())} row(s) into "
            f"generation {new_generation} in {timer.lap('history'):.2f}s"
        )

    n_nodes = sum(p["rows"] for p in node_byid_manifest)
    n_tagged_nodes = sum(e.get("tagged", {}).get("rows", 0) for e in node_table_manifest["cells"].values())
    n_ways = sum(p["rows"] for p in way_byid_manifest)
    n_relations = sum(p["rows"] for p in relation_byid_manifest)
    old_stats = old_man.get("stats", {})
    new_man["stats"] = {
        "nodes": n_nodes,
        "tagged_nodes": n_tagged_nodes,
        "ways": n_ways,
        "relations": n_relations,
        "leaf_cells": old_stats.get("leaf_cells", len(old_man.get("leaf_cells", []))),
        "bytes": bytes_by_kind,
    }
    if areas_field is not None:
        new_man["stats"]["areas"] = areas_field["index"]["rows"]
        new_man["stats"]["way_areas"] = areas_field["way_index"]["rows"]

    gen_number = latest_num + 1
    manifest_dir.mkdir(parents=True, exist_ok=True)
    # Write via temp file + rename: manifest files may be hardlinked across
    # snapshot copies (cp -al), and an in-place write would change every copy.
    for name, text in ((f"{gen_number}.json", json.dumps(new_man, indent=2, sort_keys=False)),
                       ("LATEST", str(gen_number))):
        tmp = manifest_dir / f".{name}.tmp"
        tmp.write_text(text)
        os.replace(tmp, manifest_dir / name)
    _log(f"wrote manifest/{gen_number}.json and manifest/LATEST")
    _log(f"done in {time.time() - t_start:.2f}s total")

    con.close()
    if db_path.exists():
        db_path.unlink()
    return new_man


# --------------------------------------------------------------------------
# CLI entry point (coordinator wires this into osmpq.cli)
# --------------------------------------------------------------------------


def compact_main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="osmpq compact", description=__doc__)
    p.add_argument("root")
    p.add_argument("--generation", default=None)
    p.add_argument("--threads", type=int, default=None)
    p.add_argument("--memory-limit", default=None)
    p.add_argument("--tmpdir", default=None)
    args = p.parse_args(argv if argv is not None else sys.argv[1:])
    compact(CompactOptions(
        root=args.root, generation=args.generation, threads=args.threads,
        memory_limit=args.memory_limit, tmpdir=args.tmpdir,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(compact_main())
