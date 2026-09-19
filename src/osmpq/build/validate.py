"""``osmpq validate <root>``: docs/m1-contracts.md section 7.

Checks every manifest path exists, row counts match, byid parts are sorted
by id with non-overlapping ranges, spatial files are sorted by
``(hilbert, id)``, every way/relation's cell fully contains its bbox and
obeys the v2 depth rule, and the row-group index covers every spatial file
and its row-group count. Prints a summary; the caller exits non-zero when
problems are found.
"""
from __future__ import annotations

from pathlib import Path

from osmpq.layout import cells as cells_mod
from osmpq.layout import manifest as manifest_mod


def validate(root: str) -> tuple[bool, list[str]]:
    problems: list[str] = []
    info: list[str] = []

    man = manifest_mod.load_latest(root)
    root_path = Path(root)
    is_v2 = man.manifest_version >= 2

    import duckdb

    con = duckdb.connect()
    # Keep insertion order preserved (the default): the ordering checks below
    # rely on read_parquet() returning rows in on-disk order, with no
    # ORDER BY, so they actually detect an unsorted file rather than just
    # re-sorting it first.
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")

    # ---- 1. every manifest path exists, row counts match -----------------------
    checked = 0
    for rel_path, expected_rows in _paths_and_rows(man):
        full = root_path / rel_path
        checked += 1
        if not full.exists():
            problems.append(f"missing file: {rel_path}")
            continue
        if expected_rows is None:
            continue
        n = con.execute(f"SELECT count(*) FROM read_parquet('{_esc(full)}')").fetchone()[0]
        if n != expected_rows:
            problems.append(f"row count mismatch in {rel_path}: manifest says {expected_rows}, file has {n}")
    info.append(f"checked {checked} manifest paths")

    # ---- 2. byid parts sorted by id, non-overlapping ----------------------------
    for table, parts in man.byid.items():
        parts_sorted = sorted(parts, key=lambda p: (p["min_id"] if p["min_id"] is not None else 0))
        for a, b in zip(parts_sorted, parts_sorted[1:]):
            if a["max_id"] is not None and b["min_id"] is not None and a["max_id"] >= b["min_id"]:
                problems.append(f"byid/{table} parts overlap: {a['path']} (max {a['max_id']}) vs {b['path']} (min {b['min_id']})")
        for p in parts:
            full = root_path / p["path"]
            if not full.exists():
                continue
            # On-disk order (no ORDER BY): must already be non-decreasing.
            bad = con.execute(f"""
                SELECT count(*) FROM (
                    SELECT id, lag(id) OVER () AS prev FROM read_parquet('{_esc(full)}')
                ) WHERE prev IS NOT NULL AND id <= prev
            """).fetchone()[0]
            if bad:
                problems.append(f"byid/{table} part not strictly sorted by id: {p['path']} ({bad} out-of-order rows)")
    info.append("checked byid part ordering / overlap")

    # ---- 3. spatial files sorted by (hilbert, id) -------------------------------
    spatial_way_paths: list[tuple[str, dict]] = []
    spatial_relation_paths: list[tuple[str, dict]] = []
    for table, spec in man.tables.items():
        for cell, entry in spec.get("cells", {}).items():
            files: list[str] = []
            if "path" in entry:
                files.append(entry["path"])
                if table == "way":
                    spatial_way_paths.append((entry["path"], entry))
                elif table == "relation":
                    spatial_relation_paths.append((entry["path"], entry))
            else:
                for part in ("tagged", "untagged"):
                    if entry.get(part):
                        files.append(entry[part]["path"])
            for rel_path in files:
                full = root_path / rel_path
                if not full.exists():
                    continue
                # Check the file's *on-disk* row order (no ORDER BY) is
                # already non-decreasing by (hilbert, id).
                bad2 = con.execute(f"""
                    SELECT count(*) FROM (
                        SELECT hilbert, id,
                               lag(hilbert) OVER () AS ph, lag(id) OVER () AS pid
                        FROM read_parquet('{_esc(full)}')
                    ) WHERE ph IS NOT NULL AND (hilbert < ph OR (hilbert = ph AND id < pid))
                """).fetchone()[0]
                if bad2:
                    problems.append(f"spatial file not sorted by (hilbert, id): {rel_path} ({bad2} out-of-order rows)")
    info.append("checked spatial file (hilbert, id) ordering")

    # ---- 4. way/relation cell contains bbox + v2 depth rule ---------------------
    leaf_set = set(man.leaf_cells)
    ancestor_depths = set(man.ancestor_depths or cells_mod.DEFAULT_ANCESTOR_DEPTHS) if is_v2 else None
    for label, plist in (("way", spatial_way_paths), ("relation", spatial_relation_paths)):
        for rel_path, _entry in plist:
            full = root_path / rel_path
            if not full.exists():
                continue
            rows = con.execute(
                f"SELECT cell, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM read_parquet('{_esc(full)}') "
                "WHERE xmin_e7 IS NOT NULL"
            ).fetchall()
            for cell, ymin, xmin, ymax, xmax in rows:
                south, west, north, east = ymin / 1e7, xmin / 1e7, ymax / 1e7, xmax / 1e7
                c_south, c_west, c_north, c_east = cells_mod.cell_bbox(cell)
                if not (south >= c_south and west >= c_west and north <= c_north and east <= c_east):
                    problems.append(f"{label} cell {cell} does not contain its bbox in {rel_path}")
                    continue
                if is_v2:
                    depth = 0 if cell == cells_mod.ROOT else len(cell)
                    if cell not in leaf_set and depth not in ancestor_depths:
                        problems.append(
                            f"{label} cell {cell} in {rel_path} is neither a leaf nor at an allowed ancestor depth {sorted(ancestor_depths)}"
                        )
    info.append(f"checked {len(spatial_way_paths)} way + {len(spatial_relation_paths)} relation spatial files for cell placement")

    # ---- 4b. areas (docs/m3-contracts.md section 4.2): spatial cell files,
    # sorted (hilbert, id) like way/relation, and cell-contains-bbox / depth
    # rule; the index file, sorted by id. ----------------------------------
    if man.areas and man.areas.get("index"):
        area_cells = man.areas.get("cells", {})
        for cell, entry in area_cells.items():
            rel_path = entry["path"]
            full = root_path / rel_path
            if not full.exists():
                continue
            bad = con.execute(f"""
                SELECT count(*) FROM (
                    SELECT hilbert, id, lag(hilbert) OVER () AS ph, lag(id) OVER () AS pid
                    FROM read_parquet('{_esc(full)}')
                ) WHERE ph IS NOT NULL AND (hilbert < ph OR (hilbert = ph AND id < pid))
            """).fetchone()[0]
            if bad:
                problems.append(f"area spatial file not sorted by (hilbert, id): {rel_path} ({bad} out-of-order rows)")
            rows = con.execute(
                f"SELECT ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM read_parquet('{_esc(full)}') WHERE xmin_e7 IS NOT NULL"
            ).fetchall()
            for ymin, xmin, ymax, xmax in rows:
                south, west, north, east = ymin / 1e7, xmin / 1e7, ymax / 1e7, xmax / 1e7
                c_south, c_west, c_north, c_east = cells_mod.cell_bbox(cell)
                if not (south >= c_south and west >= c_west and north <= c_north and east <= c_east):
                    problems.append(f"area cell {cell} does not contain its bbox in {rel_path}")
                    continue
                if is_v2:
                    depth = 0 if cell == cells_mod.ROOT else len(cell)
                    if cell not in leaf_set and depth not in ancestor_depths:
                        problems.append(
                            f"area cell {cell} in {rel_path} is neither a leaf nor at an allowed ancestor depth {sorted(ancestor_depths)}"
                        )
        index_path = root_path / man.areas["index"]["path"]
        if index_path.exists():
            # Sorted, not *strictly* sorted: relation ids are unique on
            # their own, but a non-decreasing check is all the invariant
            # actually requires and stays correct even if that ever
            # changes.
            bad_idx = con.execute(f"""
                SELECT count(*) FROM (
                    SELECT id, lag(id) OVER () AS prev FROM read_parquet('{_esc(index_path)}')
                ) WHERE prev IS NOT NULL AND id < prev
            """).fetchone()[0]
            if bad_idx:
                problems.append(f"index/areas.parquet not sorted by id ({bad_idx} out-of-order rows)")
        info.append(f"checked {len(area_cells)} area spatial file(s) + the area index")

    # ---- 4c. way_areas index (docs/m3-contracts.md section 9.2): no
    # spatial files (no stored geometry), just the index sorted by id. -----
    way_index = man.areas.get("way_index") if man.areas else None
    if way_index:
        way_index_path = root_path / way_index["path"]
        if way_index_path.exists():
            bad_way_idx = con.execute(f"""
                SELECT count(*) FROM (
                    SELECT id, lag(id) OVER () AS prev FROM read_parquet('{_esc(way_index_path)}')
                ) WHERE prev IS NOT NULL AND id < prev
            """).fetchone()[0]
            if bad_way_idx:
                problems.append(f"index/way_areas.parquet not sorted by id ({bad_way_idx} out-of-order rows)")
        info.append("checked the way_areas index")

    # ---- 5. row-group index coverage --------------------------------------------
    if is_v2 and man.rowgroup_index:
        import pyarrow.parquet as pq

        for table_name, rg_path in man.rowgroup_index.items():
            full_rg = root_path / rg_path
            if not full_rg.exists():
                problems.append(f"missing rowgroup index: {rg_path}")
                continue
            rg_rows = con.execute(f"SELECT path, count(*) FROM read_parquet('{_esc(full_rg)}') GROUP BY path").fetchall()
            rg_counts = dict(rg_rows)
            spec = man.tables.get(table_name, {})
            expected_paths: set[str] = set()
            for entry in spec.get("cells", {}).values():
                if "path" in entry:
                    expected_paths.add(entry["path"])
                else:
                    for part in ("tagged", "untagged"):
                        if entry.get(part):
                            expected_paths.add(entry[part]["path"])
            for rel_path in expected_paths:
                full = root_path / rel_path
                if not full.exists():
                    continue
                actual_rg_count = pq.ParquetFile(str(full)).metadata.num_row_groups
                indexed_count = rg_counts.get(rel_path, 0)
                if indexed_count != actual_rg_count:
                    problems.append(
                        f"rowgroup index for {table_name} missing/mismatched coverage of {rel_path}: "
                        f"index has {indexed_count} row groups, file has {actual_rg_count}"
                    )
            extra = set(rg_counts) - expected_paths
            if extra:
                problems.append(f"rowgroup index for {table_name} references {len(extra)} file(s) not in the manifest")
        info.append("checked rowgroup index coverage")
    elif is_v2:
        problems.append("manifest_version 2 but rowgroup_index is empty")

    con.close()
    ok = not problems
    summary = [f"osmpq validate: {root}", f"generation: {man.generation}  manifest_version: {man.manifest_version}"]
    summary.extend(f"  ok: {line}" for line in info)
    if ok:
        summary.append("PASS: no problems found")
    else:
        summary.append(f"FAIL: {len(problems)} problem(s) found")
        summary.extend(f"  - {p}" for p in problems)
    return ok, summary


def _esc(path: Path) -> str:
    return str(path).replace("'", "''")


def _paths_and_rows(man: manifest_mod.Manifest) -> list[tuple[str, int | None]]:
    out: list[tuple[str, int | None]] = []
    for spec in man.tables.values():
        for entry in spec.get("cells", {}).values():
            if "path" in entry:
                out.append((entry["path"], entry.get("rows")))
            else:
                for part in ("tagged", "untagged"):
                    if entry.get(part):
                        out.append((entry[part]["path"], entry[part].get("rows")))
    for parts in man.byid.values():
        for p in parts:
            out.append((p["path"], p.get("rows")))
    for parts in man.index.values():
        for p in parts:
            out.append((p["path"], p.get("rows")))
    for path in man.rowgroup_index.values():
        out.append((path, None))
    if man.areas:
        index_entry = man.areas.get("index")
        if index_entry:
            out.append((index_entry["path"], index_entry.get("rows")))
        way_index_entry = man.areas.get("way_index")
        if way_index_entry:
            out.append((way_index_entry["path"], way_index_entry.get("rows")))
        for entry in man.areas.get("cells", {}).values():
            out.append((entry["path"], entry.get("rows")))
    return out
