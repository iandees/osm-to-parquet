"""M1 builder: two stages over docs/m1-contracts.md.

``build(opts)`` is the M0-compatible entry point (``osmpq build <pbf>
<root>``): it runs :func:`osmpq.build.raw.raw_build` into a temp ``raw/``
directory and then :func:`build_from_raw`, per m1-contracts.md section 7.

``build_from_raw(opts)`` is ``osmpq build --raw <rawdir> <root>``: it places
the raw node/way files (link/copy/move), computes relation
bbox/cell/hilbert/centroid from the raw parts (a join, not a full node
scan), writes the member index, builds the row-group index side files from
Parquet footers, and writes manifest v2. It works from *either* producer's
``raw/`` layout (``osmpq-raw`` or ``osmpq raw-py``).
"""
from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

from osmpq.build import common
from osmpq.build import rowgroups as rowgroups_mod
from osmpq.build.raw import RawBuildOptions, raw_build
from osmpq.layout import cells as cells_mod
from osmpq.layout import hilbert as hilbert_mod
from osmpq.layout import manifest as manifest_mod

BUILD_PRODUCER_NAME = "osmpq 0.1.0"
RELATION_PART_ROWS = common.RANGE_TARGET_ROWS


def _log(msg: str) -> None:
    common.log("osmpq build", msg)


# --------------------------------------------------------------------------
# M0-compatible entry point: PBF -> root, via a temp raw/ dir
# --------------------------------------------------------------------------


@dataclass
class BuildOptions:
    pbf_path: str
    root: str
    bbox: Optional[tuple[float, float, float, float]] = None  # s,w,n,e
    generation: Optional[str] = None
    max_nodes_per_cell: int = 1_000_000
    max_depth: int = cells_mod.DEFAULT_MAX_DEPTH_V2
    promoted_keys: Optional[list[str]] = None
    timestamp: Optional[str] = None
    replication_sequence: Optional[int] = None
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    tmpdir: Optional[str] = None
    mode: str = "link"  # link|copy|move, for placing the intermediate raw/ files
    run_areas: bool = True  # docs/m3-contracts.md section 4.3: run `osmpq areas` at the end


def build(opts: BuildOptions) -> manifest_mod.Manifest:
    """``osmpq build <pbf> <root>``: raw-py into a temp dir, then build --raw."""
    tmpdir = Path(opts.tmpdir) if opts.tmpdir else Path.cwd() / ".osmpq-tmp"
    tmpdir.mkdir(parents=True, exist_ok=True)
    rawdir = tmpdir / "raw"
    raw_opts = RawBuildOptions(
        pbf_path=opts.pbf_path,
        rawdir=str(rawdir),
        bbox=opts.bbox,
        max_nodes_per_cell=opts.max_nodes_per_cell,
        max_depth=opts.max_depth,
        promoted_keys=opts.promoted_keys,
        threads=opts.threads,
        memory_limit=opts.memory_limit,
        tmpdir=str(tmpdir / "raw-build-tmp"),
    )
    raw_build(raw_opts)
    from_raw_opts = BuildFromRawOptions(
        rawdir=str(rawdir),
        root=opts.root,
        generation=opts.generation,
        timestamp=opts.timestamp,
        replication_sequence=opts.replication_sequence,
        threads=opts.threads,
        memory_limit=opts.memory_limit,
        tmpdir=str(tmpdir / "build-from-raw-tmp"),
        mode=opts.mode,
        run_areas=opts.run_areas,
    )
    return build_from_raw(from_raw_opts)


# --------------------------------------------------------------------------
# build --raw: raw/ -> dataset root, manifest v2
# --------------------------------------------------------------------------


@dataclass
class BuildFromRawOptions:
    rawdir: str
    root: str
    generation: Optional[str] = None
    timestamp: Optional[str] = None
    replication_sequence: Optional[int] = None
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    tmpdir: Optional[str] = None
    mode: str = "link"  # link|copy|move
    run_areas: bool = True  # docs/m3-contracts.md section 4.3: run `osmpq areas` at the end
    extent: Optional[tuple[float, float, float, float]] = None  # (S, W, N, E): the intended
    # coverage of a regional dataset; the updater keeps new elements inside it. Defaults to the
    # data bbox from raw/summary.json, which is wider than the cut bbox for extracts because a
    # kept way keeps all of its nodes.


def build_from_raw(opts: BuildFromRawOptions) -> manifest_mod.Manifest:
    t_start = time.time()
    timer = common.Timer()
    rawdir = Path(opts.rawdir)
    root_is_local = "://" not in opts.root
    if root_is_local:
        Path(opts.root).mkdir(parents=True, exist_ok=True)
    tmpdir = Path(opts.tmpdir) if opts.tmpdir else Path.cwd() / ".osmpq-build-raw-tmp"
    tmpdir.mkdir(parents=True, exist_ok=True)

    leaves_json = json.loads((rawdir / "leaves.json").read_text())
    leaves = list(leaves_json["leaves"])
    max_depth = int(leaves_json.get("max_depth", cells_mod.DEFAULT_MAX_DEPTH_V2))
    ancestor_depths = list(leaves_json.get("ancestor_depths", cells_mod.DEFAULT_ANCESTOR_DEPTHS))
    leaf_index = cells_mod.LeafIndex(leaves)

    summary_path = rawdir / "summary.json"
    raw_summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    promoted_keys = list(raw_summary.get("promoted_keys") or manifest_mod.DEFAULT_PROMOTED_KEYS)
    producer_raw = raw_summary.get("producer", "unknown")
    source = raw_summary.get("source", rawdir.name)

    gen_number = manifest_mod.next_manifest_number(opts.root)
    generation = opts.generation or f"g{gen_number:04d}"
    _log(f"writing generation {generation} to {opts.root} from raw {rawdir} ...")

    import duckdb

    db_path = tmpdir / "build-from-raw.duckdb"
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

    root = Path(opts.root)
    bytes_by_kind = {"spatial": 0, "byid": 0, "index": 0}

    # ---- place node/way byid + spatial files as-is -----------------------------
    node_byid_manifest, n_bytes = _place_and_index_byid(rawdir / "node", root / "byid" / generation / "node", opts.mode)
    bytes_by_kind["byid"] += n_bytes
    way_byid_manifest, w_bytes = _place_and_index_byid(rawdir / "way", root / "byid" / generation / "way", opts.mode)
    bytes_by_kind["byid"] += w_bytes
    _log(f"placed byid node/way files in {timer.lap('place_byid'):.1f}s")

    node_table_manifest, node_bytes = _place_node_spatial(rawdir, root, generation, opts.mode)
    bytes_by_kind["spatial"] += node_bytes
    way_table_manifest, way_bytes, way_files_for_rg = _place_way_spatial(rawdir, root, generation, opts.mode)
    bytes_by_kind["spatial"] += way_bytes
    _log(f"placed spatial node/way files in {timer.lap('place_spatial'):.1f}s")

    node_way_manifest, nw_bytes = _place_node_way_index(rawdir, root, generation, opts.mode)
    bytes_by_kind["index"] += nw_bytes
    _log(f"placed node_way index in {timer.lap('place_node_way'):.1f}s")

    # ---- relations: bbox/cell/hilbert/centroid, byid + spatial + member index --
    relation_table_manifest, relation_byid_manifest, member_manifest, rel_bytes = _build_relations(
        con, rawdir, root, generation, promoted_keys, leaf_index, ancestor_depths, max_depth
    )
    bytes_by_kind["spatial"] += rel_bytes["spatial"]
    bytes_by_kind["byid"] += rel_bytes["byid"]
    bytes_by_kind["index"] += rel_bytes["member"]
    _log(f"computed + wrote relations in {timer.lap('relations'):.1f}s")

    # ---- row-group index side files ---------------------------------------------
    node_rg_files = _node_rowgroup_files(node_table_manifest, generation)
    way_rg_files = way_files_for_rg
    relation_rg_files = _relation_rowgroup_files(relation_table_manifest, generation)
    node_rg_path, node_rg_rows = rowgroups_mod.write_rowgroup_index(con, opts.root, generation, "node", node_rg_files)
    way_rg_path, way_rg_rows = rowgroups_mod.write_rowgroup_index(con, opts.root, generation, "way", way_rg_files)
    relation_rg_path, relation_rg_rows = rowgroups_mod.write_rowgroup_index(
        con, opts.root, generation, "relation", relation_rg_files
    )
    for p in (node_rg_path, way_rg_path, relation_rg_path):
        bytes_by_kind["index"] += (root / p).stat().st_size
    _log(
        f"built rowgroup index ({node_rg_rows} node, {way_rg_rows} way, {relation_rg_rows} relation rows) "
        f"in {timer.lap('rowgroup_index'):.1f}s"
    )

    # ---- manifest -----------------------------------------------------------------
    if opts.timestamp:
        timestamp_osm_base = opts.timestamp
    elif raw_summary.get("max_timestamp"):
        timestamp_osm_base = raw_summary["max_timestamp"]
    else:
        timestamp_osm_base = "1970-01-01T00:00:00Z"

    extent = list(opts.extent) if opts.extent else (raw_summary.get("extent") or [0.0, 0.0, 0.0, 0.0])

    n_nodes = sum(p["rows"] for p in node_byid_manifest)
    n_tagged_nodes = sum(
        entry.get("tagged", {}).get("rows", 0) for entry in node_table_manifest["cells"].values()
    )
    n_ways = sum(p["rows"] for p in way_byid_manifest)
    n_relations = sum(p["rows"] for p in relation_byid_manifest)

    man = manifest_mod.Manifest(
        generation=generation,
        timestamp_osm_base=timestamp_osm_base,
        source=source,
        extent=[float(x) for x in extent],
        leaf_cells=sorted(leaves),
        tables={"node": node_table_manifest, "way": way_table_manifest, "relation": relation_table_manifest},
        byid={"node": node_byid_manifest, "way": way_byid_manifest, "relation": relation_byid_manifest},
        index={"node_way": node_way_manifest, "member": member_manifest},
        promoted_keys=promoted_keys,
        replication_sequence=opts.replication_sequence,
        manifest_version=2,
        ancestor_depths=ancestor_depths,
        max_depth=max_depth,
        rowgroup_index={"node": node_rg_path, "way": way_rg_path, "relation": relation_rg_path},
        producer={"raw": producer_raw, "build": BUILD_PRODUCER_NAME},
        stats={
            "nodes": n_nodes,
            "tagged_nodes": n_tagged_nodes,
            "ways": n_ways,
            "relations": n_relations,
            "leaf_cells": len(leaves),
            "bytes": bytes_by_kind,
        },
    )
    con.close()
    if db_path.exists():
        db_path.unlink()

    if opts.run_areas:
        # docs/m3-contracts.md section 4.3: `osmpq build` derives areas at
        # the end unless `--no-areas`. Folded into this build's single
        # manifest (v4) rather than writing a second one, so manifest
        # numbering stays "one build, one manifest".
        from osmpq.build import areas as areas_mod
        from osmpq.engine import catalog

        areas_tmpdir = tmpdir / "areas-tmp"
        areas_tmpdir.mkdir(parents=True, exist_ok=True)
        acon = areas_mod._connect(opts.threads, opts.memory_limit, areas_tmpdir)
        cat_manifest = catalog.Manifest(root=str(opts.root), data=man.to_dict())
        man.areas = areas_mod.build_areas_for_manifest(acon, Path(opts.root), cat_manifest, promoted_keys, threads=opts.threads)
        acon.close()
        man.manifest_version = 4
        man.stats["areas"] = man.areas["index"]["rows"]
        man.stats["way_areas"] = man.areas["way_index"]["rows"]
        _log(
            f"derived {man.stats['areas']} relation area(s) across {len(man.areas['cells'])} cell(s), "
            f"indexed {man.stats['way_areas']} way area(s)"
        )

    manifest_mod.write_manifest(opts.root, man, gen_number)
    _log(f"wrote manifest/{gen_number}.json and manifest/LATEST")
    _log(f"done in {time.time()-t_start:.1f}s total")
    return man


# --------------------------------------------------------------------------
# file placement (link/copy/move)
# --------------------------------------------------------------------------


def _place_file(src: Path, dst: Path, mode: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    if mode == "copy":
        shutil.copy2(src, dst)
        return
    if mode == "move":
        shutil.move(str(src), str(dst))
        return
    # link (default): hardlink, falling back to copy across filesystems.
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _parquet_stats(path: Path) -> tuple[int, int]:
    """(rows, bytes) for a Parquet file, from the footer only."""
    import pyarrow.parquet as pq

    md = pq.ParquetFile(str(path)).metadata
    return md.num_rows, path.stat().st_size


def _parquet_id_range(path: Path) -> tuple[Optional[int], Optional[int]]:
    """(min_id, max_id) for a Parquet file's ``id`` column, from row-group
    statistics in the footer only (no data read)."""
    import pyarrow.parquet as pq

    md = pq.ParquetFile(str(path)).metadata
    schema = md.schema
    id_idx = next((i for i in range(len(schema)) if schema.column(i).name == "id"), None)
    if id_idx is None:
        return None, None
    lo, hi = None, None
    for rg in range(md.num_row_groups):
        stats = md.row_group(rg).column(id_idx).statistics
        if stats is None or not stats.has_min_max:
            continue
        lo = stats.min if lo is None else min(lo, stats.min)
        hi = stats.max if hi is None else max(hi, stats.max)
    return (int(lo) if lo is not None else None), (int(hi) if hi is not None else None)


def _place_and_index_byid(src_dir: Path, dst_dir: Path, mode: str) -> tuple[list[dict], int]:
    parts = []
    total_bytes = 0
    if not src_dir.exists():
        return parts, total_bytes
    root = dst_dir.parents[2]  # <root>/byid/<gen>/<table> -> <root>
    for src in sorted(src_dir.glob("part-*.parquet")):
        dst = dst_dir / src.name
        _place_file(src, dst, mode)
        rows, size = _parquet_stats(dst)
        min_id, max_id = _parquet_id_range(dst)
        parts.append({
            "path": str(dst.relative_to(root)).replace(os.sep, "/"),
            "min_id": min_id,
            "max_id": max_id,
            "rows": rows,
            "bytes": size,
        })
        total_bytes += size
    return parts, total_bytes


def _place_node_spatial(rawdir: Path, root: Path, generation: str, mode: str) -> tuple[dict, int]:
    src_base = rawdir / "spatial" / "node"
    dst_base = root / "spatial" / generation / "node"
    out: dict = {"cells": {}}
    total_bytes = 0
    if not src_base.exists():
        return out, total_bytes
    for cell_dir in sorted(src_base.glob("cell=*")):
        cell = cell_dir.name.split("=", 1)[1]
        entry: dict = {}
        for tagged, suffix in ((True, "true"), (False, "false")):
            src = cell_dir / f"tagged={suffix}" / "part-0.parquet"
            if not src.exists():
                continue
            dst = dst_base / f"cell={cell}" / f"tagged={suffix}" / "part-0.parquet"
            _place_file(src, dst, mode)
            rows, size = _parquet_stats(dst)
            if rows == 0:
                dst.unlink()
                continue
            entry[("tagged" if tagged else "untagged")] = {
                "path": str(dst.relative_to(root)).replace(os.sep, "/"),
                "rows": rows,
                "bytes": size,
            }
            total_bytes += size
        if entry:
            out["cells"][cell] = entry
    return out, total_bytes


def _place_way_spatial(rawdir: Path, root: Path, generation: str, mode: str) -> tuple[dict, int, list[dict]]:
    src_base = rawdir / "spatial" / "way"
    dst_base = root / "spatial" / generation / "way"
    out: dict = {"cells": {}}
    total_bytes = 0
    rg_files: list[dict] = []
    if not src_base.exists():
        return out, total_bytes, rg_files
    import duckdb

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    for cell_dir in sorted(src_base.glob("cell=*")):
        cell = cell_dir.name.split("=", 1)[1]
        src = cell_dir / "part-0.parquet"
        if not src.exists():
            continue
        dst = dst_base / f"cell={cell}" / "part-0.parquet"
        _place_file(src, dst, mode)
        rows, size = _parquet_stats(dst)
        if rows == 0:
            dst.unlink()
            continue
        posix = str(dst).replace("'", "''")
        bbox = con.execute(
            f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
            f"FROM read_parquet('{posix}')"
        ).fetchone()
        rel_path = str(dst.relative_to(root)).replace(os.sep, "/")
        out["cells"][cell] = {
            "path": rel_path,
            "rows": rows,
            "bytes": size,
            "bbox": [float(x) if x is not None else None for x in bbox],
        }
        rg_files.append({"rel_path": rel_path, "cell": cell, "tagged": None})
        total_bytes += size
    con.close()
    return out, total_bytes, rg_files


def _node_rowgroup_files(node_table_manifest: dict, generation: str) -> list[dict]:
    out = []
    for cell, entry in node_table_manifest["cells"].items():
        for tagged_key, tagged_bool in (("tagged", True), ("untagged", False)):
            if tagged_key in entry:
                out.append({"rel_path": entry[tagged_key]["path"], "cell": cell, "tagged": tagged_bool})
    return out


def _relation_rowgroup_files(relation_table_manifest: dict, generation: str) -> list[dict]:
    return [
        {"rel_path": entry["path"], "cell": cell, "tagged": None}
        for cell, entry in relation_table_manifest["cells"].items()
    ]


def _place_node_way_index(rawdir: Path, root: Path, generation: str, mode: str) -> tuple[list[dict], int]:
    src_dir = rawdir / "node_way"
    dst_dir = root / "index" / generation / "node_way"
    parts = []
    total_bytes = 0
    if not src_dir.exists():
        return parts, total_bytes
    for src in sorted(src_dir.glob("part-*.parquet")):
        dst = dst_dir / src.name
        _place_file(src, dst, mode)
        rows, size = _parquet_stats(dst)
        import pyarrow.parquet as pq

        md = pq.ParquetFile(str(dst)).metadata
        schema = md.schema
        idx = next((i for i in range(len(schema)) if schema.column(i).name == "node_id"), None)
        min_id = max_id = None
        if idx is not None:
            for rg in range(md.num_row_groups):
                stats = md.row_group(rg).column(idx).statistics
                if stats is None or not stats.has_min_max:
                    continue
                min_id = stats.min if min_id is None else min(min_id, stats.min)
                max_id = stats.max if max_id is None else max(max_id, stats.max)
        parts.append({
            "path": str(dst.relative_to(root)).replace(os.sep, "/"),
            "min_id": int(min_id) if min_id is not None else None,
            "max_id": int(max_id) if max_id is not None else None,
            "rows": rows,
            "bytes": size,
        })
        total_bytes += size
    return parts, total_bytes


# --------------------------------------------------------------------------
# relations: bbox / cell / hilbert / centroid from raw parts
# --------------------------------------------------------------------------


def _build_relations(
    con,
    rawdir: Path,
    root: Path,
    generation: str,
    promoted_keys: list[str],
    leaf_index: cells_mod.LeafIndex,
    ancestor_depths: list[int],
    max_depth: int,
) -> tuple[dict, list[dict], list[dict], dict]:
    rel_dir = rawdir / "relation"
    rel_parts = sorted(str(p) for p in rel_dir.glob("part-*.parquet")) if rel_dir.exists() else []
    empty_table_manifest = {"cells": {}}
    empty_bytes = {"spatial": 0, "byid": 0, "member": 0}
    if not rel_parts:
        return empty_table_manifest, [], [], empty_bytes

    node_byid_paths = sorted(str(p) for p in (root / "byid" / generation / "node").glob("part-*.parquet"))
    way_byid_paths = sorted(str(p) for p in (root / "byid" / generation / "way").glob("part-*.parquet"))

    con.execute(f"CREATE VIEW relation0 AS SELECT * FROM read_parquet({rel_parts!r})")
    if node_byid_paths:
        con.execute(f"CREATE VIEW node_byid AS SELECT id, lat_e7, lon_e7 FROM read_parquet({node_byid_paths!r})")
    else:
        con.execute("CREATE VIEW node_byid AS SELECT NULL::BIGINT AS id, NULL::INTEGER AS lat_e7, NULL::INTEGER AS lon_e7 WHERE FALSE")
    if way_byid_paths:
        con.execute(
            f"CREATE VIEW way_byid AS SELECT id, xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM read_parquet({way_byid_paths!r})"
        )
    else:
        con.execute(
            "CREATE VIEW way_byid AS SELECT NULL::BIGINT AS id, NULL::INTEGER AS xmin_e7, "
            "NULL::INTEGER AS ymin_e7, NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7 WHERE FALSE"
        )

    # Flatten members once; the node/way bbox joins below are then plain
    # equi-joins keyed by member id, i.e. a semi-join against the byid
    # tables on just the referenced ids -- not a scan of every node.
    con.execute("""
        CREATE TABLE rel_members AS
        SELECT r.id AS rel_id, m.type AS mtype, m.ref AS mref
        FROM relation0 r, UNNEST(r.members) AS t(m)
    """)
    con.execute("""
        CREATE TABLE rel_member_bbox AS
        SELECT rm.rel_id, nn.lat_e7 AS ymin_e7, nn.lat_e7 AS ymax_e7,
               nn.lon_e7 AS xmin_e7, nn.lon_e7 AS xmax_e7
        FROM rel_members rm JOIN node_byid nn ON nn.id = rm.mref
        WHERE rm.mtype = 'n'
        UNION ALL
        SELECT rm.rel_id, ww.ymin_e7, ww.ymax_e7, ww.xmin_e7, ww.xmax_e7
        FROM rel_members rm JOIN way_byid ww ON ww.id = rm.mref
        WHERE rm.mtype = 'w' AND ww.xmin_e7 IS NOT NULL
    """)
    con.execute("""
        CREATE TABLE relation_bbox0 AS
        SELECT r.id,
               min(b.ymin_e7) AS ymin_e7, max(b.ymax_e7) AS ymax_e7,
               min(b.xmin_e7) AS xmin_e7, max(b.xmax_e7) AS xmax_e7
        FROM relation0 r LEFT JOIN rel_member_bbox b ON b.rel_id = r.id
        GROUP BY r.id
    """)
    # second pass: fold in bboxes of member relations resolved in pass 0
    con.execute("""
        CREATE TABLE relation_bbox1 AS
        SELECT r0.id,
               LEAST(r0.ymin_e7, min(rb.ymin_e7)) AS ymin_e7,
               GREATEST(r0.ymax_e7, max(rb.ymax_e7)) AS ymax_e7,
               LEAST(r0.xmin_e7, min(rb.xmin_e7)) AS xmin_e7,
               GREATEST(r0.xmax_e7, max(rb.xmax_e7)) AS xmax_e7
        FROM relation_bbox0 r0
        LEFT JOIN rel_members rm ON rm.rel_id = r0.id AND rm.mtype = 'r'
        LEFT JOIN relation_bbox0 rb ON rb.id = rm.mref
        GROUP BY r0.id, r0.ymin_e7, r0.ymax_e7, r0.xmin_e7, r0.xmax_e7
    """)
    con.execute("DROP TABLE rel_member_bbox")
    con.execute("DROP TABLE rel_members")
    con.execute("DROP TABLE relation_bbox0")

    promoted_sql = common.promoted_select(promoted_keys)
    con.execute(f"""
        CREATE TABLE relation1 AS
        SELECT r.id, r.tags, r.members, {promoted_sql},
               r.version, r.changeset, r.timestamp, r.uid, r."user",
               b.xmin_e7, b.ymin_e7, b.xmax_e7, b.ymax_e7,
               CASE WHEN b.xmin_e7 IS NULL THEN NULL
                    ELSE CAST(round((b.ymin_e7 + b.ymax_e7) / 2.0) AS INTEGER) END AS centroid_lat_e7,
               CASE WHEN b.xmin_e7 IS NULL THEN NULL
                    ELSE CAST(round((b.xmin_e7 + b.xmax_e7) / 2.0) AS INTEGER) END AS centroid_lon_e7
        FROM relation0 r JOIN relation_bbox1 b ON b.id = r.id
    """)
    con.execute("DROP TABLE relation_bbox1")

    # v2 cell + hilbert, vectorized.
    ids, ymin, xmin, ymax, xmax = con.execute(
        "SELECT id, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM relation1"
    ).fetchnumpy().values()

    def _filled_f64(col: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        if isinstance(col, np.ma.MaskedArray):
            return col.astype("float64").filled(np.nan), np.ma.getmaskarray(col)
        arr = np.asarray(col, dtype="float64")
        return arr, np.zeros(len(arr), dtype=bool)

    ymin_f, ymin_null = _filled_f64(ymin)
    xmin_f, xmin_null = _filled_f64(xmin)
    ymax_f, ymax_null = _filled_f64(ymax)
    xmax_f, xmax_null = _filled_f64(xmax)
    has_bbox = ~(ymin_null | xmin_null | ymax_null | xmax_null)

    rel_cell = np.full(len(ids), cells_mod.ROOT, dtype=object)
    rel_hilbert = np.zeros(len(ids), dtype=np.uint64)
    if has_bbox.any():
        sub_cell = cells_mod.containing_cells_v2_np(
            ymin_f[has_bbox].astype(np.int64),
            xmin_f[has_bbox].astype(np.int64),
            ymax_f[has_bbox].astype(np.int64),
            xmax_f[has_bbox].astype(np.int64),
            leaf_index,
            ancestor_depths,
            max_depth,
        )
        clat_e7 = np.round((ymin_f[has_bbox] + ymax_f[has_bbox]) / 2.0).astype(np.int64)
        clon_e7 = np.round((xmin_f[has_bbox] + xmax_f[has_bbox]) / 2.0).astype(np.int64)
        sub_hilbert = hilbert_mod.hilbert_keys(clat_e7, clon_e7)
        rel_cell[has_bbox] = sub_cell
        rel_hilbert[has_bbox] = sub_hilbert
    common.register_assignment(con, "rel_assign", np.asarray(ids, dtype=np.int64), rel_cell, rel_hilbert)
    con.execute("""
        CREATE TABLE relation2 AS
        SELECT r.*, a.cell, a.hilbert FROM relation1 r JOIN rel_assign a USING (id)
    """)
    con.execute("DROP TABLE relation1")

    # ---- write spatial/relation ------------------------------------------------
    select_cols_spatial = (
        f"id, members, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, NULL::GEOMETRY AS geometry, "
        f"centroid_lat_e7, centroid_lon_e7, cell, hilbert"
    )
    table_manifest: dict = {"cells": {}}
    spatial_bytes = 0
    cells_rows = con.execute("SELECT DISTINCT cell FROM relation2").fetchall()
    for (cell,) in cells_rows:
        select_sql = f"SELECT {select_cols_spatial} FROM relation2 WHERE cell = '{cell}' ORDER BY hilbert, id"
        rel_path = f"spatial/{generation}/relation/cell={cell}/part-0.parquet"
        path = root / rel_path
        rows, size = common.copy_to_parquet(con, select_sql, path, row_group_size=100_000)
        if rows == 0:
            path.unlink()
            continue
        bbox = con.execute(
            f"SELECT min(ymin_e7)/1e7, min(xmin_e7)/1e7, max(ymax_e7)/1e7, max(xmax_e7)/1e7 "
            f"FROM relation2 WHERE cell = '{cell}'"
        ).fetchone()
        table_manifest["cells"][cell] = {
            "path": rel_path,
            "rows": rows,
            "bytes": size,
            "bbox": [float(x) if x is not None else None for x in bbox],
        }
        spatial_bytes += size

    # ---- write byid/relation ----------------------------------------------------
    select_cols_byid = (
        f"id, members, tags, {promoted_sql}, version, changeset, timestamp, uid, \"user\", "
        f"xmin_e7, ymin_e7, xmax_e7, ymax_e7, cell"
    )
    total = con.execute("SELECT count(*) FROM relation2").fetchone()[0]
    byid_manifest: list[dict] = []
    byid_bytes = 0
    for k, (lo, hi) in enumerate(common.range_bounds(con, "relation2", "id", total, RELATION_PART_ROWS)):
        cond = common.range_cond("id", lo, hi)
        select_sql = f"SELECT {select_cols_byid} FROM relation2 WHERE {cond} ORDER BY id"
        rel_path = f"byid/{generation}/relation/part-{k:05d}.parquet"
        path = root / rel_path
        rows, size = common.copy_to_parquet(con, select_sql, path, row_group_size=100_000)
        min_id, max_id = con.execute(f"SELECT min(id), max(id) FROM relation2 WHERE {cond}").fetchone()
        byid_manifest.append({"path": rel_path, "min_id": min_id, "max_id": max_id, "rows": rows, "bytes": size})
        byid_bytes += size

    # ---- member index -------------------------------------------------------------
    con.execute("""
        CREATE TABLE _mem_index AS
        SELECT m.type AS member_type, m.ref AS member_id, r.id AS parent_id, m.role AS role, r.cell AS parent_cell
        FROM relation2 r, UNNEST(r.members) AS t(m)
    """)
    mem_total = con.execute("SELECT count(*) FROM _mem_index").fetchone()[0]
    member_manifest: list[dict] = []
    member_bytes = 0
    if mem_total > 0:
        for k, (lo, hi) in enumerate(common.range_bounds(con, "_mem_index", "member_id", mem_total)):
            cond = common.range_cond("member_id", lo, hi)
            select_sql = (
                f"SELECT member_type, member_id, parent_id, role, parent_cell FROM _mem_index "
                f"WHERE {cond} ORDER BY member_type, member_id, parent_id"
            )
            rel_path = f"index/{generation}/member/part-{k:05d}.parquet"
            path = root / rel_path
            rows, size = common.copy_to_parquet(con, select_sql, path, row_group_size=100_000)
            member_manifest.append({"path": rel_path, "rows": rows, "bytes": size})
            member_bytes += size
    con.execute("DROP TABLE _mem_index")
    con.execute("DROP TABLE relation2")
    con.execute("DROP VIEW relation0")
    con.execute("DROP VIEW node_byid")
    con.execute("DROP VIEW way_byid")

    return (
        table_manifest,
        byid_manifest,
        member_manifest,
        {"spatial": spatial_bytes, "byid": byid_bytes, "member": member_bytes},
    )
