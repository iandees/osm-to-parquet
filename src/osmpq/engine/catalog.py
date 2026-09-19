"""Manifest loading and cell/quadkey math for the query engine.

Implements contract sections 1-3 (dataset root and paths, cells) without
depending on ``osmpq.layout`` so the engine can be developed independently
of the builder. See docs/m0-contracts.md.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

BBox = tuple[float, float, float, float]  # (south, west, north, east)


def is_remote(root: str) -> bool:
    return "://" in root and not root.startswith("file://")


def join_root(root: str, relpath: str) -> str:
    """Join a manifest-relative path (forward slashes, no leading slash) to root."""
    root = root.rstrip("/")
    relpath = relpath.lstrip("/")
    if is_remote(root):
        return f"{root}/{relpath}"
    # Local: resolve as filesystem path.
    return str(Path(root) / relpath)


def _read_text_local(path: str) -> str:
    return Path(path).read_text()


def _read_text_remote(path: str) -> str:
    import duckdb

    con = duckdb.connect()
    try:
        con.execute("LOAD httpfs;")
    except Exception:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            pass
    row = con.execute("SELECT content FROM read_text(?)", [path]).fetchone()
    con.close()
    if row is None:
        raise FileNotFoundError(path)
    return row[0]


def read_text_any(path: str) -> str:
    if is_remote(path):
        return _read_text_remote(path)
    return _read_text_local(path)


@dataclass
class Manifest:
    root: str
    data: dict

    def __post_init__(self) -> None:
        # Not dataclass fields (excluded from repr/eq on purpose): a small
        # per-Engine cache of the row-group index (section 5/6 of
        # docs/m1-contracts.md), lazily populated and reused across
        # `Engine.run()` calls since a Manifest instance lives for the
        # Engine's whole lifetime; and a transient per-run accumulator that
        # `Engine.run_program` sets up before planning and reads back after,
        # so `catalog.prune_files_by_bbox` can report files-considered vs
        # files-read without threading an extra return value through every
        # SQL-builder function in sources.py/recurse.py/planner.py.
        self._rowgroup_cache: dict[str, Optional["RowGroupIndex"]] = {}
        self._file_stats: Optional["FileStats"] = None

    @property
    def promoted_keys(self) -> list[str]:
        return list(self.data.get("promoted_keys", []))

    @property
    def generation(self) -> str:
        return self.data["generation"]

    @property
    def timestamp_osm_base(self) -> Optional[str]:
        return self.data.get("timestamp_osm_base")

    @property
    def leaf_cells(self) -> list[str]:
        return list(self.data.get("leaf_cells", []))

    @property
    def manifest_version(self) -> int:
        return int(self.data.get("manifest_version", 1))

    @property
    def ancestor_depths(self) -> list[int]:
        """Manifest v2's `ancestor_depths` (docs/m1-contracts.md section 5),
        e.g. [0, 3, 6, 9, 12]. Empty for v1 (unused there: v1's
        `cells_for_bbox` keeps using every ancestor, section 2 of
        docs/m0-contracts.md)."""
        return list(self.data.get("ancestor_depths", []))

    @property
    def max_depth(self) -> int:
        return int(self.data.get("max_depth", 20))

    @property
    def rowgroup_index_paths(self) -> dict[str, str]:
        """{'node'|'way'|'relation': manifest-relative path}, from manifest
        v2's `rowgroup_index` (docs/m1-contracts.md section 5). Empty dict
        when absent (v1, or a v2 manifest built without it)."""
        return dict(self.data.get("rowgroup_index", {}))

    def table_cells(self, table: str) -> dict:
        return self.data.get("tables", {}).get(table, {}).get("cells", {})

    def byid_parts(self, table: str) -> list[dict]:
        return list(self.data.get("byid", {}).get(table, []))

    def index_parts(self, name: str) -> list[dict]:
        return list(self.data.get("index", {}).get(name, []))

    def path(self, relpath: str) -> str:
        return join_root(self.root, relpath)


def load_manifest(root: str) -> Manifest:
    latest_path = join_root(root, "manifest/LATEST")
    latest = read_text_any(latest_path).strip()
    manifest_path = join_root(root, f"manifest/{latest}.json")
    data = json.loads(read_text_any(manifest_path))
    return Manifest(root=root, data=data)


# --------------------------------------------------------------------------
# Quadkey math (contract section 2)
# --------------------------------------------------------------------------

WORLD_BBOX: BBox = (-90.0, -180.0, 90.0, 180.0)


def cell_bbox(key: str) -> BBox:
    """Return (south, west, north, east) for a cell key ('root' or digit string)."""
    s, w, n, e = WORLD_BBOX
    if key == "root":
        return (s, w, n, e)
    for d in key:
        midlat = (s + n) / 2.0
        midlon = (w + e) / 2.0
        if d == "0":  # NW
            s, e = midlat, midlon
        elif d == "1":  # NE
            s, w = midlat, midlon
        elif d == "2":  # SW
            n, e = midlat, midlon
        elif d == "3":  # SE
            n, w = midlat, midlon
        else:
            raise ValueError(f"bad quadkey digit {d!r} in {key!r}")
    return (s, w, n, e)


def bbox_intersects(a: BBox, b: BBox) -> bool:
    a_s, a_w, a_n, a_e = a
    b_s, b_w, b_n, b_e = b
    return a_s <= b_n and a_n >= b_s and a_w <= b_e and a_e >= b_w


def ancestors_and_self(leaf_key: str) -> list[str]:
    """['root', <prefix1>, ..., leaf_key] for a leaf key; ['root'] for 'root' itself."""
    if leaf_key == "root":
        return ["root"]
    out = ["root"]
    for i in range(1, len(leaf_key) + 1):
        out.append(leaf_key[:i])
    return out


def leaves_intersecting(manifest: Manifest, bbox: BBox) -> set[str]:
    hits = set()
    for leaf in manifest.leaf_cells:
        if bbox_intersects(bbox, cell_bbox(leaf)):
            hits.add(leaf)
    return hits


def _ancestor_at_depth(leaf_key: str, depth: int) -> str:
    """The ancestor of `leaf_key` at `depth` digits (0 = 'root')."""
    if depth <= 0:
        return "root"
    return leaf_key[:depth]


def cells_for_bbox(manifest: Manifest, table: str, bbox: Optional[BBox]) -> list[str]:
    """Contract section 2 (v1) / m1-contracts.md section 2 and 6 (v2):
    leaves intersecting bbox, plus ancestors, filtered to cells present for
    `table`. bbox=None means "everything".

    - Manifest v1 (or v2 for the `node` table, which is leaf-only by
      construction regardless of version -- section 2: nodes always live in
      a leaf, so `present` never contains a non-leaf key for it and the
      ancestors added below are filtered out for free): every ancestor of
      every intersecting leaf, up to and including root.
    - Manifest v2, `way`/`relation`: every intersecting leaf itself (an
      element can be stored exactly at a leaf), plus -- for each such leaf
      -- only the ancestors whose depth is in `ancestor_depths` (root, depth
      0, is always included even if the manifest's list omits it)."""
    present = manifest.table_cells(table)
    if bbox is None:
        return sorted(present.keys())
    leaves = leaves_intersecting(manifest, bbox)
    wanted: set[str] = set()
    if manifest.manifest_version >= 2 and table in ("way", "relation"):
        depths = set(manifest.ancestor_depths)
        depths.add(0)
        for leaf in leaves:
            wanted.add(leaf)
            for d in depths:
                if d <= len(leaf):
                    wanted.add(_ancestor_at_depth(leaf, d))
    else:
        for leaf in leaves:
            wanted.update(ancestors_and_self(leaf))
    return sorted(c for c in wanted if c in present)


def parts_for_range(parts: list[dict], lo: Optional[int], hi: Optional[int]) -> list[dict]:
    """Filter manifest parts (each with min_id/max_id) to those whose id
    range can overlap [lo, hi]. Parts lacking min_id/max_id (e.g. the member
    index, which is not sorted by a single id) are always kept."""
    if lo is None or hi is None:
        return []
    if not all("min_id" in p and "max_id" in p for p in parts):
        return parts
    return [p for p in parts if p["max_id"] >= lo and p["min_id"] <= hi]


def byid_parts_for_range(manifest: Manifest, table: str, lo: Optional[int], hi: Optional[int]) -> list[dict]:
    return parts_for_range(manifest.byid_parts(table), lo, hi)


def byid_parts_for_ids(manifest: Manifest, table: str, ids: Iterable[int]) -> list[dict]:
    ids = list(ids)
    if not ids:
        return []
    return byid_parts_for_range(manifest, table, min(ids), max(ids))


def all_byid_parts(manifest: Manifest, table: str) -> list[dict]:
    return manifest.byid_parts(table)


def index_parts_for_range(manifest: Manifest, name: str, lo: Optional[int], hi: Optional[int]) -> list[dict]:
    return parts_for_range(manifest.index_parts(name), lo, hi)


def index_parts_for_ids(manifest: Manifest, name: str, ids: Iterable[int]) -> list[dict]:
    """Like byid_parts_for_ids but for index/<name> parts. Falls back to all
    parts when a part lacks min_id/max_id (e.g. the member index, which is
    not sorted by a single id)."""
    ids = list(ids)
    parts = manifest.index_parts(name)
    if not ids or not parts:
        return parts if ids else []
    return index_parts_for_range(manifest, name, min(ids), max(ids))


# --------------------------------------------------------------------------
# Row-group index and pruning (m1-contracts.md sections 4/6): manifest v2's
# index/<gen>/rowgroups/{node,way,relation}.parquet, one row per Parquet row
# group of the spatial files: path, cell, tagged (nodes only), rg, rows,
# xmin_e7, ymin_e7, xmax_e7, ymax_e7 (for nodes: min/max of lon_e7/lat_e7).
# --------------------------------------------------------------------------

RowGroupBBox = tuple[int, int, int, int]  # (xmin_e7, ymin_e7, xmax_e7, ymax_e7)


@dataclass
class RowGroupIndex:
    """One table's row-group index, loaded once per Engine and cached on the
    Manifest (`Manifest._rowgroup_cache`): every spatial file's absolute
    path (already joined with `manifest.path`, so it matches exactly what
    `sources.py`'s `_node_files`/`_way_files`/`_relation_files` produce) ->
    the bboxes of its row groups."""

    by_path: dict[str, list[RowGroupBBox]]


@dataclass
class FileStats:
    """Transient per-`Engine.run()` accumulator for the row-group-prunable
    file selections (see `Manifest.__post_init__` and `prune_files_by_bbox`
    below): `considered` is the candidate-file count before pruning,
    `read` the count that survived it. `Engine.run_program` derives
    `Result.stats["files_considered"]` from this plus the (already-correct,
    because pruning mutates the file lists in place before anything counts
    them) existing `files_read` total."""

    considered: int = 0
    read: int = 0


def _load_rowgroup_index(manifest: Manifest, table: str) -> Optional[RowGroupIndex]:
    relpath = manifest.rowgroup_index_paths.get(table)
    if not relpath:
        return None
    path = manifest.path(relpath)
    import duckdb

    con = duckdb.connect()
    try:
        try:
            con.execute("LOAD httpfs;")
        except Exception:
            try:
                con.execute("INSTALL httpfs; LOAD httpfs;")
            except Exception:
                pass
        try:
            rows = con.execute(
                "SELECT path, xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM read_parquet(?)",
                [path],
            ).fetchall()
        except Exception:
            # Manifest declares a rowgroup_index path but the file is
            # missing/unreadable: treat as "no index" rather than failing
            # the whole query, same spirit as v1 simply lacking the key.
            return None
    finally:
        con.close()

    by_path: dict[str, list[RowGroupBBox]] = {}
    for relp, xmin, ymin, xmax, ymax in rows:
        if None in (relp, xmin, ymin, xmax, ymax):
            continue
        abspath = manifest.path(relp)
        by_path.setdefault(abspath, []).append((xmin, ymin, xmax, ymax))
    return RowGroupIndex(by_path=by_path)


def _rowgroup_index_for(manifest: Manifest, table: str) -> Optional[RowGroupIndex]:
    cache = getattr(manifest, "_rowgroup_cache", None)
    if cache is None:
        cache = {}
        manifest._rowgroup_cache = cache
    if table not in cache:
        cache[table] = _load_rowgroup_index(manifest, table)
    return cache[table]


def prune_files_by_bbox(manifest: Manifest, table: str, files: list[str], bbox_e7: RowGroupBBox) -> list[str]:
    """Drop any file in `files` that has no row group whose
    [xmin_e7..xmax_e7] x [ymin_e7..ymax_e7] intersects `bbox_e7`
    (docs/m1-contracts.md section 6). A file the index has no entries for
    (e.g. a manifest/index mismatch) is kept rather than guessed away.
    When the manifest has no rowgroup index for `table` (v1, or a v2
    manifest built without one), returns `files` unchanged -- pruning is
    skipped entirely, per contract.

    Updates `manifest._file_stats` (set up for the duration of one
    `Engine.run()` by `executor.Engine.run_program`) with the candidate
    count (`considered`) and the surviving count (`read`), when pruning
    actually runs; callers don't need to touch it themselves."""
    if not files:
        return files
    idx = _rowgroup_index_for(manifest, table)
    if idx is None:
        return files

    xmin, ymin, xmax, ymax = bbox_e7
    kept = []
    for f in files:
        boxes = idx.by_path.get(f)
        if not boxes:
            kept.append(f)
            continue
        if any(bx0 <= xmax and bx1 <= ymax and bx2 >= xmin and bx3 >= ymin for bx0, bx1, bx2, bx3 in boxes):
            kept.append(f)

    stats = getattr(manifest, "_file_stats", None)
    if stats is not None:
        stats.considered += len(files)
        stats.read += len(kept)
    return kept
