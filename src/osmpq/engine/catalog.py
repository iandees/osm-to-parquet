"""Manifest loading and cell/quadkey math for the query engine.

Implements contract sections 1-3 (dataset root and paths, cells) without
depending on ``osmpq.layout`` so the engine can be developed independently
of the builder. See docs/m0-contracts.md.
"""
from __future__ import annotations

import contextvars
import json
import threading
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


def _read_text_remote(path: str, con=None) -> str:
    """Read a remote (http(s)://, s3://, ...) text file via DuckDB's
    ``read_text``. When `con` is given (the Engine's shared database, for
    an s3:// root -- see `executor.Engine`), reuse it through a cursor so
    any S3 secret created on it applies; a cursor is a separate connection
    sharing the database, so this is safe to call concurrently with
    queries running on other cursors. Falls back to a throwaway connection
    when no `con` is given (e.g. `load_manifest` called directly, without
    an Engine, as tests do)."""
    import duckdb

    owns_con = con is None
    cur = duckdb.connect() if owns_con else con.cursor()
    try:
        if owns_con:
            try:
                cur.execute("LOAD httpfs;")
            except Exception:
                try:
                    cur.execute("INSTALL httpfs; LOAD httpfs;")
                except Exception:
                    pass
        row = cur.execute("SELECT content FROM read_text(?)", [path]).fetchone()
    finally:
        cur.close()
    if row is None:
        raise FileNotFoundError(path)
    return row[0]


def read_text_any(path: str, con=None) -> str:
    if is_remote(path):
        return _read_text_remote(path, con=con)
    return _read_text_local(path)


@dataclass
class Manifest:
    root: str
    data: dict

    def __post_init__(self) -> None:
        # Not dataclass fields (excluded from repr/eq on purpose):
        # - `_rowgroup_cache`: a small per-Engine cache of the row-group
        #   index (section 5/6 of docs/m1-contracts.md), lazily populated
        #   and reused across `Engine.run()` calls since a Manifest
        #   instance lives for the Engine's whole lifetime. `_rowgroup_lock`
        #   guards first-population of each table's entry so two
        #   concurrent `Engine.run()` calls racing on the same
        #   not-yet-cached table don't both kick off a load (each still
        #   ends up correct even without the lock -- the load is
        #   idempotent -- but the lock avoids the wasted duplicate work).
        # - `_db`: the Engine's shared DuckDB database (see
        #   `executor.Engine`), used so a row-group-index load or a
        #   manifest-bootstrap read for an `s3://` root goes through the
        #   connection that carries the S3 secret, instead of a throwaway
        #   `duckdb.connect()` with no credentials. None when the Manifest
        #   was built without an Engine (e.g. `load_manifest(root)` in
        #   tests) -- callers fall back to a throwaway connection then.
        #
        # Per-run file-considered/file-read accounting used to live here
        # too (`_file_stats`), but that made two concurrent `Engine.run()`
        # calls stomp on each other's counts (FastAPI runs sync endpoints
        # in a thread pool). It now lives in the `FILE_STATS` ContextVar
        # below, which `Engine.run_program` sets for the duration of one
        # run: each thread/run sees only its own value.
        self._rowgroup_cache: dict[str, Optional["RowGroupIndex"]] = {}
        self._rowgroup_lock = threading.Lock()
        self._db = None

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

    @property
    def replication_source(self) -> Optional[str]:
        """Manifest v3's `replication_source` (docs/m2-contracts.md section
        1): the Osmosis-style replication directory URL the updater reads
        diffs from. None for v1/v2 or a v3 manifest without it."""
        return self.data.get("replication_source")

    def delta_tiers(self) -> list[dict]:
        """Manifest v3's `deltas` (docs/m2-contracts.md sections 3-4),
        as a list in precedence order (hour > day > week -- highest rank
        first), each element:

            {"name": "hour", "rank": 3, "version": 17,
             "seq_from": ..., "seq_to": ..., "timestamp": ..., "rows": {...},
             "files": {"node": {"spatial": <abspath>, "byid": <abspath>},
                       "way": {...}, "relation": {...}},
             "tombstones": <abspath>,
             "cells": {"node": [...], "way": [...], "relation": [...]}}

        `cells` (added for the "new cell, no base file yet" follow-up) is
        the updater/compactor-declared `deltas.<tier>.cells`: sorted lists
        of cell keys that have at least one row in that tier's `<type>
        .spatial.parquet`. Missing/absent per type or altogether (an
        older or hand-built manifest) is just `[]` -- see
        `delta_present_cells`.

        Every path is already resolved through `self.path` (joined with
        `self.root`), so callers never touch `join_root` themselves. A tier
        absent from the manifest (contract: "a tier that is empty is absent
        from deltas") is simply missing from this list. Empty for manifest
        v1/v2, or a v3 manifest with no `deltas` key/an empty one -- so
        every read-path helper that starts with ``if not
        manifest.delta_tiers(): <old code, unchanged>`` costs nothing and
        emits identical SQL for those manifests (docs/m2-contracts.md
        section 4's "no extra scans" requirement)."""
        if self.manifest_version < 3:
            return []
        deltas = self.data.get("deltas") or {}
        out: list[dict] = []
        for name, rank in (("hour", 3), ("day", 2), ("week", 1)):
            tier = deltas.get(name)
            if not tier:
                continue
            files = tier.get("files", {}) or {}
            resolved_files: dict[str, dict[str, str]] = {}
            for t in ("node", "way", "relation"):
                tf = files.get(t) or {}
                resolved_files[t] = {k: self.path(v) for k, v in tf.items() if v}
            tomb = files.get("tombstones")
            cells = tier.get("cells") or {}
            out.append(
                {
                    "name": name,
                    "rank": rank,
                    "version": tier.get("version"),
                    "seq_from": tier.get("seq_from"),
                    "seq_to": tier.get("seq_to"),
                    "timestamp": tier.get("timestamp"),
                    "rows": dict(tier.get("rows", {})),
                    "files": resolved_files,
                    "tombstones": self.path(tomb) if tomb else None,
                    "cells": {t: list(cells.get(t, [])) for t in ("node", "way", "relation")},
                }
            )
        return out

    def has_deltas(self) -> bool:
        return bool(self.delta_tiers())

    def table_cells(self, table: str) -> dict:
        return self.data.get("tables", {}).get(table, {}).get("cells", {})

    def byid_parts(self, table: str) -> list[dict]:
        return list(self.data.get("byid", {}).get(table, []))

    def index_parts(self, name: str) -> list[dict]:
        return list(self.data.get("index", {}).get(name, []))

    def path(self, relpath: str) -> str:
        return join_root(self.root, relpath)


def load_manifest(root: str, con=None) -> Manifest:
    """Load `root`'s manifest. `con` is the Engine's shared DuckDB database
    (see `executor.Engine`); when given, remote (`s3://`, `http(s)://`)
    reads go through it (via a cursor) instead of a throwaway connection,
    so a secret created on it (S3 credentials) applies. The returned
    Manifest also keeps `con` for later lazy row-group-index loads
    (`_rowgroup_index_for`), for the same reason."""
    latest_path = join_root(root, "manifest/LATEST")
    latest = read_text_any(latest_path, con=con).strip()
    manifest_path = join_root(root, f"manifest/{latest}.json")
    data = json.loads(read_text_any(manifest_path, con=con))
    manifest = Manifest(root=root, data=data)
    manifest._db = con
    return manifest


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


def delta_present_cells(manifest: Manifest, table: str) -> set[str]:
    """Union, over every present delta tier, of the cell keys that tier's
    manifest entry declares for `table` (`deltas.<tier>.cells.<table>`,
    docs/m2-contracts.md section 3/4 follow-up): sorted lists of cells that
    have at least one row in that tier's `<table>.spatial.parquet`, written
    by the updater/compactor alongside the tier. Empty when there are no
    delta tiers, or none declare `cells` (an older/hand-built manifest --
    treated the same as "no extra cells", not an error).

    This is what lets a query discover an element the updater placed in an
    allowed cell (a leaf, or an ancestor at one of `ancestor_depths`) that
    the *base* has no file for yet -- new data waiting for the next
    compaction. Cheap: pure Python set-union over already-loaded manifest
    JSON, no I/O."""
    out: set[str] = set()
    for tier in manifest.delta_tiers():
        out.update(tier.get("cells", {}).get(table, []))
    return out


def cells_for_bbox(manifest: Manifest, table: str, bbox: Optional[BBox]) -> list[str]:
    """Contract section 2 (v1) / m1-contracts.md section 2 and 6 (v2) /
    m2-contracts.md section 4: leaves intersecting bbox, plus ancestors,
    filtered to cells present for `table` -- where "present" is the base's
    own cells *plus* every cell any delta tier declares rows for
    (`delta_present_cells`), so a query's candidate cell list already
    includes cells the base has no file for yet. bbox=None means
    "everything".

    - Manifest v1 (or v2 for the `node` table, which is leaf-only by
      construction regardless of version -- section 2: nodes always live in
      a leaf, so `present` never contains a non-leaf key for it and the
      ancestors added below are filtered out for free): every ancestor of
      every intersecting leaf, up to and including root.
    - Manifest v2, `way`/`relation`: every intersecting leaf itself (an
      element can be stored exactly at a leaf), plus -- for each such leaf
      -- only the ancestors whose depth is in `ancestor_depths` (root, depth
      0, is always included even if the manifest's list omits it).
    - A cell that is present *only* via a delta tier's declaration may have
      no leaf of its own beneath it in `manifest.leaf_cells` at all (e.g. a
      brand-new way placed straight at an allowed ancestor cell no base
      leaf has been split under), so it can never be reached by the
      leaf-then-ancestor walk above; such cells are checked directly
      against their own quadkey bbox instead. Skipped whenever nothing is
      delta-only (every v1/v2 manifest, and any v3 manifest whose deltas
      don't introduce a new cell), so this costs nothing in the common
      case and never changes a manifest-without-deltas' output."""
    base_present = manifest.table_cells(table)
    present = set(base_present.keys())
    present |= delta_present_cells(manifest, table)
    if bbox is None:
        return sorted(present)
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
    delta_only = present - set(base_present.keys())
    for c in delta_only:
        if c not in wanted and bbox_intersects(bbox, cell_bbox(c)):
            wanted.add(c)
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
    """Per-`Engine.run()` accumulator for the row-group-prunable file
    selections (see `prune_files_by_bbox` below): `considered` is the
    candidate-file count before pruning, `read` the count that survived
    it. `Engine.run_program` derives `Result.stats["files_considered"]`
    from this plus the (already-correct, because pruning mutates the file
    lists in place before anything counts them) existing `files_read`
    total.

    Kept out of two concurrent `Engine.run()` calls' way via `FILE_STATS`
    below (a ContextVar) rather than being stashed on the shared
    `Manifest`, since a `Manifest` instance -- and so any attribute on
    it -- is reused across every `run()` of one `Engine`, including
    concurrent ones."""

    considered: int = 0
    read: int = 0


#: The current run's `FileStats`, or None outside of a run. `ContextVar`s
#: are per-thread by default (each OS thread starts with its own context
#: unless it explicitly copies another's), so this is safe under
#: `concurrent.futures.ThreadPoolExecutor`-style concurrent calls to one
#: `Engine.run()`: each call's `.set()` in `executor.Engine.run_program`
#: is invisible to any other thread's concurrent run, and `.reset()` in
#: its `finally` restores whatever was there before (None, normally).
FILE_STATS: "contextvars.ContextVar[Optional[FileStats]]" = contextvars.ContextVar(
    "osmpq_file_stats", default=None
)


@dataclass
class DeltaStats:
    """Per-`Engine.run()` accumulator for docs/m2-contracts.md section 4's
    `Result.stats["delta_rows"]`/`["shadowed"]`: `delta_rows` is the number
    of ranked delta candidate rows considered across every cell-scoped or
    by-id read this run touched; `shadowed` is the number of base rows
    those candidates (plus tombstones) caused to be excluded. Kept in a
    ContextVar for the same reason `FILE_STATS` is (see its docstring):
    `Manifest`/`Engine` are reused across concurrent `run()` calls, so
    per-run counters can't live there."""

    delta_rows: int = 0
    shadowed: int = 0


DELTA_STATS: "contextvars.ContextVar[Optional[DeltaStats]]" = contextvars.ContextVar(
    "osmpq_delta_stats", default=None
)


def _load_rowgroup_index(manifest: Manifest, table: str) -> Optional[RowGroupIndex]:
    relpath = manifest.rowgroup_index_paths.get(table)
    if not relpath:
        return None
    path = manifest.path(relpath)
    import duckdb

    shared_db = getattr(manifest, "_db", None)
    # A fresh cursor either way: when `shared_db` is the Engine's shared
    # database, a cursor is a separate connection sharing it, so this is
    # safe to run concurrently with queries on other cursors; when there
    # is no shared database (Manifest built without an Engine), it's a
    # plain standalone connection like before.
    con = shared_db.cursor() if shared_db is not None else duckdb.connect()
    try:
        if shared_db is None:
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
    if table in cache:
        return cache[table]
    lock = getattr(manifest, "_rowgroup_lock", None)
    if lock is None:
        lock = threading.Lock()
        manifest._rowgroup_lock = lock
    with lock:
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

    Updates `FILE_STATS` (the current run's accumulator, set up for the
    duration of one `Engine.run()` by `executor.Engine.run_program`) with
    the candidate count (`considered`) and the surviving count (`read`),
    when pruning actually runs; callers don't need to touch it
    themselves."""
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

    stats = FILE_STATS.get()
    if stats is not None:
        stats.considered += len(files)
        stats.read += len(kept)
    return kept
