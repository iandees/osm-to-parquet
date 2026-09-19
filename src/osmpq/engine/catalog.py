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


def cells_for_bbox(manifest: Manifest, table: str, bbox: Optional[BBox]) -> list[str]:
    """Contract section 2: leaves intersecting bbox + ancestors incl. root,
    filtered to cells present for `table`. bbox=None means "everything"."""
    present = manifest.table_cells(table)
    if bbox is None:
        return sorted(present.keys())
    wanted: set[str] = set()
    for leaf in leaves_intersecting(manifest, bbox):
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
