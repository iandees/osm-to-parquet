"""Quadtree cells over plain lon/lat, per docs/m0-contracts.md section 2.

The root cell covers lon [-180, 180], lat [-90, 90]. Each split produces
four children in Bing quadkey order: ``0`` = NW, ``1`` = NE, ``2`` = SW,
``3`` = SE. A cell key is the string of digits from the root; the root
itself is the key ``"root"``. Depth = number of digits (root = 0). Max
depth 20.

A point belongs to the cell where ``lon < east`` and ``lat < north`` are
strict on the high side (half-open intervals), except at the world's
eastern/northern edges, which are included in the last cell.

Internally every point is mapped to a 40-bit "quadkey code": the depth-20
digit string read as a base-4 integer. Because each digit is exactly 2 bits
and digits are ordered most-significant-first, the code of any cell at
depth ``d`` occupies a contiguous range of the 40-bit space, which is what
makes vectorized point-to-leaf assignment (and the builder's leaf-cell
selection over aggregated counts) possible without a per-row Python loop.
"""
from __future__ import annotations

from typing import Iterable, Sequence, Union

import numpy as np

MAX_DEPTH = 20
ROOT = "root"

# Bounding box in degrees, always (south, west, north, east).
BBox = tuple[float, float, float, float]

_WORLD = (-90.0, -180.0, 90.0, 180.0)


def _depth(key: str) -> int:
    return 0 if key == ROOT else len(key)


def cell_bbox(key: str) -> BBox:
    """Return (south, west, north, east) in degrees for a cell key."""
    if key == ROOT:
        return _WORLD
    south, west, north, east = _WORLD
    for digit in key:
        mid_lon = (west + east) / 2.0
        mid_lat = (south + north) / 2.0
        if digit == "0":  # NW
            west, east, south, north = west, mid_lon, mid_lat, north
        elif digit == "1":  # NE
            west, east, south, north = mid_lon, east, mid_lat, north
        elif digit == "2":  # SW
            west, east, south, north = west, mid_lon, south, mid_lat
        elif digit == "3":  # SE
            west, east, south, north = mid_lon, east, south, mid_lat
        else:
            raise ValueError(f"invalid quadkey digit {digit!r} in {key!r}")
    return (south, west, north, east)


def children(key: str) -> list[str]:
    """The four child keys of ``key``, in digit order 0,1,2,3."""
    if _depth(key) >= MAX_DEPTH:
        raise ValueError(f"cell {key!r} is already at max depth {MAX_DEPTH}")
    prefix = "" if key == ROOT else key
    return [prefix + d for d in "0123"]


def parent(key: str) -> str | None:
    """The parent key of ``key``, or None if ``key`` is the root."""
    if key == ROOT:
        return None
    if len(key) == 1:
        return ROOT
    return key[:-1]


def ancestors(key: str) -> list[str]:
    """Ancestors of ``key`` from its parent up to and including root.

    Does not include ``key`` itself. ``ancestors("root") == []``.
    """
    out: list[str] = []
    cur = parent(key)
    while cur is not None:
        out.append(cur)
        cur = parent(cur)
    return out


# --------------------------------------------------------------------------
# Quadkey codes (depth-20 integer encoding), scalar and vectorized
# --------------------------------------------------------------------------


def _grid_xy(lat: float, lon: float) -> tuple[int, int]:
    n = 1 << MAX_DEPTH
    x = int((lon + 180.0) / 360.0 * n)
    y = int((lat + 90.0) / 180.0 * n)
    x = min(max(x, 0), n - 1)
    y = min(max(y, 0), n - 1)
    return x, y


def point_to_qk(lat: float, lon: float) -> int:
    """Depth-20 quadkey code (40-bit int) for a point, MSB-first digits."""
    x, y = _grid_xy(lat, lon)
    inv_y = ((1 << MAX_DEPTH) - 1) ^ y
    code = 0
    for i in range(MAX_DEPTH - 1, -1, -1):
        bx = (x >> i) & 1
        biy = (inv_y >> i) & 1
        digit = (biy << 1) | bx
        code = (code << 2) | digit
    return code


def point_to_qk_np(lat_e7: np.ndarray, lon_e7: np.ndarray) -> np.ndarray:
    """Vectorized :func:`point_to_qk` over INT32 e7 coordinate arrays.

    Returns a uint64 array of 40-bit quadkey codes.
    """
    lat_e7 = np.asarray(lat_e7, dtype=np.int64)
    lon_e7 = np.asarray(lon_e7, dtype=np.int64)
    n = 1 << MAX_DEPTH
    lat = lat_e7.astype(np.float64) / 1e7
    lon = lon_e7.astype(np.float64) / 1e7
    x = np.floor((lon + 180.0) / 360.0 * n).astype(np.int64)
    y = np.floor((lat + 90.0) / 180.0 * n).astype(np.int64)
    np.clip(x, 0, n - 1, out=x)
    np.clip(y, 0, n - 1, out=y)
    inv_y = ((1 << MAX_DEPTH) - 1) ^ y
    code = np.zeros_like(x, dtype=np.uint64)
    for i in range(MAX_DEPTH - 1, -1, -1):
        bx = (x >> i) & 1
        biy = (inv_y >> i) & 1
        digit = (biy << 1) | bx
        code = (code << np.uint64(2)) | digit.astype(np.uint64)
    return code


def key_to_value(key: str) -> int:
    """The base-4 digit value of a key (0 for root)."""
    value = 0
    if key != ROOT:
        for ch in key:
            value = value * 4 + int(ch)
    return value


def qk_range(key: str) -> tuple[int, int]:
    """Inclusive [lo, hi] range of depth-20 quadkey codes covered by ``key``."""
    depth = _depth(key)
    value = key_to_value(key)
    shift = 2 * (MAX_DEPTH - depth)
    lo = value << shift
    span = 1 << shift
    return lo, lo + span - 1


class LeafIndex:
    """Precomputed structure over a leaf-cell set for fast lookups.

    Because the depth-20 quadkey code of every cell occupies a contiguous
    range of the 40-bit space, and the leaves partition the whole world,
    sorting leaves by their range's lower bound lets us find "the leaf
    containing this point" with a single vectorized ``searchsorted`` instead
    of a per-row descent.
    """

    def __init__(self, leaves: Iterable[str]):
        self.leaves: list[str] = sorted(set(leaves))
        self._leafset: frozenset[str] = frozenset(self.leaves)
        ranges = [(qk_range(k)[0], qk_range(k)[1], k) for k in self.leaves]
        ranges.sort(key=lambda t: t[0])
        self._los = np.array([r[0] for r in ranges], dtype=np.uint64)
        self._his = np.array([r[1] for r in ranges], dtype=np.uint64)
        self._keys_by_lo = [r[2] for r in ranges]

    def __contains__(self, key: object) -> bool:
        return key in self._leafset

    def __iter__(self):
        return iter(self.leaves)

    def __len__(self) -> int:
        return len(self.leaves)

    def leaf_index_for_qk(self, qk: np.ndarray) -> np.ndarray:
        """Index into ``self._keys_by_lo`` (== ``self.leaves_by_lo``) per code."""
        qk = np.asarray(qk, dtype=np.uint64)
        idx = np.searchsorted(self._los, qk, side="right") - 1
        np.clip(idx, 0, len(self._los) - 1, out=idx)
        return idx

    @property
    def leaves_by_lo(self) -> list[str]:
        return self._keys_by_lo

    def leaf_for_qk(self, qk: np.ndarray) -> np.ndarray:
        """Vectorized leaf key lookup; returns an object array of str."""
        idx = self.leaf_index_for_qk(qk)
        keys = np.array(self._keys_by_lo, dtype=object)
        return keys[idx]


Leaves = Union[set, frozenset, LeafIndex]


def _leaf_lookup(leaves: Leaves, key: str) -> bool:
    return key in leaves


def point_cell(lat: float, lon: float, leaves: Leaves) -> str:
    """The leaf cell (from ``leaves``) containing point (lat, lon)."""
    south, west, north, east = _WORLD
    key = ROOT
    depth = 0
    while True:
        if _leaf_lookup(leaves, key):
            return key
        if depth >= MAX_DEPTH:
            return key
        mid_lon = (west + east) / 2.0
        mid_lat = (south + north) / 2.0
        north_half = lat >= mid_lat
        east_half = lon >= mid_lon
        if north_half and not east_half:
            digit, west, east, south, north = "0", west, mid_lon, mid_lat, north
        elif north_half and east_half:
            digit, west, east, south, north = "1", mid_lon, east, mid_lat, north
        elif not north_half and not east_half:
            digit, west, east, south, north = "2", west, mid_lon, south, mid_lat
        else:
            digit, west, east, south, north = "3", mid_lon, east, south, mid_lat
        key = digit if key == ROOT else key + digit
        depth += 1


def point_cells_np(lat_e7: np.ndarray, lon_e7: np.ndarray, leaves: Leaves) -> np.ndarray:
    """Vectorized leaf-cell assignment for arrays of INT32 e7 coordinates.

    ``leaves`` may be a plain set/frozenset of leaf keys (converted to a
    :class:`LeafIndex` internally) or an existing :class:`LeafIndex`.
    Returns an object array of str leaf keys, one per point.
    """
    index = leaves if isinstance(leaves, LeafIndex) else LeafIndex(leaves)
    qk = point_to_qk_np(lat_e7, lon_e7)
    return index.leaf_for_qk(qk)


def _bbox_contains(outer: BBox, inner: BBox) -> bool:
    o_south, o_west, o_north, o_east = outer
    i_south, i_west, i_north, i_east = inner
    return i_south >= o_south and i_west >= o_west and i_north <= o_north and i_east <= o_east


def containing_cell(bbox: BBox, leaves: Leaves) -> str:
    """Smallest cell (leaf or ancestor, incl. root) fully containing ``bbox``.

    Descends from the root while exactly one child fully contains the whole
    bbox and that child is not itself a leaf (a leaf is where a branch of
    the actual tree stops, even if the bbox would geometrically fit deeper).
    """
    key = ROOT
    cur_bbox = _WORLD
    depth = 0
    while True:
        if _leaf_lookup(leaves, key):
            return key
        if depth >= MAX_DEPTH:
            return key
        south, west, north, east = cur_bbox
        mid_lon = (west + east) / 2.0
        mid_lat = (south + north) / 2.0
        candidates = {
            "0": (mid_lat, west, north, mid_lon),
            "1": (mid_lat, mid_lon, north, east),
            "2": (south, west, mid_lat, mid_lon),
            "3": (south, mid_lon, mid_lat, east),
        }
        contained = [d for d, cb in candidates.items() if _bbox_contains(cb, bbox)]
        if len(contained) != 1:
            return key
        digit = contained[0]
        key = digit if key == ROOT else key + digit
        cur_bbox = candidates[digit]
        depth += 1


def _bbox_intersects(a: BBox, b: BBox) -> bool:
    a_south, a_west, a_north, a_east = a
    b_south, b_west, b_north, b_east = b
    if a_east < b_west or a_west > b_east:
        return False
    if a_north < b_south or a_south > b_north:
        return False
    return True


def cells_for_bbox(bbox: BBox, leaves: Leaves) -> list[str]:
    """Leaves intersecting ``bbox`` plus all their ancestors (incl. root).

    This is the planner rule: "every leaf cell that intersects the bbox,
    plus every ancestor of those leaves up to and including root".
    """
    leaf_keys: Sequence[str] = leaves.leaves if isinstance(leaves, LeafIndex) else list(leaves)
    out: set[str] = set()
    for leaf in leaf_keys:
        if _bbox_intersects(cell_bbox(leaf), bbox):
            out.add(leaf)
            out.update(ancestors(leaf))
    out.add(ROOT)
    return sorted(out)
