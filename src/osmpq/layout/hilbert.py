"""Hilbert curve indexing, per docs/m0-contracts.md section 2.

Maps (lon, lat) to a 2^20 x 2^20 grid and takes the Hilbert curve index of
(x, y) at order 20. This is the classic Wikipedia ``xy2d`` algorithm
(https://en.wikipedia.org/wiki/Hilbert_curve#Applications_and_mapping_algorithms),
plus a numpy-vectorized version for bulk assignment.
"""
from __future__ import annotations

import numpy as np

ORDER = 20


def hilbert_xy2d(order: int, x: int, y: int) -> int:
    """Classic Wikipedia xy2d: Hilbert curve index of (x, y) at ``order``.

    ``x`` and ``y`` must be in [0, 2**order).
    """
    n = 1 << order
    rx = ry = 0
    d = 0
    s = n // 2
    while s > 0:
        rx = 1 if (x & s) > 0 else 0
        ry = 1 if (y & s) > 0 else 0
        d += s * s * ((3 * rx) ^ ry)
        # rotate
        if ry == 0:
            if rx == 1:
                x = n - 1 - x
                y = n - 1 - y
            x, y = y, x
        s //= 2
    return d


def _grid_xy(lat_e7: int, lon_e7: int) -> tuple[int, int]:
    n = 1 << ORDER
    lat = lat_e7 / 1e7
    lon = lon_e7 / 1e7
    x = int((lon + 180.0) / 360.0 * n)
    y = int((lat + 90.0) / 180.0 * n)
    x = min(max(x, 0), n - 1)
    y = min(max(y, 0), n - 1)
    return x, y


def hilbert_key(lat_e7: int, lon_e7: int) -> int:
    """Scalar Hilbert key for a point given as INT32 e7 coordinates."""
    x, y = _grid_xy(lat_e7, lon_e7)
    return hilbert_xy2d(ORDER, x, y)


def hilbert_keys(lat_e7: np.ndarray, lon_e7: np.ndarray) -> np.ndarray:
    """Vectorized Hilbert key computation for arrays of INT32 e7 coordinates.

    Returns a uint64 array, one Hilbert index per point, at ``ORDER`` = 20
    (grid 2**20 x 2**20).
    """
    lat_e7 = np.asarray(lat_e7, dtype=np.int64)
    lon_e7 = np.asarray(lon_e7, dtype=np.int64)
    n = 1 << ORDER
    lat = lat_e7.astype(np.float64) / 1e7
    lon = lon_e7.astype(np.float64) / 1e7
    x = np.floor((lon + 180.0) / 360.0 * n).astype(np.int64)
    y = np.floor((lat + 90.0) / 180.0 * n).astype(np.int64)
    np.clip(x, 0, n - 1, out=x)
    np.clip(y, 0, n - 1, out=y)

    d = np.zeros_like(x, dtype=np.uint64)
    s = n // 2
    while s > 0:
        rx = ((x & s) > 0).astype(np.int64)
        ry = ((y & s) > 0).astype(np.int64)
        d += np.uint64(s * s) * ((3 * rx) ^ ry).astype(np.uint64)

        swap_mask = ry == 0
        flip_mask = swap_mask & (rx == 1)
        x = np.where(flip_mask, n - 1 - x, x)
        y = np.where(flip_mask, n - 1 - y, y)
        new_x = np.where(swap_mask, y, x)
        new_y = np.where(swap_mask, x, y)
        x, y = new_x, new_y

        s //= 2
    return d
