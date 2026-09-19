"""Unit tests for osmpq.layout.hilbert, per docs/m0-contracts.md section 2."""
from __future__ import annotations

import numpy as np

from osmpq.layout import hilbert

# The classic order-2 (4x4 grid) Hilbert curve, from Wikipedia's diagram.
ORDER2_TABLE = {
    (0, 0): 0, (1, 0): 1, (1, 1): 2, (0, 1): 3,
    (0, 2): 4, (0, 3): 5, (1, 3): 6, (1, 2): 7,
    (2, 2): 8, (2, 3): 9, (3, 3): 10, (3, 2): 11,
    (3, 1): 12, (2, 1): 13, (2, 0): 14, (3, 0): 15,
}


def test_xy2d_order2_known_values():
    for (x, y), d in ORDER2_TABLE.items():
        assert hilbert.hilbert_xy2d(2, x, y) == d


def test_xy2d_order1_known_values():
    # order 1 (2x2 grid): standard U-shape
    assert hilbert.hilbert_xy2d(1, 0, 0) == 0
    assert hilbert.hilbert_xy2d(1, 0, 1) == 1
    assert hilbert.hilbert_xy2d(1, 1, 1) == 2
    assert hilbert.hilbert_xy2d(1, 1, 0) == 3


def test_xy2d_is_a_bijection_order3():
    n = 1 << 3
    seen = set()
    for x in range(n):
        for y in range(n):
            d = hilbert.hilbert_xy2d(3, x, y)
            assert 0 <= d < n * n
            assert d not in seen
            seen.add(d)
    assert len(seen) == n * n


def test_xy2d_adjacent_d_are_grid_neighbors_order4():
    """Locality: consecutive Hilbert indices are always adjacent cells."""
    n = 1 << 4
    by_d = {}
    for x in range(n):
        for y in range(n):
            by_d[hilbert.hilbert_xy2d(4, x, y)] = (x, y)
    for d in range(n * n - 1):
        x0, y0 = by_d[d]
        x1, y1 = by_d[d + 1]
        assert abs(x0 - x1) + abs(y0 - y1) == 1


def test_hilbert_keys_vectorized_matches_scalar():
    rng = np.random.default_rng(7)
    lat = rng.uniform(-90, 90, size=2000)
    lon = rng.uniform(-180, 180, size=2000)
    lat_e7 = np.round(lat * 1e7).astype(np.int64)
    lon_e7 = np.round(lon * 1e7).astype(np.int64)
    vec = hilbert.hilbert_keys(lat_e7, lon_e7)
    assert vec.dtype == np.uint64
    for i in range(0, 2000, 37):
        scalar = hilbert.hilbert_key(int(lat_e7[i]), int(lon_e7[i]))
        assert scalar == vec[i]


def test_hilbert_keys_world_corners_are_extremes_of_grid():
    n = 1 << hilbert.ORDER
    # South-west corner of the world maps to grid (0, 0)
    sw = hilbert.hilbert_key(-900000000, -1800000000)
    assert sw == hilbert.hilbert_xy2d(hilbert.ORDER, 0, 0)
    # North-east corner clamps into the last grid cell (n-1, n-1)
    ne = hilbert.hilbert_key(900000000, 1800000000)
    assert ne == hilbert.hilbert_xy2d(hilbert.ORDER, n - 1, n - 1)


def test_hilbert_keys_empty_array():
    out = hilbert.hilbert_keys(np.array([], dtype=np.int64), np.array([], dtype=np.int64))
    assert len(out) == 0
