"""Hilbert curve key, per docs/m0-contracts.md section 2.

(lon, lat) -> 2^20 x 2^20 grid -> Hilbert index at order 20 (classic
Wikipedia xy2d algorithm). Implemented in Python (rather than depending on
``osmpq.layout.hilbert``, which the builder team owns) and registered as a
DuckDB scalar UDF so SQL can compute it for byid-only rows.
"""
from __future__ import annotations

ORDER = 20
SIDE = 1 << ORDER  # 2^20
MAXCOORD = SIDE - 1


def _rot(n: int, x: int, y: int, rx: int, ry: int) -> tuple[int, int]:
    if ry == 0:
        if rx == 1:
            x = n - 1 - x
            y = n - 1 - y
        x, y = y, x
    return x, y


def xy2d(order: int, x: int, y: int) -> int:
    n = 1 << order
    rx = ry = 0
    d = 0
    s = n // 2
    while s > 0:
        rx = 1 if (x & s) > 0 else 0
        ry = 1 if (y & s) > 0 else 0
        d += s * s * ((3 * rx) ^ ry)
        x, y = _rot(n, x, y, rx, ry)
        s //= 2
    return d


def lonlat_to_xy(lon: float, lat: float) -> tuple[int, int]:
    x = int((lon + 180.0) / 360.0 * SIDE)
    y = int((lat + 90.0) / 180.0 * SIDE)
    x = max(0, min(MAXCOORD, x))
    y = max(0, min(MAXCOORD, y))
    return x, y


def lonlat_to_hilbert(lon: float, lat: float) -> int:
    x, y = lonlat_to_xy(lon, lat)
    return xy2d(ORDER, x, y)


def e7_to_hilbert(lon_e7: int, lat_e7: int) -> int:
    return lonlat_to_hilbert(lon_e7 / 1e7, lat_e7 / 1e7)


def bbox_e7_center_hilbert(xmin_e7: int, ymin_e7: int, xmax_e7: int, ymax_e7: int) -> int:
    lon = ((xmin_e7 + xmax_e7) / 2.0) / 1e7
    lat = ((ymin_e7 + ymax_e7) / 2.0) / 1e7
    return lonlat_to_hilbert(lon, lat)


def register_duckdb_udfs(con) -> None:
    """Register hilbert helpers as SQL scalar functions on a connection."""

    def _node_hilbert(lon_e7, lat_e7):
        if lon_e7 is None or lat_e7 is None:
            return None
        return e7_to_hilbert(int(lon_e7), int(lat_e7))

    def _bbox_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7):
        if xmin_e7 is None or ymin_e7 is None or xmax_e7 is None or ymax_e7 is None:
            return None
        return bbox_e7_center_hilbert(int(xmin_e7), int(ymin_e7), int(xmax_e7), int(ymax_e7))

    con.create_function(
        "opq_node_hilbert", _node_hilbert, ["BIGINT", "BIGINT"], "UBIGINT", null_handling="special"
    )
    con.create_function(
        "opq_bbox_hilbert",
        _bbox_hilbert,
        ["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
        "UBIGINT",
        null_handling="special",
    )
