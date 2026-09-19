"""Hilbert curve key, per docs/m0-contracts.md section 2.

(lon, lat) -> 2^20 x 2^20 grid -> Hilbert index at order 20 (classic
Wikipedia xy2d algorithm). Implemented in Python (rather than depending on
``osmpq.layout.hilbert``, which the builder team owns) and registered as a
DuckDB UDF so SQL can compute it for byid-only rows (the spatial tables
already carry a precomputed ``hilbert`` column).

The UDFs are registered with ``type=ARROW`` (whole-batch pyarrow arrays in
and out) rather than the default per-row NATIVE calling convention. A
per-row Python callback pays a Python-level call plus a GIL acquisition for
*every single row*; profiling a `>` recurse over ~83k byid-resolved nodes
showed the scalar UDF alone accounting for ~23s of a ~25s query (DuckDB
reads Parquet with multiple threads, so those per-row GIL acquisitions are
also contended). The batch version below does the same bit-for-bit
arithmetic with ``pyarrow.compute`` so one Python call processes a whole
vector (thousands of rows) with no per-row interpreter overhead, which cut
that same query to well under a second of hilbert-computation time. See
``_vec_xy2d``/``_vec_lonlat_to_xy`` for the vectorized mirror of
``xy2d``/``lonlat_to_xy`` below; keep them in lockstep if the algorithm
ever changes.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

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



# --------------------------------------------------------------------------
# Vectorized (pyarrow.compute) mirror of xy2d/lonlat_to_xy above, for the
# ARROW-type DuckDB UDFs registered below.
# --------------------------------------------------------------------------

_ZERO = pa.scalar(0, pa.int64())
_ONE = pa.scalar(1, pa.int64())
_THREE = pa.scalar(3, pa.int64())
_MAXCOORD_SCALAR = pa.scalar(MAXCOORD, pa.int64())


def _vec_deg_to_xy(lon_deg, lat_deg):
    """pyarrow.compute mirror of ``lonlat_to_xy``, taking/returning whole
    (Chunked)Arrays of DOUBLE degrees (nulls propagate)."""
    xf = pc.multiply(pc.divide(pc.add(lon_deg, 180.0), 360.0), float(SIDE))
    yf = pc.multiply(pc.divide(pc.add(lat_deg, 90.0), 180.0), float(SIDE))
    # int() truncates toward zero; matching pc.cast needs safe=False since
    # values sit right at the SIDE boundary for lon=180/lat=90.
    x = pc.cast(xf, pa.int64(), safe=False)
    y = pc.cast(yf, pa.int64(), safe=False)
    # skip_nulls=False: element-wise min/max otherwise treat NULL as "not
    # present" and silently return the other operand instead of propagating
    # the null, which would turn a NULL coordinate into a fabricated hilbert
    # value instead of NULL.
    x = pc.max_element_wise(pc.min_element_wise(x, _MAXCOORD_SCALAR, skip_nulls=False), _ZERO, skip_nulls=False)
    y = pc.max_element_wise(pc.min_element_wise(y, _MAXCOORD_SCALAR, skip_nulls=False), _ZERO, skip_nulls=False)
    return x, y


def _vec_lonlat_to_xy(lon_e7, lat_e7):
    lon_deg = pc.divide(pc.cast(lon_e7, pa.float64()), 1e7)
    lat_deg = pc.divide(pc.cast(lat_e7, pa.float64()), 1e7)
    return _vec_deg_to_xy(lon_deg, lat_deg)


def _vec_xy2d(x, y):
    """pyarrow.compute mirror of ``xy2d(ORDER, x, y)`` over whole arrays."""
    d = pc.multiply(x, _ZERO)  # 0 with x's nullability/length
    s = SIDE // 2
    while s > 0:
        s_scalar = pa.scalar(s, pa.int64())
        rx = pc.cast(pc.greater(pc.bit_wise_and(x, s_scalar), _ZERO), pa.int64())
        ry = pc.cast(pc.greater(pc.bit_wise_and(y, s_scalar), _ZERO), pa.int64())
        d = pc.add(d, pc.multiply(pa.scalar(s * s, pa.int64()), pc.bit_wise_xor(pc.multiply(_THREE, rx), ry)))
        neg = pc.and_(pc.equal(ry, _ZERO), pc.equal(rx, _ONE))
        n1 = pa.scalar(SIDE - 1, pa.int64())
        x2 = pc.if_else(neg, pc.subtract(n1, x), x)
        y2 = pc.if_else(neg, pc.subtract(n1, y), y)
        swap = pc.equal(ry, _ZERO)
        x, y = pc.if_else(swap, y2, x), pc.if_else(swap, x2, y)
        s //= 2
    return d


def _vec_hilbert_from_xy(x, y):
    return pc.cast(_vec_xy2d(x, y), pa.uint64())


def _vec_node_hilbert(lon_e7, lat_e7):
    x, y = _vec_lonlat_to_xy(lon_e7, lat_e7)
    return _vec_hilbert_from_xy(x, y)


def _vec_bbox_hilbert(xmin_e7, ymin_e7, xmax_e7, ymax_e7):
    """Mirrors ``bbox_e7_center_hilbert``: the bbox center is computed as a
    float degree value straight from the e7 sums, same as the scalar
    version -- not rounded back through an intermediate e7 integer, which
    would lose sub-1e-7-degree precision the scalar path keeps."""
    lon_deg = pc.divide(pc.add(pc.cast(xmin_e7, pa.float64()), pc.cast(xmax_e7, pa.float64())), 2.0 * 1e7)
    lat_deg = pc.divide(pc.add(pc.cast(ymin_e7, pa.float64()), pc.cast(ymax_e7, pa.float64())), 2.0 * 1e7)
    x, y = _vec_deg_to_xy(lon_deg, lat_deg)
    return _vec_hilbert_from_xy(x, y)


def register_duckdb_udfs(con) -> None:
    """Register hilbert helpers as SQL functions on a connection.

    Registered with ``type=ARROW`` (see module docstring): DuckDB hands the
    whole column as a pyarrow array per call instead of invoking the
    callback once per row, which is what makes these usable on the byid
    (id-lookup) path where a `>`/`<`/`(id:...)` query can hydrate tens or
    hundreds of thousands of rows at once.
    """
    try:
        # Public location in some duckdb builds.
        from duckdb.functional import PythonUDFType  # type: ignore
    except ImportError:
        # DuckDB 1.5.5's Python wheel only exposes it on the C extension
        # module; still the officially documented enum (`type=...` on
        # `create_function`), just not re-exported from the `duckdb` package.
        from _duckdb._func import PythonUDFType  # type: ignore

    arrow_type = PythonUDFType.ARROW

    con.create_function(
        "opq_node_hilbert", _vec_node_hilbert, ["BIGINT", "BIGINT"], "UBIGINT", type=arrow_type
    )
    con.create_function(
        "opq_bbox_hilbert",
        _vec_bbox_hilbert,
        ["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
        "UBIGINT",
        type=arrow_type,
    )
