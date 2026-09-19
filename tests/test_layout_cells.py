"""Unit tests for osmpq.layout.cells, per docs/m0-contracts.md section 2."""
from __future__ import annotations

import numpy as np
import pytest

from osmpq.layout import cells


def test_root_bbox():
    assert cells.cell_bbox("root") == (-90.0, -180.0, 90.0, 180.0)


def test_children_order_and_bboxes():
    assert cells.children("root") == ["0", "1", "2", "3"]
    assert cells.children("1") == ["10", "11", "12", "13"]

    # 0=NW, 1=NE, 2=SW, 3=SE
    assert cells.cell_bbox("0") == (0.0, -180.0, 90.0, 0.0)
    assert cells.cell_bbox("1") == (0.0, 0.0, 90.0, 180.0)
    assert cells.cell_bbox("2") == (-90.0, -180.0, 0.0, 0.0)
    assert cells.cell_bbox("3") == (-90.0, 0.0, 0.0, 180.0)


def test_parent_and_ancestors():
    assert cells.parent("root") is None
    assert cells.parent("0") == "root"
    assert cells.parent("021") == "02"
    assert cells.ancestors("root") == []
    assert cells.ancestors("0") == ["root"]
    assert cells.ancestors("021") == ["02", "0", "root"]


def test_children_roundtrip_bbox():
    """Each cell's 4 children exactly tile its own bbox."""
    for key in ["root", "0", "13", "021"]:
        south, west, north, east = cells.cell_bbox(key)
        mid_lat = (south + north) / 2
        mid_lon = (west + east) / 2
        child_bboxes = {c[-1]: cells.cell_bbox(c) for c in cells.children(key)}
        assert child_bboxes["0"] == (mid_lat, west, north, mid_lon)
        assert child_bboxes["1"] == (mid_lat, mid_lon, north, east)
        assert child_bboxes["2"] == (south, west, mid_lat, mid_lon)
        assert child_bboxes["3"] == (south, mid_lon, mid_lat, east)


def test_max_depth_children_raises():
    key = "0" * cells.MAX_DEPTH
    with pytest.raises(ValueError):
        cells.children(key)


def _all_leaves_at_depth(depth: int) -> set[str]:
    leaves: set[str] = set()

    def rec(key: str, d: int) -> None:
        if d == depth:
            leaves.add(key)
            return
        for c in cells.children(key):
            rec(c, d + 1)

    rec(cells.ROOT, 0)
    return leaves


def test_point_cell_world_edges_included_in_last_cell():
    leaves = _all_leaves_at_depth(3)
    li = cells.LeafIndex(leaves)

    ne_key = cells.point_cell(90.0, 180.0, li)
    assert cells.cell_bbox(ne_key)[2] == 90.0
    assert cells.cell_bbox(ne_key)[3] == 180.0

    sw_key = cells.point_cell(-90.0, -180.0, li)
    assert cells.cell_bbox(sw_key)[0] == -90.0
    assert cells.cell_bbox(sw_key)[1] == -180.0

    # A point just below the world's north edge must NOT fall in the same
    # leaf as one just above the midline it straddles at a coarser level:
    # half-open on the high side, i.e. a point at the cell's own north bound
    # belongs to the cell above (or the world edge cell, if there is none).
    a = cells.point_cell(0.0, 0.0, li)  # exactly on internal boundaries
    assert cells.cell_bbox(a)[0] <= 0.0 <= cells.cell_bbox(a)[2]
    assert cells.cell_bbox(a)[1] <= 0.0 <= cells.cell_bbox(a)[3]


def test_point_cell_matches_vectorized():
    leaves = _all_leaves_at_depth(4)
    li = cells.LeafIndex(leaves)
    rng = np.random.default_rng(42)
    lat = rng.uniform(-90, 90, size=500)
    lon = rng.uniform(-180, 180, size=500)
    lat_e7 = np.round(lat * 1e7).astype(np.int64)
    lon_e7 = np.round(lon * 1e7).astype(np.int64)
    vec = cells.point_cells_np(lat_e7, lon_e7, li)
    for i in range(len(lat)):
        scalar = cells.point_cell(float(lat[i]), float(lon[i]), li)
        assert scalar == vec[i], (lat[i], lon[i], scalar, vec[i])


def test_point_cell_uneven_leaf_depths():
    """Leaves need not all be at the same depth (adaptive quadtree)."""
    leaves = {"0", "1", "2", "30", "31", "32", "33"}
    li = cells.LeafIndex(leaves)
    # A point in quadrant 0 should stop at "0" even though other branches
    # split further.
    assert cells.point_cell(45.0, -90.0, li) == "0"
    assert cells.point_cell(-80.0, 135.0, li) == "33"


def test_containing_cell_root_for_world_bbox():
    leaves = _all_leaves_at_depth(3)
    li = cells.LeafIndex(leaves)
    assert cells.containing_cell((-90, -180, 90, 180), li) == "root"


def test_containing_cell_descends_to_smallest_fully_containing_cell():
    leaves = _all_leaves_at_depth(4)
    li = cells.LeafIndex(leaves)
    # A tiny bbox well inside a single depth-4 leaf should resolve to that
    # leaf exactly.
    leaf = "1230"
    south, west, north, east = cells.cell_bbox(leaf)
    cx, cy = (west + east) / 2, (south + north) / 2
    tiny = (cy - 0.001, cx - 0.001, cy + 0.001, cx + 0.001)
    assert cells.containing_cell(tiny, li) == leaf


def test_containing_cell_bbox_spanning_two_children_stops_one_level_up():
    leaves = _all_leaves_at_depth(2)
    li = cells.LeafIndex(leaves)
    # A bbox straddling the prime meridian in the northern hemisphere spans
    # children "0" (NW) and "1" (NE) of root: root is the answer.
    bbox = (10.0, -5.0, 20.0, 5.0)
    assert cells.containing_cell(bbox, li) == "root"


def test_containing_cell_stops_at_existing_leaf_even_if_bbox_fits_deeper():
    # leaves = {"0", ...} means the "0" branch never split further, even
    # though a tiny bbox inside it would geometrically fit a much deeper cell.
    leaves = {"0", "1", "2", "3"}
    li = cells.LeafIndex(leaves)
    tiny = (44.999, -90.001, 45.001, -89.999)
    assert cells.containing_cell(tiny, li) == "0"


def test_cells_for_bbox_includes_ancestors_and_root():
    leaves = _all_leaves_at_depth(3)
    li = cells.LeafIndex(leaves)
    bbox = (1.0, 1.0, 2.0, 2.0)
    result = cells.cells_for_bbox(bbox, li)
    assert "root" in result
    # every returned non-leaf must be an ancestor of some returned leaf
    leafset = set(li.leaves)
    result_leaves = [k for k in result if k in leafset]
    assert result_leaves, "expected at least one intersecting leaf"
    for leaf in result_leaves:
        for anc in cells.ancestors(leaf):
            assert anc in result


def test_cells_for_bbox_only_intersecting_leaves():
    leaves = {"0", "1", "2", "3"}
    li = cells.LeafIndex(leaves)
    # bbox entirely within quadrant "3" (south, east)
    bbox = (-10.0, 10.0, -5.0, 20.0)
    result = cells.cells_for_bbox(bbox, li)
    assert set(result) == {"3", "root"}


def test_qk_range_nesting():
    # A cell's qk range must be a subset of its parent's, and the 4 children
    # exactly partition the parent's range.
    for key in ["root", "0", "13"]:
        lo, hi = cells.qk_range(key)
        children_ranges = [cells.qk_range(c) for c in cells.children(key)]
        # contiguous, non-overlapping, covering [lo, hi]
        children_ranges.sort()
        assert children_ranges[0][0] == lo
        assert children_ranges[-1][1] == hi
        for (a_lo, a_hi), (b_lo, b_hi) in zip(children_ranges, children_ranges[1:]):
            assert b_lo == a_hi + 1


def test_leaf_index_contains():
    li = cells.LeafIndex(["0", "1", "20", "21", "22", "23"])
    assert "0" in li
    assert "22" in li
    assert "2" not in li
    assert len(li) == 6
    assert sorted(li) == ["0", "1", "20", "21", "22", "23"]
