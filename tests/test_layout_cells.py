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


# --------------------------------------------------------------------------
# containing_cell_v2 / containing_cells_v2_np, per docs/m1-contracts.md section 2
# --------------------------------------------------------------------------


def test_containing_cell_v2_whole_world_bbox_is_root():
    leaves = _all_leaves_at_depth(3)
    li = cells.LeafIndex(leaves)
    assert cells.containing_cell_v2((-90, -180, 90, 180), li, [0, 3], 13) == "root"


def test_containing_cell_v2_bbox_in_one_leaf_uses_that_leaf_exactly():
    leaves = _all_leaves_at_depth(4)
    li = cells.LeafIndex(leaves)
    leaf = "1230"
    south, west, north, east = cells.cell_bbox(leaf)
    cx, cy = (west + east) / 2, (south + north) / 2
    tiny = (cy - 0.001, cx - 0.001, cy + 0.001, cx + 0.001)
    # ancestor_depths deliberately excludes 4: a leaf is used as-is even when
    # its own depth isn't one of the allowed ancestor depths.
    assert cells.containing_cell_v2(tiny, li, [0, 3], 13) == leaf


def test_containing_cell_v2_matches_v1_when_ancestor_depths_is_unrestricted():
    """With every depth allowed and max_depth == the leaf depth, v2 must
    agree with the unrestricted v1 rule (containing_cell_v2 is a strict
    generalization of it)."""
    leaves = _all_leaves_at_depth(4)
    li = cells.LeafIndex(leaves)
    rng = np.random.default_rng(1)
    for _ in range(200):
        lat1, lat2 = sorted(rng.uniform(-89, 89, 2))
        lon1, lon2 = sorted(rng.uniform(-179, 179, 2))
        bbox = (lat1, lon1, lat2, lon2)
        v1 = cells.containing_cell(bbox, li)
        v2 = cells.containing_cell_v2(bbox, li, [0, 1, 2, 3, 4], 4)
        assert v1 == v2, (bbox, v1, v2)


def test_containing_cell_v2_spans_two_leaves_under_non_allowed_depth_rounds_up():
    """A bbox whose unrestricted containing cell C sits at depth 7 (not in
    ancestor_depths) must round up to the ancestor at depth 6."""
    key7 = "0123012"  # depth 7, digits in {0,1,2,3}
    leaves = {key7 + d for d in "0123"}  # depth-8 leaves under key7
    li = cells.LeafIndex(leaves)
    s0, w0, n0, e0 = cells.cell_bbox(key7 + "0")
    cx0, cy0 = (w0 + e0) / 2, (s0 + n0) / 2
    s1, w1, n1, e1 = cells.cell_bbox(key7 + "1")
    cx1, cy1 = (w1 + e1) / 2, (s1 + n1) / 2
    bbox = (min(cy0, cy1), min(cx0, cx1), max(cy0, cy1), max(cx0, cx1))
    result = cells.containing_cell_v2(bbox, li, cells.DEFAULT_ANCESTOR_DEPTHS, 13)
    assert result == key7[:6]


def test_containing_cell_v2_leaf_deeper_than_12_stays_as_leaf():
    leaf13 = "0123012301230"
    assert len(leaf13) == 13
    li = cells.LeafIndex({leaf13})
    south, west, north, east = cells.cell_bbox(leaf13)
    cx, cy = (west + east) / 2, (south + north) / 2
    tiny = (cy - 1e-7, cx - 1e-7, cy + 1e-7, cx + 1e-7)
    result = cells.containing_cell_v2(tiny, li, cells.DEFAULT_ANCESTOR_DEPTHS, 13)
    assert result == leaf13


def test_containing_cells_v2_np_batch_matches_scalar():
    leaves = _all_leaves_at_depth(4)
    li = cells.LeafIndex(leaves)
    rng = np.random.default_rng(2)
    n = 300
    lat1 = rng.uniform(-89, 89, n)
    lat2 = rng.uniform(-89, 89, n)
    lon1 = rng.uniform(-179, 179, n)
    lon2 = rng.uniform(-179, 179, n)
    ymin = np.minimum(lat1, lat2)
    ymax = np.maximum(lat1, lat2)
    xmin = np.minimum(lon1, lon2)
    xmax = np.maximum(lon1, lon2)
    ymin_e7 = np.round(ymin * 1e7).astype(np.int64)
    xmin_e7 = np.round(xmin * 1e7).astype(np.int64)
    ymax_e7 = np.round(ymax * 1e7).astype(np.int64)
    xmax_e7 = np.round(xmax * 1e7).astype(np.int64)
    batch = cells.containing_cells_v2_np(
        ymin_e7, xmin_e7, ymax_e7, xmax_e7, li, cells.DEFAULT_ANCESTOR_DEPTHS, 13
    )
    for i in range(n):
        scalar = cells.containing_cell_v2(
            (ymin[i], xmin[i], ymax[i], xmax[i]), li, cells.DEFAULT_ANCESTOR_DEPTHS, 13
        )
        assert batch[i] == scalar, (i, batch[i], scalar)


# --------------------------------------------------------------------------
# containing_cell_v2 / containing_cells_v2_np with a gap in leaf coverage.
#
# osmpq.build.common.select_leaf_cells never adds a zero-node cell as a
# leaf (not even as an empty placeholder), so leaves don't tile the world
# on an extract: there can be whole regions with no leaf at all (an extract
# pulls in a boundary-crossing way's every node, "smart" bbox extraction,
# docs/m0-contracts.md section 5, so a new element from a later replication
# diff can land somewhere the base build never put a node). Found live on
# the Minnesota extract: a batch of new ways at (44.016, -88.150), inside
# the dataset's wide recorded extent but in a part of the quadtree with no
# real leaf, placed at cell "03000003" (depth 8) -- neither a real leaf nor
# one of ancestor_depths [0,3,6,9,12], which osmpq validate rejects.
# --------------------------------------------------------------------------


def test_leaf_index_contains_qk_true_for_a_real_leaf_point():
    leaves = {"0300000" + d for d in "012"}  # "0300003" deliberately missing
    li = cells.LeafIndex(leaves)
    south, west, north, east = cells.cell_bbox("03000001")
    cx, cy = (west + east) / 2, (south + north) / 2
    qk = cells.point_to_qk(cy, cx)
    idx = li.leaf_index_for_qk(np.array([qk], dtype=np.uint64))
    assert li.leaves_by_lo[idx[0]] == "03000001"
    assert bool(li.contains_qk(idx, np.array([qk], dtype=np.uint64))[0]) is True


def test_leaf_index_contains_qk_false_in_a_coverage_gap():
    """A point inside the *missing* child "03000003" gets matched to its
    nearest sorted neighbour ("03000002") by leaf_index_for_qk, but that
    leaf's own range doesn't actually reach the point: contains_qk must say
    so, which is what lets containing_cells_v2_np detect the gap."""
    leaves = {"0300000" + d for d in "012"}
    li = cells.LeafIndex(leaves)
    south, west, north, east = cells.cell_bbox("03000003")
    cx, cy = (west + east) / 2, (south + north) / 2
    qk = np.array([cells.point_to_qk(cy, cx)], dtype=np.uint64)
    idx = li.leaf_index_for_qk(qk)
    assert li.leaves_by_lo[idx[0]] == "03000002"  # nearest match, but wrong
    assert bool(li.contains_qk(idx, qk)[0]) is False


def test_containing_cell_v2_bbox_in_coverage_gap_snaps_to_allowed_depth_not_the_nearest_leaf():
    """Regression test for the real Minnesota incident: a tiny bbox fully
    inside a gap (no leaf at any depth covers it) must resolve to a real
    leaf or an ancestor_depths value -- never the depth of whatever leaf
    happened to sort nearest, if that leaf doesn't actually contain it."""
    leaves = {"0300000" + d for d in "012"}  # depth 8; "...3" has no leaf
    li = cells.LeafIndex(leaves)
    south, west, north, east = cells.cell_bbox("03000003")
    cx, cy = (west + east) / 2, (south + north) / 2
    tiny = (cy - 1e-7, cx - 1e-7, cy + 1e-7, cx + 1e-7)
    result = cells.containing_cell_v2(tiny, li, cells.DEFAULT_ANCESTOR_DEPTHS, 13)
    assert result != "03000002"  # must not silently reuse the wrong neighbour
    assert result != "03000003"  # must not return the unrounded depth-8 prefix either
    assert result in li or len(result) in cells.DEFAULT_ANCESTOR_DEPTHS or result == "root"
    # deterministic: nearest (invalid) leaf is at depth 8, greatest allowed
    # ancestor_depth <= 8 is 6, so the depth-8 prefix is rounded up to 6.
    assert result == "030000"


def test_containing_cell_v2_bbox_entirely_outside_the_leaf_tree_falls_back_to_root():
    """No leaf anywhere near the query bbox at all (not just one missing
    sibling): the nearest sorted match is far away and shallow, so even the
    smallest allowed ancestor depth (0) is the only safe answer."""
    leaves = {"00"}  # a single very shallow leaf, nowhere near branch "3"
    li = cells.LeafIndex(leaves)
    south, west, north, east = cells.cell_bbox("321")
    cx, cy = (west + east) / 2, (south + north) / 2
    tiny = (cy - 1e-7, cx - 1e-7, cy + 1e-7, cx + 1e-7)
    result = cells.containing_cell_v2(tiny, li, cells.DEFAULT_ANCESTOR_DEPTHS, 13)
    assert result == "root"


def test_containing_cell_v2_bbox_in_coverage_gap_shallow_neighbour_rounds_to_depth_3():
    """Same gap scenario, but the nearest (invalid) leaf match is shallow
    (depth 3), so the rounded answer is ancestor_depths' depth-3 entry, not
    depth 6 or root -- i.e. the rounding genuinely tracks the nearest
    match's depth, it isn't hardcoded to a single fallback depth."""
    leaves = {"123"}  # depth 3, only under branch "1"
    li = cells.LeafIndex(leaves)
    south, west, north, east = cells.cell_bbox("2000")  # branch "2": no leaf there at all
    cx, cy = (west + east) / 2, (south + north) / 2
    tiny = (cy - 1e-7, cx - 1e-7, cy + 1e-7, cx + 1e-7)
    result = cells.containing_cell_v2(tiny, li, cells.DEFAULT_ANCESTOR_DEPTHS, 13)
    assert result == "200"  # depth-3 ancestor of the query point


def test_containing_cells_v2_np_batch_with_gaps_matches_scalar():
    """The batched/vectorized path and the scalar wrapper must agree even
    when every row falls in a coverage gap (not just the happy path
    covered by test_containing_cells_v2_np_batch_matches_scalar)."""
    leaves = {"0300000" + d for d in "012"}
    li = cells.LeafIndex(leaves)
    rng = np.random.default_rng(3)
    south, west, north, east = cells.cell_bbox("03000003")
    n = 50
    lat = rng.uniform(south, north, n)
    lon = rng.uniform(west, east, n)
    lat_e7 = np.round(lat * 1e7).astype(np.int64)
    lon_e7 = np.round(lon * 1e7).astype(np.int64)
    batch = cells.containing_cells_v2_np(
        lat_e7, lon_e7, lat_e7, lon_e7, li, cells.DEFAULT_ANCESTOR_DEPTHS, 13
    )
    for i in range(n):
        scalar = cells.containing_cell_v2(
            (lat[i], lon[i], lat[i], lon[i]), li, cells.DEFAULT_ANCESTOR_DEPTHS, 13
        )
        assert batch[i] == scalar, (i, batch[i], scalar)
        assert batch[i] == "030000"
