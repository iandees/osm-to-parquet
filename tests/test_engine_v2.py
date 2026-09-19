"""M1 engine changes (docs/m1-contracts.md sections 2/4/5/6):

- manifest v2's ancestor-depth-restricted `cells_for_bbox` for ways/
  relations (a pure `catalog` unit test, no fixture needed).
- the row-group index and file pruning it drives.
- metadata (version/changeset/timestamp/uid/user) on untagged nodes,
  emitted by `out meta` when present and omitted when NULL.
- a v1-vs-v2 equivalence check: the same queries against a v1-mode and a
  v2-mode build of the same logical fixture return the same elements.

v1 behaviour itself stays covered by test_engine_basic.py; this file only
adds what v2 changes.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine, catalog

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


def bbox_args(bbox):
    s, w, n, e = bbox
    return f"{s},{w},{n},{e}"


# --------------------------------------------------------------------------
# catalog.cells_for_bbox: v2 ancestor-depth filtering, in isolation
# --------------------------------------------------------------------------


def _hand_built_v2_manifest(extra_way_cells: dict) -> catalog.Manifest:
    """A manifest with a hand-picked quadtree, independent of
    make_fixture.py, so this test proves `cells_for_bbox`'s depth-filtering
    logic itself rather than anything about how a real builder happens to
    place things. Leaves: "00000".."00003" (depth 5, children of "0000",
    child of "000", child of "00", child of "0"). `extra_way_cells` lets a
    test plant a way at a "trap" depth (e.g. 4 or 2) that a v2 builder
    would never legitimately produce, to prove the filter excludes it even
    when the manifest *does* have data there."""
    leaves = ["00000", "00001", "00002", "00003"]
    way_cells = {c: {"path": f"way-{c}.parquet", "rows": 1, "bbox": None} for c in extra_way_cells}
    for c, entry in extra_way_cells.items():
        way_cells[c] = entry
    data = {
        "manifest_version": 2,
        "generation": "g0001",
        "ancestor_depths": [0, 3, 6, 9, 12],
        "max_depth": 13,
        "leaf_cells": leaves,
        "tables": {
            "node": {"cells": {}},
            "way": {"cells": way_cells},
            "relation": {"cells": {}},
        },
        "byid": {"node": [], "way": [], "relation": []},
        "index": {"node_way": [], "member": []},
    }
    return catalog.Manifest(root="/nonexistent", data=data)


def test_cells_for_bbox_v2_only_leaf_and_ancestor_depths_are_wanted():
    # Cells present at every possible ancestor depth of leaf "00000"
    # (1, 2, 3, 4, "root"=0) plus the leaf itself and an unrelated sibling
    # ("00001") -- so the only thing that can make a depth get excluded is
    # the filtering logic, never simple absence from the manifest.
    manifest = _hand_built_v2_manifest({
        "root": {"path": "way-root.parquet", "rows": 1, "bbox": None},
        "0": {"path": "way-0.parquet", "rows": 1, "bbox": None},
        "00": {"path": "way-00.parquet", "rows": 1, "bbox": None},
        "000": {"path": "way-000.parquet", "rows": 1, "bbox": None},
        "0000": {"path": "way-0000.parquet", "rows": 1, "bbox": None},
        "00000": {"path": "way-00000.parquet", "rows": 1, "bbox": None},
        "00001": {"path": "way-00001.parquet", "rows": 1, "bbox": None},
    })
    # Strictly inside "00000", not touching its shared edge with sibling
    # leaf "00001" (bbox_intersects is boundary-inclusive, so the full
    # cell bbox would also "intersect" that neighbor -- irrelevant to what
    # this test checks).
    s, w, n, e = catalog.cell_bbox("00000")
    ds, dw = (n - s) * 0.1, (e - w) * 0.1
    bbox = (s + ds, w + dw, n - ds, e - dw)
    cells = catalog.cells_for_bbox(manifest, "way", bbox)
    # depth 0 (root), depth 3 ("000") and the leaf itself (depth 5,
    # "00000") are wanted; depths 1 ("0"), 2 ("00") and 4 ("0000") are not
    # in ancestor_depths=[0,3,6,9,12] and must be excluded even though the
    # manifest has data there. The unrelated sibling leaf "00001" must not
    # appear either (it doesn't intersect the query bbox).
    assert cells == ["000", "00000", "root"]


def test_cells_for_bbox_v2_leaf_itself_always_wanted_even_off_ancestor_depths():
    # A leaf at a depth that is *not* in ancestor_depths (5) must still be
    # selected on its own: an element can be stored exactly at a leaf,
    # regardless of that leaf's depth (m1-contracts.md section 2).
    manifest = _hand_built_v2_manifest({
        "00000": {"path": "way-00000.parquet", "rows": 1, "bbox": None},
    })
    cells = catalog.cells_for_bbox(manifest, "way", catalog.cell_bbox("00000"))
    assert cells == ["00000"]


def test_cells_for_bbox_v1_manifest_keeps_every_ancestor():
    leaves = ["00000", "00001"]
    data = {
        "manifest_version": 1,
        "generation": "g0001",
        "leaf_cells": leaves,
        "tables": {
            "node": {"cells": {}},
            "way": {
                "cells": {
                    "root": {"path": "w-root.parquet"},
                    "0": {"path": "w-0.parquet"},
                    "00": {"path": "w-00.parquet"},
                    "000": {"path": "w-000.parquet"},
                    "0000": {"path": "w-0000.parquet"},
                }
            },
            "relation": {"cells": {}},
        },
        "byid": {"node": [], "way": [], "relation": []},
        "index": {"node_way": [], "member": []},
    }
    manifest = catalog.Manifest(root="/nonexistent", data=data)
    cells = catalog.cells_for_bbox(manifest, "way", catalog.cell_bbox("00000"))
    # v1: every ancestor of the intersecting leaf, unrestricted by depth.
    assert cells == ["0", "00", "000", "0000", "root"]


# --------------------------------------------------------------------------
# Fixture-backed tests (row-group pruning, out meta, v1/v2 equivalence)
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fixture_v1(tmp_path_factory):
    root = tmp_path_factory.mktemp("engine_v2_fixture_v1")
    return make_fixture.build(str(root), manifest_version=1)


@pytest.fixture(scope="module")
def fixture_v2(tmp_path_factory):
    root = tmp_path_factory.mktemp("engine_v2_fixture_v2")
    return make_fixture.build(str(root), manifest_version=2)


@pytest.fixture(scope="module")
def engine_v1(fixture_v1):
    return Engine(fixture_v1.root)


@pytest.fixture(scope="module")
def engine_v2(fixture_v2):
    return Engine(fixture_v2.root)


def test_v2_manifest_round_trips(fixture_v2):
    manifest = catalog.load_manifest(fixture_v2.root)
    assert manifest.manifest_version == 2
    assert manifest.ancestor_depths == [0, 3, 6, 9, 12]
    assert manifest.max_depth == 13
    assert set(manifest.rowgroup_index_paths) == {"node", "way", "relation"}


def test_v2_trap_way_placed_at_promoted_ancestor_depth(fixture_v2):
    # way TRAP_WAY_ID straddles two leaves under "3000" (depth 4); v1's
    # unrestricted rule would store it at "3000" itself, but v2 restricts
    # loose placement to leaves + ancestor_depths, so it promotes to "300"
    # (depth 3, the greatest allowed depth <= 4).
    manifest = catalog.load_manifest(fixture_v2.root)
    way_cells = manifest.table_cells("way")
    assert "300" in way_cells
    assert "3000" not in way_cells
    assert "30" not in way_cells
    assert "3" not in way_cells


def test_v2_spanning_way_and_relations_promoted_to_root(fixture_v2):
    # way 110 and relations 202/204/205/206 sit at ancestor "00" (depth 2)
    # under v1; depth 2 is not in ancestor_depths, so v2 promotes them to
    # "root" (depth 0) -- the "one long way at root" case.
    manifest = catalog.load_manifest(fixture_v2.root)
    assert "00" not in manifest.table_cells("way")
    assert "00" not in manifest.table_cells("relation")
    root_ways = manifest.table_cells("way")["root"]
    assert fixture_v2.spanning_way_id in _ids_in_spatial_file(fixture_v2.root, root_ways["path"], "id")


def _ids_in_spatial_file(root, relpath, id_col):
    import duckdb

    con = duckdb.connect()
    ids = [r[0] for r in con.execute(f"SELECT {id_col} FROM read_parquet('{root}/{relpath}')").fetchall()]
    con.close()
    return ids


def test_v2_row_group_pruning_skips_file_with_no_intersecting_row_group(engine_v2, fixture_v2):
    # A pure bbox way query scoped to leaf "001": cells_for_bbox (v2) also
    # candidates "root" (always included) and the touching-edge neighbor
    # leaves "000"/"002" (m0's cell-intersection test is boundary-
    # inclusive), but none of way 110's real bbox (filed at "root" after
    # promotion) or ways 101-104/111/112 (filed at "000") actually
    # intersects leaf "001" -- so their files should be pruned by the
    # row-group index, while the query still returns exactly the ways
    # genuinely in "001".
    b = bbox_args(fixture_v1_leaf001_bbox(fixture_v2))
    r = engine_v2.run(f"[out:json];way({b});out ids;")
    ids = sorted(e["id"] for e in r.elements)
    assert ids == [105, 106, 107]
    assert r.stats["files_read"] < r.stats["files_considered"]
    assert r.stats["files_read"] >= 1


def fixture_v1_leaf001_bbox(fixture_v2):
    return fixture_v2.leaf_bbox["001"]


def test_v2_row_group_pruning_matches_unpruned_v1_results(engine_v1, fixture_v1, engine_v2, fixture_v2):
    # Same bbox, same logical ways, v1 (no rowgroup index -> no pruning) vs
    # v2 (pruned): identical result sets, proving pruning drops only files,
    # never rows that belong in the answer.
    b = bbox_args(fixture_v1.leaf_bbox["001"])
    r1 = engine_v1.run(f"[out:json];way({b});out ids;")
    r2 = engine_v2.run(f"[out:json];way({b});out ids;")
    assert sorted(e["id"] for e in r1.elements) == sorted(e["id"] for e in r2.elements)
    # v1 has no rowgroup_index at all -- files_considered == files_read.
    assert r1.stats["files_considered"] == r1.stats["files_read"]
    # v2 actually pruned something for this query.
    assert r2.stats["files_read"] < r2.stats["files_considered"]


def test_v1_manifest_has_no_rowgroup_pruning_effect(engine_v1, fixture_v1):
    b = bbox_args(fixture_v1.total_bbox)
    r = engine_v1.run(f"[out:json];node({b});out;")
    assert r.stats["files_considered"] == r.stats["files_read"]


# --------------------------------------------------------------- out meta


def test_out_meta_on_untagged_node_with_metadata(engine_v2, fixture_v2):
    r = engine_v2.run(f"[out:json];node({fixture_v2.trap_node_with_meta_id});out meta;")
    assert len(r.elements) == 1
    el = r.elements[0]
    for key in ("version", "timestamp", "changeset", "user", "uid"):
        assert key in el, f"{key} missing from {el}"
    assert el["timestamp"].endswith("Z")
    assert "tags" not in el  # untagged: no tags key at all (skel+meta, no tags present)


def test_out_meta_on_untagged_node_without_metadata_omits_fields(engine_v2, fixture_v2):
    r = engine_v2.run(f"[out:json];node({fixture_v2.trap_node_without_meta_id});out meta;")
    assert len(r.elements) == 1
    el = r.elements[0]
    for key in ("version", "timestamp", "changeset", "user", "uid"):
        assert key not in el, f"{key} should be omitted (NULL) but is present: {el}"
    # Still a well-formed node otherwise.
    assert el["type"] == "node"
    assert "lat" in el and "lon" in el


def _xml_element_fragment(body: str, node_id: int) -> str:
    start = body.index(f'<node id="{node_id}"')
    end = body.index("/>", start)
    return body[start:end]


def test_out_meta_on_untagged_node_xml_omits_null_attributes(engine_v2, fixture_v2):
    r = engine_v2.run(f"[out:xml];node({fixture_v2.trap_node_without_meta_id});out meta;")
    body, content_type = r.render()
    assert content_type == "application/osm3s+xml"
    fragment = _xml_element_fragment(body, fixture_v2.trap_node_without_meta_id)
    for attr in ("version=", "timestamp=", "changeset=", "uid=", "user="):
        assert attr not in fragment

    r2 = engine_v2.run(f"[out:xml];node({fixture_v2.trap_node_with_meta_id});out meta;")
    body2, _ = r2.render()
    fragment2 = _xml_element_fragment(body2, fixture_v2.trap_node_with_meta_id)
    for attr in ("version=", "timestamp=", "changeset=", "uid=", "user="):
        assert attr in fragment2


# ------------------------------------------------------- v1/v2 equivalence


EQUIVALENCE_QUERIES = [
    "[out:json];node[amenity=cafe]({b});out;",
    "[out:json];node[amenity!=cafe]({b});out ids;",
    "[out:json];node(id:{cafe},{restaurant});out;",
    "[out:json];way({closed_way});out geom;",
    "[out:json];way({spanning_way});>;out ids;",
    "[out:json];relation({spanning_relation});out geom;",
    "[out:json];node({cafe});out meta;",
    "[out:json];(node({cafe});node({restaurant}););out ids;",
    "[out:json];way({closed_way});node(w);out ids;",
]


def test_v1_v2_equivalence_same_queries_same_results(engine_v1, fixture_v1, engine_v2, fixture_v2):
    # v2 mode reuses every v1 id/tag/geometry verbatim (see
    # make_fixture.build's "v2 cell-placement adjustments"); only cell
    # placement, metadata-on-untagged-nodes and the row-group index differ.
    # So the same queries, addressed by id/bbox shared between both fixture
    # builds, must return the same elements (ignoring stats, which are
    # expected to differ -- that's the whole point of the row-group index).
    assert fixture_v1.leaf_bbox["000"] == fixture_v2.leaf_bbox["000"]
    ctx = {
        "b": bbox_args(fixture_v1.leaf_bbox["000"]),
        "cafe": fixture_v1.cafe_node_id,
        "restaurant": fixture_v1.restaurant_node_id,
        "closed_way": fixture_v1.closed_way_id,
        "spanning_way": fixture_v1.spanning_way_id,
        "spanning_relation": fixture_v1.spanning_relation_id,
    }
    for template in EQUIVALENCE_QUERIES:
        q = template.format(**ctx)
        r1 = engine_v1.run(q)
        r2 = engine_v2.run(q)
        assert r1.elements == r2.elements, f"mismatch for query: {q}"
