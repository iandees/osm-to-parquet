"""Unit tests for ``tools/diffcheck.py``'s comparison logic (no network),
per docs/m2-contracts.md section 7.1's deliverable: "unit-test the
comparison on two hand-written JSON responses."
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "tools"))

import diffcheck  # noqa: E402


# --------------------------------------------------------------------------
# compare_element: alive elements expected to match on both sides
# --------------------------------------------------------------------------


def test_matching_node_passes():
    ref = {"type": "node", "id": 1, "lat": 32.30001, "lon": -64.70002, "version": 3, "tags": {"amenity": "cafe"}}
    local = {"type": "node", "id": 1, "lat": 32.30001, "lon": -64.70002, "version": 3, "tags": {"amenity": "cafe"}}
    assert diffcheck.compare_element("node", ref, local) == []


def test_node_coords_within_tolerance_passes():
    ref = {"type": "node", "id": 1, "lat": 32.30000010, "lon": -64.70000020, "version": 1, "tags": {}}
    local = {"type": "node", "id": 1, "lat": 32.30000015, "lon": -64.70000015, "version": 1, "tags": {}}
    assert diffcheck.compare_element("node", ref, local) == []


def test_node_coords_outside_tolerance_fails():
    ref = {"type": "node", "id": 1, "lat": 32.300000, "lon": -64.700000, "version": 1, "tags": {}}
    local = {"type": "node", "id": 1, "lat": 32.300100, "lon": -64.700000, "version": 1, "tags": {}}
    problems = diffcheck.compare_element("node", ref, local)
    assert any("coordinates differ" in p for p in problems)


def test_tag_mismatch_reported():
    ref = {"type": "node", "id": 1, "lat": 1.0, "lon": 1.0, "version": 1, "tags": {"amenity": "cafe"}}
    local = {"type": "node", "id": 1, "lat": 1.0, "lon": 1.0, "version": 1, "tags": {"amenity": "restaurant"}}
    problems = diffcheck.compare_element("node", ref, local)
    assert any("tags differ" in p for p in problems)


def test_version_mismatch_reported():
    ref = {"type": "node", "id": 1, "lat": 1.0, "lon": 1.0, "version": 5, "tags": {}}
    local = {"type": "node", "id": 1, "lat": 1.0, "lon": 1.0, "version": 4, "tags": {}}
    problems = diffcheck.compare_element("node", ref, local)
    assert any("version differs" in p for p in problems)


def test_way_refs_match_passes():
    ref = {"type": "way", "id": 10, "version": 2, "tags": {"highway": "residential"}, "nodes": [1, 2, 3]}
    local = {"type": "way", "id": 10, "version": 2, "tags": {"highway": "residential"}, "nodes": [1, 2, 3]}
    assert diffcheck.compare_element("way", ref, local) == []


def test_way_refs_mismatch_reported():
    ref = {"type": "way", "id": 10, "version": 2, "tags": {}, "nodes": [1, 2, 3]}
    local = {"type": "way", "id": 10, "version": 2, "tags": {}, "nodes": [1, 2, 4]}
    problems = diffcheck.compare_element("way", ref, local)
    assert any("way refs differ" in p for p in problems)


def test_relation_members_match_passes():
    ref = {
        "type": "relation", "id": 20, "version": 1, "tags": {},
        "members": [{"type": "way", "ref": 10, "role": "outer"}, {"type": "node", "ref": 1, "role": ""}],
    }
    local = {
        "type": "relation", "id": 20, "version": 1, "tags": {},
        "members": [{"type": "way", "ref": 10, "role": "outer"}, {"type": "node", "ref": 1, "role": ""}],
    }
    assert diffcheck.compare_element("relation", ref, local) == []


def test_relation_members_mismatch_reported():
    ref = {
        "type": "relation", "id": 20, "version": 1, "tags": {},
        "members": [{"type": "way", "ref": 10, "role": "outer"}],
    }
    local = {
        "type": "relation", "id": 20, "version": 1, "tags": {},
        "members": [{"type": "way", "ref": 11, "role": "outer"}],
    }
    problems = diffcheck.compare_element("relation", ref, local)
    assert any("relation members differ" in p for p in problems)


def test_missing_on_local_reported():
    ref = {"type": "node", "id": 1, "lat": 1.0, "lon": 1.0, "version": 1, "tags": {}}
    problems = diffcheck.compare_element("node", ref, None)
    assert any("missing on local" in p for p in problems)


def test_missing_on_reference_reported():
    local = {"type": "node", "id": 1, "lat": 1.0, "lon": 1.0, "version": 1, "tags": {}}
    problems = diffcheck.compare_element("node", None, local)
    assert any("missing on reference" in p for p in problems)


def test_missing_on_both_reported():
    problems = diffcheck.compare_element("node", None, None)
    assert any("missing on both" in p for p in problems)


# --------------------------------------------------------------------------
# compare_deleted: elements expected absent on both sides
# --------------------------------------------------------------------------


def test_deleted_absent_both_passes():
    assert diffcheck.compare_deleted(None, None) == []


def test_deleted_present_on_reference_fails():
    ref = {"type": "way", "id": 99, "version": 1, "tags": {}}
    problems = diffcheck.compare_deleted(ref, None)
    assert any("present on reference" in p for p in problems)


def test_deleted_present_on_local_fails():
    local = {"type": "way", "id": 99, "version": 1, "tags": {}}
    problems = diffcheck.compare_deleted(None, local)
    assert any("present on local" in p for p in problems)


def test_deleted_present_on_both_fails_with_two_problems():
    el = {"type": "way", "id": 99, "version": 1, "tags": {}}
    problems = diffcheck.compare_deleted(el, el)
    assert len(problems) == 2


# --------------------------------------------------------------------------
# query building + batching
# --------------------------------------------------------------------------


def test_build_query_single_type_no_union():
    q = diffcheck.build_query({"node": [1, 2, 3]})
    assert "node(id:1,2,3);" in q
    assert "out meta;" in q
    assert q.count("(") == 1  # no union wrapper needed for a single statement type


def test_build_query_multiple_types_wraps_in_union():
    q = diffcheck.build_query({"node": [1], "way": [2]})
    assert "node(id:1);" in q
    assert "way(id:2);" in q
    assert q.strip().startswith("[out:json];\n(")


def test_build_query_includes_date_setting():
    q = diffcheck.build_query({"node": [1]}, date="2026-09-19T00:00:00Z")
    assert '[date:"2026-09-19T00:00:00Z"]' in q


def test_chunks_batches_ids():
    batches = diffcheck._chunks([1, 2, 3, 4, 5], 2)
    assert batches == [[1, 2], [3, 4], [5]]


def test_elements_by_key_skips_count():
    elements = [
        {"type": "count", "tags": {"total": "1"}},
        {"type": "node", "id": 5, "tags": {}},
    ]
    keyed = diffcheck.elements_by_key(elements)
    assert ("count", None) not in keyed
    assert ("node", 5) in keyed


# --------------------------------------------------------------------------
# id sourcing
# --------------------------------------------------------------------------


def test_sample_ids_returns_all_when_fewer_than_sample():
    touched = {"node": {1: False, 2: True}, "way": {}, "relation": {}}
    sampled = diffcheck.sample_ids(touched, 200)
    assert sampled["node"] == {1: False, 2: True}


def test_sample_ids_spreads_when_more_than_sample():
    touched = {"node": {i: False for i in range(1, 101)}, "way": {}, "relation": {}}
    sampled = diffcheck.sample_ids(touched, 10)
    assert len(sampled["node"]) == 10


def test_load_ids_file_roundtrip(tmp_path):
    import json

    payload = {"node": [[1, False], [2, True]], "way": [[3, False]], "relation": []}
    path = tmp_path / "ids.json"
    path.write_text(json.dumps(payload))
    loaded = diffcheck.load_ids_file(path)
    assert loaded["node"] == {1: False, 2: True}
    assert loaded["way"] == {3: False}
    assert loaded["relation"] == {}
