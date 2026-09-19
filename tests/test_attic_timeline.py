"""`timeline(type, id[, version])` (docs/m4-contracts.md section 3.2)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_history_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("attic_timeline_fixture")
    return make_history_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def test_timeline_node_has_one_entry_per_own_version(engine, fixture):
    r = engine.run(f"[out:json];timeline(node,{fixture.hist_node_id});out;")
    assert [e["type"] for e in r.elements] == ["timeline"] * 4
    tags = [e["tags"] for e in r.elements]
    assert [t["refversion"] for t in tags] == ["1", "2", "3", "4"]
    assert [int(e["id"]) for e in r.elements] == [1, 2, 3, 4]


def test_timeline_reftype_and_ref_are_strings(engine, fixture):
    r = engine.run(f"[out:json];timeline(node,{fixture.hist_node_id});out;")
    tags = r.elements[0]["tags"]
    assert tags["reftype"] == "node"
    assert tags["ref"] == str(fixture.hist_node_id)
    assert isinstance(tags["refversion"], str)


def test_timeline_created_and_expired_chain(engine, fixture):
    r = engine.run(f"[out:json];timeline(node,{fixture.hist_node_id});out;")
    tags = [e["tags"] for e in r.elements]
    assert tags[0]["created"] == fixture.hist_node_v1_from
    assert tags[0]["expired"] == fixture.hist_node_v2_from
    assert tags[1]["created"] == fixture.hist_node_v2_from
    assert tags[1]["expired"] == fixture.hist_node_v3_from
    assert tags[2]["created"] == fixture.hist_node_v3_from
    assert tags[2]["expired"] == fixture.hist_node_v4_from


def test_timeline_last_entry_has_no_expired_key(engine, fixture):
    r = engine.run(f"[out:json];timeline(node,{fixture.hist_node_id});out;")
    assert "expired" not in r.elements[-1]["tags"]


def test_timeline_way_excludes_minor_versions(engine, fixture):
    # docs/m4-contracts.md section 6 (confirmed against a live probe of
    # both a node-move-only way and m4probe/timeline_way_json.json):
    # minor/geometry-only states never get their own timeline entry.
    r = engine.run(f"[out:json];timeline(way,{fixture.hist_way_id});out;")
    tags = [e["tags"] for e in r.elements]
    assert [t["refversion"] for t in tags] == ["1", "2"]
    assert tags[0]["reftype"] == "way"


def test_timeline_relation_own_and_minor(engine, fixture):
    r = engine.run(f"[out:json];timeline(relation,{fixture.hist_relation_id});out;")
    tags = [e["tags"] for e in r.elements]
    # 1 minor version between own versions 1 and 2: still only 2 entries
    # (one per own version).
    assert [t["refversion"] for t in tags] == ["1", "2"]


def test_timeline_with_version_returns_a_single_renumbered_entry(engine, fixture):
    r = engine.run(f"[out:json];timeline(node,{fixture.hist_node_id},2);out;")
    assert len(r.elements) == 1
    assert r.elements[0]["id"] == 1
    assert r.elements[0]["tags"]["refversion"] == "2"


def test_timeline_missing_element_is_empty(engine, fixture):
    r = engine.run("[out:json];timeline(node,999999999);out;")
    assert r.elements == []


def test_timeline_xml_shape(engine, fixture):
    r = engine.run(f"[out:xml];timeline(node,{fixture.hist_node_id});out;")
    xml, _ctype = r.render()
    assert '<timeline id="1">' in xml
    assert '<tag k="reftype" v="node"/>' in xml
    assert '<tag k="refversion" v="1"/>' in xml
    assert "</timeline>" in xml


def test_out_count_puts_timeline_rows_under_total_only(engine, fixture):
    r = engine.run(f"[out:json];timeline(node,{fixture.hist_node_id});out count;")
    tags = r.elements[0]["tags"]
    assert tags["nodes"] == "0"
    assert tags["ways"] == "0"
    assert tags["relations"] == "0"
    assert tags["total"] == "4"
