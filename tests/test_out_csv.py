"""Tests for docs/m3-contracts.md section 3.6: `[out:csv(...)]`, against
the synthetic fixture (tests/fixtures/make_fixture.py).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine
from osmpq.engine.result import Result
from osmpq.ql.ast import Settings

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("out_csv_fixture")
    return make_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


def bbox_args(bbox):
    s, w, n, e = bbox
    return f"{s},{w},{n},{e}"


def test_csv_content_type(engine, fixture):
    r = engine.run(f"[out:csv(::id)];node({fixture.cafe_node_id});out;")
    text, content_type = r.render()
    assert content_type == "text/csv; charset=utf-8"
    assert text == "@id\n1\n"


def test_csv_tag_key_and_special_fields_node(engine, fixture):
    r = engine.run(f"[out:csv(name,::id,::type,::otype,::lat,::lon)];node({fixture.cafe_node_id});out;")
    lines = r.render()[0].splitlines()
    assert lines[0] == "name\t@id\t@type\t@otype\t@lat\t@lon"
    fields = lines[1].split("\t")
    assert fields[0] == "Aroma Cafe"
    assert fields[1] == str(fixture.cafe_node_id)
    assert fields[2] == "node"
    assert fields[3] == "n"
    # lat/lon are the node's own position, not rounded specially.
    assert float(fields[4]) == pytest.approx(70.875)
    assert float(fields[5]) == pytest.approx(-141.75)


def test_csv_missing_tag_key_is_empty(engine, fixture):
    # The restaurant node has no "name" tag at all.
    r = engine.run(f"[out:csv(name,::id)];node({fixture.restaurant_node_id});out;")
    lines = r.render()[0].splitlines()
    assert lines[1] == f"\t{fixture.restaurant_node_id}"


def test_csv_lat_lon_empty_for_way_without_center(engine, fixture):
    r = engine.run(f"[out:csv(::id,::lat,::lon)];way({fixture.closed_way_id});out skel;")
    lines = r.render()[0].splitlines()
    assert lines[1] == f"{fixture.closed_way_id}\t\t"


def test_csv_lat_lon_uses_center_for_way(engine, fixture):
    r = engine.run(f"[out:csv(::id,::lat,::lon)];way({fixture.closed_way_id});out center;")
    fields = r.render()[0].splitlines()[1].split("\t")
    assert fields[0] == str(fixture.closed_way_id)
    assert float(fields[1]) != 0.0
    assert float(fields[2]) != 0.0


def test_csv_out_count_fills_only_count(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(f"[out:csv(name,::id,::count)];way[building]({b});out count;")
    lines = r.render()[0].splitlines()
    assert lines[0] == "name\t@id\t@count"
    # `out count` leaves every other field empty and fills only ::count.
    fields = lines[1].split("\t")
    assert fields[0] == "" and fields[1] == ""
    assert fields[2] == "1"


def test_csv_meta_fields(engine, fixture):
    r = engine.run(
        f"[out:csv(::version,::changeset,::timestamp,::uid,::user)];way({fixture.closed_way_id});out meta;"
    )
    fields = r.render()[0].splitlines()[1].split("\t")
    assert all(f != "" for f in fields)


def test_csv_meta_fields_empty_without_out_meta(engine, fixture):
    # Without `out meta`, the element dict never carried version/user/etc,
    # so those columns come back empty rather than erroring.
    r = engine.run(
        f"[out:csv(::version,::changeset,::timestamp,::uid,::user)];way({fixture.closed_way_id});out skel;"
    )
    fields = r.render()[0].splitlines()[1].split("\t")
    assert fields == ["", "", "", "", ""]


def test_csv_header_can_be_disabled(engine, fixture):
    r = engine.run(f'[out:csv(name;false;"\t")];node({fixture.cafe_node_id});out;')
    lines = r.render()[0].splitlines()
    assert lines == ["Aroma Cafe"]


def test_csv_custom_separator(engine, fixture):
    r = engine.run(f'[out:csv(name,::id;true;",")];node({fixture.cafe_node_id});out;')
    lines = r.render()[0].splitlines()
    assert lines[0] == "name,@id"
    assert lines[1] == f"Aroma Cafe,{fixture.cafe_node_id}"


def test_csv_multiple_rows_multiset(engine, fixture):
    b = bbox_args(fixture.leaf_bbox["000"])
    r = engine.run(f"[out:csv(::id)][timeout:25];node[amenity=cafe]({b});out;")
    lines = r.render()[0].splitlines()
    assert lines[0] == "@id"
    assert sorted(int(l) for l in lines[1:]) == sorted([fixture.cafe_node_id, fixture.cafe_node2_id])


# ----------------------------------------------------- direct Result unit


def test_render_csv_directly_on_result():
    settings = Settings(out_format="csv", csv_fields=["::id", "name"], csv_header=True, csv_separator="\t")
    result = Result(
        elements=[
            {"type": "node", "id": 5, "lat": 1.0, "lon": 2.0, "tags": {"name": "A"}},
            {"type": "node", "id": 6, "lat": 3.0, "lon": 4.0, "tags": {}},
        ],
        settings=settings,
    )
    text, content_type = result.render()
    assert content_type == "text/csv; charset=utf-8"
    assert text == "@id\tname\n5\tA\n6\t\n"


def test_render_csv_count_element_directly_on_result():
    settings = Settings(out_format="csv", csv_fields=["name", "::count"], csv_header=True)
    result = Result(
        elements=[{"type": "count", "id": 0, "tags": {"nodes": "2", "ways": "0", "relations": "0", "total": "2"}}],
        settings=settings,
    )
    text, _ = result.render()
    assert text == "name\t@count\n\t2\n"
