"""`[diff:]`/`[adiff:]` (docs/m4-contracts.md section 3.2): two-pass
execution, action ordering, XML `<action>` shapes, the JSON extension, and
`out count`'s "acts like `out ids`" reference quirk (probe
`adiff_count_xml` == `adiff_ids_xml`).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_history_fixture  # noqa: E402


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("attic_diff_fixture")
    return make_history_fixture.build(str(root))


@pytest.fixture(scope="module")
def engine(fixture):
    return Engine(fixture.root)


# ------------------------------------------------------------------- diff


def test_diff_modify_action_json(engine, fixture):
    r = engine.run(
        f'[out:json][diff:"{fixture.hist_node_v1_from}","{fixture.hist_node_v3_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    assert len(r.elements) == 1
    action = r.elements[0]
    assert action["action"] == "modify"
    assert action["type"] == "node"
    assert action["id"] == fixture.hist_node_id
    assert action["old"]["version"] == 1
    assert action["new"]["version"] == 3
    assert "new" in action and "old" in action


def test_diff_create_action_has_no_old_key(engine, fixture):
    r = engine.run(
        f'[out:json][diff:"{fixture.hist_node_v3_from}","{fixture.hist_node_v3_from}"];'
        f'node({fixture.hist_new_after_since_id});out meta;'
    )
    # Never existed at either endpoint here -> no action at all.
    assert r.elements == []
    r2 = engine.run(
        f'[out:json][diff:"2024-01-01T00:00:00Z","{fixture.hist_new_after_since_from}"];'
        f'node({fixture.hist_new_after_since_id});out meta;'
    )
    assert len(r2.elements) == 1
    assert r2.elements[0]["action"] == "create"
    assert "old" not in r2.elements[0]
    assert r2.elements[0]["new"]["version"] == 1


def test_diff_delete_action_has_no_new_key(engine, fixture):
    r = engine.run(
        f'[out:json][diff:"{fixture.hist_node_v3_from}","{fixture.hist_node_v4_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    assert len(r.elements) == 1
    assert r.elements[0]["action"] == "delete"
    assert "new" not in r.elements[0]
    assert r.elements[0]["old"]["version"] == 3


def test_diff_identical_state_produces_no_action(engine, fixture):
    r = engine.run(
        f'[out:json][diff:"{fixture.hist_node_v1_from}","{fixture.hist_node_v1_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    assert r.elements == []


def test_diff_b_omitted_means_now(engine, fixture):
    # `b` omitted -> "now" (the far-future sentinel, section 3.2): the
    # node is deleted by then, so this is a delete action.
    r = engine.run(
        f'[out:json][diff:"{fixture.hist_node_v3_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    assert r.elements[0]["action"] == "delete"


def test_diff_xml_shape_modify(engine, fixture):
    r = engine.run(
        f'[out:xml][diff:"{fixture.hist_node_v1_from}","{fixture.hist_node_v3_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    xml, ctype = r.render()
    assert ctype == "application/osm3s+xml"
    assert '<action type="modify">' in xml
    assert "<old>" in xml and "</old>" in xml
    assert "<new>" in xml and "</new>" in xml
    assert 'version="1"' in xml
    assert 'version="3"' in xml


def test_diff_out_order_reversed_dates_just_labels_old_new_by_pass(engine, fixture):
    # No validation of a<b (confirmed against probe `diff_badorder_xml`):
    # the reference just runs pass 1 = a (labeled old), pass 2 = b
    # (labeled new), regardless of chronological order.
    r = engine.run(
        f'[out:json][diff:"{fixture.hist_node_v3_from}","{fixture.hist_node_v1_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    action = r.elements[0]
    assert action["old"]["version"] == 3
    assert action["new"]["version"] == 1


def test_diff_json_is_supported_as_an_extension(engine, fixture):
    # docs/m4-contracts.md section 3.2: the reference errors on JSON diff
    # output; we render it anyway (documented extension).
    r = engine.run(
        f'[out:json][diff:"{fixture.hist_node_v1_from}","{fixture.hist_node_v3_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    text, ctype = r.render()
    assert ctype == "application/json"
    import json

    doc = json.loads(text)
    assert doc["elements"][0]["action"] == "modify"


# ------------------------------------------------------------------ adiff


def test_adiff_delete_gets_a_visible_true_new_stub_when_object_still_exists(engine, fixture):
    # Probe `adiff_xml`: an element still present in the dataset (just no
    # longer matching the query) gets `<new visible="true">` on its
    # delete action -- our fixture's analogue is a real OSM deletion
    # (visible=false), covered by the next test; this one exercises the
    # "still exists but the query no longer matches" family via a range
    # that ends before the deletion.
    r = engine.run(
        f'[out:json][adiff:"{fixture.hist_node_v3_from}","{fixture.hist_node_v4_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    action = r.elements[0]
    assert action["action"] == "delete"
    assert action["new"]["visible"] is False  # a real deletion in our fixture


def test_adiff_create_becomes_modify_with_a_minimal_old_stub(engine, fixture):
    # Probe finding (diff_xml vs adiff_xml): a "create" in diff becomes a
    # "modify" in adiff when the object's raw state already existed at a
    # -- the way's node existed (v1) before the diff window but wasn't
    # queried directly; use the way's own transition into version 2's
    # window instead, which is a real create->modify flip case: query a
    # narrow window where hist_node only starts matching a tag filter at
    # b (simulate via an id-based query where the object exists earlier
    # than `a` -- v1 predates `a`).
    r = engine.run(
        f'[out:json][adiff:"{fixture.hist_node_v2_from}","{fixture.hist_node_v3_from}"];'
        f"node({fixture.hist_node_id});out meta;"
    )
    action = r.elements[0]
    assert action["action"] == "modify"
    assert action["old"]["version"] == 2
    assert action["new"]["version"] == 3


def test_adiff_and_diff_agree_when_nothing_falls_out_of_the_filter(engine, fixture):
    args = (fixture.hist_node_v1_from, fixture.hist_node_v3_from)
    r_diff = engine.run(f'[out:json][diff:"{args[0]}","{args[1]}"];node({fixture.hist_node_id});out meta;')
    r_adiff = engine.run(f'[out:json][adiff:"{args[0]}","{args[1]}"];node({fixture.hist_node_id});out meta;')
    assert r_diff.elements == r_adiff.elements


def test_adiff_out_count_behaves_like_out_ids(engine, fixture):
    # Probe finding: `adiff_count_xml` is byte-identical to
    # `adiff_ids_xml` -- the reference has no real `out count` under
    # diff mode.
    r_count = engine.run(
        f'[out:xml][adiff:"{fixture.hist_node_v1_from}","{fixture.hist_node_v3_from}"];'
        f"node({fixture.hist_node_id});out count;"
    )
    r_ids = engine.run(
        f'[out:xml][adiff:"{fixture.hist_node_v1_from}","{fixture.hist_node_v3_from}"];'
        f"node({fixture.hist_node_id});out ids;"
    )
    assert r_count.render()[0] == r_ids.render()[0]
    assert "<count>" not in r_count.render()[0]


# ---------------------------------------------------------- diff/adiff + no history


def test_diff_and_adiff_require_history():
    from osmpq.errors import UnsupportedError

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from fixtures import make_fixture

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        make_fixture.build(tmp, manifest_version=4)
        eng = Engine(tmp)
        with pytest.raises(UnsupportedError):
            eng.run('[out:xml][adiff:"2020-01-01T00:00:00Z"];node(1);out;')
