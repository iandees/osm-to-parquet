#!/usr/bin/env python3
"""Self-test for tools/difftest.py's comparison logic.

Not a pytest module on purpose (other agents' test discovery should not
pick this up) -- run it directly:

    python tests/corpus/selftest.py

It exercises compare() against two hand-written fixture Overpass JSON
responses (fixture_ref.json / fixture_local.json) that are wired up with
a known set of matches, misses, and mismatches, plus a couple of direct
checks of the tolerance helpers and the `out count` comparison path.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "tools"))

import difftest as dt  # noqa: E402

FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"ok   - {label}")
    else:
        print(f"FAIL - {label} {detail}")
        FAILURES.append(label)


def main() -> int:
    ref = json.loads((HERE / "fixture_ref.json").read_text())
    local = json.loads((HERE / "fixture_local.json").read_text())

    cmp = dt.compare(ref["elements"], local["elements"])

    check("overall status is FAIL (fixtures are deliberately mismatched)", cmp.status == "FAIL")

    missing_set = set(cmp.missing)
    extra_set = set(cmp.extra)
    check("node/2 (ref-only) reported missing", ("node", 2) in missing_set, str(cmp.missing))
    check("node/3 (local-only) reported extra", ("node", 3) in extra_set, str(cmp.extra))
    check("exactly one missing element", len(cmp.missing) == 1, str(cmp.missing))
    check("exactly one extra element", len(cmp.extra) == 1, str(cmp.extra))

    tag_mismatch_elements = {tm["element"] for tm in cmp.tag_mismatches}
    check("way/10 tag mismatch detected", "way/10" in tag_mismatch_elements, str(cmp.tag_mismatches))
    check("exactly one tag mismatch", len(cmp.tag_mismatches) == 1, str(cmp.tag_mismatches))
    check(
        "way/11 (nodes list mismatch) NOT reported as a tag mismatch",
        "way/11" not in tag_mismatch_elements,
    )

    other = "\n".join(cmp.other_mismatches)
    check("way/11 nodes-list mismatch detected", "way/11" in other and "nodes" in other, other)
    check("node/20 coordinate mismatch detected", "node/20" in other and "coordinates" in other, other)
    check("way/40 geometry mismatch detected", "way/40" in other and "geometry" in other, other)
    check(
        "node/21 (tiny coord diff, within 1e-7) NOT reported",
        "node/21" not in other,
        other,
    )
    check(
        "way/41 (tiny geometry diff, within 1e-6) NOT reported",
        "way/41" not in other,
        other,
    )
    check("relation/50 (identical) produces no mismatch", "relation/50" not in other and "relation/50" not in tag_mismatch_elements)
    check("node/1 (identical) produces no mismatch", "node/1" not in other and "node/1" not in tag_mismatch_elements)

    # A perfectly matching pair should PASS with nothing to report.
    identical_cmp = dt.compare(ref["elements"], ref["elements"])
    check("comparing a response against itself is PASS", identical_cmp.status == "PASS")
    check("no missing/extra when comparing against itself", not identical_cmp.missing and not identical_cmp.extra)

    # --- tolerance helpers ---
    check("coords_close: exactly equal", dt.coords_close(44.98, 44.98, dt.COORD_TOL))
    check("coords_close: within 1e-7 passes", dt.coords_close(44.98, 44.98 + 5e-8, dt.COORD_TOL))
    check("coords_close: beyond 1e-7 fails", not dt.coords_close(44.98, 44.98 + 5e-7, dt.COORD_TOL))
    check("coords_close: both None is close", dt.coords_close(None, None, dt.COORD_TOL))
    check("coords_close: one None is not close", not dt.coords_close(None, 1.0, dt.COORD_TOL))

    geom_a = [{"lat": 1.0, "lon": 2.0}, {"lat": 1.001, "lon": 2.001}]
    geom_b_close = [{"lat": 1.0, "lon": 2.0}, {"lat": 1.001 + 5e-7, "lon": 2.001}]
    geom_b_far = [{"lat": 1.0, "lon": 2.0}, {"lat": 1.001 + 5e-5, "lon": 2.001}]
    check("geometry_close: within 1e-6 passes", dt.geometry_close(geom_a, geom_b_close))
    check("geometry_close: beyond 1e-6 fails", not dt.geometry_close(geom_a, geom_b_far))
    check("geometry_close: different lengths fail", not dt.geometry_close(geom_a, geom_a[:1]))

    # --- `out count` comparison path ---
    count_match_a = [{"type": "count", "id": 0, "tags": {"nodes": "5", "ways": "2", "relations": "0", "total": "7"}}]
    count_match_b = [{"type": "count", "id": 0, "tags": {"nodes": "5", "ways": "2", "relations": "0", "total": "7"}}]
    count_diff_b = [{"type": "count", "id": 0, "tags": {"nodes": "5", "ways": "3", "relations": "0", "total": "8"}}]
    count_cmp_pass = dt.compare(count_match_a, count_match_b)
    count_cmp_fail = dt.compare(count_match_a, count_diff_b)
    check("out count: identical counts PASS", count_cmp_pass.status == "PASS")
    check("out count: differing counts FAIL", count_cmp_fail.status == "FAIL")
    check(
        "out count: FAIL carries both sides' counts",
        count_cmp_fail.count_mismatch == {"ref": count_match_a[0]["tags"], "local": count_diff_b[0]["tags"]},
        str(count_cmp_fail.count_mismatch),
    )

    # --- query text plumbing (substitution / [out:] forcing / [date:] insertion) ---
    bbox = [44.97, -93.28, 44.985, -93.255]
    q = '[out:xml][timeout:25];node["amenity"="cafe"]({{bbox}});out body;'
    substituted = dt.substitute_bbox(q, bbox)
    check("substitute_bbox removes the placeholder", "{{bbox}}" not in substituted)
    check("substitute_bbox inserts s,w,n,e", "44.97,-93.28,44.985,-93.255" in substituted)

    forced = dt.force_out_json(substituted)
    check("force_out_json turns [out:xml] into [out:json]", "[out:json]" in forced and "[out:xml]" not in forced)
    check("is_xml_variant still true on the original query", dt.is_xml_variant(q))

    dated = dt.prepend_date(forced, "2026-09-19T00:21:52Z")
    check('prepend_date inserts [date:"..."]', '[date:"2026-09-19T00:21:52Z"]' in dated)
    check("prepend_date keeps the settings block as a single prefix", dated.startswith("[out:json]"))

    no_settings_query = 'node["amenity"="cafe"]({{bbox}});out body;'
    dated_no_settings = dt.prepend_date(dt.substitute_bbox(no_settings_query, bbox), "2026-09-19T00:21:52Z")
    check(
        "prepend_date synthesizes a settings block when none exists",
        dated_no_settings.startswith('[out:json][date:"2026-09-19T00:21:52Z"];'),
        dated_no_settings,
    )

    comment_query = '// a static id lookup, no bbox needed\nnode(id:123);\nout body;'
    dated_comment = dt.prepend_date(comment_query, "2026-09-19T00:21:52Z")
    check(
        "prepend_date preserves a leading comment ahead of the settings block",
        dated_comment.startswith("// a static id lookup"),
        dated_comment,
    )
    check('prepend_date still inserts [date:"..."] after a leading comment', '[date:"2026-09-19T00:21:52Z"]' in dated_comment)

    # --- attic queries: --date must not be double-injected (m4-contracts.md section 7) ---
    check("should_apply_date: plain query gets --date", dt.should_apply_date('[out:json];node({{bbox}});out;'))
    check(
        "should_apply_date: (changed:a,b) still gets --date (corpus 43/55, unchanged from M0-M3)",
        dt.should_apply_date('[out:json];way({{bbox}})(changed:"a","b");out;'),
    )
    for attic_query in [
        '[out:json][date:"2026-09-19T06:00:00Z"];node({{bbox}});out;',
        '[out:json];retro("2026-09-19T06:00:00Z"){ node({{bbox}}); out; }',
        '[out:json];timeline(node,123);out;',
        '[out:xml][diff:"a","b"];node({{bbox}});out;',
        '[out:xml][adiff:"a","b"];node({{bbox}});out;',
    ]:
        check(f"should_apply_date: False for {attic_query[:40]!r}...", not dt.should_apply_date(attic_query))

    check("is_diff_variant: true for [diff:]", dt.is_diff_variant('[out:xml][diff:"a","b"];node(1);out;'))
    check("is_diff_variant: true for [adiff:]", dt.is_diff_variant('[out:xml][adiff:"a","b"];node(1);out;'))
    check("is_diff_variant: false otherwise", not dt.is_diff_variant('[out:json];node(1);out;'))

    forced_xml = dt.force_out_xml('[out:json][timeout:25];node(1);out;')
    check("force_out_xml turns [out:json] into [out:xml]", "[out:xml]" in forced_xml and "[out:json]" not in forced_xml)
    check(
        "force_out_xml leaves an already-xml query alone",
        dt.force_out_xml('[out:xml];node(1);out;') == '[out:xml];node(1);out;',
    )
    check(
        "force_out_xml synthesizes [out:xml] when no [out:] setting exists",
        dt.force_out_xml('node(1);out;').startswith("[out:xml]"),
    )

    # --- timeline comparison: sets of tag dicts, ignoring the synthetic id ---
    tl_ref = [
        {"type": "timeline", "id": 1, "tags": {"reftype": "node", "ref": "1", "refversion": "1", "created": "2020-01-01T00:00:00Z"}},
        {"type": "timeline", "id": 2, "tags": {"reftype": "node", "ref": "1", "refversion": "2", "created": "2021-01-01T00:00:00Z"}},
    ]
    tl_local_same_states_different_ids = [
        {"type": "timeline", "id": 7, "tags": {"reftype": "node", "ref": "1", "refversion": "2", "created": "2021-01-01T00:00:00Z"}},
        {"type": "timeline", "id": 9, "tags": {"reftype": "node", "ref": "1", "refversion": "1", "created": "2020-01-01T00:00:00Z"}},
    ]
    tl_local_missing_one = tl_local_same_states_different_ids[:1]
    tl_cmp_pass = dt.compare(tl_ref, tl_local_same_states_different_ids)
    tl_cmp_fail = dt.compare(tl_ref, tl_local_missing_one)
    check(
        "timeline: same states under different synthetic ids PASS",
        tl_cmp_pass.status == "PASS",
        str(tl_cmp_pass.to_dict()),
    )
    check("timeline: a missing state FAILs", tl_cmp_fail.status == "FAIL")
    check("compare() dispatches to compare_timeline when elements are type=timeline", dt.compare_timeline is not None)

    # --- diff/adiff XML action-list parsing and comparison ---
    diff_xml_ref = """<?xml version="1.0"?>
<osm version="0.6">
<action type="create">
  <node id="1" lat="1.0" lon="2.0" version="1"><tag k="amenity" v="cafe"/></node>
</action>
<action type="modify">
<old>
  <node id="2" lat="3.0" lon="4.0" version="1"><tag k="amenity" v="bar"/></node>
</old>
<new>
  <node id="2" lat="3.0" lon="4.0" version="2"><tag k="amenity" v="pub"/></node>
</new>
</action>
<action type="delete">
<old>
  <node id="3" lat="5.0" lon="6.0" version="1"><tag k="amenity" v="bank"/></node>
</old>
</action>
</osm>"""
    ref_actions = dt.parse_diff_actions(diff_xml_ref)
    check("parse_diff_actions: 3 actions found", len(ref_actions) == 3, str(ref_actions))
    by_action = {a["action"]: a for a in ref_actions}
    check("parse_diff_actions: create has no old, has new", by_action["create"]["old"] is None and by_action["create"]["new"] is not None)
    check(
        "parse_diff_actions: modify has both old and new with differing tags",
        by_action["modify"]["old"]["tags"] != by_action["modify"]["new"]["tags"],
    )
    check("parse_diff_actions: delete has old, no new", by_action["delete"]["old"] is not None and by_action["delete"]["new"] is None)

    # Local response identical to reference: PASS.
    cmp_identical = dt.compare_diff_actions(ref_actions, dt.parse_diff_actions(diff_xml_ref))
    check("compare_diff_actions: identical action lists PASS", cmp_identical.status == "PASS")

    # Local missing the delete action, and modify's new tags differ slightly.
    diff_xml_local = """<?xml version="1.0"?>
<osm version="0.6">
<action type="create">
  <node id="1" lat="1.0" lon="2.0" version="1"><tag k="amenity" v="cafe"/></node>
</action>
<action type="modify">
<old>
  <node id="2" lat="3.0" lon="4.0" version="1"><tag k="amenity" v="bar"/></node>
</old>
<new>
  <node id="2" lat="3.0" lon="4.0" version="2"><tag k="amenity" v="restaurant"/></node>
</new>
</action>
</osm>"""
    local_actions = dt.parse_diff_actions(diff_xml_local)
    cmp_diff = dt.compare_diff_actions(ref_actions, local_actions)
    check("compare_diff_actions: missing delete action detected", cmp_diff.status == "FAIL")
    check("compare_diff_actions: missing action reported", ("delete", "node", 3) in cmp_diff.missing, str(cmp_diff.missing))
    check("compare_diff_actions: modify tag mismatch detected", len(cmp_diff.tag_mismatches) == 1, str(cmp_diff.tag_mismatches))

    # A `delete` action whose <new> is a visible-but-unresolved stub (no
    # tags/nd children) must compare fine against an identical stub on the
    # other side, per the reference's recursion-drops-selection behavior
    # (docs/m4-contracts.md section 6.2 / probe adiff_recurse_xml).
    diff_xml_stub = """<?xml version="1.0"?>
<osm version="0.6">
<action type="delete">
<old>
  <way id="5" version="3"><nd ref="10"/><nd ref="11"/><tag k="building" v="yes"/></way>
</old>
<new>
  <way id="5" version="4"/>
</new>
</action>
</osm>"""
    stub_actions = dt.parse_diff_actions(diff_xml_stub)
    check("parse_diff_actions: delete with a <new> stub keeps it", stub_actions[0]["new"] is not None)
    check("parse_diff_actions: stub way has no nodes", stub_actions[0]["new"]["nodes"] is None)
    cmp_stub = dt.compare_diff_actions(stub_actions, dt.parse_diff_actions(diff_xml_stub))
    check("compare_diff_actions: identical stubs PASS", cmp_stub.status == "PASS", str(cmp_stub.to_dict()))

    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
