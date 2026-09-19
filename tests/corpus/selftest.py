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

    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
