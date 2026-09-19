"""Unit tests for the evaluator subset (docs/m3-contracts.md section 5.2):
the tokenizer/parser (``osmpq.ql.evaluator``) and the SQL/Python compilers
(``osmpq.engine.evalsql``). No Engine/fixture here -- see
tests/test_engine_metafilters.py and tests/test_engine_control.py for the
end-to-end (planner/hook) behavior.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from osmpq.errors import ParseError, RuntimeQueryError, UnsupportedError  # noqa: E402
from osmpq.engine import evalsql  # noqa: E402
from osmpq.engine.schema import project  # noqa: E402
from osmpq.ql import evaluator as ev  # noqa: E402

# --------------------------------------------------------------------- parser


def test_parse_literals():
    assert ev.parse("42") == ev.Num(42.0)
    assert ev.parse('"hi"') == ev.Str("hi")
    assert ev.parse("'hi'") == ev.Str("hi")


def test_parse_tag_access():
    assert ev.parse('t["amenity"]') == ev.TagAccess("amenity")


def test_parse_element_calls_zero_and_one_arg():
    assert ev.parse("id()") == ev.ElementCall("id", [])
    assert ev.parse("is_closed()") == ev.ElementCall("is_closed", [])
    assert ev.parse('is_tag("amenity")') == ev.ElementCall("is_tag", [ev.Str("amenity")])
    assert ev.parse('count_by_role("outer")') == ev.ElementCall("count_by_role", [ev.Str("outer")])
    assert ev.parse('number(t["lanes"])') == ev.ElementCall("number", [ev.TagAccess("lanes")])


def test_parse_wrong_arity_is_parse_error():
    with pytest.raises(ParseError):
        ev.parse("id(1)")
    with pytest.raises(ParseError):
        ev.parse("is_tag()")


def test_parse_set_count_and_named_set_count():
    assert ev.parse("count(ways)") == ev.SetCount("ways")
    assert ev.parse("count(nwr)") == ev.SetCount("nwr")
    assert ev.parse(".a.count(nodes)") == ev.NamedSetCount("a", "nodes")


def test_parse_bad_type_keyword_is_parse_error():
    with pytest.raises(ParseError):
        ev.parse("count(bogus)")


def test_parse_aggregators():
    assert ev.parse('u(t["name"])') == ev.Aggregate("u", ev.TagAccess("name"))
    assert ev.parse("min(length())") == ev.Aggregate("min", ev.ElementCall("length", []))
    assert ev.parse("sum(number(id()))") == ev.Aggregate(
        "sum", ev.ElementCall("number", [ev.ElementCall("id", [])])
    )


def test_parse_precedence_and_associativity():
    # * binds tighter than +.
    got = ev.parse("1 + 2 * 3")
    assert got == ev.BinOp("+", ev.Num(1.0), ev.BinOp("*", ev.Num(2.0), ev.Num(3.0)))
    # comparisons bind tighter than && which binds tighter than ||.
    got2 = ev.parse("1 < 2 && 3 > 2 || 0")
    assert isinstance(got2, ev.BinOp) and got2.op == "||"
    assert got2.left.op == "&&"


def test_parse_ternary_is_right_associative():
    got = ev.parse('1 ? "a" : 0 ? "b" : "c"')
    assert got == ev.Ternary(ev.Num(1.0), ev.Str("a"), ev.Ternary(ev.Num(0.0), ev.Str("b"), ev.Str("c")))


def test_parse_unary_and_parens():
    assert ev.parse("-3") == ev.Unary("-", ev.Num(3.0))
    assert ev.parse('!t["a"]') == ev.Unary("!", ev.TagAccess("a"))
    assert ev.parse("(1 + 2) * 3") == ev.BinOp("*", ev.BinOp("+", ev.Num(1.0), ev.Num(2.0)), ev.Num(3.0))


def test_parse_lanes_example_from_corpus_48():
    got = ev.parse('t["lanes"] > 2')
    assert got == ev.BinOp(">", ev.TagAccess("lanes"), ev.Num(2.0))


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "1 +",
        "((1)",
        "1 2",
        "unknown_fn()",
        '"unterminated',
        "t[amenity]",  # t[...] requires a quoted string
        ".a.frobnicate(ways)",
    ],
)
def test_parse_errors(bad):
    with pytest.raises(ParseError):
        ev.parse(bad)


# ------------------------------------------------------- compile_element (SQL)


def _con():
    c = duckdb.connect()
    c.execute("INSTALL spatial; LOAD spatial;")
    return c


def _row_sql(**overrides) -> str:
    """A single canonical-row SELECT (schema.CANONICAL_COLUMNS), typed NULL
    for anything not overridden -- mirrors what `__q`/`set_<name>` rows
    look like at the point compile_element sees them."""
    return "SELECT " + project(overrides)


def _eval(con, expr_text: str, row_overrides: dict, promoted_keys=frozenset()):
    expr = ev.parse(expr_text)
    sql = evalsql.compile_element(expr, "e", set(promoted_keys))
    row = con.execute(f"SELECT {sql} FROM ({_row_sql(**row_overrides)}) e").fetchone()
    return row[0]


def test_missing_tag_reads_as_empty_string():
    con = _con()
    assert _eval(con, 't["nope"]', {"type": "'way'"}) == ""


def test_present_tag_reads_value():
    con = _con()
    assert _eval(con, 't["amenity"]', {"tags": "MAP {'amenity': 'cafe'}"}) == "cafe"


def test_is_tag_distinguishes_absent_from_empty_string():
    con = _con()
    assert _eval(con, 'is_tag("k")', {"tags": "MAP {'k': ''}"}) == "1"
    assert _eval(con, 'is_tag("k")', {"tags": "NULL::MAP(VARCHAR, VARCHAR)"}) == "0"


@pytest.mark.parametrize(
    "truthy_value,expected",
    [("", False), ("0", False), ("0.0", True), ("00", True), ("no", True), ("1", True)],
)
def test_truthiness_rule(truthy_value, expected):
    # "" and "0" are false; everything else -- including "0.0" -- is true.
    assert evalsql.truthy(truthy_value) is expected


def test_comparison_is_numeric_when_both_sides_parse_as_numbers():
    con = _con()
    # "10" > "9" numerically (true) but "10" < "9" lexically (true) --
    # this distinguishes the two rules.
    assert _eval(con, 't["lanes"] > "9"', {"tags": "MAP {'lanes': '10'}"}) == "1"


def test_comparison_is_lexical_when_either_side_is_not_numeric():
    con = _con()
    # "abc" doesn't parse as a number, so the whole comparison is lexical
    # (ordinary string ordering, not numeric).
    assert _eval(con, 't["surface"] < "abc"', {"tags": "MAP {'surface': 'aaa'}"}) == "1"
    assert _eval(con, 't["surface"] < "abc"', {"tags": "MAP {'surface': 'zzz'}"}) == "0"


def test_lanes_gt_2_numeric_true_and_false():
    con = _con()
    assert _eval(con, 't["lanes"] > 2', {"tags": "MAP {'lanes': '3'}"}) == "1"
    assert _eval(con, 't["lanes"] > 2', {"tags": "MAP {'lanes': '1'}"}) == "0"
    # Missing tag: "" > "2" is a lexical comparison (2 isn't a number issue
    # here -- "" fails to parse), and "" < "2" lexically, so > is false.
    assert _eval(con, 't["lanes"] > 2', {}) == "0"


def test_arithmetic():
    con = _con()
    assert _eval(con, "1 + 2 * 3", {}) == "7"
    assert _eval(con, "(1 + 2) * 3", {}) == "9"
    assert _eval(con, "7 / 2", {}) == "3.5"
    assert _eval(con, "4 / 2", {}) == "2"  # integral result formatted without ".0"


def test_division_by_zero_is_nan():
    con = _con()
    assert _eval(con, "1 / 0", {}) == "nan"


def test_logical_and_or_and_unary_not():
    con = _con()
    assert _eval(con, '"1" && "1"', {}) == "1"
    assert _eval(con, '"0" && "1"', {}) == "0"
    assert _eval(con, '"0" || "1"', {}) == "1"
    assert _eval(con, '!"0"', {}) == "1"
    assert _eval(con, '!"x"', {}) == "0"


def test_ternary_passes_through_branch_value_unboolified():
    con = _con()
    assert _eval(con, 't["surface"] ? t["surface"] : "unknown"', {"tags": "MAP {'surface': 'paved'}"}) == "paved"
    assert _eval(con, 't["surface"] ? t["surface"] : "unknown"', {}) == "unknown"


def test_number_and_is_number():
    con = _con()
    assert _eval(con, 'is_number("42")', {}) == "1"
    assert _eval(con, 'is_number("abc")', {}) == "0"
    assert _eval(con, 'number("42")', {}) == "42"
    assert _eval(con, 'number("abc")', {}) == "nan"


def test_id_type_version_meta_element_calls():
    con = _con()
    row = {
        "type": "'node'",
        "id": "7",
        "version": "3",
        "changeset": "555",
        "uid": "42",
        "user": "'alice'",
    }
    assert _eval(con, "id()", row) == "7"
    assert _eval(con, "type()", row) == "node"
    assert _eval(con, "version()", row) == "3"
    assert _eval(con, "changeset()", row) == "555"
    assert _eval(con, "uid()", row) == "42"
    assert _eval(con, "user()", row) == "alice"


def test_meta_calls_default_to_empty_string_when_null():
    con = _con()
    assert _eval(con, "version()", {"type": "'node'"}) == ""
    assert _eval(con, "user()", {"type": "'node'"}) == ""


def test_count_tags():
    con = _con()
    assert _eval(con, "count_tags()", {"tags": "MAP {'a': '1', 'b': '2'}"}) == "2"
    assert _eval(con, "count_tags()", {}) == "0"


def test_count_members_way_vs_relation_vs_node():
    con = _con()
    assert _eval(con, "count_members()", {"type": "'way'", "refs": "[1, 2, 3]::BIGINT[]"}) == "3"
    members = "[{'type': 'w', 'ref': 1, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]"
    assert _eval(con, "count_members()", {"type": "'relation'", "members": members}) == "1"
    assert _eval(con, "count_members()", {"type": "'node'"}) == "0"


def test_count_distinct_members():
    con = _con()
    assert _eval(con, "count_distinct_members()", {"type": "'way'", "refs": "[1, 1, 2]::BIGINT[]"}) == "2"


def test_count_by_role():
    con = _con()
    members = (
        "[{'type': 'w', 'ref': 1, 'role': 'outer'}, {'type': 'w', 'ref': 2, 'role': 'inner'}, "
        "{'type': 'n', 'ref': 3, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]"
    )
    assert _eval(con, 'count_by_role("outer")', {"type": "'relation'", "members": members}) == "2"
    assert _eval(con, 'count_by_role("inner")', {"type": "'relation'", "members": members}) == "1"
    assert _eval(con, 'count_by_role("nope")', {"type": "'relation'", "members": members}) == "0"


def test_is_closed():
    con = _con()
    assert _eval(con, "is_closed()", {"type": "'way'", "refs": "[1, 2, 3, 1]::BIGINT[]"}) == "1"
    assert _eval(con, "is_closed()", {"type": "'way'", "refs": "[1, 2, 3]::BIGINT[]"}) == "0"


def test_lat_lon():
    con = _con()
    row = {"type": "'node'", "lat_e7": "449778000", "lon_e7": "-932650000"}
    assert _eval(con, "lat()", row) == "44.9778"
    assert _eval(con, "lon()", row) == "-93.265"
    # lat()/lon() are only meaningful for nodes (contract 5.2); on any
    # other type this compiles to NaN like any other unparseable number,
    # not to a special-cased 0 (unlike length(), which the contract
    # explicitly pins to 0 for a NULL/way-less geometry).
    assert _eval(con, "lat()", {"type": "'way'"}) == "nan"


def test_length_null_geometry_is_zero():
    con = _con()
    assert _eval(con, "length()", {"type": "'way'", "geometry": "NULL::GEOMETRY"}) == "0"
    assert _eval(con, "length()", {"type": "'node'"}) == "0"


def test_length_axis_order_empirical():
    """docs/m3-contracts.md 5.2: length() must be in meters. DuckDB spatial
    1.5.5's ST_Length_Spheroid expects (lat, lon)-ordered points -- the
    OPPOSITE of the (lon, lat) WKT order this codebase stores geometry in
    everywhere else (see make_fixture.py, render.py). Feeding it our
    normal (lon, lat) order silently returns NaN; ST_FlipCoordinates first
    fixes it. Verified here against an independent haversine reference for
    a real Minnesota-latitude segment, to within 0.2% (the spheroid/sphere
    difference at this scale)."""
    con = _con()
    lat1, lon1 = 44.9778, -93.2650
    lat2, lon2 = 44.9850, -93.2700
    wkt_lonlat = f"LINESTRING({lon1} {lat1}, {lon2} {lat2})"

    # Standard (lon, lat) order -- what compile_element's length() must NOT
    # feed straight to ST_Length_Spheroid.
    raw = con.execute(f"SELECT ST_Length_Spheroid(ST_GeomFromText('{wkt_lonlat}'))").fetchone()[0]
    assert raw is None or math.isnan(raw)

    flipped = con.execute(
        f"SELECT ST_Length_Spheroid(ST_FlipCoordinates(ST_GeomFromText('{wkt_lonlat}')))"
    ).fetchone()[0]

    def haversine(lat1, lon1, lat2, lon2):
        r = 6371000.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlmb = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
        return 2 * r * math.asin(math.sqrt(a))

    ref = haversine(lat1, lon1, lat2, lon2)
    assert abs(flipped - ref) / ref < 0.002

    # compile_element's length() must reproduce this (via evalsql, on a
    # canonical way row carrying that same lon/lat geometry).
    got = _eval(con, "length()", {"type": "'way'", "geometry": f"ST_GeomFromText('{wkt_lonlat}')"})
    assert abs(float(got) - ref) / ref < 0.002


def test_compile_element_rejects_set_scoped_constructs():
    with pytest.raises(UnsupportedError):
        evalsql.compile_element(ev.parse("count(ways)"), "e", set())
    with pytest.raises(UnsupportedError):
        evalsql.compile_element(ev.parse(".a.count(ways)"), "e", set())
    with pytest.raises(UnsupportedError):
        evalsql.compile_element(ev.parse('u(t["name"])'), "e", set())


def test_compile_predicate_wraps_truthiness():
    con = _con()
    sql = evalsql.compile_predicate(ev.parse('t["amenity"]'), "e", set())
    with_tag = _row_sql(tags="MAP {'amenity': 'cafe'}")
    row = con.execute(f"SELECT {sql} FROM ({with_tag}) e").fetchone()
    assert row[0] is True
    without_tag = _row_sql()
    row2 = con.execute(f"SELECT {sql} FROM ({without_tag}) e").fetchone()
    assert row2[0] is False


# --------------------------------------------------------- evaluate_set (Python)


def _fake_ctx(con, promoted_keys=frozenset()):
    return SimpleNamespace(con=con, promoted_keys=set(promoted_keys))


def _materialize_set(con, name: str, rows: list[dict]):
    selects = [_row_sql(**r) for r in rows] or [_row_sql() + " WHERE FALSE"]
    con.execute(f"CREATE OR REPLACE TEMP TABLE set_{name} AS " + "\nUNION ALL\n".join(selects))


def test_evaluate_set_count():
    con = _con()
    _materialize_set(con, "_", [{"type": "'way'"}, {"type": "'way'"}, {"type": "'node'"}])
    ctx = _fake_ctx(con)
    assert evalsql.evaluate_set(ctx, ev.parse("count(ways)"), "_") == "2"
    assert evalsql.evaluate_set(ctx, ev.parse("count(nodes)"), "_") == "1"
    assert evalsql.evaluate_set(ctx, ev.parse("count(ways) > 1"), "_") == "1"


def test_evaluate_set_named_set_count_ignores_ambient_set_name():
    con = _con()
    _materialize_set(con, "_", [])
    _materialize_set(con, "a", [{"type": "'way'"}, {"type": "'way'"}])
    ctx = _fake_ctx(con)
    assert evalsql.evaluate_set(ctx, ev.parse(".a.count(ways)"), "_") == "2"


def test_evaluate_set_missing_set_is_runtime_error():
    con = _con()
    ctx = _fake_ctx(con)
    with pytest.raises(RuntimeQueryError):
        evalsql.evaluate_set(ctx, ev.parse("count(ways)"), "nope")


def test_evaluate_set_aggregators():
    con = _con()
    rows = [
        {"type": "'way'", "tags": "MAP {'lanes': '2'}"},
        {"type": "'way'", "tags": "MAP {'lanes': '4'}"},
        {"type": "'way'", "tags": "MAP {}"},  # non-numeric ("") ignored by min/max/sum
    ]
    _materialize_set(con, "_", rows)
    ctx = _fake_ctx(con)
    # Aggregators cast their argument to a number themselves (via
    # try_cast), so passing the bare tag -- not number(t["lanes"]), which
    # would pre-format the missing tag to the literal string "nan" and
    # defeat the "ignore non-numeric rows" behavior -- is the idiom.
    lanes = 't["lanes"]'
    assert evalsql.evaluate_set(ctx, ev.parse(f"min({lanes})"), "_") == "2"
    assert evalsql.evaluate_set(ctx, ev.parse(f"max({lanes})"), "_") == "4"
    assert evalsql.evaluate_set(ctx, ev.parse(f"sum({lanes})"), "_") == "6"


def test_evaluate_set_u_unique_vs_not():
    con = _con()
    _materialize_set(con, "_", [{"type": "'way'"}, {"type": "'way'"}])
    ctx = _fake_ctx(con)
    assert evalsql.evaluate_set(ctx, ev.parse("u(type())"), "_") == "way"

    _materialize_set(con, "_", [{"type": "'way'"}, {"type": "'node'"}])
    assert evalsql.evaluate_set(ctx, ev.parse("u(type())"), "_") == ""


def test_evaluate_set_set_aggregator_joins_distinct_values():
    con = _con()
    rows = [
        {"tags": "MAP {'highway': 'primary'}"},
        {"tags": "MAP {'highway': 'secondary'}"},
        {"tags": "MAP {'highway': 'primary'}"},
    ]
    _materialize_set(con, "_", rows)
    ctx = _fake_ctx(con)
    assert evalsql.evaluate_set(ctx, ev.parse('set(t["highway"])'), "_") == "primary, secondary"


def test_evaluate_set_bare_element_function_is_runtime_error():
    con = _con()
    _materialize_set(con, "_", [{"type": "'way'"}])
    ctx = _fake_ctx(con)
    with pytest.raises(RuntimeQueryError):
        evalsql.evaluate_set(ctx, ev.parse('t["lanes"] == "2"'), "_")
    with pytest.raises(RuntimeQueryError):
        evalsql.evaluate_set(ctx, ev.parse("count(ways) > 0 && is_closed()"), "_")
