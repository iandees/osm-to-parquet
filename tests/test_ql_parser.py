"""Tests for the hand-written Overpass QL lexer/parser (osmpq.ql).

Covers the tier-1 language subset from docs/m0-contracts.md section 8, plus
constructs the parser must swallow syntactically even though the M0 planner
rejects them at run time (area/around/poly/pivot/newer/changed/user/uid/if,
is_in, map_to_area, foreach, if/else, and the for/complete/retro/compare/
make/convert/timeline/local statements captured as Unsupported).
"""
from __future__ import annotations

import pytest

from osmpq.errors import ParseError
from osmpq.ql import parse
from osmpq.ql.ast import (
    AreaFilter,
    AroundFilter,
    BboxFilter,
    ChangedFilter,
    Difference,
    Foreach,
    If,
    IdFilter,
    IfFilter,
    IsIn,
    Item,
    MapToArea,
    NewerFilter,
    Out,
    PivotFilter,
    PolyFilter,
    Query,
    Recurse,
    RecurseFilter,
    Retro,
    TagFilter,
    Timeline,
    UidFilter,
    Union,
    Unsupported,
    UserFilter,
)


def parse_one(text):
    """Parse and return the single top-level statement (settings ignored)."""
    prog = parse(text)
    assert len(prog.statements) == 1, prog.statements
    return prog.statements[0]


# ---------------------------------------------------------------------------
# Settings block
# ---------------------------------------------------------------------------


def test_settings_out_json_timeout():
    prog = parse('[out:json][timeout:25];node(1);out;')
    assert prog.settings.out_format == "json"
    assert prog.settings.timeout == 25


def test_settings_out_xml_default_when_absent():
    prog = parse("node(1);out;")
    assert prog.settings.out_format == "xml"
    assert prog.settings.timeout == 180


def test_settings_maxsize():
    prog = parse("[maxsize:1073741824];node(1);out;")
    assert prog.settings.maxsize == 1073741824


def test_settings_bbox():
    prog = parse("[bbox:44.9,-93.3,45.0,-93.2];node[amenity=cafe];out;")
    assert prog.settings.bbox == (44.9, -93.3, 45.0, -93.2)


def test_settings_date():
    prog = parse('[date:"2020-01-01T00:00:00Z"];node(1);out;')
    assert prog.settings.date == "2020-01-01T00:00:00Z"


def test_settings_diff_one_arg():
    prog = parse('[diff:"a"];node(1);out;')
    assert prog.settings.diff == ("a", None)


def test_settings_diff_two_args():
    prog = parse('[diff:"a","b"];node(1);out;')
    assert prog.settings.diff == ("a", "b")


def test_settings_adiff_two_args():
    prog = parse('[adiff:"a","b"];node(1);out;')
    assert prog.settings.adiff == ("a", "b")


def test_settings_any_order():
    prog = parse("[timeout:90][out:json][maxsize:100];node(1);out;")
    assert prog.settings.timeout == 90
    assert prog.settings.out_format == "json"
    assert prog.settings.maxsize == 100


def test_settings_csv_out():
    prog = parse('[out:csv(name, "addr:street", ::id, ::lat; true; ",")];node(1);out;')
    assert prog.settings.out_format == "csv"
    assert prog.settings.csv_fields == ["name", "addr:street", "::id", "::lat"]
    assert prog.settings.csv_header is True
    assert prog.settings.csv_separator == ","


def test_settings_csv_out_minimal_fields_only():
    prog = parse("[out:csv(::id,::type)];node(1);out;")
    assert prog.settings.csv_fields == ["::id", "::type"]
    # header/separator keep their dataclass defaults when omitted
    assert prog.settings.csv_header is True


def test_no_settings_block_is_optional():
    prog = parse("node(1);out;")
    assert prog.settings.bbox is None


# ---------------------------------------------------------------------------
# Comments
# ---------------------------------------------------------------------------


def test_line_comment():
    prog = parse("// hi\nnode(1);\nout; // trailing\n")
    assert len(prog.statements) == 2


def test_block_comment_multiline():
    prog = parse("node(1); /* a\nb\nc */ out;")
    assert len(prog.statements) == 2


def test_comment_in_odd_place_inside_filter_list():
    q = parse_one('node["amenity"=/*x*/"cafe"];')
    assert isinstance(q, Query)
    assert q.filters[0].value == "cafe"


def test_comment_between_settings_and_semicolon():
    prog = parse("[out:json] // fmt\n;node(1);out;")
    assert prog.settings.out_format == "json"


# ---------------------------------------------------------------------------
# Query statement basics: types, input/output sets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "keyword,expected",
    [
        ("node", ["node"]),
        ("way", ["way"]),
        ("rel", ["relation"]),
        ("relation", ["relation"]),
        ("nwr", ["node", "way", "relation"]),
        ("nw", ["node", "way"]),
        ("nr", ["node", "relation"]),
        ("wr", ["way", "relation"]),
    ],
)
def test_query_type_expansion(keyword, expected):
    q = parse_one(f"{keyword}(1);")
    assert isinstance(q, Query)
    assert q.types == expected


def test_area_query_type():
    q = parse_one('area[name="Minneapolis"]->.a;')
    assert q.types == ["area"]
    assert q.output_set == "a"


def test_query_input_sets_single():
    q = parse_one("node.a(around:10);")
    assert q.input_sets == ["a"]


def test_query_input_sets_multiple_is_intersection():
    q = parse_one("node.a.b;")
    assert q.input_sets == ["a", "b"]


def test_query_output_set_arrow():
    q = parse_one("node(1)->.foo;")
    assert q.output_set == "foo"


def test_query_default_output_set_is_underscore():
    q = parse_one("node(1);")
    assert q.output_set == "_"


# ---------------------------------------------------------------------------
# Tag filters
# ---------------------------------------------------------------------------


def test_tag_filter_exists():
    q = parse_one("node[amenity];")
    f = q.filters[0]
    assert isinstance(f, TagFilter) and f.key == "amenity" and f.op == "exists"


def test_tag_filter_not_exists():
    q = parse_one("node[!amenity];")
    f = q.filters[0]
    assert f.key == "amenity" and f.op == "not_exists"


def test_tag_filter_equals_quoted():
    q = parse_one('node["amenity"="cafe"];')
    f = q.filters[0]
    assert f.key == "amenity" and f.op == "=" and f.value == "cafe"


def test_tag_filter_equals_bare():
    q = parse_one("node[amenity=cafe];")
    f = q.filters[0]
    assert f.key == "amenity" and f.op == "=" and f.value == "cafe"


def test_tag_filter_bare_key_with_colon():
    q = parse_one("node[addr:street];")
    f = q.filters[0]
    assert f.key == "addr:street" and f.op == "exists"


def test_tag_filter_not_equals():
    q = parse_one('node["amenity"!="cafe"];')
    f = q.filters[0]
    assert f.op == "!="


def test_tag_filter_regex_value():
    q = parse_one('node["name"~"^Cafe"];')
    f = q.filters[0]
    assert f.op == "~" and f.value == "^Cafe" and not f.key_is_regex


def test_tag_filter_regex_not_match():
    q = parse_one('node["name"!~"^Cafe"];')
    f = q.filters[0]
    assert f.op == "!~"


def test_tag_filter_key_regex():
    q = parse_one('node[~"^addr:"~"."];')
    f = q.filters[0]
    assert f.key_is_regex is True
    assert f.key == "^addr:" and f.value == "."


def test_tag_filter_case_insensitive_flag():
    q = parse_one('node["name"~"cafe",i];')
    f = q.filters[0]
    assert f.case_insensitive is True
    assert f.value == "cafe"


def test_tag_filter_key_regex_case_insensitive():
    q = parse_one('node[~"name"~"cafe",i];')
    f = q.filters[0]
    assert f.case_insensitive is True and f.key_is_regex is True


def test_tag_filter_regex_value_looking_like_operators_stays_atomic():
    # The value contains a literal '~' inside quotes; must not confuse the lexer.
    q = parse_one('node["note"="~not a regex~"];')
    f = q.filters[0]
    assert f.op == "=" and f.value == "~not a regex~"


def test_tag_filter_single_quoted_strings():
    q = parse_one("node['amenity'='cafe'];")
    f = q.filters[0]
    assert f.key == "amenity" and f.value == "cafe"


def test_string_escapes():
    q = parse_one(r'node["name"="line1\nline2\ttab\\slash\"quote"];')
    f = q.filters[0]
    assert f.value == 'line1\nline2\ttab\\slash"quote'


def test_string_unicode_escape():
    q = parse_one(r'node["name"="ሴ"];')
    f = q.filters[0]
    assert f.value == "ሴ"
    assert f.value == "ሴ"


def test_multiple_tag_filters_on_one_query():
    q = parse_one('node["amenity"="cafe"]["name"];')
    assert len(q.filters) == 2
    assert q.filters[0].key == "amenity"
    assert q.filters[1].key == "name" and q.filters[1].op == "exists"


# ---------------------------------------------------------------------------
# Parenthesized filters
# ---------------------------------------------------------------------------


def test_bbox_filter_positional():
    q = parse_one("node(44.9,-93.3,45.0,-93.2);")
    f = q.filters[0]
    assert isinstance(f, BboxFilter)
    assert (f.south, f.west, f.north, f.east) == (44.9, -93.3, 45.0, -93.2)


def test_id_filter_single_number():
    q = parse_one("node(123);")
    f = q.filters[0]
    assert isinstance(f, IdFilter)
    assert f.ids == [123]


def test_id_filter_explicit_keyword():
    q = parse_one("way(id:1,2,3);")
    f = q.filters[0]
    assert isinstance(f, IdFilter)
    assert f.ids == [1, 2, 3]


@pytest.mark.parametrize("kind", ["w", "r", "bn", "bw", "br"])
def test_recurse_filter_bare(kind):
    q = parse_one(f"node({kind});")
    f = q.filters[0]
    assert isinstance(f, RecurseFilter)
    assert f.kind == kind
    assert f.set_name == "_"
    assert f.role is None


def test_recurse_filter_with_set():
    q = parse_one("way(w.a);")
    f = q.filters[0]
    assert f.kind == "w" and f.set_name == "a"


def test_recurse_filter_with_role_only():
    q = parse_one('node(r:"stop");')
    f = q.filters[0]
    assert f.kind == "r" and f.role == "stop" and f.set_name == "_"


def test_recurse_filter_with_set_and_role():
    q = parse_one('node(r.bus_stops:"stop");')
    f = q.filters[0]
    assert f.kind == "r" and f.set_name == "bus_stops" and f.role == "stop"


def test_recurse_filter_bw_with_set_and_role():
    q = parse_one('rel(bw.x:"outer");')
    f = q.filters[0]
    assert isinstance(f, RecurseFilter)
    assert f.kind == "bw" and f.set_name == "x" and f.role == "outer"


def test_around_filter_radius_only():
    q = parse_one("node(around:100);")
    f = q.filters[0]
    assert isinstance(f, AroundFilter)
    assert f.radius == 100
    assert f.set_name is None
    assert f.coords is None


def test_around_filter_with_set():
    q = parse_one("node(around.a:100);")
    f = q.filters[0]
    assert f.set_name == "a" and f.radius == 100


def test_around_filter_with_coords():
    q = parse_one("nwr[shop](around:500,44.97,-93.27);")
    f = q.filters[1]
    assert isinstance(f, AroundFilter)
    assert f.radius == 500
    assert f.coords == [(44.97, -93.27)]


def test_around_filter_with_multiple_coords():
    q = parse_one("node(around:50,1.0,2.0,3.0,-4.0);")
    f = q.filters[0]
    assert f.coords == [(1.0, 2.0), (3.0, -4.0)]


def test_poly_filter():
    q = parse_one('node(poly:"1.0 2.0 3.0 4.0 5.0 6.0");')
    f = q.filters[0]
    assert isinstance(f, PolyFilter)
    assert f.coords == [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]


def test_area_filter_bare():
    q = parse_one("nwr[amenity=cafe](area.a);")
    f = q.filters[1]
    assert isinstance(f, AreaFilter)
    assert f.set_name == "a" and f.area_id is None


def test_area_filter_default_set():
    q = parse_one("node(area);")
    f = q.filters[0]
    assert f.set_name is None and f.area_id is None


def test_area_filter_by_id():
    q = parse_one("node(area:3600123);")
    f = q.filters[0]
    assert f.area_id == 3600123


def test_pivot_filter_default():
    q = parse_one("way(pivot);")
    f = q.filters[0]
    assert isinstance(f, PivotFilter)
    assert f.set_name == "_"


def test_pivot_filter_with_set():
    q = parse_one("way(pivot.a);")
    f = q.filters[0]
    assert f.set_name == "a"


def test_newer_filter():
    q = parse_one('node(newer:"2020-01-01T00:00:00Z");')
    f = q.filters[0]
    assert isinstance(f, NewerFilter)
    assert f.timestamp == "2020-01-01T00:00:00Z"


def test_changed_filter_one_arg():
    q = parse_one('node(changed:"a");')
    f = q.filters[0]
    assert isinstance(f, ChangedFilter)
    assert f.since == "a" and f.until is None


def test_changed_filter_two_args():
    q = parse_one('node(changed:"a","b");')
    f = q.filters[0]
    assert f.since == "a" and f.until == "b"


def test_user_filter_one_name():
    q = parse_one('node(user:"Steve");')
    f = q.filters[0]
    assert isinstance(f, UserFilter)
    assert f.names == ["Steve"]


def test_user_filter_two_names():
    q = parse_one('node(user:"a","b");')
    f = q.filters[0]
    assert f.names == ["a", "b"]


def test_uid_filter_one():
    q = parse_one("node(uid:1);")
    f = q.filters[0]
    assert isinstance(f, UidFilter)
    assert f.uids == [1]


def test_uid_filter_two():
    q = parse_one("node(uid:1,2);")
    f = q.filters[0]
    assert f.uids == [1, 2]


def test_if_filter_captures_expression_source():
    q = parse_one('nwr(if: t["name"] == "x");')
    f = q.filters[0]
    assert isinstance(f, IfFilter)
    assert f.expression == 't["name"] == "x"'


def test_if_filter_with_nested_parens():
    q = parse_one("node(if: count_tags() > 0);")
    f = q.filters[0]
    assert f.expression == "count_tags() > 0"


def test_filters_chained_type_then_tag_then_paren():
    q = parse_one('way["highway"](44.9,-93.3,45.0,-93.2)(newer:"2020-01-01T00:00:00Z");')
    assert len(q.filters) == 3
    assert isinstance(q.filters[0], TagFilter)
    assert isinstance(q.filters[1], BboxFilter)
    assert isinstance(q.filters[2], NewerFilter)


# ---------------------------------------------------------------------------
# Union / difference blocks
# ---------------------------------------------------------------------------


def test_union_block():
    stmt = parse_one("(node(1);way(2);relation(3););")
    assert isinstance(stmt, Union)
    assert len(stmt.statements) == 3
    assert stmt.statements[0].types == ["node"]
    assert stmt.statements[1].types == ["way"]
    assert stmt.statements[2].types == ["relation"]


def test_union_block_with_output_set():
    stmt = parse_one("(node(1);way(2);)->.merged;")
    assert isinstance(stmt, Union)
    assert stmt.output_set == "merged"


def test_difference_block():
    stmt = parse_one("(way[highway];-way[highway=service];);")
    assert isinstance(stmt, Difference)
    assert isinstance(stmt.first, Query)
    assert isinstance(stmt.second, Query)
    assert stmt.first.filters[0].key == "highway" and stmt.first.filters[0].op == "exists"
    assert stmt.second.filters[0].value == "service"


def test_difference_block_too_many_dashes_is_error():
    with pytest.raises(ParseError):
        parse("(way(1);-way(2);-way(3););")


def test_wizard_shape_union_and_out():
    q = """
    [out:json][timeout:25];
    // gather results
    (
      node["amenity"="cafe"](44.9,-93.3,45.0,-93.2);
      way["amenity"="cafe"](44.9,-93.3,45.0,-93.2);
      relation["amenity"="cafe"](44.9,-93.3,45.0,-93.2);
    );
    // print results
    out body;
    >;
    out skel qt;
    """
    prog = parse(q)
    assert prog.settings.out_format == "json"
    assert prog.settings.timeout == 25
    union, out1, recurse, out2 = prog.statements
    assert isinstance(union, Union) and len(union.statements) == 3
    assert isinstance(out1, Out) and out1.verbosity == "body"
    assert isinstance(recurse, Recurse) and recurse.kind == ">"
    assert isinstance(out2, Out) and out2.verbosity == "skel" and out2.order == "qt"


def test_josm_shape_with_recurse_up_and_down():
    q = '[out:xml][timeout:90][bbox:44.9,-93.3,45.0,-93.2];(node(123);<;>;);out meta;'
    prog = parse(q)
    assert prog.settings.out_format == "xml"
    assert prog.settings.timeout == 90
    assert prog.settings.bbox == (44.9, -93.3, 45.0, -93.2)
    union, out = prog.statements
    assert isinstance(union, Union)
    node_q, down, up = union.statements
    assert isinstance(node_q, Query)
    assert isinstance(down, Recurse) and down.kind == "<"
    assert isinstance(up, Recurse) and up.kind == ">"
    assert isinstance(out, Out) and out.verbosity == "meta"


# ---------------------------------------------------------------------------
# Recurse operators / item / is_in / map_to_area
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", [">", ">>", "<", "<<"])
def test_bare_recurse_operator(op):
    stmt = parse_one(f"{op};")
    assert isinstance(stmt, Recurse)
    assert stmt.kind == op
    assert stmt.input_set == "_" and stmt.output_set == "_"


def test_recurse_operator_with_input_and_output_set():
    stmt = parse_one(".a > -> .b;")
    assert isinstance(stmt, Recurse)
    assert stmt.kind == ">" and stmt.input_set == "a" and stmt.output_set == "b"


def test_item_bare_set():
    stmt = parse_one(".a;")
    assert isinstance(stmt, Item)
    assert stmt.input_set == "a" and stmt.output_set == "_"


def test_item_with_output_set():
    stmt = parse_one(".a -> .b;")
    assert isinstance(stmt, Item)
    assert stmt.input_set == "a" and stmt.output_set == "b"


def test_item_with_output_set_no_spaces():
    stmt = parse_one(".a->.b;")
    assert isinstance(stmt, Item)
    assert stmt.input_set == "a" and stmt.output_set == "b"


def test_is_in_bare():
    stmt = parse_one("is_in;")
    assert isinstance(stmt, IsIn)
    assert stmt.input_set == "_" and stmt.coords is None


def test_is_in_with_coords():
    stmt = parse_one("is_in(44.9,-93.3);")
    assert isinstance(stmt, IsIn)
    assert stmt.coords == (44.9, -93.3)


def test_is_in_with_input_and_output_set():
    stmt = parse_one(".a is_in->.b;")
    assert isinstance(stmt, IsIn)
    assert stmt.input_set == "a" and stmt.output_set == "b"


def test_map_to_area_bare():
    stmt = parse_one("map_to_area;")
    assert isinstance(stmt, MapToArea)
    assert stmt.input_set == "_" and stmt.output_set == "_"


def test_map_to_area_with_sets():
    stmt = parse_one(".a map_to_area->.b;")
    assert isinstance(stmt, MapToArea)
    assert stmt.input_set == "a" and stmt.output_set == "b"


# ---------------------------------------------------------------------------
# out statement
# ---------------------------------------------------------------------------


def test_out_default_is_body():
    stmt = parse_one("out;")
    assert isinstance(stmt, Out)
    assert stmt.verbosity == "body"
    assert stmt.geometry == "none"
    assert stmt.order == "asc"
    assert stmt.limit is None
    assert not stmt.noids
    assert not stmt.count


@pytest.mark.parametrize("word", ["ids", "skel", "body", "tags", "meta"])
def test_out_verbosity_options(word):
    stmt = parse_one(f"out {word};")
    assert stmt.verbosity == word


def test_out_ids_count_and_geom_combined_any_order():
    stmt = parse_one("out meta geom qt 50;")
    assert stmt.verbosity == "meta"
    assert stmt.geometry == "geom"
    assert stmt.order == "qt"
    assert stmt.limit == 50


def test_out_noids():
    stmt = parse_one("out noids;")
    assert stmt.noids is True


def test_out_bb():
    stmt = parse_one("out bb;")
    assert stmt.geometry == "bb"


def test_out_center():
    stmt = parse_one("out center;")
    assert stmt.geometry == "center"


def test_out_count():
    stmt = parse_one("out count;")
    assert stmt.count is True


def test_out_limit_number():
    stmt = parse_one("out 10;")
    assert stmt.limit == 10


def test_out_geom_with_bbox():
    stmt = parse_one("out geom(1,2,3,4);")
    assert stmt.geometry == "geom"
    assert stmt.geom_bbox == (1.0, 2.0, 3.0, 4.0)


def test_out_with_input_set_prefix():
    stmt = parse_one(".a out;")
    assert stmt.input_set == "a"


def test_out_with_input_set_prefix_and_modifiers():
    stmt = parse_one(".foo out skel qt;")
    assert stmt.input_set == "foo"
    assert stmt.verbosity == "skel"
    assert stmt.order == "qt"


# ---------------------------------------------------------------------------
# foreach / if-else
# ---------------------------------------------------------------------------


def test_foreach_bare():
    stmt = parse_one("foreach { out; }")
    assert isinstance(stmt, Foreach)
    assert stmt.input_set == "_" and stmt.output_set == "_"
    assert len(stmt.body) == 1 and isinstance(stmt.body[0], Out)


def test_foreach_with_sets():
    stmt = parse_one("foreach.a->.b { .b out; }")
    assert isinstance(stmt, Foreach)
    assert stmt.input_set == "a" and stmt.output_set == "b"
    assert len(stmt.body) == 1
    assert isinstance(stmt.body[0], Out)
    assert stmt.body[0].input_set == "b"


def test_if_statement_without_else():
    stmt = parse_one("if (count(nodes) > 0) { out; }")
    assert isinstance(stmt, If)
    assert stmt.condition == "count(nodes) > 0"
    assert len(stmt.then) == 1 and isinstance(stmt.then[0], Out)
    assert stmt.otherwise == []


def test_if_statement_with_else():
    stmt = parse_one("if (1 == 1) { node(1); } else { way(2); }")
    assert isinstance(stmt, If)
    assert len(stmt.then) == 1 and stmt.then[0].types == ["node"]
    assert len(stmt.otherwise) == 1 and stmt.otherwise[0].types == ["way"]


# ---------------------------------------------------------------------------
# Unsupported statements (parsed but planner-rejected)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("keyword", ["for", "complete", "compare"])
def test_block_keywords_captured_as_unsupported(keyword):
    stmt = parse_one(f"{keyword}(t) {{ out; }}")
    assert isinstance(stmt, Unsupported)
    assert stmt.keyword == keyword
    assert stmt.source.startswith(keyword)
    assert "out;" in stmt.source


def test_retro_is_a_real_parsed_statement_not_unsupported():
    # docs/m4-contracts.md section 3.2: M4 gives `retro` real AST support
    # (tests/test_ql_parser.py's own retro/timeline tests -- see
    # `test_engine_control`/`test_attic_*` for behavior); it's no longer
    # a placeholder `Unsupported` node like `for`/`complete`/`compare`.
    stmt = parse_one('retro("2020-01-01T00:00:00Z") { out; }')
    assert isinstance(stmt, Retro)
    assert stmt.time_expr == '"2020-01-01T00:00:00Z"'
    assert len(stmt.body) == 1


@pytest.mark.parametrize("keyword", ["make", "convert", "local"])
def test_simple_keywords_captured_as_unsupported(keyword):
    stmt = parse_one(f"{keyword} point = geom;")
    assert isinstance(stmt, Unsupported)
    assert stmt.keyword == keyword
    assert stmt.source.startswith(keyword)
    assert stmt.source.endswith(";")


def test_timeline_is_a_real_parsed_statement_not_unsupported():
    # docs/m4-contracts.md section 3.2: same story as `retro` above.
    stmt = parse_one("timeline(way,23125943,4)->.t;")
    assert isinstance(stmt, Timeline)
    assert stmt.element_type == "way"
    assert stmt.element_id == 23125943
    assert stmt.version == 4
    assert stmt.output_set == "t"


def test_unsupported_simple_does_not_split_on_semicolon_inside_parens():
    stmt = parse_one('make point geometry=geom(id:1;2);')
    assert isinstance(stmt, Unsupported)
    assert stmt.keyword == "make"
    assert stmt.source == 'make point geometry=geom(id:1;2);'


def test_unsupported_block_after_capture_parsing_continues():
    prog = parse("for (x) { out; } out count;")
    assert len(prog.statements) == 2
    assert isinstance(prog.statements[0], Unsupported)
    assert isinstance(prog.statements[1], Out)
    assert prog.statements[1].count is True


# ---------------------------------------------------------------------------
# Larger, realistic queries
# ---------------------------------------------------------------------------


def test_area_named_then_nwr_center():
    prog = parse('area[name="Minneapolis"]->.a; nwr[amenity=cafe](area.a); out center;')
    area_q, nwr_q, out = prog.statements
    assert area_q.types == ["area"] and area_q.output_set == "a"
    assert nwr_q.types == ["node", "way", "relation"]
    assert isinstance(nwr_q.filters[0], TagFilter)
    assert isinstance(nwr_q.filters[1], AreaFilter) and nwr_q.filters[1].set_name == "a"
    assert out.geometry == "center"


def test_way_id_list_out_geom():
    prog = parse("way(id:1,2,3); out geom;")
    way_q, out = prog.statements
    assert way_q.filters[0].ids == [1, 2, 3]
    assert out.geometry == "geom"


def test_node_by_id_bare_out():
    prog = parse("node(123); out;")
    node_q, out = prog.statements
    assert node_q.filters[0].ids == [123]
    assert isinstance(out, Out)


def test_difference_then_out_ids():
    prog = parse("( way[highway]; - way[highway=service]; ); out ids;")
    diff, out = prog.statements
    assert isinstance(diff, Difference)
    assert out.verbosity == "ids"


def test_full_program_with_everything_mixed():
    q = """
    [out:json][timeout:30][maxsize:100000000];
    (
      node["shop"="bakery"](45.0,-93.3,45.1,-93.2);
      way(around:200,45.05,-93.25);
    )->.all;
    .all out meta;
    """
    prog = parse(q)
    assert prog.settings.timeout == 30
    union, item_out = prog.statements
    assert union.output_set == "all"
    assert item_out.input_set == "all"
    assert item_out.verbosity == "meta"


# ---------------------------------------------------------------------------
# Error cases: line numbers, unbalanced brackets, unknown filters
# ---------------------------------------------------------------------------


def test_error_missing_semicolon():
    with pytest.raises(ParseError) as exc:
        parse("node(1)\nout;")
    assert "line 2" in str(exc.value)


def test_error_unbalanced_paren_in_filter():
    with pytest.raises(ParseError):
        parse("node(1;")


def test_error_unbalanced_bracket():
    with pytest.raises(ParseError):
        parse("node[amenity=cafe;")


def test_error_unclosed_union_block():
    with pytest.raises(ParseError):
        parse("(node(1);")


def test_error_unknown_filter_keyword():
    with pytest.raises(ParseError) as exc:
        parse("node(bogus:1);")
    assert "unknown filter" in str(exc.value)


def test_error_unknown_statement_keyword():
    with pytest.raises(ParseError) as exc:
        parse("frobnicate(1);")
    assert "unknown statement" in str(exc.value)


def test_error_reports_correct_line_number():
    q = "node(1);\nway(2);\nbadkeyword(3);\n"
    with pytest.raises(ParseError) as exc:
        parse(q)
    assert exc.value.line == 3


def test_error_unterminated_string():
    with pytest.raises(ParseError):
        parse('node["name"="unterminated];')


def test_error_missing_operator_in_tag_filter():
    with pytest.raises(ParseError):
        parse('node["amenity" "cafe"];')


def test_error_message_style_matches_overpass():
    with pytest.raises(ParseError) as exc:
        parse("frobnicate(1);")
    msg = str(exc.value)
    assert msg.startswith("line 1: parse error:")
