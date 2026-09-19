"""Hand-written recursive-descent parser for Overpass QL.

Turns source text into an ``osmpq.ql.ast.Program``. See ``osmpq.ql.lexer``
for tokenization details (in particular how signed numbers and permissive
bare words are handled).
"""
from __future__ import annotations

from typing import Optional

from osmpq.errors import ParseError

from . import ast
from .lexer import (
    ARROW,
    BANG,
    COLON,
    COMMA,
    DOT,
    EOF,
    EQ,
    GT,
    GTGT,
    IDENT,
    LBRACE,
    LBRACKET,
    LPAREN,
    LT,
    LTLT,
    MINUS,
    NEQ,
    NOTTILDE,
    NUMBER,
    RBRACE,
    RBRACKET,
    RPAREN,
    SEMI,
    STRING,
    TILDE,
    Token,
    tokenize,
)

# Statement keywords that introduce a node/way/relation(-set) query.
_QUERY_KEYWORDS = {
    "node": ("node",),
    "way": ("way",),
    "rel": ("relation",),
    "relation": ("relation",),
    "nwr": ("node", "way", "relation"),
    "nw": ("node", "way"),
    "nr": ("node", "relation"),
    "wr": ("way", "relation"),
    "area": ("area",),
    # Not covered by ast.py's documented conventions (only nwr/nw/nr/wr/rel
    # are said to expand); represented as a single synthetic "derived" type
    # since Query.types is a plain list[str]. See the parser report.
    "derived": ("derived",),
}

_RECURSE_KIND_BY_TOKEN = {GT: ">", GTGT: ">>", LT: "<", LTLT: "<<"}

_BARE_GLUE_TYPES = (IDENT, NUMBER, COLON, DOT, MINUS)


class Parser:
    def __init__(self, text: str):
        self.text = text
        self.tokens: list[Token] = tokenize(text)
        self.pos = 0

    # -- token stream helpers -------------------------------------------------

    def current(self) -> Token:
        return self.tokens[self.pos]

    def peek(self, offset: int = 1) -> Token:
        idx = self.pos + offset
        if idx < len(self.tokens):
            return self.tokens[idx]
        return self.tokens[-1]

    def advance(self) -> Token:
        tok = self.tokens[self.pos]
        if self.pos < len(self.tokens) - 1:
            self.pos += 1
        return tok

    def check(self, type_: str) -> bool:
        return self.current().type == type_

    def at_end(self) -> bool:
        return self.current().type == EOF

    def error(self, msg: str, tok: Optional[Token] = None) -> None:
        tok = tok or self.current()
        raise ParseError(f"line {tok.line}: parse error: {msg}", line=tok.line, column=tok.col)

    def expect(self, type_: str) -> Token:
        if not self.check(type_):
            got = self.current()
            label = "end of input" if got.type == EOF else f"'{got.text}'"
            self.error(f"expected '{type_}', got {label}")
        return self.advance()

    def expect_ident_text(self) -> str:
        if not self.check(IDENT):
            got = self.current()
            label = "end of input" if got.type == EOF else f"'{got.text}'"
            self.error(f"expected identifier, got {label}")
        return self.advance().text

    # -- shared low-level readers ---------------------------------------------

    def parse_bare_or_string(self) -> str:
        """A double/single-quoted string, or a permissively-charactered bare
        word (letters, digits, underscore, colon, dot, hyphen) built by
        gluing adjacent tokens that touch with no whitespace between them."""
        if self.check(STRING):
            return self.advance().value
        if self.current().type in _BARE_GLUE_TYPES:
            parts = []
            prev_end = None
            while self.current().type in _BARE_GLUE_TYPES:
                tok = self.current()
                if prev_end is not None and tok.start != prev_end:
                    break
                parts.append(tok.text)
                prev_end = tok.end
                self.advance()
            return "".join(parts)
        got = self.current()
        label = "end of input" if got.type == EOF else f"'{got.text}'"
        self.error(f"expected a key/value, got {label}")

    def expect_string_value(self) -> str:
        """A quoted string, falling back to a bare word for leniency (some
        constructs like roles are normally quoted but not strictly required
        by every Overpass dialect)."""
        return self.parse_bare_or_string()

    def parse_signed_number(self) -> float:
        if self.check(MINUS):
            minus_tok = self.advance()
            if not self.check(NUMBER) or self.current().start != minus_tok.end:
                self.error("expected a number after '-'")
            return -self.advance().value
        if self.check(NUMBER):
            return self.advance().value
        got = self.current()
        label = "end of input" if got.type == EOF else f"'{got.text}'"
        self.error(f"expected a number, got {label}")

    def parse_number_list(self) -> list[float]:
        nums = [self.parse_signed_number()]
        while self.check(COMMA):
            self.advance()
            nums.append(self.parse_signed_number())
        return nums

    def parse_int_list(self) -> list[int]:
        return [int(n) for n in self.parse_number_list()]

    def capture_until_matching_rparen(self) -> str:
        """Raw source text from the current position up to (excluding) the
        ')' that closes the *currently open* paren (i.e. the one the caller
        already consumed to get here), tracking nested parens. Leaves the
        parser positioned at that closing ')'. Used for (if: ...) filters
        and if (...) statement conditions, which are kept as source text."""
        start_offset = self.current().start
        end_offset = start_offset
        depth = 0
        while True:
            if self.at_end():
                self.error("unbalanced parentheses")
            tok = self.current()
            if tok.type == RPAREN and depth == 0:
                break
            if tok.type == LPAREN:
                depth += 1
            elif tok.type == RPAREN:
                depth -= 1
            end_offset = tok.end
            self.advance()
        return self.text[start_offset:end_offset].strip()

    def parse_output_set(self) -> str:
        """Optional '->.name' suffix, returning "_" if absent."""
        if self.check(ARROW):
            self.advance()
            self.expect(DOT)
            return self.expect_ident_text()
        return "_"

    # -- program ---------------------------------------------------------------

    def parse_program(self) -> ast.Program:
        settings = self.parse_settings()
        statements: list[ast.Statement] = []
        while not self.at_end():
            stmt = self.parse_statement()
            if stmt is not None:
                statements.append(stmt)
        return ast.Program(settings=settings, statements=statements)

    def parse_settings(self) -> ast.Settings:
        settings = ast.Settings()
        saw_any = False
        while self.check(LBRACKET):
            saw_any = True
            self.advance()
            name = self.expect_ident_text()
            self.expect(COLON)
            if name == "out":
                fmt = self.expect_ident_text()
                if fmt not in ("json", "xml", "csv"):
                    self.error(f"unknown output format '{fmt}'")
                settings.out_format = fmt  # type: ignore[assignment]
                if fmt == "csv":
                    self.expect(LPAREN)
                    fields: list[str] = []
                    while not (self.check(SEMI) or self.check(RPAREN)):
                        fields.append(self.parse_csv_field())
                        if self.check(COMMA):
                            self.advance()
                        elif not (self.check(SEMI) or self.check(RPAREN)):
                            self.error("expected ',' in csv field list")
                    settings.csv_fields = fields
                    if self.check(SEMI):
                        self.advance()
                        if self.check(IDENT):
                            settings.csv_header = self.expect_ident_text() == "true"
                        if self.check(SEMI):
                            self.advance()
                            settings.csv_separator = self.expect_string_value()
                    self.expect(RPAREN)
            elif name == "timeout":
                settings.timeout = int(self.parse_signed_number())
            elif name == "maxsize":
                settings.maxsize = int(self.parse_signed_number())
            elif name == "bbox":
                nums = self.parse_number_list()
                if len(nums) != 4:
                    self.error("[bbox:...] requires exactly 4 numbers (south,west,north,east)")
                settings.bbox = (nums[0], nums[1], nums[2], nums[3])
            elif name == "date":
                settings.date = self.expect_string_value()
            elif name == "diff":
                a = self.expect_string_value()
                b = None
                if self.check(COMMA):
                    self.advance()
                    b = self.expect_string_value()
                settings.diff = (a, b)
            elif name == "adiff":
                a = self.expect_string_value()
                b = None
                if self.check(COMMA):
                    self.advance()
                    b = self.expect_string_value()
                settings.adiff = (a, b)
            else:
                self.error(f"unknown global setting '{name}'")
            self.expect(RBRACKET)
        if saw_any:
            self.expect(SEMI)
        return settings

    def parse_csv_field(self) -> str:
        if self.check(COLON) and self.peek(1).type == COLON:
            self.advance()
            self.advance()
            return "::" + self.expect_ident_text()
        if self.check(STRING):
            return self.advance().value
        return self.parse_bare_or_string()

    # -- statements --------------------------------------------------------

    def parse_statement(self) -> Optional[ast.Statement]:
        if self.check(SEMI):
            self.advance()
            return None
        tok = self.current()
        if tok.type == LPAREN:
            return self.parse_block()
        if tok.type == DOT:
            return self.parse_dot_statement()
        if tok.type in _RECURSE_KIND_BY_TOKEN:
            return self.parse_recurse_op("_")
        if tok.type == IDENT:
            word = tok.text
            if word in _QUERY_KEYWORDS:
                return self.parse_query(word)
            if word == "out":
                self.advance()
                return self.parse_out("_")
            if word == "foreach":
                return self.parse_foreach()
            if word == "if":
                return self.parse_if_statement()
            if word == "is_in":
                self.advance()
                return self.parse_is_in("_")
            if word == "map_to_area":
                self.advance()
                return self.parse_map_to_area("_")
            if word in ("for", "complete", "retro", "compare"):
                return self.parse_unsupported_block(word)
            if word in ("make", "convert", "timeline", "local"):
                return self.parse_unsupported_simple(word)
            self.error(f"unknown statement '{word}'")
        label = "end of input" if tok.type == EOF else f"'{tok.text}'"
        self.error(f"unexpected token {label}")

    def parse_dot_statement(self) -> ast.Statement:
        self.advance()  # consume '.'
        name = self.expect_ident_text()
        if self.check(IDENT) and self.current().text == "is_in":
            self.advance()
            return self.parse_is_in(name)
        if self.check(IDENT) and self.current().text == "map_to_area":
            self.advance()
            return self.parse_map_to_area(name)
        if self.check(IDENT) and self.current().text == "out":
            self.advance()
            return self.parse_out(name)
        if self.current().type in _RECURSE_KIND_BY_TOKEN:
            return self.parse_recurse_op(name)
        output_set = self.parse_output_set()
        self.expect(SEMI)
        return ast.Item(input_set=name, output_set=output_set)

    def parse_recurse_op(self, input_set: str) -> ast.Recurse:
        kind = _RECURSE_KIND_BY_TOKEN[self.current().type]
        self.advance()
        output_set = self.parse_output_set()
        self.expect(SEMI)
        return ast.Recurse(kind=kind, input_set=input_set, output_set=output_set)  # type: ignore[arg-type]

    def parse_is_in(self, input_set: str) -> ast.IsIn:
        coords = None
        if self.check(LPAREN):
            self.advance()
            lat = self.parse_signed_number()
            self.expect(COMMA)
            lon = self.parse_signed_number()
            self.expect(RPAREN)
            coords = (lat, lon)
        output_set = self.parse_output_set()
        self.expect(SEMI)
        return ast.IsIn(input_set=input_set, output_set=output_set, coords=coords)

    def parse_map_to_area(self, input_set: str) -> ast.MapToArea:
        output_set = self.parse_output_set()
        self.expect(SEMI)
        return ast.MapToArea(input_set=input_set, output_set=output_set)

    def parse_query(self, keyword: str) -> ast.Query:
        self.advance()  # consume the type keyword
        types = list(_QUERY_KEYWORDS[keyword])
        input_sets: list[str] = []
        while self.check(DOT):
            self.advance()
            input_sets.append(self.expect_ident_text())
        filters: list[ast.Filter] = []
        while self.check(LBRACKET) or self.check(LPAREN):
            if self.check(LBRACKET):
                filters.append(self.parse_tag_filter())
            else:
                filters.append(self.parse_paren_filter())
        output_set = self.parse_output_set()
        self.expect(SEMI)
        return ast.Query(types=types, filters=filters, input_sets=input_sets, output_set=output_set)

    def parse_tag_filter(self) -> ast.TagFilter:
        self.expect(LBRACKET)
        if self.check(BANG):
            self.advance()
            key = self.parse_bare_or_string()
            self.expect(RBRACKET)
            return ast.TagFilter(key=key, op="not_exists")
        if self.check(TILDE):
            self.advance()
            keypat = self.parse_bare_or_string()
            self.expect(TILDE)
            valpat = self.parse_bare_or_string()
            case_insensitive = self._parse_optional_i_flag()
            self.expect(RBRACKET)
            return ast.TagFilter(
                key=keypat, op="~", value=valpat, key_is_regex=True, case_insensitive=case_insensitive
            )
        key = self.parse_bare_or_string()
        if self.check(RBRACKET):
            self.advance()
            return ast.TagFilter(key=key, op="exists")
        if self.check(EQ):
            op = "="
        elif self.check(NEQ):
            op = "!="
        elif self.check(NOTTILDE):
            op = "!~"
        elif self.check(TILDE):
            op = "~"
        else:
            got = self.current()
            label = "end of input" if got.type == EOF else f"'{got.text}'"
            self.error(f"expected an operator (=, !=, ~, !~) or ']' in tag filter, got {label}")
        self.advance()
        value = self.parse_bare_or_string()
        case_insensitive = self._parse_optional_i_flag()
        self.expect(RBRACKET)
        return ast.TagFilter(key=key, op=op, value=value, case_insensitive=case_insensitive)  # type: ignore[arg-type]

    def _parse_optional_i_flag(self) -> bool:
        if self.check(COMMA):
            self.advance()
            flag = self.expect_ident_text()
            if flag != "i":
                self.error(f"unknown filter flag ',{flag}' (only ',i' is supported)")
            return True
        return False

    def parse_paren_filter(self) -> ast.Filter:
        self.expect(LPAREN)
        if self.check(IDENT):
            word = self.current().text
            if word == "id":
                self.advance()
                self.expect(COLON)
                filt: ast.Filter = ast.IdFilter(ids=self.parse_int_list())
            elif word in ("w", "r", "bn", "bw", "br"):
                self.advance()
                set_name = "_"
                if self.check(DOT):
                    self.advance()
                    set_name = self.expect_ident_text()
                role = None
                if self.check(COLON):
                    self.advance()
                    role = self.expect_string_value()
                filt = ast.RecurseFilter(kind=word, set_name=set_name, role=role)  # type: ignore[arg-type]
            elif word == "around":
                self.advance()
                set_name = None
                if self.check(DOT):
                    self.advance()
                    set_name = self.expect_ident_text()
                self.expect(COLON)
                radius = self.parse_signed_number()
                coords = None
                nums: list[float] = []
                while self.check(COMMA):
                    self.advance()
                    nums.append(self.parse_signed_number())
                if nums:
                    if len(nums) % 2 != 0:
                        self.error("(around: ...) coordinate list must have an even number of values")
                    coords = [(nums[k], nums[k + 1]) for k in range(0, len(nums), 2)]
                filt = ast.AroundFilter(radius=radius, set_name=set_name, coords=coords)
            elif word == "poly":
                self.advance()
                self.expect(COLON)
                s = self.expect_string_value()
                nums = [float(x) for x in s.split()]
                if len(nums) % 2 != 0 or len(nums) < 6:
                    self.error("(poly: ...) requires an even number of coordinates (at least 3 points)")
                coords = [(nums[k], nums[k + 1]) for k in range(0, len(nums), 2)]
                filt = ast.PolyFilter(coords=coords)
            elif word == "area":
                self.advance()
                set_name = None
                area_id = None
                if self.check(DOT):
                    self.advance()
                    set_name = self.expect_ident_text()
                elif self.check(COLON):
                    self.advance()
                    area_id = int(self.parse_signed_number())
                filt = ast.AreaFilter(set_name=set_name, area_id=area_id)
            elif word == "pivot":
                self.advance()
                set_name = "_"
                if self.check(DOT):
                    self.advance()
                    set_name = self.expect_ident_text()
                filt = ast.PivotFilter(set_name=set_name)
            elif word == "newer":
                self.advance()
                self.expect(COLON)
                filt = ast.NewerFilter(timestamp=self.expect_string_value())
            elif word == "changed":
                self.advance()
                self.expect(COLON)
                since = self.expect_string_value()
                until = None
                if self.check(COMMA):
                    self.advance()
                    until = self.expect_string_value()
                filt = ast.ChangedFilter(since=since, until=until)
            elif word == "user":
                self.advance()
                self.expect(COLON)
                names = [self.expect_string_value()]
                while self.check(COMMA):
                    self.advance()
                    names.append(self.expect_string_value())
                filt = ast.UserFilter(names=names)
            elif word == "uid":
                self.advance()
                self.expect(COLON)
                uids = [int(self.parse_signed_number())]
                while self.check(COMMA):
                    self.advance()
                    uids.append(int(self.parse_signed_number()))
                filt = ast.UidFilter(uids=uids)
            elif word == "if":
                self.advance()
                self.expect(COLON)
                filt = ast.IfFilter(expression=self.capture_until_matching_rparen())
            else:
                self.error(f"unknown filter '{word}'")
        elif self.current().type in (NUMBER, MINUS):
            nums = self.parse_number_list()
            if len(nums) == 4:
                filt = ast.BboxFilter(south=nums[0], west=nums[1], north=nums[2], east=nums[3])
            else:
                filt = ast.IdFilter(ids=[int(x) for x in nums])
        else:
            got = self.current()
            label = "end of input" if got.type == EOF else f"'{got.text}'"
            self.error(f"unexpected token {label} in filter")
        self.expect(RPAREN)
        return filt

    def parse_out(self, input_set: str) -> ast.Out:
        out = ast.Out(input_set=input_set)
        while not self.check(SEMI):
            if self.check(IDENT):
                word = self.current().text
                if word in ("ids", "skel", "body", "tags", "meta"):
                    out.verbosity = word  # type: ignore[assignment]
                    self.advance()
                elif word == "noids":
                    out.noids = True
                    self.advance()
                elif word == "geom":
                    self.advance()
                    out.geometry = "geom"
                    if self.check(LPAREN):
                        self.advance()
                        nums = self.parse_number_list()
                        if len(nums) != 4:
                            self.error("out geom(...) requires exactly 4 numbers (south,west,north,east)")
                        out.geom_bbox = (nums[0], nums[1], nums[2], nums[3])
                        self.expect(RPAREN)
                elif word == "bb":
                    out.geometry = "bb"
                    self.advance()
                elif word == "center":
                    out.geometry = "center"
                    self.advance()
                elif word in ("qt", "asc"):
                    out.order = word  # type: ignore[assignment]
                    self.advance()
                elif word == "count":
                    out.count = True
                    self.advance()
                else:
                    self.error(f"unknown 'out' modifier '{word}'")
            elif self.check(NUMBER):
                out.limit = int(self.advance().value)
            else:
                got = self.current()
                label = "end of input" if got.type == EOF else f"'{got.text}'"
                self.error(f"unexpected token {label} in 'out' statement")
        self.advance()  # consume ';'
        return out

    def parse_block(self) -> ast.Statement:
        self.expect(LPAREN)
        parts: list[list[ast.Statement]] = [[]]
        while not self.check(RPAREN):
            if self.check(MINUS):
                self.advance()
                parts.append([])
                continue
            if self.at_end():
                self.error("unbalanced '(' ')' in union/difference block")
            stmt = self.parse_statement()
            if stmt is not None:
                parts[-1].append(stmt)
        self.expect(RPAREN)
        output_set = self.parse_output_set()
        self.expect(SEMI)
        if len(parts) == 1:
            return ast.Union(statements=parts[0], output_set=output_set)
        if len(parts) == 2:
            first = parts[0][0] if len(parts[0]) == 1 else ast.Union(statements=parts[0])
            second = parts[1][0] if len(parts[1]) == 1 else ast.Union(statements=parts[1])
            return ast.Difference(first=first, second=second, output_set=output_set)
        self.error("too many '-' separators in a difference block (expected exactly one)")

    def parse_foreach(self) -> ast.Foreach:
        self.advance()  # consume 'foreach'
        input_set = "_"
        if self.check(DOT):
            self.advance()
            input_set = self.expect_ident_text()
        output_set = self.parse_output_set()
        self.expect(LBRACE)
        body: list[ast.Statement] = []
        while not self.check(RBRACE):
            if self.at_end():
                self.error("unbalanced '{' '}' in foreach block")
            stmt = self.parse_statement()
            if stmt is not None:
                body.append(stmt)
        self.expect(RBRACE)
        return ast.Foreach(input_set=input_set, output_set=output_set, body=body)

    def parse_if_statement(self) -> ast.If:
        self.advance()  # consume 'if'
        self.expect(LPAREN)
        condition = self.capture_until_matching_rparen()
        self.expect(RPAREN)
        self.expect(LBRACE)
        then_stmts: list[ast.Statement] = []
        while not self.check(RBRACE):
            if self.at_end():
                self.error("unbalanced '{' '}' in if block")
            stmt = self.parse_statement()
            if stmt is not None:
                then_stmts.append(stmt)
        self.expect(RBRACE)
        otherwise: list[ast.Statement] = []
        if self.check(IDENT) and self.current().text == "else":
            self.advance()
            self.expect(LBRACE)
            while not self.check(RBRACE):
                if self.at_end():
                    self.error("unbalanced '{' '}' in else block")
                stmt = self.parse_statement()
                if stmt is not None:
                    otherwise.append(stmt)
            self.expect(RBRACE)
        return ast.If(condition=condition, then=then_stmts, otherwise=otherwise)

    def parse_unsupported_block(self, keyword: str) -> ast.Unsupported:
        """for/complete/retro/compare: consume up to and including the
        matching '{' ... '}' block (plus a trailing ';' if present)."""
        start_offset = self.current().start
        self.advance()  # consume the keyword
        depth_paren = 0
        while not (self.check(LBRACE) and depth_paren == 0):
            if self.at_end():
                self.error(f"expected '{{' after '{keyword}'")
            t = self.current()
            if t.type == LPAREN:
                depth_paren += 1
            elif t.type == RPAREN:
                depth_paren -= 1
            self.advance()
        self.advance()  # consume '{'
        depth_brace = 1
        end_tok = self.tokens[self.pos - 1]
        while depth_brace > 0:
            if self.at_end():
                self.error(f"unbalanced '{{' '}}' in '{keyword}' block")
            t = self.current()
            if t.type == LBRACE:
                depth_brace += 1
            elif t.type == RBRACE:
                depth_brace -= 1
            end_tok = t
            self.advance()
        end_offset = end_tok.end
        if self.check(SEMI):
            end_offset = self.current().end
            self.advance()
        return ast.Unsupported(keyword=keyword, source=self.text[start_offset:end_offset].strip())

    def parse_unsupported_simple(self, keyword: str) -> ast.Unsupported:
        """make/convert/timeline/local: consume up to and including the next
        top-level ';' (not nested inside parens/braces)."""
        start_tok = self.current()
        start_offset = start_tok.start
        self.advance()  # consume the keyword
        depth_paren = 0
        depth_brace = 0
        end_offset = start_tok.end
        while True:
            if self.at_end():
                self.error(f"missing ';' after '{keyword}' statement")
            tok = self.current()
            if tok.type == SEMI and depth_paren == 0 and depth_brace == 0:
                end_offset = tok.end
                self.advance()
                break
            if tok.type == LPAREN:
                depth_paren += 1
            elif tok.type == RPAREN:
                depth_paren -= 1
            elif tok.type == LBRACE:
                depth_brace += 1
            elif tok.type == RBRACE:
                depth_brace -= 1
            end_offset = tok.end
            self.advance()
        return ast.Unsupported(keyword=keyword, source=self.text[start_offset:end_offset].strip())


def parse(text: str) -> ast.Program:
    """Parse Overpass QL source text into a :class:`osmpq.ql.ast.Program`.

    Raises ``osmpq.errors.ParseError`` (with 1-based ``line``/``column``) on
    syntax errors, in the style Overpass itself uses:
    ``line N: parse error: ...``.
    """
    return Parser(text).parse_program()
