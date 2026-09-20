"""Tokenizer and recursive-descent parser for the Overpass QL evaluator
subset used by ``(if:)`` filters and ``if``/``foreach`` statements
(docs/m3-contracts.md section 5.2).

The parser turns the raw source text the main QL parser already captured
verbatim (``IfFilter.expression``, ``If.condition``) into a small
expression AST. It knows nothing about SQL or the engine's canonical
columns -- that translation lives in ``osmpq.engine.evalsql``. Keeping the
two apart lets ``evalsql`` compile the *same* AST two different ways
(a batch SQL expression for ``(if:)``, a one-shot Python-side evaluation
for ``if``).

Grammar (highest to lowest precedence)::

    primary    := NUMBER | STRING
                | 't' '[' STRING ']'
                | '.' IDENT '.' IDENT '(' TYPE_KEYWORD ')'      # .a.count(ways)
                | IDENT '(' TYPE_KEYWORD ')'                     # count(ways)
                | IDENT '(' expr ')'                             # u/min/max/sum/set
                | IDENT '(' [expr (',' expr)*] ')'               # element-scoped calls
                | '(' expr ')'
    unary      := ('!' | '-') unary | primary
    mult       := unary (('*' | '/') unary)*
    additive   := mult (('+' | '-') mult)*
    relational := additive (('<' | '<=' | '>' | '>=') additive)*
    equality   := relational (('==' | '!=') relational)*
    and_expr   := equality ('&&' equality)*
    or_expr    := and_expr ('||' and_expr)*
    ternary    := or_expr ('?' expr ':' ternary)?
    expr       := ternary

Element-scoped functions (0 or 1 argument, see ``_ARITY``): ``id()``,
``type()``, ``version()``, ``timestamp()``, ``changeset()``, ``uid()``,
``user()``, ``count_tags()``, ``count_members()``,
``count_distinct_members()``, ``count_by_role(e)``, ``is_closed()``,
``length()``, ``lat()``, ``lon()``, ``is_tag(e)``, ``number(e)``,
``is_number(e)``. Set-scoped: bare ``count(nodes|ways|relations|areas|nwr)``
(the "current" set -- whichever ``evaluate_set`` was invoked against),
``.name.count(nodes|ways|relations|areas|nwr)`` (an explicitly named set),
and the aggregators ``u(e)``, ``min(e)``, ``max(e)``, ``sum(e)``, ``set(e)``
which run ``e`` (an element-scoped expression) once per element of the
current set and combine the results.

Raises ``osmpq.errors.ParseError`` on malformed input. Line/column info is
not tracked (the source text is a short, already-isolated fragment; see
docs/m3-contracts.md section 5.2).
"""
from __future__ import annotations

from dataclasses import dataclass

from osmpq.errors import ParseError

# -- token types ------------------------------------------------------------

NUMBER = "NUMBER"
STRING = "STRING"
IDENT = "IDENT"
LPAREN = "("
RPAREN = ")"
LBRACKET = "["
RBRACKET = "]"
COMMA = ","
DOT = "."
PLUS = "+"
MINUS = "-"
STAR = "*"
SLASH = "/"
BANG = "!"
LT = "<"
LE = "<="
GT = ">"
GE = ">="
EQEQ = "=="
NEQ = "!="
ANDAND = "&&"
OROR = "||"
QUESTION = "?"
COLON = ":"
EOF = "EOF"

_SINGLE_CHAR = {
    "(": LPAREN,
    ")": RPAREN,
    "[": LBRACKET,
    "]": RBRACKET,
    ",": COMMA,
    ".": DOT,
    "+": PLUS,
    "*": STAR,
    "/": SLASH,
}

_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", '"': '"', "'": "'", "/": "/"}


@dataclass
class Token:
    type: str
    text: str
    value: object
    pos: int


def _read_string(text: str, i: int) -> tuple[str, int]:
    quote = text[i]
    n = len(text)
    j = i + 1
    out: list[str] = []
    while True:
        if j >= n:
            raise ParseError("evaluator: unterminated string literal")
        c = text[j]
        if c == quote:
            return "".join(out), j + 1
        if c == "\\" and j + 1 < n:
            esc = text[j + 1]
            out.append(_ESCAPES.get(esc, esc))
            j += 2
            continue
        out.append(c)
        j += 1


def tokenize(text: str) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c in " \t\r\n":
            i += 1
            continue
        start = i
        if c == '"' or c == "'":
            value, i = _read_string(text, i)
            tokens.append(Token(STRING, text[start:i], value, start))
            continue
        if c == "&" and i + 1 < n and text[i + 1] == "&":
            tokens.append(Token(ANDAND, "&&", None, start))
            i += 2
            continue
        if c == "|" and i + 1 < n and text[i + 1] == "|":
            tokens.append(Token(OROR, "||", None, start))
            i += 2
            continue
        if c == "<" and i + 1 < n and text[i + 1] == "=":
            tokens.append(Token(LE, "<=", None, start))
            i += 2
            continue
        if c == ">" and i + 1 < n and text[i + 1] == "=":
            tokens.append(Token(GE, ">=", None, start))
            i += 2
            continue
        if c == "=" and i + 1 < n and text[i + 1] == "=":
            tokens.append(Token(EQEQ, "==", None, start))
            i += 2
            continue
        if c == "!" and i + 1 < n and text[i + 1] == "=":
            tokens.append(Token(NEQ, "!=", None, start))
            i += 2
            continue
        if c == "<":
            tokens.append(Token(LT, "<", None, start))
            i += 1
            continue
        if c == ">":
            tokens.append(Token(GT, ">", None, start))
            i += 1
            continue
        if c == "!":
            tokens.append(Token(BANG, "!", None, start))
            i += 1
            continue
        if c == "-":
            tokens.append(Token(MINUS, "-", None, start))
            i += 1
            continue
        if c == "?":
            tokens.append(Token(QUESTION, "?", None, start))
            i += 1
            continue
        if c == ":":
            tokens.append(Token(COLON, ":", None, start))
            i += 1
            continue
        if c in _SINGLE_CHAR:
            tokens.append(Token(_SINGLE_CHAR[c], c, None, start))
            i += 1
            continue
        if c.isdigit() or (c == "." and i + 1 < n and text[i + 1].isdigit()):
            j = i
            while j < n and text[j].isdigit():
                j += 1
            if j < n and text[j] == "." and j + 1 < n and text[j + 1].isdigit():
                j += 1
                while j < n and text[j].isdigit():
                    j += 1
            numtext = text[start:j]
            tokens.append(Token(NUMBER, numtext, float(numtext), start))
            i = j
            continue
        if c.isalpha() or c == "_":
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
            idtext = text[start:j]
            tokens.append(Token(IDENT, idtext, idtext, start))
            i = j
            continue
        raise ParseError(f"evaluator: unexpected character {c!r} in expression")
    tokens.append(Token(EOF, "", None, n))
    return tokens


# -- AST ----------------------------------------------------------------


@dataclass
class Num:
    value: float


@dataclass
class Str:
    value: str


@dataclass
class TagAccess:
    """``t["k"]``."""

    key: str


@dataclass
class ElementCall:
    """Element-scoped function: ``id()``, ``is_tag("k")``, ``length()``, ..."""

    name: str
    args: list


@dataclass
class SetCount:
    """``count(nodes|ways|relations|areas|nwr)`` over the ambient set."""

    type_name: str


@dataclass
class NamedSetCount:
    """``.name.count(nodes|ways|relations|areas|nwr)``."""

    set_name: str
    type_name: str


@dataclass
class Aggregate:
    """``u(e)``, ``min(e)``, ``max(e)``, ``sum(e)``, ``set(e)`` over the
    ambient set; ``expr`` is an element-scoped expression."""

    func: str
    expr: object


@dataclass
class Unary:
    op: str
    expr: object


@dataclass
class BinOp:
    op: str
    left: object
    right: object


@dataclass
class Ternary:
    cond: object
    then: object
    otherwise: object


Expr = object  # any of the dataclasses above

_TYPE_KEYWORDS = {"nodes", "ways", "relations", "areas", "nwr"}
_AGG_FUNCS = {"u", "min", "max", "sum", "set"}
# name -> exact argument count
_ELEMENT_FUNC_ARITY = {
    "id": 0,
    "type": 0,
    "version": 0,
    "timestamp": 0,
    "changeset": 0,
    "uid": 0,
    "user": 0,
    "count_tags": 0,
    "count_members": 0,
    "count_distinct_members": 0,
    "is_closed": 0,
    "length": 0,
    "lat": 0,
    "lon": 0,
    "is_tag": 1,
    "count_by_role": 1,
    "number": 1,
    "is_number": 1,
}


class Parser:
    def __init__(self, text: str):
        self.text = text
        self.tokens = tokenize(text)
        self.pos = 0

    def current(self) -> Token:
        return self.tokens[self.pos]

    def check(self, type_: str) -> bool:
        return self.current().type == type_

    def advance(self) -> Token:
        tok = self.tokens[self.pos]
        if self.pos < len(self.tokens) - 1:
            self.pos += 1
        return tok

    def error(self, msg: str) -> None:
        tok = self.current()
        label = "end of expression" if tok.type == EOF else f"'{tok.text}'"
        raise ParseError(f"evaluator: {msg}, got {label}")

    def expect(self, type_: str) -> Token:
        if not self.check(type_):
            self.error(f"expected '{type_}'")
        return self.advance()

    def expect_ident(self) -> str:
        if not self.check(IDENT):
            self.error("expected an identifier")
        return self.advance().text

    def expect_type_keyword(self) -> str:
        if not self.check(IDENT) or self.current().text not in _TYPE_KEYWORDS:
            self.error("expected one of nodes/ways/relations/areas/nwr")
        return self.advance().text

    # -- expression grammar (lowest to highest precedence) ---------------

    def parse_expr(self) -> Expr:
        return self.parse_ternary()

    def parse_ternary(self) -> Expr:
        cond = self.parse_or()
        if self.check(QUESTION):
            self.advance()
            then = self.parse_expr()
            self.expect(COLON)
            otherwise = self.parse_ternary()
            return Ternary(cond, then, otherwise)
        return cond

    def parse_or(self) -> Expr:
        left = self.parse_and()
        while self.check(OROR):
            self.advance()
            left = BinOp("||", left, self.parse_and())
        return left

    def parse_and(self) -> Expr:
        left = self.parse_equality()
        while self.check(ANDAND):
            self.advance()
            left = BinOp("&&", left, self.parse_equality())
        return left

    def parse_equality(self) -> Expr:
        left = self.parse_relational()
        while self.check(EQEQ) or self.check(NEQ):
            op = self.advance().type
            left = BinOp(op, left, self.parse_relational())
        return left

    def parse_relational(self) -> Expr:
        left = self.parse_additive()
        while self.current().type in (LT, LE, GT, GE):
            op = self.advance().type
            left = BinOp(op, left, self.parse_additive())
        return left

    def parse_additive(self) -> Expr:
        left = self.parse_multiplicative()
        while self.current().type in (PLUS, MINUS):
            op = self.advance().type
            left = BinOp(op, left, self.parse_multiplicative())
        return left

    def parse_multiplicative(self) -> Expr:
        left = self.parse_unary()
        while self.current().type in (STAR, SLASH):
            op = self.advance().type
            left = BinOp(op, left, self.parse_unary())
        return left

    def parse_unary(self) -> Expr:
        if self.check(BANG):
            self.advance()
            return Unary("!", self.parse_unary())
        if self.check(MINUS):
            self.advance()
            return Unary("-", self.parse_unary())
        return self.parse_primary()

    def parse_primary(self) -> Expr:
        tok = self.current()
        if tok.type == NUMBER:
            self.advance()
            return Num(tok.value)
        if tok.type == STRING:
            self.advance()
            return Str(tok.value)
        if tok.type == LPAREN:
            self.advance()
            e = self.parse_expr()
            self.expect(RPAREN)
            return e
        if tok.type == DOT:
            self.advance()
            set_name = self.expect_ident()
            self.expect(DOT)
            method = self.expect_ident()
            if method != "count":
                self.error(f"unknown set method '.{method}(...)' (only '.count(...)' is supported)")
            self.expect(LPAREN)
            type_name = self.expect_type_keyword()
            self.expect(RPAREN)
            return NamedSetCount(set_name, type_name)
        if tok.type == IDENT:
            name = tok.text
            if name == "t":
                self.advance()
                self.expect(LBRACKET)
                if not self.check(STRING):
                    self.error("expected a quoted tag key in t[...]")
                key = self.advance().value
                self.expect(RBRACKET)
                return TagAccess(key)
            if name == "count":
                self.advance()
                self.expect(LPAREN)
                type_name = self.expect_type_keyword()
                self.expect(RPAREN)
                return SetCount(type_name)
            if name in _AGG_FUNCS:
                self.advance()
                self.expect(LPAREN)
                inner = self.parse_expr()
                self.expect(RPAREN)
                return Aggregate(name, inner)
            if name in _ELEMENT_FUNC_ARITY:
                self.advance()
                self.expect(LPAREN)
                args: list[Expr] = []
                if not self.check(RPAREN):
                    args.append(self.parse_expr())
                    while self.check(COMMA):
                        self.advance()
                        args.append(self.parse_expr())
                self.expect(RPAREN)
                expected = _ELEMENT_FUNC_ARITY[name]
                if len(args) != expected:
                    self.error(f"'{name}()' takes {expected} argument(s), got {len(args)}")
                return ElementCall(name, args)
            self.error(f"unknown function '{name}'")
        self.error("expected an expression")

    def parse_program(self) -> Expr:
        e = self.parse_expr()
        if not self.check(EOF):
            self.error("unexpected trailing input in expression")
        return e


def parse(text: str) -> Expr:
    """Parse an evaluator expression (the source text captured by
    ``IfFilter.expression`` or ``If.condition``) into an AST. Raises
    ``osmpq.errors.ParseError`` on malformed input."""
    return Parser(text).parse_program()
