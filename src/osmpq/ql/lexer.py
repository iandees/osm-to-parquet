"""Hand-written lexer for Overpass QL.

Produces a flat list of :class:`Token`. Whitespace and comments (``//...``
and ``/* ... */``) are skipped between tokens; a trailing ``EOF`` token is
always appended so the parser never runs off the end of the list.

Numbers are always emitted unsigned (``-`` is its own ``MINUS`` token); the
parser glues an adjacent ``MINUS``+``NUMBER`` pair into a negative literal
where a signed number is expected. This avoids ambiguity with the
difference-statement ``-`` (which is always followed by whitespace and a
keyword, never a digit).

Bare (unquoted) identifiers only ever contain ``[A-Za-z0-9_]`` at the lexer
level. Overpass's more permissive bare tokens -- tag keys/values like
``addr:street`` -- are reassembled by the parser by gluing adjacent
``IDENT``/``NUMBER``/``COLON``/``DOT``/``MINUS`` tokens that touch with no
whitespace between them (see ``Parser.parse_bare_or_string``).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from osmpq.errors import ParseError

# Token type constants. Kept as plain strings (rather than an Enum) so the
# parser can compare/format them without an extra `.value` indirection.
STRING = "STRING"
NUMBER = "NUMBER"
IDENT = "IDENT"

LBRACKET = "["
RBRACKET = "]"
LPAREN = "("
RPAREN = ")"
LBRACE = "{"
RBRACE = "}"
SEMI = ";"
COMMA = ","
COLON = ":"
DOT = "."

EQ = "="
NEQ = "!="
TILDE = "~"
NOTTILDE = "!~"
BANG = "!"

LT = "<"
LTLT = "<<"
GT = ">"
GTGT = ">>"

ARROW = "->"
MINUS = "-"
EOF = "EOF"

_SINGLE_CHAR_MAP = {
    "[": LBRACKET,
    "]": RBRACKET,
    "(": LPAREN,
    ")": RPAREN,
    "{": LBRACE,
    "}": RBRACE,
    ";": SEMI,
    ",": COMMA,
    ":": COLON,
    ".": DOT,
    "=": EQ,
    "~": TILDE,
    "!": BANG,
    "<": LT,
    ">": GT,
    "-": MINUS,
}

_ESCAPES = {
    "n": "\n",
    "t": "\t",
    "r": "\r",
    "\\": "\\",
    '"': '"',
    "'": "'",
    "/": "/",
}


@dataclass
class Token:
    type: str
    text: str
    value: Any
    start: int
    end: int
    line: int
    col: int

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Token({self.type!r}, {self.text!r}, {self.start}:{self.end})"


def _line_col(text: str, offset: int) -> tuple[int, int]:
    line = text.count("\n", 0, offset) + 1
    last_nl = text.rfind("\n", 0, offset)
    col = offset - last_nl
    return line, col


def _read_string(text: str, i: int) -> tuple[str, int]:
    """Read a quoted string starting at ``i`` (which points at the opening
    quote). Returns (decoded value, index just past the closing quote)."""
    quote = text[i]
    n = len(text)
    j = i + 1
    out: list[str] = []
    while True:
        if j >= n:
            line, col = _line_col(text, i)
            raise ParseError(f"line {line}: parse error: unterminated string literal", line=line, column=col)
        c = text[j]
        if c == quote:
            return "".join(out), j + 1
        if c == "\\":
            if j + 1 >= n:
                line, col = _line_col(text, j)
                raise ParseError(f"line {line}: parse error: unterminated escape sequence", line=line, column=col)
            esc = text[j + 1]
            if esc == "u":
                hex_digits = text[j + 2 : j + 6]
                if len(hex_digits) != 4 or not all(ch in "0123456789abcdefABCDEF" for ch in hex_digits):
                    line, col = _line_col(text, j)
                    raise ParseError(
                        f"line {line}: parse error: invalid \\u escape in string literal", line=line, column=col
                    )
                out.append(chr(int(hex_digits, 16)))
                j += 6
                continue
            if esc in _ESCAPES:
                out.append(_ESCAPES[esc])
                j += 2
                continue
            # Unknown escape: Overpass is lenient here, keep the char as-is.
            out.append(esc)
            j += 2
            continue
        if c == "\n":
            line, col = _line_col(text, j)
            raise ParseError(f"line {line}: parse error: newline in string literal", line=line, column=col)
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
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = text.find("\n", i)
            i = n if j == -1 else j
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            j = text.find("*/", i + 2)
            if j == -1:
                line, col = _line_col(text, i)
                raise ParseError(f"line {line}: parse error: unterminated block comment", line=line, column=col)
            i = j + 2
            continue

        start = i
        line, col = _line_col(text, start)

        if c == '"' or c == "'":
            value, i = _read_string(text, i)
            tokens.append(Token(STRING, text[start:i], value, start, i, line, col))
            continue

        if c == "-" and i + 1 < n and text[i + 1] == ">":
            tokens.append(Token(ARROW, "->", None, start, i + 2, line, col))
            i += 2
            continue
        if c == "<" and i + 1 < n and text[i + 1] == "<":
            tokens.append(Token(LTLT, "<<", None, start, i + 2, line, col))
            i += 2
            continue
        if c == ">" and i + 1 < n and text[i + 1] == ">":
            tokens.append(Token(GTGT, ">>", None, start, i + 2, line, col))
            i += 2
            continue
        if c == "!" and i + 1 < n and text[i + 1] == "=":
            tokens.append(Token(NEQ, "!=", None, start, i + 2, line, col))
            i += 2
            continue
        if c == "!" and i + 1 < n and text[i + 1] == "~":
            tokens.append(Token(NOTTILDE, "!~", None, start, i + 2, line, col))
            i += 2
            continue

        if c in _SINGLE_CHAR_MAP:
            tokens.append(Token(_SINGLE_CHAR_MAP[c], c, None, start, i + 1, line, col))
            i += 1
            continue

        if c.isdigit():
            j = i + 1
            while j < n and text[j].isdigit():
                j += 1
            if j < n and text[j] == "." and j + 1 < n and text[j + 1].isdigit():
                j += 1
                while j < n and text[j].isdigit():
                    j += 1
            numtext = text[start:j]
            tokens.append(Token(NUMBER, numtext, float(numtext), start, j, line, col))
            i = j
            continue

        if c.isalpha() or c == "_":
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
            idtext = text[start:j]
            tokens.append(Token(IDENT, idtext, idtext, start, j, line, col))
            i = j
            continue

        raise ParseError(f"line {line}: parse error: unexpected character '{c}'", line=line, column=col)

    line, col = _line_col(text, n)
    tokens.append(Token(EOF, "", None, n, n, line, col))
    return tokens
