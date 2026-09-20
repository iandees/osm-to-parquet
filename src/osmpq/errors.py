"""osmpq's exception hierarchy, shared by the parser, the engine and
``osmpq.server``. Each subclass maps to a specific HTTP behavior there:
``ParseError`` and ``UnsupportedError`` become an HTTP 400 response with
the Overpass-shaped HTML error page, while ``RuntimeQueryError`` (and a
timeout or cancellation) becomes an Overpass ``remark`` with HTTP 200 --
see docs/api.md for the exact response shapes.
"""


class OsmpqError(Exception):
    """Base class."""


class ParseError(OsmpqError):
    """Overpass QL syntax error. ``line``/``column`` are 1-based when known."""

    def __init__(self, message: str, line: int | None = None, column: int | None = None):
        super().__init__(message)
        self.message = message
        self.line = line
        self.column = column


class UnsupportedError(OsmpqError):
    """Valid Overpass QL that this engine does not implement (yet)."""


class RuntimeQueryError(OsmpqError):
    """Query failed at run time (timeout, memory, bad set reference...).
    Reported as an Overpass ``remark`` with HTTP 200."""
