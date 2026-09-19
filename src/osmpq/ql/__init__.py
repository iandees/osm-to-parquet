"""Overpass QL lexer/parser for osmpq.

``parse(text)`` is the public entry point; it returns an
``osmpq.ql.ast.Program`` (see ``osmpq.ql.ast`` for the AST shapes) or raises
``osmpq.errors.ParseError``.
"""
from . import ast
from .parser import parse

__all__ = ["parse", "ast"]
