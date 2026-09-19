"""Translate osmpq.ql.ast.TagFilter into SQL fragments.

Promoted keys get pushed to their own column; everything else uses the
``tags`` MAP column. DuckDB ``map['k']`` returns the value or NULL when the
key is absent (verified against DuckDB 1.5.5), which is exactly what we
want for equality/regex, but `!=` and key-existence semantics need explicit
NULL handling to match Overpass ("[k!=v] selects elements where the tag is
absent OR has a different value").
"""
from __future__ import annotations

from osmpq.ql.ast import TagFilter

_IDENT_OK = set("abcdefghijklmnopqrstuvwxyz0123456789_")


def _is_promotable_ident(key: str) -> bool:
    return bool(key) and all(c in _IDENT_OK for c in key)


def _col_ref(key: str, promoted_keys: set[str]) -> tuple[str, bool]:
    """Return (sql_expr_for_value, is_promoted)."""
    if key in promoted_keys and _is_promotable_ident(key):
        return f'"{key}"', True
    return f"tags['{_sql_escape(key)}']", False


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


def is_negative_only(filters: list[TagFilter]) -> bool:
    """True if every filter is `!=` or `not_exists` (so untagged rows can
    still match; contract section 8 / design.md 3.4)."""
    return all(f.op in ("!=", "not_exists") for f in filters)


def tag_filter_sql(f: TagFilter, promoted_keys: set[str]) -> str:
    op = f.op
    if f.key_is_regex:
        # [~"regex"~"value"] : any tag whose key matches the regex has a
        # value matching the value regex.
        key_pat = _sql_escape(f.key)
        val_pat = _sql_escape(f.value or "")
        flag = ", 'i'" if f.case_insensitive else ""
        return (
            "tags IS NOT NULL AND list_bool_or(list_transform(map_entries(tags), "
            f"e -> regexp_matches(e.key, '{key_pat}') AND regexp_matches(e.value, '{val_pat}'{flag})))"
        )

    val_expr, promoted = _col_ref(f.key, promoted_keys)

    if op == "exists":
        return f"{val_expr} IS NOT NULL"
    if op == "not_exists":
        return f"{val_expr} IS NULL"
    if op == "=":
        return f"{val_expr} = '{_sql_escape(f.value or '')}'"
    if op == "!=":
        return f"({val_expr} IS NULL OR {val_expr} != '{_sql_escape(f.value or '')}')"
    if op == "~":
        flag = ", 'i'" if f.case_insensitive else ""
        return f"{val_expr} IS NOT NULL AND regexp_matches({val_expr}, '{_sql_escape(f.value or '')}'{flag})"
    if op == "!~":
        flag = ", 'i'" if f.case_insensitive else ""
        return f"({val_expr} IS NULL OR NOT regexp_matches({val_expr}, '{_sql_escape(f.value or '')}'{flag}))"
    raise ValueError(f"unhandled tag filter op {op!r}")


def tag_filters_sql(filters: list[TagFilter], promoted_keys: set[str]) -> str:
    parts = [tag_filter_sql(f, promoted_keys) for f in filters]
    if not parts:
        return "TRUE"
    return " AND ".join(f"({p})" for p in parts)
