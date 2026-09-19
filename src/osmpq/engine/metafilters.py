"""Meta filter hooks: ``(newer:"T")``, ``(changed:"A"[,"B"])``, ``(user:...)``,
``(uid:...)`` (docs/m3-contracts.md section 5.1).

None of these can bound a bbox (they filter on metadata, not geometry), so
every hook's ``implied_bbox`` returns ``None``. Each predicate compares the
canonical ``timestamp``/``user``/``uid`` columns (``osmpq.engine.schema``)
directly -- no temp tables needed.

Datasets built with ``raw-py`` lack metadata (version/changeset/timestamp/
uid/user) on untagged nodes (see the M1 report); an untagged node then has
NULL in all of these columns and is simply excluded by every filter here
(NULL compared with anything is NULL, i.e. not a match), same as Overpass
would treat an element it has no history for. Nothing further to do.
"""
from __future__ import annotations

import re

from osmpq.errors import RuntimeQueryError
from osmpq.ql.ast import ChangedFilter, NewerFilter, UidFilter, UserFilter

from . import hooks

_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _check_timestamp(value: str) -> str:
    if not _TIMESTAMP_RE.match(value):
        raise RuntimeQueryError(
            f'runtime error: date "{value}" is invalid. It should look like "2000-01-01T00:00:00Z".'
        )
    return value


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


class _NewerHook:
    def implied_bbox(self, ctx, q, f: NewerFilter):
        return None

    def predicate(self, ctx, q, f: NewerFilter, alias: str) -> str:
        ts = _check_timestamp(f.timestamp)
        return f'{alias}."timestamp" >= TIMESTAMP {_sql_str(ts.rstrip("Z"))}'


class _ChangedHook:
    def implied_bbox(self, ctx, q, f: ChangedFilter):
        return None

    def predicate(self, ctx, q, f: ChangedFilter, alias: str) -> str:
        since = _check_timestamp(f.since)
        if f.until is None:
            # No attic support (contract 5.1): "last edit at or after A".
            return f'{alias}."timestamp" >= TIMESTAMP {_sql_str(since.rstrip("Z"))}'
        until = _check_timestamp(f.until)
        return (
            f'{alias}."timestamp" BETWEEN TIMESTAMP {_sql_str(since.rstrip("Z"))} '
            f'AND TIMESTAMP {_sql_str(until.rstrip("Z"))}'
        )


class _UserHook:
    def implied_bbox(self, ctx, q, f: UserFilter):
        return None

    def predicate(self, ctx, q, f: UserFilter, alias: str) -> str:
        names_sql = ", ".join(_sql_str(n) for n in f.names)
        return f'{alias}."user" IN ({names_sql})'


class _UidHook:
    def implied_bbox(self, ctx, q, f: UidFilter):
        return None

    def predicate(self, ctx, q, f: UidFilter, alias: str) -> str:
        ids_sql = ", ".join(str(int(u)) for u in f.uids)
        return f"{alias}.uid IN ({ids_sql})"


hooks.register_filter(NewerFilter, _NewerHook())
hooks.register_filter(ChangedFilter, _ChangedHook())
hooks.register_filter(UserFilter, _UserHook())
hooks.register_filter(UidFilter, _UidHook())
