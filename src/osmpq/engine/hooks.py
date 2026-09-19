"""Extension points the tier-2 language modules plug into (docs/m3-contracts.md
section 2). The planner owns statement dispatch and the base SELECT for a
query; everything a new filter or statement needs is reachable from the
`Context` it is handed. Modules register at import time; `planner` imports
`HOOK_MODULES` lazily on the first run so a missing module (a workstream
not merged yet) is simply "unsupported", never an import error.

Filter hooks
------------
A `FilterHook` handles one `osmpq.ql.ast` filter class:

* `implied_bbox(ctx, q, f)` -> (s, w, n, e) in degrees or None. The planner
  intersects every implied bbox with the explicit/global bbox to pick cells
  and row groups; the hook's predicate does the exact test afterwards. A
  hook that cannot bound its result returns None (the query then behaves
  like one without a bbox: every cell, with the usual warning).
* `predicate(ctx, q, f, alias)` -> SQL boolean expression over the canonical
  columns (`osmpq.engine.schema.CANONICAL_COLUMNS`) of a row aliased
  `alias`. It may create TEMP tables on `ctx.con` (fresh names from
  `ctx.fresh_name`) and reference them in the expression. Rows for which the
  expression is NULL are dropped, like SQL WHERE.

The planner applies predicates as `SELECT * FROM (<base>) <alias> WHERE
p1 AND p2 ...` after the base select (spatial/byid/set/recurse-filter path)
is built, so hooks never care where rows came from. Note that `geometry`
is only populated for ways read from spatial files (NULL from byid parts
and for relations) -- hooks must degrade gracefully (bbox columns are
always present for ways/relations; lat_e7/lon_e7 for nodes).

Statement hooks
---------------
`STATEMENT_HOOKS[cls](ctx, stmt)` executes a statement class the core
planner does not handle (IsIn, MapToArea, Foreach, If). Bodies recurse via
`planner.execute_statement`. `AREA_QUERY_HOOK`, if set, is called for a
`Query` whose types include "area" instead of the normal path.
"""
from __future__ import annotations

from typing import Callable, Optional, Protocol

BBox = tuple[float, float, float, float]


class FilterHook(Protocol):
    def implied_bbox(self, ctx, q, f) -> Optional[BBox]: ...

    def predicate(self, ctx, q, f, alias: str) -> str: ...


FILTER_HOOKS: dict[type, FilterHook] = {}
STATEMENT_HOOKS: dict[type, Callable] = {}
AREA_QUERY_HOOK: list[Callable] = []  # 0 or 1 entries; a list so modules can set it without `global`

# Modules that register hooks on import; imported lazily by the planner.
HOOK_MODULES = (
    "osmpq.engine.geofilters",   # W1: around, poly
    "osmpq.engine.metafilters",  # W3: newer, changed, user, uid
    "osmpq.engine.evalfilter",   # W3: (if:) filter, foreach, if
    "osmpq.engine.areas",        # W2: area queries, (area), (pivot), is_in, map_to_area
)


def register_filter(cls: type, hook: FilterHook) -> None:
    FILTER_HOOKS[cls] = hook


def register_statement(cls: type, fn: Callable) -> None:
    STATEMENT_HOOKS[cls] = fn


def set_area_query_hook(fn: Callable) -> None:
    AREA_QUERY_HOOK[:] = [fn]


def intersect_bbox(a: Optional[BBox], b: Optional[BBox]) -> Optional[BBox]:
    """Intersection of two (s, w, n, e) bboxes; None means "unbounded".
    An empty intersection is returned as a degenerate bbox with s > n so
    the caller can detect it (`is_empty_bbox`)."""
    if a is None:
        return b
    if b is None:
        return a
    s, w, n, e = max(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), min(a[3], b[3])
    return (s, w, n, e)


def is_empty_bbox(b: Optional[BBox]) -> bool:
    return b is not None and (b[0] > b[2] or b[1] > b[3])
