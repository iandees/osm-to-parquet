"""Overpass QL abstract syntax tree.

Shared contract between the parser (osmpq.ql.parser) and the planner
(osmpq.engine.planner). Keep this file small and stable: both sides import it.

Conventions
-----------
* Element types are the strings "node", "way", "relation". The query keywords
  ``nwr``, ``nw``, ``nr``, ``wr`` and ``rel`` are expanded by the parser into
  the corresponding set of element types; the AST never contains them.
* Set names are stored without the leading dot. The default set is "_".
* Coordinates are floats in degrees; bboxes are (south, west, north, east).
* Anything the parser understands syntactically but the planner does not
  support yet is still represented here; the planner raises
  ``osmpq.errors.UnsupportedError`` for it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional
from typing import Union as TypingUnion

ElementType = Literal["node", "way", "relation"]
ALL_TYPES: tuple[ElementType, ...] = ("node", "way", "relation")

# --------------------------------------------------------------------------
# Settings (the leading [out:json][timeout:25][bbox:...]; block)
# --------------------------------------------------------------------------


@dataclass
class Settings:
    out_format: Literal["json", "xml", "csv"] = "xml"
    csv_fields: Optional[list[str]] = None  # for [out:csv(name, ::id; true; ",")]
    csv_header: bool = True
    csv_separator: str = "\t"
    timeout: int = 180  # seconds
    maxsize: int = 536870912  # bytes
    bbox: Optional[tuple[float, float, float, float]] = None  # (s, w, n, e)
    date: Optional[str] = None  # [date:"..."] ISO timestamp; attic, unsupported in M0
    diff: Optional[tuple[str, Optional[str]]] = None  # [diff:"a","b"]; unsupported
    adiff: Optional[tuple[str, Optional[str]]] = None  # unsupported


# --------------------------------------------------------------------------
# Filters attached to a query statement: node[...](...)(...)
# --------------------------------------------------------------------------


@dataclass
class TagFilter:
    """[k=v], [k!=v], [k~v], [k!~v], [k], [!k], [~k~v] (key regex)."""

    key: str
    op: Literal["=", "!=", "~", "!~", "exists", "not_exists"]
    value: Optional[str] = None
    key_is_regex: bool = False  # [~"^addr:"~"."]
    case_insensitive: bool = False  # trailing ,i on regex filters


@dataclass
class BboxFilter:
    south: float
    west: float
    north: float
    east: float


@dataclass
class IdFilter:
    ids: list[int]


@dataclass
class RecurseFilter:
    """(w), (r), (bn), (bw), (br), optionally with a set and a role:
    node(w.a), way(r:"outer"), rel(br.x:"stop")."""

    kind: Literal["w", "r", "bn", "bw", "br"]
    set_name: str = "_"
    role: Optional[str] = None


@dataclass
class AroundFilter:
    """(around:r), (around.set:r), (around:r,lat,lon[,lat,lon...])."""

    radius: float
    set_name: Optional[str] = None
    coords: Optional[list[tuple[float, float]]] = None  # [(lat, lon), ...]


@dataclass
class PolyFilter:
    coords: list[tuple[float, float]]  # [(lat, lon), ...]


@dataclass
class AreaFilter:
    """(area), (area.set), (area:id)."""

    set_name: Optional[str] = None
    area_id: Optional[int] = None


@dataclass
class PivotFilter:
    set_name: str = "_"


@dataclass
class NewerFilter:
    timestamp: str


@dataclass
class ChangedFilter:
    since: str
    until: Optional[str] = None


@dataclass
class UserFilter:
    names: list[str]


@dataclass
class UidFilter:
    uids: list[int]


@dataclass
class IfFilter:
    """(if: <evaluator expression>). Kept as source text in M0."""

    expression: str


Filter = TypingUnion[
    TagFilter,
    BboxFilter,
    IdFilter,
    RecurseFilter,
    AroundFilter,
    PolyFilter,
    AreaFilter,
    PivotFilter,
    NewerFilter,
    ChangedFilter,
    UserFilter,
    UidFilter,
    IfFilter,
]

# --------------------------------------------------------------------------
# Statements
# --------------------------------------------------------------------------


@dataclass
class Query:
    """node/way/relation/area query with filters.

    ``input_sets`` holds the sets named on the type keyword (``node.a.b``);
    several sets mean intersection. Empty list means no set restriction.
    """

    types: list[str]  # subset of ALL_TYPES, or ["area"] for area queries
    filters: list[Filter] = field(default_factory=list)
    input_sets: list[str] = field(default_factory=list)
    output_set: str = "_"


@dataclass
class Union:  # noqa: A001 - mirrors the Overpass name
    statements: list["Statement"]
    output_set: str = "_"


@dataclass
class Difference:
    first: "Statement"
    second: "Statement"
    output_set: str = "_"


@dataclass
class Recurse:
    """>, >>, <, << with optional input/output sets: .a > -> .b;"""

    kind: Literal[">", ">>", "<", "<<"]
    input_set: str = "_"
    output_set: str = "_"


@dataclass
class Item:
    """.a;  or  .a -> .b;  (copy a set)."""

    input_set: str = "_"
    output_set: str = "_"


@dataclass
class IsIn:
    """is_in;  is_in(lat, lon);  .a is_in -> .b;"""

    input_set: str = "_"
    output_set: str = "_"
    coords: Optional[tuple[float, float]] = None


@dataclass
class MapToArea:
    input_set: str = "_"
    output_set: str = "_"


@dataclass
class Out:
    input_set: str = "_"
    verbosity: Literal["ids", "skel", "body", "tags", "meta"] = "body"
    geometry: Literal["none", "geom", "bb", "center"] = "none"
    geom_bbox: Optional[tuple[float, float, float, float]] = None  # out geom(s,w,n,e)
    order: Literal["asc", "qt"] = "asc"
    limit: Optional[int] = None
    noids: bool = False
    count: bool = False  # out count;


@dataclass
class Foreach:
    input_set: str = "_"
    output_set: str = "_"
    body: list["Statement"] = field(default_factory=list)


@dataclass
class If:
    condition: str  # evaluator source text
    then: list["Statement"] = field(default_factory=list)
    otherwise: list["Statement"] = field(default_factory=list)


@dataclass
class Unsupported:
    """Syntactically consumed statement the planner cannot run (for, complete,
    compare, make, convert, local ...)."""

    keyword: str
    source: str


@dataclass
class Retro:
    """``retro(<time-expr>) { <body> }`` (docs/m4-contracts.md section 3.2).
    ``time_expr`` is the raw source text between the parens: either a
    quoted string literal or an evaluator expression the M3 evaluator
    (``osmpq.ql.evaluator``) supports, evaluated against the ambient
    default set at run time. The body runs with ``catalog.SNAPSHOT`` set
    to the evaluated timestamp, restored to whatever it was before on
    exit (including on error). Block-local set scope (confirmed against
    a live reference probe): every set the body assigns -- including the
    default set ``_`` -- is restored to whatever it held before the block
    once it ends; only the body's own ``out`` statements are visible
    outside it."""

    time_expr: str
    body: list["Statement"] = field(default_factory=list)


@dataclass
class Timeline:
    """``timeline(<type>, <id>[, <version>])`` (docs/m4-contracts.md
    section 3.2). Produces a set of synthetic ``timeline`` elements, one
    per state (own version or minor version) of the named element."""

    element_type: ElementType
    element_id: int
    version: Optional[int] = None
    output_set: str = "_"


Statement = TypingUnion[
    Query,
    Union,
    Difference,
    Recurse,
    Item,
    IsIn,
    MapToArea,
    Out,
    Foreach,
    If,
    Retro,
    Timeline,
    Unsupported,
]


@dataclass
class Program:
    settings: Settings
    statements: list[Statement]
