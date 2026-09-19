"""OsmChange parsing, docs/m2-contracts.md section 1 and section 5 step 2.

Parses a batch of ``.osc``/``.osc.gz`` files (already downloaded, in
ascending sequence order) with pyosmium's ``FileProcessor`` into three
pyarrow tables (node, way, relation). Within one file, and across the whole
batch, the same id can appear more than once; the *last* occurrence wins
(processing files in sequence order and overwriting a per-id dict gives
that for free). Each output row carries the ``seq`` of the file its
surviving occurrence came from.

Column order: ``id, deleted, version, timestamp, changeset, uid, user,
tags`` (contract's order) plus the type-specific payload (``lat_e7,
lon_e7`` | ``refs`` | ``members``), then ``seq``. ``tags`` is a native
pyarrow ``map_(string, string)`` column (NULL when the element has no
tags), so it is read by DuckDB as ``MAP(VARCHAR, VARCHAR)`` with no SQL-side
conversion needed -- registering the table already gives a ``MAP`` column
(verified: a pyarrow ``map_`` array round-trips through
``duckdb.Connection.register`` as ``MAP(VARCHAR, VARCHAR)``, which is the
same "list of key/value pairs" the contract describes, just built directly
as an Arrow Map array instead of two side-by-side list columns + a
``map_from_entries`` SQL step).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import osmium
import pyarrow as pa

_ENTITIES = osmium.osm.NODE | osmium.osm.WAY | osmium.osm.RELATION

_MEMBER_TYPE = pa.struct([
    pa.field("type", pa.string()),
    pa.field("ref", pa.int64()),
    pa.field("role", pa.string()),
])


@dataclass
class _NodeRow:
    id: int
    deleted: bool
    version: int
    timestamp: object
    changeset: int
    uid: Optional[int]
    user: str
    tags: Optional[dict]
    lat_e7: Optional[int]
    lon_e7: Optional[int]
    seq: int


@dataclass
class _WayRow:
    id: int
    deleted: bool
    version: int
    timestamp: object
    changeset: int
    uid: Optional[int]
    user: str
    tags: Optional[dict]
    refs: list
    seq: int


@dataclass
class _RelationRow:
    id: int
    deleted: bool
    version: int
    timestamp: object
    changeset: int
    uid: Optional[int]
    user: str
    tags: Optional[dict]
    members: list
    seq: int


@dataclass
class BatchResult:
    node: pa.Table
    way: pa.Table
    relation: pa.Table
    first_seq: Optional[int]
    last_seq: Optional[int]
    n_files: int
    # docs/m4-contracts.md section 5.1: every occurrence of every element
    # across the whole batch (not deduplicated to "last occurrence wins"
    # like ``node``/``way``/``relation`` above), sorted by ``(id,
    # version)``, so a catch-up batch (several diffs applied in one run)
    # still gives the history writer a meta row (version, timestamp,
    # changeset, uid, user, tags/refs/members) for every version an id
    # passed through -- not just the one it ended the run on. Same columns
    # as ``node``/``way``/``relation``.
    node_all: pa.Table = None
    way_all: pa.Table = None
    relation_all: pa.Table = None


def _tags_of(obj) -> Optional[dict]:
    if obj.tags is None or len(obj.tags) == 0:
        return None
    return {t.k: t.v for t in obj.tags}


def _e7(x: float) -> int:
    return int(round(x * 1e7))


def parse_batch(seq_files: list[tuple[int, "str | Path"]]) -> BatchResult:
    """Parse a batch of ``(seq, osc_path)`` pairs, in ascending ``seq``
    order, into three deduplicated pyarrow tables. ``osc_path`` may be
    ``.osc`` or ``.osc.gz`` (pyosmium infers the format from the name)."""
    nodes: dict[int, _NodeRow] = {}
    ways: dict[int, _WayRow] = {}
    relations: dict[int, _RelationRow] = {}
    nodes_all: list[_NodeRow] = []
    ways_all: list[_WayRow] = []
    relations_all: list[_RelationRow] = []
    first_seq: Optional[int] = None
    last_seq: Optional[int] = None

    for seq, path in seq_files:
        first_seq = seq if first_seq is None else min(first_seq, seq)
        last_seq = seq if last_seq is None else max(last_seq, seq)
        fp = osmium.FileProcessor(str(path), _ENTITIES)
        for obj in fp:
            ts = obj.timestamp
            if ts is not None and ts.tzinfo is not None:
                ts = ts.replace(tzinfo=None)
            if isinstance(obj, osmium.osm.Node):
                loc = obj.location
                lat_e7 = lon_e7 = None
                if loc is not None and loc.valid():
                    lat_e7 = _e7(loc.lat)
                    lon_e7 = _e7(loc.lon)
                row = _NodeRow(
                    id=obj.id, deleted=bool(obj.deleted), version=obj.version,
                    timestamp=ts, changeset=obj.changeset, uid=obj.uid, user=obj.user,
                    tags=_tags_of(obj), lat_e7=lat_e7, lon_e7=lon_e7, seq=seq,
                )
                nodes[obj.id] = row
                nodes_all.append(row)
            elif isinstance(obj, osmium.osm.Way):
                row = _WayRow(
                    id=obj.id, deleted=bool(obj.deleted), version=obj.version,
                    timestamp=ts, changeset=obj.changeset, uid=obj.uid, user=obj.user,
                    tags=_tags_of(obj), refs=[n.ref for n in obj.nodes], seq=seq,
                )
                ways[obj.id] = row
                ways_all.append(row)
            elif isinstance(obj, osmium.osm.Relation):
                row = _RelationRow(
                    id=obj.id, deleted=bool(obj.deleted), version=obj.version,
                    timestamp=ts, changeset=obj.changeset, uid=obj.uid, user=obj.user,
                    tags=_tags_of(obj),
                    members=[(m.type, m.ref, m.role) for m in obj.members], seq=seq,
                )
                relations[obj.id] = row
                relations_all.append(row)

    return BatchResult(
        node=_nodes_to_table(nodes),
        way=_ways_to_table(ways),
        relation=_relations_to_table(relations),
        first_seq=first_seq,
        last_seq=last_seq,
        n_files=len(seq_files),
        node_all=_nodes_to_table(sorted(nodes_all, key=lambda r: (r.id, r.version)), presorted=True),
        way_all=_ways_to_table(sorted(ways_all, key=lambda r: (r.id, r.version)), presorted=True),
        relation_all=_relations_to_table(sorted(relations_all, key=lambda r: (r.id, r.version)), presorted=True),
    )


def _tags_array(tags_list: list[Optional[dict]]) -> pa.Array:
    entries = [None if t is None else list(t.items()) for t in tags_list]
    return pa.array(entries, type=pa.map_(pa.string(), pa.string()))


def _nodes_to_table(nodes, presorted: bool = False) -> pa.Table:
    rows = nodes if presorted else sorted(nodes.values(), key=lambda r: r.id)
    return pa.table({
        "id": pa.array([r.id for r in rows], type=pa.int64()),
        "deleted": pa.array([r.deleted for r in rows], type=pa.bool_()),
        "version": pa.array([r.version for r in rows], type=pa.int32()),
        "timestamp": pa.array([r.timestamp for r in rows], type=pa.timestamp("us")),
        "changeset": pa.array([r.changeset for r in rows], type=pa.int64()),
        "uid": pa.array([r.uid for r in rows], type=pa.int32()),
        "user": pa.array([r.user for r in rows], type=pa.string()),
        "tags": _tags_array([r.tags for r in rows]),
        "lat_e7": pa.array([r.lat_e7 for r in rows], type=pa.int32()),
        "lon_e7": pa.array([r.lon_e7 for r in rows], type=pa.int32()),
        "seq": pa.array([r.seq for r in rows], type=pa.int64()),
    })


def _ways_to_table(ways, presorted: bool = False) -> pa.Table:
    rows = ways if presorted else sorted(ways.values(), key=lambda r: r.id)
    return pa.table({
        "id": pa.array([r.id for r in rows], type=pa.int64()),
        "deleted": pa.array([r.deleted for r in rows], type=pa.bool_()),
        "version": pa.array([r.version for r in rows], type=pa.int32()),
        "timestamp": pa.array([r.timestamp for r in rows], type=pa.timestamp("us")),
        "changeset": pa.array([r.changeset for r in rows], type=pa.int64()),
        "uid": pa.array([r.uid for r in rows], type=pa.int32()),
        "user": pa.array([r.user for r in rows], type=pa.string()),
        "tags": _tags_array([r.tags for r in rows]),
        "refs": pa.array([r.refs for r in rows], type=pa.list_(pa.int64())),
        "seq": pa.array([r.seq for r in rows], type=pa.int64()),
    })


def _relations_to_table(relations, presorted: bool = False) -> pa.Table:
    rows = relations if presorted else sorted(relations.values(), key=lambda r: r.id)
    members = [
        [{"type": t, "ref": ref, "role": role} for (t, ref, role) in r.members]
        for r in rows
    ]
    return pa.table({
        "id": pa.array([r.id for r in rows], type=pa.int64()),
        "deleted": pa.array([r.deleted for r in rows], type=pa.bool_()),
        "version": pa.array([r.version for r in rows], type=pa.int32()),
        "timestamp": pa.array([r.timestamp for r in rows], type=pa.timestamp("us")),
        "changeset": pa.array([r.changeset for r in rows], type=pa.int64()),
        "uid": pa.array([r.uid for r in rows], type=pa.int32()),
        "user": pa.array([r.user for r in rows], type=pa.string()),
        "tags": _tags_array([r.tags for r in rows]),
        "members": pa.array(members, type=pa.list_(_MEMBER_TYPE)),
        "seq": pa.array([r.seq for r in rows], type=pa.int64()),
    })
