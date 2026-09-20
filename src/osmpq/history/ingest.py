"""History ingest layer, docs/m4-contracts.md section 4 (before "4.1").

Reads either ``--pbf`` (+ ``--osc``/``--osc.gz`` files) or a full-history
``--osh`` file into three DuckDB tables holding **every version** of every
element, then applies the M2 extent rule (docs/m2-contracts.md section 2,
generalized over time) to drop what was never in the dataset.

Deviation from the contract's literal "read the base PBF the way
``build/raw.py`` does (``ST_ReadOSM`` + ``osmium_read``, fast)": that path
joins structure (``ST_ReadOSM``) to metadata (the community ``osmium_read``
extension) by ``(type, id)``, and ``osmium_read`` only returns metadata for
*tagged* nodes (``build/raw.py``'s own docstring; ``docs/m0-contracts.md``
section 5) -- so an untagged node (the vast majority: ~54M of Minnesota's
~55M nodes) would get a NULL ``version``/``timestamp`` and therefore a NULL
``valid_from``, which is fatal here: a way's minor-version geometry
resolution picks "the node state with the greatest ``valid_from <= ts``"
(section 4.1.2), and a NULL ``valid_from`` never satisfies that comparison,
so the node would silently contribute nothing to *any* way's geometry at
*any* time -- corrupting the vast majority of way geometries throughout
history. Untagged-node metadata is exactly the M0/M1 gap the contract's own
"fast" note is echoing (M1 fixed it with the Rust reader; we don't have one
here). We use ``pyosmium`` uniformly instead: one ``FileProcessor`` pass per
input file gives full metadata (`version`, `timestamp`, `changeset`, `uid`,
`user`) for *every* element (tagged or not) at whatever throughput
benchmarking showed keeps Minnesota's base + diffs well inside the 30 minute
target (section 8) -- see ``docs/m4-report.md`` / the workstream report for
the numbers. This also means one code path serves ``--pbf`` (a plain
snapshot: every object comes back with ``deleted=False`` and its own
recorded ``version``, which *is* "the first state of every element" the
contract asks for -- the base extract need not literally be version 1),
``--osc``/``--osc.gz`` (one diff each; unlike
``osmpq.update.osc.parse_batch``, nothing is deduplicated per id here --
every version must survive) and ``--osh`` (already carries every version
with ``obj.deleted`` set).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import osmium
import pyarrow as pa

from osmpq.build import common

_ENTITIES = osmium.osm.NODE | osmium.osm.WAY | osmium.osm.RELATION

_MEMBER_TYPE = pa.struct([
    pa.field("type", pa.string()),
    pa.field("ref", pa.int64()),
    pa.field("role", pa.string()),
])

CHUNK_ROWS = 2_000_000


def _log(msg: str) -> None:
    common.log("osmpq history ingest", msg)


@dataclass
class IngestOptions:
    root: str
    pbf_path: Optional[str] = None
    osc_paths: Optional[list[str]] = None  # files and/or directories
    osh_path: Optional[str] = None


def _seq_key(path: Path) -> tuple[int, str]:
    """Sort key for an ``.osc``/``.osc.gz`` file: the leading run of digits
    in its name (replication sequence, as the cache names them -- e.g.
    ``7293720.osc.gz``), falling back to the name itself so an unrelated
    filename still sorts deterministically instead of erroring."""
    m = re.match(r"0*(\d+)", path.name)
    return (int(m.group(1)) if m else -1, path.name)


def _resolve_osc_files(osc_paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for p in osc_paths:
        pp = Path(p)
        if pp.is_dir():
            files.extend(sorted(
                (f for f in pp.iterdir() if f.name.endswith(".osc") or f.name.endswith(".osc.gz")),
                key=_seq_key,
            ))
        else:
            files.append(pp)
    files.sort(key=_seq_key)
    return files


# --------------------------------------------------------------------------
# chunked pyosmium -> DuckDB table accumulation
# --------------------------------------------------------------------------


class _ChunkWriter:
    """Buffers rows of one element type and flushes them to a DuckDB table
    in ``CHUNK_ROWS``-sized batches, so peak memory stays bounded regardless
    of how many nodes the input has (Minnesota: ~55M)."""

    def __init__(self, con, table: str, columns: list[str], types: dict[str, "pa.DataType"]):
        self.con = con
        self.table = table
        self.columns = columns
        self.types = types
        self.buf: dict[str, list] = {c: [] for c in columns}
        self.created = False
        self.n = 0

    def add(self, **row) -> None:
        for c in self.columns:
            self.buf[c].append(row[c])
        self.n += 1
        if len(self.buf[self.columns[0]]) >= CHUNK_ROWS:
            self.flush()

    def flush(self) -> None:
        if not self.buf[self.columns[0]]:
            return
        tbl = pa.table({c: pa.array(self.buf[c], type=self.types[c]) for c in self.columns})
        self.con.register("_chunk_arrow", tbl)
        if not self.created:
            self.con.execute(f"CREATE OR REPLACE TABLE {self.table} AS SELECT * FROM _chunk_arrow")
            self.created = True
        else:
            self.con.execute(f"INSERT INTO {self.table} SELECT * FROM _chunk_arrow")
        self.con.unregister("_chunk_arrow")
        for c in self.columns:
            self.buf[c] = []

    def finish(self) -> None:
        self.flush()
        if not self.created:
            empty = pa.table({c: pa.array([], type=self.types[c]) for c in self.columns})
            self.con.register("_chunk_arrow_empty", empty)
            self.con.execute(f"CREATE OR REPLACE TABLE {self.table} AS SELECT * FROM _chunk_arrow_empty")
            self.con.unregister("_chunk_arrow_empty")


_NODE_COLS = ["__ord", "id", "version", "timestamp", "visible", "changeset", "uid", "user", "tags", "lat_e7", "lon_e7"]
_WAY_COLS = ["__ord", "id", "version", "timestamp", "visible", "changeset", "uid", "user", "tags", "refs"]
_REL_COLS = ["__ord", "id", "version", "timestamp", "visible", "changeset", "uid", "user", "tags", "members"]

_NODE_TYPES = {
    "__ord": pa.int64(), "id": pa.int64(), "version": pa.int32(), "timestamp": pa.timestamp("us"),
    "visible": pa.bool_(), "changeset": pa.int64(), "uid": pa.int32(), "user": pa.string(),
    "tags": pa.map_(pa.string(), pa.string()), "lat_e7": pa.int32(), "lon_e7": pa.int32(),
}
_WAY_TYPES = {
    "__ord": pa.int64(), "id": pa.int64(), "version": pa.int32(), "timestamp": pa.timestamp("us"),
    "visible": pa.bool_(), "changeset": pa.int64(), "uid": pa.int32(), "user": pa.string(),
    "tags": pa.map_(pa.string(), pa.string()), "refs": pa.list_(pa.int64()),
}
_REL_TYPES = {
    "__ord": pa.int64(), "id": pa.int64(), "version": pa.int32(), "timestamp": pa.timestamp("us"),
    "visible": pa.bool_(), "changeset": pa.int64(), "uid": pa.int32(), "user": pa.string(),
    "tags": pa.map_(pa.string(), pa.string()), "members": pa.list_(_MEMBER_TYPE),
}


def _tags_of(obj) -> Optional[dict]:
    if obj.tags is None or len(obj.tags) == 0:
        return None
    return {t.k: t.v for t in obj.tags}


def _e7(x: float) -> int:
    return int(round(x * 1e7))


def _read_files_into(
    con, paths: list[Path], node_w: _ChunkWriter, way_w: _ChunkWriter, rel_w: _ChunkWriter,
) -> tuple[int, int]:
    """Returns ``(n_objects, base_ord_max)``: ``base_ord_max`` is the last
    ``__ord`` assigned while reading ``paths[0]`` (the base ``--pbf``/
    ``--osh`` file), so :func:`_dedup_and_extent_filter` can tell "the base
    file's own rows" apart from ``.osc``-sourced ones for computing
    ``since`` (docs/m4-contracts.md section 2.3) without a extra column in
    the final tables."""
    ordc = [0]
    n = 0
    base_ord_max = 0
    for path_i, path in enumerate(paths):
        fp = osmium.FileProcessor(str(path), _ENTITIES)
        for obj in fp:
            ordc[0] += 1
            n += 1
            visible = not bool(obj.deleted)
            ts = obj.timestamp
            if ts is not None and ts.tzinfo is not None:
                ts = ts.replace(tzinfo=None)
            if isinstance(obj, osmium.osm.Node):
                lat_e7 = lon_e7 = None
                tags = None
                if visible:
                    loc = obj.location
                    if loc is not None and loc.valid():
                        lat_e7, lon_e7 = _e7(loc.lat), _e7(loc.lon)
                    tags = _tags_of(obj)
                node_w.add(
                    __ord=ordc[0], id=obj.id, version=obj.version, timestamp=ts, visible=visible,
                    changeset=obj.changeset, uid=obj.uid, user=obj.user, tags=tags,
                    lat_e7=lat_e7, lon_e7=lon_e7,
                )
            elif isinstance(obj, osmium.osm.Way):
                refs = [nd.ref for nd in obj.nodes] if visible else None
                way_w.add(
                    __ord=ordc[0], id=obj.id, version=obj.version, timestamp=ts, visible=visible,
                    changeset=obj.changeset, uid=obj.uid, user=obj.user,
                    tags=_tags_of(obj) if visible else None, refs=refs,
                )
            elif isinstance(obj, osmium.osm.Relation):
                members = [{"type": m.type, "ref": m.ref, "role": m.role} for m in obj.members] if visible else None
                rel_w.add(
                    __ord=ordc[0], id=obj.id, version=obj.version, timestamp=ts, visible=visible,
                    changeset=obj.changeset, uid=obj.uid, user=obj.user,
                    tags=_tags_of(obj) if visible else None, members=members,
                )
        if path_i == 0:
            base_ord_max = ordc[0]
    return n, base_ord_max


def _dedup_and_extent_filter(
    con, extent_e7: tuple[int, int, int, int], base_ord_max: int, is_osh: bool,
) -> tuple[dict[str, int], "object"]:
    """Dedup by (id, version) keeping the last-seen (highest ``__ord``) row,
    then apply the extent rule (section 4, generalizing docs/m2-contracts.md
    section 2 over the whole timeline instead of one batch): a node version
    is kept if inside the extent or the id is already known by then; a way
    version if any ref is known by then or the way is already known; a
    relation version if any member is known by then or the relation is
    already known; a deletion if the id is known. "Known" is monotonic (an
    id, once known, stays known forever), so "is X known by time ts" reduces
    to a single ``MIN(timestamp) WHERE <own condition>`` per id, computed
    with a handful of set-based SQL passes -- no per-id loop."""
    south_e7, west_e7, north_e7, east_e7 = extent_e7

    con.execute("""
        CREATE OR REPLACE TEMP TABLE node_dedup AS
        SELECT * EXCLUDE (__rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY id, version ORDER BY __ord DESC) AS __rn
            FROM hist_node_all
        ) WHERE __rn = 1
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_dedup AS
        SELECT * EXCLUDE (__rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY id, version ORDER BY __ord DESC) AS __rn
            FROM hist_way_all
        ) WHERE __rn = 1
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE relation_dedup AS
        SELECT * EXCLUDE (__rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY id, version ORDER BY __ord DESC) AS __rn
            FROM hist_relation_all
        ) WHERE __rn = 1
    """)
    # Free the (larger, possibly duplicate-version-laden) raw tables now
    # that the deduplicated ones hold everything needed -- Minnesota-scale
    # runs OOM'd without this (docs/m4-contracts.md section 4, "Memory").
    con.execute("DROP TABLE hist_node_all")
    con.execute("DROP TABLE hist_way_all")
    con.execute("DROP TABLE hist_relation_all")

    # `since` (section 2.3): the earliest instant the history is complete
    # from. For `--osh`, that's the earliest state the file records at all
    # (a full-history extract's own limit). For `--pbf` (+ `--osc`), it's
    # the base file's own timestamp -- approximated, like `build/raw.py`'s
    # `max_timestamp`, as the latest element timestamp *within the base
    # file itself* (rows with `__ord <= base_ord_max`).
    if is_osh:
        since = con.execute("""
            SELECT min(x) FROM (
                SELECT min(timestamp) AS x FROM node_dedup
                UNION ALL SELECT min(timestamp) FROM way_dedup
                UNION ALL SELECT min(timestamp) FROM relation_dedup
            )
        """).fetchone()[0]
    else:
        since = con.execute(f"""
            SELECT max(x) FROM (
                SELECT max(timestamp) AS x FROM node_dedup WHERE __ord <= {base_ord_max}
                UNION ALL SELECT max(timestamp) FROM way_dedup WHERE __ord <= {base_ord_max}
                UNION ALL SELECT max(timestamp) FROM relation_dedup WHERE __ord <= {base_ord_max}
            )
        """).fetchone()[0]

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE node_first_known AS
        SELECT id, min(timestamp) AS first_ts FROM node_dedup
        WHERE visible AND lat_e7 IS NOT NULL AND lon_e7 IS NOT NULL
          AND lat_e7 BETWEEN {south_e7} AND {north_e7} AND lon_e7 BETWEEN {west_e7} AND {east_e7}
        GROUP BY id
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE hist_node_raw AS
        SELECT n.* EXCLUDE (__ord) FROM node_dedup n JOIN node_first_known f
          ON f.id = n.id AND n.timestamp >= f.first_ts
    """)
    con.execute("DROP TABLE node_dedup")

    # way: any ref known by this version's own timestamp, OR the way is
    # already known by an earlier version of its own.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW way_refs_flat AS
        SELECT w.id AS way_id, w.timestamp AS ts, t.ref
        FROM way_dedup w, UNNEST(w.refs) AS t(ref) WHERE w.visible
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_any_ref_known AS
        SELECT wf.way_id, wf.ts,
               bool_or(nf.first_ts IS NOT NULL AND nf.first_ts <= wf.ts) AS any_known
        FROM way_refs_flat wf LEFT JOIN node_first_known nf ON nf.id = wf.ref
        GROUP BY wf.way_id, wf.ts
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_first_known AS
        SELECT way_id AS id, min(ts) AS first_ts FROM way_any_ref_known WHERE any_known GROUP BY way_id
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE hist_way_raw AS
        SELECT w.* EXCLUDE (__ord) FROM way_dedup w JOIN way_first_known f
          ON f.id = w.id AND w.timestamp >= f.first_ts
    """)
    con.execute("DROP TABLE way_dedup")
    con.execute("DROP TABLE way_any_ref_known")

    # relation: node_first_known ∪ way_first_known cover most cases; one
    # extra pass lets a relation-typed member's own first_known feed a
    # parent relation (one level of nesting, matching the base builder's
    # own one-level nested-relation handling for bbox -- section 4.1.3).
    def _relation_first_known(extra_rel_known) -> None:
        pieces = [
            "SELECT rm.rel_id, rm.ts, (nf.first_ts IS NOT NULL AND nf.first_ts <= rm.ts) AS known "
            "FROM rel_members_n rm LEFT JOIN node_first_known nf ON nf.id = rm.ref",
            "SELECT rm.rel_id, rm.ts, (wf.first_ts IS NOT NULL AND wf.first_ts <= rm.ts) AS known "
            "FROM rel_members_w rm LEFT JOIN way_first_known wf ON wf.id = rm.ref",
        ]
        if extra_rel_known is not None:
            pieces.append(
                f"SELECT rm.rel_id, rm.ts, (rf.first_ts IS NOT NULL AND rf.first_ts <= rm.ts) AS known "
                f"FROM rel_members_r rm LEFT JOIN {extra_rel_known} rf ON rf.id = rm.ref"
            )
        union_sql = " UNION ALL ".join(pieces)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE rel_any_member_known AS
            SELECT rel_id, ts, bool_or(known) AS any_known FROM ({union_sql}) u GROUP BY rel_id, ts
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE rel_first_known_next AS
            SELECT rel_id AS id, min(ts) AS first_ts FROM rel_any_member_known WHERE any_known GROUP BY rel_id
        """)

    con.execute("""
        CREATE OR REPLACE TEMP VIEW rel_members_n AS
        SELECT r.id AS rel_id, r.timestamp AS ts, m.ref AS ref
        FROM relation_dedup r, UNNEST(r.members) AS t(m) WHERE r.visible AND m.type = 'n'
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW rel_members_w AS
        SELECT r.id AS rel_id, r.timestamp AS ts, m.ref AS ref
        FROM relation_dedup r, UNNEST(r.members) AS t(m) WHERE r.visible AND m.type = 'w'
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW rel_members_r AS
        SELECT r.id AS rel_id, r.timestamp AS ts, m.ref AS ref
        FROM relation_dedup r, UNNEST(r.members) AS t(m) WHERE r.visible AND m.type = 'r'
    """)
    _relation_first_known(None)
    con.execute("CREATE OR REPLACE TEMP TABLE relation_first_known AS SELECT * FROM rel_first_known_next")
    _relation_first_known("relation_first_known")
    con.execute("CREATE OR REPLACE TEMP TABLE relation_first_known AS SELECT * FROM rel_first_known_next")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE hist_relation_raw AS
        SELECT r.* EXCLUDE (__ord) FROM relation_dedup r JOIN relation_first_known f
          ON f.id = r.id AND r.timestamp >= f.first_ts
    """)
    con.execute("DROP TABLE relation_dedup")
    con.execute("DROP TABLE rel_any_member_known")
    con.execute("DROP TABLE rel_first_known_next")

    counts = {
        t: con.execute(f"SELECT count(*) FROM hist_{t}_raw").fetchone()[0]
        for t in ("node", "way", "relation")
    }
    return counts, since


def ingest_history(con, opts: IngestOptions, extent: tuple[float, float, float, float]) -> tuple[dict[str, int], object]:
    """Populate ``hist_node_raw``/``hist_way_raw``/``hist_relation_raw`` in
    ``con`` (every kept version; see the module docstring and
    :func:`_dedup_and_extent_filter`). Returns ``(counts, since)``."""
    south, west, north, east = extent
    extent_e7 = (round(south * 1e7), round(west * 1e7), round(north * 1e7), round(east * 1e7))

    node_w = _ChunkWriter(con, "hist_node_all", _NODE_COLS, _NODE_TYPES)
    way_w = _ChunkWriter(con, "hist_way_all", _WAY_COLS, _WAY_TYPES)
    rel_w = _ChunkWriter(con, "hist_relation_all", _REL_COLS, _REL_TYPES)

    is_osh = bool(opts.osh_path)
    if is_osh:
        paths = [Path(opts.osh_path)]
        _log(f"reading full-history file {opts.osh_path} ...")
    else:
        if not opts.pbf_path:
            raise ValueError("history ingest needs --pbf (+ optional --osc) or --osh")
        paths = [Path(opts.pbf_path)]
        if opts.osc_paths:
            osc_files = _resolve_osc_files(opts.osc_paths)
            _log(f"{len(osc_files)} .osc file(s) after --pbf, in sequence order")
            paths.extend(osc_files)

    n, base_ord_max = _read_files_into(con, paths, node_w, way_w, rel_w)
    node_w.finish()
    way_w.finish()
    rel_w.finish()
    _log(f"read {n} object-versions from {len(paths)} file(s)")

    counts, since = _dedup_and_extent_filter(con, extent_e7, base_ord_max, is_osh)
    _log(f"kept after extent filter: {counts}, since={since}")
    return counts, since
