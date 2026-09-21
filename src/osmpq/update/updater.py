"""The stateless minutely updater, docs/m2-contracts.md section 5.

Everything a run needs comes from the dataset root (manifest + byid/index
Parquet files, read by range/batch queries) and the replication source;
``--tmpdir`` holds a scratch DuckDB database plus the downloaded
``.osc.gz``/``.state.txt`` cache (``osmpq.update.replication``), and the
scratch database is deleted at the end of the run.

Implementation notes for the engine (delta reads) and compaction authors:

- Column order/types of every delta file = the corresponding base
  byid/spatial schema (see ``osmpq.build.raw``/``osmpq.build.builder`` and
  docs/m0-contracts.md section 4) with ``deleted BOOLEAN, prev_cell VARCHAR,
  seq BIGINT`` appended at the end. Spatial delta files additionally carry
  an explicit ``cell`` column (the base spatial files don't need one --
  it's the Hive partition directory -- but a delta spatial file holds many
  cells in one file).
- For a ``deleted`` row, **every** payload column is NULL (tags, refs /
  members, bbox, geometry, centroid, version/changeset/timestamp/uid/user,
  hilbert) except ``id, deleted, prev_cell, seq`` and ``cell`` (spatial
  rows only), which is set to ``prev_cell`` so a query on the old cell
  finds the tombstone.
- ``prev_cell`` is first-write-wins *within* a tier: once an id has a
  ``prev_cell`` recorded in a tier (possibly NULL, meaning "new in this
  generation"), later batches merged into that tier keep it even though
  the rest of the row is overwritten with the newest data.
- Tier versions are a plain increasing integer per tier
  (``manifest.deltas[tier]["version"]``), starting at 1 the first time a
  tier is written; files live at ``delta/<generation>/<tier>/<version>/``.
  ``hour`` is rewritten every run; ``day``/``week`` only on an hour/day UTC
  boundary crossing, per the fold rule below -- untouched tiers keep
  whatever version/paths they already had in the manifest.
- ``seq`` on a directly-changed element is the specific replication
  sequence of its last occurrence within the batch (from ``osmpq.update.osc``).
  On an element that is only *cascaded* into this run (e.g. a way
  re-resolved because one of its nodes moved, but the way itself wasn't in
  any ``.osc``), ``seq`` is the batch's last applied sequence -- there is
  no more precise single sequence to attribute a multi-node bbox change to.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import numpy as np

from osmpq import store as store_mod
from osmpq.build import common
from osmpq.history import schema as history_schema
from osmpq.layout import cells as cells_mod
from osmpq.layout import hilbert as hilbert_mod
from osmpq.layout import manifest as manifest_mod
from osmpq.update import osc as osc_mod
from osmpq.update.replication import ReplicationClient

TIERS = ("hour", "day", "week")
_TYPES = ("node", "way", "relation")


def _log(msg: str) -> None:
    common.log("osmpq update", msg)


# --------------------------------------------------------------------------
# options / summary
# --------------------------------------------------------------------------


@dataclass
class UpdateOptions:
    root: str
    source: Optional[str] = None
    max_diffs: int = 60
    tmpdir: Optional[str] = None
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    follow: bool = False
    poll_interval: float = 30.0


@dataclass
class RunSummary:
    applied: int
    first_seq: Optional[int]
    last_seq: Optional[int]
    timestamp: Optional[str]
    rows_touched: dict[str, int] = field(default_factory=dict)
    tier_versions: dict[str, int] = field(default_factory=dict)
    tier_bytes: dict[str, int] = field(default_factory=dict)
    dropped: dict[str, int] = field(default_factory=dict)
    seconds: float = 0.0
    no_op: bool = False
    # docs/m4-contracts.md section 5.1: empty on a root without history, or
    # on a run whose history batch was empty ("a run that touches nothing
    # writes no tier file").
    history_tier_versions: dict[str, int] = field(default_factory=dict)
    history_tier_bytes: dict[str, int] = field(default_factory=dict)


def _print_summary(s: RunSummary) -> None:
    if s.no_op:
        _log("no new diffs available")
        return
    rows = " ".join(f"{k}={v}" for k, v in s.rows_touched.items())
    tiers = " ".join(f"{k}=v{v}" for k, v in s.tier_versions.items())
    hist = " ".join(f"{k}=v{v}" for k, v in s.history_tier_versions.items())
    msg = (
        f"applied seq {s.first_seq}..{s.last_seq} ({s.applied} diffs) ts={s.timestamp} "
        f"touched[{rows}] tiers[{tiers}] dropped={s.dropped} in {s.seconds:.1f}s"
    )
    if hist:
        msg += f" history_tiers[{hist}]"
    _log(msg)


# --------------------------------------------------------------------------
# column schemas (docs/m2-contracts.md section 3)
# --------------------------------------------------------------------------


def _promoted_refs(promoted_keys: list[str]) -> list[str]:
    return [f'"{k}"' for k in promoted_keys]


def _meta_cols() -> list[str]:
    return ["version", "changeset", "timestamp", "uid", '"user"']


def node_byid_columns(promoted_keys: list[str]) -> list[str]:
    return ["id", "lat_e7", "lon_e7", "tags", *_promoted_refs(promoted_keys), *_meta_cols(), "cell", "hilbert"]


def way_byid_columns(promoted_keys: list[str]) -> list[str]:
    return [
        "id", "refs", "tags", *_promoted_refs(promoted_keys), *_meta_cols(),
        "xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7", "is_closed", "is_area", "cell", "hilbert",
    ]


def relation_byid_columns(promoted_keys: list[str]) -> list[str]:
    return [
        "id", "members", "tags", *_promoted_refs(promoted_keys), *_meta_cols(),
        "xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7", "cell",
    ]


def node_spatial_columns(promoted_keys: list[str]) -> list[str]:
    return ["id", "lat_e7", "lon_e7", "tags", *_promoted_refs(promoted_keys), *_meta_cols(), "hilbert", "cell"]


def way_spatial_columns(promoted_keys: list[str]) -> list[str]:
    return [
        "id", "refs", "tags", *_promoted_refs(promoted_keys), *_meta_cols(),
        "xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7", "geometry", "is_closed", "is_area",
        "centroid_lat_e7", "centroid_lon_e7", "cell", "hilbert",
    ]


def relation_spatial_columns(promoted_keys: list[str]) -> list[str]:
    return [
        "id", "members", "tags", *_promoted_refs(promoted_keys), *_meta_cols(),
        "xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7", "geometry",
        "centroid_lat_e7", "centroid_lon_e7", "cell", "hilbert",
    ]


BYID_COLUMNS = {"node": node_byid_columns, "way": way_byid_columns, "relation": relation_byid_columns}
SPATIAL_COLUMNS = {"node": node_spatial_columns, "way": way_spatial_columns, "relation": relation_spatial_columns}
_DELTA_EXTRA = ["deleted", "prev_cell", "seq"]

# id column used by the base node_way index (node_id), not needed elsewhere.


def delta_byid_columns(typ: str, promoted_keys: list[str]) -> list[str]:
    return BYID_COLUMNS[typ](promoted_keys) + _DELTA_EXTRA


def delta_spatial_columns(typ: str, promoted_keys: list[str]) -> list[str]:
    return SPATIAL_COLUMNS[typ](promoted_keys) + _DELTA_EXTRA


# --------------------------------------------------------------------------
# small SQL/id-set helpers
# --------------------------------------------------------------------------


def _esc(p: str) -> str:
    return str(p).replace("'", "''")


def _select_parts(parts: list[dict], lo: int, hi: int) -> list[str]:
    """Manifest byid/index parts whose ``[min_id, max_id]`` overlaps ``[lo,hi]``."""
    out = []
    for p in parts:
        mn, mx = p.get("min_id"), p.get("max_id")
        if mn is None or mx is None:
            continue
        if mx < lo or mn > hi:
            continue
        out.append(p["path"])
    return out


def _register_ids(con, name: str, ids) -> int:
    """Register a distinct id set as TEMP TABLE ``name(id BIGINT)``. Returns
    the row count."""
    import pyarrow as pa

    arr = np.asarray(list(ids), dtype=np.int64) if not isinstance(ids, np.ndarray) else ids.astype(np.int64)
    tbl = pa.table({"id": pa.array(arr, type=pa.int64())})
    con.register("_ids_arrow", tbl)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT DISTINCT id FROM _ids_arrow")
    con.unregister("_ids_arrow")
    return con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]


def _union_ids_sql(con, *table_names: str) -> np.ndarray:
    parts = " UNION ".join(f"SELECT id FROM {t}" for t in table_names if t)
    if not parts:
        return np.array([], dtype=np.int64)
    rows = con.execute(f"SELECT id FROM ({parts}) t").fetchnumpy()["id"]
    return np.asarray(rows, dtype=np.int64)


# --------------------------------------------------------------------------
# the run
# --------------------------------------------------------------------------


def run(opts: UpdateOptions) -> None:
    if not opts.follow:
        summary = run_once(opts)
        if summary is not None:
            _print_summary(summary)
        return
    while True:
        summary = run_once(opts)
        if summary is not None:
            _print_summary(summary)
        time.sleep(opts.poll_interval)


def run_once(opts: UpdateOptions) -> Optional[RunSummary]:
    t0 = time.time()
    store = store_mod.for_root(opts.root)
    is_remote = store_mod.is_remote_root(opts.root)
    man = manifest_mod.load_latest(opts.root)
    source = opts.source or man.replication_source
    if not source:
        raise ValueError("no replication source: pass --source (manifest has none yet)")
    promoted_keys = list(man.promoted_keys)
    ancestor_depths = list(man.ancestor_depths or cells_mod.DEFAULT_ANCESTOR_DEPTHS)
    max_depth = int(man.max_depth or cells_mod.DEFAULT_MAX_DEPTH_V2)
    leaf_index = cells_mod.LeafIndex(man.leaf_cells)
    south, west, north, east = man.extent
    south_e7, west_e7, north_e7, east_e7 = (
        round(south * 1e7), round(west * 1e7), round(north * 1e7), round(east * 1e7),
    )

    tmpdir = Path(opts.tmpdir) if opts.tmpdir else Path.cwd() / ".osmpq-update-tmp"
    tmpdir.mkdir(parents=True, exist_ok=True)
    osc_cache = tmpdir / "osc-cache"

    # contract section 6.2: on a local root, new tier files are written
    # straight into their final place under the root, exactly as before
    # (byte-for-byte unchanged -- `write_root` *is* the root). `Path(opts.
    # root)` isn't a usable filesystem path at all for an `s3://` root, so
    # there `write_root` is a local staging directory instead; once
    # `_write_tiers` has written this run's new tier files there, they're
    # uploaded to `store` (`_upload_new_tier_files`) before the manifest is
    # written (which always goes through `manifest_mod.write_manifest`,
    # itself store-backed for either root kind).
    if is_remote:
        write_root = tmpdir / "staging"
        write_root.mkdir(parents=True, exist_ok=True)
    else:
        write_root = Path(opts.root)

    from_seq = (man.replication_sequence or 0) + 1

    with ReplicationClient(source) as client:
        fetched = client.fetch_range(from_seq, opts.max_diffs, osc_cache)

    if not fetched:
        return RunSummary(applied=0, first_seq=None, last_seq=None, timestamp=None, no_op=True, seconds=time.time() - t0)

    seq_files = [(r.seq, r.osc_path) for r in fetched]
    batch = osc_mod.parse_batch(seq_files)
    last_state_ts = fetched[-1].timestamp  # e.g. "2026-09-19T00:23:06Z", authoritative
    batch_timestamp = last_state_ts or _max_element_timestamp(batch)

    db_path = tmpdir / "updater.duckdb"
    if db_path.exists():
        db_path.unlink()
    import duckdb

    con = duckdb.connect(str(db_path))
    con.execute("SET TimeZone='UTC'")
    if opts.threads:
        con.execute(f"SET threads={int(opts.threads)}")
    if opts.memory_limit:
        con.execute(f"SET memory_limit='{opts.memory_limit}'")
    con.execute(f"SET temp_directory='{tmpdir.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    if is_remote:
        # Same S3 secret the query engine issues (docs/m3-contracts.md
        # section 6.2: `store.s3_secret_sql`, shared with
        # `osmpq.engine.executor`), so `read_parquet(store.url(...))` below
        # can reach the bucket.
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        secret_sql = store_mod.s3_secret_sql()
        if secret_sql:
            con.execute(secret_sql)

    try:
        result = _run_once_impl(
            con, write_root, store, is_remote, man, promoted_keys, ancestor_depths, max_depth, leaf_index,
            (south_e7, west_e7, north_e7, east_e7), batch, fetched, batch_timestamp, opts,
        )
    finally:
        con.close()
        if db_path.exists():
            db_path.unlink()

    result.seconds = time.time() - t0
    return result


def _max_element_timestamp(batch: osc_mod.BatchResult) -> Optional[str]:
    ts = None
    for tbl in (batch.node, batch.way, batch.relation):
        if tbl.num_rows == 0:
            continue
        col = tbl.column("timestamp")
        mx = max(v.as_py() for v in col if v.is_valid)
        if ts is None or mx > ts:
            ts = mx
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ") if ts is not None else None


# --------------------------------------------------------------------------
# main body (kept out of run_once so the connection setup above stays terse)
# --------------------------------------------------------------------------


def _run_once_impl(
    con, root: Path, store, is_remote: bool, man: manifest_mod.Manifest, promoted_keys: list[str],
    ancestor_depths: list[int], max_depth: int, leaf_index: cells_mod.LeafIndex,
    extent_e7: tuple[int, int, int, int], batch: osc_mod.BatchResult,
    fetched: list, batch_timestamp: Optional[str], opts: UpdateOptions,
) -> RunSummary:
    south_e7, west_e7, north_e7, east_e7 = extent_e7
    last_seq = fetched[-1].seq
    first_seq = fetched[0].seq

    con.register("batch_node_raw", batch.node)
    con.register("batch_way_raw", batch.way)
    con.register("batch_relation_raw", batch.relation)
    # docs/m4-contracts.md section 5.1: every version's own meta row, kept
    # regardless of whether history is enabled on this root (cheap; the
    # tables are the same size class as the deduplicated ones above).
    con.register("batch_node_all_raw", batch.node_all)
    con.register("batch_way_all_raw", batch.way_all)
    con.register("batch_relation_all_raw", batch.relation_all)

    # ---- step 1: load current delta tiers (byid ⊕ tombstones) -----------------
    tier_state = _load_tiers(con, store, man, promoted_keys)
    _build_delta_indexes(con, tier_state)

    # ---- correctness rule: drop rows whose version is not newer ----------------
    _drop_stale_versions(con, tier_state, store, man)

    # ---- step 3: extent filter --------------------------------------------------
    kept, dropped_counts = _extent_filter(con, tier_state, store, man, promoted_keys, south_e7, west_e7, north_e7, east_e7)

    # ---- step 4: touched set -----------------------------------------------------
    touched_way_ids, touched_relation_ids = _touched_set(con, store, man, tier_state, kept)

    # ---- step 5/6: fetch current state + node coords, re-resolve ---------------
    resolved = _resolve(
        con, store, man, promoted_keys, tier_state, kept, touched_way_ids, touched_relation_ids,
        leaf_index, ancestor_depths, max_depth, last_seq,
    )

    rows_touched = {t: con.execute(f"SELECT count(*) FROM {resolved[t]}").fetchone()[0] for t in _TYPES}

    # ---- step 7: rolling tiers ------------------------------------------------
    # `root` is the local write target for freshly-written Parquet files:
    # the real dataset root for a local `Store`, or a scratch staging
    # directory for an `s3://` one (`run_once`) -- `_write_tiers`/
    # `_write_tier_version` always write local files with DuckDB's `COPY`,
    # identically on both.
    tier_versions, tier_bytes, new_deltas = _write_tiers(
        con, root, store, man, promoted_keys, tier_state, resolved, first_seq, last_seq, batch_timestamp,
    )
    if is_remote:
        # Those files only exist in the local staging dir so far (contract
        # 6.2: "tier files are written to the local tmpdir and uploaded
        # with upload_file"); push the ones this run actually (re)wrote.
        _upload_new_tier_files(store, root, new_deltas, tier_versions)

    # ---- history tiers (docs/m4-contracts.md section 5.1) ---------------------
    new_history = man.history
    history_tier_versions: dict[str, int] = {}
    history_tier_bytes: dict[str, int] = {}
    if man.history is not None:
        has_history_rows = _build_history_batch(
            con, store, man, promoted_keys, kept, touched_way_ids, touched_relation_ids, batch_timestamp,
        )
        if has_history_rows:
            hour_crossed, day_crossed = _tier_crossings(man.deltas, batch_timestamp)
            batch_tables = {t: (f"history_batch_byid_{t}", f"history_batch_spatial_{t}") for t in _TYPES}
            history_tier_versions, history_tier_bytes, new_history = _write_history_tiers(
                con, root, store, man, promoted_keys, batch_tables, first_seq, last_seq, batch_timestamp,
                hour_crossed, day_crossed,
            )
            if is_remote:
                _upload_new_history_tier_files(store, root, new_history, history_tier_versions)

    # ---- step 8: manifest -----------------------------------------------------------
    source = opts.source or man.replication_source
    new_manifest_version = max(3, man.manifest_version)
    if new_history is not None:
        new_manifest_version = max(new_manifest_version, 5)
    new_man = manifest_mod.Manifest(
        generation=man.generation,
        timestamp_osm_base=batch_timestamp or man.timestamp_osm_base,
        source=man.source,
        extent=man.extent,
        leaf_cells=man.leaf_cells,
        tables=man.tables,
        byid=man.byid,
        index=man.index,
        promoted_keys=man.promoted_keys,
        replication_sequence=last_seq,
        manifest_version=new_manifest_version,
        schema_version=man.schema_version,
        coordinate_scale=man.coordinate_scale,
        ancestor_depths=man.ancestor_depths or list(cells_mod.DEFAULT_ANCESTOR_DEPTHS),
        max_depth=man.max_depth or cells_mod.DEFAULT_MAX_DEPTH_V2,
        rowgroup_index=man.rowgroup_index,
        producer=man.producer,
        stats=man.stats,
        replication_source=source,
        deltas=new_deltas,
        history=new_history,
        areas=man.areas,
    )
    # `opts.root`, not `root` (the local write target above, a staging dir
    # for `s3://`): this is the actual root string, and
    # `manifest_mod.write_manifest`/`next_manifest_number` are store-backed
    # for either root kind (contract section 6.2).
    gen_number = manifest_mod.next_manifest_number(opts.root)
    manifest_mod.write_manifest(opts.root, new_man, gen_number)

    return RunSummary(
        applied=len(fetched), first_seq=first_seq, last_seq=last_seq, timestamp=batch_timestamp,
        rows_touched=rows_touched, tier_versions=tier_versions, tier_bytes=tier_bytes, dropped=dropped_counts,
        history_tier_versions=history_tier_versions, history_tier_bytes=history_tier_bytes,
    )


def _upload_new_tier_files(store, write_root: Path, new_deltas: dict, tier_versions: dict) -> None:
    """After `_write_tiers` writes this run's freshly-created tier files
    under `write_root` (an s3:// root's local staging dir -- see
    `run_once`), upload each one to `store` at its manifest-relative path.
    Only tiers this run actually (re)wrote (`tier_versions`'s keys) have
    new files to push; an untouched tier keeps whatever's already at its
    old paths in `store`."""
    for tier_name in tier_versions:
        files = (new_deltas.get(tier_name) or {}).get("files", {})
        for typ, entry in files.items():
            if typ == "tombstones":
                if entry:
                    store.upload_file(str(write_root / entry), entry)
                continue
            for _kind, relpath in (entry or {}).items():
                if relpath:
                    store.upload_file(str(write_root / relpath), relpath)


# --------------------------------------------------------------------------
# step 1: load tiers, build delta indexes
# --------------------------------------------------------------------------


@dataclass
class TierState:
    # per type: name of a TEMP VIEW with delta_byid_columns(typ), one row
    # per (type,id) across all present tiers (highest tier rank wins),
    # INCLUDING deleted rows.
    raw_view: dict[str, str] = field(default_factory=dict)
    # per type: name of a TEMP VIEW = raw_view filtered to NOT deleted, with
    # the *_byid_columns(typ) (base schema, no deleted/prev_cell/seq) --
    # this is "the delta layer" for current-state lookups.
    alive_view: dict[str, str] = field(default_factory=dict)
    present_tiers: dict[str, dict] = field(default_factory=dict)  # tier -> man.deltas[tier] (or {})


# Column name -> DuckDB type, covering every column any base/delta/history
# row can have (a superset -- callers only ever index the columns they
# actually use). Shared by ``_empty_delta_table_sql`` (delta byid columns
# only) and the history-row NULL projections in the section-5.1 code below
# (which also need ``geometry``/``centroid_*`` and the four history-extra
# columns, docs/m4-contracts.md section 2.1).
_BASE_TYPE_MAP: dict[str, str] = {
    "id": "BIGINT", "lat_e7": "INTEGER", "lon_e7": "INTEGER", "tags": "MAP(VARCHAR,VARCHAR)",
    "refs": "BIGINT[]", "members": 'STRUCT("type" VARCHAR, ref BIGINT, role VARCHAR)[]',
    "version": "INTEGER", "changeset": "BIGINT", "timestamp": "TIMESTAMP", "uid": "INTEGER",
    '"user"': "VARCHAR", "xmin_e7": "INTEGER", "ymin_e7": "INTEGER", "xmax_e7": "INTEGER",
    "ymax_e7": "INTEGER", "is_closed": "BOOLEAN", "is_area": "BOOLEAN", "cell": "VARCHAR",
    "hilbert": "UBIGINT", "deleted": "BOOLEAN", "prev_cell": "VARCHAR", "seq": "BIGINT",
    "geometry": "GEOMETRY", "centroid_lat_e7": "INTEGER", "centroid_lon_e7": "INTEGER",
    "minor": "INTEGER", "valid_from": "TIMESTAMP", "valid_to": "TIMESTAMP", "visible": "BOOLEAN",
}


def _type_map_with_promoted(promoted_keys: list[str]) -> dict[str, str]:
    m = dict(_BASE_TYPE_MAP)
    for k in promoted_keys:
        m[f'"{k}"'] = "VARCHAR"
    return m


def _empty_delta_table_sql(typ: str, promoted_keys: list[str]) -> str:
    cols = delta_byid_columns(typ, promoted_keys)
    type_map = _type_map_with_promoted(promoted_keys)
    select = ", ".join(f"NULL::{type_map[c]} AS {c}" for c in cols)
    return f"SELECT {select} WHERE FALSE"


def _load_tiers(con, store, man: manifest_mod.Manifest, promoted_keys: list[str]) -> TierState:
    state = TierState()
    for typ in _TYPES:
        pieces = []
        for tier in TIERS:
            entry = man.deltas.get(tier) if man.deltas else None
            if not entry:
                continue
            fpath = entry.get("files", {}).get(typ, {}).get("byid")
            if not fpath:
                continue
            if not store.exists(fpath):
                continue
            cols = delta_byid_columns(typ, promoted_keys)
            col_sql = ", ".join(cols)
            rank = {"hour": 3, "day": 2, "week": 1}[tier]
            pieces.append(f"SELECT {col_sql}, {rank} AS __tier_rank FROM read_parquet('{_esc(store.url(fpath))}')")
            state.present_tiers[tier] = entry
        raw_name = f"delta_raw_{typ}"
        if pieces:
            union_sql = " UNION ALL ".join(pieces)
            con.execute(f"""
                CREATE OR REPLACE TEMP VIEW {raw_name} AS
                SELECT * EXCLUDE (__tier_rank) FROM (
                    SELECT *, row_number() OVER (PARTITION BY id ORDER BY __tier_rank DESC) AS __rn
                    FROM ({union_sql}) u
                ) WHERE __rn = 1
            """)
        else:
            con.execute(f"CREATE OR REPLACE TEMP VIEW {raw_name} AS {_empty_delta_table_sql(typ, promoted_keys)}")
        state.raw_view[typ] = raw_name

        alive_name = f"delta_alive_{typ}"
        base_cols = BYID_COLUMNS[typ](promoted_keys)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW {alive_name} AS
            SELECT {", ".join(base_cols)} FROM {raw_name} WHERE NOT deleted
        """)
        state.alive_view[typ] = alive_name
    return state


def _build_delta_indexes(con, state: TierState) -> None:
    """In-memory inverted indexes over delta ways'/relations' current
    (alive) refs/members (docs/m2-contracts.md section 5 step 1)."""
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW delta_node_way AS
        SELECT DISTINCT unnest(refs) AS node_id, id AS way_id FROM {state.alive_view['way']}
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW delta_member AS
        SELECT DISTINCT m.type AS member_type, m.ref AS member_id, id AS parent_id
        FROM {state.alive_view['relation']}, UNNEST(members) AS t(m)
    """)


# --------------------------------------------------------------------------
# current-state fetch: delta (alive) ⊕ base byid parts, batched by id range
# --------------------------------------------------------------------------


def _fetch_current(con, store, typ: str, man: manifest_mod.Manifest, promoted_keys: list[str],
                    state: TierState, ids_table: str, out_table: str) -> None:
    """``out_table`` <- current alive effective row (delta shadows base) for
    every id in ``ids_table`` that currently exists. Columns =
    ``*_byid_columns(typ)`` (includes ``cell`` and, for node/way, ``hilbert``)."""
    cols = BYID_COLUMNS[typ](promoted_keys)
    col_sql = ", ".join(cols)
    n, lo, hi = con.execute(f"SELECT count(*), min(id), max(id) FROM {ids_table}").fetchone()
    if n == 0:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {out_table} AS SELECT {col_sql} FROM {state.alive_view[typ]} WHERE FALSE")
        return
    parts = _select_parts(man.byid.get(typ, []), lo, hi)
    if parts:
        paths = [store.url(p) for p in parts]
        base_sql = (
            f"SELECT {col_sql} FROM read_parquet({paths!r}) b "
            f"WHERE b.id IN (SELECT id FROM {ids_table}) AND b.id NOT IN (SELECT id FROM {state.raw_view[typ]})"
        )
    else:
        base_sql = f"SELECT {col_sql} FROM {state.alive_view[typ]} WHERE FALSE"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {out_table} AS
        SELECT {col_sql} FROM {state.alive_view[typ]} d WHERE d.id IN (SELECT id FROM {ids_table})
        UNION ALL
        {base_sql}
    """)


# --------------------------------------------------------------------------
# correctness rule: drop stale (already-applied) element versions
# --------------------------------------------------------------------------


def _drop_stale_versions(con, state: TierState, store, man: manifest_mod.Manifest) -> None:
    for typ in _TYPES:
        raw = f"batch_{typ}_raw"
        n = con.execute(f"SELECT count(*) FROM {raw}").fetchone()[0]
        _register_ids(con, f"__ver_ids_{typ}", con.execute(f"SELECT id FROM {raw}").fetchnumpy()["id"] if n else np.array([], dtype=np.int64))
        prior = f"__ver_prior_{typ}"
        _fetch_current(con, store, typ, man, man.promoted_keys, state, f"__ver_ids_{typ}", prior)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE batch_{typ} AS
            SELECT b.* FROM {raw} b LEFT JOIN {prior} p ON p.id = b.id
            WHERE p.id IS NULL OR b.version > p.version
        """)


# --------------------------------------------------------------------------
# step 3: extent filter (docs/m2-contracts.md section 2)
# --------------------------------------------------------------------------


def _member_ids(con, table: str, mtype: str) -> str:
    """Registers/returns a TEMP VIEW of distinct member ids of ``mtype``
    ('n'/'w'/'r') across all rows of a batch relation table."""
    name = f"__members_{table}_{mtype}"
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {name} AS
        SELECT DISTINCT m.ref AS id FROM {table}, UNNEST(members) AS t(m) WHERE m.type = '{mtype}'
    """)
    return name


def _extent_filter(con, state: TierState, store, man: manifest_mod.Manifest, promoted_keys: list[str],
                    south_e7: int, west_e7: int, north_e7: int, east_e7: int) -> tuple[dict[str, str], dict[str, int]]:
    # ids needed for existence ("known") checks, per type
    mrel_n = _member_ids(con, "batch_relation", "n")
    mrel_w = _member_ids(con, "batch_relation", "w")
    mrel_r = _member_ids(con, "batch_relation", "r")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW __node_ids_check AS
        SELECT id FROM batch_node
        UNION SELECT unnest(refs) AS id FROM batch_way
        UNION SELECT id FROM {mrel_n}
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW __way_ids_check AS
        SELECT id FROM batch_way UNION SELECT id FROM {mrel_w}
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW __rel_ids_check AS
        SELECT id FROM batch_relation UNION SELECT id FROM {mrel_r}
    """)
    _register_ids(con, "__node_ids_check_t", con.execute("SELECT id FROM __node_ids_check").fetchnumpy()["id"])
    _register_ids(con, "__way_ids_check_t", con.execute("SELECT id FROM __way_ids_check").fetchnumpy()["id"])
    _register_ids(con, "__rel_ids_check_t", con.execute("SELECT id FROM __rel_ids_check").fetchnumpy()["id"])

    _fetch_current(con, store, "node", man, promoted_keys, state, "__node_ids_check_t", "exists_node")
    _fetch_current(con, store, "way", man, promoted_keys, state, "__way_ids_check_t", "exists_way")
    _fetch_current(con, store, "relation", man, promoted_keys, state, "__rel_ids_check_t", "exists_relation")

    # kept_node: id_in_exists_before OR (NOT deleted AND inside extent)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE kept_node AS
        SELECT b.* FROM batch_node b
        WHERE b.id IN (SELECT id FROM exists_node)
           OR (NOT b.deleted AND b.lat_e7 IS NOT NULL AND b.lon_e7 IS NOT NULL
               AND b.lat_e7 BETWEEN {south_e7} AND {north_e7} AND b.lon_e7 BETWEEN {west_e7} AND {east_e7})
    """)
    n_node_dropped = con.execute("SELECT count(*) FROM batch_node").fetchone()[0] - con.execute("SELECT count(*) FROM kept_node").fetchone()[0]

    con.execute("""
        CREATE OR REPLACE TEMP VIEW known_node_ids AS
        SELECT id FROM exists_node UNION SELECT id FROM kept_node
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE kept_way AS
        SELECT b.* FROM batch_way b
        WHERE b.id IN (SELECT id FROM exists_way)
           OR (NOT b.deleted AND EXISTS (
                 SELECT 1 FROM UNNEST(b.refs) AS t(ref) WHERE ref IN (SELECT id FROM known_node_ids)
              ))
    """)
    n_way_dropped = con.execute("SELECT count(*) FROM batch_way").fetchone()[0] - con.execute("SELECT count(*) FROM kept_way").fetchone()[0]

    con.execute("""
        CREATE OR REPLACE TEMP TABLE kept_relation AS
        SELECT b.* FROM batch_relation b
        WHERE b.id IN (SELECT id FROM exists_relation)
           OR (NOT b.deleted AND EXISTS (
                 SELECT 1 FROM UNNEST(b.members) AS t(m) WHERE
                    (m.type = 'n' AND m.ref IN (SELECT id FROM exists_node))
                 OR (m.type = 'w' AND m.ref IN (SELECT id FROM exists_way))
                 OR (m.type = 'r' AND m.ref IN (SELECT id FROM exists_relation))
              ))
    """)
    n_rel_dropped = con.execute("SELECT count(*) FROM batch_relation").fetchone()[0] - con.execute("SELECT count(*) FROM kept_relation").fetchone()[0]

    kept = {"node": "kept_node", "way": "kept_way", "relation": "kept_relation"}
    dropped = {"node": n_node_dropped, "way": n_way_dropped, "relation": n_rel_dropped}
    return kept, dropped


# --------------------------------------------------------------------------
# step 4: touched set
# --------------------------------------------------------------------------


def _touched_set(con, store, man: manifest_mod.Manifest, state: TierState, kept: dict[str, str]) -> tuple[np.ndarray, np.ndarray]:
    node_way_parts = man.index.get("node_way", [])
    member_parts = man.index.get("member", [])
    member_paths = [store.url(p["path"]) for p in member_parts if store.exists(p["path"])]

    kept_node_ids = con.execute(f"SELECT id FROM {kept['node']}").fetchnumpy()["id"]
    kept_way_ids = con.execute(f"SELECT id FROM {kept['way']}").fetchnumpy()["id"]
    kept_relation_ids = con.execute(f"SELECT id FROM {kept['relation']}").fetchnumpy()["id"]

    # parent ways of changed nodes: base node_way (id-range selected) ∪ delta_node_way
    if len(kept_node_ids) > 0:
        lo, hi = int(kept_node_ids.min()), int(kept_node_ids.max())
        parts = _select_parts(node_way_parts, lo, hi)
        _register_ids(con, "__kept_node_ids_t", kept_node_ids)
        if parts:
            paths = [store.url(p) for p in parts]
            base_pw = con.execute(
                f"SELECT DISTINCT way_id FROM read_parquet({paths!r}) WHERE node_id IN (SELECT id FROM __kept_node_ids_t)"
            ).fetchnumpy()["way_id"]
        else:
            base_pw = np.array([], dtype=np.int64)
        delta_pw = con.execute(
            "SELECT DISTINCT way_id FROM delta_node_way WHERE node_id IN (SELECT id FROM __kept_node_ids_t)"
        ).fetchnumpy()["way_id"]
    else:
        base_pw = np.array([], dtype=np.int64)
        delta_pw = np.array([], dtype=np.int64)

    touched_way_candidate = np.unique(np.concatenate([kept_way_ids, np.asarray(base_pw, dtype=np.int64), np.asarray(delta_pw, dtype=np.int64)]))

    # verify touched-only (not in kept_way) way candidates actually exist
    not_in_batch_way = np.setdiff1d(touched_way_candidate, kept_way_ids, assume_unique=False)
    _register_ids(con, "__touched_way_check_t", not_in_batch_way)
    _fetch_current(con, store, "way", man, man.promoted_keys, state, "__touched_way_check_t", "__touched_way_exists")
    alive_touched_only_way = con.execute("SELECT id FROM __touched_way_exists").fetchnumpy()["id"]
    touched_way_ids = np.unique(np.concatenate([kept_way_ids, np.asarray(alive_touched_only_way, dtype=np.int64)]))

    # fixed point for touched relations: seed = kept_node ∪ touched_way_ids ∪ kept_relation
    def _parent_relations(ids: np.ndarray) -> np.ndarray:
        if len(ids) == 0:
            return np.array([], dtype=np.int64)
        _register_ids(con, "__pr_ids_t", ids)
        if member_paths:
            base = con.execute(f"""
                SELECT DISTINCT parent_id FROM read_parquet({member_paths!r})
                WHERE member_id IN (SELECT id FROM __pr_ids_t)
            """).fetchnumpy()["parent_id"]
        else:
            base = np.array([], dtype=np.int64)
        delta = con.execute(
            "SELECT DISTINCT parent_id FROM delta_member WHERE member_id IN (SELECT id FROM __pr_ids_t)"
        ).fetchnumpy()["parent_id"]
        return np.unique(np.concatenate([np.asarray(base, dtype=np.int64), np.asarray(delta, dtype=np.int64)]))

    touched_relation_ids = np.unique(np.concatenate([
        kept_relation_ids.astype(np.int64),
        _parent_relations(kept_node_ids),
        _parent_relations(touched_way_ids),
    ]))
    for _ in range(8):
        more = _parent_relations(touched_relation_ids)
        combined = np.unique(np.concatenate([touched_relation_ids, more]))
        if len(combined) == len(touched_relation_ids):
            break
        touched_relation_ids = combined

    not_in_batch_rel = np.setdiff1d(touched_relation_ids, kept_relation_ids, assume_unique=False)
    _register_ids(con, "__touched_rel_check_t", not_in_batch_rel)
    _fetch_current(con, store, "relation", man, man.promoted_keys, state, "__touched_rel_check_t", "__touched_rel_exists")
    alive_touched_only_rel = con.execute("SELECT id FROM __touched_rel_exists").fetchnumpy()["id"]
    touched_relation_ids_final = np.unique(np.concatenate([kept_relation_ids.astype(np.int64), np.asarray(alive_touched_only_rel, dtype=np.int64)]))

    return touched_way_ids, touched_relation_ids_final


# --------------------------------------------------------------------------
# steps 5/6: fetch current state + node coords, re-resolve geometry/bbox/cell
# --------------------------------------------------------------------------


def _promoted_select(prefix: str, promoted_keys: list[str]) -> str:
    return ", ".join(f'{prefix}."{k}"' for k in promoted_keys)


def _promoted_null_select(promoted_keys: list[str]) -> str:
    return ", ".join(f'NULL::VARCHAR AS "{k}"' for k in promoted_keys)


def _filled_i64(col) -> tuple[np.ndarray, np.ndarray]:
    """(values as float64 with NaN for NULL, null-mask) for a possibly-masked
    numpy array from ``fetchnumpy()`` -- see ``osmpq.build.raw`` for the
    original of this helper."""
    if isinstance(col, np.ma.MaskedArray):
        return col.astype("float64").filled(np.nan), np.ma.getmaskarray(col)
    arr = np.asarray(col, dtype="float64")
    return arr, np.zeros(len(arr), dtype=bool)


def _resolve(
    con, store, man: manifest_mod.Manifest, promoted_keys: list[str], state: TierState,
    kept: dict[str, str], touched_way_ids: np.ndarray, touched_relation_ids: np.ndarray,
    leaf_index: cells_mod.LeafIndex, ancestor_depths: list[int], max_depth: int, last_seq: int,
) -> dict[str, str]:
    # ``kept_*`` (batch) tables only carry a ``tags`` MAP (osc.py doesn't
    # promote keys -- there's no schema to promote *into*), so their
    # promoted columns are extracted from the map here; ``prior_*`` tables
    # are byid-schema rows and already have real promoted columns.
    promoted_k = common.promoted_select(promoted_keys, tags_expr="k.tags")
    promoted_p = _promoted_select("p", promoted_keys)
    promoted_null = _promoted_null_select(promoted_keys)

    _register_ids(con, "touched_way_ids_t", touched_way_ids)
    _register_ids(con, "touched_relation_ids_t", touched_relation_ids)
    _fetch_current(con, store, "way", man, promoted_keys, state, "touched_way_ids_t", "prior_way_all")
    _fetch_current(con, store, "relation", man, promoted_keys, state, "touched_relation_ids_t", "prior_relation_all")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE touched_way_base AS
        SELECT k.id, k.refs, k.tags, {promoted_k}, k.version, k.changeset, k.timestamp, k.uid, k."user",
               k.deleted, k.seq, p.cell AS prev_cell
        FROM {kept['way']} k LEFT JOIN prior_way_all p ON p.id = k.id
        UNION ALL
        SELECT p.id, p.refs, p.tags, {promoted_p}, p.version, p.changeset, p.timestamp, p.uid, p."user",
               FALSE AS deleted, {int(last_seq)} AS seq, p.cell AS prev_cell
        FROM prior_way_all p
        WHERE p.id IN (SELECT id FROM touched_way_ids_t) AND p.id NOT IN (SELECT id FROM {kept['way']})
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE touched_relation_base AS
        SELECT k.id, k.members, k.tags, {promoted_k}, k.version, k.changeset, k.timestamp, k.uid, k."user",
               k.deleted, k.seq, p.cell AS prev_cell
        FROM {kept['relation']} k LEFT JOIN prior_relation_all p ON p.id = k.id
        UNION ALL
        SELECT p.id, p.members, p.tags, {promoted_p}, p.version, p.changeset, p.timestamp, p.uid, p."user",
               FALSE AS deleted, {int(last_seq)} AS seq, p.cell AS prev_cell
        FROM prior_relation_all p
        WHERE p.id IN (SELECT id FROM touched_relation_ids_t) AND p.id NOT IN (SELECT id FROM {kept['relation']})
    """)

    # ---- node coordinates for every ref of a touched way / member of a touched relation
    con.execute("""
        CREATE OR REPLACE TEMP VIEW __way_refs_needed AS
        SELECT unnest(refs) AS id FROM touched_way_base WHERE NOT deleted
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW __rel_member_nodes_needed AS
        SELECT m.ref AS id FROM touched_relation_base, UNNEST(members) AS t(m) WHERE NOT deleted AND m.type = 'n'
    """)
    node_ids_needed = _union_ids_sql(con, kept["node"], "__way_refs_needed", "__rel_member_nodes_needed")
    _register_ids(con, "node_ids_needed_t", node_ids_needed)
    _fetch_current(con, store, "node", man, promoted_keys, state, "node_ids_needed_t", "prior_node_all")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE node_coords_all AS
        SELECT id, lat_e7, lon_e7 FROM {kept['node']} WHERE NOT deleted
        UNION ALL
        SELECT id, lat_e7, lon_e7 FROM prior_node_all WHERE id NOT IN (SELECT id FROM {kept['node']})
    """)

    # ---- extra member way/relation bbox needed for touched relations, for
    # members that aren't themselves touched this run --------------------------
    con.execute("""
        CREATE OR REPLACE TEMP VIEW __rel_member_ways_needed AS
        SELECT DISTINCT m.ref AS id FROM touched_relation_base, UNNEST(members) AS t(m)
        WHERE NOT deleted AND m.type = 'w' AND m.ref NOT IN (SELECT id FROM touched_way_ids_t)
    """)
    _register_ids(con, "extra_way_ids_t", con.execute("SELECT id FROM __rel_member_ways_needed").fetchnumpy()["id"])
    _fetch_current(con, store, "way", man, promoted_keys, state, "extra_way_ids_t", "extra_way_bbox")

    con.execute("""
        CREATE OR REPLACE TEMP VIEW __rel_member_rels_needed AS
        SELECT DISTINCT m.ref AS id FROM touched_relation_base, UNNEST(members) AS t(m)
        WHERE NOT deleted AND m.type = 'r' AND m.ref NOT IN (SELECT id FROM touched_relation_ids_t)
    """)
    _register_ids(con, "extra_rel_ids_t", con.execute("SELECT id FROM __rel_member_rels_needed").fetchnumpy()["id"])
    _fetch_current(con, store, "relation", man, promoted_keys, state, "extra_rel_ids_t", "extra_relation_bbox")

    # ---- way geometry/bbox (mirrors osmpq.build.raw's way_pts logic) -----------
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_pts AS
        SELECT w.id AS way_id, t.ordinal, n.lat_e7, n.lon_e7
        FROM touched_way_base w, UNNEST(w.refs) WITH ORDINALITY AS t(ref, ordinal)
        LEFT JOIN node_coords_all n ON n.id = t.ref
        WHERE NOT w.deleted
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_geom AS
        SELECT way_id AS id,
               CASE WHEN count(lat_e7) > 0 THEN min(lat_e7) END AS ymin_e7,
               CASE WHEN count(lat_e7) > 0 THEN max(lat_e7) END AS ymax_e7,
               CASE WHEN count(lat_e7) > 0 THEN min(lon_e7) END AS xmin_e7,
               CASE WHEN count(lat_e7) > 0 THEN max(lon_e7) END AS xmax_e7,
               CASE WHEN count(lat_e7) >= 2 THEN
                   ST_MakeLine(list(ST_Point(lon_e7 / 1e7, lat_e7 / 1e7) ORDER BY ordinal)
                               FILTER (WHERE lat_e7 IS NOT NULL))
               END AS geometry
        FROM way_pts GROUP BY way_id
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_resolved_pre AS
        SELECT w.id, w.refs, w.tags,
    """ + _promoted_select("w", promoted_keys) + """,
               w.version, w.changeset, w.timestamp, w.uid, w."user",
               g.xmin_e7, g.ymin_e7, g.xmax_e7, g.ymax_e7, g.geometry,
               (coalesce(len(w.refs), 0) >= 4 AND w.refs[1] = w.refs[len(w.refs)]) AS is_closed,
               w.prev_cell, w.seq
        FROM touched_way_base w LEFT JOIN way_geom g ON g.id = w.id
        WHERE NOT w.deleted
    """)
    con.execute("""
        ALTER TABLE way_resolved_pre ADD COLUMN is_area BOOLEAN
    """)
    con.execute("""
        UPDATE way_resolved_pre SET is_area = (
            is_closed
            AND coalesce(tags['area'], '') != 'no'
            AND NOT (
                (tags['highway'] IS NOT NULL OR tags['barrier'] IS NOT NULL)
                AND coalesce(tags['area'], '') != 'yes'
            )
        )
    """)

    way_ids_np, ymin, xmin, ymax, xmax = con.execute(
        "SELECT id, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM way_resolved_pre"
    ).fetchnumpy().values()
    if len(way_ids_np) > 0:
        ymin_f, ymin_null = _filled_i64(ymin)
        xmin_f, xmin_null = _filled_i64(xmin)
        ymax_f, ymax_null = _filled_i64(ymax)
        xmax_f, xmax_null = _filled_i64(xmax)
        has_bbox = ~(ymin_null | xmin_null | ymax_null | xmax_null)
        way_cell = np.full(len(way_ids_np), cells_mod.ROOT, dtype=object)
        way_hilbert = np.zeros(len(way_ids_np), dtype=np.uint64)
        way_clat = np.zeros(len(way_ids_np), dtype=np.int64)
        way_clon = np.zeros(len(way_ids_np), dtype=np.int64)
        if has_bbox.any():
            sub_cell = cells_mod.containing_cells_v2_np(
                ymin_f[has_bbox].astype(np.int64), xmin_f[has_bbox].astype(np.int64),
                ymax_f[has_bbox].astype(np.int64), xmax_f[has_bbox].astype(np.int64),
                leaf_index, ancestor_depths, max_depth,
            )
            clat_e7 = np.round((ymin_f[has_bbox] + ymax_f[has_bbox]) / 2.0).astype(np.int64)
            clon_e7 = np.round((xmin_f[has_bbox] + xmax_f[has_bbox]) / 2.0).astype(np.int64)
            sub_hilbert = hilbert_mod.hilbert_keys(clat_e7, clon_e7)
            way_cell[has_bbox] = sub_cell
            way_hilbert[has_bbox] = sub_hilbert
            way_clat[has_bbox] = clat_e7
            way_clon[has_bbox] = clon_e7
        con.register("_way_cell_arrow", _assignment_table(
            way_ids_np, way_cell, way_hilbert, way_clat, way_clon, has_bbox,
        ))
        con.execute("CREATE OR REPLACE TEMP TABLE way_cell_assign AS SELECT * FROM _way_cell_arrow")
        con.unregister("_way_cell_arrow")
    else:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE way_cell_assign AS
            SELECT NULL::BIGINT AS id, NULL::VARCHAR AS cell, NULL::UBIGINT AS hilbert,
                   NULL::INTEGER AS centroid_lat_e7, NULL::INTEGER AS centroid_lon_e7 WHERE FALSE
        """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE resolved_way AS
        SELECT w.id, w.refs, w.tags, {_promoted_select("w", promoted_keys)},
               w.version, w.changeset, w.timestamp, w.uid, w."user",
               w.xmin_e7, w.ymin_e7, w.xmax_e7, w.ymax_e7, w.geometry, w.is_closed, w.is_area,
               a.centroid_lat_e7, a.centroid_lon_e7, a.cell, a.hilbert,
               FALSE AS deleted, w.prev_cell, w.seq
        FROM way_resolved_pre w JOIN way_cell_assign a ON a.id = w.id
        UNION ALL
        SELECT id, NULL::BIGINT[], NULL::MAP(VARCHAR,VARCHAR), {promoted_null},
               NULL::INTEGER, NULL::BIGINT, NULL::TIMESTAMP, NULL::INTEGER, NULL::VARCHAR,
               NULL::INTEGER, NULL::INTEGER, NULL::INTEGER, NULL::INTEGER, NULL::GEOMETRY, NULL::BOOLEAN, NULL::BOOLEAN,
               NULL::INTEGER, NULL::INTEGER, prev_cell, NULL::UBIGINT,
               TRUE, prev_cell, seq
        FROM touched_way_base WHERE deleted
    """)

    # ---- relation bbox: pass0 (member nodes/ways), pass1 (nested relations) ---
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rel_members AS
        SELECT r.id AS rel_id, m.type AS mtype, m.ref AS mref
        FROM touched_relation_base r, UNNEST(r.members) AS t(m) WHERE NOT r.deleted
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_bbox_all AS
        SELECT id, xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM resolved_way WHERE NOT deleted AND xmin_e7 IS NOT NULL
        UNION ALL
        SELECT id, xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM extra_way_bbox WHERE xmin_e7 IS NOT NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rel_member_bbox AS
        SELECT rm.rel_id, n.lat_e7 AS ymin_e7, n.lat_e7 AS ymax_e7, n.lon_e7 AS xmin_e7, n.lon_e7 AS xmax_e7
        FROM rel_members rm JOIN node_coords_all n ON n.id = rm.mref WHERE rm.mtype = 'n'
        UNION ALL
        SELECT rm.rel_id, w.ymin_e7, w.ymax_e7, w.xmin_e7, w.xmax_e7
        FROM rel_members rm JOIN way_bbox_all w ON w.id = rm.mref WHERE rm.mtype = 'w'
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE relation_bbox0 AS
        SELECT r.id, min(b.ymin_e7) AS ymin_e7, max(b.ymax_e7) AS ymax_e7,
               min(b.xmin_e7) AS xmin_e7, max(b.xmax_e7) AS xmax_e7
        FROM touched_relation_base r LEFT JOIN rel_member_bbox b ON b.rel_id = r.id
        WHERE NOT r.deleted
        GROUP BY r.id
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rel_bbox_member_lookup AS
        SELECT id, xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM relation_bbox0 WHERE xmin_e7 IS NOT NULL
        UNION ALL
        SELECT id, xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM extra_relation_bbox WHERE xmin_e7 IS NOT NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE relation_bbox1 AS
        SELECT r0.id,
               LEAST(r0.ymin_e7, min(rb.ymin_e7)) AS ymin_e7,
               GREATEST(r0.ymax_e7, max(rb.ymax_e7)) AS ymax_e7,
               LEAST(r0.xmin_e7, min(rb.xmin_e7)) AS xmin_e7,
               GREATEST(r0.xmax_e7, max(rb.xmax_e7)) AS xmax_e7
        FROM relation_bbox0 r0
        LEFT JOIN rel_members rm ON rm.rel_id = r0.id AND rm.mtype = 'r'
        LEFT JOIN rel_bbox_member_lookup rb ON rb.id = rm.mref
        GROUP BY r0.id, r0.ymin_e7, r0.ymax_e7, r0.xmin_e7, r0.xmax_e7
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE relation_resolved_pre AS
        SELECT r.id, r.members, r.tags, {_promoted_select("r", promoted_keys)},
               r.version, r.changeset, r.timestamp, r.uid, r."user",
               b.xmin_e7, b.ymin_e7, b.xmax_e7, b.ymax_e7, r.prev_cell, r.seq
        FROM touched_relation_base r JOIN relation_bbox1 b ON b.id = r.id
        WHERE NOT r.deleted
    """)

    rel_ids_np, rymin, rxmin, rymax, rxmax = con.execute(
        "SELECT id, ymin_e7, xmin_e7, ymax_e7, xmax_e7 FROM relation_resolved_pre"
    ).fetchnumpy().values()
    if len(rel_ids_np) > 0:
        ymin_f, ymin_null = _filled_i64(rymin)
        xmin_f, xmin_null = _filled_i64(rxmin)
        ymax_f, ymax_null = _filled_i64(rymax)
        xmax_f, xmax_null = _filled_i64(rxmax)
        has_bbox = ~(ymin_null | xmin_null | ymax_null | xmax_null)
        rel_cell = np.full(len(rel_ids_np), cells_mod.ROOT, dtype=object)
        rel_hilbert = np.zeros(len(rel_ids_np), dtype=np.uint64)
        rel_clat = np.zeros(len(rel_ids_np), dtype=np.int64)
        rel_clon = np.zeros(len(rel_ids_np), dtype=np.int64)
        if has_bbox.any():
            sub_cell = cells_mod.containing_cells_v2_np(
                ymin_f[has_bbox].astype(np.int64), xmin_f[has_bbox].astype(np.int64),
                ymax_f[has_bbox].astype(np.int64), xmax_f[has_bbox].astype(np.int64),
                leaf_index, ancestor_depths, max_depth,
            )
            clat_e7 = np.round((ymin_f[has_bbox] + ymax_f[has_bbox]) / 2.0).astype(np.int64)
            clon_e7 = np.round((xmin_f[has_bbox] + xmax_f[has_bbox]) / 2.0).astype(np.int64)
            sub_hilbert = hilbert_mod.hilbert_keys(clat_e7, clon_e7)
            rel_cell[has_bbox] = sub_cell
            rel_hilbert[has_bbox] = sub_hilbert
            rel_clat[has_bbox] = clat_e7
            rel_clon[has_bbox] = clon_e7
        con.register("_rel_cell_arrow", _assignment_table(
            rel_ids_np, rel_cell, rel_hilbert, rel_clat, rel_clon, has_bbox,
        ))
        con.execute("CREATE OR REPLACE TEMP TABLE rel_cell_assign AS SELECT * FROM _rel_cell_arrow")
        con.unregister("_rel_cell_arrow")
    else:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE rel_cell_assign AS
            SELECT NULL::BIGINT AS id, NULL::VARCHAR AS cell, NULL::UBIGINT AS hilbert,
                   NULL::INTEGER AS centroid_lat_e7, NULL::INTEGER AS centroid_lon_e7 WHERE FALSE
        """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE resolved_relation AS
        SELECT r.id, r.members, r.tags, {_promoted_select("r", promoted_keys)},
               r.version, r.changeset, r.timestamp, r.uid, r."user",
               r.xmin_e7, r.ymin_e7, r.xmax_e7, r.ymax_e7, NULL::GEOMETRY AS geometry,
               a.centroid_lat_e7, a.centroid_lon_e7, a.cell, a.hilbert,
               FALSE AS deleted, r.prev_cell, r.seq
        FROM relation_resolved_pre r JOIN rel_cell_assign a ON a.id = r.id
        UNION ALL
        SELECT id, NULL::STRUCT("type" VARCHAR, ref BIGINT, role VARCHAR)[], NULL::MAP(VARCHAR,VARCHAR), {promoted_null},
               NULL::INTEGER, NULL::BIGINT, NULL::TIMESTAMP, NULL::INTEGER, NULL::VARCHAR,
               NULL::INTEGER, NULL::INTEGER, NULL::INTEGER, NULL::INTEGER, NULL::GEOMETRY,
               NULL::INTEGER, NULL::INTEGER, prev_cell, NULL::UBIGINT,
               TRUE, prev_cell, seq
        FROM touched_relation_base WHERE deleted
    """)

    # ---- resolved_node: kept_node reshaped to the delta spatial schema --------
    node_ids_np, nlat, nlon, ndeleted = con.execute(
        f"SELECT id, lat_e7, lon_e7, deleted FROM {kept['node']}"
    ).fetchnumpy().values()
    node_cell = np.array([""] * len(node_ids_np), dtype=object)
    node_hilbert = np.zeros(len(node_ids_np), dtype=np.uint64)
    not_del = ~np.asarray(ndeleted, dtype=bool)
    if not_del.any():
        lat_arr = np.asarray(nlat.filled(0) if isinstance(nlat, np.ma.MaskedArray) else nlat, dtype=np.int64)
        lon_arr = np.asarray(nlon.filled(0) if isinstance(nlon, np.ma.MaskedArray) else nlon, dtype=np.int64)
        node_cell[not_del] = cells_mod.point_cells_np(lat_arr[not_del], lon_arr[not_del], leaf_index)
        node_hilbert[not_del] = hilbert_mod.hilbert_keys(lat_arr[not_del], lon_arr[not_del])
    import pyarrow as pa

    node_assign_tbl = pa.table({
        "id": pa.array(np.asarray(node_ids_np, dtype=np.int64)),
        "n_cell": pa.array([c if c else None for c in node_cell], type=pa.string()),
        "n_hilbert": pa.array(node_hilbert, type=pa.uint64()),
    })
    con.register("_node_cell_arrow", node_assign_tbl)
    con.execute("CREATE OR REPLACE TEMP TABLE node_cell_assign AS SELECT * FROM _node_cell_arrow")
    con.unregister("_node_cell_arrow")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE resolved_node AS
        SELECT k.id, k.lat_e7, k.lon_e7, k.tags, {common.promoted_select(promoted_keys, tags_expr="k.tags")},
               k.version, k.changeset, k.timestamp, k.uid, k."user",
               a.n_hilbert AS hilbert, a.n_cell AS cell,
               k.deleted, p.cell AS prev_cell, k.seq
        FROM {kept['node']} k
        JOIN node_cell_assign a ON a.id = k.id
        LEFT JOIN exists_node p ON p.id = k.id
        WHERE NOT k.deleted
        UNION ALL
        SELECT k.id, NULL::INTEGER, NULL::INTEGER, NULL::MAP(VARCHAR,VARCHAR), {promoted_null},
               NULL::INTEGER, NULL::BIGINT, NULL::TIMESTAMP, NULL::INTEGER, NULL::VARCHAR,
               NULL::UBIGINT, p.cell,
               TRUE, p.cell, k.seq
        FROM {kept['node']} k LEFT JOIN exists_node p ON p.id = k.id
        WHERE k.deleted
    """)

    return {"node": "resolved_node", "way": "resolved_way", "relation": "resolved_relation"}


def _assignment_table(ids: np.ndarray, cell: np.ndarray, hilbert: np.ndarray, clat: np.ndarray, clon: np.ndarray, has_bbox: np.ndarray):
    import pyarrow as pa

    return pa.table({
        "id": pa.array(np.asarray(ids, dtype=np.int64)),
        "cell": pa.array([c if c else None for c in cell], type=pa.string()),
        "hilbert": pa.array(hilbert, type=pa.uint64()),
        "centroid_lat_e7": pa.array(
            [int(v) if m else None for v, m in zip(clat.tolist(), has_bbox.tolist())], type=pa.int32()
        ),
        "centroid_lon_e7": pa.array(
            [int(v) if m else None for v, m in zip(clon.tolist(), has_bbox.tolist())], type=pa.int32()
        ),
    })


# --------------------------------------------------------------------------
# step 7/8: rolling tiers (merge/fold), file writing, manifest deltas
# --------------------------------------------------------------------------

_uid_counter = [0]


def _uid() -> str:
    _uid_counter[0] += 1
    return str(_uid_counter[0])


def _rel(root: Path, path: Path) -> str:
    import os

    return str(path.relative_to(root)).replace(os.sep, "/")


def _parse_ts(s: Optional[str]):
    from datetime import datetime

    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _floor_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)


def _floor_day(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _load_old(con, store, typ: str, meta: Optional[dict]) -> tuple[Optional[str], Optional[str]]:
    if meta is None:
        return None, None
    byid_url = store.url(meta["files"][typ]["byid"])
    spatial_url = store.url(meta["files"][typ]["spatial"])
    byid_name = f"__old_byid_{typ}_{_uid()}"
    spatial_name = f"__old_spatial_{typ}_{_uid()}"
    con.execute(f"CREATE OR REPLACE TEMP VIEW {byid_name} AS SELECT * FROM read_parquet('{_esc(byid_url)}')")
    con.execute(f"CREATE OR REPLACE TEMP VIEW {spatial_name} AS SELECT * FROM read_parquet('{_esc(spatial_url)}')")
    return byid_name, spatial_name


def _merge_type(con, typ: str, promoted_keys: list[str], new_byid: str, old_byid: Optional[str],
                 new_spatial: str, old_spatial: Optional[str]) -> tuple[str, str]:
    """merge(new, old): newest (``new``) wins per id; ``prev_cell`` is
    first-write-wins (``old``'s recorded prev_cell survives if the id was
    already in ``old``). Returns (byid_table_name, spatial_table_name)."""
    byid_cols = BYID_COLUMNS[typ](promoted_keys)
    spatial_cols = SPATIAL_COLUMNS[typ](promoted_keys)
    out_byid = f"__merged_byid_{typ}_{_uid()}"
    out_spatial = f"__merged_spatial_{typ}_{_uid()}"

    if old_byid is None:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {out_byid} AS
            SELECT {', '.join(byid_cols)}, deleted, prev_cell, seq FROM {new_byid}
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {out_spatial} AS
            SELECT {', '.join(spatial_cols)}, deleted, prev_cell, seq FROM {new_spatial}
        """)
        return out_byid, out_spatial

    n_byid_cols = ', '.join(f'n.{c}' for c in byid_cols)
    o_byid_cols = ', '.join(f'o.{c}' for c in byid_cols)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {out_byid} AS
        SELECT {n_byid_cols}, n.deleted,
               CASE WHEN o.id IS NOT NULL THEN o.prev_cell ELSE n.prev_cell END AS prev_cell, n.seq
        FROM {new_byid} n LEFT JOIN {old_byid} o ON o.id = n.id
        UNION ALL
        SELECT {o_byid_cols}, o.deleted, o.prev_cell, o.seq
        FROM {old_byid} o WHERE o.id NOT IN (SELECT id FROM {new_byid})
    """)

    n_spatial_cols = ', '.join(f'n.{c}' for c in spatial_cols)
    os_spatial_cols = ', '.join(f'os.{c}' for c in spatial_cols)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {out_spatial} AS
        SELECT {n_spatial_cols}, n.deleted,
               CASE WHEN ob.id IS NOT NULL THEN ob.prev_cell ELSE n.prev_cell END AS prev_cell, n.seq
        FROM {new_spatial} n LEFT JOIN {old_byid} ob ON ob.id = n.id
        UNION ALL
        SELECT {os_spatial_cols}, ob.deleted, ob.prev_cell, ob.seq
        FROM {old_byid} ob JOIN {old_spatial} os ON os.id = ob.id
        WHERE ob.id NOT IN (SELECT id FROM {new_byid})
    """)
    return out_byid, out_spatial


def _write_tier_version(
    con, root: Path, man: manifest_mod.Manifest, promoted_keys: list[str], tier: str,
    merged: dict[str, tuple[str, str]], seq_from: Optional[int], seq_to: Optional[int],
    timestamp: Optional[str], version: int,
) -> Optional[tuple[dict, int]]:
    total_rows = sum(con.execute(f"SELECT count(*) FROM {merged[t][0]}").fetchone()[0] for t in _TYPES)
    if total_rows == 0:
        return None
    out_dir = root / "delta" / man.generation / tier / str(version)
    out_dir.mkdir(parents=True, exist_ok=True)
    files: dict[str, Any] = {}
    rows: dict[str, int] = {}
    cells: dict[str, list[str]] = {}
    tomb_pieces = []
    total_bytes = 0
    for typ in _TYPES:
        byid_name, spatial_name = merged[typ]
        byid_path = out_dir / f"{typ}.byid.parquet"
        spatial_path = out_dir / f"{typ}.spatial.parquet"
        byid_cols = delta_byid_columns(typ, promoted_keys)
        spatial_cols = delta_spatial_columns(typ, promoted_keys)
        n_rows, n_bytes = common.copy_to_parquet(
            con, f"SELECT {', '.join(byid_cols)} FROM {byid_name} ORDER BY id",
            byid_path, row_group_size_bytes=1_000_000,
        )
        _srows, s_bytes = common.copy_to_parquet(
            con, f"SELECT {', '.join(spatial_cols)} FROM {spatial_name} ORDER BY cell, hilbert, id",
            spatial_path, row_group_size_bytes=1_000_000,
        )
        files[typ] = {"spatial": _rel(root, spatial_path), "byid": _rel(root, byid_path)}
        rows[typ] = n_rows
        total_bytes += n_bytes + s_bytes
        # Every row (deleted or not) carries a real cell value (deleted rows
        # have cell = prev_cell), so this is exactly "every cell this tier
        # has at least one row -- live or tombstone -- for, for this table".
        cell_rows = con.execute(f"SELECT DISTINCT cell FROM {spatial_name} WHERE cell IS NOT NULL").fetchall()
        cells[typ] = sorted(c[0] for c in cell_rows)
        tomb_pieces.append(
            f"SELECT '{typ}' AS type, id, prev_cell, seq FROM {byid_name} "
            f"WHERE prev_cell IS NOT NULL AND (prev_cell != cell OR deleted)"
        )
    tomb_path = out_dir / "tombstones.parquet"
    tomb_sql = " UNION ALL ".join(tomb_pieces)
    _tomb_rows, tomb_bytes = common.copy_to_parquet(
        con, f"SELECT * FROM ({tomb_sql}) t ORDER BY prev_cell, type, id", tomb_path, row_group_size_bytes=1_000_000,
    )
    total_bytes += tomb_bytes
    files["tombstones"] = _rel(root, tomb_path)
    meta = {
        "version": version, "seq_from": seq_from, "seq_to": seq_to, "timestamp": timestamp,
        "rows": rows, "cells": cells, "files": files,
    }
    return meta, total_bytes


def _tier_crossings(man_deltas: Optional[dict], batch_timestamp: Optional[str]) -> tuple[bool, bool]:
    """Whether this batch's timestamp crosses the current hour/day tier's
    UTC hour/day boundary (docs/m2-contracts.md section 5 step 7) --
    factored out so the history tiers (docs/m4-contracts.md section 5.1,
    "folding at the same boundaries as the delta tiers") fold at exactly
    the same instants as the current-state delta tiers, driven from the
    *delta* tiers' own timestamps even when the history tiers don't exist
    yet (a fresh v5 root's history starts with no tiers at all)."""
    hour_old_meta = man_deltas.get("hour") if man_deltas else None
    day_old_meta = man_deltas.get("day") if man_deltas else None
    batch_dt = _parse_ts(batch_timestamp)
    hour_old_dt = _parse_ts(hour_old_meta["timestamp"]) if hour_old_meta else None
    day_old_dt = _parse_ts(day_old_meta["timestamp"]) if day_old_meta else None
    hour_crossed = hour_old_dt is not None and batch_dt is not None and _floor_hour(hour_old_dt) != _floor_hour(batch_dt)
    day_crossed = hour_crossed and day_old_dt is not None and batch_dt is not None and _floor_day(day_old_dt) != _floor_day(batch_dt)
    return hour_crossed, day_crossed


def _write_tiers(
    con, root: Path, store, man: manifest_mod.Manifest, promoted_keys: list[str], state: TierState,
    resolved: dict[str, str], first_seq: int, last_seq: int, batch_timestamp: Optional[str],
) -> tuple[dict[str, int], dict[str, int], dict[str, Any]]:
    hour_old_meta = man.deltas.get("hour") if man.deltas else None
    day_old_meta = man.deltas.get("day") if man.deltas else None
    week_old_meta = man.deltas.get("week") if man.deltas else None

    hour_crossed, day_crossed = _tier_crossings(man.deltas, batch_timestamp)

    for typ in _TYPES:
        cols = BYID_COLUMNS[typ](promoted_keys)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW batch_byid_{typ} AS
            SELECT {', '.join(cols)}, deleted, prev_cell, seq FROM {resolved[typ]}
        """)

    tier_versions: dict[str, int] = {}
    tier_bytes: dict[str, int] = {}
    new_deltas: dict[str, Any] = dict(man.deltas or {})

    if not hour_crossed:
        merged_hour = {}
        for typ in _TYPES:
            old_byid, old_spatial = _load_old(con, store, typ, hour_old_meta)
            merged_hour[typ] = _merge_type(con, typ, promoted_keys, f"batch_byid_{typ}", old_byid, resolved[typ], old_spatial)
        seq_from = hour_old_meta["seq_from"] if hour_old_meta else first_seq
        version = (hour_old_meta["version"] if hour_old_meta else 0) + 1
        out = _write_tier_version(con, root, man, promoted_keys, "hour", merged_hour, seq_from, last_seq, batch_timestamp, version)
        if out:
            meta, nbytes = out
            new_deltas["hour"] = meta
            tier_versions["hour"] = meta["version"]
            tier_bytes["hour"] = nbytes
        return tier_versions, tier_bytes, new_deltas

    # ---- hour boundary crossed: fold hour_old into day, hour' = batch alone ---
    merged_day = {}
    for typ in _TYPES:
        hour_byid, hour_spatial = _load_old(con, store, typ, hour_old_meta)
        day_byid, day_spatial = _load_old(con, store, typ, day_old_meta)
        merged_day[typ] = _merge_type(con, typ, promoted_keys, hour_byid, day_byid, hour_spatial, day_spatial)
    day_seq_from = min(x for x in [(day_old_meta or {}).get("seq_from"), hour_old_meta["seq_from"]] if x is not None)
    day_seq_to = hour_old_meta["seq_to"]
    day_timestamp = hour_old_meta["timestamp"]
    day_version = (day_old_meta["version"] if day_old_meta else 0) + 1

    if day_crossed:
        merged_week = {}
        for typ in _TYPES:
            week_byid, week_spatial = _load_old(con, store, typ, week_old_meta)
            mb, ms = merged_day[typ]
            merged_week[typ] = _merge_type(con, typ, promoted_keys, mb, week_byid, ms, week_spatial)
        week_seq_from = min(x for x in [(week_old_meta or {}).get("seq_from"), day_seq_from] if x is not None)
        week_seq_to = day_seq_to
        week_timestamp = day_timestamp
        week_version = (week_old_meta["version"] if week_old_meta else 0) + 1
        out = _write_tier_version(con, root, man, promoted_keys, "week", merged_week, week_seq_from, week_seq_to, week_timestamp, week_version)
        if out:
            meta, nbytes = out
            new_deltas["week"] = meta
            tier_versions["week"] = meta["version"]
            tier_bytes["week"] = nbytes
        # day' resets to batch alone (mirrors hour')
        merged_day_final = {typ: (f"batch_byid_{typ}", resolved[typ]) for typ in _TYPES}
        out = _write_tier_version(con, root, man, promoted_keys, "day", merged_day_final, first_seq, last_seq, batch_timestamp, day_version)
    else:
        out = _write_tier_version(con, root, man, promoted_keys, "day", merged_day, day_seq_from, day_seq_to, day_timestamp, day_version)
    if out:
        meta, nbytes = out
        new_deltas["day"] = meta
        tier_versions["day"] = meta["version"]
        tier_bytes["day"] = nbytes

    merged_hour_final = {typ: (f"batch_byid_{typ}", resolved[typ]) for typ in _TYPES}
    hour_version = (hour_old_meta["version"] if hour_old_meta else 0) + 1
    out = _write_tier_version(con, root, man, promoted_keys, "hour", merged_hour_final, first_seq, last_seq, batch_timestamp, hour_version)
    if out:
        meta, nbytes = out
        new_deltas["hour"] = meta
        tier_versions["hour"] = meta["version"]
        tier_bytes["hour"] = nbytes

    return tier_versions, tier_bytes, new_deltas


# ==========================================================================
# history tier append (docs/m4-contracts.md section 5.1)
#
# Runs only when ``man.history is not None``. Uses the same connection
# state ``_run_once_impl`` already built for the current-state path:
# ``kept_node``/``kept_way``/``kept_relation`` (the extent-filtered batch,
# section 2 of docs/m2-contracts.md), ``exists_node``/``exists_way``/
# ``exists_relation`` (the id's state *before* this batch, from
# ``_extent_filter``'s ``_fetch_current`` calls) and ``resolved_node``/
# ``resolved_way``/``resolved_relation`` (this batch's final re-resolved
# state, from ``_resolve``) -- all TEMP TABLEs/VIEWs still alive on ``con``.
#
# Every history row is the base row's columns (``BYID_COLUMNS``/
# ``SPATIAL_COLUMNS`` -- the same base schema the current-state delta files
# use) plus ``history_schema.HISTORY_EXTRA_NAMES`` (``minor``,
# ``valid_from``, ``valid_to``, ``visible``); column *names* drive every
# SQL projection here (not position), so a DuckDB ``SELECT <cols> FROM
# <table>`` always lands the right value under the right name regardless of
# the source table's own physical column order.
#
# Approximation documented by the contract (section 5.1's "the one
# approximation"): a way/relation *own-version* history row for a version
# superseded within the same run (a catch-up batch applying several diffs
# at once) gets that version's own tags/refs/members (exact -- they're in
# the parsed diff) but the *final* resolved state's geometry/bbox/cell/
# hilbert (approximate -- re-resolving geometry at every intermediate
# version's point in time would need historical node positions this run
# doesn't keep). This implementation extends the same substitution to a
# superseded *node* version's cell/hilbert too (its own lat/lon is still
# exact), and does not attempt a precise move-tombstone cell for a
# superseded version that was itself a delete-then-recreate within one
# run (an edge case that only arises for a very stale updater catching up
# many diffs at once) -- see the report for the full list.
# ==========================================================================


def _history_type_map(promoted_keys: list[str]) -> dict[str, str]:
    return _type_map_with_promoted(promoted_keys)


def _history_null_or_override(cols: list[str], type_map: dict[str, str], overrides: dict[str, str]) -> str:
    parts = []
    for c in cols:
        if c in overrides:
            parts.append(f"{overrides[c]} AS {c}")
        else:
            parts.append(f"NULL::{type_map[c]} AS {c}")
    return ", ".join(parts)


def _history_touched_only_ids(con, all_touched: np.ndarray, kept_view: str, out_name: str) -> None:
    """Registers ``out_name`` (TEMP TABLE, ``id BIGINT``): ids in
    ``all_touched`` (this run's full touched-way/relation-id set from
    ``_touched_set``) that are *not* in ``kept_view`` (this batch's own
    changed elements) -- the "touched parent whose version did not change"
    case (section 5.1), which only ever applies to ways/relations (nodes
    have no minor versions)."""
    kept_ids = con.execute(f"SELECT id FROM {kept_view}").fetchnumpy()["id"]
    only = np.setdiff1d(
        np.asarray(all_touched, dtype=np.int64), np.asarray(kept_ids, dtype=np.int64),
    )
    _register_ids(con, out_name, only)


def _history_prev_minor_view(
    con, store, man: manifest_mod.Manifest, typ: str, ids_table: str, version_lookup_sql: str,
) -> str:
    """A TEMP VIEW ``(id, prev_minor)``: the greatest ``minor`` already on
    record (in the still-open history tiers, or the base history byid
    parts) for each id in ``ids_table`` *at the version* ``version_lookup_sql``
    (a ``SELECT id, version`` over the ids' current -- unchanged -- version)
    gives for that id. An id with no matching row anywhere gets no row here
    (the caller's ``COALESCE(prev_minor, 0) + 1`` then gives ``1``, section
    5.1's "unknown -> 1")."""
    hist = man.history or {}
    tiers = hist.get("tiers") or {}
    pieces = []
    for tier in history_schema.TIERS:
        entry = tiers.get(tier)
        if not entry:
            continue
        fpath = ((entry.get("files") or {}).get(typ) or {}).get("byid")
        if fpath and store.exists(fpath):
            pieces.append(f"SELECT id, version, minor FROM read_parquet('{_esc(store.url(fpath))}')")
    n, lo, hi = con.execute(f"SELECT count(*), min(id), max(id) FROM {ids_table}").fetchone()
    if n:
        base_parts = _select_parts((hist.get("byid") or {}).get(typ, []) or [], lo, hi)
        if base_parts:
            paths = [store.url(p) for p in base_parts]
            pieces.append(f"SELECT id, version, minor FROM read_parquet({paths!r})")
    view_name = f"__hist_prev_minor_{typ}_{_uid()}"
    if not pieces:
        con.execute(f"CREATE OR REPLACE TEMP VIEW {view_name} AS SELECT NULL::BIGINT AS id, NULL::INTEGER AS prev_minor WHERE FALSE")
        return view_name
    union_sql = " UNION ALL ".join(pieces)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT h.id, max(h.minor) AS prev_minor
        FROM ({union_sql}) h
        JOIN {version_lookup_sql} tv ON tv.id = h.id AND tv.version = h.version
        GROUP BY h.id
    """)
    return view_name


def _history_node(con, promoted_keys: list[str]) -> None:
    """``history_own_node`` (TEMP TABLE): one row per (id, version) in this
    batch's ``node_all`` occurrences for ids that passed the extent filter
    (``kept_node``) and are genuinely newer than what existed before this
    batch (``exists_node``) -- section 5.1's "every resolved row ... becomes
    a history row" plus "keep every version's meta row"."""
    con.execute("""
        CREATE OR REPLACE TEMP VIEW history_all_node_f AS
        SELECT a.* FROM batch_node_all_raw a
        JOIN kept_node k ON k.id = a.id
        LEFT JOIN exists_node p ON p.id = a.id
        WHERE p.id IS NULL OR a.version > p.version
    """)
    promoted_eff = common.promoted_select(promoted_keys, tags_expr="eff_tags")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE history_own_node AS
        WITH base AS (
            SELECT a.*, CASE WHEN a.deleted THEN NULL ELSE a.tags END AS eff_tags
            FROM history_all_node_f a
        )
        SELECT b.id,
               CASE WHEN b.deleted THEN NULL ELSE b.lat_e7 END AS lat_e7,
               CASE WHEN b.deleted THEN NULL ELSE b.lon_e7 END AS lon_e7,
               b.eff_tags AS tags,
               {promoted_eff},
               b.version, b.changeset, b.timestamp, b.uid, b."user",
               CASE WHEN b.deleted THEN NULL ELSE r.hilbert END AS hilbert,
               CASE WHEN b.deleted THEN r.prev_cell ELSE r.cell END AS cell,
               0 AS minor, b.timestamp AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to,
               (NOT b.deleted) AS visible,
               CASE WHEN b.version = r.version THEN r.prev_cell ELSE NULL END AS __prev_cell
        FROM base b JOIN resolved_node r ON r.id = b.id
    """)


def _history_way(con, store, man: manifest_mod.Manifest, promoted_keys: list[str],
                  touched_way_ids: np.ndarray, batch_ts_sql: str) -> None:
    con.execute("""
        CREATE OR REPLACE TEMP VIEW history_all_way_f AS
        SELECT a.* FROM batch_way_all_raw a
        JOIN kept_way k ON k.id = a.id
        LEFT JOIN exists_way p ON p.id = a.id
        WHERE p.id IS NULL OR a.version > p.version
    """)
    promoted_eff = common.promoted_select(promoted_keys, tags_expr="eff_tags")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE history_own_way AS
        WITH base AS (
            SELECT a.*, CASE WHEN a.deleted THEN NULL ELSE a.tags END AS eff_tags,
                   CASE WHEN a.deleted THEN NULL ELSE a.refs END AS eff_refs
            FROM history_all_way_f a
        )
        SELECT b.id, b.eff_refs AS refs, b.eff_tags AS tags,
               {promoted_eff},
               b.version, b.changeset, b.timestamp, b.uid, b."user",
               CASE WHEN b.deleted THEN NULL ELSE r.xmin_e7 END AS xmin_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.ymin_e7 END AS ymin_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.xmax_e7 END AS xmax_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.ymax_e7 END AS ymax_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.geometry END AS geometry,
               CASE WHEN b.deleted THEN NULL ELSE r.is_closed END AS is_closed,
               CASE WHEN b.deleted THEN NULL ELSE r.is_area END AS is_area,
               CASE WHEN b.deleted THEN NULL ELSE r.centroid_lat_e7 END AS centroid_lat_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.centroid_lon_e7 END AS centroid_lon_e7,
               CASE WHEN b.deleted THEN r.prev_cell ELSE r.cell END AS cell,
               CASE WHEN b.deleted THEN NULL ELSE r.hilbert END AS hilbert,
               0 AS minor, b.timestamp AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to,
               (NOT b.deleted) AS visible,
               CASE WHEN b.version = r.version THEN r.prev_cell ELSE NULL END AS __prev_cell
        FROM base b JOIN resolved_way r ON r.id = b.id
    """)

    _history_touched_only_ids(con, touched_way_ids, "kept_way", "minor_only_way_ids_t")
    con.execute("""
        CREATE OR REPLACE TEMP VIEW way_minor_dep_ts AS
        SELECT w.id AS way_id, n.timestamp AS ts
        FROM resolved_way w, UNNEST(w.refs) AS t(ref)
        JOIN kept_node n ON n.id = t.ref
        WHERE w.id IN (SELECT id FROM minor_only_way_ids_t)
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE way_minor_valid_from AS
        SELECT m.id, COALESCE(MAX(d.ts), {batch_ts_sql}) AS valid_from
        FROM minor_only_way_ids_t m LEFT JOIN way_minor_dep_ts d ON d.way_id = m.id
        GROUP BY m.id
    """)
    prev_minor_view = _history_prev_minor_view(
        con, store, man, "way", "minor_only_way_ids_t",
        "(SELECT id, version FROM resolved_way WHERE id IN (SELECT id FROM minor_only_way_ids_t))",
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE way_minor_meta AS
        SELECT m.id, COALESCE(pm.prev_minor, 0) + 1 AS minor, v.valid_from
        FROM minor_only_way_ids_t m
        JOIN way_minor_valid_from v ON v.id = m.id
        LEFT JOIN {prev_minor_view} pm ON pm.id = m.id
    """)
    promoted_r = common.promoted_select(promoted_keys, tags_expr="r.tags")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE history_minor_way AS
        SELECT r.id, r.refs, r.tags,
               {promoted_r},
               r.version, r.changeset, r.timestamp, r.uid, r."user",
               r.xmin_e7, r.ymin_e7, r.xmax_e7, r.ymax_e7, r.geometry, r.is_closed, r.is_area,
               r.centroid_lat_e7, r.centroid_lon_e7, r.cell, r.hilbert,
               mm.minor AS minor, mm.valid_from AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to,
               TRUE AS visible,
               r.prev_cell AS __prev_cell
        FROM resolved_way r JOIN way_minor_meta mm ON mm.id = r.id
        WHERE NOT r.deleted
    """)


def _history_relation(con, store, man: manifest_mod.Manifest, promoted_keys: list[str],
                       touched_relation_ids: np.ndarray, batch_ts_sql: str) -> None:
    con.execute("""
        CREATE OR REPLACE TEMP VIEW history_all_relation_f AS
        SELECT a.* FROM batch_relation_all_raw a
        JOIN kept_relation k ON k.id = a.id
        LEFT JOIN exists_relation p ON p.id = a.id
        WHERE p.id IS NULL OR a.version > p.version
    """)
    promoted_eff = common.promoted_select(promoted_keys, tags_expr="eff_tags")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE history_own_relation AS
        WITH base AS (
            SELECT a.*, CASE WHEN a.deleted THEN NULL ELSE a.tags END AS eff_tags,
                   CASE WHEN a.deleted THEN NULL ELSE a.members END AS eff_members
            FROM history_all_relation_f a
        )
        SELECT b.id, b.eff_members AS members, b.eff_tags AS tags,
               {promoted_eff},
               b.version, b.changeset, b.timestamp, b.uid, b."user",
               CASE WHEN b.deleted THEN NULL ELSE r.xmin_e7 END AS xmin_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.ymin_e7 END AS ymin_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.xmax_e7 END AS xmax_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.ymax_e7 END AS ymax_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.geometry END AS geometry,
               CASE WHEN b.deleted THEN NULL ELSE r.centroid_lat_e7 END AS centroid_lat_e7,
               CASE WHEN b.deleted THEN NULL ELSE r.centroid_lon_e7 END AS centroid_lon_e7,
               CASE WHEN b.deleted THEN r.prev_cell ELSE r.cell END AS cell,
               CASE WHEN b.deleted THEN NULL ELSE r.hilbert END AS hilbert,
               0 AS minor, b.timestamp AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to,
               (NOT b.deleted) AS visible,
               CASE WHEN b.version = r.version THEN r.prev_cell ELSE NULL END AS __prev_cell
        FROM base b JOIN resolved_relation r ON r.id = b.id
    """)

    _history_touched_only_ids(con, touched_relation_ids, "kept_relation", "minor_only_relation_ids_t")
    con.execute("""
        CREATE OR REPLACE TEMP VIEW relation_minor_dep_ts AS
        SELECT rm.rel_id, kn.timestamp AS ts
        FROM (SELECT rr.id AS rel_id, m.ref AS mref FROM resolved_relation rr, UNNEST(rr.members) AS t(m)
              WHERE m.type = 'n' AND rr.id IN (SELECT id FROM minor_only_relation_ids_t)) rm
        JOIN kept_node kn ON kn.id = rm.mref
        UNION ALL
        SELECT rm.rel_id, kw.timestamp AS ts
        FROM (SELECT rr.id AS rel_id, m.ref AS mref FROM resolved_relation rr, UNNEST(rr.members) AS t(m)
              WHERE m.type = 'w' AND rr.id IN (SELECT id FROM minor_only_relation_ids_t)) rm
        JOIN kept_way kw ON kw.id = rm.mref
        UNION ALL
        SELECT rm.rel_id, kr.timestamp AS ts
        FROM (SELECT rr.id AS rel_id, m.ref AS mref FROM resolved_relation rr, UNNEST(rr.members) AS t(m)
              WHERE m.type = 'r' AND rr.id IN (SELECT id FROM minor_only_relation_ids_t)) rm
        JOIN kept_relation kr ON kr.id = rm.mref
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE relation_minor_valid_from AS
        SELECT m.id, COALESCE(MAX(d.ts), {batch_ts_sql}) AS valid_from
        FROM minor_only_relation_ids_t m LEFT JOIN relation_minor_dep_ts d ON d.rel_id = m.id
        GROUP BY m.id
    """)
    prev_minor_view = _history_prev_minor_view(
        con, store, man, "relation", "minor_only_relation_ids_t",
        "(SELECT id, version FROM resolved_relation WHERE id IN (SELECT id FROM minor_only_relation_ids_t))",
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE relation_minor_meta AS
        SELECT m.id, COALESCE(pm.prev_minor, 0) + 1 AS minor, v.valid_from
        FROM minor_only_relation_ids_t m
        JOIN relation_minor_valid_from v ON v.id = m.id
        LEFT JOIN {prev_minor_view} pm ON pm.id = m.id
    """)
    promoted_r = common.promoted_select(promoted_keys, tags_expr="r.tags")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE history_minor_relation AS
        SELECT r.id, r.members, r.tags,
               {promoted_r},
               r.version, r.changeset, r.timestamp, r.uid, r."user",
               r.xmin_e7, r.ymin_e7, r.xmax_e7, r.ymax_e7, r.geometry,
               r.centroid_lat_e7, r.centroid_lon_e7, r.cell, r.hilbert,
               mm.minor AS minor, mm.valid_from AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to,
               TRUE AS visible,
               r.prev_cell AS __prev_cell
        FROM resolved_relation r JOIN relation_minor_meta mm ON mm.id = r.id
        WHERE NOT r.deleted
    """)


def _finalize_history_batch(con, typ: str, promoted_keys: list[str], has_minor: bool) -> None:
    """From ``history_own_{typ}`` (and ``history_minor_{typ}`` for way/
    relation), each with the spatial column set plus a bookkeeping
    ``__prev_cell`` column, builds ``history_batch_spatial_{typ}`` (state
    rows + move-tombstone rows, section 2.1) and ``history_batch_byid_{typ}``
    (state rows only -- "the byid copy has no move tombstones")."""
    spatial_cols = history_schema.history_columns(SPATIAL_COLUMNS[typ](promoted_keys))
    byid_cols = history_schema.history_columns(BYID_COLUMNS[typ](promoted_keys))
    if has_minor:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE history_spatial_state_{typ} AS
            SELECT {', '.join(spatial_cols)}, __prev_cell FROM history_own_{typ}
            UNION ALL
            SELECT {', '.join(spatial_cols)}, __prev_cell FROM history_minor_{typ}
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE history_spatial_state_{typ} AS
            SELECT {', '.join(spatial_cols)}, __prev_cell FROM history_own_{typ}
        """)
    type_map = _history_type_map(promoted_keys)
    overrides = {
        "id": "id", "version": "version", "minor": "minor", "valid_from": "valid_from",
        "timestamp": "valid_from", "cell": "__prev_cell", "visible": "FALSE",
    }
    tomb_select = _history_null_or_override(spatial_cols, type_map, overrides)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE history_batch_spatial_{typ} AS
        SELECT {', '.join(spatial_cols)} FROM history_spatial_state_{typ}
        UNION ALL
        SELECT {tomb_select} FROM history_spatial_state_{typ}
        WHERE __prev_cell IS NOT NULL AND __prev_cell != cell
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE history_batch_byid_{typ} AS
        SELECT {', '.join(byid_cols)} FROM history_spatial_state_{typ}
    """)


def _build_history_batch(
    con, store, man: manifest_mod.Manifest, promoted_keys: list[str], kept: dict[str, str],
    touched_way_ids: np.ndarray, touched_relation_ids: np.ndarray, batch_timestamp: Optional[str],
) -> bool:
    """Populates ``history_batch_{byid,spatial}_{node,way,relation}`` TEMP
    TABLEs for this run (docs/m4-contracts.md section 5.1). Returns True
    iff at least one type has at least one row -- the caller skips writing
    tiers otherwise ("a run that touches nothing writes no tier file")."""
    batch_dt = _parse_ts(batch_timestamp)
    batch_ts_sql = f"TIMESTAMP '{batch_dt.strftime('%Y-%m-%d %H:%M:%S')}'" if batch_dt else "NULL::TIMESTAMP"

    _history_node(con, promoted_keys)
    _finalize_history_batch(con, "node", promoted_keys, has_minor=False)

    _history_way(con, store, man, promoted_keys, touched_way_ids, batch_ts_sql)
    _finalize_history_batch(con, "way", promoted_keys, has_minor=True)

    _history_relation(con, store, man, promoted_keys, touched_relation_ids, batch_ts_sql)
    _finalize_history_batch(con, "relation", promoted_keys, has_minor=True)

    total = sum(con.execute(f"SELECT count(*) FROM history_batch_byid_{t}").fetchone()[0] for t in _TYPES)
    return total > 0


# --------------------------------------------------------------------------
# history tier fold (append-only) + write
# --------------------------------------------------------------------------


def _history_load_old(con, store, typ: str, tier_meta: Optional[dict]) -> tuple[Optional[str], Optional[str]]:
    if tier_meta is None:
        return None, None
    files = tier_meta["files"][typ]
    byid_url = store.url(files["byid"])
    spatial_url = store.url(files["spatial"])
    byid_name = f"__hist_old_byid_{typ}_{_uid()}"
    spatial_name = f"__hist_old_spatial_{typ}_{_uid()}"
    con.execute(f"CREATE OR REPLACE TEMP VIEW {byid_name} AS SELECT * FROM read_parquet('{_esc(byid_url)}')")
    con.execute(f"CREATE OR REPLACE TEMP VIEW {spatial_name} AS SELECT * FROM read_parquet('{_esc(spatial_url)}')")
    return byid_name, spatial_name


def _history_append_tables(
    con, typ: str, promoted_keys: list[str], new_byid: str, old_byid: Optional[str],
    new_spatial: str, old_spatial: Optional[str],
) -> tuple[str, str]:
    """Append-only fold (docs/m4-contracts.md section 2.2: "the hour tier's
    new version = old hour rows + this run's rows ... nothing is ever
    merged by id")."""
    byid_cols = history_schema.history_columns(BYID_COLUMNS[typ](promoted_keys))
    spatial_cols = history_schema.history_columns(SPATIAL_COLUMNS[typ](promoted_keys))
    out_byid = f"__hist_merged_byid_{typ}_{_uid()}"
    out_spatial = f"__hist_merged_spatial_{typ}_{_uid()}"
    if old_byid is None:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {out_byid} AS SELECT {', '.join(byid_cols)} FROM {new_byid}")
        con.execute(f"CREATE OR REPLACE TEMP TABLE {out_spatial} AS SELECT {', '.join(spatial_cols)} FROM {new_spatial}")
    else:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {out_byid} AS
            SELECT {', '.join(byid_cols)} FROM {old_byid}
            UNION ALL SELECT {', '.join(byid_cols)} FROM {new_byid}
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {out_spatial} AS
            SELECT {', '.join(spatial_cols)} FROM {old_spatial}
            UNION ALL SELECT {', '.join(spatial_cols)} FROM {new_spatial}
        """)
    return out_byid, out_spatial


def _write_history_tier_version(
    con, root: Path, man: manifest_mod.Manifest, promoted_keys: list[str], tier: str,
    merged: dict[str, tuple[str, str]], seq_from: Optional[int], seq_to: Optional[int],
    timestamp: Optional[str], version: int,
) -> Optional[tuple[dict, int]]:
    total_rows = sum(con.execute(f"SELECT count(*) FROM {merged[t][0]}").fetchone()[0] for t in _TYPES)
    if total_rows == 0:
        return None
    gen = (man.history or {}).get("generation") or man.generation
    out_dir = root / history_schema.tier_dir(gen, tier, version)
    out_dir.mkdir(parents=True, exist_ok=True)
    files: dict[str, Any] = {}
    rows: dict[str, int] = {}
    cells: dict[str, list[str]] = {}
    total_bytes = 0
    for typ in _TYPES:
        byid_name, spatial_name = merged[typ]
        byid_cols = history_schema.history_columns(BYID_COLUMNS[typ](promoted_keys))
        spatial_cols = history_schema.history_columns(SPATIAL_COLUMNS[typ](promoted_keys))
        byid_path = out_dir / f"{typ}.byid.parquet"
        spatial_path = out_dir / f"{typ}.spatial.parquet"
        n_rows, n_bytes = common.copy_to_parquet(
            con, f"SELECT {', '.join(byid_cols)} FROM {byid_name} ORDER BY id, valid_from",
            byid_path, row_group_size_bytes=1_000_000,
        )
        _srows, s_bytes = common.copy_to_parquet(
            con, f"SELECT {', '.join(spatial_cols)} FROM {spatial_name} ORDER BY cell, id, valid_from",
            spatial_path, row_group_size_bytes=1_000_000,
        )
        files[typ] = {"spatial": _rel(root, spatial_path), "byid": _rel(root, byid_path)}
        rows[typ] = n_rows
        total_bytes += n_bytes + s_bytes
        cell_rows = con.execute(f"SELECT DISTINCT cell FROM {spatial_name} WHERE cell IS NOT NULL").fetchall()
        cells[typ] = sorted(c[0] for c in cell_rows)
    meta = {
        "version": version, "seq_from": seq_from, "seq_to": seq_to, "timestamp": timestamp,
        "rows": rows, "cells": cells, "files": files,
    }
    return meta, total_bytes


def _write_history_tiers(
    con, root: Path, store, man: manifest_mod.Manifest, promoted_keys: list[str],
    batch_tables: dict[str, tuple[str, str]], first_seq: int, last_seq: int,
    batch_timestamp: Optional[str], hour_crossed: bool, day_crossed: bool,
) -> tuple[dict[str, int], dict[str, int], dict]:
    """Mirrors ``_write_tiers``'s hour/day/week roll (docs/m2-contracts.md
    section 5 step 7, folding at the same instants -- ``hour_crossed``/
    ``day_crossed`` are computed once from the *delta* tiers by the caller
    and passed in here unchanged), but with append-only folds
    (``_history_append_tables``) instead of newest-wins-per-id merges.
    Returns (tier_versions, tier_bytes, new "history" manifest dict)."""
    hist = man.history or {}
    old_tiers = hist.get("tiers") or {}
    hour_old = old_tiers.get("hour")
    day_old = old_tiers.get("day")
    week_old = old_tiers.get("week")

    tier_versions: dict[str, int] = {}
    tier_bytes: dict[str, int] = {}
    new_tiers: dict = dict(old_tiers)

    if not hour_crossed:
        merged_hour = {}
        for typ in _TYPES:
            old_byid, old_spatial = _history_load_old(con, store, typ, hour_old)
            new_byid, new_spatial = batch_tables[typ]
            merged_hour[typ] = _history_append_tables(con, typ, promoted_keys, new_byid, old_byid, new_spatial, old_spatial)
        seq_from = hour_old["seq_from"] if hour_old else first_seq
        version = (hour_old["version"] if hour_old else 0) + 1
        out = _write_history_tier_version(con, root, man, promoted_keys, "hour", merged_hour, seq_from, last_seq, batch_timestamp, version)
        if out:
            meta, nbytes = out
            new_tiers["hour"] = meta
            tier_versions["hour"] = meta["version"]
            tier_bytes["hour"] = nbytes
        return tier_versions, tier_bytes, {**hist, "tiers": new_tiers}

    # ---- hour boundary crossed: fold hour_old into day, hour' = batch alone ---
    merged_day = {}
    for typ in _TYPES:
        hour_byid, hour_spatial = _history_load_old(con, store, typ, hour_old)
        day_byid, day_spatial = _history_load_old(con, store, typ, day_old)
        merged_day[typ] = _history_append_tables(con, typ, promoted_keys, hour_byid, day_byid, hour_spatial, day_spatial)
    day_seq_from = min(x for x in [(day_old or {}).get("seq_from"), hour_old["seq_from"]] if x is not None)
    day_seq_to = hour_old["seq_to"]
    day_timestamp = hour_old["timestamp"]
    day_version = (day_old["version"] if day_old else 0) + 1

    if day_crossed:
        merged_week = {}
        for typ in _TYPES:
            week_byid, week_spatial = _history_load_old(con, store, typ, week_old)
            mb, ms = merged_day[typ]
            merged_week[typ] = _history_append_tables(con, typ, promoted_keys, mb, week_byid, ms, week_spatial)
        week_seq_from = min(x for x in [(week_old or {}).get("seq_from"), day_seq_from] if x is not None)
        week_seq_to = day_seq_to
        week_timestamp = day_timestamp
        week_version = (week_old["version"] if week_old else 0) + 1
        out = _write_history_tier_version(con, root, man, promoted_keys, "week", merged_week, week_seq_from, week_seq_to, week_timestamp, week_version)
        if out:
            meta, nbytes = out
            new_tiers["week"] = meta
            tier_versions["week"] = meta["version"]
            tier_bytes["week"] = nbytes
        merged_day_final = {typ: batch_tables[typ] for typ in _TYPES}
        out = _write_history_tier_version(con, root, man, promoted_keys, "day", merged_day_final, first_seq, last_seq, batch_timestamp, day_version)
    else:
        out = _write_history_tier_version(con, root, man, promoted_keys, "day", merged_day, day_seq_from, day_seq_to, day_timestamp, day_version)
    if out:
        meta, nbytes = out
        new_tiers["day"] = meta
        tier_versions["day"] = meta["version"]
        tier_bytes["day"] = nbytes

    merged_hour_final = {typ: batch_tables[typ] for typ in _TYPES}
    hour_version = (hour_old["version"] if hour_old else 0) + 1
    out = _write_history_tier_version(con, root, man, promoted_keys, "hour", merged_hour_final, first_seq, last_seq, batch_timestamp, hour_version)
    if out:
        meta, nbytes = out
        new_tiers["hour"] = meta
        tier_versions["hour"] = meta["version"]
        tier_bytes["hour"] = nbytes

    return tier_versions, tier_bytes, {**hist, "tiers": new_tiers}


def _upload_new_history_tier_files(store, write_root: Path, new_history: Optional[dict], tier_versions: dict) -> None:
    """s3:// counterpart of ``_upload_new_tier_files``, for history tier
    files (docs/m4-contracts.md section 5.1: "the s3:// root path must work
    for tier files")."""
    tiers = (new_history or {}).get("tiers") or {}
    for tier_name in tier_versions:
        entry = tiers.get(tier_name) or {}
        files = entry.get("files") or {}
        for _typ, kinds in files.items():
            for _kind, relpath in (kinds or {}).items():
                if relpath:
                    store.upload_file(str(write_root / relpath), relpath)
