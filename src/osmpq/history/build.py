"""``osmpq history build``: docs/m4-contracts.md section 4, sub-sections 4.1
(states and minor versions) and 4.3 (manifest assembly). Section 4's ingest
layer lives in :mod:`osmpq.history.ingest`; this module turns the ingested
raw versions into history *states* (section 2.1) and writes them through
:mod:`osmpq.history.writer`.

Minor-version performance (section 4, "Memory"): the event join that
detects a way's/relation's minor versions only considers node versions from
nodes that have **more than one known version** (nodes actually touched by
a diff/osh) -- Minnesota's ~54M single-version nodes never enter that join
at all. This is the documented trade-off: a way whose ref was an *unknown*
(dangling) node id in the base extract, later resolved by that node's
*first and only* version landing inside the way's still-open version
window, would in principle also need a minor version at that instant, and
this implementation does not generate one for that narrow case (the way's
geometry only reflects the new point from the way's own next real version).
Real dangling-ref-resolved-by-a-brand-new-node-within-the-same-window
sequences are rare; the alternative (joining every ref against every node
version) is the exact cost the contract calls out as unaffordable at
Minnesota scale.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

from osmpq.build import common
from osmpq.history import ingest as ingest_mod
from osmpq.history import writer as writer_mod
from osmpq.history.schema import history_columns
from osmpq.layout import cells as cells_mod
from osmpq.layout import hilbert as hilbert_mod
from osmpq.layout import manifest as manifest_mod
from osmpq.update.updater import BYID_COLUMNS, SPATIAL_COLUMNS

_TYPES = ("node", "way", "relation")


def _log(msg: str) -> None:
    common.log("osmpq history build", msg)


@dataclass
class HistoryBuildOptions:
    root: str
    pbf_path: Optional[str] = None
    osc_paths: Optional[list[str]] = None
    osh_path: Optional[str] = None
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    tmpdir: Optional[str] = None


def _q(cols: list[str]) -> str:
    return ", ".join(cols)


def _describe_types(con, table: str) -> dict[str, str]:
    return {row[0]: row[1] for row in con.execute(f"DESCRIBE {table}").fetchall()}


# --------------------------------------------------------------------------
# generic: valid_to fill + move tombstones (section 2.1, shared by all 3 types)
# --------------------------------------------------------------------------


def _finalize_states(con, states_table: str, out_table: str) -> None:
    """``states_table`` must already carry every own+minor state (one row
    per (id, valid_from), all columns of the eventual spatial-copy schema
    incl. ``minor``/``valid_from``/``visible``/``cell``, but not yet
    ``valid_to``). Fills ``valid_to`` (LEAD per id) and appends move
    tombstones (section 2.1: a visible state landing in a different cell
    than the previous state gets an extra invisible row in the old cell,
    carrying only ``id``/``cell``/``version``/``minor``/``valid_from``/
    ``visible``); writes the result (spatial-copy shape, all real states +
    move tombstones) to ``out_table``.

    ``out_table`` and its own ``_vt`` step are VIEWs, not TABLEs: they are
    each read at most twice (the tombstone branch below, and whatever the
    caller does with ``out_table`` -- always exactly one
    :func:`_reorder_to_schema` call, which is what actually materializes
    the result). At Minnesota's ~55M node rows, materializing every step
    of this chain as its own TABLE was enough to push a first attempt at
    this build over the sandbox's memory cgroup limit (see the workstream
    report); recomputing a `LEAD`/`LAG` window pass an extra time is cheap
    next to holding another full copy of the data in memory."""
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {out_table}_vt AS
        SELECT *, lead(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS valid_to
        FROM {states_table}
    """)
    types = _describe_types(con, f"{out_table}_vt")
    cols = [c for c in types if c != "valid_to"] + ["valid_to"]
    keep = {"id", "version", "minor", "valid_from", "visible", "valid_to"}
    real_select = [f'"{c}"' for c in cols]
    tomb_select = []
    for c in cols:
        if c == "cell":
            tomb_select.append('prev_cell AS "cell"')
        elif c == "visible":
            tomb_select.append('FALSE AS "visible"')
        elif c == "valid_to":
            tomb_select.append('NULL::TIMESTAMP AS "valid_to"')
        elif c in keep:
            tomb_select.append(f'"{c}"')
        else:
            tomb_select.append(f'NULL::{types[c]} AS "{c}"')
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {out_table} AS
        SELECT {_q(real_select)} FROM {out_table}_vt
        UNION ALL BY NAME
        SELECT {_q(tomb_select)} FROM (
            SELECT *, lag("cell") OVER (PARTITION BY "id" ORDER BY "valid_from") AS prev_cell FROM {out_table}_vt
        ) t
        WHERE "visible" AND prev_cell IS NOT NULL AND "cell" IS NOT NULL AND "cell" != prev_cell
    """)


def _project(con, table: str, cols: list[str], out: str) -> None:
    con.execute(f"CREATE OR REPLACE TEMP VIEW {out} AS SELECT {_q(cols)} FROM {table}")


def _reorder_to_schema(con, table: str, base_cols: list[str], out: str) -> None:
    """Final column order = ``osmpq.history.schema.history_columns(base_cols)``
    (base columns, then ``minor, valid_from, valid_to, visible`` in that
    order) -- the contract's declared order, independent of whatever order
    the intermediate SQL happened to produce."""
    cols = history_columns(base_cols)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {out} AS SELECT {_q(cols)} FROM {table}")


# --------------------------------------------------------------------------
# nodes (section 4.1.1)
# --------------------------------------------------------------------------


def _compute_node_states(con, promoted_keys: list[str], leaf_index: cells_mod.LeafIndex) -> None:
    # A VIEW, not a TABLE: at Minnesota scale (~55M rows) materializing this
    # (a near-full copy of hist_node_raw, which itself must stay resident
    # for the way/relation passes) is what pushed a first attempt at this
    # build over the sandbox's memory cgroup limit -- see the workstream
    # report. It costs re-evaluating the promoted-key extraction twice
    # (once per reader below), which is cheap next to holding another
    # 55M-row copy in memory.
    promoted_sql = common.promoted_select(promoted_keys, tags_expr="tags")
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW node_own_pre AS
        SELECT id, lat_e7, lon_e7, tags, {promoted_sql},
               version, changeset, timestamp, uid, "user",
               0 AS minor, timestamp AS valid_from, visible
        FROM hist_node_raw
    """)
    ids, lat_e7, lon_e7, valid_from, visible = con.execute(
        "SELECT id, lat_e7, lon_e7, valid_from, visible FROM node_own_pre"
    ).fetchnumpy().values()
    n = len(ids)
    own_cell = np.array([None] * n, dtype=object)
    own_hilbert = np.zeros(n, dtype=np.uint64)
    vis_mask = np.asarray(visible, dtype=bool)
    if vis_mask.any():
        lat_arr = np.asarray(lat_e7.filled(0) if isinstance(lat_e7, np.ma.MaskedArray) else lat_e7, dtype=np.int64)
        lon_arr = np.asarray(lon_e7.filled(0) if isinstance(lon_e7, np.ma.MaskedArray) else lon_e7, dtype=np.int64)
        placed = cells_mod.point_cells_np(lat_arr[vis_mask], lon_arr[vis_mask], leaf_index)
        own_cell[vis_mask] = placed
        own_hilbert[vis_mask] = hilbert_mod.hilbert_keys(lat_arr[vis_mask], lon_arr[vis_mask])
    import pyarrow as pa

    tbl = pa.table({
        "id": pa.array(np.asarray(ids, dtype=np.int64)),
        "valid_from": pa.array(valid_from),
        "own_cell": pa.array(own_cell, type=pa.string()),
        "own_hilbert": pa.array(own_hilbert, type=pa.uint64()),
    })
    del own_cell, own_hilbert, lat_e7, lon_e7, visible, vis_mask
    con.register("_node_cell_arrow", tbl)
    con.execute("CREATE OR REPLACE TEMP TABLE node_cell_assign AS SELECT * FROM _node_cell_arrow")
    con.unregister("_node_cell_arrow")
    del tbl

    # `cell` forward-fill: a node's own_cell is NULL only for a deletion
    # (visible = false), and a node can never have two consecutive
    # invisible states (a delete is terminal until/unless recreated, which
    # is itself a new *visible* state) -- so a plain `lag()` one step back
    # always finds the last real cell, standing in for the general
    # "last_value(... IGNORE NULLS) OVER (ROWS BETWEEN UNBOUNDED
    # PRECEDING...)" frame (used for ways/relations below, where a state
    # can go several NULL-bbox rows before resolving again). That general
    # frame does not appear to spill under DuckDB 1.5.5 the way a plain
    # LAG/LEAD does -- it was the direct cause of an OOM-kill at Minnesota's
    # ~55M node rows even with `memory_limit` set well under the sandbox's
    # cgroup ceiling (see the workstream report).
    promoted_refs = ", ".join(f'n."{k}"' for k in promoted_keys)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE node_states AS
        SELECT n.id, n.lat_e7, n.lon_e7, n.tags, {promoted_refs},
               n.version, n.changeset, n.timestamp, n.uid, n."user",
               n.minor, n.valid_from, n.visible,
               coalesce(a.own_cell, lag(a.own_cell) OVER (PARTITION BY n.id ORDER BY n.valid_from)) AS cell,
               CASE WHEN n.visible THEN a.own_hilbert END AS hilbert
        FROM node_own_pre n JOIN node_cell_assign a ON a.id = n.id AND a.valid_from = n.valid_from
    """)
    con.execute("DROP TABLE node_cell_assign")
    # `multi_version_node_ids` (the way/relation minor-version event join's
    # candidate set) is computed from `node_states` -- row-for-row the same
    # set of (id, version) as `hist_node_raw` -- so `hist_node_raw` (a
    # second, ~55M-row-wide, no-longer-needed copy of the same data) can be
    # dropped here instead of staying resident through the way/relation
    # passes. Keeping it around was the last big contributor to a memory
    # cgroup OOM-kill at Minnesota scale even after the other fixes in this
    # module (see the module docstring and the workstream report).
    con.execute("""
        CREATE OR REPLACE TEMP TABLE multi_version_node_ids AS
        SELECT id FROM node_states GROUP BY id HAVING count(*) > 1
    """)
    con.execute("DROP TABLE hist_node_raw")
    # The way/relation passes only ever need this narrow shape of a node
    # state (event timestamps + a position to resolve geometry against);
    # keeping the *full* `node_states` (tags, 12 promoted columns, meta)
    # resident through those passes -- on top of everything else building a
    # way's/relation's states needs -- was enough to OOM-kill a Minnesota-
    # scale run even after every other fix in this module (see the module
    # docstring and the workstream report). The caller writes node's own
    # output and drops `node_states` before calling `_compute_way_states`,
    # keeping only this ~5-column table alive for the rest of the build.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE node_states_geo AS
        SELECT id, valid_from, visible, lat_e7, lon_e7 FROM node_states
    """)


def _write_node_final(con, promoted_keys: list[str]) -> None:
    spatial_cols = SPATIAL_COLUMNS["node"](promoted_keys)
    byid_cols = BYID_COLUMNS["node"](promoted_keys)
    _project(con, "node_states", spatial_cols + ["minor", "valid_from", "visible"], "node_states_spatial_shape")
    _finalize_states(con, "node_states_spatial_shape", "__node_final_spatial_raw")
    _reorder_to_schema(con, "__node_final_spatial_raw", spatial_cols, "node_final_spatial")
    # byid: own states only (no move tombstones -- section 2.2).
    _project(con, "node_states", byid_cols + ["minor", "valid_from", "visible"], "__tmp_node_byid")
    con.execute("""
        CREATE OR REPLACE TEMP VIEW __node_final_byid_raw AS
        SELECT *, lead(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS valid_to
        FROM __tmp_node_byid
    """)
    _reorder_to_schema(con, "__node_final_byid_raw", byid_cols, "node_final_byid")


# --------------------------------------------------------------------------
# ways (section 4.1.2)
# --------------------------------------------------------------------------


def _compute_way_states(
    con, promoted_keys: list[str], leaf_index: cells_mod.LeafIndex,
    ancestor_depths: list[int], max_depth: int,
) -> None:
    promoted_sql = common.promoted_select(promoted_keys, tags_expr="tags")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE way_own AS
        SELECT id, version, tags, refs, changeset, timestamp, uid, "user", {promoted_sql},
               timestamp AS valid_from,
               lead(timestamp) OVER (PARTITION BY id ORDER BY timestamp) AS valid_to,
               visible,
               CASE WHEN visible THEN (coalesce(len(refs), 0) >= 4 AND refs[1] = refs[len(refs)]) END AS is_closed
        FROM hist_way_raw
    """)
    con.execute("DROP TABLE hist_way_raw")
    con.execute("""
        ALTER TABLE way_own ADD COLUMN is_area BOOLEAN
    """)
    con.execute("""
        UPDATE way_own SET is_area = (
            is_closed
            AND coalesce(tags['area'], '') != 'no'
            AND NOT (
                (tags['highway'] IS NOT NULL OR tags['barrier'] IS NOT NULL)
                AND coalesce(tags['area'], '') != 'yes'
            )
        )
    """)

    # `multi_version_node_ids` was already computed (from `node_states`) in
    # `_compute_node_states`, so `hist_node_raw` could be dropped there.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW way_refs_flat_events AS
        SELECT w.id AS way_id, w.version AS way_version, w.valid_from AS win_from, w.valid_to AS win_to, t.ref
        FROM way_own w, UNNEST(w.refs) AS t(ref) WHERE w.visible
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_minor_numbered AS
        SELECT way_id, way_version, event_ts,
               row_number() OVER (PARTITION BY way_id, way_version ORDER BY event_ts) AS minor
        FROM (
            SELECT DISTINCT wf.way_id, wf.way_version, ns.valid_from AS event_ts
            FROM way_refs_flat_events wf
            JOIN multi_version_node_ids mv ON mv.id = wf.ref
            JOIN node_states_geo ns ON ns.id = wf.ref
            WHERE ns.valid_from > wf.win_from AND (wf.win_to IS NULL OR ns.valid_from < wf.win_to)
        ) e
    """)

    promoted_wo = ", ".join(f'wo."{k}"' for k in promoted_keys)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE way_states_meta AS
        SELECT id, version, tags, refs, changeset, timestamp, uid, "user", {promoted_sql},
               is_closed, is_area, 0 AS minor, valid_from, visible
        FROM way_own
        UNION ALL BY NAME
        SELECT wo.id, wo.version, wo.tags, wo.refs, wo.changeset, wo.timestamp, wo.uid, wo."user", {promoted_wo},
               wo.is_closed, wo.is_area, wm.minor, wm.event_ts AS valid_from, TRUE AS visible
        FROM way_minor_numbered wm JOIN way_own wo ON wo.id = wm.way_id AND wo.version = wm.way_version
    """)

    # geometry/bbox for every own+minor state, via an ASOF join onto each
    # ref's node state valid at that state's own valid_from (section 4.1.2:
    # "the node with the greatest valid_from <= ts"; a node not yet existing
    # or deleted contributes nothing).
    con.execute("""
        CREATE OR REPLACE TEMP VIEW way_refs_flat_all AS
        SELECT ws.id AS way_id, ws.version, ws.minor, ws.valid_from AS ts, t.ordinal, t.ref
        FROM way_states_meta ws, UNNEST(ws.refs) WITH ORDINALITY AS t(ref, ordinal)
        WHERE ws.visible
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_refs_resolved AS
        SELECT wf.way_id, wf.version, wf.minor, wf.ordinal,
               CASE WHEN ns.visible THEN ns.lat_e7 END AS lat_e7,
               CASE WHEN ns.visible THEN ns.lon_e7 END AS lon_e7
        FROM way_refs_flat_all wf
        ASOF LEFT JOIN node_states_geo ns ON ns.id = wf.ref AND wf.ts >= ns.valid_from
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_geom_agg AS
        SELECT way_id, version, minor,
               CASE WHEN count(lat_e7) > 0 THEN min(lat_e7) END AS ymin_e7,
               CASE WHEN count(lat_e7) > 0 THEN max(lat_e7) END AS ymax_e7,
               CASE WHEN count(lat_e7) > 0 THEN min(lon_e7) END AS xmin_e7,
               CASE WHEN count(lat_e7) > 0 THEN max(lon_e7) END AS xmax_e7,
               CASE WHEN count(lat_e7) >= 2 THEN
                   ST_MakeLine(list(ST_Point(lon_e7 / 1e7, lat_e7 / 1e7) ORDER BY ordinal)
                               FILTER (WHERE lat_e7 IS NOT NULL))
               END AS geometry
        FROM way_refs_resolved GROUP BY way_id, version, minor
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_states_pre_cell AS
        SELECT ws.*, g.ymin_e7, g.ymax_e7, g.xmin_e7, g.xmax_e7, g.geometry
        FROM way_states_meta ws LEFT JOIN way_geom_agg g
          ON g.way_id = ws.id AND g.version = ws.version AND g.minor = ws.minor
    """)

    way_ids_np, ymin, xmin, ymax, xmax, valid_from_np = con.execute(
        "SELECT id, ymin_e7, xmin_e7, ymax_e7, xmax_e7, valid_from FROM way_states_pre_cell"
    ).fetchnumpy().values()
    n = len(way_ids_np)

    def _filled(col):
        if isinstance(col, np.ma.MaskedArray):
            return col.astype("float64").filled(np.nan), np.ma.getmaskarray(col)
        arr = np.asarray(col, dtype="float64")
        return arr, np.zeros(len(arr), dtype=bool)

    ymin_f, ymin_null = _filled(ymin)
    xmin_f, xmin_null = _filled(xmin)
    ymax_f, ymax_null = _filled(ymax)
    xmax_f, xmax_null = _filled(xmax)
    has_bbox = ~(ymin_null | xmin_null | ymax_null | xmax_null)
    own_cell = np.array([None] * n, dtype=object)
    own_hilbert = np.zeros(n, dtype=np.uint64)
    own_clat = np.zeros(n, dtype=np.int64)
    own_clon = np.zeros(n, dtype=np.int64)
    if has_bbox.any():
        sub_cell = cells_mod.containing_cells_v2_np(
            ymin_f[has_bbox].astype(np.int64), xmin_f[has_bbox].astype(np.int64),
            ymax_f[has_bbox].astype(np.int64), xmax_f[has_bbox].astype(np.int64),
            leaf_index, ancestor_depths, max_depth,
        )
        clat_e7 = np.round((ymin_f[has_bbox] + ymax_f[has_bbox]) / 2.0).astype(np.int64)
        clon_e7 = np.round((xmin_f[has_bbox] + xmax_f[has_bbox]) / 2.0).astype(np.int64)
        sub_hilbert = hilbert_mod.hilbert_keys(clat_e7, clon_e7)
        own_cell[has_bbox] = sub_cell
        own_hilbert[has_bbox] = sub_hilbert
        own_clat[has_bbox] = clat_e7
        own_clon[has_bbox] = clon_e7

    import pyarrow as pa

    tbl = pa.table({
        "id": pa.array(np.asarray(way_ids_np, dtype=np.int64)),
        "valid_from": pa.array(valid_from_np),
        "own_cell": pa.array(own_cell, type=pa.string()),
        "own_hilbert": pa.array(own_hilbert, type=pa.uint64()),
        "own_clat": pa.array([int(v) if m else None for v, m in zip(own_clat.tolist(), has_bbox.tolist())], type=pa.int32()),
        "own_clon": pa.array([int(v) if m else None for v, m in zip(own_clon.tolist(), has_bbox.tolist())], type=pa.int32()),
    })
    con.register("_way_cell_arrow", tbl)
    con.execute("CREATE OR REPLACE TEMP TABLE way_cell_assign AS SELECT * FROM _way_cell_arrow")
    con.unregister("_way_cell_arrow")
    del tbl

    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_states AS
        SELECT w.*,
               last_value(a.own_cell IGNORE NULLS) OVER (
                   PARTITION BY w.id ORDER BY w.valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS cell_filled,
               a.own_hilbert, a.own_clat, a.own_clon
        FROM way_states_pre_cell w JOIN way_cell_assign a ON a.id = w.id AND a.valid_from = w.valid_from
    """)
    con.execute("DROP TABLE way_cell_assign")
    con.execute("DROP TABLE way_states_pre_cell")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE way_states_final AS
        SELECT id, refs, tags,
    """ + ", ".join(f'"{k}"' for k in promoted_keys) + """,
               version, changeset, timestamp, uid, "user",
               xmin_e7, ymin_e7, xmax_e7, ymax_e7, geometry, is_closed, is_area,
               own_clat AS centroid_lat_e7, own_clon AS centroid_lon_e7,
               coalesce(cell_filled, 'root') AS cell,
               CASE WHEN xmin_e7 IS NOT NULL THEN own_hilbert END AS hilbert,
               minor, valid_from, visible
        FROM way_states
    """)
    con.execute("DROP TABLE way_states")
    con.execute("DROP TABLE way_own")


def _write_way_final(con, promoted_keys: list[str]) -> None:
    spatial_cols = SPATIAL_COLUMNS["way"](promoted_keys)
    byid_cols = BYID_COLUMNS["way"](promoted_keys)
    _project(con, "way_states_final", spatial_cols + ["minor", "valid_from", "visible"], "way_states_spatial_shape")
    _finalize_states(con, "way_states_spatial_shape", "__way_final_spatial_raw")
    _reorder_to_schema(con, "__way_final_spatial_raw", spatial_cols, "way_final_spatial")
    _project(con, "way_states_final", byid_cols + ["minor", "valid_from", "visible"], "__tmp_way_byid")
    con.execute("""
        CREATE OR REPLACE TEMP VIEW __way_final_byid_raw AS
        SELECT *, lead(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS valid_to
        FROM __tmp_way_byid
    """)
    _reorder_to_schema(con, "__way_final_byid_raw", byid_cols, "way_final_byid")


# --------------------------------------------------------------------------
# relations (section 4.1.3)
# --------------------------------------------------------------------------


def _load_current_relation_bbox(con, root: str, man: manifest_mod.Manifest) -> None:
    """Nested (member-relation) bbox contribution uses the CURRENT relation
    bbox, exactly as the base builder/updater do for one level of relation
    nesting (``osmpq.update.updater._resolve``'s ``extra_relation_bbox``) --
    not a historical, point-in-time bbox (that would need relation states to
    depend on themselves recursively across time)."""
    from osmpq import store as store_mod

    store = store_mod.for_root(root)
    parts = man.byid.get("relation", [])
    if not parts:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE current_relation_bbox AS
            SELECT NULL::BIGINT AS id, NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7,
                   NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7 WHERE FALSE
        """)
        return
    paths = [store.url(p["path"]) for p in parts if store.exists(p["path"])]
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE current_relation_bbox AS
        SELECT id, xmin_e7, ymin_e7, xmax_e7, ymax_e7 FROM read_parquet({paths!r})
    """)


def _compute_relation_states(
    con, promoted_keys: list[str], leaf_index: cells_mod.LeafIndex,
    ancestor_depths: list[int], max_depth: int,
) -> None:
    promoted_sql = common.promoted_select(promoted_keys, tags_expr="tags")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE relation_own AS
        SELECT id, version, tags, members, changeset, timestamp, uid, "user", {promoted_sql},
               timestamp AS valid_from,
               lead(timestamp) OVER (PARTITION BY id ORDER BY timestamp) AS valid_to,
               visible
        FROM hist_relation_raw
    """)
    con.execute("DROP TABLE hist_relation_raw")

    con.execute("""
        CREATE OR REPLACE TEMP VIEW rel_members_flat AS
        SELECT r.id AS rel_id, r.version AS rel_version, r.valid_from AS win_from, r.valid_to AS win_to,
               m.type AS mtype, m.ref AS mref
        FROM relation_own r, UNNEST(r.members) AS t(m) WHERE r.visible
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE relation_minor_numbered AS
        SELECT rel_id, rel_version, event_ts,
               row_number() OVER (PARTITION BY rel_id, rel_version ORDER BY event_ts) AS minor
        FROM (
            SELECT DISTINCT rf.rel_id, rf.rel_version, ns.valid_from AS event_ts
            FROM rel_members_flat rf
            JOIN multi_version_node_ids mv ON mv.id = rf.mref AND rf.mtype = 'n'
            JOIN node_states_geo ns ON ns.id = rf.mref
            WHERE ns.valid_from > rf.win_from AND (rf.win_to IS NULL OR ns.valid_from < rf.win_to)
            UNION ALL
            SELECT DISTINCT rf.rel_id, rf.rel_version, ws.valid_from AS event_ts
            FROM rel_members_flat rf
            JOIN way_states_final ws ON ws.id = rf.mref
            WHERE rf.mtype = 'w'
              AND ws.valid_from > rf.win_from AND (rf.win_to IS NULL OR ws.valid_from < rf.win_to)
        ) e
    """)

    promoted_ro = ", ".join(f'ro."{k}"' for k in promoted_keys)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE relation_states_meta AS
        SELECT id, version, tags, members, changeset, timestamp, uid, "user", {promoted_sql},
               0 AS minor, valid_from, visible
        FROM relation_own
        UNION ALL BY NAME
        SELECT ro.id, ro.version, ro.tags, ro.members, ro.changeset, ro.timestamp, ro.uid, ro."user", {promoted_ro},
               rm.minor, rm.event_ts AS valid_from, TRUE AS visible
        FROM relation_minor_numbered rm JOIN relation_own ro ON ro.id = rm.rel_id AND ro.version = rm.rel_version
    """)

    con.execute("""
        CREATE OR REPLACE TEMP VIEW rel_members_flat_all AS
        SELECT rs.id AS rel_id, rs.version, rs.minor, rs.valid_from AS ts, m.type AS mtype, m.ref AS mref
        FROM relation_states_meta rs, UNNEST(rs.members) AS t(m) WHERE rs.visible
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rel_member_bbox_node AS
        SELECT rf.rel_id, rf.version, rf.minor,
               ns.lat_e7 AS ymin_e7, ns.lat_e7 AS ymax_e7, ns.lon_e7 AS xmin_e7, ns.lon_e7 AS xmax_e7
        FROM rel_members_flat_all rf
        ASOF LEFT JOIN node_states_geo ns ON ns.id = rf.mref AND rf.ts >= ns.valid_from
        WHERE rf.mtype = 'n' AND ns.visible AND ns.lat_e7 IS NOT NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rel_member_bbox_way AS
        SELECT rf.rel_id, rf.version, rf.minor, ws.ymin_e7, ws.ymax_e7, ws.xmin_e7, ws.xmax_e7
        FROM rel_members_flat_all rf
        ASOF LEFT JOIN way_states_final ws ON ws.id = rf.mref AND rf.ts >= ws.valid_from
        WHERE rf.mtype = 'w' AND ws.visible AND ws.xmin_e7 IS NOT NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rel_member_bbox_rel AS
        SELECT rf.rel_id, rf.version, rf.minor, crb.ymin_e7, crb.ymax_e7, crb.xmin_e7, crb.xmax_e7
        FROM rel_members_flat_all rf
        JOIN current_relation_bbox crb ON crb.id = rf.mref
        WHERE rf.mtype = 'r' AND crb.xmin_e7 IS NOT NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rel_bbox_agg AS
        SELECT rel_id, version, minor,
               min(ymin_e7) AS ymin_e7, max(ymax_e7) AS ymax_e7,
               min(xmin_e7) AS xmin_e7, max(xmax_e7) AS xmax_e7
        FROM (
            SELECT * FROM rel_member_bbox_node
            UNION ALL SELECT * FROM rel_member_bbox_way
            UNION ALL SELECT * FROM rel_member_bbox_rel
        ) u
        GROUP BY rel_id, version, minor
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE relation_states_pre_cell AS
        SELECT rs.*, b.ymin_e7, b.ymax_e7, b.xmin_e7, b.xmax_e7
        FROM relation_states_meta rs LEFT JOIN rel_bbox_agg b
          ON b.rel_id = rs.id AND b.version = rs.version AND b.minor = rs.minor
    """)

    rel_ids_np, ymin, xmin, ymax, xmax, valid_from_np = con.execute(
        "SELECT id, ymin_e7, xmin_e7, ymax_e7, xmax_e7, valid_from FROM relation_states_pre_cell"
    ).fetchnumpy().values()
    n = len(rel_ids_np)

    def _filled(col):
        if isinstance(col, np.ma.MaskedArray):
            return col.astype("float64").filled(np.nan), np.ma.getmaskarray(col)
        arr = np.asarray(col, dtype="float64")
        return arr, np.zeros(len(arr), dtype=bool)

    ymin_f, ymin_null = _filled(ymin)
    xmin_f, xmin_null = _filled(xmin)
    ymax_f, ymax_null = _filled(ymax)
    xmax_f, xmax_null = _filled(xmax)
    has_bbox = ~(ymin_null | xmin_null | ymax_null | xmax_null)
    own_cell = np.array([None] * n, dtype=object)
    own_hilbert = np.zeros(n, dtype=np.uint64)
    own_clat = np.zeros(n, dtype=np.int64)
    own_clon = np.zeros(n, dtype=np.int64)
    if has_bbox.any():
        sub_cell = cells_mod.containing_cells_v2_np(
            ymin_f[has_bbox].astype(np.int64), xmin_f[has_bbox].astype(np.int64),
            ymax_f[has_bbox].astype(np.int64), xmax_f[has_bbox].astype(np.int64),
            leaf_index, ancestor_depths, max_depth,
        )
        clat_e7 = np.round((ymin_f[has_bbox] + ymax_f[has_bbox]) / 2.0).astype(np.int64)
        clon_e7 = np.round((xmin_f[has_bbox] + xmax_f[has_bbox]) / 2.0).astype(np.int64)
        sub_hilbert = hilbert_mod.hilbert_keys(clat_e7, clon_e7)
        own_cell[has_bbox] = sub_cell
        own_hilbert[has_bbox] = sub_hilbert
        own_clat[has_bbox] = clat_e7
        own_clon[has_bbox] = clon_e7

    import pyarrow as pa

    tbl = pa.table({
        "id": pa.array(np.asarray(rel_ids_np, dtype=np.int64)),
        "valid_from": pa.array(valid_from_np),
        "own_cell": pa.array(own_cell, type=pa.string()),
        "own_hilbert": pa.array(own_hilbert, type=pa.uint64()),
        "own_clat": pa.array([int(v) if m else None for v, m in zip(own_clat.tolist(), has_bbox.tolist())], type=pa.int32()),
        "own_clon": pa.array([int(v) if m else None for v, m in zip(own_clon.tolist(), has_bbox.tolist())], type=pa.int32()),
    })
    con.register("_rel_cell_arrow", tbl)
    con.execute("CREATE OR REPLACE TEMP TABLE rel_cell_assign AS SELECT * FROM _rel_cell_arrow")
    con.unregister("_rel_cell_arrow")
    del tbl

    con.execute("""
        CREATE OR REPLACE TEMP TABLE relation_states AS
        SELECT r.*,
               last_value(a.own_cell IGNORE NULLS) OVER (
                   PARTITION BY r.id ORDER BY r.valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS cell_filled,
               a.own_hilbert, a.own_clat, a.own_clon
        FROM relation_states_pre_cell r JOIN rel_cell_assign a ON a.id = r.id AND a.valid_from = r.valid_from
    """)
    con.execute("DROP TABLE rel_cell_assign")
    con.execute("DROP TABLE relation_states_pre_cell")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE relation_states_final AS
        SELECT id, members, tags,
    """ + ", ".join(f'"{k}"' for k in promoted_keys) + """,
               version, changeset, timestamp, uid, "user",
               xmin_e7, ymin_e7, xmax_e7, ymax_e7, NULL::GEOMETRY AS geometry,
               own_clat AS centroid_lat_e7, own_clon AS centroid_lon_e7,
               coalesce(cell_filled, 'root') AS cell,
               CASE WHEN xmin_e7 IS NOT NULL THEN own_hilbert END AS hilbert,
               minor, valid_from, visible
        FROM relation_states
    """)
    con.execute("DROP TABLE relation_states")
    con.execute("DROP TABLE relation_own")
    con.execute("DROP TABLE current_relation_bbox")


def _write_relation_final(con, promoted_keys: list[str]) -> None:
    spatial_cols = SPATIAL_COLUMNS["relation"](promoted_keys)
    byid_cols = BYID_COLUMNS["relation"](promoted_keys)
    _project(con, "relation_states_final", spatial_cols + ["minor", "valid_from", "visible"], "relation_states_spatial_shape")
    _finalize_states(con, "relation_states_spatial_shape", "__relation_final_spatial_raw")
    _reorder_to_schema(con, "__relation_final_spatial_raw", spatial_cols, "relation_final_spatial")
    _project(con, "relation_states_final", byid_cols + ["minor", "valid_from", "visible"], "__tmp_relation_byid")
    con.execute("""
        CREATE OR REPLACE TEMP VIEW __relation_final_byid_raw AS
        SELECT *, lead(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS valid_to
        FROM __tmp_relation_byid
    """)
    _reorder_to_schema(con, "__relation_final_byid_raw", byid_cols, "relation_final_byid")


# --------------------------------------------------------------------------
# orchestration (section 4, 4.3)
# --------------------------------------------------------------------------


def history_build(opts: HistoryBuildOptions) -> dict:
    t0 = time.time()
    man = manifest_mod.load_latest(opts.root)
    promoted_keys = list(man.promoted_keys)
    ancestor_depths = list(man.ancestor_depths or cells_mod.DEFAULT_ANCESTOR_DEPTHS)
    max_depth = int(man.max_depth or cells_mod.DEFAULT_MAX_DEPTH_V2)
    leaf_index = cells_mod.LeafIndex(man.leaf_cells)
    extent = tuple(man.extent)

    tmpdir = Path(opts.tmpdir) if opts.tmpdir else Path(opts.root) / ".osmpq-history-tmp"
    tmpdir.mkdir(parents=True, exist_ok=True)

    import duckdb

    db_path = tmpdir / "history-build.duckdb"
    if db_path.exists():
        db_path.unlink()
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

    try:
        gen = man.generation
        spatial_frag: dict[str, dict] = {}
        byid_frag: dict[str, list] = {}
        stats_rows: dict[str, int] = {}
        stats_minor_rows: dict[str, int] = {}
        total_bytes = 0

        def _write_and_drop(typ: str, drop_tables: list[str]) -> None:
            nonlocal total_bytes
            spatial_table = f"{typ}_final_spatial"
            byid_table = f"{typ}_final_byid"
            spatial_frag[typ] = writer_mod.write_spatial(con, opts.root, gen, typ, spatial_table)
            byid_frag[typ] = writer_mod.write_byid(con, opts.root, gen, typ, byid_table)
            n_rows = con.execute(f"SELECT count(*) FROM {byid_table}").fetchone()[0]
            n_minor = con.execute(f"SELECT count(*) FROM {byid_table} WHERE minor > 0").fetchone()[0]
            stats_rows[typ] = n_rows
            stats_minor_rows[typ] = n_minor
            for cell_parts in spatial_frag[typ].values():
                total_bytes += sum(p["bytes"] for p in cell_parts)
            total_bytes += sum(p["bytes"] for p in byid_frag[typ])
            _log(f"{typ}: wrote {n_rows} history rows ({n_minor} minor) in {time.time()-t0:.1f}s total")
            for t in drop_tables:
                con.execute(f"DROP TABLE IF EXISTS {t}")

        ingest_opts = ingest_mod.IngestOptions(
            root=opts.root, pbf_path=opts.pbf_path, osc_paths=opts.osc_paths, osh_path=opts.osh_path,
        )
        counts_raw, since = ingest_mod.ingest_history(con, ingest_opts, extent)

        _compute_node_states(con, promoted_keys, leaf_index)
        _write_node_final(con, promoted_keys)
        _log(f"node states done in {time.time()-t0:.1f}s total")
        # `node_states_geo` (built in `_compute_node_states`) is all the
        # way/relation passes need from nodes -- drop the wide `node_states`
        # (and this type's own now-written final tables) before building
        # way states, which is what keeps peak memory from stacking the
        # full node/way/relation working sets on top of each other at
        # Minnesota scale (see the module docstring).
        _write_and_drop("node", ["node_states", "node_final_spatial", "node_final_byid"])

        _compute_way_states(con, promoted_keys, leaf_index, ancestor_depths, max_depth)
        _write_way_final(con, promoted_keys)
        _log(f"way states done in {time.time()-t0:.1f}s total")
        _write_and_drop("way", ["way_final_spatial", "way_final_byid"])

        _load_current_relation_bbox(con, opts.root, man)
        _compute_relation_states(con, promoted_keys, leaf_index, ancestor_depths, max_depth)
        _write_relation_final(con, promoted_keys)
        _log(f"relation states done in {time.time()-t0:.1f}s total")
        _write_and_drop("relation", ["relation_final_spatial", "relation_final_byid"])

        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ") if since is not None else man.timestamp_osm_base
        history_fragment = {
            "generation": gen,
            "since": since_str,
            "minor_versions": True,
            "spatial": spatial_frag,
            "byid": byid_frag,
            "tiers": {},
            "stats": {"rows": stats_rows, "minor_rows": stats_minor_rows, "bytes": total_bytes},
        }

        new_man = manifest_mod.Manifest(
            generation=man.generation, timestamp_osm_base=man.timestamp_osm_base, source=man.source,
            extent=man.extent, leaf_cells=man.leaf_cells, tables=man.tables, byid=man.byid, index=man.index,
            promoted_keys=man.promoted_keys, replication_sequence=man.replication_sequence,
            manifest_version=5, schema_version=man.schema_version, coordinate_scale=man.coordinate_scale,
            ancestor_depths=man.ancestor_depths, max_depth=man.max_depth, rowgroup_index=man.rowgroup_index,
            producer={**man.producer, "history": "osmpq history build"}, stats=man.stats,
            replication_source=man.replication_source, deltas=man.deltas, areas=man.areas,
            history=history_fragment,
        )
        gen_number = manifest_mod.next_manifest_number(opts.root)
        manifest_mod.write_manifest(opts.root, new_man, gen_number)
        _log(f"done in {time.time()-t0:.1f}s total; wrote manifest/{gen_number}.json")
        return {"counts_raw": counts_raw, "since": since_str, "stats": history_fragment["stats"], "seconds": time.time() - t0}
    finally:
        con.close()
        if db_path.exists():
            db_path.unlink()
