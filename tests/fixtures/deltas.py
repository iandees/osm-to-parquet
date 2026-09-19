"""Synthetic M2 delta-tier fixture writer, per docs/m2-contracts.md section 3.

Given an existing dataset root (v2 or v3 manifest, e.g. a Bermuda build from
``osmpq build data/bermuda-latest.osm.pbf <tmp> --max-nodes-per-cell 20000``),
:func:`write_deltas` writes ``delta/<gen>/<tier>/<ver>/...`` files for one or
more tiers from a small Python description, and a new manifest v3 whose
``deltas`` block points at them (same ``generation`` -- the updater never
creates a new generation, only ``osmpq compact`` does).

Description format::

    {
      "week": [("node", 123, {"tags": {...}}), ("way", 456, "delete"), ...],
      "day": [...],
      "hour": [...],
    }

Tiers are processed in the fixed order ``week`` (oldest) -> ``day`` ->
``hour`` (newest), so that an id touched in more than one tier chains
``prev_cell``/``version`` correctly from the earlier tier's result, and the
newest tier's row is the one compaction (and the M2 read path) picks per the
tier-precedence rule (``hour`` > ``day`` > ``week``).

Each entry is ``(type, id, spec)`` where ``type`` is ``"node"``, ``"way"`` or
``"relation"`` and ``spec`` is either the string ``"delete"`` (writes a
tombstone; the id must already exist, either in the base or an earlier tier
processed in this same call) or a dict of field overrides relative to the
element's *current* state (base, or an earlier tier in this call):

- node: ``{"tags": {...} | None, "lat": float, "lon": float}``
- way: ``{"tags": {...} | None, "refs": [id, ...]}``
- relation: ``{"tags": {...} | None, "members": [{"type","ref","role"}, ...]}``

A field left out of the dict carries over from the current state (per
section 3: "untouched columns are never NULL-filled"). A brand-new id (no
current state) must give every field the schema needs.

Way/relation bbox, geometry, cell and hilbert are recomputed the same way
the builder does: ``ST_MakeLine`` over resolved ref/member node coordinates
for way geometry, ``osmpq.layout.cells.containing_cells_v2_np`` for
placement, ``osmpq.layout.hilbert.hilbert_keys`` for the sort key. Relation
bbox only folds in direct node/way members (no nested relation-of-relation
bbox -- out of scope for this fixture).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import numpy as np

from osmpq.layout import cells as cells_mod
from osmpq.layout import hilbert as hilbert_mod

TIERS = ["week", "day", "hour"]  # oldest -> newest processing order
DELTA_ROW_GROUP = 10_000  # tiers are small (m2-contracts.md section 3)


def to_e7(deg: float) -> int:
    return int(round(deg * 1e7))


def _sqlesc(s: Any) -> str:
    return str(s).replace("'", "''")


def _str_or_null(v: Optional[str]) -> str:
    return "NULL::VARCHAR" if v is None else f"'{_sqlesc(v)}'"


def _int_or_null(v: Optional[int], cast: str = "INTEGER") -> str:
    return f"NULL::{cast}" if v is None else str(int(v))


def _bool_sql(v: bool) -> str:
    return "TRUE" if v else "FALSE"


def _tags_literal(tags: Optional[dict]) -> str:
    if not tags:
        return "NULL::MAP(VARCHAR, VARCHAR)"
    pairs = ", ".join(f"'{_sqlesc(k)}': '{_sqlesc(v)}'" for k, v in tags.items())
    return f"MAP {{{pairs}}}"


def _promoted_cols_sql(tags: Optional[dict], promoted_keys: list[str]) -> str:
    tags = tags or {}
    return ", ".join(
        (f"'{_sqlesc(tags[k])}'::VARCHAR AS \"{k}\"" if k in tags else f'NULL::VARCHAR AS "{k}"')
        for k in promoted_keys
    )


def _promoted_cols_null(promoted_keys: list[str]) -> str:
    return ", ".join(f'NULL::VARCHAR AS "{k}"' for k in promoted_keys)


def _refs_literal(refs: list[int]) -> str:
    return "[" + ", ".join(str(r) for r in refs) + "]::BIGINT[]"


def _members_literal(members: list[dict]) -> str:
    items = ", ".join(
        f"{{'type': '{_sqlesc(m['type'])}', 'ref': {int(m['ref'])}, 'role': '{_sqlesc(m.get('role') or '')}'}}"
        for m in members
    )
    return f"[{items}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]"


def _bump_timestamp(base_timestamp: str, minutes: int) -> str:
    # base_timestamp like "2026-09-19T01:00:00Z"; DuckDB parses this fine
    # via its TIMESTAMP literal + INTERVAL arithmetic, done at write time
    # instead, so this just returns the base string (each row's own SQL
    # applies "+ INTERVAL (n) MINUTE"). Kept for callers wanting a plain
    # per-row timestamp string; unused internally (see row builders).
    return base_timestamp


# --------------------------------------------------------------------------
# manifest / base-state lookups
# --------------------------------------------------------------------------


def _load_manifest(root: Path) -> tuple[dict, int]:
    latest = int((root / "manifest" / "LATEST").read_text().strip())
    man = json.loads((root / "manifest" / f"{latest}.json").read_text())
    return man, latest


class _StateCache:
    """Tracks each touched element's current field values across the whole
    call: seeded from the base byid parts on first lookup, then updated as
    each tier's entries are processed, so a later tier sees an earlier
    tier's effect (chained ``prev_cell``/``version``)."""

    def __init__(self, con, root: Path, man: dict):
        self.con = con
        self.root = root
        self.man = man
        self._cache: dict[tuple[str, int], Optional[dict]] = {}

    def get(self, etype: str, id_: int) -> Optional[dict]:
        key = (etype, id_)
        if key in self._cache:
            return self._cache[key]
        row = self._lookup_base(etype, id_)
        self._cache[key] = row
        return row

    def set(self, etype: str, id_: int, row: Optional[dict]) -> None:
        self._cache[(etype, id_)] = row

    def _lookup_base(self, etype: str, id_: int) -> Optional[dict]:
        parts = self.man.get("byid", {}).get(etype, []) or []
        for p in parts:
            lo, hi = p.get("min_id"), p.get("max_id")
            if lo is not None and hi is not None and not (lo <= id_ <= hi):
                continue
            path = str(self.root / p["path"]).replace("'", "''")
            row = self.con.execute(f"SELECT * FROM read_parquet('{path}') WHERE id = {int(id_)}").fetchone()
            if row is None:
                continue
            cols = [d[0] for d in self.con.description]
            return dict(zip(cols, row))
        return None


# --------------------------------------------------------------------------
# row -> SQL SELECT (one row per statement, UNION ALL'd, matching the style
# tests/fixtures/make_fixture.py already uses for the M0/M1 fixture)
# --------------------------------------------------------------------------


def _node_spatial_live(row: dict, prev_cell: Optional[str], seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {row['id']} AS id, {row['lat_e7']} AS lat_e7, {row['lon_e7']} AS lon_e7, "
        f"{_tags_literal(row['tags'])} AS tags, {_promoted_cols_sql(row['tags'], promoted_keys)}, "
        f"{row['version']} AS version, {row['changeset']} AS changeset, "
        f"TIMESTAMP '{row['timestamp']}' AS \"timestamp\", {row['uid']} AS uid, "
        f"'{_sqlesc(row['user'])}' AS \"user\", {row['hilbert']}::UBIGINT AS hilbert, "
        f"'{row['cell']}' AS cell, FALSE AS deleted, {_str_or_null(prev_cell)} AS prev_cell, {seq}::BIGINT AS seq"
    )


def _node_spatial_deleted(id_: int, prev_cell: str, seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {id_} AS id, NULL::INTEGER AS lat_e7, NULL::INTEGER AS lon_e7, "
        f"NULL::MAP(VARCHAR, VARCHAR) AS tags, {_promoted_cols_null(promoted_keys)}, "
        f"NULL::INTEGER AS version, NULL::BIGINT AS changeset, NULL::TIMESTAMP AS \"timestamp\", "
        f"NULL::INTEGER AS uid, NULL::VARCHAR AS \"user\", NULL::UBIGINT AS hilbert, "
        f"'{_sqlesc(prev_cell)}' AS cell, TRUE AS deleted, '{_sqlesc(prev_cell)}' AS prev_cell, {seq}::BIGINT AS seq"
    )


def _node_byid_live(row: dict, prev_cell: Optional[str], seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {row['id']} AS id, {row['lat_e7']} AS lat_e7, {row['lon_e7']} AS lon_e7, "
        f"{_tags_literal(row['tags'])} AS tags, {_promoted_cols_sql(row['tags'], promoted_keys)}, "
        f"{row['version']} AS version, {row['changeset']} AS changeset, "
        f"TIMESTAMP '{row['timestamp']}' AS \"timestamp\", {row['uid']} AS uid, "
        f"'{_sqlesc(row['user'])}' AS \"user\", '{row['cell']}' AS cell, {row['hilbert']}::UBIGINT AS hilbert, "
        f"FALSE AS deleted, {_str_or_null(prev_cell)} AS prev_cell, {seq}::BIGINT AS seq"
    )


def _node_byid_deleted(id_: int, prev_cell: str, seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {id_} AS id, NULL::INTEGER AS lat_e7, NULL::INTEGER AS lon_e7, "
        f"NULL::MAP(VARCHAR, VARCHAR) AS tags, {_promoted_cols_null(promoted_keys)}, "
        f"NULL::INTEGER AS version, NULL::BIGINT AS changeset, NULL::TIMESTAMP AS \"timestamp\", "
        f"NULL::INTEGER AS uid, NULL::VARCHAR AS \"user\", '{_sqlesc(prev_cell)}' AS cell, "
        f"NULL::UBIGINT AS hilbert, TRUE AS deleted, '{_sqlesc(prev_cell)}' AS prev_cell, {seq}::BIGINT AS seq"
    )


def _way_spatial_live(row: dict, prev_cell: Optional[str], seq: int, promoted_keys: list[str]) -> str:
    geom_expr = f"ST_GeomFromText('{row['wkt']}')" if row.get("wkt") else "NULL::GEOMETRY"
    return (
        f"SELECT {row['id']} AS id, {_refs_literal(row['refs'])} AS refs, "
        f"{_tags_literal(row['tags'])} AS tags, {_promoted_cols_sql(row['tags'], promoted_keys)}, "
        f"{row['version']} AS version, {row['changeset']} AS changeset, "
        f"TIMESTAMP '{row['timestamp']}' AS \"timestamp\", {row['uid']} AS uid, "
        f"'{_sqlesc(row['user'])}' AS \"user\", "
        f"{_int_or_null(row['xmin_e7'])} AS xmin_e7, {_int_or_null(row['ymin_e7'])} AS ymin_e7, "
        f"{_int_or_null(row['xmax_e7'])} AS xmax_e7, {_int_or_null(row['ymax_e7'])} AS ymax_e7, "
        f"{geom_expr} AS geometry, {_bool_sql(row['is_closed'])} AS is_closed, {_bool_sql(row['is_area'])} AS is_area, "
        f"{_int_or_null(row['centroid_lat_e7'])} AS centroid_lat_e7, {_int_or_null(row['centroid_lon_e7'])} AS centroid_lon_e7, "
        f"'{row['cell']}' AS cell, {row['hilbert']}::UBIGINT AS hilbert, "
        f"FALSE AS deleted, {_str_or_null(prev_cell)} AS prev_cell, {seq}::BIGINT AS seq"
    )


def _way_spatial_deleted(id_: int, prev_cell: str, seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {id_} AS id, NULL::BIGINT[] AS refs, NULL::MAP(VARCHAR, VARCHAR) AS tags, "
        f"{_promoted_cols_null(promoted_keys)}, "
        f"NULL::INTEGER AS version, NULL::BIGINT AS changeset, NULL::TIMESTAMP AS \"timestamp\", "
        f"NULL::INTEGER AS uid, NULL::VARCHAR AS \"user\", "
        f"NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, "
        f"NULL::GEOMETRY AS geometry, NULL::BOOLEAN AS is_closed, NULL::BOOLEAN AS is_area, "
        f"NULL::INTEGER AS centroid_lat_e7, NULL::INTEGER AS centroid_lon_e7, "
        f"'{_sqlesc(prev_cell)}' AS cell, NULL::UBIGINT AS hilbert, "
        f"TRUE AS deleted, '{_sqlesc(prev_cell)}' AS prev_cell, {seq}::BIGINT AS seq"
    )


def _way_byid_live(row: dict, prev_cell: Optional[str], seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {row['id']} AS id, {_refs_literal(row['refs'])} AS refs, "
        f"{_tags_literal(row['tags'])} AS tags, {_promoted_cols_sql(row['tags'], promoted_keys)}, "
        f"{row['version']} AS version, {row['changeset']} AS changeset, "
        f"TIMESTAMP '{row['timestamp']}' AS \"timestamp\", {row['uid']} AS uid, "
        f"'{_sqlesc(row['user'])}' AS \"user\", "
        f"{_int_or_null(row['xmin_e7'])} AS xmin_e7, {_int_or_null(row['ymin_e7'])} AS ymin_e7, "
        f"{_int_or_null(row['xmax_e7'])} AS xmax_e7, {_int_or_null(row['ymax_e7'])} AS ymax_e7, "
        f"{_bool_sql(row['is_closed'])} AS is_closed, {_bool_sql(row['is_area'])} AS is_area, "
        f"'{row['cell']}' AS cell, {row['hilbert']}::UBIGINT AS hilbert, "
        f"FALSE AS deleted, {_str_or_null(prev_cell)} AS prev_cell, {seq}::BIGINT AS seq"
    )


def _way_byid_deleted(id_: int, prev_cell: str, seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {id_} AS id, NULL::BIGINT[] AS refs, NULL::MAP(VARCHAR, VARCHAR) AS tags, "
        f"{_promoted_cols_null(promoted_keys)}, "
        f"NULL::INTEGER AS version, NULL::BIGINT AS changeset, NULL::TIMESTAMP AS \"timestamp\", "
        f"NULL::INTEGER AS uid, NULL::VARCHAR AS \"user\", "
        f"NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, "
        f"NULL::BOOLEAN AS is_closed, NULL::BOOLEAN AS is_area, "
        f"'{_sqlesc(prev_cell)}' AS cell, NULL::UBIGINT AS hilbert, "
        f"TRUE AS deleted, '{_sqlesc(prev_cell)}' AS prev_cell, {seq}::BIGINT AS seq"
    )


def _relation_spatial_live(row: dict, prev_cell: Optional[str], seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {row['id']} AS id, {_members_literal(row['members'])} AS members, "
        f"{_tags_literal(row['tags'])} AS tags, {_promoted_cols_sql(row['tags'], promoted_keys)}, "
        f"{row['version']} AS version, {row['changeset']} AS changeset, "
        f"TIMESTAMP '{row['timestamp']}' AS \"timestamp\", {row['uid']} AS uid, "
        f"'{_sqlesc(row['user'])}' AS \"user\", "
        f"{_int_or_null(row['xmin_e7'])} AS xmin_e7, {_int_or_null(row['ymin_e7'])} AS ymin_e7, "
        f"{_int_or_null(row['xmax_e7'])} AS xmax_e7, {_int_or_null(row['ymax_e7'])} AS ymax_e7, "
        f"NULL::GEOMETRY AS geometry, "
        f"{_int_or_null(row['centroid_lat_e7'])} AS centroid_lat_e7, {_int_or_null(row['centroid_lon_e7'])} AS centroid_lon_e7, "
        f"'{row['cell']}' AS cell, {row['hilbert']}::UBIGINT AS hilbert, "
        f"FALSE AS deleted, {_str_or_null(prev_cell)} AS prev_cell, {seq}::BIGINT AS seq"
    )


def _relation_spatial_deleted(id_: int, prev_cell: str, seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {id_} AS id, NULL::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[] AS members, "
        f"NULL::MAP(VARCHAR, VARCHAR) AS tags, {_promoted_cols_null(promoted_keys)}, "
        f"NULL::INTEGER AS version, NULL::BIGINT AS changeset, NULL::TIMESTAMP AS \"timestamp\", "
        f"NULL::INTEGER AS uid, NULL::VARCHAR AS \"user\", "
        f"NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, "
        f"NULL::GEOMETRY AS geometry, NULL::INTEGER AS centroid_lat_e7, NULL::INTEGER AS centroid_lon_e7, "
        f"'{_sqlesc(prev_cell)}' AS cell, NULL::UBIGINT AS hilbert, "
        f"TRUE AS deleted, '{_sqlesc(prev_cell)}' AS prev_cell, {seq}::BIGINT AS seq"
    )


def _relation_byid_live(row: dict, prev_cell: Optional[str], seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {row['id']} AS id, {_members_literal(row['members'])} AS members, "
        f"{_tags_literal(row['tags'])} AS tags, {_promoted_cols_sql(row['tags'], promoted_keys)}, "
        f"{row['version']} AS version, {row['changeset']} AS changeset, "
        f"TIMESTAMP '{row['timestamp']}' AS \"timestamp\", {row['uid']} AS uid, "
        f"'{_sqlesc(row['user'])}' AS \"user\", "
        f"{_int_or_null(row['xmin_e7'])} AS xmin_e7, {_int_or_null(row['ymin_e7'])} AS ymin_e7, "
        f"{_int_or_null(row['xmax_e7'])} AS xmax_e7, {_int_or_null(row['ymax_e7'])} AS ymax_e7, "
        f"'{row['cell']}' AS cell, "
        f"FALSE AS deleted, {_str_or_null(prev_cell)} AS prev_cell, {seq}::BIGINT AS seq"
    )


def _relation_byid_deleted(id_: int, prev_cell: str, seq: int, promoted_keys: list[str]) -> str:
    return (
        f"SELECT {id_} AS id, NULL::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[] AS members, "
        f"NULL::MAP(VARCHAR, VARCHAR) AS tags, {_promoted_cols_null(promoted_keys)}, "
        f"NULL::INTEGER AS version, NULL::BIGINT AS changeset, NULL::TIMESTAMP AS \"timestamp\", "
        f"NULL::INTEGER AS uid, NULL::VARCHAR AS \"user\", "
        f"NULL::INTEGER AS xmin_e7, NULL::INTEGER AS ymin_e7, NULL::INTEGER AS xmax_e7, NULL::INTEGER AS ymax_e7, "
        f"'{_sqlesc(prev_cell)}' AS cell, "
        f"TRUE AS deleted, '{_sqlesc(prev_cell)}' AS prev_cell, {seq}::BIGINT AS seq"
    )


def _tombstone_row(etype: str, id_: int, prev_cell: str, seq: int) -> str:
    return f"SELECT '{etype}' AS type, {id_} AS id, '{_sqlesc(prev_cell)}' AS prev_cell, {seq}::BIGINT AS seq"


# --------------------------------------------------------------------------
# main entry point
# --------------------------------------------------------------------------


def write_deltas(
    root: str,
    tiers: dict[str, list[tuple[str, int, Any]]],
    *,
    seq_start: int = 900_001,
    base_timestamp: str = "2026-09-19T01:00:00Z",
) -> dict:
    """See module docstring. Returns the ``deltas`` manifest block written."""
    import duckdb

    root_path = Path(root)
    man, latest_num = _load_manifest(root_path)
    generation = man["generation"]
    leaf_index = cells_mod.LeafIndex(man["leaf_cells"])
    ancestor_depths = man.get("ancestor_depths") or cells_mod.DEFAULT_ANCESTOR_DEPTHS
    max_depth = man.get("max_depth") or cells_mod.DEFAULT_MAX_DEPTH_V2
    promoted_keys = list(man.get("promoted_keys") or [])

    con = duckdb.connect()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")

    state = _StateCache(con, root_path, man)
    deltas_block: dict = dict(man.get("deltas") or {})
    seq = seq_start
    last_ts = base_timestamp

    for tier in TIERS:
        entries = tiers.get(tier)
        if not entries:
            continue
        version = int(((deltas_block.get(tier) or {}).get("version") or 0)) + 1
        node_spatial, node_byid = [], []
        way_spatial, way_byid = [], []
        relation_spatial, relation_byid = [], []
        tombstones = []
        seq_from = seq

        for etype, id_, spec in entries:
            row_seq = seq
            seq += 1
            ts = f"2026-09-19T01:00:00Z"  # placeholder overwritten below via minute offset
            minutes = row_seq - seq_start
            ts_literal = f"TIMESTAMP '{base_timestamp[:19].replace('T',' ')}' + INTERVAL ({minutes}) MINUTE"
            # Materialize the actual timestamp string via DuckDB so row dicts
            # (used for chaining into later tiers/way geometry) carry a
            # concrete value, not a SQL expression.
            ts_str = con.execute(f"SELECT strftime({ts_literal}, '%Y-%m-%dT%H:%M:%SZ')").fetchone()[0]
            last_ts = ts_str

            if spec == "delete":
                cur = state.get(etype, id_)
                if cur is None:
                    raise ValueError(f"deltas fixture: delete of unknown {etype} {id_} (no base/prior-tier state)")
                prev_cell = cur["cell"]
                tombstones.append(_tombstone_row(etype, id_, prev_cell, row_seq))
                if etype == "node":
                    node_spatial.append(_node_spatial_deleted(id_, prev_cell, row_seq, promoted_keys))
                    node_byid.append(_node_byid_deleted(id_, prev_cell, row_seq, promoted_keys))
                elif etype == "way":
                    way_spatial.append(_way_spatial_deleted(id_, prev_cell, row_seq, promoted_keys))
                    way_byid.append(_way_byid_deleted(id_, prev_cell, row_seq, promoted_keys))
                else:
                    relation_spatial.append(_relation_spatial_deleted(id_, prev_cell, row_seq, promoted_keys))
                    relation_byid.append(_relation_byid_deleted(id_, prev_cell, row_seq, promoted_keys))
                state.set(etype, id_, None)
                continue

            cur = state.get(etype, id_)
            prev_cell = cur["cell"] if cur else None
            version_num = (int(cur["version"]) + 1) if cur and cur.get("version") is not None else 1
            changeset = 900_000 + row_seq

            if etype == "node":
                tags = spec["tags"] if "tags" in spec else (cur["tags"] if cur else None)
                lat = spec["lat"] if "lat" in spec else ((cur["lat_e7"] / 1e7) if cur else None)
                lon = spec["lon"] if "lon" in spec else ((cur["lon_e7"] / 1e7) if cur else None)
                if lat is None or lon is None:
                    raise ValueError(f"deltas fixture: new node {id_} needs lat/lon")
                lat_e7, lon_e7 = to_e7(lat), to_e7(lon)
                qk = cells_mod.point_to_qk_np(np.array([lat_e7], dtype=np.int64), np.array([lon_e7], dtype=np.int64))
                cell = str(leaf_index.leaf_for_qk(qk)[0])
                hb = int(hilbert_mod.hilbert_keys(np.array([lat_e7], dtype=np.int64), np.array([lon_e7], dtype=np.int64))[0])
                new_row = {
                    "id": id_, "lat_e7": lat_e7, "lon_e7": lon_e7, "tags": tags,
                    "version": version_num, "changeset": changeset, "timestamp": ts_str,
                    "uid": 999, "user": "deltas-fixture", "cell": cell, "hilbert": hb,
                }
                state.set("node", id_, new_row)
                node_spatial.append(_node_spatial_live(new_row, prev_cell, row_seq, promoted_keys))
                node_byid.append(_node_byid_live(new_row, prev_cell, row_seq, promoted_keys))

            elif etype == "way":
                tags = spec["tags"] if "tags" in spec else (cur["tags"] if cur else None)
                refs = spec["refs"] if "refs" in spec else (cur["refs"] if cur else None)
                if refs is None:
                    raise ValueError(f"deltas fixture: new way {id_} needs refs")
                pts = []
                for ref in refs:
                    nrow = state.get("node", ref)
                    if nrow is not None and nrow.get("lat_e7") is not None:
                        pts.append((nrow["lon_e7"] / 1e7, nrow["lat_e7"] / 1e7))
                if len(pts) >= 2:
                    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
                    xmin, xmax, ymin, ymax = min(xs), max(xs), min(ys), max(ys)
                    wkt = "LINESTRING (" + ", ".join(f"{lo} {la}" for lo, la in pts) + ")"
                else:
                    xmin = ymin = xmax = ymax = None
                    wkt = None
                is_closed = len(refs) >= 4 and refs[0] == refs[-1]
                tags_d = tags or {}
                has_area_no = tags_d.get("area") == "no"
                has_linear = ("highway" in tags_d or "barrier" in tags_d) and tags_d.get("area") != "yes"
                is_area = bool(is_closed and not has_area_no and not has_linear)
                if xmin is not None:
                    xmin_e7, ymin_e7, xmax_e7, ymax_e7 = to_e7(xmin), to_e7(ymin), to_e7(xmax), to_e7(ymax)
                    cell = str(cells_mod.containing_cells_v2_np(
                        np.array([ymin_e7], dtype=np.int64), np.array([xmin_e7], dtype=np.int64),
                        np.array([ymax_e7], dtype=np.int64), np.array([xmax_e7], dtype=np.int64),
                        leaf_index, ancestor_depths, max_depth,
                    )[0])
                    clat = int(round((ymin_e7 + ymax_e7) / 2.0))
                    clon = int(round((xmin_e7 + xmax_e7) / 2.0))
                    hb = int(hilbert_mod.hilbert_keys(np.array([clat], dtype=np.int64), np.array([clon], dtype=np.int64))[0])
                else:
                    xmin_e7 = ymin_e7 = xmax_e7 = ymax_e7 = clat = clon = None
                    cell = cells_mod.ROOT
                    hb = 0
                new_row = {
                    "id": id_, "refs": refs, "tags": tags, "wkt": wkt,
                    "xmin_e7": xmin_e7, "ymin_e7": ymin_e7, "xmax_e7": xmax_e7, "ymax_e7": ymax_e7,
                    "is_closed": is_closed, "is_area": is_area,
                    "centroid_lat_e7": clat, "centroid_lon_e7": clon,
                    "version": version_num, "changeset": changeset, "timestamp": ts_str,
                    "uid": 999, "user": "deltas-fixture", "cell": cell, "hilbert": hb,
                }
                state.set("way", id_, new_row)
                way_spatial.append(_way_spatial_live(new_row, prev_cell, row_seq, promoted_keys))
                way_byid.append(_way_byid_live(new_row, prev_cell, row_seq, promoted_keys))

            else:  # relation
                tags = spec["tags"] if "tags" in spec else (cur["tags"] if cur else None)
                members = spec["members"] if "members" in spec else (cur["members"] if cur else None)
                if members is None:
                    raise ValueError(f"deltas fixture: new relation {id_} needs members")
                xs, ys = [], []
                for m in members:
                    if m["type"] == "n":
                        nrow = state.get("node", m["ref"])
                        if nrow is not None and nrow.get("lat_e7") is not None:
                            xs.append(nrow["lon_e7"] / 1e7)
                            ys.append(nrow["lat_e7"] / 1e7)
                    elif m["type"] == "w":
                        wrow = state.get("way", m["ref"])
                        if wrow is not None and wrow.get("xmin_e7") is not None:
                            xs += [wrow["xmin_e7"] / 1e7, wrow["xmax_e7"] / 1e7]
                            ys += [wrow["ymin_e7"] / 1e7, wrow["ymax_e7"] / 1e7]
                    # nested relation members: not resolved (out of scope).
                if xs:
                    xmin, xmax, ymin, ymax = min(xs), max(xs), min(ys), max(ys)
                    xmin_e7, ymin_e7, xmax_e7, ymax_e7 = to_e7(xmin), to_e7(ymin), to_e7(xmax), to_e7(ymax)
                    cell = str(cells_mod.containing_cells_v2_np(
                        np.array([ymin_e7], dtype=np.int64), np.array([xmin_e7], dtype=np.int64),
                        np.array([ymax_e7], dtype=np.int64), np.array([xmax_e7], dtype=np.int64),
                        leaf_index, ancestor_depths, max_depth,
                    )[0])
                    clat = int(round((ymin_e7 + ymax_e7) / 2.0))
                    clon = int(round((xmin_e7 + xmax_e7) / 2.0))
                    hb = int(hilbert_mod.hilbert_keys(np.array([clat], dtype=np.int64), np.array([clon], dtype=np.int64))[0])
                else:
                    xmin_e7 = ymin_e7 = xmax_e7 = ymax_e7 = clat = clon = None
                    cell = cells_mod.ROOT
                    hb = 0
                new_row = {
                    "id": id_, "members": members, "tags": tags,
                    "xmin_e7": xmin_e7, "ymin_e7": ymin_e7, "xmax_e7": xmax_e7, "ymax_e7": ymax_e7,
                    "centroid_lat_e7": clat, "centroid_lon_e7": clon,
                    "version": version_num, "changeset": changeset, "timestamp": ts_str,
                    "uid": 999, "user": "deltas-fixture", "cell": cell, "hilbert": hb,
                }
                state.set("relation", id_, new_row)
                relation_spatial.append(_relation_spatial_live(new_row, prev_cell, row_seq, promoted_keys))
                relation_byid.append(_relation_byid_live(new_row, prev_cell, row_seq, promoted_keys))

        seq_to = seq - 1
        tier_dir = root_path / "delta" / generation / tier / str(version)

        def _write(rows: list[str], rel_path: str) -> str:
            sql = " UNION ALL ".join(rows) if rows else None
            path = root_path / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            if sql is None:
                # Table had zero entries this tier: still write an empty
                # file with the right schema (build it, then filter it out).
                return rel_path
            con.execute(
                f"COPY ({sql}) TO '{str(path).replace(chr(39), chr(39) * 2)}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {DELTA_ROW_GROUP})"
            )
            return rel_path

        files: dict = {}
        rel = f"delta/{generation}/{tier}/{version}"
        for name, spatial_rows, byid_rows in (
            ("node", node_spatial, node_byid),
            ("way", way_spatial, way_byid),
            ("relation", relation_spatial, relation_byid),
        ):
            entry = {}
            if spatial_rows:
                entry["spatial"] = _write(spatial_rows, f"{rel}/{name}.spatial.parquet")
            if byid_rows:
                entry["byid"] = _write(byid_rows, f"{rel}/{name}.byid.parquet")
            if entry:
                files[name] = entry
        if tombstones:
            files["tombstones"] = _write(tombstones, f"{rel}/tombstones.parquet")

        deltas_block[tier] = {
            "version": version,
            "seq_from": seq_from,
            "seq_to": seq_to,
            "timestamp": last_ts,
            "rows": {
                "node": len(node_spatial),
                "way": len(way_spatial),
                "relation": len(relation_spatial),
            },
            "files": files,
        }

    man2 = dict(man)
    man2["manifest_version"] = 3
    man2["deltas"] = deltas_block
    man2["replication_source"] = man.get("replication_source") or "synthetic (tests/fixtures/deltas.py)"
    man2["replication_sequence"] = seq - 1
    man2["timestamp_osm_base"] = last_ts
    new_num = latest_num + 1
    (root_path / "manifest" / f"{new_num}.json").write_text(json.dumps(man2, indent=2))
    (root_path / "manifest" / "LATEST").write_text(str(new_num))

    con.close()
    return deltas_block


def sample_ids(root: str, table: str, n: int = 1, *, con=None) -> list[int]:
    """Convenience helper for tests: ``n`` real ids for ``table`` from the
    root's current byid parts, evenly spread (not just the first n), so
    tests can build a realistic ``tiers`` description without hardcoding
    ids that happen to exist in a particular Bermuda extract."""
    import duckdb as _duckdb

    root_path = Path(root)
    man, _ = _load_manifest(root_path)
    parts = man.get("byid", {}).get(table, []) or []
    if not parts:
        return []
    own_con = con is None
    con = con or _duckdb.connect()
    paths = [str(root_path / p["path"]) for p in parts]
    rows = con.execute(
        f"SELECT id FROM read_parquet({paths!r}) ORDER BY id"
    ).fetchall()
    ids = [r[0] for r in rows]
    if not ids:
        return []
    if n >= len(ids):
        picked = ids
    else:
        step = len(ids) / n
        picked = [ids[int(i * step)] for i in range(n)]
    if own_con:
        con.close()
    return picked
