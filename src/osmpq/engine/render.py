"""`out`: turn a materialized set into Overpass JSON-shaped element dicts
(contract sections 6-7). The XML/JSON text renderers in result.py are pure
formatting over this same list.
"""
from __future__ import annotations

from typing import Optional

from osmpq.ql.ast import Out

from . import catalog, idset, sources
from .schema import CANONICAL_COLUMNS

MEMBER_TYPE_NAME = {"n": "node", "w": "way", "r": "relation"}


def _quote_list(paths: list[str]) -> str:
    return "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def e7(v: Optional[int]) -> Optional[float]:
    if v is None:
        return None
    return round(v / 1e7, 7)


def format_timestamp(ts) -> Optional[str]:
    if ts is None:
        return None
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_linestring_wkt(wkt: Optional[str]) -> Optional[list[tuple[float, float]]]:
    """'LINESTRING (lon lat, lon lat, ...)' -> [(lon, lat), ...]."""
    if not wkt:
        return None
    inner = wkt.strip()
    if inner.upper().startswith("LINESTRING"):
        inner = inner[inner.index("(") + 1 : inner.rindex(")")]
    pts = []
    for pair in inner.split(","):
        pair = pair.strip()
        if not pair:
            continue
        lon_s, lat_s = pair.split()
        pts.append((float(lon_s), float(lat_s)))
    return pts


def bounds_dict(row: dict) -> Optional[dict]:
    xmin, ymin, xmax, ymax = row.get("xmin_e7"), row.get("ymin_e7"), row.get("xmax_e7"), row.get("ymax_e7")
    if None in (xmin, ymin, xmax, ymax):
        return None
    return {
        "minlat": e7(ymin),
        "minlon": e7(xmin),
        "maxlat": e7(ymax),
        "maxlon": e7(xmax),
    }


def center_dict(row: dict) -> Optional[dict]:
    xmin, ymin, xmax, ymax = row.get("xmin_e7"), row.get("ymin_e7"), row.get("xmax_e7"), row.get("ymax_e7")
    if None in (xmin, ymin, xmax, ymax):
        return None
    return {
        "lat": round(((ymin + ymax) / 2.0) / 1e7, 7),
        "lon": round(((xmin + xmax) / 2.0) / 1e7, 7),
    }


# --------------------------------------------------------------------------
# Fetching the ordered/limited rows for `out`
# --------------------------------------------------------------------------

_ORDER_SQL = {
    "asc": "CASE type WHEN 'node' THEN 0 WHEN 'way' THEN 1 ELSE 2 END, id",
    "qt": "hilbert IS NULL, hilbert, id",
}


def fetch_out_rows(con, target_set: str, order: str, limit: Optional[int]) -> list[dict]:
    order_sql = _ORDER_SQL[order]
    limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
    sql = f"""
        SELECT type, id, cell, lat_e7, lon_e7, refs, members, tags, version, changeset,
               "timestamp", uid, "user", xmin_e7, ymin_e7, xmax_e7, ymax_e7,
               ST_AsText(geometry) AS geometry_wkt, hilbert
        FROM set_{target_set}
        ORDER BY {order_sql}
        {limit_sql}
    """
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetch_counts(con, target_set: str) -> dict:
    rows = con.execute(f"SELECT type, count(*) FROM set_{target_set} GROUP BY type").fetchall()
    counts = {"nodes": 0, "ways": 0, "relations": 0}
    label = {"node": "nodes", "way": "ways", "relation": "relations"}
    for t, c in rows:
        if t in label:
            counts[label[t]] = c
    counts["total"] = sum(counts.values())
    return counts


# --------------------------------------------------------------------------
# Lazy hydration: way geometry by (cell, id); relation member geometry
# --------------------------------------------------------------------------


def hydrate_way_geometry(con, manifest: catalog.Manifest, rows: list[dict]) -> None:
    """Fill in `geometry_wkt` for byid-sourced way rows (no geometry column
    there; contract section 4) by (cell, id). The rows already know their
    own cell, so this is one registered (cell, id) TEMP TABLE plus one
    join across the handful of spatial way files those cells need --
    instead of a Python loop doing one `id IN (<n literals>)` query per
    cell."""
    missing = [r for r in rows if r["type"] == "way" and not r.get("geometry_wkt") and r.get("cell")]
    if not missing:
        return
    tc = manifest.table_cells("way")
    needed_cells = sorted({r["cell"] for r in missing if r["cell"] in tc})
    if not needed_cells:
        return
    files = [manifest.path(tc[c]["path"]) for c in needed_cells]
    pairs_table = idset.register_pairs_table(con, [(r["cell"], r["id"]) for r in missing])
    resolved = {
        wid: wkt
        for wid, wkt in con.execute(
            f"SELECT t.id, ST_AsText(t.geometry) FROM read_parquet({_quote_list(files)}) t "
            f"JOIN {pairs_table} p ON t.cell = p.cell AND t.id = p.id"
        ).fetchall()
        if wkt
    }
    for r in missing:
        if r["id"] in resolved:
            r["geometry_wkt"] = resolved[r["id"]]


def resolve_node_coords(
    con,
    manifest: catalog.Manifest,
    ids: list[int],
    bbox_hints: Optional[list[tuple[int, int, int, int]]] = None,
) -> dict[int, tuple[int, int]]:
    """Node coordinates for `out geom` on relations: a relation's member
    nodes lie inside the relation's own bbox (xmin_e7/ymin_e7/xmax_e7/
    ymax_e7, already on the row -- contract section 4), so when the
    caller passes the bboxes of the relations these ids came from
    (`bbox_hints`), resolve from the spatial node files of the leaf cells
    those bboxes touch (design.md 3.1) instead of a byid scan, falling
    back to byid for anything not found there. `bbox_hints` omitted or
    empty (e.g. plain node-member ids with no known bounding relation)
    skips straight to byid, same as before this existed."""
    ids = sorted(set(ids))
    if not ids:
        return {}
    ids_table = idset.register_ids_table(con, ids)
    bbox_selects: list[str] = []
    if bbox_hints:
        xmin = min(b[0] for b in bbox_hints)
        ymin = min(b[1] for b in bbox_hints)
        xmax = max(b[2] for b in bbox_hints)
        ymax = max(b[3] for b in bbox_hints)
        bbox_selects = [
            f"SELECT {xmin} AS xmin_e7, {ymin} AS ymin_e7, {xmax} AS xmax_e7, {ymax} AS ymax_e7"
        ]
    sql, _nfiles = sources.build_node_hydrate_via_bbox_select(con, manifest, ids_table, bbox_selects, set())
    if not sql:
        return {}
    rows = con.execute(f"SELECT id, lat_e7, lon_e7 FROM ({sql}) __r").fetchall()
    return {i: (lat, lon) for i, lat, lon in rows}


def resolve_way_geometries(con, manifest: catalog.Manifest, ids: list[int]) -> dict[int, list[tuple[float, float]]]:
    """(way id) -> geometry points, for relation members: look up each way's
    cell via byid, then group by cell *in SQL* (one join across the needed
    spatial files) instead of issuing one `id IN (...)` query per cell."""
    ids = sorted(set(ids))
    if not ids:
        return {}
    lo, hi = idset.id_range(ids)
    parts = catalog.byid_parts_for_range(manifest, "way", lo, hi)
    files = [manifest.path(p["path"]) for p in parts]
    if not files:
        return {}
    pred = idset.id_predicate(con, "id", ids)
    id_cell = con.execute(
        f"SELECT id, cell FROM read_parquet({_quote_list(files)}) WHERE {pred} AND cell IS NOT NULL"
    ).fetchall()
    if not id_cell:
        return {}
    tc = manifest.table_cells("way")
    needed_cells = sorted({cell for _, cell in id_cell if cell in tc})
    if not needed_cells:
        return {}
    spatial_files = [manifest.path(tc[c]["path"]) for c in needed_cells]
    pairs_table = idset.register_pairs_table(con, [(cell, wid) for wid, cell in id_cell])
    out: dict[int, list[tuple[float, float]]] = {}
    for wid, wkt in con.execute(
        f"SELECT t.id, ST_AsText(t.geometry) FROM read_parquet({_quote_list(spatial_files)}) t "
        f"JOIN {pairs_table} p ON t.cell = p.cell AND t.id = p.id"
    ).fetchall():
        pts = parse_linestring_wkt(wkt)
        if pts is not None:
            out[wid] = pts
    return out


# --------------------------------------------------------------------------
# Row -> Overpass JSON element dict
# --------------------------------------------------------------------------


def build_elements(con, manifest: catalog.Manifest, target_set: str, out: Out) -> tuple[list[dict], dict]:
    """Returns (elements, extra_stats)."""
    if out.count:
        counts = fetch_counts(con, target_set)
        el = {
            "type": "count",
            "id": 0,
            "tags": {
                "nodes": str(counts["nodes"]),
                "ways": str(counts["ways"]),
                "relations": str(counts["relations"]),
                "total": str(counts["total"]),
            },
        }
        return [el], {}

    rows = fetch_out_rows(con, target_set, out.order, out.limit)

    if out.geometry == "geom":
        hydrate_way_geometry(con, manifest, rows)
        node_ids_needed: set[int] = set()
        way_ids_needed: set[int] = set()
        relation_bboxes: list[tuple[int, int, int, int]] = []
        for r in rows:
            if r["type"] == "relation" and r.get("members"):
                for m in r["members"]:
                    if m["type"] == "n":
                        node_ids_needed.add(m["ref"])
                    elif m["type"] == "w":
                        way_ids_needed.add(m["ref"])
                bbox = (r.get("xmin_e7"), r.get("ymin_e7"), r.get("xmax_e7"), r.get("ymax_e7"))
                if None not in bbox:
                    relation_bboxes.append(bbox)
        node_coords = (
            resolve_node_coords(con, manifest, list(node_ids_needed), relation_bboxes)
            if node_ids_needed
            else {}
        )
        way_geoms = resolve_way_geometries(con, manifest, list(way_ids_needed)) if way_ids_needed else {}
    else:
        node_coords = {}
        way_geoms = {}

    elements = [_row_to_element(r, out, node_coords, way_geoms) for r in rows]
    return elements, {}


def _row_to_element(row: dict, out: Out, node_coords: dict, way_geoms: dict) -> dict:
    t = row["type"]
    el: dict = {"type": t, "id": row["id"]}
    v = out.verbosity

    if t == "node":
        if v != "ids" and v != "tags":
            el["lat"] = e7(row["lat_e7"])
            el["lon"] = e7(row["lon_e7"])
    elif t == "way":
        if out.geometry in ("bb", "geom"):
            b = bounds_dict(row)
            if b:
                el["bounds"] = b
        if v != "ids" and v != "tags" and not out.noids:
            el["nodes"] = list(row["refs"]) if row["refs"] else []
        if out.geometry == "geom":
            pts = parse_linestring_wkt(row.get("geometry_wkt"))
            if pts is not None:
                el["geometry"] = [{"lat": round(lat, 7), "lon": round(lon, 7)} for lon, lat in pts]
        if out.geometry == "center":
            c = center_dict(row)
            if c:
                el["center"] = c
    elif t == "relation":
        if out.geometry in ("bb", "geom"):
            b = bounds_dict(row)
            if b:
                el["bounds"] = b
        if v != "ids" and v != "tags":
            members = row["members"] or []
            out_members = []
            for m in members:
                md = {"type": MEMBER_TYPE_NAME.get(m["type"], m["type"]), "ref": m["ref"], "role": m["role"] or ""}
                if out.geometry == "geom":
                    if m["type"] == "n" and m["ref"] in node_coords:
                        lat_e7, lon_e7 = node_coords[m["ref"]]
                        md["lat"] = e7(lat_e7)
                        md["lon"] = e7(lon_e7)
                    elif m["type"] == "w" and m["ref"] in way_geoms:
                        md["geometry"] = [
                            {"lat": round(lat, 7), "lon": round(lon, 7)} for lon, lat in way_geoms[m["ref"]]
                        ]
                out_members.append(md)
            el["members"] = out_members
        if out.geometry == "center":
            c = center_dict(row)
            if c:
                el["center"] = c

    if v == "meta":
        ts = format_timestamp(row.get("timestamp"))
        if ts is not None:
            el["timestamp"] = ts
        if row.get("version") is not None:
            el["version"] = row["version"]
        if row.get("changeset") is not None:
            el["changeset"] = row["changeset"]
        if row.get("user") is not None:
            el["user"] = row["user"]
        if row.get("uid") is not None:
            el["uid"] = row["uid"]

    if v in ("body", "tags", "meta") and row.get("tags"):
        el["tags"] = dict(row["tags"])

    return el
