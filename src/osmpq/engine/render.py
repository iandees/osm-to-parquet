"""`out`: turn a materialized set into Overpass JSON-shaped element dicts
(contract sections 6-7). The XML/JSON text renderers in result.py are pure
formatting over this same list.

`out geom(s,w,n,e)` clipping (docs/m3-contracts.md section 3.4)
------------------------------------------------------------------
Established empirically against the reference Overpass instance
(`https://maps.mail.ru/osm/tools/overpass/api/interpreter`, `[date:
"2026-09-19T00:21:52Z"]`), for a real way crossing a small bbox (East Lake
Street, Minneapolis, way 6001527, selected with
`way[highway=primary](44.9470,-93.2790,44.9490,-93.2760)` and clipped with
`out geom(44.9480,-93.2777,44.9487,-93.2770)`; see
`tests/corpus/46_out_geom_bbox.overpassql`), in both `[out:json]` and
`[out:xml]`:

* A way's node list (`nodes`)/member list is unaffected -- clipping only
  changes *coordinates*, never which elements/nodes are present (contract:
  "the bbox restricts geometry, not membership"). Plain node elements
  always show their own `lat`/`lon` regardless of `out geom(bbox)`.
* For each vertex of a way's geometry: if the vertex itself is inside the
  clip bbox, or if either of its immediate neighbours in the way's node
  sequence is inside the bbox, its real coordinates are kept. Otherwise
  the coordinate is dropped: JSON emits a `null` entry in the `geometry`
  array at that position (`"geometry": [null, {"lat":...,"lon":...}, ...]`)
  and XML emits a bare `<nd ref="..."/>` (no `lat`/`lon` attributes) --
  confirmed byte-for-byte against the reference for a 3-node way with one
  interior vertex outside the bbox but adjacent to one inside it (kept)
  and a 4-node way with two consecutive vertices outside and non-adjacent
  to any inside vertex (dropped). This "keep one bounding vertex on each
  side" rule is what lets the drawn line still reach the bbox edge.
* `bounds` is recomputed from whichever vertices survived clipping (not
  the way's own full stored bbox); a way with *no* vertex inside the bbox
  (so nothing survives clipping) omits `bounds` entirely, in both JSON and
  XML -- confirmed against the reference (way 700231761 in the same
  response, entirely outside the clip bbox, has no `bounds` key/element).
* Relation member *way* geometry follows the same per-vertex rule
  (contract: "Relation member geometry follows the same rule"). This
  extension was not independently re-verified against the reference (the
  reference mirror timed out on the multipolygon queries tried for it) --
  it is applied by analogy to the verified plain-way case. Since a
  relation member's geometry list carries no `ref` to anchor a bare
  placeholder against (unlike a top-level way's `nodes`/`geometry` pair),
  dropped vertices are omitted from the member's `geometry` list entirely
  rather than represented as `null`/an empty `<nd>`.
* Relation member *node* coordinates, and a relation's own `bounds`, are
  unaffected by `out geom(bbox)` in this implementation -- only member way
  geometry is clipped.
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
    resolved: dict[int, str] = {}
    if needed_cells:
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

    # docs/m2-contracts.md section 4: these rows came from a byid lookup
    # (no `geometry` column there, contract section 4 of m0), so the join
    # above only ever reads *base* spatial files by (cell, id). A way
    # that's been re-noded (or moved cell) in a delta tier needs its
    # current geometry instead -- the byid delta files carry no geometry
    # either (same as base byid), so pull it from the delta *spatial*
    # files, keyed by id alone (not by the possibly-stale `cell` hint on
    # `r`), tier-ranked, and let it override whatever the base join found.
    tiers = manifest.delta_tiers()
    if tiers:
        ids = [r["id"] for r in missing]
        ids_tbl = idset.register_ids_table(con, ids)
        # Keep `geometry` a plain passthrough column (no ST_* call) inside
        # each per-tier SELECT: an empty tier's way spatial file (0 rows
        # for "way" this tier) can come back with a plain BLOB physical
        # type for `geometry` instead of GEOMETRY (DuckDB has nothing to
        # infer the GeoParquet type from with zero rows), and calling
        # `ST_AsText` on that BLOB directly fails to bind. `UNION ALL BY
        # NAME` across tiers promotes BLOB to GEOMETRY by column name once
        # at least one tier's rows are typed GEOMETRY, so `ST_AsText` only
        # ever runs after the union, on the settled/ranked result.
        cand_parts = [
            f"SELECT id, geometry, deleted, {tier['rank']} AS __rank "
            f"FROM read_parquet('{tier['files']['way']['spatial']}') "
            f"WHERE id IN (SELECT id FROM {ids_tbl})"
            for tier in tiers
            if tier["files"].get("way", {}).get("spatial")
        ]
        if cand_parts:
            union_sql = "\nUNION ALL BY NAME\n".join(cand_parts)
            best = con.execute(
                f"SELECT id, ST_AsText(geometry) AS wkt FROM (\n"
                f"  SELECT * FROM ({union_sql}) __c\n"
                f"  QUALIFY row_number() OVER (PARTITION BY id ORDER BY __rank DESC) = 1\n"
                f") WHERE NOT deleted"
            ).fetchall()
            for wid, wkt in best:
                if wkt:
                    resolved[wid] = wkt

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


def resolve_way_geometries(
    con,
    manifest: catalog.Manifest,
    ids: list[int],
    bbox_hints: Optional[list[tuple[int, int, int, int]]] = None,
) -> dict[int, list[tuple[float, float]]]:
    """(way id) -> geometry points, for relation members: a relation's
    member ways lie inside the relation's own bbox (design.md 3.1 item 3),
    so when the caller passes the bboxes of the relations these way ids
    came from (`bbox_hints`), resolve from the spatial way files of the
    leaf cells those bboxes touch -- the spatial rows already carry
    `geometry`, so the common case is a single pass with no second
    hydration. `bbox_hints` omitted or empty skips straight to byid, same
    as before this existed.

    `build_way_hydrate_via_bbox_select`'s own fallback (the bbox guard
    tripping, or a way just not found in the bbox-scoped read) lands on
    byid, whose way rows carry no geometry column at all (contract section
    4) -- only `cell`. For any id that comes back that way, fall back once
    more to the (cell, id) spatial join `hydrate_way_geometry` uses for
    top-level way rows, so member-way geometry is still resolved; this
    second pass only runs for the ids the first one didn't already settle."""
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
    sql, _nfiles = sources.build_way_hydrate_via_bbox_select(con, manifest, ids_table, bbox_selects, set())
    if not sql:
        return {}
    out: dict[int, list[tuple[float, float]]] = {}
    missing_cell_pairs: list[tuple[str, int]] = []
    for wid, cell, wkt in con.execute(f"SELECT id, cell, ST_AsText(geometry) FROM ({sql}) __r").fetchall():
        pts = parse_linestring_wkt(wkt)
        if pts is not None:
            out[wid] = pts
        elif cell:
            missing_cell_pairs.append((cell, wid))

    if missing_cell_pairs:
        tc = manifest.table_cells("way")
        needed_cells = sorted({c for c, _ in missing_cell_pairs if c in tc})
        if needed_cells:
            files = [manifest.path(tc[c]["path"]) for c in needed_cells]
            pairs_table = idset.register_pairs_table(con, missing_cell_pairs)
            for wid, wkt in con.execute(
                f"SELECT t.id, ST_AsText(t.geometry) FROM read_parquet({_quote_list(files)}) t "
                f"JOIN {pairs_table} p ON t.cell = p.cell AND t.id = p.id"
            ).fetchall():
                pts = parse_linestring_wkt(wkt)
                if pts is not None:
                    out[wid] = pts
    return out


def _point_in_bbox(lon: float, lat: float, bbox) -> bool:
    s, w, n, e = bbox
    return s <= lat <= n and w <= lon <= e


def clip_way_geometry(
    pts: list[tuple[float, float]], bbox
) -> list[Optional[tuple[float, float]]]:
    """`out geom(s,w,n,e)` clipping (module docstring): a vertex keeps its
    real (lon, lat) if it or an adjacent vertex is inside `bbox`, else it
    becomes None (rendered as JSON null / a bare XML `<nd ref=.../>`)."""
    n = len(pts)
    inside = [_point_in_bbox(lon, lat, bbox) for lon, lat in pts]
    return [
        pts[i] if (inside[i] or (i > 0 and inside[i - 1]) or (i < n - 1 and inside[i + 1])) else None
        for i in range(n)
    ]


def clipped_bounds(points: list[Optional[tuple[float, float]]]) -> Optional[dict]:
    kept = [p for p in points if p is not None]
    if not kept:
        return None
    lons = [p[0] for p in kept]
    lats = [p[1] for p in kept]
    return {
        "minlat": round(min(lats), 7),
        "minlon": round(min(lons), 7),
        "maxlat": round(max(lats), 7),
        "maxlon": round(max(lons), 7),
    }


def _geometry_points_to_json(points: list[Optional[tuple[float, float]]]) -> list[Optional[dict]]:
    out: list[Optional[dict]] = []
    for p in points:
        if p is None:
            out.append(None)
        else:
            lon, lat = p
            out.append({"lat": round(lat, 7), "lon": round(lon, 7)})
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
        way_geoms = (
            resolve_way_geometries(con, manifest, list(way_ids_needed), relation_bboxes)
            if way_ids_needed
            else {}
        )
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
        pts = parse_linestring_wkt(row.get("geometry_wkt")) if out.geometry == "geom" else None
        clipped = clip_way_geometry(pts, out.geom_bbox) if (pts is not None and out.geom_bbox is not None) else None

        if out.geometry in ("bb", "geom"):
            b = clipped_bounds(clipped) if clipped is not None else bounds_dict(row)
            if b:
                el["bounds"] = b
        if v != "ids" and v != "tags" and not out.noids:
            el["nodes"] = list(row["refs"]) if row["refs"] else []
        if out.geometry == "geom" and pts is not None:
            el["geometry"] = _geometry_points_to_json(clipped if clipped is not None else pts)
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
                        member_pts = way_geoms[m["ref"]]
                        if out.geom_bbox is not None:
                            # Same per-vertex rule as a top-level way
                            # (module docstring), but a member has no
                            # `ref` list to anchor a placeholder against,
                            # so a dropped vertex is simply omitted here
                            # instead of appearing as a null/empty entry.
                            member_pts = [
                                p for p in clip_way_geometry(member_pts, out.geom_bbox) if p is not None
                            ]
                        md["geometry"] = [
                            {"lat": round(lat, 7), "lon": round(lon, 7)} for lon, lat in member_pts
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
