"""Cut a reference-complete bbox extract out of a larger PBF.

Semantics (close to `osmium extract --strategy smart`): keep nodes inside the
bbox, ways with at least one node inside the bbox plus all of their nodes,
and relations with at least one kept node/way member (relation members are
not completed further). Id selection runs in DuckDB (spatial extension's
ST_ReadOSM, which yields raw nodes/ways/relations); the copy runs in
libosmium via pyosmium filters so no Python per-object loop is involved.

Usage: python tools/bbox_extract.py IN.osm.pbf OUT.osm.pbf S,W,N,E [--threads N] [--memory 8GB]
"""
import argparse
import sys
import time

import duckdb
import osmium
import osmium.filter


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("bbox", help="S,W,N,E")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--memory", default="8GB")
    ap.add_argument("--tmpdir", default=None)
    a = ap.parse_args()
    s, w, n, e = (float(v) for v in a.bbox.split(","))
    t0 = time.time()

    con = duckdb.connect()
    con.execute("LOAD spatial")
    con.execute(f"SET threads={a.threads}")
    con.execute(f"SET memory_limit='{a.memory}'")
    if a.tmpdir:
        con.execute(f"SET temp_directory='{a.tmpdir}'")
    src = a.src.replace("'", "''")

    con.execute(f"""
        CREATE TABLE bbox_nodes AS
        SELECT id FROM ST_ReadOSM('{src}')
        WHERE kind = 'node' AND lat BETWEEN {s} AND {n} AND lon BETWEEN {w} AND {e}
    """)
    nb = con.execute("SELECT count(*) FROM bbox_nodes").fetchone()[0]
    print(f"nodes in bbox: {nb} ({time.time()-t0:.0f}s)", file=sys.stderr)

    con.execute(f"""
        CREATE TABLE way_refs AS
        SELECT id, refs FROM ST_ReadOSM('{src}') WHERE kind = 'way'
    """)
    con.execute("""
        CREATE TABLE kept_ways AS
        SELECT DISTINCT w.id FROM way_refs w, unnest(w.refs) AS u(ref)
        JOIN bbox_nodes b ON b.id = u.ref
    """)
    con.execute("""
        CREATE TABLE kept_nodes AS
        SELECT id FROM bbox_nodes
        UNION
        SELECT DISTINCT u.ref FROM way_refs w JOIN kept_ways k ON k.id = w.id, unnest(w.refs) AS u(ref)
    """)
    nw, nn = con.execute("SELECT (SELECT count(*) FROM kept_ways), (SELECT count(*) FROM kept_nodes)").fetchone()
    print(f"kept ways: {nw}, kept nodes incl. way completion: {nn} ({time.time()-t0:.0f}s)", file=sys.stderr)
    con.execute("DROP TABLE way_refs")

    con.execute(f"""
        CREATE TABLE rel_members AS
        SELECT id, refs, ref_types::VARCHAR[] AS ref_types FROM ST_ReadOSM('{src}') WHERE kind = 'relation'
    """)
    con.execute("""
        CREATE TABLE kept_rels AS
        SELECT DISTINCT r.id
        FROM rel_members r, unnest(list_zip(r.refs, r.ref_types)) AS t(z)
        WHERE (z[2] = 'node' AND z[1] IN (SELECT id FROM kept_nodes))
           OR (z[2] = 'way' AND z[1] IN (SELECT id FROM kept_ways))
    """)
    nr = con.execute("SELECT count(*) FROM kept_rels").fetchone()[0]
    print(f"kept relations: {nr} ({time.time()-t0:.0f}s)", file=sys.stderr)

    def ids(table: str):
        return [r[0] for r in con.execute(f"SELECT id FROM {table} ORDER BY id").fetchall()]

    node_filter = osmium.filter.IdFilter(ids("kept_nodes")).enable_for(osmium.osm.NODE)
    way_filter = osmium.filter.IdFilter(ids("kept_ways")).enable_for(osmium.osm.WAY)
    rel_filter = osmium.filter.IdFilter(ids("kept_rels")).enable_for(osmium.osm.RELATION)
    con.close()
    print(f"id filters built ({time.time()-t0:.0f}s); copying...", file=sys.stderr)

    with osmium.SimpleWriter(a.dst, overwrite=True) as writer:
        osmium.apply(a.src, node_filter, way_filter, rel_filter, writer)
    print(f"wrote {a.dst} in {time.time()-t0:.0f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
