"""``osmpq history init``: start a history dataset from a root's *current*
tables (docs/m4-contracts.md section 2; the "fresh at `since`" state).

Every current row becomes its own ``minor = 0`` state with ``valid_from``
= the version's ``timestamp``, ``valid_to`` NULL and ``visible`` true --
which is exactly what the history of a dataset looks like at the instant
its base extract was taken. From there the minutely updater appends every
later version (section 5.1) and compaction folds them into the base
history (section 5.2), so a regional dataset gets a complete history from
its base timestamp on without ever re-reading the base PBF.

Compared with ``osmpq history build --pbf ... --osc ...`` (section 4),
which recomputes every state from the raw object stream, this path
streams one Parquet file at a time (``COPY (SELECT ...) TO ...``) and never
materializes a table of all nodes, so its memory use is flat regardless of
the extract size. It requires a root whose current state is a single
generation with no delta tiers (compact first otherwise), because the
history must start from exactly the state the manifest describes.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from osmpq.build import common
from osmpq.history import schema as schema_mod
from osmpq.layout import manifest as manifest_mod

_TYPES = ("node", "way", "relation")
def _extra_sql(since_sql: str) -> str:
    # A row without a timestamp (older extracts, hand-built fixtures) has
    # existed at least since the extract was taken.
    return (
        f"0::INTEGER AS minor, COALESCE(\"timestamp\", {since_sql}) AS valid_from, "
        "CAST(NULL AS TIMESTAMP) AS valid_to, TRUE AS visible"
    )


def _log(msg: str) -> None:
    common.log("osmpq history init", msg)


@dataclass
class HistoryInitOptions:
    root: str
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    tmpdir: Optional[str] = None


def _esc(p: str) -> str:
    return p.replace("'", "''")


def _copy(con, src: Path, dst: Path, since_sql: str) -> tuple[int, int]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    return common.copy_to_parquet(
        con,
        f"SELECT *, {_extra_sql(since_sql)} FROM read_parquet('{_esc(str(src))}')",
        dst,
        row_group_size_bytes=1_000_000,
    )


def history_init(opts: HistoryInitOptions) -> dict:
    t0 = time.time()
    root = Path(opts.root)
    man = manifest_mod.load_latest(opts.root)
    if man.history:
        raise SystemExit(f"{opts.root}: manifest already carries history (generation {man.history.get('generation')})")
    if man.deltas:
        raise SystemExit(f"{opts.root}: manifest has delta tiers; run `osmpq compact` first so the current state is one generation")

    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    if opts.threads:
        con.execute(f"SET threads={int(opts.threads)}")
    if opts.memory_limit:
        con.execute(f"SET memory_limit='{opts.memory_limit}'")
    if opts.tmpdir:
        Path(opts.tmpdir).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{_esc(Path(opts.tmpdir).as_posix())}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("INSTALL spatial; LOAD spatial;")

    gen = man.generation
    since_sql = f"TIMESTAMP '{man.timestamp_osm_base.replace('T', ' ').replace('Z', '')}'"
    spatial: dict = {}
    byid: dict = {}
    rows_by_type: dict = {}
    total_bytes = 0
    for typ in _TYPES:
        # byid: one history part per current part, same id ranges.
        parts = []
        for k, part in enumerate(man.byid.get(typ) or []):
            dst_rel = f"{schema_mod.byid_dir(gen, typ)}/part-{k:05d}.parquet"
            rows, size = _copy(con, root / part["path"], root / dst_rel, since_sql)
            parts.append({"path": dst_rel, "min_id": part["min_id"], "max_id": part["max_id"], "rows": rows, "bytes": size})
            total_bytes += size
        byid[typ] = parts
        rows_by_type[typ] = sum(p["rows"] for p in parts)

        # spatial: one history part per current file of each cell (nodes
        # have a tagged and an untagged file; both become parts).
        cells_out: dict = {}
        table = man.tables.get(typ) or {}
        cells = table.get("cells", table)
        for cell in sorted(cells):
            entry = cells[cell]
            files = [entry[k]["path"] for k in ("untagged", "tagged") if entry.get(k)] if typ == "node" else [entry["path"]]
            out_parts = []
            for n, rel in enumerate(files):
                dst_rel = f"{schema_mod.spatial_dir(gen, typ, cell)}/part-{n}.parquet"
                rows, size = _copy(con, root / rel, root / dst_rel, since_sql)
                if rows == 0:
                    (root / dst_rel).unlink(missing_ok=True)
                    continue
                out_parts.append({"path": dst_rel, "rows": rows, "bytes": size})
                total_bytes += size
            if out_parts:
                cells_out[cell] = out_parts
        spatial[typ] = cells_out
        _log(f"{typ}: {rows_by_type[typ]} rows, {len(cells_out)} cells in {time.time()-t0:.1f}s total")

    man.history = {
        "generation": gen,
        "since": man.timestamp_osm_base,
        "minor_versions": True,
        "spatial": spatial,
        "byid": byid,
        "tiers": {},
        "stats": {"rows": rows_by_type, "minor_rows": {t: 0 for t in _TYPES}, "bytes": total_bytes},
    }
    man.manifest_version = 5
    man.producer = {**(man.producer or {}), "history": "osmpq history init"}
    gen_number = manifest_mod.next_manifest_number(opts.root)
    manifest_mod.write_manifest(opts.root, man, gen_number)
    _log(f"done in {time.time()-t0:.1f}s; wrote manifest/{gen_number}.json")
    return {"since": man.timestamp_osm_base, "rows": rows_by_type, "bytes": total_bytes, "seconds": time.time() - t0, "manifest": gen_number}
