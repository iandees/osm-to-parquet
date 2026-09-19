#!/usr/bin/env python3
"""``tools/diffcheck.py``: docs/m2-contracts.md section 7.1.

Samples ids touched since the dataset's base (from its delta byid files, or
from a ``--ids`` file / ``--since-seq`` cutoff the updater can write),
queries a local Overpass-compatible endpoint and the reference Overpass at
``[date:"T"]`` (``T`` = the manifest's ``timestamp_osm_base``, or ``--date``)
for those ids with ``out meta``, and compares existence, version, tags,
coordinates (1e-7), way refs and relation members. Deleted elements must be
absent on both sides. Prints a table; exits non-zero on any mismatch.

Reads the manifest as plain JSON (no dependency on
``osmpq.layout.manifest.Manifest`` gaining v3 ``deltas`` fields).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx

DEFAULT_CACHE_DIR = "tests/corpus/.cache/diffcheck"
COORD_TOL = 1e-7
DEFAULT_SAMPLE = 200
DEFAULT_BATCH_SIZE = 100

OVERPASS_TYPE = {"node": "node", "way": "way", "relation": "relation"}


def _esc(path: Path) -> str:
    return str(path).replace("'", "''")


# --------------------------------------------------------------------------
# id sourcing: from the root's delta byid files, or from a --ids file
# --------------------------------------------------------------------------


def load_manifest(root: Path) -> dict:
    latest = int((root / "manifest" / "LATEST").read_text().strip())
    return json.loads((root / "manifest" / f"{latest}.json").read_text())


def touched_ids_from_deltas(con, root: Path, man: dict, since_seq: Optional[int] = None) -> dict[str, dict[int, bool]]:
    """``{"node": {id: deleted}, "way": {...}, "relation": {...}}``, newest
    tier wins (processed oldest -> newest so a later tier's row overwrites
    an earlier one for the same id), read from the manifest's current
    ``deltas`` block (empty once ``osmpq compact`` has run)."""
    deltas = man.get("deltas") or {}
    result: dict[str, dict[int, bool]] = {"node": {}, "way": {}, "relation": {}}
    for tier in ("week", "day", "hour"):
        info = deltas.get(tier)
        if not info:
            continue
        for table in ("node", "way", "relation"):
            files = (info.get("files") or {}).get(table)
            if not files or not files.get("byid"):
                continue
            path = root / files["byid"]
            rows = con.execute(f"SELECT id, deleted, seq FROM read_parquet('{_esc(path)}')").fetchall()
            for id_, del_, seq_ in rows:
                if since_seq is not None and (seq_ is None or seq_ < since_seq):
                    continue
                result[table][int(id_)] = bool(del_)
    return result


def load_ids_file(path: Path) -> dict[str, dict[int, bool]]:
    """``--ids FILE``: JSON ``{"node": [[id, deleted], ...], "way": [...],
    "relation": [...]}`` -- e.g. written by the updater from the ids it
    just touched, so diffcheck can run after ``osmpq compact`` has emptied
    the manifest's own ``deltas`` block."""
    obj = json.loads(path.read_text())
    out: dict[str, dict[int, bool]] = {"node": {}, "way": {}, "relation": {}}
    for table in out:
        for entry in obj.get(table, []):
            id_ = int(entry[0])
            deleted = bool(entry[1]) if len(entry) > 1 else False
            out[table][id_] = deleted
    return out


def sample_ids(touched: dict[str, dict[int, bool]], sample: int) -> dict[str, dict[int, bool]]:
    """Evenly-spread sample of at most ``sample`` ids per type (all of them
    if fewer), per m2-contracts.md section 7.1."""
    out: dict[str, dict[int, bool]] = {}
    for table, id_map in touched.items():
        ids = sorted(id_map.keys())
        if sample and len(ids) > sample:
            step = len(ids) / sample
            ids = [ids[int(i * step)] for i in range(sample)]
        out[table] = {i: id_map[i] for i in ids}
    return out


# --------------------------------------------------------------------------
# Overpass query construction + HTTP
# --------------------------------------------------------------------------


def build_query(ids_by_table: dict[str, list[int]], date: Optional[str] = None) -> str:
    settings = "[out:json]"
    if date:
        settings += f'[date:"{date}"]'
    stmts = []
    for table, ids in ids_by_table.items():
        if not ids:
            continue
        id_list = ",".join(str(i) for i in ids)
        stmts.append(f"{OVERPASS_TYPE[table]}(id:{id_list});")
    if not stmts:
        return f"{settings};out count;"
    body = "(" + "".join(stmts) + ");" if len(stmts) > 1 else stmts[0]
    return f"{settings};\n{body}\nout meta;"


def _chunks(ids: list[int], size: int) -> list[list[int]]:
    return [ids[i : i + size] for i in range(0, len(ids), size)] or [[]]


@dataclass
class FetchResult:
    ok: bool
    status_code: int
    body: str
    error: Optional[str] = None


def fetch(client: httpx.Client, url: str, query: str, timeout: float, retries: int, sleep_between: float) -> FetchResult:
    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = client.post(url, data={"data": query}, timeout=timeout)
        except httpx.HTTPError as exc:
            last_exc = f"http error: {exc}"
            time.sleep(sleep_between * (attempt + 1))
            continue
        if resp.status_code in (429, 504) and attempt < retries:
            time.sleep(sleep_between * (2**attempt))
            continue
        return FetchResult(ok=resp.status_code == 200, status_code=resp.status_code, body=resp.text)
    return FetchResult(ok=False, status_code=0, body="", error=last_exc or "exhausted retries")


def parse_elements(body: str) -> list[dict[str, Any]]:
    obj = json.loads(body)
    return obj.get("elements", [])


def elements_by_key(elements: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    return {(el.get("type"), el.get("id")): el for el in elements if el.get("type") != "count"}


# --------------------------------------------------------------------------
# cache (reference responses only, keyed by the exact query text)
# --------------------------------------------------------------------------


def cache_key(query: str) -> str:
    return hashlib.sha256(query.encode()).hexdigest()


def cache_path(cache_dir: Path, key: str) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{key}.json"


def cache_write(path: Path, result: FetchResult) -> None:
    path.write_text(json.dumps({"ok": result.ok, "status_code": result.status_code, "body": result.body, "error": result.error}))


def cache_read(path: Path) -> Optional[FetchResult]:
    if not path.exists():
        return None
    obj = json.loads(path.read_text())
    return FetchResult(**obj)


# --------------------------------------------------------------------------
# comparison (docs/m2-contracts.md section 7.1)
# --------------------------------------------------------------------------


def coords_close(a: Optional[float], b: Optional[float], tol: float = COORD_TOL) -> bool:
    if a is None or b is None:
        return a == b
    return abs(a - b) <= tol


def _normalize_members(members: Optional[list[dict]]) -> list[tuple[str, int, str]]:
    return [(m.get("type"), m.get("ref"), m.get("role") or "") for m in (members or [])]


def compare_element(etype: str, ref_el: Optional[dict], local_el: Optional[dict]) -> list[str]:
    """Compare one (type, id) expected to exist: both missing is reported
    as a problem too (the id was supposedly touched and alive)."""
    problems: list[str] = []
    if ref_el is None and local_el is None:
        problems.append("missing on both reference and local")
        return problems
    if ref_el is None:
        problems.append("missing on reference (present on local)")
        return problems
    if local_el is None:
        problems.append("missing on local (present on reference)")
        return problems

    ref_tags = ref_el.get("tags") or {}
    local_tags = local_el.get("tags") or {}
    if ref_tags != local_tags:
        problems.append(f"tags differ: ref={ref_tags!r} local={local_tags!r}")

    if ref_el.get("version") != local_el.get("version"):
        problems.append(f"version differs: ref={ref_el.get('version')!r} local={local_el.get('version')!r}")

    if etype == "node":
        if not coords_close(ref_el.get("lat"), local_el.get("lat")) or not coords_close(ref_el.get("lon"), local_el.get("lon")):
            problems.append(
                f"coordinates differ: ref=({ref_el.get('lat')},{ref_el.get('lon')}) "
                f"local=({local_el.get('lat')},{local_el.get('lon')})"
            )
    elif etype == "way":
        if ref_el.get("nodes") != local_el.get("nodes"):
            problems.append(f"way refs differ: ref={ref_el.get('nodes')!r} local={local_el.get('nodes')!r}")
    elif etype == "relation":
        if _normalize_members(ref_el.get("members")) != _normalize_members(local_el.get("members")):
            problems.append("relation members differ")

    return problems


def compare_deleted(ref_el: Optional[dict], local_el: Optional[dict]) -> list[str]:
    problems = []
    if ref_el is not None:
        problems.append("present on reference (expected deleted)")
    if local_el is not None:
        problems.append("present on local (expected deleted)")
    return problems


@dataclass
class TypeReport:
    table: str
    sampled: int
    checked_alive: int = 0
    checked_deleted: int = 0
    passed: int = 0
    failed: int = 0
    problems: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------


def run(args: argparse.Namespace) -> int:
    import duckdb

    root = Path(args.root)
    man = load_manifest(root)
    date = args.date or man.get("timestamp_osm_base")

    con = duckdb.connect()

    if args.ids:
        touched = load_ids_file(Path(args.ids))
    else:
        touched = touched_ids_from_deltas(con, root, man, since_seq=args.since_seq)

    sampled = sample_ids(touched, args.sample)
    total_sampled = sum(len(v) for v in sampled.values())
    if total_sampled == 0:
        print("diffcheck: nothing to check (no touched ids found -- deltas empty and no --ids given)", file=sys.stderr)
        return 0

    cache_dir = Path(args.cache_dir)
    reports: dict[str, TypeReport] = {t: TypeReport(table=t, sampled=len(sampled[t])) for t in sampled}

    with httpx.Client() as client:
        for table, id_map in sampled.items():
            if not id_map:
                continue
            ids = sorted(id_map.keys())
            for batch in _chunks(ids, args.batch_size):
                if not batch:
                    continue
                ref_query = build_query({table: batch}, date=date)
                key = cache_key(ref_query)
                cpath = cache_path(cache_dir, key)
                cached = cache_read(cpath)
                if cached is not None and cached.ok:
                    ref_result = cached
                else:
                    ref_result = fetch(client, args.reference, ref_query, args.timeout, args.retries, args.sleep)
                    cache_write(cpath, ref_result)
                    time.sleep(args.sleep)

                if not ref_result.ok:
                    reports[table].failed += len(batch)
                    reports[table].problems.append(f"reference fetch failed for batch starting {batch[0]}: HTTP {ref_result.status_code} {ref_result.error or ''}")
                    continue
                ref_elements = elements_by_key(parse_elements(ref_result.body))

                local_elements: dict[tuple[str, int], dict] = {}
                if args.local:
                    local_query = build_query({table: batch}, date=None)
                    local_result = fetch(client, args.local, local_query, args.timeout, args.retries, 0.0)
                    if not local_result.ok:
                        reports[table].failed += len(batch)
                        reports[table].problems.append(f"local fetch failed for batch starting {batch[0]}: HTTP {local_result.status_code} {local_result.error or ''}")
                        continue
                    local_elements = elements_by_key(parse_elements(local_result.body))
                elif not args.reference_only:
                    reports[table].failed += len(batch)
                    reports[table].problems.append("no --local given")
                    continue
                else:
                    continue

                for id_ in batch:
                    key2 = (OVERPASS_TYPE[table], id_)
                    ref_el = ref_elements.get(key2)
                    local_el = local_elements.get(key2)
                    if id_map[id_]:
                        reports[table].checked_deleted += 1
                        problems = compare_deleted(ref_el, local_el)
                    else:
                        reports[table].checked_alive += 1
                        problems = compare_element(table, ref_el, local_el)
                    if problems:
                        reports[table].failed += 1
                        reports[table].problems.append(f"{table}/{id_}: " + "; ".join(problems))
                    else:
                        reports[table].passed += 1

    print_table(reports)
    if args.json:
        Path(args.json).write_text(json.dumps(
            {t: {"sampled": r.sampled, "passed": r.passed, "failed": r.failed, "problems": r.problems} for t, r in reports.items()},
            indent=2,
        ))
    return 0 if all(r.failed == 0 for r in reports.values()) else 1


def print_table(reports: dict[str, TypeReport]) -> None:
    headers = ["type", "sampled", "alive", "deleted", "passed", "failed"]
    rows = []
    for t, r in reports.items():
        rows.append([t, str(r.sampled), str(r.checked_alive), str(r.checked_deleted), str(r.passed), str(r.failed)])
    widths = [max(len(h), *(len(row[i]) for row in rows)) if rows else len(h) for i, h in enumerate(headers)]

    def fmt(cells: list[str]) -> str:
        return "  ".join(c.ljust(w) for c, w in zip(cells, widths))

    print(fmt(headers))
    print(fmt(["-" * w for w in widths]))
    for r_row, (t, r) in zip(rows, reports.items()):
        print(fmt(r_row))
        for p in r.problems[:10]:
            print(f"    {p}")
        if len(r.problems) > 10:
            print(f"    ... and {len(r.problems) - 10} more")

    total_passed = sum(r.passed for r in reports.values())
    total_failed = sum(r.failed for r in reports.values())
    print()
    print(f"{total_passed}/{total_passed + total_failed} PASS")


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", required=True, help="dataset root")
    p.add_argument("--reference", required=True, help="reference Overpass endpoint URL")
    p.add_argument("--local", default=None, help="local Overpass-compatible endpoint URL")
    p.add_argument("--date", default=None, help='[date:"..."] for the reference side; default: manifest timestamp_osm_base')
    p.add_argument("--ids", default=None, help="JSON file of ids to check instead of reading the root's delta byid files")
    p.add_argument("--since-seq", type=int, default=None, help="only touched ids with seq >= this (delta-file source only)")
    p.add_argument("--sample", type=int, default=DEFAULT_SAMPLE, help="max ids per type to check (all if fewer)")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="ids per Overpass request")
    p.add_argument("--sleep", type=float, default=2.0, help="seconds between reference calls")
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--cache-dir", default=DEFAULT_CACHE_DIR)
    p.add_argument("--json", default=None, help="write the full report to this JSON file")
    p.add_argument("--reference-only", action="store_true", help="only fetch+cache reference responses; skip --local")
    args = p.parse_args(argv)
    if not args.local and not args.reference_only:
        p.error("--local is required unless --reference-only is given")
    return args


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
