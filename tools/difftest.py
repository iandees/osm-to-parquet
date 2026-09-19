#!/usr/bin/env python3
"""Differential test harness for osmpq: compare a local Overpass-QL server
against a reference Overpass instance over a corpus of queries.

See docs/m0-contracts.md section 9 for the contract this implements, and
tools/README.md for usage.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx

DEFAULT_CORPUS = "tests/corpus"
DEFAULT_BBOXES = "tests/corpus/bboxes.json"
DEFAULT_CACHE_DIRNAME = ".cache"

# ---------------------------------------------------------------------------
# Query loading / substitution
# ---------------------------------------------------------------------------


def load_bboxes(path: Path) -> dict[str, list[float]]:
    with open(path) as f:
        return json.load(f)


def load_corpus(corpus_dir: Path, only: Optional[str]) -> list[Path]:
    files = sorted(corpus_dir.glob("*.overpassql"))
    if only:
        files = [f for f in files if fnmatch.fnmatch(f.name, only)]
    return files


def substitute_bbox(query: str, bbox: list[float]) -> str:
    s, w, n, e = bbox
    bbox_str = f"{s},{w},{n},{e}"
    return query.replace("{{bbox}}", bbox_str)


def force_out_json(query: str) -> str:
    """Force [out:json] for comparison purposes, regardless of what the
    corpus entry originally asked for."""
    if re.search(r"\[out:\s*xml\s*\]", query):
        return re.sub(r"\[out:\s*xml\s*\]", "[out:json]", query, count=1)
    if re.search(r"\[out:\s*json\s*\]", query):
        return query
    # No [out:] setting present at all: add one at the front.
    return "[out:json]" + query


def is_xml_variant(query: str) -> bool:
    return bool(re.search(r"\[out:\s*xml\s*\]", query))


_LEADING_COMMENT_RE = re.compile(r"\s*(?://[^\n]*\n|/\*.*?\*/)", re.DOTALL)


def _split_leading_comments(query: str) -> tuple[str, str]:
    """Split off // and /* */ comments (and surrounding whitespace) at the
    very start of the query, so settings-block detection can look past
    them. Returns (leading_comments, rest)."""
    pos = 0
    while True:
        m = _LEADING_COMMENT_RE.match(query, pos)
        if not m:
            break
        pos = m.end()
    return query[:pos], query[pos:]


def prepend_date(query: str, date: str) -> str:
    """Insert [date:"<date>"] into the existing settings-block prefix if
    there is one, else prepend a fresh [out:json][date:...]; block. Leading
    comments (as in the static id-lookup corpus entries) are preserved
    ahead of the settings block."""
    date_clause = f'[date:"{date}"]'
    leading, remainder = _split_leading_comments(query)
    m = re.match(r"^(\s*(?:\[[^\]]*\]\s*)+)", remainder)
    if m:
        prefix = m.group(1)
        rest = remainder[len(prefix) :]
        return leading + prefix.rstrip() + date_clause + rest
    return leading + f"[out:json]{date_clause};" + remainder


# ---------------------------------------------------------------------------
# HTTP calls
# ---------------------------------------------------------------------------


@dataclass
class FetchResult:
    ok: bool
    status_code: int
    body: str
    content_type: str
    elapsed_ms: float
    error: Optional[str] = None
    remark: Optional[str] = None


def fetch(
    client: httpx.Client,
    url: str,
    query: str,
    timeout: float,
    retries: int,
    sleep_between: float,
    label: str = "",
) -> FetchResult:
    """POST a query to an Overpass-compatible endpoint, retrying on 429/504."""
    last_exc: Optional[str] = None
    for attempt in range(retries + 1):
        t0 = time.monotonic()
        try:
            resp = client.post(url, data={"data": query}, timeout=timeout)
        except httpx.TimeoutException as exc:
            last_exc = f"timeout: {exc}"
            time.sleep(sleep_between * (attempt + 1))
            continue
        except httpx.HTTPError as exc:
            last_exc = f"http error: {exc}"
            time.sleep(sleep_between * (attempt + 1))
            continue
        elapsed_ms = (time.monotonic() - t0) * 1000.0
        if resp.status_code in (429, 504) and attempt < retries:
            wait = sleep_between * (2**attempt)
            time.sleep(wait)
            continue
        content_type = resp.headers.get("content-type", "")
        body = resp.text
        remark = extract_remark(body, content_type)
        return FetchResult(
            ok=resp.status_code == 200 and remark is None,
            status_code=resp.status_code,
            body=body,
            content_type=content_type,
            elapsed_ms=elapsed_ms,
            remark=remark,
        )
    elapsed_ms = 0.0
    return FetchResult(
        ok=False,
        status_code=0,
        body="",
        content_type="",
        elapsed_ms=elapsed_ms,
        error=last_exc or "exhausted retries",
    )


def extract_remark(body: str, content_type: str) -> Optional[str]:
    """Overpass reports both parse errors (HTML body, non-200) and runtime
    errors (200 with a `remark` field/element) as text. Pull that text out
    so callers can treat either as a FAIL with a message."""
    stripped = body.lstrip()
    if stripped.startswith("<!DOCTYPE") or stripped.startswith("<html") or "<html" in stripped[:200].lower():
        m = re.search(r"Error</strong>:\s*(.*?)</p>", body, re.DOTALL)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()
        m = re.search(r"<p>(.*?)</p>", body, re.DOTALL)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()
        return "unrecognized HTML error body"
    if "json" in content_type or stripped.startswith("{"):
        try:
            obj = json.loads(body)
        except json.JSONDecodeError:
            return None
        remark = obj.get("remark")
        return remark
    if "xml" in content_type or stripped.startswith("<?xml") or stripped.startswith("<osm"):
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            return None
        remark_el = root.find("remark")
        if remark_el is not None and remark_el.text:
            return remark_el.text.strip()
    return None


# ---------------------------------------------------------------------------
# Parsing responses into elements
# ---------------------------------------------------------------------------


def parse_elements(body: str, content_type: str) -> list[dict[str, Any]]:
    """Parse an Overpass JSON response body into its element list. We only
    ever compare the [out:json]-forced responses, so this only needs to
    handle JSON."""
    obj = json.loads(body)
    return obj.get("elements", [])


def elements_by_key(elements: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for el in elements:
        if el.get("type") == "count":
            continue
        key = (el.get("type"), el.get("id"))
        out[key] = el
    return out


def extract_count(elements: list[dict[str, Any]]) -> Optional[dict[str, str]]:
    for el in elements:
        if el.get("type") == "count":
            return el.get("tags", {})
    return None


# ---------------------------------------------------------------------------
# Comparison (section 9)
# ---------------------------------------------------------------------------


COORD_TOL = 1e-7
GEOM_TOL = 1e-6


@dataclass
class Comparison:
    status: str  # "PASS" or "FAIL"
    ref_count: int
    local_count: int
    missing: list[tuple[str, int]] = field(default_factory=list)
    extra: list[tuple[str, int]] = field(default_factory=list)
    tag_mismatches: list[dict[str, Any]] = field(default_factory=list)
    other_mismatches: list[str] = field(default_factory=list)
    count_mismatch: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "ref_count": self.ref_count,
            "local_count": self.local_count,
            "missing": [f"{t}/{i}" for t, i in self.missing[:5]],
            "missing_total": len(self.missing),
            "extra": [f"{t}/{i}" for t, i in self.extra[:5]],
            "extra_total": len(self.extra),
            "tag_mismatches": self.tag_mismatches[:5],
            "tag_mismatches_total": len(self.tag_mismatches),
            "other_mismatches": self.other_mismatches[:5],
            "count_mismatch": self.count_mismatch,
        }


def coords_close(a: Optional[float], b: Optional[float], tol: float) -> bool:
    if a is None or b is None:
        return a == b
    return abs(a - b) <= tol


def geometry_close(a: Optional[list[dict]], b: Optional[list[dict]]) -> bool:
    if a is None or b is None:
        return a == b
    if len(a) != len(b):
        return False
    for pa, pb in zip(a, b):
        if not coords_close(pa.get("lat"), pb.get("lat"), GEOM_TOL):
            return False
        if not coords_close(pa.get("lon"), pb.get("lon"), GEOM_TOL):
            return False
    return True


def compare_elements(ref_el: dict[str, Any], local_el: dict[str, Any]) -> list[str]:
    """Compare one pair of same-(type,id) elements. Returns a list of
    human-readable mismatch descriptions (empty if they match)."""
    problems: list[str] = []
    key = f"{ref_el.get('type')}/{ref_el.get('id')}"

    ref_tags = ref_el.get("tags") or {}
    local_tags = local_el.get("tags") or {}
    if ref_tags != local_tags:
        missing_keys = {k: v for k, v in ref_tags.items() if local_tags.get(k) != v}
        extra_keys = {k: v for k, v in local_tags.items() if ref_tags.get(k) != v}
        problems.append(f"{key}: tags differ (ref-only/changed={missing_keys!r} local-only/changed={extra_keys!r})")

    if ref_el.get("type") == "node":
        if not coords_close(ref_el.get("lat"), local_el.get("lat"), COORD_TOL) or not coords_close(
            ref_el.get("lon"), local_el.get("lon"), COORD_TOL
        ):
            problems.append(
                f"{key}: coordinates differ (ref={ref_el.get('lat')},{ref_el.get('lon')} "
                f"local={local_el.get('lat')},{local_el.get('lon')})"
            )

    if ref_el.get("type") == "way":
        ref_nodes = ref_el.get("nodes")
        local_nodes = local_el.get("nodes")
        if ref_nodes is not None or local_nodes is not None:
            if ref_nodes != local_nodes:
                problems.append(f"{key}: way nodes list differs")

    if "geometry" in ref_el or "geometry" in local_el:
        if not geometry_close(ref_el.get("geometry"), local_el.get("geometry")):
            problems.append(f"{key}: geometry differs")

    return problems


def compare(ref_elements: list[dict[str, Any]], local_elements: list[dict[str, Any]]) -> Comparison:
    ref_count = extract_count(ref_elements)
    local_count = extract_count(local_elements)
    if ref_count is not None or local_count is not None:
        status = "PASS" if ref_count == local_count else "FAIL"
        cmp = Comparison(status=status, ref_count=1 if ref_count else 0, local_count=1 if local_count else 0)
        if status == "FAIL":
            cmp.count_mismatch = {"ref": ref_count, "local": local_count}
        return cmp

    ref_by_key = elements_by_key(ref_elements)
    local_by_key = elements_by_key(local_elements)

    ref_keys = set(ref_by_key)
    local_keys = set(local_by_key)

    missing = sorted(ref_keys - local_keys)  # in reference, not local
    extra = sorted(local_keys - ref_keys)  # in local, not reference
    common = ref_keys & local_keys

    tag_mismatches: list[dict[str, Any]] = []
    other_mismatches: list[str] = []
    for key in sorted(common):
        problems = compare_elements(ref_by_key[key], local_by_key[key])
        for p in problems:
            if "tags differ" in p:
                tag_mismatches.append({"element": f"{key[0]}/{key[1]}", "detail": p})
            else:
                other_mismatches.append(p)

    status = "PASS" if not missing and not extra and not tag_mismatches and not other_mismatches else "FAIL"

    return Comparison(
        status=status,
        ref_count=len(ref_elements),
        local_count=len(local_elements),
        missing=missing,
        extra=extra,
        tag_mismatches=tag_mismatches,
        other_mismatches=other_mismatches,
    )


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


def cache_key(query_file: str, bbox_name: str, query_text: str) -> str:
    h = hashlib.sha256()
    h.update(query_file.encode())
    h.update(b"\0")
    h.update(bbox_name.encode())
    h.update(b"\0")
    h.update(query_text.encode())
    return h.hexdigest()


def cache_path(corpus_dir: Path, key: str) -> Path:
    d = corpus_dir / DEFAULT_CACHE_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{key}.json"


def cache_write(path: Path, fetch_result: FetchResult) -> None:
    with open(path, "w") as f:
        json.dump(
            {
                "ok": fetch_result.ok,
                "status_code": fetch_result.status_code,
                "body": fetch_result.body,
                "content_type": fetch_result.content_type,
                "elapsed_ms": fetch_result.elapsed_ms,
                "error": fetch_result.error,
                "remark": fetch_result.remark,
            },
            f,
        )


def cache_read(path: Path) -> Optional[FetchResult]:
    if not path.exists():
        return None
    with open(path) as f:
        obj = json.load(f)
    return FetchResult(**obj)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

# A few queries whose interesting behavior varies most by geography; these
# run against every bbox by default. Everything else runs against just the
# first bbox in bboxes.json.
ALL_BBOX_PATTERNS = [
    "01_wizard_cafe_json.overpassql",
    "03_wizard_building.overpassql",
    "04_wizard_highway_residential.overpassql",
    "05_wizard_shop.overpassql",
    "06_wizard_natural_water.overpassql",
    "07_wizard_leisure_park.overpassql",
]


@dataclass
class QueryRunReport:
    query_file: str
    bbox_name: str
    status: str
    ref_elements: int
    local_elements: Optional[int]
    missing: int
    extra: int
    tag_mismatches: int
    ref_ms: float
    local_ms: Optional[float]
    message: str = ""
    detail: dict[str, Any] = field(default_factory=dict)


def bboxes_for(query_file: str, bbox_names: list[str], bbox_name_arg: Optional[str]) -> list[str]:
    if bbox_name_arg:
        return [bbox_name_arg]
    if query_file in ALL_BBOX_PATTERNS:
        return list(bbox_names)
    return [bbox_names[0]]


def run(args: argparse.Namespace) -> int:
    corpus_dir = Path(args.corpus)
    bboxes = load_bboxes(Path(args.bboxes))
    bbox_names = list(bboxes.keys())
    files = load_corpus(corpus_dir, args.only)

    if not files:
        print(f"No corpus files matched in {corpus_dir} (only={args.only!r})", file=sys.stderr)
        return 2

    reports: list[QueryRunReport] = []

    with httpx.Client() as client:
        for qfile in files:
            raw_query = qfile.read_text()
            for bbox_name in bboxes_for(qfile.name, bbox_names, args.bbox_name):
                bbox = bboxes[bbox_name]
                substituted = substitute_bbox(raw_query, bbox)
                ref_query = force_out_json(substituted)
                if args.date:
                    ref_query = prepend_date(ref_query, args.date)
                local_query_json = force_out_json(substituted)

                key = cache_key(qfile.name, bbox_name, ref_query)
                cpath = cache_path(corpus_dir, key)

                # --- reference side ---
                if args.local_only:
                    ref_result = cache_read(cpath)
                    if ref_result is None:
                        reports.append(
                            QueryRunReport(
                                query_file=qfile.name,
                                bbox_name=bbox_name,
                                status="FAIL",
                                ref_elements=0,
                                local_elements=None,
                                missing=0,
                                extra=0,
                                tag_mismatches=0,
                                ref_ms=0.0,
                                local_ms=None,
                                message=f"no cached reference response at {cpath}; run --reference-only first",
                            )
                        )
                        continue
                else:
                    ref_result = fetch(
                        client,
                        args.reference,
                        ref_query,
                        timeout=args.timeout,
                        retries=args.retries,
                        sleep_between=args.sleep,
                        label="reference",
                    )
                    cache_write(cpath, ref_result)
                    time.sleep(args.sleep)

                if not ref_result.ok:
                    msg = ref_result.remark or ref_result.error or f"HTTP {ref_result.status_code}"
                    reports.append(
                        QueryRunReport(
                            query_file=qfile.name,
                            bbox_name=bbox_name,
                            status="FAIL",
                            ref_elements=0,
                            local_elements=None,
                            missing=0,
                            extra=0,
                            tag_mismatches=0,
                            ref_ms=ref_result.elapsed_ms,
                            local_ms=None,
                            message=f"reference error: {msg}",
                        )
                    )
                    continue

                if args.reference_only:
                    ref_elements = parse_elements(ref_result.body, ref_result.content_type)
                    reports.append(
                        QueryRunReport(
                            query_file=qfile.name,
                            bbox_name=bbox_name,
                            status="PASS",
                            ref_elements=len(ref_elements),
                            local_elements=None,
                            missing=0,
                            extra=0,
                            tag_mismatches=0,
                            ref_ms=ref_result.elapsed_ms,
                            local_ms=None,
                            message="reference-only (cached)",
                        )
                    )
                    continue

                # --- local side ---
                if args.local is None:
                    reports.append(
                        QueryRunReport(
                            query_file=qfile.name,
                            bbox_name=bbox_name,
                            status="FAIL",
                            ref_elements=0,
                            local_elements=None,
                            missing=0,
                            extra=0,
                            tag_mismatches=0,
                            ref_ms=ref_result.elapsed_ms,
                            local_ms=None,
                            message="no --local given",
                        )
                    )
                    continue

                local_result = fetch(
                    client,
                    args.local,
                    local_query_json,
                    timeout=args.timeout,
                    retries=args.retries,
                    sleep_between=0.0,
                    label="local",
                )

                if not local_result.ok:
                    msg = local_result.remark or local_result.error or f"HTTP {local_result.status_code}"
                    reports.append(
                        QueryRunReport(
                            query_file=qfile.name,
                            bbox_name=bbox_name,
                            status="FAIL",
                            ref_elements=0,
                            local_elements=None,
                            missing=0,
                            extra=0,
                            tag_mismatches=0,
                            ref_ms=ref_result.elapsed_ms,
                            local_ms=local_result.elapsed_ms,
                            message=f"local error: {msg}",
                        )
                    )
                    continue

                ref_elements = parse_elements(ref_result.body, ref_result.content_type)
                local_elements = parse_elements(local_result.body, local_result.content_type)
                cmp = compare(ref_elements, local_elements)

                message = ""
                if cmp.status == "FAIL":
                    parts = []
                    if cmp.missing:
                        parts.append(f"missing={len(cmp.missing)} e.g. {cmp.missing[:5]}")
                    if cmp.extra:
                        parts.append(f"extra={len(cmp.extra)} e.g. {cmp.extra[:5]}")
                    if cmp.tag_mismatches:
                        parts.append(f"tag_mismatches={len(cmp.tag_mismatches)}")
                    if cmp.other_mismatches:
                        parts.append(f"other={cmp.other_mismatches[:3]}")
                    if cmp.count_mismatch:
                        parts.append(f"count ref={cmp.count_mismatch['ref']} local={cmp.count_mismatch['local']}")
                    message = "; ".join(parts)

                reports.append(
                    QueryRunReport(
                        query_file=qfile.name,
                        bbox_name=bbox_name,
                        status=cmp.status,
                        ref_elements=cmp.ref_count,
                        local_elements=cmp.local_count,
                        missing=len(cmp.missing),
                        extra=len(cmp.extra),
                        tag_mismatches=len(cmp.tag_mismatches),
                        ref_ms=ref_result.elapsed_ms,
                        local_ms=local_result.elapsed_ms,
                        message=message,
                        detail=cmp.to_dict(),
                    )
                )

                # Also check the XML variant parses as XML when this corpus
                # entry declared [out:xml] originally.
                if is_xml_variant(raw_query) and args.local is not None and not args.reference_only:
                    xml_query = substitute_bbox(raw_query, bbox)
                    xml_result = fetch(
                        client,
                        args.local,
                        xml_query,
                        timeout=args.timeout,
                        retries=args.retries,
                        sleep_between=0.0,
                        label="local-xml",
                    )
                    xml_ok = xml_result.ok
                    if xml_ok:
                        try:
                            ET.fromstring(xml_result.body)
                        except ET.ParseError as exc:
                            xml_ok = False
                            xml_result.error = f"XML did not parse: {exc}"
                    reports.append(
                        QueryRunReport(
                            query_file=qfile.name + " [xml-parse-check]",
                            bbox_name=bbox_name,
                            status="PASS" if xml_ok else "FAIL",
                            ref_elements=0,
                            local_elements=0,
                            missing=0,
                            extra=0,
                            tag_mismatches=0,
                            ref_ms=0.0,
                            local_ms=xml_result.elapsed_ms,
                            message="" if xml_ok else (xml_result.error or xml_result.remark or "xml parse failed"),
                        )
                    )

    print_table(reports)

    if args.json:
        write_json_report(Path(args.json), reports)

    return 0 if all(r.status == "PASS" for r in reports) else 1


def print_table(reports: list[QueryRunReport]) -> None:
    headers = [
        "query",
        "bbox",
        "ref_els",
        "local_els",
        "missing",
        "extra",
        "tag_mm",
        "ref_ms",
        "local_ms",
        "status",
    ]
    rows = []
    for r in reports:
        rows.append(
            [
                r.query_file,
                r.bbox_name,
                str(r.ref_elements),
                "" if r.local_elements is None else str(r.local_elements),
                str(r.missing),
                str(r.extra),
                str(r.tag_mismatches),
                f"{r.ref_ms:.0f}",
                "" if r.local_ms is None else f"{r.local_ms:.0f}",
                r.status,
            ]
        )
    widths = [max(len(h), *(len(row[i]) for row in rows)) if rows else len(h) for i, h in enumerate(headers)]

    def fmt_row(cells: list[str]) -> str:
        return "  ".join(c.ljust(w) for c, w in zip(cells, widths))

    print(fmt_row(headers))
    print(fmt_row(["-" * w for w in widths]))
    for r, row in zip(reports, rows):
        print(fmt_row(row))
        if r.message:
            print(f"    {r.message}")

    total = len(reports)
    passed = sum(1 for r in reports if r.status == "PASS")
    print()
    print(f"{passed}/{total} PASS")


def write_json_report(path: Path, reports: list[QueryRunReport]) -> None:
    payload = {
        "summary": {
            "total": len(reports),
            "passed": sum(1 for r in reports if r.status == "PASS"),
            "failed": sum(1 for r in reports if r.status == "FAIL"),
        },
        "results": [
            {
                "query_file": r.query_file,
                "bbox": r.bbox_name,
                "status": r.status,
                "ref_elements": r.ref_elements,
                "local_elements": r.local_elements,
                "missing": r.missing,
                "extra": r.extra,
                "tag_mismatches": r.tag_mismatches,
                "ref_ms": r.ref_ms,
                "local_ms": r.local_ms,
                "message": r.message,
                "detail": r.detail,
            }
            for r in reports
        ],
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--reference", help="Reference Overpass endpoint URL", default=None)
    p.add_argument("--local", help="Local Overpass-compatible endpoint URL", default=None)
    p.add_argument("--corpus", default=DEFAULT_CORPUS, help="Directory of *.overpassql files")
    p.add_argument("--bboxes", default=DEFAULT_BBOXES, help="Path to bboxes.json")
    p.add_argument("--date", default=None, help='Value for [date:"..."], reference side only')
    p.add_argument("--only", default=None, help="Glob to filter corpus file names")
    p.add_argument("--bbox-name", default=None, help="Run every query against just this bbox")
    p.add_argument("--json", default=None, help="Write the full report to this JSON file")
    p.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds")
    p.add_argument("--sleep", type=float, default=2.0, help="Seconds to sleep between reference calls")
    p.add_argument("--retries", type=int, default=5, help="Retries on 429/504/timeout")
    p.add_argument(
        "--reference-only",
        action="store_true",
        help="Only fetch+cache reference responses under tests/corpus/.cache; do not call --local",
    )
    p.add_argument(
        "--local-only",
        action="store_true",
        help="Only call --local, comparing against previously cached reference responses",
    )
    args = p.parse_args(argv)

    if args.local_only and args.reference_only:
        p.error("--reference-only and --local-only are mutually exclusive")
    if not args.local_only and not args.reference:
        p.error("--reference is required unless --local-only is given")
    if not args.reference_only and not args.local:
        p.error("--local is required unless --reference-only is given")
    return args


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
