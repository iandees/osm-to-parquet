"""Run corpus queries through the engine against an HTTP root and account for
range requests and bytes per query, using the request log of
scratchpad/range_server.py (one line per request: METHOD PATH bytes=a-b STATUS).

Usage: python tools/remote_profile.py --root http://127.0.0.1:8090 --log <range_server.log> \
           --corpus tests/corpus --bbox-name downtown_minneapolis [--repeat 2] [--json out.json]
"""
import argparse
import glob
import json
import os
import re
import time
from pathlib import Path

from osmpq.engine import Engine

RANGE = re.compile(r"bytes=(\d+)-(\d+)")


def log_stats(path: str, start: int) -> tuple[int, int, int, set]:
    with open(path) as f:
        lines = f.readlines()[start:]
    reqs, nbytes, files = 0, 0, set()
    for ln in lines:
        parts = ln.split()
        if len(parts) < 4 or parts[0] != "GET":
            continue
        reqs += 1
        files.add(parts[1])
        m = RANGE.search(ln)
        if m:
            nbytes += int(m.group(2)) - int(m.group(1)) + 1
    return reqs, nbytes, len(files), files


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--log", required=True)
    ap.add_argument("--corpus", default="tests/corpus")
    ap.add_argument("--bbox-name", default="downtown_minneapolis")
    ap.add_argument("--only", default="*")
    ap.add_argument("--repeat", type=int, default=2, help="runs per query; first is cold-ish, last is warm")
    ap.add_argument("--json", default=None)
    a = ap.parse_args()

    bboxes = json.load(open(Path(a.corpus) / "bboxes.json"))
    s, w, n, e = bboxes[a.bbox_name]
    bbox = f"{s},{w},{n},{e}"
    rows = []
    print(f"{'query':46} {'run':>3} {'elems':>7} {'sec':>6} {'reqs':>5} {'files':>5} {'MB':>7}")
    for qf in sorted(glob.glob(str(Path(a.corpus) / f"{a.only}.overpassql"))):
        text = open(qf).read().replace("{{bbox}}", bbox)
        for run in range(a.repeat):
            eng = Engine(root=a.root)  # fresh engine = fresh DuckDB, no in-process cache
            start = sum(1 for _ in open(a.log))
            t = time.time()
            try:
                res = eng.run(text)
                elems, err = len(res.elements), res.remark
            except Exception as ex:  # noqa: BLE001
                elems, err = -1, f"{type(ex).__name__}: {str(ex)[:80]}"
            dt = time.time() - t
            reqs, nbytes, nfiles, _ = log_stats(a.log, start)
            rows.append({"query": os.path.basename(qf), "run": run, "elements": elems, "seconds": dt,
                         "requests": reqs, "files": nfiles, "bytes": nbytes, "error": err})
            print(f"{os.path.basename(qf):46} {run:>3} {elems:>7} {dt:>6.2f} {reqs:>5} {nfiles:>5} {nbytes/1e6:>7.2f}" + (f"  {err}" if err else ""))
    if a.json:
        json.dump(rows, open(a.json, "w"), indent=1)


if __name__ == "__main__":
    main()
