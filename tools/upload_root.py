#!/usr/bin/env python3
"""Upload a dataset root to an S3-compatible bucket (Cloudflare R2), the
manifest last so a reader never sees a manifest before its files.

    python tools/upload_root.py <local-root> s3://<bucket>/<prefix> [--workers 16]

Credentials and endpoint come from the `OSMPQ_S3_*` variables the engine
and updater use (`src/osmpq/store.py`): `OSMPQ_S3_KEY_ID`,
`OSMPQ_S3_SECRET`, `OSMPQ_S3_ENDPOINT` (host only). Files already present
with the same size are skipped, so re-running after an interruption
finishes the job. Only the files the latest manifest references are
uploaded (run `osmpq gc` first to drop old generations).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.config import Config


def _client():
    endpoint = os.environ["OSMPQ_S3_ENDPOINT"]
    if "://" not in endpoint:
        endpoint = "https://" + endpoint
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["OSMPQ_S3_KEY_ID"],
        aws_secret_access_key=os.environ["OSMPQ_S3_SECRET"],
        region_name=os.environ.get("OSMPQ_S3_REGION", "auto"),
        config=Config(max_pool_connections=64, retries={"max_attempts": 8, "mode": "adaptive"}),
    )


def _referenced_files(root: Path) -> list[str]:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from osmpq.build.gc import _referenced_paths

    latest = (root / "manifest" / "LATEST").read_text().strip()
    man = json.loads((root / "manifest" / f"{latest}.json").read_text())
    files = sorted(_referenced_paths(man))
    return files + [f"manifest/{latest}.json", "manifest/LATEST"]


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("root")
    p.add_argument("dest", help="s3://bucket/prefix")
    p.add_argument("--workers", type=int, default=16)
    args = p.parse_args(argv)
    root = Path(args.root)
    assert args.dest.startswith("s3://")
    bucket, _, prefix = args.dest[5:].partition("/")
    prefix = prefix.strip("/")

    files = _referenced_files(root)
    manifest_files = [f for f in files if f.startswith("manifest/")]
    data_files = [f for f in files if not f.startswith("manifest/")]
    client = _client()

    existing: dict[str, int] = {}
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix + "/" if prefix else ""}
        if token:
            kw["ContinuationToken"] = token
        r = client.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            existing[o["Key"]] = o["Size"]
        token = r.get("NextContinuationToken")
        if not token:
            break

    def key_for(rel: str) -> str:
        return f"{prefix}/{rel}" if prefix else rel

    todo = [f for f in data_files if existing.get(key_for(f)) != (root / f).stat().st_size]
    total_bytes = sum((root / f).stat().st_size for f in todo)
    print(f"{len(data_files)} data files referenced, {len(todo)} to upload ({total_bytes/1e9:.2f} GB), {len(existing)} keys already present")
    t0 = time.time()
    done_bytes = 0

    def upload(rel: str) -> int:
        path = root / rel
        client.upload_file(str(path), bucket, key_for(rel))
        return path.stat().st_size

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(upload, f): f for f in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            done_bytes += fut.result()
            if i % 200 == 0 or i == len(todo):
                el = time.time() - t0
                print(f"  {i}/{len(todo)} files, {done_bytes/1e9:.2f} GB, {el:.0f}s, {done_bytes/1e6/max(el,1e-9):.1f} MB/s", flush=True)
    for f in manifest_files:  # numbered manifest first, LATEST last
        upload(f)
    print(f"done in {time.time()-t0:.0f}s; root is {args.dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
