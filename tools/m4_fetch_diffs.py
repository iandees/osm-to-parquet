#!/usr/bin/env python3
"""Fetch osm.fr replication diffs for the Minnesota M4 history dataset.

Downloads consecutive minute diffs ``[--from-seq, latest]`` from an
osmosis-style replication source into ``--dest``, skipping any
``.osc.gz``/``.state.txt`` pair already present there, using
``osmpq.update.replication.ReplicationClient`` (retries with backoff --
docs/m2-contracts.md section 1). Used by ``tools/m4_dataset.sh``'s
``fetch`` stage (docs/m4-contracts.md section 7); can also be run
standalone to keep the cache topped up to the source's current sequence.

Prints a progress line every ``--batch`` diffs and a final summary line
(``done: fetched=... skipped=... last_seq=... latest=... in ...s``) so a
caller can scrape the elapsed time for the report.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# This worktree's src/, not whatever `osmpq` an editable install elsewhere
# on sys.path resolves to -- keeps this script tied to the code actually
# reviewed in this worktree regardless of where it's invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from osmpq.update.replication import ReplicationClient  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", required=True, help="replication source base URL")
    p.add_argument("--from-seq", type=int, required=True, help="first sequence to fetch")
    p.add_argument("--dest", required=True, help="destination directory for .osc.gz/.state.txt pairs")
    p.add_argument("--batch", type=int, default=50, help="print progress every N diffs (default 50)")
    p.add_argument("--retries", type=int, default=8)
    p.add_argument("--backoff", type=float, default=2.0)
    args = p.parse_args(argv)

    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)

    with ReplicationClient(args.source, retries=args.retries, backoff=args.backoff) as client:
        latest = client.latest_sequence()
        print(f"source latest sequence: {latest}", flush=True)
        seq = args.from_seq
        fetched = 0
        skipped = 0
        t0 = time.monotonic()
        while seq <= latest:
            osc_path = dest / f"{seq}.osc.gz"
            state_path = dest / f"{seq}.state.txt"
            if osc_path.exists() and state_path.exists():
                skipped += 1
            else:
                r = client.fetch(seq, dest)
                if r is None:
                    print(f"sequence {seq} not yet available on the source; stopping", flush=True)
                    break
                fetched += 1
            done = fetched + skipped
            if done % args.batch == 0:
                elapsed = time.monotonic() - t0
                print(
                    f"... seq {seq}/{latest} (fetched={fetched} skipped={skipped} elapsed={elapsed:.0f}s)",
                    flush=True,
                )
            seq += 1
        elapsed = time.monotonic() - t0
        print(
            f"done: fetched={fetched} skipped={skipped} last_seq={seq - 1} latest={latest} in {elapsed:.1f}s",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
