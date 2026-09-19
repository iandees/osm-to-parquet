"""``osmpq`` command line interface: ``build`` and ``manifest``.

See docs/m0-contracts.md section 5 for the ``build`` command's exact flags.
"""
from __future__ import annotations

import argparse
import sys

from osmpq.build.builder import BuildOptions, build
from osmpq.layout import manifest as manifest_mod


def _parse_bbox(s: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("--bbox needs 4 comma-separated numbers: S,W,N,E")
    south, west, north, east = (float(p) for p in parts)
    return south, west, north, east


def _cmd_build(args: argparse.Namespace) -> int:
    promoted_keys = None
    if args.promoted_keys:
        promoted_keys = [k.strip() for k in args.promoted_keys.split(",") if k.strip()]
    opts = BuildOptions(
        pbf_path=args.input,
        root=args.root,
        bbox=args.bbox,
        generation=args.generation,
        max_nodes_per_cell=args.max_nodes_per_cell,
        promoted_keys=promoted_keys,
        timestamp=args.timestamp,
        replication_sequence=args.replication_sequence,
        threads=args.threads,
        memory_limit=args.memory_limit,
        tmpdir=args.tmpdir,
    )
    build(opts)
    return 0


def _cmd_manifest(args: argparse.Namespace) -> int:
    man = manifest_mod.load_latest(args.root)
    print(f"generation: {man.generation}")
    print(f"schema_version: {man.schema_version}  manifest_version: {man.manifest_version}")
    print(f"timestamp_osm_base: {man.timestamp_osm_base}")
    print(f"replication_sequence: {man.replication_sequence}")
    print(f"source: {man.source}")
    print(f"extent: {man.extent}")
    print(f"promoted_keys: {', '.join(man.promoted_keys)}")
    print(f"leaf_cells: {len(man.leaf_cells)}")
    for table_name, spec in man.tables.items():
        n_cells = len(spec.get("cells", {}))
        rows = 0
        byte_total = 0
        for entry in spec.get("cells", {}).values():
            if "rows" in entry:
                rows += entry["rows"]
                byte_total += entry["bytes"]
            else:
                for part in ("tagged", "untagged"):
                    if entry.get(part):
                        rows += entry[part]["rows"]
                        byte_total += entry[part]["bytes"]
        print(f"table {table_name}: {n_cells} cells, {rows} rows, {byte_total} bytes")
    for table_name, parts in man.byid.items():
        rows = sum(p["rows"] for p in parts)
        byte_total = sum(p["bytes"] for p in parts)
        print(f"byid {table_name}: {len(parts)} parts, {rows} rows, {byte_total} bytes")
    for table_name, parts in man.index.items():
        rows = sum(p["rows"] for p in parts)
        byte_total = sum(p["bytes"] for p in parts)
        print(f"index {table_name}: {len(parts)} parts, {rows} rows, {byte_total} bytes")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="osmpq")
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build", help="build a dataset root from a PBF")
    p_build.add_argument("input")
    p_build.add_argument("root")
    p_build.add_argument("--bbox", type=_parse_bbox, default=None, help="S,W,N,E")
    p_build.add_argument("--generation", default=None)
    p_build.add_argument("--max-nodes-per-cell", type=int, default=1_000_000)
    p_build.add_argument("--promoted-keys", default=None, help="comma-separated list")
    p_build.add_argument("--timestamp", default=None)
    p_build.add_argument("--replication-sequence", type=int, default=None)
    p_build.add_argument("--threads", type=int, default=None)
    p_build.add_argument("--memory-limit", default=None)
    p_build.add_argument("--tmpdir", default=None)
    p_build.set_defaults(func=_cmd_build)

    p_manifest = sub.add_parser("manifest", help="print a summary of a dataset root's manifest")
    p_manifest.add_argument("root")
    p_manifest.set_defaults(func=_cmd_manifest)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
