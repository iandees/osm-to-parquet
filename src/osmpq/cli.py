"""``osmpq`` command line interface.

See docs/m0-contracts.md section 5 and docs/m1-contracts.md section 7 for
each subcommand's exact flags.
"""
from __future__ import annotations

import argparse
import sys

from osmpq.build.builder import BuildFromRawOptions, BuildOptions, build, build_from_raw
from osmpq.build.raw import RawBuildOptions, raw_build
from osmpq.layout import cells as cells_mod
from osmpq.layout import manifest as manifest_mod
from osmpq.update.updater import UpdateOptions
from osmpq.update.updater import run as update_run


def _parse_bbox(s: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("--bbox needs 4 comma-separated numbers: S,W,N,E")
    south, west, north, east = (float(p) for p in parts)
    return south, west, north, east


def _cmd_raw_py(args: argparse.Namespace) -> int:
    promoted_keys = None
    if args.promoted_keys:
        promoted_keys = [k.strip() for k in args.promoted_keys.split(",") if k.strip()]
    opts = RawBuildOptions(
        pbf_path=args.input,
        rawdir=args.rawdir,
        bbox=args.bbox,
        max_nodes_per_cell=args.max_nodes_per_cell,
        max_depth=args.max_depth,
        promoted_keys=promoted_keys,
        threads=args.threads,
        memory_limit=args.memory_limit,
        tmpdir=args.tmpdir,
    )
    raw_build(opts)
    return 0


def _copy_mode(args: argparse.Namespace) -> str:
    if args.copy:
        return "copy"
    if args.move:
        return "move"
    return "link"


def _cmd_build(args: argparse.Namespace) -> int:
    if args.raw:
        opts = BuildFromRawOptions(
            rawdir=args.input,
            root=args.root,
            generation=args.generation,
            timestamp=args.timestamp,
            replication_sequence=args.replication_sequence,
            threads=args.threads,
            memory_limit=args.memory_limit,
            tmpdir=args.tmpdir,
            mode=_copy_mode(args),
            run_areas=not args.no_areas,
            extent=args.extent,
        )
        build_from_raw(opts)
        return 0

    promoted_keys = None
    if args.promoted_keys:
        promoted_keys = [k.strip() for k in args.promoted_keys.split(",") if k.strip()]
    opts = BuildOptions(
        pbf_path=args.input,
        root=args.root,
        bbox=args.bbox,
        generation=args.generation,
        max_nodes_per_cell=args.max_nodes_per_cell,
        max_depth=args.max_depth,
        promoted_keys=promoted_keys,
        timestamp=args.timestamp,
        replication_sequence=args.replication_sequence,
        threads=args.threads,
        memory_limit=args.memory_limit,
        tmpdir=args.tmpdir,
        mode=_copy_mode(args),
        run_areas=not args.no_areas,
    )
    build(opts)
    return 0


def _cmd_areas(args: argparse.Namespace) -> int:
    from osmpq.build.areas import BuildAreasOptions, build_areas

    build_areas(BuildAreasOptions(
        root=args.root, threads=args.threads, memory_limit=args.memory_limit, tmpdir=args.tmpdir,
    ))
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
    if man.manifest_version >= 2:
        print(f"ancestor_depths: {man.ancestor_depths}  max_depth: {man.max_depth}")
        print(f"producer: {man.producer}")
        print(f"stats: {man.stats}")
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
    if man.manifest_version >= 2 and man.rowgroup_index:
        for table_name, path in man.rowgroup_index.items():
            print(f"rowgroup_index {table_name}: {path}")
    areas = man.areas
    if areas and areas.get("index"):
        idx = areas["index"]
        n_cells = len(areas.get("cells", {}))
        print(f"areas: {idx.get('rows', 0)} rows, {n_cells} cells, {idx.get('bytes', 0)} index bytes")
    if man.history:
        h = man.history
        print(f"history: generation={h.get('generation')} since={h.get('since')} minor_versions={h.get('minor_versions')}")
        print(f"  stats: {h.get('stats')}")
        if h.get("tiers"):
            print(f"  tiers: {sorted(h['tiers'].keys())}")
    return 0


def _cmd_update(args: argparse.Namespace) -> int:
    opts = UpdateOptions(
        root=args.root,
        source=args.source,
        max_diffs=args.max_diffs,
        tmpdir=args.tmpdir,
        threads=args.threads,
        memory_limit=args.memory_limit,
        follow=args.follow,
        poll_interval=args.poll_interval,
    )
    update_run(opts)
    return 0


def _cmd_history_init(args: argparse.Namespace) -> int:
    from osmpq.history.init import HistoryInitOptions, history_init

    summary = history_init(HistoryInitOptions(
        root=args.root, threads=args.threads, memory_limit=args.memory_limit, tmpdir=args.tmpdir,
    ))
    print(f"osmpq history init: {args.root}")
    print(f"  since={summary['since']} rows={summary['rows']} bytes={summary['bytes']} seconds={summary['seconds']:.1f}")
    return 0


def _cmd_history_build(args: argparse.Namespace) -> int:
    from osmpq.history.build import HistoryBuildOptions, history_build

    if args.osh and (args.pbf or args.osc):
        raise SystemExit("osmpq history build: --osh is mutually exclusive with --pbf/--osc")
    if not args.osh and not args.pbf:
        raise SystemExit("osmpq history build: give --pbf (+ optional --osc) or --osh")

    opts = HistoryBuildOptions(
        root=args.root,
        pbf_path=args.pbf,
        osc_paths=args.osc,
        osh_path=args.osh,
        threads=args.threads,
        memory_limit=args.memory_limit,
        tmpdir=args.tmpdir,
    )
    summary = history_build(opts)
    print(f"osmpq history build: {args.root}")
    print(f"  since: {summary['since']}")
    print(f"  rows: {summary['stats']['rows']}  minor_rows: {summary['stats']['minor_rows']}  bytes: {summary['stats']['bytes']}")
    print(f"  {summary['seconds']:.1f}s total")
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    from osmpq.build.validate import validate

    ok, summary = validate(args.root)
    print("\n".join(summary))
    return 0 if ok else 1


def _cmd_serve(args: argparse.Namespace) -> int:
    """`osmpq serve` (docs/m3-contracts.md section 6.1): run `osmpq.server`
    (`/api/interpreter`, `/api/status`, `/api/kill_my_queries`,
    `/healthz`, ...) under uvicorn. The dataset root and every other
    behaviour knob come from the `OSMPQ_*` environment variables the app
    itself reads (see `osmpq.server`'s module docstring); this subcommand
    only owns the bind address, port and worker count."""
    import uvicorn

    uvicorn.run("osmpq.server:app", host=args.host, port=args.port, workers=args.workers)
    return 0


def _cmd_updater_server(args: argparse.Namespace) -> int:
    """`osmpq updater-server` (docs/m3-contracts.md section 6.3): run
    `osmpq.update.server` (`POST /run`, `GET /status`, `GET /healthz`)
    under uvicorn."""
    import uvicorn

    uvicorn.run("osmpq.update.server:app", host=args.host, port=args.port, workers=args.workers)
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if argv[:1] == ["compact"]:
        from osmpq.build.compact import compact_main
        return compact_main(argv[1:])
    if argv[:1] == ["gc"]:
        from osmpq.build.gc import gc_main
        return gc_main(argv[1:])

    parser = argparse.ArgumentParser(prog="osmpq")
    sub = parser.add_subparsers(dest="command", required=True)

    p_raw = sub.add_parser("raw-py", help="produce raw/ from a PBF with DuckDB (no Rust binary needed)")
    p_raw.add_argument("input")
    p_raw.add_argument("rawdir")
    p_raw.add_argument("--bbox", type=_parse_bbox, default=None, help="S,W,N,E")
    p_raw.add_argument("--max-nodes-per-cell", type=int, default=1_000_000)
    p_raw.add_argument("--max-depth", type=int, default=cells_mod.DEFAULT_MAX_DEPTH_V2)
    p_raw.add_argument("--promoted-keys", default=None, help="comma-separated list")
    p_raw.add_argument("--threads", type=int, default=None)
    p_raw.add_argument("--memory-limit", default=None)
    p_raw.add_argument("--tmpdir", default=None)
    p_raw.set_defaults(func=_cmd_raw_py)

    p_build = sub.add_parser("build", help="build a dataset root from a PBF, or from raw/ with --raw")
    p_build.add_argument("--raw", action="store_true", help="input is a raw/ dir (from raw-py or osmpq-raw)")
    p_build.add_argument("input", help="PBF path, or a raw/ dir when --raw is given")
    p_build.add_argument("root")
    p_build.add_argument("--bbox", type=_parse_bbox, default=None, help="S,W,N,E (ignored with --raw)")
    p_build.add_argument("--extent", type=_parse_bbox, default=None,
                         help="S,W,N,E intended coverage recorded in the manifest (with --raw); "
                              "defaults to the data bbox, which is wider than the cut bbox for extracts")
    p_build.add_argument("--generation", default=None)
    p_build.add_argument("--max-nodes-per-cell", type=int, default=1_000_000, help="ignored with --raw")
    p_build.add_argument("--max-depth", type=int, default=cells_mod.DEFAULT_MAX_DEPTH_V2, help="ignored with --raw")
    p_build.add_argument("--promoted-keys", default=None, help="comma-separated list (ignored with --raw)")
    p_build.add_argument("--timestamp", default=None)
    p_build.add_argument("--replication-sequence", type=int, default=None)
    p_build.add_argument("--threads", type=int, default=None)
    p_build.add_argument("--memory-limit", default=None)
    p_build.add_argument("--tmpdir", default=None)
    mode_group = p_build.add_mutually_exclusive_group()
    mode_group.add_argument("--link", action="store_true", help="hardlink raw files into root (default)")
    mode_group.add_argument("--copy", action="store_true", help="copy raw files into root")
    mode_group.add_argument("--move", action="store_true", help="move raw files into root")
    p_build.add_argument("--no-areas", action="store_true",
                          help="skip running `osmpq areas` at the end (docs/m3-contracts.md section 4.3)")
    p_build.set_defaults(func=_cmd_build)

    p_areas = sub.add_parser("areas", help="derive the area table for the current generation (docs/m3-contracts.md section 4)")
    p_areas.add_argument("root")
    p_areas.add_argument("--threads", type=int, default=None)
    p_areas.add_argument("--memory-limit", default=None)
    p_areas.add_argument("--tmpdir", default=None)
    p_areas.set_defaults(func=_cmd_areas)

    p_manifest = sub.add_parser("manifest", help="print a summary of a dataset root's manifest")
    p_manifest.add_argument("root")
    p_manifest.set_defaults(func=_cmd_manifest)

    p_validate = sub.add_parser("validate", help="check a dataset root against the manifest contract")
    p_validate.add_argument("root")
    p_validate.set_defaults(func=_cmd_validate)

    p_update = sub.add_parser("update", help="apply minutely replication diffs (docs/m2-contracts.md section 5)")
    p_update.add_argument("root")
    p_update.add_argument("--source", default=None, help="replication source URL (defaults to the manifest's)")
    once_group = p_update.add_mutually_exclusive_group()
    once_group.add_argument("--once", action="store_true", help="apply one batch and exit (default)")
    once_group.add_argument("--follow", action="store_true", help="poll the source and run repeatedly")
    p_update.add_argument("--max-diffs", type=int, default=60)
    p_update.add_argument("--tmpdir", default=None)
    p_update.add_argument("--threads", type=int, default=None)
    p_update.add_argument("--memory-limit", default=None)
    p_update.add_argument("--poll-interval", type=float, default=30.0, help="seconds between --follow polls")
    p_update.set_defaults(func=_cmd_update)

    p_history = sub.add_parser("history", help="history (attic) dataset commands (docs/m4-contracts.md section 4)")
    history_sub = p_history.add_subparsers(dest="history_command", required=True)
    p_history_build = history_sub.add_parser(
        "build", help="add history/<gen>/ and a v5 manifest to an existing dataset root"
    )
    p_history_build.add_argument("root")
    p_history_build.add_argument("--pbf", default=None, help="base extract PBF (the first state of every element)")
    p_history_build.add_argument(
        "--osc", action="append", default=None,
        help="a .osc/.osc.gz file or a directory of them, sorted by sequence number; may be given more than once",
    )
    p_history_build.add_argument("--osh", default=None, help="a full-history .osh.pbf file (mutually exclusive with --pbf/--osc)")
    p_history_build.add_argument("--threads", type=int, default=None)
    p_history_build.add_argument("--memory-limit", default=None)
    p_history_build.add_argument("--tmpdir", default=None)
    p_history_build.set_defaults(func=_cmd_history_build)
    p_history_init = history_sub.add_parser(
        "init", help="start history from the root's current tables (every current row as its first state); "
        "then `osmpq update` appends later versions"
    )
    p_history_init.add_argument("root")
    p_history_init.add_argument("--threads", type=int, default=None)
    p_history_init.add_argument("--memory-limit", default=None)
    p_history_init.add_argument("--tmpdir", default=None)
    p_history_init.set_defaults(func=_cmd_history_init)

    p_serve = sub.add_parser("serve", help="run the query API server (docs/m3-contracts.md section 6.1)")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8080)
    p_serve.add_argument("--workers", type=int, default=1)
    p_serve.set_defaults(func=_cmd_serve)

    p_updater_server = sub.add_parser(
        "updater-server", help="run the updater's control API (docs/m3-contracts.md section 6.3)"
    )
    p_updater_server.add_argument("--host", default="0.0.0.0")
    p_updater_server.add_argument("--port", type=int, default=8081)
    p_updater_server.add_argument("--workers", type=int, default=1)
    p_updater_server.set_defaults(func=_cmd_updater_server)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
