"""`Engine`: one DuckDB connection per `run()`, contract section 6."""
from __future__ import annotations

import threading
import time
from typing import Optional

import duckdb

from osmpq.errors import RuntimeQueryError
from osmpq.ql.ast import Program

from . import catalog, hilbert, planner
from .result import Result


class Engine:
    def __init__(self, root: str, duckdb_config: Optional[dict] = None):
        self.root = root
        self.duckdb_config = dict(duckdb_config or {})
        self.manifest = catalog.load_manifest(root)

    # -- public API -----------------------------------------------------

    def run(self, query_text: str, timeout: Optional[float] = None) -> Result:
        from osmpq.ql import parse  # imported lazily: the parser may not exist yet

        program = parse(query_text)
        return self.run_program(program, timeout=timeout)

    def run_program(self, program: Program, timeout: Optional[float] = None) -> Result:
        settings = program.settings
        effective_timeout = timeout if timeout is not None else (settings.timeout or None)

        con = duckdb.connect(":memory:", config=self.duckdb_config)
        timer: Optional[threading.Timer] = None
        # m1-contracts.md section 6: a per-run accumulator so
        # `catalog.prune_files_by_bbox` can report files-considered vs
        # files-read (Manifest instances are cached/reused across
        # `Engine.run()` calls) without threading an extra return value
        # through every SQL-builder call chain. Always reset, even on an
        # error/timeout path, so a later run never inherits stale counts.
        self.manifest._file_stats = catalog.FileStats()
        try:
            self._setup_connection(con, settings)
            hilbert.register_duckdb_udfs(con)

            if effective_timeout:
                def _kill() -> None:
                    try:
                        con.interrupt()
                    except Exception:
                        pass

                timer = threading.Timer(effective_timeout, _kill)
                timer.daemon = True
                timer.start()

            start = time.monotonic()
            try:
                ctx = planner.run_program(con, self.manifest, program)
            except duckdb.InterruptException:
                elapsed = time.monotonic() - start
                return Result(
                    elements=[],
                    settings=settings,
                    remark=(
                        f'runtime error: Query timed out in "osmpq" at line 1 '
                        f"after {effective_timeout} seconds."
                    ),
                    timestamp_osm_base=self.manifest.timestamp_osm_base,
                    stats={"seconds": elapsed, "timed_out": True},
                )
            except RuntimeQueryError as e:
                elapsed = time.monotonic() - start
                return Result(
                    elements=[],
                    settings=settings,
                    remark=str(e),
                    timestamp_osm_base=self.manifest.timestamp_osm_base,
                    stats={"seconds": elapsed},
                )
            finally:
                if timer is not None:
                    timer.cancel()

            elapsed = time.monotonic() - start
            file_stats = self.manifest._file_stats
            # `ctx.files_read` already reflects row-group pruning (the file
            # lists shrink in place, inside sources.py, before anything
            # counts them), so it doubles as "files_read" directly; the
            # only thing missing is the pre-prune candidate count for those
            # same (row-group-indexed) selections, which `file_stats`
            # tracked on the side.
            files_considered = ctx.files_read + max(0, file_stats.considered - file_stats.read)
            stats = {
                "seconds": elapsed,
                "files_read": ctx.files_read,
                "files_considered": files_considered,
                "elements": len(ctx.elements),
            }
            if ctx.warnings:
                stats["warnings"] = ctx.warnings
            try:
                prof = con.execute("PRAGMA last_profiling_output").fetchall()
                if prof:
                    stats["profiling_rows"] = len(prof)
            except Exception:
                pass

            return Result(
                elements=ctx.elements,
                settings=settings,
                remark=None,
                timestamp_osm_base=self.manifest.timestamp_osm_base,
                stats=stats,
            )
        finally:
            con.close()
            self.manifest._file_stats = None

    # -- internals --------------------------------------------------------

    def _setup_connection(self, con, settings) -> None:
        for ext in ("spatial", "httpfs"):
            try:
                con.execute(f"LOAD {ext};")
            except Exception:
                con.execute(f"INSTALL {ext}; LOAD {ext};")

        threads = self.duckdb_config.get("threads")
        if threads:
            con.execute(f"SET threads={int(threads)}")

        maxsize = getattr(settings, "maxsize", None)
        if maxsize:
            mb = max(1, int(maxsize) // (1024 * 1024))
            con.execute(f"SET memory_limit='{mb}MB'")
