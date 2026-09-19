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
            stats = {
                "seconds": elapsed,
                "files_read": ctx.files_read,
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
