"""`Engine`: one DuckDB database per Engine, one cursor per `run()`,
contract section 6.

**Concurrency.** `Engine.run()`/`run_program()` may be called concurrently
from multiple threads on the same `Engine` (FastAPI runs sync endpoints in
a thread pool, so two `/api/interpreter` requests can interleave). Each
call opens its own cursor via `self._db.cursor()`: a cursor is a separate
connection sharing the same database, and temp tables are private to a
connection, so two concurrent runs' `set_<name>` temp tables (the planner's
OverpassQL sets) never collide even though both runs pick names from the
same counter space. (`SET` is *not* uniformly private, though: most
DuckDB settings are GLOBAL-scope, not per-connection -- `memory_limit` and
`threads` among them -- so the per-run `[maxsize:N]` -> `SET
memory_limit=...` in `_setup_connection` below still mutates the whole
database, same as any other concurrent run's. That is an existing,
unfixed sharp edge of sharing one database across concurrent runs with
different `[maxsize:...]`; it doesn't affect correctness of results, only
how precisely a given run's own memory cap is honored while another run's
`[maxsize:...]` is also changing it.) A run's timeout timer calls
`.interrupt()` on that same cursor, not on `self._db` or any other run's
cursor. The one piece of state that used to live on the shared `Manifest`
(`catalog.prune_files_by_bbox`'s considered/read file counts, via
`Manifest._file_stats`) is now kept in `catalog.FILE_STATS`, a
`contextvars.ContextVar`: `run_program` sets it for the duration of one
run and resets it in `finally`, so each run only ever sees its own count.

**One database, reused across runs.** Opening a fresh in-memory
`duckdb.connect()` and `LOAD`ing `spatial`/`httpfs` per `run()` cost
~0.25-0.3s and, worse, threw away DuckDB's Parquet metadata/object cache
between queries -- every run re-fetched every Parquet footer even for a
root it had just queried. `Engine.__init__` now opens `self._db` once,
loads extensions and turns on `enable_object_cache`/
`enable_http_metadata_cache` there, and every `run()` reuses it through a
fresh cursor. For a remote (http(s)://, s3://) root this means the 2nd+
query against a file this `Engine` has already touched skips re-fetching
its footer, at the cost of holding that cached metadata (and the page
cache DuckDB keeps under `enable_object_cache`) in memory for the
`Engine`'s lifetime -- a caveat for a long-lived server process against a
large or ever-changing dataset.

**S3 / object-store credentials.** When `root` starts with `s3://`,
`Engine.__init__` creates a DuckDB secret from environment variables, if
they are set:

- `OSMPQ_S3_KEY_ID`, `OSMPQ_S3_SECRET`: access key id / secret access key.
- `OSMPQ_S3_ENDPOINT`: host only (no scheme), e.g.
  `<account>.r2.cloudflarestorage.com`.
- `OSMPQ_S3_REGION`: default `"auto"`.
- `OSMPQ_S3_URL_STYLE`: default `"path"`.
- `OSMPQ_S3_USE_SSL`: default `"true"` (anything else, case-insensitively
  `"0"`/`"false"`/`"no"`, disables SSL).

If `OSMPQ_S3_KEY_ID`/`OSMPQ_S3_SECRET`/`OSMPQ_S3_ENDPOINT` aren't all set,
`Engine` does nothing and DuckDB's normal credential chain applies (e.g.
ambient AWS environment variables/instance role). The secret is created
on `self._db`, so it applies to every cursor -- including manifest and
row-group-index loads (`catalog.load_manifest`/`_load_rowgroup_index`),
which take `self._db` rather than opening their own throwaway connection
for a remote root, precisely so the secret reaches them too.
"""
from __future__ import annotations

import os
import threading
import time
from typing import Optional

import duckdb

from osmpq import store as store_mod
from osmpq.errors import RuntimeQueryError
from osmpq.ql.ast import Program

from . import attic, catalog, hilbert, planner
from .result import Result


def _s3_secret_sql(env: Optional[dict] = None) -> Optional[str]:
    """The `CREATE SECRET` SQL `Engine.__init__` issues for an `s3://`
    root, built from environment variables (see module docstring), or
    None when the required ones (`OSMPQ_S3_KEY_ID`/`OSMPQ_S3_SECRET`/
    `OSMPQ_S3_ENDPOINT`) aren't all set -- meaning "do nothing, let
    DuckDB's own credential chain apply".

    A pure function (takes `env` instead of reading `os.environ`
    directly) so tests can assert on the exact SQL without a real bucket
    or DuckDB connection, and without mutating process environment.

    Factored into `osmpq.store.s3_secret_sql` (docs/m3-contracts.md
    section 6.2) so `osmpq.update.updater` can issue the identical secret
    for its own DuckDB connection when reading an `s3://` root's Parquet
    files through `Store.url(...)`; kept here, delegating, for backward
    compatibility (existing callers/tests use `executor._s3_secret_sql`)."""
    return store_mod.s3_secret_sql(env)


class CancelToken:
    """Handed to `Engine.run`/`Engine.run_program`; `cancel()` interrupts
    that run's DuckDB cursor (docs/m3-contracts.md section 6.1,
    `/api/kill_my_queries`). Thread-safe: `cancel()` is typically called
    from a different thread (another request's handler) than the one
    running the query, so binding a cursor and cancelling are both guarded
    by the same lock -- a `cancel()` that arrives before the run has bound
    its cursor yet is remembered and applied as soon as it does."""

    def __init__(self) -> None:
        self._con = None
        self._cancelled = False
        self._lock = threading.Lock()

    def _bind(self, con) -> None:
        with self._lock:
            self._con = con
            if self._cancelled:
                self._interrupt_locked()

    def _unbind(self) -> None:
        with self._lock:
            self._con = None

    def _interrupt_locked(self) -> None:
        try:
            self._con.interrupt()
        except Exception:
            pass

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            if self._con is not None:
                self._interrupt_locked()

    def is_cancelled(self) -> bool:
        with self._lock:
            return self._cancelled


class Engine:
    def __init__(
        self,
        root: str,
        duckdb_config: Optional[dict] = None,
        manifest_refresh_seconds: Optional[float] = None,
    ):
        self.root = root
        self.duckdb_config = dict(duckdb_config or {})
        self._db = duckdb.connect(":memory:", config=self.duckdb_config)
        self._setup_database()
        # For an s3:// root, pass `self._db` through so the manifest and
        # (lazily, later) the row-group index load via a cursor of it,
        # rather than a throwaway connection with no secret.
        self.manifest = catalog.load_manifest(root, con=self._db)

        # -- manifest refresh (docs/m3-contracts.md section 6.1) --------
        # `manifest_refresh_seconds` defaults from `OSMPQ_MANIFEST_REFRESH_
        # SECONDS` (60) rather than being read once at import time, so a
        # test that sets the env var before constructing an `Engine` sees
        # it, same spirit as `server.get_engine()` reading `OSMPQ_ROOT`
        # fresh. `_manifest_refresh_lock` serializes the "is a refresh due,
        # and if so reload" check across concurrent `run_program` calls;
        # `_manifest_number` is `manifest/LATEST`'s raw text, compared
        # cheaply on every check without re-parsing the full manifest JSON
        # unless it actually changed.
        if manifest_refresh_seconds is None:
            manifest_refresh_seconds = float(os.environ.get("OSMPQ_MANIFEST_REFRESH_SECONDS", "60"))
        self._manifest_refresh_seconds = manifest_refresh_seconds
        self._manifest_refresh_lock = threading.Lock()
        self._last_manifest_check = time.monotonic()
        try:
            latest_path = catalog.join_root(self.root, "manifest/LATEST")
            self._manifest_number: Optional[str] = catalog.read_text_any(latest_path, con=self._db).strip()
        except Exception:
            self._manifest_number = None

    @property
    def manifest_number(self):
        """`manifest/LATEST`'s raw contents (usually an integer string) as
        of the last refresh check -- used for `/healthz` and the
        `X-OSMPQ-Manifest` response header."""
        n = self._manifest_number
        try:
            return int(n)
        except (TypeError, ValueError):
            return n

    def refresh_manifest_if_due(self) -> None:
        """Contract section 6.1: re-read `manifest/LATEST` at most every
        `manifest_refresh_seconds` (checked here, called on the next
        request rather than from a background thread); when the number
        changed, reload the manifest (a fresh `catalog.Manifest` carries
        its own empty `_rowgroup_cache`, so this is also "drop the
        row-group index cache") and atomically swap `self.manifest` --
        Python attribute assignment is atomic under the GIL, and
        `run_program` captures `self.manifest` into a local right after
        calling this, so a run already in flight keeps the manifest it
        started with even if a refresh swaps `self.manifest` out from
        under it midway through."""
        now = time.monotonic()
        with self._manifest_refresh_lock:
            if now - self._last_manifest_check < self._manifest_refresh_seconds:
                return
            self._last_manifest_check = now
            try:
                latest_path = catalog.join_root(self.root, "manifest/LATEST")
                latest_text = catalog.read_text_any(latest_path, con=self._db).strip()
            except Exception:
                return
            if latest_text == self._manifest_number:
                return
            try:
                new_manifest = catalog.load_manifest(self.root, con=self._db)
            except Exception:
                return
            self._manifest_number = latest_text
            self.manifest = new_manifest

    # -- public API -----------------------------------------------------

    def run(
        self,
        query_text: str,
        timeout: Optional[float] = None,
        cancel: Optional[CancelToken] = None,
    ) -> Result:
        from osmpq.ql import parse  # imported lazily: the parser may not exist yet

        program = parse(query_text)
        return self.run_program(program, timeout=timeout, cancel=cancel)

    def run_program(
        self,
        program: Program,
        timeout: Optional[float] = None,
        cancel: Optional[CancelToken] = None,
    ) -> Result:
        self.refresh_manifest_if_due()
        # Captured once: see `refresh_manifest_if_due`'s docstring -- this
        # run always sees the manifest it started with, even if another
        # thread's request swaps `self.manifest` to a newer one while this
        # run is still in flight.
        manifest = self.manifest
        settings = program.settings
        effective_timeout = timeout if timeout is not None else (settings.timeout or None)

        # A cursor: a separate connection sharing `self._db`'s database,
        # catalog (including the S3 secret and loaded extensions) and
        # Parquet metadata/object cache, but with its own temp tables and
        # `SET` state -- so concurrent `run()` calls on this Engine never
        # collide, per the module docstring.
        con = self._db.cursor()
        if cancel is not None:
            cancel._bind(con)
        timer: Optional[threading.Timer] = None
        timed_out_flag = [False]
        # m1-contracts.md section 6: a per-run accumulator so
        # `catalog.prune_files_by_bbox` can report files-considered vs
        # files-read. Lives in a ContextVar (`catalog.FILE_STATS`) rather
        # than on `self.manifest`, which is shared and reused across every
        # `run()` -- including concurrent ones -- so it can't safely hold
        # per-run state itself. Always reset, even on an error/timeout
        # path, so a later run (on this thread or another) never inherits
        # stale counts.
        file_stats = catalog.FileStats()
        stats_token = catalog.FILE_STATS.set(file_stats)
        delta_stats = catalog.DeltaStats()
        delta_stats_token = catalog.DELTA_STATS.set(delta_stats)
        try:
            self._setup_connection(con, settings)

            if effective_timeout:
                def _kill() -> None:
                    timed_out_flag[0] = True
                    try:
                        con.interrupt()
                    except Exception:
                        pass

                timer = threading.Timer(effective_timeout, _kill)
                timer.daemon = True
                timer.start()

            start = time.monotonic()
            is_diff = settings.diff is not None or settings.adiff is not None
            try:
                if is_diff:
                    # docs/m4-contracts.md section 3.2: two passes of the
                    # whole program (`SNAPSHOT = a` then `= b`), turned
                    # into an action list -- `ctx` below stands in for the
                    # normal single-pass `Context` so the stats/error
                    # handling beneath is unchanged either way.
                    planner.check_settings(manifest, settings)
                    diff_result = attic.run_diff_program(con, manifest, program)
                    ctx = diff_result
                else:
                    # Unchanged from before M4: `planner.run_program` is
                    # still the one call site for a normal (non-diff) run
                    # -- it calls `check_settings` itself and then
                    # `run_program_body`, exactly as pre-M4.
                    ctx = planner.run_program(con, manifest, program)
            except duckdb.InterruptException:
                elapsed = time.monotonic() - start
                if timed_out_flag[0]:
                    remark = (
                        f'runtime error: Query timed out in "osmpq" at line 1 '
                        f"after {effective_timeout} seconds."
                    )
                    stats = {"seconds": elapsed, "timed_out": True}
                else:
                    # Not the timeout timer: a `CancelToken.cancel()` from
                    # `/api/kill_my_queries` interrupted this run's cursor
                    # instead (contract section 6.1).
                    remark = (
                        f'runtime error: Query aborted in "osmpq" at line 1 '
                        f"after {elapsed:.1f} seconds (killed by kill_my_queries)."
                    )
                    stats = {"seconds": elapsed, "timed_out": False, "cancelled": True}
                return Result(
                    elements=[],
                    settings=settings,
                    remark=remark,
                    timestamp_osm_base=manifest.timestamp_osm_base,
                    stats=stats,
                )
            except RuntimeQueryError as e:
                elapsed = time.monotonic() - start
                return Result(
                    elements=[],
                    settings=settings,
                    remark=str(e),
                    timestamp_osm_base=manifest.timestamp_osm_base,
                    stats={"seconds": elapsed},
                )
            finally:
                if timer is not None:
                    timer.cancel()

            elapsed = time.monotonic() - start
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
                # docs/m2-contracts.md section 4: 0/0 for a manifest with no
                # delta tiers (or a query that touched none), always present
                # so callers don't need to branch on manifest_version.
                "delta_rows": delta_stats.delta_rows,
                "shadowed": delta_stats.shadowed,
            }
            if ctx.warnings:
                stats["warnings"] = ctx.warnings
            try:
                prof = con.execute("PRAGMA last_profiling_output").fetchall()
                if prof:
                    stats["profiling_rows"] = len(prof)
            except Exception:
                pass

            # docs/m4-contracts.md 3.1: a date before `history.since` is
            # answered from the earliest known state and says so in the
            # OSM `remark`, not only in the stats warnings.
            attic_remark = next((w for w in ctx.warnings if w.startswith("history starts at ")), None)
            return Result(
                elements=ctx.elements,
                settings=settings,
                remark=attic_remark,
                timestamp_osm_base=manifest.timestamp_osm_base,
                stats=stats,
                is_diff=is_diff,
            )
        finally:
            con.close()
            if cancel is not None:
                cancel._unbind()
            catalog.FILE_STATS.reset(stats_token)
            catalog.DELTA_STATS.reset(delta_stats_token)

    # -- internals --------------------------------------------------------

    def _setup_database(self) -> None:
        """One-time, at Engine construction: extensions, caches, and
        credentials that every cursor of `self._db` shares."""
        for ext in ("spatial", "httpfs"):
            try:
                self._db.execute(f"LOAD {ext};")
            except Exception:
                self._db.execute(f"INSTALL {ext}; LOAD {ext};")

        # Python UDFs (`create_function`) are registered in the database's
        # function catalog, not per-connection: doing this once here,
        # rather than per `run()` on each fresh cursor, is not just an
        # optimization -- calling `create_function` again for a name that
        # already exists raises `CatalogException`, so every cursor after
        # the first would fail outright if this stayed in `run_program`.
        hilbert.register_duckdb_udfs(self._db)

        # Keep DuckDB's Parquet metadata (footers) and page/object cache
        # around across `run()` calls instead of paying for them again on
        # every query -- see the "One database, reused across runs" note
        # in this module's docstring.
        self._db.execute("SET enable_object_cache=true")
        self._db.execute("SET enable_http_metadata_cache=true")
        # An in-memory DuckDB spills to `<cwd>/.tmp` by default, which two
        # engines in the same directory (a server and a script) would
        # share and corrupt for each other; give each engine its own.
        import tempfile

        self._temp_dir = tempfile.mkdtemp(prefix="osmpq-duckdb-")
        self._db.execute(f"SET temp_directory='{self._temp_dir}'")

        threads = self.duckdb_config.get("threads")
        if threads:
            self._db.execute(f"SET threads={int(threads)}")
        memory_limit = self.duckdb_config.get("memory_limit")
        if memory_limit:
            self._db.execute(f"SET memory_limit='{memory_limit}'")

        if self.root.startswith("s3://"):
            sql = _s3_secret_sql()
            if sql:
                self._db.execute(sql)

    def _setup_connection(self, con, settings) -> None:
        """Per-run settings, issued on this run's cursor. Note
        `memory_limit` is a GLOBAL-scope DuckDB setting (see the module
        docstring's concurrency note): this still changes it for the
        whole shared database, not just this cursor's queries."""
        maxsize = getattr(settings, "maxsize", None)
        if maxsize:
            mb = max(1, int(maxsize) // (1024 * 1024))
            con.execute(f"SET memory_limit='{mb}MB'")
