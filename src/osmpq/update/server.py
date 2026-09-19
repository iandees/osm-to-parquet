"""FastAPI app for `osmpq updater-server` (contract section 6.3).

Environment:

- `OSMPQ_ROOT`: dataset root, local or `s3://` (required for `/run`;
  `/status` and `/healthz` degrade gracefully without it).
- `OSMPQ_REPLICATION_SOURCE`: overrides the manifest's own
  `replication_source` for this run, same as `osmpq update --source`.
- `OSMPQ_UPDATE_MAX_DIFFS` (60): `UpdateOptions.max_diffs`.
- `OSMPQ_UPDATE_TMPDIR`: `UpdateOptions.tmpdir` (defaults, like the CLI, to
  `.osmpq-update-tmp` under the current directory).
- `OSMPQ_UPDATE_THREADS` / `OSMPQ_UPDATE_MEMORY_LIMIT`: DuckDB
  `threads`/`memory_limit` for the run's scratch database.

`POST /run` executes one `osmpq.update.updater.run_once` and returns its
`RunSummary` as JSON; a second `/run` while one is in flight returns 409
without starting another. `GET /status` reports whether a run is currently
in progress, the last run's summary, and the root's current manifest
number/`timestamp_osm_base`. `GET /healthz` is a plain liveness check (this
process doesn't hold a persistent Engine/manifest the way `osmpq serve`
does, so there's nothing to be "not ready" for).
"""
from __future__ import annotations

import dataclasses
import os
import sys
import threading
import traceback
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from osmpq import store as store_mod
from osmpq.layout import manifest as manifest_mod
from osmpq.update.updater import RunSummary, UpdateOptions, run_once

app = FastAPI(title="osmpq-updater")

_lock = threading.Lock()
_running = False
_last_summary: Optional[RunSummary] = None
_last_error: Optional[str] = None


def _env_int(name: str) -> Optional[int]:
    raw = os.environ.get(name)
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _build_options() -> UpdateOptions:
    root = os.environ.get("OSMPQ_ROOT")
    if not root:
        raise RuntimeError("OSMPQ_ROOT is not set")
    return UpdateOptions(
        root=root,
        source=os.environ.get("OSMPQ_REPLICATION_SOURCE") or None,
        max_diffs=_env_int("OSMPQ_UPDATE_MAX_DIFFS") or 60,
        tmpdir=os.environ.get("OSMPQ_UPDATE_TMPDIR") or None,
        threads=_env_int("OSMPQ_UPDATE_THREADS"),
        memory_limit=os.environ.get("OSMPQ_UPDATE_MEMORY_LIMIT") or None,
    )


def _manifest_info(root: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    if not root:
        return None, None
    number: Optional[int] = None
    timestamp: Optional[str] = None
    try:
        store = store_mod.for_root(root)
        number = int(store.read_bytes("manifest/LATEST").decode("utf-8").strip())
    except Exception:
        number = None
    try:
        man = manifest_mod.load_latest(root)
        timestamp = man.timestamp_osm_base
    except Exception:
        timestamp = None
    return number, timestamp


def _run_locked() -> RunSummary:
    """Runs on a worker thread (`run_in_threadpool`, below); the module-
    level `_running` flag is already `True` by the time this starts and is
    always cleared in `finally`, whether the run raises or not -- contract:
    "the lock is always released"."""
    global _last_summary, _last_error, _running
    try:
        opts = _build_options()
        summary = run_once(opts)
        _last_summary = summary
        _last_error = None
        return summary
    except Exception as e:
        _last_error = f"{type(e).__name__}: {e}"
        print(
            f"osmpq updater-server: run failed: {_last_error}\n{traceback.format_exc()}",
            file=sys.stderr, flush=True,
        )
        raise
    finally:
        with _lock:
            _running = False


@app.post("/run")
async def run_endpoint() -> JSONResponse:
    global _running
    with _lock:
        if _running:
            return JSONResponse({"error": "a run is already in progress"}, status_code=409)
        _running = True

    try:
        summary = await run_in_threadpool(_run_locked)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    return JSONResponse(dataclasses.asdict(summary) if summary is not None else None)


@app.get("/status")
def status() -> JSONResponse:
    root = os.environ.get("OSMPQ_ROOT")
    manifest_number, timestamp_osm_base = _manifest_info(root)
    with _lock:
        running = _running
    last = dataclasses.asdict(_last_summary) if _last_summary is not None else None
    return JSONResponse({
        "running": running,
        "last": last,
        "manifest": manifest_number,
        "timestamp_osm_base": timestamp_osm_base,
    })


@app.get("/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"ok": True})
