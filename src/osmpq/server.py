"""FastAPI app for `/api/interpreter`, `/api/status`, `/api/timestamp`,
`/api/kill_my_queries`, `/healthz` (contract section 6.1). Root dataset
from env `OSMPQ_ROOT`; the `Engine` is created once at startup. Kept thin:
all query semantics live in `osmpq.engine`.

Environment (defaults in parentheses), all read fresh on each request (not
cached at import time) so tests can `monkeypatch.setenv` per-test:

- `OSMPQ_SLOTS_PER_IP` (2): concurrent queries per client IP.
- `OSMPQ_MAX_CONCURRENT` (8): concurrent queries per process.
- `OSMPQ_MAX_TIMEOUT` (180): `[timeout:]` is clamped to this.
- `OSMPQ_MAX_MAXSIZE` (1073741824): `[maxsize:]` clamp.
- `OSMPQ_TRUST_PROXY` (0): when 1, client IP is `CF-Connecting-IP`, else
  the first `X-Forwarded-For` entry; when 0 (default), always the peer
  address -- proxy headers are spoofable without an actual trusted proxy
  in front, so they're ignored unless this is set.
- `OSMPQ_ANNOUNCED_ENDPOINT` ("none"): shown in `/api/status`.
- `OSMPQ_MANIFEST_REFRESH_SECONDS` (60): passed through to `Engine` --
  see `osmpq.engine.executor.Engine.refresh_manifest_if_due`.
- `OSMPQ_LOG_QUERIES` (0): include the query text in the request log.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import os
import sys
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.concurrency import run_in_threadpool

from osmpq.engine import Engine
from osmpq.engine.executor import CancelToken
from osmpq.errors import ParseError, UnsupportedError


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Fail fast if the manifest can't be loaded, but don't crash import
    # (useful for tests that set OSMPQ_ROOT per-test, after import time).
    try:
        get_engine()
    except Exception:
        pass
    yield


app = FastAPI(title="osmpq", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        root = os.environ.get("OSMPQ_ROOT")
        if not root:
            raise RuntimeError("OSMPQ_ROOT is not set")
        _engine = Engine(root)
    return _engine


def reset_engine() -> None:
    """Drop the cached Engine so the next request re-reads OSMPQ_ROOT.
    Used by tests that point at a fresh fixture root per test."""
    global _engine
    _engine = None


# --------------------------------------------------------------------------
# env accessors (read fresh every call -- see module docstring)
# --------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "")


def _slots_per_ip() -> int:
    return _env_int("OSMPQ_SLOTS_PER_IP", 2)


def _max_concurrent() -> int:
    return _env_int("OSMPQ_MAX_CONCURRENT", 8)


def _max_timeout() -> int:
    return _env_int("OSMPQ_MAX_TIMEOUT", 180)


def _max_maxsize() -> int:
    return _env_int("OSMPQ_MAX_MAXSIZE", 1073741824)


def _trust_proxy() -> bool:
    return _env_bool("OSMPQ_TRUST_PROXY", False)


def _announced_endpoint() -> str:
    return os.environ.get("OSMPQ_ANNOUNCED_ENDPOINT", "none")


def _log_queries() -> bool:
    return _env_bool("OSMPQ_LOG_QUERIES", False)


def _client_ip(request: Request) -> str:
    if _trust_proxy():
        cf = request.headers.get("CF-Connecting-IP")
        if cf:
            return cf.strip()
        xff = request.headers.get("X-Forwarded-For")
        if xff:
            return xff.split(",")[0].strip()
    client = request.client
    return client.host if client else "unknown"


def _manifest_number(engine: Engine):
    return engine.manifest_number


# --------------------------------------------------------------------------
# concurrency slots (contract section 6.1: per-IP + global, 429 on excess)
# --------------------------------------------------------------------------


@dataclass
class RunningQuery:
    id: int
    ip: str
    maxsize: int
    timeout: int
    start: float
    cancel: CancelToken


_slots_lock = threading.Lock()
_running: dict[int, RunningQuery] = {}
_ip_counts: dict[str, int] = {}
_id_seq = itertools.count(1)


def _try_acquire(ip: str) -> Optional[RunningQuery]:
    """Reserve a slot for `ip`, or None if either the per-IP or the global
    limit is currently at capacity (checked-and-incremented atomically)."""
    with _slots_lock:
        if len(_running) >= _max_concurrent():
            return None
        if _ip_counts.get(ip, 0) >= _slots_per_ip():
            return None
        qid = next(_id_seq)
        rq = RunningQuery(id=qid, ip=ip, maxsize=0, timeout=0, start=time.time(), cancel=CancelToken())
        _running[qid] = rq
        _ip_counts[ip] = _ip_counts.get(ip, 0) + 1
        return rq


def _release(rq: RunningQuery) -> None:
    with _slots_lock:
        _running.pop(rq.id, None)
        remaining = _ip_counts.get(rq.ip, 0) - 1
        if remaining <= 0:
            _ip_counts.pop(rq.ip, None)
        else:
            _ip_counts[rq.ip] = remaining


def _running_for_ip(ip: str) -> list[RunningQuery]:
    with _slots_lock:
        return [rq for rq in _running.values() if rq.ip == ip]


# Kept close to what overpass turbo greps for in a 429 body (contract
# section 6.1): the literal "rate_limited" token and the "/api/status"
# hint, inside an Overpass-shaped error <p>.
_RATE_LIMIT_HTML = (
    "<html><body><p>Error: runtime error: open64: 0 Success /osm3s_v0.7.62_osm_base "
    "Dispatcher_Client::request_read_and_idx::rate_limited. Please check "
    "/api/status for the quota of your IP address.</p></body></html>"
)


def _error_html(message: str, line: int = 1) -> str:
    return (
        "<html><body><p><strong style=\"color:#FF0000\">Error</strong>: "
        f"line {line}: parse error: {message}</p></body></html>"
    )


def _extract_query(data: Optional[str], body: bytes) -> str:
    if data:
        return data
    if not body:
        return ""
    text = body.decode("utf-8", errors="replace")
    # Accept `data=<query>` form-encoded bodies too; otherwise the raw body
    # *is* the query (contract: "raw POST body also accepted when no data
    # field").
    try:
        from urllib.parse import parse_qs

        parsed = parse_qs(text, keep_blank_values=True)
        if parsed.get("data"):
            return parsed["data"][0]
    except Exception:
        pass
    return text


def _log_request(
    *, ip: str, status: int, seconds: float, files_read: int, elements: int,
    nbytes: int, query_text: str, timed_out: bool, remark: Optional[str],
) -> None:
    """One JSON line per request on stdout (contract section 6.1)."""
    entry = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ip": ip,
        "status": status,
        "seconds": round(seconds, 3),
        "files_read": files_read,
        "elements": elements,
        "bytes": nbytes,
        "query_sha256": hashlib.sha256(query_text.encode("utf-8")).hexdigest(),
        "timed_out": bool(timed_out),
        "remark": remark,
    }
    if _log_queries():
        entry["query"] = query_text
    print(json.dumps(entry), file=sys.stdout, flush=True)


async def _handle_interpreter(request: Request, data: Optional[str]) -> Response:
    t0 = time.time()
    body = await request.body()
    query_text = _extract_query(data, body)
    ip = _client_ip(request)

    from osmpq.ql import parse as ql_parse

    try:
        program = ql_parse(query_text)
    except ParseError as e:
        line = e.line or 1
        _log_request(ip=ip, status=400, seconds=time.time() - t0, files_read=0, elements=0,
                     nbytes=0, query_text=query_text, timed_out=False, remark="parse error")
        return HTMLResponse(_error_html(e.message, line), status_code=400)

    settings = program.settings
    # contract section 6.1: `[timeout:]`/`[maxsize:]` are clamped to the
    # server's own caps, never widened.
    settings.timeout = min(settings.timeout, _max_timeout()) if settings.timeout else _max_timeout()
    settings.maxsize = min(settings.maxsize, _max_maxsize()) if settings.maxsize else _max_maxsize()

    rq = _try_acquire(ip)
    if rq is None:
        _log_request(ip=ip, status=429, seconds=time.time() - t0, files_read=0, elements=0,
                     nbytes=0, query_text=query_text, timed_out=False, remark="rate_limited")
        return HTMLResponse(_RATE_LIMIT_HTML, status_code=429)
    rq.maxsize = settings.maxsize
    rq.timeout = settings.timeout

    try:
        engine = get_engine()
        try:
            # `run_program` is a blocking DuckDB call (seconds, for a real
            # query); offloading it to Starlette's worker thread pool is
            # what actually lets two `/api/interpreter` requests run
            # concurrently on one process (contract section 6.1's slots) --
            # an `async def` handler that called it directly would instead
            # block the single event loop and serialize every request.
            result = await run_in_threadpool(
                engine.run_program, program, timeout=settings.timeout, cancel=rq.cancel
            )
        except UnsupportedError as e:
            _log_request(ip=ip, status=400, seconds=time.time() - t0, files_read=0, elements=0,
                         nbytes=0, query_text=query_text, timed_out=False, remark=str(e))
            return HTMLResponse(_error_html(str(e), 1), status_code=400)
    finally:
        _release(rq)

    body_text, content_type = result.render()
    nbytes = len(body_text.encode("utf-8")) if isinstance(body_text, str) else len(body_text)
    stats = result.stats or {}
    status_code = 200
    _log_request(
        ip=ip, status=status_code, seconds=time.time() - t0,
        files_read=stats.get("files_read", 0), elements=stats.get("elements", 0),
        nbytes=nbytes, query_text=query_text, timed_out=bool(stats.get("timed_out")),
        remark=result.remark,
    )
    resp = Response(content=body_text, media_type=content_type, status_code=status_code)
    resp.headers["X-OSMPQ-Manifest"] = str(_manifest_number(engine))
    # A `remark` (timeout, `kill_my_queries` cancellation, or any other
    # runtime error -- see `Engine.run_program`'s `duckdb.InterruptException`/
    # `RuntimeQueryError` handling) means `elements` isn't the real,
    # complete result, so it must never be cached -- a 200 status alone
    # (Overpass's own convention for these) isn't a safe cacheability
    # signal, and the Worker's cache layer (`handleInterpreter` in
    # `deploy/cloudflare/src/index.ts`) relies on this header rather than
    # re-deriving the same judgment from the response body.
    resp.headers["Cache-Control"] = "no-store" if result.remark else "public, max-age=60"
    return resp


@app.get("/api/interpreter")
async def interpreter_get(request: Request, data: Optional[str] = None) -> Response:
    return await _handle_interpreter(request, data)


@app.post("/api/interpreter")
async def interpreter_post(request: Request, data: Optional[str] = None) -> Response:
    return await _handle_interpreter(request, data)


@app.get("/api/status")
def status(request: Request) -> PlainTextResponse:
    ip = _client_ip(request)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    slots_per_ip = _slots_per_ip()
    mine = _running_for_ip(ip)
    available = max(0, slots_per_ip - len(mine))
    lines = [
        f"Connected as: {ip}",
        f"Current time: {now}",
        f"Announced endpoint: {_announced_endpoint()}",
        f"Rate limit: {slots_per_ip}",
        f"{available} slots available now.",
        "Currently running queries (pid, space limit, time limit, start time):",
    ]
    for rq in sorted(mine, key=lambda r: r.id):
        start_iso = datetime.fromtimestamp(rq.start, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        lines.append(f"{rq.id} {rq.maxsize} {rq.timeout} {start_iso}")
    return PlainTextResponse("\n".join(lines) + "\n")


@app.get("/api/kill_my_queries")
def kill_my_queries(request: Request) -> HTMLResponse:
    ip = _client_ip(request)
    mine = _running_for_ip(ip)
    for rq in mine:
        rq.cancel.cancel()
    killed_lines = "".join(f"<p> pid: {rq.id} </p>\n" for rq in sorted(mine, key=lambda r: r.id))
    html = (
        "<html><body><p>The following queries have been killed:</p>\n"
        f"{killed_lines}</body></html>"
    )
    return HTMLResponse(html)


@app.get("/healthz")
def healthz() -> Response:
    try:
        engine = get_engine()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=503)
    engine.refresh_manifest_if_due()
    return JSONResponse({
        "manifest": _manifest_number(engine),
        "timestamp_osm_base": engine.manifest.timestamp_osm_base,
    })


@app.get("/api/timestamp")
def timestamp() -> PlainTextResponse:
    engine = get_engine()
    engine.refresh_manifest_if_due()
    ts = engine.manifest.timestamp_osm_base or ""
    return PlainTextResponse(ts)
