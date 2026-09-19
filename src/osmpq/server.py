"""FastAPI app for `/api/interpreter`, `/api/status`, `/api/timestamp`
(contract section 6). Root dataset from env `OSMPQ_ROOT`; the `Engine` is
created once at startup. Kept thin: all query semantics live in
`osmpq.engine`.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse

from osmpq.engine import Engine
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


async def _handle_interpreter(request: Request, data: Optional[str]) -> Response:
    body = await request.body()
    query_text = _extract_query(data, body)
    engine = get_engine()
    try:
        result = engine.run(query_text)
    except ParseError as e:
        line = e.line or 1
        return HTMLResponse(_error_html(e.message, line), status_code=400)
    except UnsupportedError as e:
        return HTMLResponse(_error_html(str(e), 1), status_code=400)
    body_text, content_type = result.render()
    return Response(content=body_text, media_type=content_type, status_code=200)


@app.get("/api/interpreter")
async def interpreter_get(request: Request, data: Optional[str] = None) -> Response:
    return await _handle_interpreter(request, data)


@app.post("/api/interpreter")
async def interpreter_post(request: Request, data: Optional[str] = None) -> Response:
    return await _handle_interpreter(request, data)


@app.get("/api/status")
def status() -> PlainTextResponse:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    text = (
        "Connected as: 0\n"
        f"Current time: {now}\n"
        "Announced endpoint: osmpq\n"
        "Rate limit: 0\n"
        "Currently running queries (pid, space limit, time limit, start time):\n"
    )
    return PlainTextResponse(text)


@app.get("/api/timestamp")
def timestamp() -> PlainTextResponse:
    engine = get_engine()
    ts = engine.manifest.timestamp_osm_base or ""
    return PlainTextResponse(ts)
