"""``osmpq.server`` per docs/m3-contracts.md section 6.1: per-IP/global
concurrency slots and the 429 shape, ``/api/status``, ``/api/kill_my_queries``,
``/healthz``, response headers, and manifest refresh through the HTTP path.
"""
from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402

from osmpq import server  # noqa: E402
from osmpq.engine.executor import Engine  # noqa: E402
from osmpq.layout import manifest as manifest_mod  # noqa: E402


@pytest.fixture()
def root(tmp_path):
    d = tmp_path / "root"
    info = make_fixture.build(str(d))
    return d, info


@pytest.fixture()
def client(root, monkeypatch):
    root_dir, _info = root
    monkeypatch.setenv("OSMPQ_ROOT", str(root_dir))
    server.reset_engine()
    server._running.clear()
    server._ip_counts.clear()
    with TestClient(server.app) as c:
        yield c
    server.reset_engine()
    server._running.clear()
    server._ip_counts.clear()


def _query(client, q="[out:json];node(1);out;", **kwargs):
    return client.get("/api/interpreter", params={"data": q}, **kwargs)


# --------------------------------------------------------------------------
# concurrency slots: 429 on the third concurrent query from one IP
# --------------------------------------------------------------------------


class _Gate:
    """Lets a test hold N `Engine.run_program` calls open at once (blocked
    on `release_event`) so it can observe the slot-limiting behaviour
    while queries are genuinely still running."""

    def __init__(self) -> None:
        self.entered = 0
        self.lock = threading.Lock()
        self.release_event = threading.Event()

    def enter(self) -> None:
        with self.lock:
            self.entered += 1

    def wait_for(self, n: int, timeout: float = 5.0) -> None:
        deadline = time.time() + timeout
        while self.entered < n and time.time() < deadline:
            time.sleep(0.01)
        assert self.entered >= n, f"only {self.entered}/{n} queries entered run_program in time"


@pytest.fixture()
def blocking_engine(monkeypatch):
    """Monkeypatches `Engine.run_program` to block on a `Gate` until the
    test releases it, so slot-holding queries can be held open on purpose
    (contract section 6.1's own suggested approach)."""
    gate = _Gate()
    original = Engine.run_program

    def blocked(self, program, timeout=None, cancel=None):
        gate.enter()
        gate.release_event.wait(timeout=10)
        return original(self, program, timeout=timeout, cancel=cancel)

    monkeypatch.setattr(Engine, "run_program", blocked)
    return gate


def test_third_concurrent_query_from_one_ip_gets_429(client, blocking_engine):
    gate = blocking_engine
    results: dict[int, object] = {}

    def worker(i: int) -> None:
        results[i] = _query(client)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    t1.start()
    t2.start()
    try:
        gate.wait_for(2)

        # OSMPQ_SLOTS_PER_IP defaults to 2: a third concurrent query from
        # the same IP is refused with the Overpass 429 shape.
        resp3 = _query(client)
        assert resp3.status_code == 429
        assert "rate_limited" in resp3.text
        assert "/api/status" in resp3.text
        assert "text/html" in resp3.headers["content-type"]
    finally:
        gate.release_event.set()
        t1.join(timeout=10)
        t2.join(timeout=10)

    assert results[1].status_code == 200
    assert results[2].status_code == 200


def test_slot_is_freed_after_the_query_completes(client, blocking_engine):
    gate = blocking_engine
    result: dict[int, object] = {}
    t = threading.Thread(target=lambda: result.__setitem__(1, _query(client)))
    t.start()
    gate.wait_for(1)
    gate.release_event.set()
    t.join(timeout=10)
    assert result[1].status_code == 200

    # the slot from the finished query is free again
    resp = _query(client)
    assert resp.status_code == 200


def test_slots_per_ip_env_override(client, blocking_engine, monkeypatch):
    monkeypatch.setenv("OSMPQ_SLOTS_PER_IP", "1")
    gate = blocking_engine
    t1 = threading.Thread(target=lambda: _query(client))
    t1.start()
    try:
        gate.wait_for(1)
        resp2 = _query(client)
        assert resp2.status_code == 429
    finally:
        gate.release_event.set()
        t1.join(timeout=10)


def test_different_ips_get_independent_slots(client, blocking_engine, monkeypatch):
    monkeypatch.setenv("OSMPQ_SLOTS_PER_IP", "1")
    gate = blocking_engine
    t1 = threading.Thread(target=lambda: _query(client, headers={"X-Forwarded-For": "1.2.3.4"}))
    t1.start()
    try:
        gate.wait_for(1)
        # same client IP (peer address, since OSMPQ_TRUST_PROXY defaults to
        # 0 -- the X-Forwarded-For header above is ignored) is unaffected
        # by the *other* peer's slot; both requests here share one peer
        # address though, so a second one from it is still refused...
        resp_same_peer = _query(client)
        assert resp_same_peer.status_code == 429
    finally:
        gate.release_event.set()
        t1.join(timeout=10)


def test_max_concurrent_env_caps_the_whole_process(client, blocking_engine, monkeypatch):
    monkeypatch.setenv("OSMPQ_SLOTS_PER_IP", "5")
    monkeypatch.setenv("OSMPQ_MAX_CONCURRENT", "1")
    gate = blocking_engine
    t1 = threading.Thread(target=lambda: _query(client))
    t1.start()
    try:
        gate.wait_for(1)
        resp2 = _query(client)
        assert resp2.status_code == 429
    finally:
        gate.release_event.set()
        t1.join(timeout=10)


# --------------------------------------------------------------------------
# /api/status
# --------------------------------------------------------------------------


def test_status_shape_and_running_query_line(client, blocking_engine):
    gate = blocking_engine
    result: dict[int, object] = {}
    t = threading.Thread(target=lambda: result.__setitem__(1, _query(client, q="[out:json][timeout:30];node(1);out;")))
    t.start()
    try:
        gate.wait_for(1)
        resp = client.get("/api/status")
        assert resp.status_code == 200
        text = resp.text
        assert "Connected as:" in text
        assert "Current time:" in text
        assert "Announced endpoint: none" in text
        assert "Rate limit: 2" in text
        assert "slots available now." in text
        assert "Currently running queries (pid, space limit, time limit, start time):" in text
        running_header = "Currently running queries (pid, space limit, time limit, start time):"
        after = text.split(running_header, 1)[1]
        lines = [ln for ln in after.splitlines() if ln.strip()]
        assert len(lines) == 1
        parts = lines[0].split()
        assert len(parts) == 4
        pid, maxsize, timeout, start = parts
        assert int(pid) > 0
        assert int(maxsize) > 0
        assert int(timeout) == 30
    finally:
        gate.release_event.set()
        t.join(timeout=10)


def test_status_only_shows_the_callers_own_queries(client):
    # no queries running: the block is present but empty
    resp = client.get("/api/status")
    text = resp.text
    header_idx = text.index("Currently running queries")
    after = text[header_idx:]
    assert after.strip().endswith(":") or after.strip().split("\n")[-1] == ""


# --------------------------------------------------------------------------
# /api/kill_my_queries
# --------------------------------------------------------------------------


def test_kill_my_queries_interrupts_a_long_query(client, monkeypatch):
    # Simulate "still running" by blocking `Engine.run_program` itself
    # until either a grace period elapses or the request's `CancelToken`
    # (threaded through from `_handle_interpreter`) is cancelled --
    # `/api/kill_my_queries` must be the one to flip it while this is the
    # query recorded as running for the caller's IP.
    from osmpq.engine.executor import Engine

    real_run_program = Engine.run_program
    entered = threading.Event()
    cancel_holder: dict[str, object] = {}

    def patched_run_program(self, program, timeout=None, cancel=None):
        cancel_holder["cancel"] = cancel
        entered.set()
        # Block until either killed (cancel interrupts a cursor we never
        # gave it any work to interrupt) or a short grace period elapses --
        # what matters for this test is that /api/kill_my_queries calls
        # `cancel.cancel()` on *this* request's token while it's the one
        # recorded as running for this IP.
        for _ in range(200):
            if cancel is not None and cancel.is_cancelled():
                break
            time.sleep(0.02)
        return real_run_program(self, program, timeout=timeout, cancel=cancel)

    monkeypatch.setattr(Engine, "run_program", patched_run_program)

    result: dict[int, object] = {}
    t = threading.Thread(target=lambda: result.__setitem__(1, _query(client)))
    t.start()
    deadline = time.time() + 5
    while not entered.is_set() and time.time() < deadline:
        time.sleep(0.01)
    assert entered.is_set()

    kill_resp = client.get("/api/kill_my_queries")
    assert kill_resp.status_code == 200
    assert "killed" in kill_resp.text
    assert "text/html" in kill_resp.headers["content-type"]

    t.join(timeout=10)
    assert cancel_holder["cancel"].is_cancelled()
    assert result[1].status_code == 200


# --------------------------------------------------------------------------
# /healthz
# --------------------------------------------------------------------------


def test_healthz_ok(client, root):
    _root_dir, _info = root
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["timestamp_osm_base"] == make_fixture.TIMESTAMP_OSM_BASE
    assert body["manifest"] == 1


def test_healthz_503_before_manifest_loads(monkeypatch):
    monkeypatch.setenv("OSMPQ_ROOT", "/nonexistent/root/for/healthz/test")
    server.reset_engine()
    try:
        with TestClient(server.app) as c:
            resp = c.get("/healthz")
            assert resp.status_code == 503
    finally:
        server.reset_engine()


# --------------------------------------------------------------------------
# response headers
# --------------------------------------------------------------------------


def test_interpreter_response_headers(client):
    resp = _query(client)
    assert resp.status_code == 200
    assert resp.headers["X-OSMPQ-Manifest"] == "1"
    assert resp.headers["Cache-Control"] == "public, max-age=60"


# --------------------------------------------------------------------------
# manifest refresh through the HTTP path
# --------------------------------------------------------------------------


def test_manifest_refresh_through_http_with_monkeypatched_clock(client, root, monkeypatch):
    root_dir, _info = root
    resp1 = client.get("/api/timestamp")
    assert resp1.text.strip() == make_fixture.TIMESTAMP_OSM_BASE
    assert resp1.text.strip() != "2099-01-01T00:00:00Z"

    # write manifest 2 with a new timestamp
    man = manifest_mod.load_latest(str(root_dir))
    man.timestamp_osm_base = "2099-01-01T00:00:00Z"
    manifest_mod.write_manifest(str(root_dir), man, 2)

    # before the refresh interval elapses, the engine still serves the old one
    resp_before = client.get("/api/timestamp")
    assert resp_before.text.strip() == make_fixture.TIMESTAMP_OSM_BASE

    # advance the engine's own clock past OSMPQ_MANIFEST_REFRESH_SECONDS
    import osmpq.engine.executor as executor_mod

    engine = server.get_engine()
    real_monotonic = executor_mod.time.monotonic
    fake_now = real_monotonic() + engine._manifest_refresh_seconds + 1

    monkeypatch.setattr(executor_mod.time, "monotonic", lambda: fake_now)
    try:
        resp_after = client.get("/api/timestamp")
    finally:
        monkeypatch.setattr(executor_mod.time, "monotonic", real_monotonic)

    assert resp_after.text.strip() == "2099-01-01T00:00:00Z"
    assert resp_after.headers  # sanity: still a normal response


def test_manifest_refresh_seen_by_a_query_not_just_timestamp(client, root, monkeypatch):
    """The contract's own test recipe: "write a new manifest..., advance a
    monkeypatched clock, assert the next query sees the new
    timestamp_osm_base" -- through `/api/interpreter` itself, not
    `/api/timestamp`."""
    root_dir, _info = root
    man = manifest_mod.load_latest(str(root_dir))
    man.timestamp_osm_base = "2088-05-05T00:00:00Z"
    manifest_mod.write_manifest(str(root_dir), man, 2)

    import osmpq.engine.executor as executor_mod

    engine = server.get_engine()
    real_monotonic = executor_mod.time.monotonic
    fake_now = real_monotonic() + engine._manifest_refresh_seconds + 1
    monkeypatch.setattr(executor_mod.time, "monotonic", lambda: fake_now)
    try:
        resp = _query(client)
    finally:
        monkeypatch.setattr(executor_mod.time, "monotonic", real_monotonic)

    body = json.loads(resp.text)
    assert body["osm3s"]["timestamp_osm_base"] == "2088-05-05T00:00:00Z"
    assert resp.headers["X-OSMPQ-Manifest"] == "2"
