"""``osmpq.update.server`` (contract section 6.3): `POST /run`, `GET
/status`, `GET /healthz`, and `osmpq updater-server` (`cli.py`).
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402

from osmpq.update import server as updater_server  # noqa: E402
from osmpq.update import updater as updater_mod  # noqa: E402


@pytest.fixture()
def root(tmp_path):
    d = tmp_path / "root"
    make_fixture.build(str(d))
    return d


@pytest.fixture()
def client(root, monkeypatch, tmp_path):
    monkeypatch.setenv("OSMPQ_ROOT", str(root))
    monkeypatch.setenv("OSMPQ_UPDATE_TMPDIR", str(tmp_path / "update-tmp"))
    updater_server._running = False
    updater_server._last_summary = None
    updater_server._last_error = None
    with TestClient(updater_server.app) as c:
        yield c


def test_healthz(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_status_before_any_run(client, root):
    resp = client.get("/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["running"] is False
    assert body["last"] is None
    assert body["manifest"] == 1
    assert body["timestamp_osm_base"] == make_fixture.TIMESTAMP_OSM_BASE


def test_run_returns_the_run_summary_json(client, monkeypatch):
    fake_summary = updater_mod.RunSummary(
        applied=0, first_seq=None, last_seq=None, timestamp=None, no_op=True,
        rows_touched={}, tier_versions={}, tier_bytes={}, dropped={},
    )
    # patched at the point of use (osmpq.update.server imports run_once by name)
    monkeypatch.setattr(updater_server, "run_once", lambda opts: fake_summary)

    resp = client.post("/run")
    assert resp.status_code == 200
    body = resp.json()
    assert body["applied"] == 0
    assert body["no_op"] is True


def test_run_returns_409_while_one_is_in_flight(client, monkeypatch):
    gate = threading.Event()
    entered = threading.Event()

    def slow_run_once(opts):
        entered.set()
        gate.wait(timeout=10)
        return updater_mod.RunSummary(applied=0, first_seq=None, last_seq=None, timestamp=None, no_op=True)

    monkeypatch.setattr(updater_server, "run_once", slow_run_once)

    result = {}
    t = threading.Thread(target=lambda: result.__setitem__(1, client.post("/run")))
    t.start()
    try:
        deadline = time.time() + 5
        while not entered.is_set() and time.time() < deadline:
            time.sleep(0.01)
        assert entered.is_set()

        resp2 = client.post("/run")
        assert resp2.status_code == 409
    finally:
        gate.set()
        t.join(timeout=10)

    assert result[1].status_code == 200

    # the lock was released: a subsequent run can start
    status_resp = client.get("/status")
    assert status_resp.json()["running"] is False


def test_run_errors_return_500_and_release_the_lock(client, monkeypatch):
    def failing_run_once(opts):
        raise RuntimeError("boom")

    monkeypatch.setattr(updater_server, "run_once", failing_run_once)

    resp = client.post("/run")
    assert resp.status_code == 500
    assert "boom" in resp.json()["error"]

    status_resp = client.get("/status")
    assert status_resp.json()["running"] is False

    # the lock isn't stuck: a second call can also run (and also fails, same way)
    resp2 = client.post("/run")
    assert resp2.status_code == 500


def test_build_options_reads_env(client, monkeypatch, root):
    monkeypatch.setenv("OSMPQ_REPLICATION_SOURCE", "https://example.test/repl")
    monkeypatch.setenv("OSMPQ_UPDATE_MAX_DIFFS", "5")
    monkeypatch.setenv("OSMPQ_UPDATE_THREADS", "2")
    monkeypatch.setenv("OSMPQ_UPDATE_MEMORY_LIMIT", "512MB")
    opts = updater_server._build_options()
    assert opts.root == str(root)
    assert opts.source == "https://example.test/repl"
    assert opts.max_diffs == 5
    assert opts.threads == 2
    assert opts.memory_limit == "512MB"
