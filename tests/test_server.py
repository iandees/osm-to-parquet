"""FastAPI app tests (contract section 6): /api/interpreter, /api/status,
/api/timestamp, CORS, and the 400 HTML error path for parse errors.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


@pytest.fixture(scope="module")
def root(tmp_path_factory):
    d = tmp_path_factory.mktemp("server_fixture")
    info = make_fixture.build(str(d))
    return str(d), info


@pytest.fixture()
def client(root, monkeypatch):
    from osmpq import server

    root_dir, _info = root
    monkeypatch.setenv("OSMPQ_ROOT", root_dir)
    server.reset_engine()
    with TestClient(server.app) as c:
        yield c
    server.reset_engine()


def test_interpreter_get_json(client, root):
    _root_dir, info = root
    s, w, n, e = info.leaf_bbox["000"]
    q = f"[out:json];node[amenity=cafe]({s},{w},{n},{e});out;"
    resp = client.get("/api/interpreter", params={"data": q})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    body = resp.json()
    assert body["version"] == 0.6
    ids = sorted(e["id"] for e in body["elements"])
    assert ids == [info.cafe_node_id, info.cafe_node2_id]


def test_interpreter_post_form(client, root):
    _root_dir, info = root
    q = f"[out:json];node({info.cafe_node_id});out;"
    resp = client.post("/api/interpreter", data={"data": q})
    assert resp.status_code == 200
    body = resp.json()
    assert [e["id"] for e in body["elements"]] == [info.cafe_node_id]


def test_interpreter_post_raw_body(client, root):
    _root_dir, info = root
    q = f"[out:json];node({info.cafe_node_id});out;"
    resp = client.post("/api/interpreter", content=q)
    assert resp.status_code == 200
    body = resp.json()
    assert [e["id"] for e in body["elements"]] == [info.cafe_node_id]


def test_interpreter_xml_output(client, root):
    _root_dir, info = root
    q = f"[out:xml];node({info.cafe_node_id});out;"
    resp = client.get("/api/interpreter", params={"data": q})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/osm3s+xml")
    assert resp.text.startswith('<?xml version="1.0"')


def test_interpreter_parse_error_400(client):
    resp = client.get("/api/interpreter", params={"data": "this is not overpass ql {{{"})
    assert resp.status_code == 400
    assert "text/html" in resp.headers["content-type"]
    assert "parse error" in resp.text
    assert 'color:#FF0000' in resp.text


def test_interpreter_unsupported_400(client):
    # `[out:csv(...)]` is still rejected by `planner.check_settings`
    # (docs/m3-contracts.md section 3.6 lifts this only once W1 is merged);
    # picked as an arbitrary still-unsupported construct to exercise the
    # generic 400 path (area queries became supported in M3, section 4).
    resp = client.get("/api/interpreter", params={"data": '[out:csv(::id)];node[amenity=cafe];out;'})
    assert resp.status_code == 400
    assert "text/html" in resp.headers["content-type"]


def test_status_endpoint(client):
    resp = client.get("/api/status")
    assert resp.status_code == 200
    assert "Connected as:" in resp.text
    assert "Rate limit: 0" in resp.text


def test_timestamp_endpoint(client):
    resp = client.get("/api/timestamp")
    assert resp.status_code == 200
    assert resp.text.strip() == make_fixture.TIMESTAMP_OSM_BASE


def test_cors_headers(client):
    resp = client.get(
        "/api/interpreter",
        params={"data": "[out:json];out;"},
        headers={"Origin": "https://overpass-turbo.eu"},
    )
    assert resp.headers.get("access-control-allow-origin") == "*"
