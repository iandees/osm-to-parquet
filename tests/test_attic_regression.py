"""No-regression check (docs/m4-contracts.md section 3.3 / section 8):
a program without any attic setting must emit the same SQL -- and read
the same files -- whether or not the manifest carries `history`.
`catalog.SNAPSHOT` must never be set for such a program, and no path
under `history/` may be opened.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from osmpq.engine import Engine, catalog

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture, make_history_fixture  # noqa: E402

_QUERIES = [
    '[out:json];node({b});out meta;',
    '[out:json];way({b})[highway];out geom;',
    '[out:json];node({b})[amenity=cafe];>;out;',
    '[out:json];way({b});<;out ids;',
]


@pytest.fixture(scope="module")
def history_fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("attic_regression_history")
    return make_history_fixture.build(str(root))


@pytest.fixture(scope="module")
def plain_fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("attic_regression_plain")
    return make_fixture.build(str(root), manifest_version=4)


@pytest.fixture(scope="module")
def history_engine(history_fixture):
    return Engine(history_fixture.root)


@pytest.fixture(scope="module")
def plain_engine(plain_fixture):
    return Engine(plain_fixture.root)


def _bbox_args(bbox):
    s, w, n, e = bbox
    return f"{s},{w},{n},{e}"


@pytest.mark.parametrize("query_tpl", _QUERIES)
def test_no_attic_settings_reads_the_same_file_count_with_or_without_history(
    query_tpl, history_engine, plain_engine, history_fixture, plain_fixture
):
    # The two fixtures' *current-state* data is identical (make_history_
    # fixture.build starts from the same make_fixture.build call); only
    # one manifest additionally carries a `history` section. A query with
    # no attic setting must cost the same either way.
    b = _bbox_args(history_fixture.base.total_bbox)
    q = query_tpl.format(b=b)
    r_history = history_engine.run(q)
    r_plain = plain_engine.run(q)
    assert r_history.stats["files_read"] == r_plain.stats["files_read"]
    assert sorted(e["id"] for e in r_history.elements if "id" in e) == sorted(
        e["id"] for e in r_plain.elements if "id" in e
    )


class _SpySnapshot:
    """A stand-in for `catalog.SNAPSHOT` (a real `ContextVar`, whose `set`/
    `get`/`reset` are C-level slots `unittest.mock.patch.object` can't
    intercept on the instance) that records every `.set()` call and
    delegates to the real ContextVar underneath."""

    def __init__(self, real):
        self._real = real
        self.set_calls = []

    def get(self, *a, **kw):
        return self._real.get(*a, **kw)

    def set(self, value):
        self.set_calls.append(value)
        return self._real.set(value)

    def reset(self, token):
        return self._real.reset(token)


def test_snapshot_contextvar_never_set_for_a_plain_query(history_engine, history_fixture):
    spy = _SpySnapshot(catalog.SNAPSHOT)
    b = _bbox_args(history_fixture.base.total_bbox)
    with patch.object(catalog, "SNAPSHOT", spy):
        history_engine.run(f"[out:json];node({b});out meta;")
    assert spy.set_calls == []


def test_no_history_path_is_ever_opened_for_a_plain_query(history_engine, history_fixture, tmp_path):
    # Belt-and-suspenders: directly assert none of the SQL text any
    # `duckdb` cursor executes during a plain run mentions "history/".
    import duckdb

    seen_sql = []
    real_execute = duckdb.DuckDBPyConnection.execute

    def spy_execute(self, sql, *args, **kwargs):
        if isinstance(sql, str):
            seen_sql.append(sql)
        return real_execute(self, sql, *args, **kwargs)

    b = _bbox_args(history_fixture.base.total_bbox)
    with patch.object(duckdb.DuckDBPyConnection, "execute", spy_execute):
        history_engine.run(f"[out:json];node({b})[amenity=cafe];>;out;")
    assert not any("history/" in s for s in seen_sql)


def test_out_count_matches_between_history_and_plain_manifests(history_engine, plain_engine, history_fixture):
    b = _bbox_args(history_fixture.base.total_bbox)
    r_history = history_engine.run(f"[out:json];node({b});out count;")
    r_plain = plain_engine.run(f"[out:json];node({b});out count;")
    assert r_history.elements == r_plain.elements
