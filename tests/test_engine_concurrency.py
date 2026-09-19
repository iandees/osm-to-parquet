"""Engine concurrency and single-database-per-Engine behaviour.

`Engine.run()`/`run_program()` can be called concurrently on one `Engine`
(FastAPI runs sync endpoints in a thread pool). Covers:

- 8 concurrent queries through one `Engine`, each producing the same
  result as running it sequentially, with `files_read <= files_considered`
  for every one of them (the per-run accounting -- `catalog.FILE_STATS` --
  doesn't leak between concurrently-running queries).
- `Engine._db.cursor()` isolation: two cursors of the same shared database
  can each `CREATE TEMP TABLE set__` with the same name and get their own
  independent contents, which is what lets concurrent runs' `set_<name>`
  temp tables coexist.
- one DuckDB database is opened per `Engine` (not per `run()`).
- `executor._s3_secret_sql`: the `CREATE SECRET` SQL built from env vars,
  and that it's None (no secret, DuckDB's own credential chain applies)
  when the required variables aren't all set.
"""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from osmpq.engine import Engine
from osmpq.engine import executor as executor_mod

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


def bbox_args(bbox):
    s, w, n, e = bbox
    return f"{s},{w},{n},{e}"


@pytest.fixture(scope="module")
def fixture_v2(tmp_path_factory):
    root = tmp_path_factory.mktemp("engine_concurrency_fixture_v2")
    return make_fixture.build(str(root), manifest_version=2)


@pytest.fixture(scope="module")
def engine(fixture_v2):
    return Engine(fixture_v2.root)


def _queries(fixture):
    """8 varied queries (bbox, byid, recurse, set-algebra-ish) so
    concurrent runs actually exercise different `set_<name>` temp-table
    names, file selections and row-group pruning at once."""
    b000 = bbox_args(fixture.leaf_bbox["000"])
    b001 = bbox_args(fixture.leaf_bbox["001"])
    b002 = bbox_args(fixture.leaf_bbox["002"])
    total = bbox_args(fixture.total_bbox)
    return [
        f'[out:json];node[amenity=cafe]({b000});out;',
        f'[out:json];way({b001});out ids;',
        f'[out:json];node({b002});out;',
        f"[out:json];node({fixture.cafe_node_id});out;",
        f"[out:json];way({fixture.closed_way_id});>;out;",
        f'[out:json];way["building"]({total});out ids;',
        f'[out:json];node[amenity=cafe]({total});out;',
        f"[out:json];relation({fixture.leaf_relation_id});out;",
    ]


def test_concurrent_runs_match_sequential_results(engine, fixture_v2):
    queries = _queries(fixture_v2)

    sequential = [engine.run(q) for q in queries]

    with ThreadPoolExecutor(max_workers=8) as pool:
        concurrent_results = list(pool.map(engine.run, queries))

    for i, (seq, con) in enumerate(zip(sequential, concurrent_results)):
        seq_ids = sorted((e["type"], e["id"]) for e in seq.elements)
        con_ids = sorted((e["type"], e["id"]) for e in con.elements)
        assert con_ids == seq_ids, f"query {i} mismatched under concurrency"
        assert con.remark == seq.remark

    for r in concurrent_results:
        assert r.stats["files_read"] <= r.stats["files_considered"]


def test_concurrent_runs_many_times_stay_consistent(engine, fixture_v2):
    # Same as above, but hammered a few times back to back, to catch a
    # rare interleaving that a single pass might miss.
    queries = _queries(fixture_v2)
    sequential_ids = [
        sorted((e["type"], e["id"]) for e in engine.run(q).elements) for q in queries
    ]

    for _ in range(3):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(engine.run, queries))
        for expected, r in zip(sequential_ids, results):
            got = sorted((e["type"], e["id"]) for e in r.elements)
            assert got == expected
            assert r.stats["files_read"] <= r.stats["files_considered"]


# --------------------------------------------------------------------
# Cursor isolation on the shared database
# --------------------------------------------------------------------


def test_two_cursors_of_shared_db_have_independent_temp_tables():
    import duckdb

    db = duckdb.connect(":memory:")
    try:
        c1 = db.cursor()
        c2 = db.cursor()
        c1.execute("CREATE TEMP TABLE set__ AS SELECT 1 AS x")
        c2.execute("CREATE TEMP TABLE set__ AS SELECT 2 AS x")
        assert c1.execute("SELECT x FROM set__").fetchall() == [(1,)]
        assert c2.execute("SELECT x FROM set__").fetchall() == [(2,)]
    finally:
        db.close()


def test_two_cursors_of_shared_db_create_temp_table_concurrently():
    import duckdb

    db = duckdb.connect(":memory:")
    results: dict[int, list] = {}
    barrier = threading.Barrier(2)

    def worker(n: int) -> None:
        cur = db.cursor()
        barrier.wait()  # line the two threads up so both CREATEs race
        cur.execute(f"CREATE TEMP TABLE set__ AS SELECT {n} AS x")
        results[n] = cur.execute("SELECT x FROM set__").fetchall()
        cur.close()

    try:
        threads = [threading.Thread(target=worker, args=(n,)) for n in (1, 2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert results[1] == [(1,)]
        assert results[2] == [(2,)]
    finally:
        db.close()


def test_engine_opens_one_database_reused_by_every_run(engine, fixture_v2):
    b = bbox_args(fixture_v2.leaf_bbox["000"])
    db_before = engine._db
    engine.run(f"[out:json];node[amenity=cafe]({b});out;")
    engine.run(f"[out:json];node[amenity=cafe]({b});out;")
    assert engine._db is db_before


def test_engine_run_uses_a_cursor_not_the_shared_db_directly(engine, fixture_v2):
    # `run()` materializes `set_<name>` temp tables while it runs. If those
    # lived on `self._db` itself (rather than on a per-run cursor that
    # gets closed in `run_program`'s `finally`), they'd still be visible
    # on `self._db` afterwards; a cursor's temp tables die with the
    # cursor, so querying `self._db` straight after `run()` returns should
    # show none left over.
    b = bbox_args(fixture_v2.leaf_bbox["000"])
    engine.run(f"[out:json];node[amenity=cafe]({b});out;")
    leftover = engine._db.execute(
        "SELECT table_name FROM duckdb_tables() WHERE temporary AND table_name LIKE 'set\\_%' ESCAPE '\\'"
    ).fetchall()
    assert leftover == []


# --------------------------------------------------------------------
# S3 secret creation from environment variables
# --------------------------------------------------------------------


def test_s3_secret_sql_none_when_vars_absent():
    assert executor_mod._s3_secret_sql({}) is None


def test_s3_secret_sql_none_when_partially_set():
    env = {"OSMPQ_S3_KEY_ID": "AKID", "OSMPQ_S3_SECRET": "shh"}  # no endpoint
    assert executor_mod._s3_secret_sql(env) is None


def test_s3_secret_sql_built_from_env_defaults():
    env = {
        "OSMPQ_S3_KEY_ID": "AKID123",
        "OSMPQ_S3_SECRET": "topsecret",
        "OSMPQ_S3_ENDPOINT": "abc123.r2.cloudflarestorage.com",
    }
    sql = executor_mod._s3_secret_sql(env)
    assert sql is not None
    assert "CREATE OR REPLACE SECRET osmpq_s3" in sql
    assert "TYPE S3" in sql
    assert "KEY_ID 'AKID123'" in sql
    assert "SECRET 'topsecret'" in sql
    assert "ENDPOINT 'abc123.r2.cloudflarestorage.com'" in sql
    assert "REGION 'auto'" in sql  # default
    assert "URL_STYLE 'path'" in sql  # default
    assert "USE_SSL true" in sql  # default


def test_s3_secret_sql_honors_overrides():
    env = {
        "OSMPQ_S3_KEY_ID": "AKID123",
        "OSMPQ_S3_SECRET": "topsecret",
        "OSMPQ_S3_ENDPOINT": "abc123.r2.cloudflarestorage.com",
        "OSMPQ_S3_REGION": "us-east-1",
        "OSMPQ_S3_URL_STYLE": "vhost",
        "OSMPQ_S3_USE_SSL": "false",
    }
    sql = executor_mod._s3_secret_sql(env)
    assert "REGION 'us-east-1'" in sql
    assert "URL_STYLE 'vhost'" in sql
    assert "USE_SSL false" in sql


def test_s3_secret_sql_escapes_single_quotes():
    env = {
        "OSMPQ_S3_KEY_ID": "a'b",
        "OSMPQ_S3_SECRET": "c'd",
        "OSMPQ_S3_ENDPOINT": "host.example.com",
    }
    sql = executor_mod._s3_secret_sql(env)
    assert "KEY_ID 'a''b'" in sql
    assert "SECRET 'c''d'" in sql


def test_engine_creates_s3_secret_for_s3_root(monkeypatch, tmp_path):
    # Engine.__init__ still needs to load a real manifest; point OSMPQ at a
    # real (local) fixture root but force the `root.startswith("s3://")`
    # branch and capture what SQL gets executed on `self._db`, so this
    # doesn't need a real bucket.
    info = make_fixture.build(str(tmp_path))

    monkeypatch.setenv("OSMPQ_S3_KEY_ID", "AKID123")
    monkeypatch.setenv("OSMPQ_S3_SECRET", "topsecret")
    monkeypatch.setenv("OSMPQ_S3_ENDPOINT", "abc123.r2.cloudflarestorage.com")

    import duckdb

    executed = []
    real_connect = duckdb.connect

    class _RecordingConnection:
        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        def execute(self, sql, *a, **kw):
            executed.append(sql)
            return self._real.execute(sql, *a, **kw)

    def fake_connect(*a, **kw):
        return _RecordingConnection(real_connect(*a, **kw))

    monkeypatch.setattr(executor_mod.duckdb, "connect", fake_connect)

    # Engine.__init__ treats `root` purely as a string prefix check for the
    # secret branch and otherwise as a path to join manifest-relative
    # paths onto; a local path that merely starts with "s3://" would break
    # manifest loading, so instead call the pieces directly: build the
    # secret SQL the same way Engine.__init__ would for an s3:// root, and
    # separately confirm Engine._setup_database issues it when root is
    # s3://-prefixed, using a monkeypatched loader.
    called_with = {}
    real_load_manifest = executor_mod.catalog.load_manifest

    def fake_load_manifest(root, con=None):
        called_with["root"] = root
        called_with["con"] = con
        return real_load_manifest(info.root)

    monkeypatch.setattr(executor_mod.catalog, "load_manifest", fake_load_manifest)

    eng = Engine("s3://fake-bucket/prefix")
    try:
        assert any("CREATE OR REPLACE SECRET osmpq_s3" in sql for sql in executed)
        assert any("abc123.r2.cloudflarestorage.com" in sql for sql in executed)
        # the manifest loader was handed the Engine's shared database, not
        # a throwaway connection, so a real s3:// root's remote reads would
        # see the secret too.
        assert called_with["con"] is eng._db
    finally:
        eng._db.close()
