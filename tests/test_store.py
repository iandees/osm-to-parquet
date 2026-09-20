"""``osmpq.store`` (contract section 6.2): ``LocalStore``, ``MemoryStore``
and ``S3Store`` (the latter via ``botocore.stub.Stubber``, no real bucket),
plus ``for_root`` and ``s3_secret_sql``.
"""
from __future__ import annotations

import boto3
import pytest
from botocore.stub import Stubber

from osmpq import store as store_mod


# --------------------------------------------------------------------------
# LocalStore
# --------------------------------------------------------------------------


def test_local_store_write_read_exists_delete(tmp_path):
    s = store_mod.LocalStore(str(tmp_path))
    assert not s.exists("a/b.txt")
    s.write_bytes("a/b.txt", b"hello")
    assert s.exists("a/b.txt")
    assert s.read_bytes("a/b.txt") == b"hello"
    assert (tmp_path / "a" / "b.txt").read_bytes() == b"hello"
    s.delete("a/b.txt")
    assert not s.exists("a/b.txt")


def test_local_store_write_bytes_is_atomic_and_does_not_truncate_hardlinks(tmp_path):
    """The same concern `layout.manifest._atomic_write_text` existed for:
    a `cp -al` snapshot shares an inode with the original until one of them
    is rewritten -- `write_bytes` must never truncate that shared inode in
    place (contract: "keep the local path byte-for-byte compatible...
    hardlinked snapshots rely on the temp-file + os.replace pattern")."""
    import os

    s = store_mod.LocalStore(str(tmp_path))
    s.write_bytes("manifest/LATEST", b"1")
    original_inode = (tmp_path / "manifest" / "LATEST").stat().st_ino

    snapshot_dir = tmp_path.parent / "snapshot"
    snapshot_dir.mkdir()
    os.link(str(tmp_path / "manifest" / "LATEST"), str(snapshot_dir / "LATEST"))
    assert (snapshot_dir / "LATEST").stat().st_ino == original_inode

    s.write_bytes("manifest/LATEST", b"2")
    assert (tmp_path / "manifest" / "LATEST").read_bytes() == b"2"
    # the snapshot's copy, sharing the old inode, is untouched
    assert (snapshot_dir / "LATEST").read_bytes() == b"1"


def test_local_store_list(tmp_path):
    s = store_mod.LocalStore(str(tmp_path))
    s.write_bytes("delta/g1/hour/1/node.byid.parquet", b"x")
    s.write_bytes("delta/g1/hour/1/way.byid.parquet", b"y")
    s.write_bytes("manifest/LATEST", b"1")
    assert s.list("delta/g1/hour/1") == [
        "delta/g1/hour/1/node.byid.parquet",
        "delta/g1/hour/1/way.byid.parquet",
    ]
    assert s.list("manifest/LATEST") == ["manifest/LATEST"]
    assert s.list("nope") == []


def test_local_store_upload_file_is_atomic(tmp_path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"tier data")
    s = store_mod.LocalStore(str(tmp_path / "root"))
    s.upload_file(str(src), "delta/g1/hour/1/node.byid.parquet")
    assert s.read_bytes("delta/g1/hour/1/node.byid.parquet") == b"tier data"


def test_local_store_url_is_a_plain_path(tmp_path):
    s = store_mod.LocalStore(str(tmp_path))
    assert s.url("manifest/1.json") == str(tmp_path / "manifest" / "1.json")


# --------------------------------------------------------------------------
# MemoryStore
# --------------------------------------------------------------------------


def test_memory_store_round_trip():
    s = store_mod.MemoryStore()
    assert not s.exists("x")
    s.write_bytes("x", b"abc")
    assert s.exists("x")
    assert s.read_bytes("x") == b"abc"
    assert s.list("") == ["x"]
    s.delete("x")
    assert not s.exists("x")
    with pytest.raises(FileNotFoundError):
        s.read_bytes("x")


def test_memory_store_upload_file(tmp_path):
    src = tmp_path / "f.bin"
    src.write_bytes(b"payload")
    s = store_mod.MemoryStore()
    s.upload_file(str(src), "some/path.parquet")
    assert s.read_bytes("some/path.parquet") == b"payload"


def test_memory_store_has_no_duckdb_url():
    s = store_mod.MemoryStore()
    with pytest.raises(NotImplementedError):
        s.url("x")


# --------------------------------------------------------------------------
# for_root
# --------------------------------------------------------------------------


def test_for_root_local(tmp_path):
    s = store_mod.for_root(str(tmp_path))
    assert isinstance(s, store_mod.LocalStore)


def test_for_root_s3(monkeypatch):
    monkeypatch.setenv("OSMPQ_S3_KEY_ID", "k")
    monkeypatch.setenv("OSMPQ_S3_SECRET", "s")
    monkeypatch.setenv("OSMPQ_S3_ENDPOINT", "example.r2.cloudflarestorage.com")
    s = store_mod.for_root("s3://my-bucket/some/prefix")
    assert isinstance(s, store_mod.S3Store)
    assert s.bucket == "my-bucket"
    assert s.prefix == "some/prefix"


def test_is_remote_root():
    assert store_mod.is_remote_root("s3://bucket/prefix")
    assert not store_mod.is_remote_root("/local/path")
    assert not store_mod.is_remote_root("relative/path")


# --------------------------------------------------------------------------
# s3_secret_sql
# --------------------------------------------------------------------------


def test_s3_secret_sql_none_when_vars_absent():
    assert store_mod.s3_secret_sql({}) is None


def test_s3_secret_sql_built_from_env():
    env = {
        "OSMPQ_S3_KEY_ID": "AKIA...",
        "OSMPQ_S3_SECRET": "s3cr3t",
        "OSMPQ_S3_ENDPOINT": "abc123.r2.cloudflarestorage.com",
    }
    sql = store_mod.s3_secret_sql(env)
    assert sql is not None
    assert "KEY_ID 'AKIA...'" in sql
    assert "SECRET 's3cr3t'" in sql
    assert "ENDPOINT 'abc123.r2.cloudflarestorage.com'" in sql
    assert "REGION 'auto'" in sql
    assert "URL_STYLE 'path'" in sql
    assert "USE_SSL true" in sql


# --------------------------------------------------------------------------
# S3Store, via a stubbed boto3 client -- every Store method
# --------------------------------------------------------------------------


def _stubbed_store(bucket: str = "my-bucket", prefix: str = "root") -> tuple:
    client = boto3.client("s3", region_name="auto", endpoint_url="https://example.invalid")
    stubber = Stubber(client)
    store = store_mod.S3Store(bucket=bucket, prefix=prefix, client=client)
    return store, stubber


def test_s3_store_read_bytes():
    store, stubber = _stubbed_store()
    stubber.add_response(
        "get_object",
        {"Body": _StreamBody(b"manifest json")},
        {"Bucket": "my-bucket", "Key": "root/manifest/1.json"},
    )
    with stubber:
        assert store.read_bytes("manifest/1.json") == b"manifest json"


def test_s3_store_write_bytes():
    store, stubber = _stubbed_store()
    stubber.add_response(
        "put_object", {},
        {"Bucket": "my-bucket", "Key": "root/manifest/LATEST", "Body": b"2"},
    )
    with stubber:
        store.write_bytes("manifest/LATEST", b"2")
    stubber.assert_no_pending_responses()


def test_s3_store_exists_true_and_false():
    store, stubber = _stubbed_store()
    stubber.add_response("head_object", {}, {"Bucket": "my-bucket", "Key": "root/manifest/LATEST"})

    stubber.add_client_error(
        "head_object", service_error_code="404", http_status_code=404,
        expected_params={"Bucket": "my-bucket", "Key": "root/missing"},
    )
    with stubber:
        assert store.exists("manifest/LATEST") is True
        assert store.exists("missing") is False


def test_s3_store_exists_reraises_other_errors():
    store, stubber = _stubbed_store()
    stubber.add_client_error(
        "head_object", service_error_code="403", http_status_code=403,
        expected_params={"Bucket": "my-bucket", "Key": "root/forbidden"},
    )
    from botocore.exceptions import ClientError

    with stubber:
        with pytest.raises(ClientError):
            store.exists("forbidden")


def test_s3_store_list():
    store, stubber = _stubbed_store()
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [
            {"Key": "root/delta/g1/hour/1/node.byid.parquet"},
            {"Key": "root/delta/g1/hour/1/way.byid.parquet"},
        ]},
        {"Bucket": "my-bucket", "Prefix": "root/delta/g1/hour/1"},
    )
    with stubber:
        assert store.list("delta/g1/hour/1") == [
            "delta/g1/hour/1/node.byid.parquet",
            "delta/g1/hour/1/way.byid.parquet",
        ]


def test_s3_store_delete():
    store, stubber = _stubbed_store()
    stubber.add_response("delete_object", {}, {"Bucket": "my-bucket", "Key": "root/old.parquet"})
    with stubber:
        store.delete("old.parquet")
    stubber.assert_no_pending_responses()


def test_s3_store_upload_file(tmp_path):
    store, stubber = _stubbed_store()
    src = tmp_path / "tier.parquet"
    src.write_bytes(b"some parquet bytes")
    stubber.add_response("put_object", {})
    with stubber:
        store.upload_file(str(src), "delta/g1/hour/1/node.byid.parquet")
    stubber.assert_no_pending_responses()


def test_s3_store_url():
    store, _ = _stubbed_store(bucket="my-bucket", prefix="root")
    assert store.url("manifest/1.json") == "s3://my-bucket/root/manifest/1.json"
    store_no_prefix, _ = _stubbed_store(bucket="my-bucket", prefix="")
    assert store_no_prefix.url("manifest/1.json") == "s3://my-bucket/manifest/1.json"


class _StreamBody:
    """Minimal stand-in for botocore's `StreamingBody` (has `.read()`),
    good enough for `Stubber`-mocked `get_object` responses."""

    def __init__(self, data: bytes):
        self._data = data

    def read(self, *args, **kwargs) -> bytes:
        return self._data
