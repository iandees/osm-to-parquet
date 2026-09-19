"""Object store abstraction, contract section 6.2: ``LocalStore``,
``S3Store``, ``MemoryStore`` and ``for_root``.

Two different jobs live behind ``Store``:

- Whole-file byte read/write/list/delete/upload, done in Python (manifest
  JSON, uploading a locally-written tier Parquet file, checking whether a
  delta file exists).
- ``url(rel)``: the string DuckDB itself should read/write a Parquet file
  at -- a local filesystem path for ``LocalStore``, or an ``s3://...`` URL
  for ``S3Store`` (DuckDB's own ``httpfs`` does the actual GET/range-reads
  there, once the right secret is installed on the connection -- see
  ``s3_secret_sql`` below, shared with ``osmpq.engine.executor.Engine``).

``MemoryStore`` has no meaningful ``url()`` (DuckDB has nothing to read
in-process Python bytes from), so it raises; tests that need DuckDB to
read a store's files use ``LocalStore`` instead (docs/m3-contracts.md
section 6.2: "if the MemoryStore cannot be read by DuckDB, run the e2e
through LocalStore").
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Protocol, runtime_checkable


def is_remote_root(root: str) -> bool:
    return root.startswith("s3://")


def s3_secret_sql(env: Optional[dict] = None) -> Optional[str]:
    """The ``CREATE SECRET`` SQL for an ``s3://`` root, built from the same
    ``OSMPQ_S3_*`` environment variables ``osmpq.engine.executor.Engine``
    uses (see its module docstring) -- factored out here so both the query
    engine and the updater (which needs its own DuckDB connection to read
    ``store.url(...)`` Parquet files) issue exactly the same secret. None
    when the required variables (``OSMPQ_S3_KEY_ID``/``OSMPQ_S3_SECRET``/
    ``OSMPQ_S3_ENDPOINT``) aren't all set -- meaning "do nothing, let
    DuckDB's own credential chain apply".

    A pure function (takes ``env`` instead of reading ``os.environ``
    directly) so tests can assert on the exact SQL without a real bucket or
    DuckDB connection, and without mutating process environment.
    """
    env = os.environ if env is None else env
    key_id = env.get("OSMPQ_S3_KEY_ID")
    secret = env.get("OSMPQ_S3_SECRET")
    endpoint = env.get("OSMPQ_S3_ENDPOINT")
    if not (key_id and secret and endpoint):
        return None
    region = env.get("OSMPQ_S3_REGION", "auto")
    url_style = env.get("OSMPQ_S3_URL_STYLE", "path")
    use_ssl_raw = str(env.get("OSMPQ_S3_USE_SSL", "true")).strip().lower()
    use_ssl = "false" if use_ssl_raw in ("0", "false", "no") else "true"

    def esc(s: str) -> str:
        return s.replace("'", "''")

    return (
        "CREATE OR REPLACE SECRET osmpq_s3 (\n"
        "    TYPE S3,\n"
        f"    KEY_ID '{esc(key_id)}',\n"
        f"    SECRET '{esc(secret)}',\n"
        f"    ENDPOINT '{esc(endpoint)}',\n"
        f"    REGION '{esc(region)}',\n"
        f"    URL_STYLE '{esc(url_style)}',\n"
        f"    USE_SSL {use_ssl}\n"
        ")"
    )


@runtime_checkable
class Store(Protocol):
    def read_bytes(self, rel: str) -> bytes: ...
    def write_bytes(self, rel: str, data: bytes) -> None: ...
    def exists(self, rel: str) -> bool: ...
    def list(self, prefix: str) -> list[str]: ...
    def delete(self, rel: str) -> None: ...
    def upload_file(self, local_path: str, rel: str) -> None: ...
    def url(self, rel: str) -> str: ...


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` without mutating whatever inode ``path``
    currently names -- the same temp-file + ``os.replace`` pattern
    ``layout.manifest._atomic_write_text`` already used (see its
    docstring): dataset roots are routinely duplicated with ``cp -al``
    (hardlinked snapshots), and truncating a shared inode in place would
    silently corrupt every other snapshot sharing it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp_name, str(path))
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


class LocalStore:
    """A local directory. Reads/writes are plain filesystem I/O;
    ``write_bytes`` uses the same atomic temp-file + ``os.replace`` pattern
    the pre-M3 local manifest writer used, so ``manifest/LATEST`` (shared
    across ``cp -al`` snapshots) is still never truncated in place."""

    def __init__(self, root_dir: str):
        self.root_dir = Path(root_dir)

    def _path(self, rel: str) -> Path:
        return self.root_dir / rel.lstrip("/")

    def read_bytes(self, rel: str) -> bytes:
        return self._path(rel).read_bytes()

    def write_bytes(self, rel: str, data: bytes) -> None:
        _atomic_write_bytes(self._path(rel), data)

    def exists(self, rel: str) -> bool:
        return self._path(rel).exists()

    def list(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        out: list[str] = []
        if base.is_dir():
            for p in base.rglob("*"):
                if p.is_file():
                    out.append(str(p.relative_to(self.root_dir)).replace(os.sep, "/"))
        elif base.exists():
            out.append(str(base.relative_to(self.root_dir)).replace(os.sep, "/"))
        return sorted(out)

    def delete(self, rel: str) -> None:
        p = self._path(rel)
        if p.exists():
            p.unlink()

    def upload_file(self, local_path: str, rel: str) -> None:
        """Copy ``local_path`` into place at ``rel``, atomically (temp file
        + ``os.replace``) so a reader never observes a partially-written
        file. A plain copy, not a move: callers (the updater's local
        staging path) may still want their own copy of ``local_path``."""
        dest = self._path(rel)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if os.path.abspath(local_path) == os.path.abspath(dest):
            return
        fd, tmp_name = tempfile.mkstemp(dir=str(dest.parent), prefix=f".{dest.name}.", suffix=".tmp")
        os.close(fd)
        try:
            shutil.copyfile(local_path, tmp_name)
            os.replace(tmp_name, str(dest))
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def url(self, rel: str) -> str:
        return str(self._path(rel))


class MemoryStore:
    """In-process byte store for tests. No `url()` -- DuckDB has nothing to
    read Python-process bytes from -- see the module docstring."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}

    def _key(self, rel: str) -> str:
        return rel.lstrip("/")

    def read_bytes(self, rel: str) -> bytes:
        try:
            return self._data[self._key(rel)]
        except KeyError:
            raise FileNotFoundError(rel) from None

    def write_bytes(self, rel: str, data: bytes) -> None:
        self._data[self._key(rel)] = bytes(data)

    def exists(self, rel: str) -> bool:
        return self._key(rel) in self._data

    def list(self, prefix: str) -> list[str]:
        p = self._key(prefix)
        return sorted(k for k in self._data if k.startswith(p))

    def delete(self, rel: str) -> None:
        self._data.pop(self._key(rel), None)

    def upload_file(self, local_path: str, rel: str) -> None:
        with open(local_path, "rb") as f:
            self.write_bytes(rel, f.read())

    def url(self, rel: str) -> str:
        raise NotImplementedError(
            "MemoryStore has no DuckDB-readable URL; use LocalStore for tests that need DuckDB to read the store"
        )


def _s3_client_from_env(env: Optional[dict] = None):
    import boto3
    from botocore.config import Config

    env = os.environ if env is None else env
    key_id = env.get("OSMPQ_S3_KEY_ID")
    secret = env.get("OSMPQ_S3_SECRET")
    endpoint = env.get("OSMPQ_S3_ENDPOINT")
    region = env.get("OSMPQ_S3_REGION", "auto")
    url_style = env.get("OSMPQ_S3_URL_STYLE", "path")
    use_ssl_raw = str(env.get("OSMPQ_S3_USE_SSL", "true")).strip().lower()
    use_ssl = use_ssl_raw not in ("0", "false", "no")

    kwargs: dict = {"region_name": region}
    if endpoint:
        scheme = "https" if use_ssl else "http"
        kwargs["endpoint_url"] = f"{scheme}://{endpoint}"
    if key_id and secret:
        kwargs["aws_access_key_id"] = key_id
        kwargs["aws_secret_access_key"] = secret
    kwargs["config"] = Config(s3={"addressing_style": "path" if url_style == "path" else "virtual"})
    return boto3.client("s3", **kwargs)


class S3Store:
    """An ``s3://bucket/prefix`` root (also R2, or anything else S3-API
    compatible), contract section 6.2: a boto3 client built from the same
    ``OSMPQ_S3_*`` environment variables the query engine's
    ``s3_secret_sql`` uses, with ``endpoint_url = https://<OSMPQ_S3_ENDPOINT>``.
    """

    def __init__(self, bucket: str, prefix: str = "", client=None):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = client if client is not None else _s3_client_from_env()

    def _key(self, rel: str) -> str:
        rel = rel.lstrip("/")
        return f"{self.prefix}/{rel}" if self.prefix else rel

    def read_bytes(self, rel: str) -> bytes:
        resp = self._client.get_object(Bucket=self.bucket, Key=self._key(rel))
        return resp["Body"].read()

    def write_bytes(self, rel: str, data: bytes) -> None:
        self._client.put_object(Bucket=self.bucket, Key=self._key(rel), Body=data)

    def exists(self, rel: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._client.head_object(Bucket=self.bucket, Key=self._key(rel))
            return True
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def list(self, prefix: str) -> list[str]:
        key_prefix = self._key(prefix)
        out: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                rel = key[len(self.prefix) + 1 :] if self.prefix else key
                out.append(rel)
        return sorted(out)

    def delete(self, rel: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=self._key(rel))

    def upload_file(self, local_path: str, rel: str) -> None:
        self._client.upload_file(local_path, self.bucket, self._key(rel))

    def url(self, rel: str) -> str:
        return f"s3://{self.bucket}/{self._key(rel)}"


def for_root(root: str) -> Store:
    """``LocalStore`` for a local directory, ``S3Store`` for an ``s3://``
    root."""
    if is_remote_root(root):
        without_scheme = root[len("s3://") :]
        bucket, _, prefix = without_scheme.partition("/")
        return S3Store(bucket=bucket, prefix=prefix)
    return LocalStore(root)
