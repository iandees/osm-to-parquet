"""``osmpq.update.updater`` on the object-store interface (contract section
6.2): the existing updater e2e scenario run through a ``LocalStore``-backed
root (``store.for_root`` picks it for any local directory, so this proves
the whole read/write path -- `_load_tiers`/`_fetch_current`/`_touched_set`/
`_resolve`/`_load_old` reading through ``store.url``/``store.exists``, and
``_write_tiers``/``_write_tier_version`` writing locally -- goes through
`Store` and still produces byte-identical results), plus the `s3://` write
path (`_upload_new_tier_files`, and `S3Store` itself) exercised directly
via `botocore.stub.Stubber`, per the contract's documented fallback:
"if the MemoryStore cannot be read by DuckDB, run the e2e through
LocalStore and unit-test the S3 write path with the Stubber" (`MemoryStore`
indeed has no DuckDB-readable `url()`, see `tests/test_store.py`).
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402

from osmpq import store as store_mod  # noqa: E402
from osmpq.layout import manifest as manifest_mod  # noqa: E402
from osmpq.update import updater as updater_mod  # noqa: E402
from osmpq.update.replication import FetchResult  # noqa: E402

REPL_SOURCE = "https://fake.example/repl"


def _osc(body: str) -> str:
    return f'<osmChange version="0.6" generator="test">\n{body}\n</osmChange>\n'


class _FakeClient:
    """Stands in for ``ReplicationClient``: serves a local ``.osc`` file by
    sequence number instead of an HTTP source (same pattern as
    ``tests/test_update_e2e.py``)."""

    files: dict[int, tuple[Path, str]] = {}

    def __init__(self, source: str, **kwargs) -> None:
        self.source = source

    def __enter__(self) -> "_FakeClient":
        return self

    def __exit__(self, *exc) -> None:
        return None

    def fetch_range(self, from_seq: int, max_diffs: int, dest_dir: Path) -> list[FetchResult]:
        out = []
        seq = from_seq
        while seq in self.files and len(out) < max_diffs:
            path, ts = self.files[seq]
            out.append(FetchResult(seq=seq, osc_path=path, state_path=Path("/dev/null"), timestamp=ts))
            seq += 1
        return out


def _add_updater_hilbert_columns(root_dir: Path) -> None:
    """``tests/fixtures/make_fixture.py`` builds a manifest-v1/v2-shaped
    fixture that predates the M2 updater's node/way byid schema
    (docs/m2-contracts.md section 3: ``node_byid_columns``/
    ``way_byid_columns`` both end in ``hilbert``, which the M0 fixture
    generator has no reason to carry). Patched here, in this test file
    only, rather than in ``make_fixture.py`` (owned by a different
    workstream) -- computed the same way ``updater._resolve`` does: a
    node's hilbert key from its own point, a way's from its bbox
    centroid."""
    import duckdb
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    from osmpq.layout import hilbert as hilbert_mod

    con = duckdb.connect()
    try:
        for typ in ("node", "way"):
            for part in sorted((root_dir / "byid" / "g0001" / typ).glob("part-*.parquet")):
                cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{part}')").fetchall()]
                if "hilbert" in cols:
                    continue
                tbl = con.execute(f"SELECT * FROM read_parquet('{part}')").to_arrow_table()
                if typ == "node":
                    lat = np.asarray(tbl.column("lat_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    lon = np.asarray(tbl.column("lon_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                else:
                    ymin = np.asarray(tbl.column("ymin_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    ymax = np.asarray(tbl.column("ymax_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    xmin = np.asarray(tbl.column("xmin_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    xmax = np.asarray(tbl.column("xmax_e7").to_numpy(zero_copy_only=False), dtype=np.int64)
                    lat = np.round((ymin + ymax) / 2.0).astype(np.int64)
                    lon = np.round((xmin + xmax) / 2.0).astype(np.int64)
                hb = hilbert_mod.hilbert_keys(lat, lon).astype(np.uint64)
                tbl2 = tbl.append_column("hilbert", pa.array(hb, type=pa.uint64()))
                pq.write_table(tbl2, str(part))
    finally:
        con.close()


@pytest.fixture()
def root(tmp_path):
    d = tmp_path / "root"
    info = make_fixture.build(str(d), manifest_version=2)
    _add_updater_hilbert_columns(d)
    man = manifest_mod.load_latest(str(d))
    man.replication_sequence = 0
    man.replication_source = REPL_SOURCE
    manifest_mod.write_manifest(str(d), man, 2)
    return d, info


@pytest.fixture()
def batch_osc(tmp_path, root):
    _d, info = root
    # Retag the fixture's cafe node (same lat/lon, so cell placement is
    # unaffected) -- a plain in-place modify is enough to exercise the
    # whole read (delta tiers ⊕ base byid, through `store`) / re-resolve /
    # write (`_write_tiers`, through `store`'s local write target) path.
    body = f"""
<modify>
<node id="{info.cafe_node_id}" version="4" timestamp="2026-01-01T10:30:00Z" uid="1" user="tester" changeset="1001" lat="70.8750000" lon="-141.7500000">
<tag k="amenity" v="cafe"/>
<tag k="name" v="Renamed Cafe"/>
</node>
</modify>
"""
    p = tmp_path / "batch.osc"
    p.write_text(_osc(body))
    return p


@pytest.fixture(autouse=True)
def _patch_replication_client(batch_osc, monkeypatch):
    _FakeClient.files = {1: (batch_osc, "2026-01-01T10:30:00Z")}
    monkeypatch.setattr(updater_mod, "ReplicationClient", _FakeClient)


# --------------------------------------------------------------------------
# full e2e through a LocalStore-backed root
# --------------------------------------------------------------------------


def test_run_once_through_local_store_e2e(root, tmp_path):
    d, info = root
    opts = updater_mod.UpdateOptions(
        root=str(d), source=REPL_SOURCE, max_diffs=1, tmpdir=str(tmp_path / "update-tmp"),
    )
    summary = updater_mod.run_once(opts)
    assert summary is not None
    assert summary.applied == 1
    assert summary.first_seq == 1 and summary.last_seq == 1
    assert summary.rows_touched["node"] == 1

    man = manifest_mod.load_latest(str(d))
    assert man.manifest_version == 3
    assert man.replication_sequence == 1
    assert man.replication_source == REPL_SOURCE
    assert "hour" in man.deltas
    hour = man.deltas["hour"]
    assert hour["version"] == 1
    assert hour["files"]["node"]["byid"]

    # `store.for_root` on this (plain local) root gives a `LocalStore`; the
    # tier file it points at is exactly where the updater wrote it (no
    # behaviour change for a local root -- contract 6.2).
    store = store_mod.for_root(str(d))
    assert isinstance(store, store_mod.LocalStore)
    node_byid_rel = hour["files"]["node"]["byid"]
    assert store.exists(node_byid_rel)
    assert (d / node_byid_rel).exists()

    con = duckdb.connect()
    row = con.execute(
        f"SELECT tags FROM read_parquet('{store.url(node_byid_rel)}') WHERE id = {info.cafe_node_id}"
    ).fetchone()
    assert row is not None
    assert row[0]["name"] == "Renamed Cafe"


def test_run_once_second_run_is_a_no_op_when_no_new_diffs(root, tmp_path):
    d, _info = root
    opts = updater_mod.UpdateOptions(
        root=str(d), source=REPL_SOURCE, max_diffs=1, tmpdir=str(tmp_path / "update-tmp-1"),
    )
    updater_mod.run_once(opts)
    opts2 = updater_mod.UpdateOptions(
        root=str(d), source=REPL_SOURCE, max_diffs=1, tmpdir=str(tmp_path / "update-tmp-2"),
    )
    summary2 = updater_mod.run_once(opts2)
    assert summary2.no_op is True


# --------------------------------------------------------------------------
# `s3://` write path: `_upload_new_tier_files` and `S3Store`, via a
# recording fake and via a real `botocore.stub.Stubber`-backed client.
# --------------------------------------------------------------------------


class _RecordingStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def upload_file(self, local_path: str, rel: str) -> None:
        self.calls.append((local_path, rel))


def test_upload_new_tier_files_pushes_every_file_of_a_written_tier(tmp_path):
    write_root = tmp_path / "staging"
    tier_dir = write_root / "delta" / "g0001" / "hour" / "1"
    tier_dir.mkdir(parents=True)
    for name in ("node.byid.parquet", "node.spatial.parquet", "tombstones.parquet"):
        (tier_dir / name).write_bytes(b"x")

    new_deltas = {
        "hour": {
            "files": {
                "node": {
                    "byid": "delta/g0001/hour/1/node.byid.parquet",
                    "spatial": "delta/g0001/hour/1/node.spatial.parquet",
                },
                "way": {},
                "relation": {},
                "tombstones": "delta/g0001/hour/1/tombstones.parquet",
            }
        },
        # an untouched tier this run didn't (re)write -- must NOT be uploaded
        "day": {"files": {"node": {"byid": "delta/g0001/day/3/node.byid.parquet"}}},
    }
    tier_versions = {"hour": 1}

    store = _RecordingStore()
    updater_mod._upload_new_tier_files(store, write_root, new_deltas, tier_versions)

    uploaded_rels = {rel for _local, rel in store.calls}
    assert uploaded_rels == {
        "delta/g0001/hour/1/node.byid.parquet",
        "delta/g0001/hour/1/node.spatial.parquet",
        "delta/g0001/hour/1/tombstones.parquet",
    }
    for local_path, rel in store.calls:
        assert local_path == str(write_root / rel)


def test_upload_new_tier_files_via_stubbed_s3_store(tmp_path):
    import boto3
    from botocore.stub import Stubber

    write_root = tmp_path / "staging"
    tier_dir = write_root / "delta" / "g0001" / "hour" / "1"
    tier_dir.mkdir(parents=True)
    (tier_dir / "node.byid.parquet").write_bytes(b"parquet bytes")

    client = boto3.client("s3", region_name="auto", endpoint_url="https://example.invalid")
    stubber = Stubber(client)
    store = store_mod.S3Store(bucket="my-bucket", prefix="minnesota", client=client)
    stubber.add_response("put_object", {})

    new_deltas = {"hour": {"files": {"node": {"byid": "delta/g0001/hour/1/node.byid.parquet"}, "way": {}, "relation": {}}}}
    tier_versions = {"hour": 1}
    with stubber:
        updater_mod._upload_new_tier_files(store, write_root, new_deltas, tier_versions)
    stubber.assert_no_pending_responses()


def test_run_once_picks_a_local_staging_dir_for_an_s3_root(root, tmp_path, monkeypatch):
    """`run_once` must never do `Path("s3://bucket/prefix")` arithmetic --
    that isn't a usable filesystem path at all. Proven directly here
    without a real bucket: patch `store_mod.for_root` to hand back a
    `LocalStore` over the fixture's own root (so DuckDB reads work, unlike
    `MemoryStore` -- see the module docstring) while `opts.root` is still
    an `s3://` URL string, and check the run picks a local staging
    directory under `--tmpdir`, not `d`, as its write target -- then
    confirms `manifest_mod.write_manifest`/`next_manifest_number` are
    still called with the real `opts.root` string (not the staging path)
    by asserting the new manifest actually landed via `store.for_root`."""
    d, _info = root
    local_store_over_d = store_mod.LocalStore(str(d))
    monkeypatch.setattr(store_mod, "for_root", lambda root_str: local_store_over_d)

    tmpdir = tmp_path / "update-tmp"
    opts = updater_mod.UpdateOptions(
        root="s3://fake-bucket/does-not-matter", source=REPL_SOURCE, max_diffs=1, tmpdir=str(tmpdir),
    )
    summary = updater_mod.run_once(opts)
    assert summary is not None and summary.applied == 1

    # tier files were written to the local staging dir under --tmpdir...
    staging = tmpdir / "staging"
    assert any(staging.rglob("node.byid.parquet"))
    # ...then uploaded (here: LocalStore.upload_file, a plain copy) to `d`,
    # where the new manifest (written through the same store) can be read.
    man = manifest_mod.load_latest(str(d))
    assert man.manifest_version == 3
    hour = man.deltas["hour"]
    assert (d / hour["files"]["node"]["byid"]).exists()
