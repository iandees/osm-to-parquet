"""``osmpq gc`` test on the Bermuda extract, per docs/m2-contracts.md
section 6: after compaction, ``gc --keep 1`` removes the old generation's
touched files and the delta files but nothing the newest manifest still
references.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fixtures import deltas as deltas_fixture  # noqa: E402

from osmpq.build.builder import BuildOptions, build  # noqa: E402
from osmpq.build.compact import CompactOptions, compact  # noqa: E402
from osmpq.build.gc import gc  # noqa: E402
from osmpq.build.validate import validate  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
PBF_PATH = REPO_ROOT / "data" / "bermuda-latest.osm.pbf"

pytestmark = pytest.mark.skipif(not PBF_PATH.exists(), reason="data/bermuda-latest.osm.pbf not present")


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("INSTALL spatial")
    c.execute("LOAD spatial")
    return c


@pytest.fixture(scope="module")
def compacted_root(tmp_path_factory, con) -> Path:
    root = tmp_path_factory.mktemp("bermuda-gc-root")
    build(BuildOptions(
        pbf_path=str(PBF_PATH), root=str(root), max_nodes_per_cell=20_000,
        tmpdir=str(tmp_path_factory.mktemp("bermuda-gc-buildtmp")),
    ))
    node_ids = deltas_fixture.sample_ids(str(root), "node", 3, con=con)
    way_ids = deltas_fixture.sample_ids(str(root), "way", 2, con=con)
    tiers = {
        "week": [("node", node_ids[0], {"tags": {"amenity": "gc-test"}})],
        "day": [("way", way_ids[0], "delete")],
    }
    deltas_fixture.write_deltas(str(root), tiers)
    compact(CompactOptions(root=str(root), tmpdir=str(tmp_path_factory.mktemp("bermuda-gc-compacttmp"))))
    return root


def _referenced_files(root: Path, manifest_number: int) -> set[str]:
    man = json.loads((root / "manifest" / f"{manifest_number}.json").read_text())
    out: set[str] = set()
    for spec in man.get("tables", {}).values():
        for entry in spec.get("cells", {}).values():
            if "path" in entry:
                out.add(entry["path"])
            else:
                for part in ("tagged", "untagged"):
                    if entry.get(part):
                        out.add(entry[part]["path"])
    for parts in man.get("byid", {}).values():
        for p in parts:
            out.add(p["path"])
    for parts in man.get("index", {}).values():
        for p in parts:
            out.add(p["path"])
    for p in man.get("rowgroup_index", {}).values():
        out.add(p)
    return out


def test_gc_dry_run_lists_delta_files_and_old_generation(compacted_root):
    latest = int((compacted_root / "manifest" / "LATEST").read_text().strip())
    assert latest == 3  # 1: build, 2: deltas.write_deltas, 3: compact
    result = gc(str(compacted_root), keep=1, dry_run=True)
    removed = set(result["removed_files"])
    assert any(p.startswith("delta/") for p in removed), "delta files should be listed for removal"
    assert any("g0001" in p for p in removed), "old generation's own files should be listed for removal"
    assert "manifest/1.json" in result["removed_manifests"]
    assert "manifest/2.json" in result["removed_manifests"]
    # a dry run must not actually touch the filesystem
    assert (compacted_root / "manifest" / "1.json").exists()
    assert (compacted_root / "manifest" / "2.json").exists()


def test_gc_keep_1_removes_unreferenced_and_keeps_referenced(compacted_root):
    latest_num = int((compacted_root / "manifest" / "LATEST").read_text().strip())
    kept_referenced = _referenced_files(compacted_root, latest_num)

    result = gc(str(compacted_root), keep=1, dry_run=False)
    assert result["removed_files"], "expected some files to be removed"
    assert set(result["removed_files"]).isdisjoint(kept_referenced), (
        "gc must never remove a file the kept manifest still references"
    )

    # every file the newest manifest references must still be present
    for rel in kept_referenced:
        assert (compacted_root / rel).exists(), rel

    # old manifests gone, LATEST manifest kept
    assert not (compacted_root / "manifest" / "1.json").exists()
    assert not (compacted_root / "manifest" / "2.json").exists()
    assert (compacted_root / "manifest" / f"{latest_num}.json").exists()
    assert int((compacted_root / "manifest" / "LATEST").read_text().strip()) == latest_num

    # delta files (never referenced by manifest 3, whose deltas == {}) are gone
    remaining = [str(p.relative_to(compacted_root)) for p in compacted_root.rglob("*") if p.is_file()]
    assert not any(p.startswith("delta/") for p in remaining)

    # nothing under the old generation's own private directories remains,
    # except whatever got hardlinked into the kept generation (which lives
    # under the *new* generation's path and was asserted present above)
    assert not any("g0001" in p for p in remaining)
    assert not any("g0002" in p for p in remaining)


def test_validate_still_passes_after_gc(compacted_root):
    ok, summary = validate(str(compacted_root))
    assert ok, "\n".join(summary)
