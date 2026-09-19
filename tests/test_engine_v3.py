"""M2 engine changes (docs/m2-contracts.md sections 3-4): manifest v3's
`deltas`/`replication_source`, and the base ⊕ delta read path for spatial
scans, by-id lookups, and the `>`/`<`/`>>` recursion/hydration paths.

test_engine_v2.py's v1/v2 tests (and the whole existing suite) stay green
unchanged -- this file only adds what M2 changes. See tests/fixtures/
make_fixture.py's `manifest_version=3` mode (docs/m2-contracts.md section 3)
for the exact delta scenario this file exercises.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from osmpq.engine import Engine, catalog, sources
from osmpq.engine.schema import project

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import make_fixture  # noqa: E402


def bbox_args(bbox):
    s, w, n, e = bbox
    return f"{s},{w},{n},{e}"


def ids_of(elements, type_=None):
    return sorted(e["id"] for e in elements if type_ is None or e["type"] == type_)


# --------------------------------------------------------------------------
# catalog.Manifest.delta_tiers() / replication_source: pure unit tests, no
# fixture needed.
# --------------------------------------------------------------------------


def _hand_built_v3_manifest(deltas: dict) -> catalog.Manifest:
    data = {
        "manifest_version": 3,
        "generation": "g0001",
        "leaf_cells": ["000", "001"],
        "replication_source": "https://example.org/replication/minute",
        "replication_sequence": 42,
        "tables": {"node": {"cells": {}}, "way": {"cells": {}}, "relation": {"cells": {}}},
        "byid": {"node": [], "way": [], "relation": []},
        "index": {"node_way": [], "member": []},
        "deltas": deltas,
    }
    return catalog.Manifest(root="/nonexistent/root", data=data)


def _tier_files(n=1):
    return {
        "node": {"spatial": f"delta/g0001/t/{n}/node.spatial.parquet", "byid": f"delta/g0001/t/{n}/node.byid.parquet"},
        "way": {"spatial": f"delta/g0001/t/{n}/way.spatial.parquet", "byid": f"delta/g0001/t/{n}/way.byid.parquet"},
        "relation": {
            "spatial": f"delta/g0001/t/{n}/relation.spatial.parquet",
            "byid": f"delta/g0001/t/{n}/relation.byid.parquet",
        },
        "tombstones": f"delta/g0001/t/{n}/tombstones.parquet",
    }


def test_delta_tiers_empty_for_v1_v2_manifests():
    for version in (1, 2):
        data = {
            "manifest_version": version,
            "generation": "g0001",
            "leaf_cells": [],
            "tables": {"node": {"cells": {}}, "way": {"cells": {}}, "relation": {"cells": {}}},
            "byid": {"node": [], "way": [], "relation": []},
            "index": {"node_way": [], "member": []},
        }
        manifest = catalog.Manifest(root="/nonexistent", data=data)
        assert manifest.delta_tiers() == []
        assert manifest.has_deltas() is False
        assert manifest.replication_source is None


def test_delta_tiers_absent_or_empty_v3_manifest_means_base_only():
    manifest = _hand_built_v3_manifest({})
    assert manifest.delta_tiers() == []
    assert manifest.has_deltas() is False
    data_no_key = dict(manifest.data)
    del data_no_key["deltas"]
    manifest2 = catalog.Manifest(root="/nonexistent", data=data_no_key)
    assert manifest2.delta_tiers() == []


def test_delta_tiers_precedence_order_hour_day_week():
    manifest = _hand_built_v3_manifest({
        "week": {"version": 5, "seq_from": 1, "seq_to": 10, "timestamp": "2026-09-01T00:00:00Z",
                  "rows": {"node": 1, "way": 0, "relation": 0}, "files": _tier_files(5)},
        "hour": {"version": 20, "seq_from": 90, "seq_to": 99, "timestamp": "2026-09-19T12:00:00Z",
                  "rows": {"node": 1, "way": 0, "relation": 0}, "files": _tier_files(20)},
        "day": {"version": 8, "seq_from": 11, "seq_to": 89, "timestamp": "2026-09-19T00:00:00Z",
                 "rows": {"node": 1, "way": 0, "relation": 0}, "files": _tier_files(8)},
    })
    tiers = manifest.delta_tiers()
    assert [t["name"] for t in tiers] == ["hour", "day", "week"]
    assert [t["rank"] for t in tiers] == [3, 2, 1]
    hour = tiers[0]
    assert hour["version"] == 20
    assert hour["seq_from"] == 90 and hour["seq_to"] == 99
    # Paths are resolved against the manifest root, same as every other
    # manifest-relative path (`manifest.path`).
    assert hour["files"]["node"]["spatial"] == manifest.path("delta/g0001/t/20/node.spatial.parquet")
    assert hour["tombstones"] == manifest.path("delta/g0001/t/20/tombstones.parquet")
    assert manifest.replication_source == "https://example.org/replication/minute"
    assert manifest.has_deltas() is True


def test_delta_tiers_exposes_cells_per_type():
    files_with_cells = _tier_files(7)
    manifest = _hand_built_v3_manifest({
        "week": {"version": 7, "seq_from": 1, "seq_to": 10, "timestamp": "2026-09-01T00:00:00Z",
                  "rows": {"node": 1, "way": 1, "relation": 0}, "files": files_with_cells,
                  "cells": {"node": ["000"], "way": ["000", "301"]}},
    })
    tiers = manifest.delta_tiers()
    assert tiers[0]["cells"] == {"node": ["000"], "way": ["000", "301"], "relation": []}


def test_delta_present_cells_unions_across_tiers():
    manifest = _hand_built_v3_manifest({
        "week": {"version": 1, "seq_from": 1, "seq_to": 5, "timestamp": "t",
                  "rows": {}, "files": _tier_files(1), "cells": {"way": ["301"]}},
        "day": {"version": 2, "seq_from": 6, "seq_to": 9, "timestamp": "t",
                 "rows": {}, "files": _tier_files(2), "cells": {"way": ["302", "301"]}},
    })
    assert catalog.delta_present_cells(manifest, "way") == {"301", "302"}
    assert catalog.delta_present_cells(manifest, "node") == set()


def test_cells_for_bbox_finds_delta_only_cell_with_no_declared_leaf():
    # "301" has no leaf declared beneath it anywhere in `leaf_cells` (unlike
    # a base ancestor cell, which is always reachable by descending from
    # one of its own real leaves) -- only `delta_present_cells` says it
    # exists, so `cells_for_bbox` must check it directly against its own
    # quadkey bbox instead of via the leaf-then-ancestor walk.
    data = {
        "manifest_version": 3, "generation": "g0001",
        "leaf_cells": ["00000", "00001"],  # nothing under "301" at all
        "ancestor_depths": [0, 3, 6, 9, 12], "max_depth": 13,
        "tables": {"node": {"cells": {}}, "way": {"cells": {}}, "relation": {"cells": {}}},
        "byid": {"node": [], "way": [], "relation": []},
        "index": {"node_way": [], "member": []},
        "deltas": {
            "week": {"version": 1, "seq_from": 1, "seq_to": 1, "timestamp": "t", "rows": {},
                      "files": _tier_files(1), "cells": {"way": ["301"]}},
        },
    }
    manifest = catalog.Manifest(root="/nonexistent", data=data)
    bbox = catalog.cell_bbox("301")
    # A bbox strictly inside "301", away from any edge shared with a
    # sibling cell.
    s, w, n, e = bbox
    inset = (s + (n - s) * 0.4, w + (e - w) * 0.4, s + (n - s) * 0.6, w + (e - w) * 0.6)
    assert catalog.cells_for_bbox(manifest, "way", inset) == ["301"]
    # A bbox far away (over the real leaves) must not spuriously match it.
    assert "301" not in catalog.cells_for_bbox(manifest, "way", catalog.cell_bbox("00000"))


def test_cells_for_bbox_unaffected_when_no_delta_tiers():
    # Exactly test_engine_v2.py's v1 manifest, unaffected by the delta
    # union (delta_present_cells is empty for a v1/v2 manifest).
    data = {
        "manifest_version": 1, "generation": "g0001", "leaf_cells": ["00000", "00001"],
        "tables": {"node": {"cells": {}}, "way": {"cells": {
            "root": {"path": "w-root.parquet"}, "0": {"path": "w-0.parquet"},
        }}, "relation": {"cells": {}}},
        "byid": {"node": [], "way": [], "relation": []},
        "index": {"node_way": [], "member": []},
    }
    manifest = catalog.Manifest(root="/nonexistent", data=data)
    assert catalog.cells_for_bbox(manifest, "way", catalog.cell_bbox("00000")) == ["0", "root"]


# --------------------------------------------------------------------------
# sources.current_rows / byid_current_rows: no-tier path is byte-identical
# to the pre-M2 SQL shape (docs/m2-contracts.md section 4's "no extra
# scans" requirement) -- a direct unit test of the shared helper, not
# routed through a whole query.
# --------------------------------------------------------------------------


def test_current_rows_with_no_delta_tiers_emits_plain_base_sql():
    manifest = _hand_built_v3_manifest({})  # v3 manifest, but deltas: {}
    import duckdb

    con = duckdb.connect()
    cols = {"type": "'node'", "id": "id", "cell": "cell"}
    sql, extra_files = sources.current_rows(con, manifest, "node", ["000"], ["a.parquet"], cols, "TRUE")
    expected = (
        f"SELECT {project(cols)}\n"
        f"FROM read_parquet(['a.parquet'], hive_partitioning=true, union_by_name=true)\n"
        f"WHERE TRUE"
    )
    assert sql == expected
    assert extra_files == 0
    # And when base_files is also empty: the pre-M2 callers' own
    # `if not files: return empty_set_sql()` early-return already handles
    # this before ever reaching current_rows in real call sites, but the
    # helper itself degrades the same way if called directly.
    sql2, extra2 = sources.current_rows(con, manifest, "node", ["000"], [], cols, "TRUE")
    from osmpq.engine.schema import empty_set_sql
    assert sql2 == empty_set_sql()
    assert extra2 == 0
    con.close()


def test_byid_current_rows_with_no_delta_tiers_emits_plain_base_sql():
    manifest = _hand_built_v3_manifest({})
    import duckdb

    con = duckdb.connect()
    cols = {"type": "'node'", "id": "id"}
    sql, extra_files = sources.byid_current_rows(con, manifest, "node", ["a.parquet"], cols, "id IN (1,2)", "TRUE")
    expected = (
        f"SELECT {project(cols)}\n"
        f"FROM read_parquet(['a.parquet'], union_by_name=true)\n"
        f"WHERE (id IN (1,2)) AND (TRUE)"
    )
    assert sql == expected
    assert extra_files == 0
    con.close()


# --------------------------------------------------------------------------
# Fixture-backed tests
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fixture_v3(tmp_path_factory):
    root = tmp_path_factory.mktemp("engine_v3_fixture")
    return make_fixture.build(str(root), manifest_version=3)


@pytest.fixture(scope="module")
def engine_v3(fixture_v3):
    return Engine(fixture_v3.root)


def test_v3_manifest_round_trips(fixture_v3):
    manifest = catalog.load_manifest(fixture_v3.root)
    assert manifest.manifest_version == 3
    assert manifest.replication_source == fixture_v3.manifest_replication_source
    tiers = manifest.delta_tiers()
    assert [t["name"] for t in tiers] == ["hour", "day", "week"]
    assert all(t["files"]["node"]["spatial"] for t in tiers)
    assert all(t["tombstones"] for t in tiers)


# ------------------------------------------------------------- tag modify


def test_day_wins_over_week_for_same_node_tags(engine_v3, fixture_v3):
    r = engine_v3.run(f"[out:json];node({fixture_v3.delta_modified_node_id});out;")
    assert len(r.elements) == 1
    assert r.elements[0]["tags"] == fixture_v3.delta_modified_node_day_tags
    assert r.elements[0]["tags"] != fixture_v3.delta_modified_node_week_tags


def test_day_wins_over_week_in_bbox_query(engine_v3, fixture_v3):
    b = bbox_args(fixture_v3.leaf_bbox["000"])
    r = engine_v3.run(f"[out:json];node[amenity=cafe]({b});out;")
    by_id = {e["id"]: e for e in r.elements}
    assert fixture_v3.delta_modified_node_id in by_id
    assert by_id[fixture_v3.delta_modified_node_id]["tags"] == fixture_v3.delta_modified_node_day_tags
    # And the node never appears twice (base row is shadowed, not merely
    # supplemented).
    assert ids_of(r.elements, "node").count(fixture_v3.delta_modified_node_id) == 1


# ------------------------------------------------------- move + tier order


def test_hour_tombstone_beats_week_payload_node_absent_everywhere(engine_v3, fixture_v3):
    old_b = bbox_args(fixture_v3.leaf_bbox[fixture_v3.delta_moved_deleted_node_from_cell])
    new_b = bbox_args(fixture_v3.leaf_bbox[fixture_v3.delta_moved_deleted_node_to_cell])
    r_old = engine_v3.run(f"[out:json];node({old_b});out ids;")
    r_new = engine_v3.run(f"[out:json];node({new_b});out ids;")
    assert fixture_v3.delta_moved_deleted_node_id not in ids_of(r_old.elements)
    assert fixture_v3.delta_moved_deleted_node_id not in ids_of(r_new.elements)
    r_byid = engine_v3.run(f"[out:json];node({fixture_v3.delta_moved_deleted_node_id});out;")
    assert r_byid.elements == []


def test_moved_node_visible_only_in_new_cell_never_twice(engine_v3, fixture_v3):
    old_b = bbox_args(fixture_v3.leaf_bbox[fixture_v3.delta_moved_only_node_from_cell])
    new_b = bbox_args(fixture_v3.leaf_bbox[fixture_v3.delta_moved_only_node_to_cell])
    r_old = engine_v3.run(f"[out:json];node({old_b});out ids;")
    r_new = engine_v3.run(f"[out:json];node({new_b});out ids;")
    node_id = fixture_v3.delta_moved_only_node_id
    assert node_id not in ids_of(r_old.elements)
    assert node_id in ids_of(r_new.elements)
    assert ids_of(r_new.elements).count(node_id) == 1

    # A single query spanning both cells sees it exactly once too.
    r_both = engine_v3.run(
        f"[out:json];(node({old_b});node({new_b}););out ids;"
    )
    assert ids_of(r_both.elements).count(node_id) == 1


# ---------------------------------------------------------------- deletion


def test_deleted_way_absent_from_byid_and_bbox(engine_v3, fixture_v3):
    r = engine_v3.run(f"[out:json];way({fixture_v3.delta_deleted_way_id});out;")
    assert r.elements == []
    b = bbox_args(fixture_v3.leaf_bbox[fixture_v3.delta_deleted_way_cell])
    r2 = engine_v3.run(f"[out:json];way({b});out ids;")
    assert fixture_v3.delta_deleted_way_id not in ids_of(r2.elements)


def test_deleted_way_nodes_still_returned_by_forward_from_other_way(engine_v3, fixture_v3):
    r = engine_v3.run(f"[out:json];way({fixture_v3.delta_deleted_way_surviving_partner_way_id});>;out ids;")
    ids = ids_of(r.elements, "node")
    assert fixture_v3.delta_deleted_way_shared_node_id in ids


# ------------------------------------------------------------------ create


def test_new_way_spanning_two_leaves_queryable_by_id(engine_v3, fixture_v3):
    r = engine_v3.run(f"[out:json];way({fixture_v3.delta_new_way_id});out geom;")
    assert len(r.elements) == 1
    el = r.elements[0]
    assert el["type"] == "way"
    assert el["nodes"] == fixture_v3.delta_new_way_refs
    assert "geometry" in el and len(el["geometry"]) == 2


def test_new_way_at_cell_with_no_base_file_found_by_bbox(engine_v3, fixture_v3):
    # docs/m2-contracts.md follow-up: the updater can place a brand-new
    # element at an allowed ancestor/leaf cell the base has never written
    # a file for; `deltas.<tier>.cells` is what lets a bbox query discover
    # it before the next compaction (cells_for_bbox/delta_present_cells).
    manifest = catalog.load_manifest(fixture_v3.root)
    assert fixture_v3.delta_new_way_no_base_cell not in manifest.table_cells("way")
    assert fixture_v3.delta_new_way_no_base_cell in catalog.delta_present_cells(manifest, "way")

    b = bbox_args(fixture_v3.delta_new_way_no_base_cell_bbox)
    r = engine_v3.run(f"[out:json];way({b});out ids;")
    assert ids_of(r.elements) == [fixture_v3.delta_new_way_no_base_cell_id]

    r2 = engine_v3.run(f"[out:json];way({fixture_v3.delta_new_way_no_base_cell_id});out;")
    assert len(r2.elements) == 1
    assert r2.elements[0]["tags"] == {"building": "yes"}


def test_new_way_reachable_backward_from_its_node(engine_v3, fixture_v3):
    r = engine_v3.run(f"[out:json];node({fixture_v3.cafe_node_id});<;out ids;")
    ids = ids_of(r.elements, "way")
    assert fixture_v3.delta_new_way_id in ids


def test_new_relation_created_by_day_tier_by_id(engine_v3, fixture_v3):
    r = engine_v3.run(f"[out:json];relation({fixture_v3.delta_new_relation_id});out;")
    assert len(r.elements) == 1
    assert r.elements[0]["tags"]["name"] == "New Park (day)"


def test_forward_from_new_relation_resolves_new_way_and_node_members(engine_v3, fixture_v3):
    r = engine_v3.run(f"[out:json];relation({fixture_v3.delta_new_relation_id});>;out ids;")
    way_ids = ids_of(r.elements, "way")
    node_ids = ids_of(r.elements, "node")
    assert fixture_v3.delta_new_relation_way_member_id in way_ids
    assert fixture_v3.delta_new_relation_node_member_id in node_ids
    # The new way's own refs (its member way's nodes) resolve too.
    for nid in fixture_v3.delta_new_way_refs:
        assert nid in node_ids


# --------------------------------------------------------- refs modified


def test_modified_way_refs_and_version_via_byid(engine_v3, fixture_v3):
    r = engine_v3.run(f"[out:json];way({fixture_v3.delta_modified_refs_way_id});out meta geom;")
    assert len(r.elements) == 1
    el = r.elements[0]
    assert el["nodes"] == fixture_v3.delta_modified_refs_way_new_refs
    assert el["version"] == fixture_v3.delta_modified_refs_way_new_version
    assert "geometry" in el
    assert len(el["geometry"]) == len(fixture_v3.delta_modified_refs_way_new_refs)


def test_modified_way_refs_via_bbox_scan(engine_v3, fixture_v3):
    b = bbox_args(fixture_v3.leaf_bbox["000"])
    r = engine_v3.run(f"[out:json];way[highway=residential]({b});out meta;")
    by_id = {e["id"]: e for e in r.elements}
    assert fixture_v3.delta_modified_refs_way_id in by_id
    assert by_id[fixture_v3.delta_modified_refs_way_id]["version"] == fixture_v3.delta_modified_refs_way_new_version


# ------------------------------------------------------------------ stats


def test_stats_delta_rows_and_shadowed_present(engine_v3, fixture_v3):
    b = bbox_args(fixture_v3.leaf_bbox["000"])
    r = engine_v3.run(f"[out:json];node[amenity=cafe]({b});out;")
    assert "delta_rows" in r.stats
    assert "shadowed" in r.stats
    assert r.stats["delta_rows"] > 0
    assert r.stats["shadowed"] > 0  # the base row for delta_modified_node_id is shadowed


# --------------------------------------------------------------------------
# Performance follow-up (docs/m2-contracts.md section 4): `current_rows`/
# `byid_current_rows` used to reissue `read_parquet(tier_file)` (plus a
# fresh `QUALIFY row_number()` re-rank) on *every* cell-scoped/by-id call
# within one query, even though a tier's whole file is small enough to load
# once and reuse. `sources._load_or_cache` is the cache that fixes that
# (keyed on the DuckDB connection, so it is scoped to one `Engine.run()`);
# `sources.DELTA_WHOLE_LOAD_MAX_BYTES`/`_tier_too_large` is the planet-scale
# guard that keeps an oversized tier from being loaded whole at all.
# --------------------------------------------------------------------------


def test_load_or_cache_reuses_temp_table_across_calls_same_connection(fixture_v3):
    import duckdb

    manifest = catalog.load_manifest(fixture_v3.root)
    tier = manifest.delta_tiers()[0]
    path = tier["files"]["node"]["spatial"]

    con = duckdb.connect()
    try:
        src1, nfiles1 = sources._load_or_cache(con, path, too_large=False)
        src2, nfiles2 = sources._load_or_cache(con, path, too_large=False)
        assert nfiles1 == 1  # first call: one real Parquet read (the load)
        assert nfiles2 == 0  # second call on the same connection: reused, no read
        assert src1 == src2  # same cached TEMP TABLE name
        assert con.execute(f"SELECT count(*) FROM {src1}").fetchone()[0] > 0

        # A different connection gets its own cache -- no cross-connection
        # leakage, and no reuse across what would be two different runs.
        con2 = duckdb.connect()
        try:
            src3, nfiles3 = sources._load_or_cache(con2, path, too_large=False)
            assert nfiles3 == 1
        finally:
            con2.close()
    finally:
        con.close()


def test_tier_too_large_guard(monkeypatch):
    tier = {"rows": {"node": 1000, "way": 0, "relation": 0}}
    assert not sources._tier_too_large(tier, "node")
    monkeypatch.setattr(sources, "DELTA_WHOLE_LOAD_MAX_BYTES", 1)
    assert sources._tier_too_large(tier, "node")
    # A tier with no declared row count for the table(s) asked about is
    # never "too large" -- absence means "no effect", same convention as
    # every other manifest-v3-follow-up field.
    assert not sources._tier_too_large({"rows": {}}, "node")
    assert not sources._tier_too_large({}, "node")


def test_load_or_cache_too_large_falls_back_to_uncached_read_parquet(fixture_v3):
    import duckdb

    manifest = catalog.load_manifest(fixture_v3.root)
    tier = manifest.delta_tiers()[0]
    path = tier["files"]["node"]["spatial"]

    con = duckdb.connect()
    try:
        src1, nfiles1 = sources._load_or_cache(con, path, too_large=True)
        src2, nfiles2 = sources._load_or_cache(con, path, too_large=True)
        # Every call re-reads the raw file -- the old, pre-cache behavior
        # -- instead of ever materializing it into a TEMP TABLE.
        assert nfiles1 == 1 and nfiles2 == 1
        assert src1 == src2 == f"read_parquet('{path}')"
        assert con.execute(f"SELECT count(*) FROM {src1}").fetchone()[0] > 0
    finally:
        con.close()


def test_delta_whole_load_size_guard_falls_back_but_stays_correct(engine_v3, fixture_v3, monkeypatch):
    """With `DELTA_WHOLE_LOAD_MAX_BYTES` monkeypatched down so every tier's
    (tiny, few-row) file trips the planet-scale guard, `current_rows` and
    `byid_current_rows` take the uncached per-call `read_parquet(...)`
    fallback path for every tier/table this query touches -- and the
    results must be byte-for-byte the same as the (now-default) cached
    path exercises elsewhere in this file, since the guard only changes
    where the bytes come from, never the query semantics."""
    monkeypatch.setattr(sources, "DELTA_WHOLE_LOAD_MAX_BYTES", 1)
    manifest = catalog.load_manifest(fixture_v3.root)
    for tier in manifest.delta_tiers():
        for t in ("node", "way", "relation"):
            if (tier.get("rows") or {}).get(t):  # only tables this tier actually touched
                assert sources._tier_too_large(tier, t)

    b = bbox_args(fixture_v3.leaf_bbox["000"])
    r = engine_v3.run(f"[out:json];node[amenity=cafe]({b});out;")
    by_id = {e["id"]: e for e in r.elements}
    assert fixture_v3.delta_modified_node_id in by_id
    assert by_id[fixture_v3.delta_modified_node_id]["tags"] == fixture_v3.delta_modified_node_day_tags
    assert ids_of(r.elements, "node").count(fixture_v3.delta_modified_node_id) == 1

    r2 = engine_v3.run(f"[out:json];way({fixture_v3.delta_deleted_way_id});out;")
    assert r2.elements == []

    r3 = engine_v3.run(f"[out:json];way({fixture_v3.delta_new_way_id});out geom;")
    assert len(r3.elements) == 1
    assert r3.elements[0]["nodes"] == fixture_v3.delta_new_way_refs


# --------------------------------------------------------------------------
# v2 (no deltas) is unaffected: same results, and the SQL the executor runs
# never touches anything delta-specific. Captured via a logging proxy on
# the cursor DuckDB executes through (Engine.run_program opens exactly one
# cursor per run and every sources.py/recurse.py call reuses it).
# --------------------------------------------------------------------------


class _RecordingCursor:
    def __init__(self, real):
        self._real = real
        self.sql_log: list[str] = []

    def execute(self, sql, *a, **kw):
        self.sql_log.append(sql)
        return self._real.execute(sql, *a, **kw)

    def cursor(self):
        # Nested cursor() calls (none of the current code makes one off an
        # already-fresh run cursor, but stay safe) get wrapped too.
        return _RecordingCursor(self._real.cursor())

    def __getattr__(self, name):
        return getattr(self._real, name)


class _RecordingDB:
    """Wraps `Engine._db` (a real DuckDBPyConnection, whose `.cursor`
    attribute is read-only -- it can't be monkeypatched directly) so that
    `Engine.run_program`'s one `self._db.cursor()` call per run returns a
    `_RecordingCursor` instead. Every `sources.py`/`recurse.py` SQL this
    session issues goes through that same cursor, so this captures the
    whole run's SQL in one place."""

    def __init__(self, real):
        self._real = real
        self.logs: list[_RecordingCursor] = []

    def cursor(self, *a, **kw):
        proxy = _RecordingCursor(self._real.cursor(*a, **kw))
        self.logs.append(proxy)
        return proxy

    def __getattr__(self, name):
        return getattr(self._real, name)


def run_capturing_sql(engine: Engine, query: str):
    real_db = engine._db
    proxy_db = _RecordingDB(real_db)
    engine._db = proxy_db
    try:
        result = engine.run(query)
    finally:
        engine._db = real_db
    all_sql = "\n----\n".join(sql for proxy in proxy_db.logs for sql in proxy.sql_log)
    return result, all_sql


_DELTA_SQL_MARKERS = ("__dcand", "__dshadow", "__dbyid", "deltawayref", "deltarelmember", "prev_cell")


@pytest.fixture(scope="module")
def fixture_v2(tmp_path_factory):
    root = tmp_path_factory.mktemp("engine_v3_v2_fixture")
    return make_fixture.build(str(root), manifest_version=2)


@pytest.fixture(scope="module")
def engine_v2(fixture_v2):
    return Engine(fixture_v2.root)


REPRESENTATIVE_QUERIES = [
    "[out:json];node[amenity=cafe]({b});out;",
    "[out:json];node({cafe});out meta;",
    "[out:json];way({spanning_way});>;out ids;",
    "[out:json];node({cafe});<;out ids;",
]


def test_v2_no_deltas_sql_has_no_delta_fragments(engine_v2, fixture_v2):
    ctx = {"b": bbox_args(fixture_v2.leaf_bbox["000"]), "cafe": fixture_v2.cafe_node_id,
           "spanning_way": fixture_v2.spanning_way_id}
    for template in REPRESENTATIVE_QUERIES:
        q = template.format(**ctx)
        result, sql_text = run_capturing_sql(engine_v2, q)
        for marker in _DELTA_SQL_MARKERS:
            assert marker not in sql_text, f"delta SQL fragment {marker!r} leaked into no-delta query: {q}"
        assert result.stats["delta_rows"] == 0
        assert result.stats["shadowed"] == 0


def test_v2_fixture_still_passes_representative_query_results(engine_v2, fixture_v2):
    # Same assertions test_engine_v2.py already makes -- re-run here next
    # to the SQL-shape check above so a reviewer sees both together.
    b = bbox_args(fixture_v2.leaf_bbox["000"])
    r = engine_v2.run(f"[out:json];node[amenity=cafe]({b});out;")
    assert sorted(e["id"] for e in r.elements) == sorted([fixture_v2.cafe_node_id, fixture_v2.cafe_node2_id])
