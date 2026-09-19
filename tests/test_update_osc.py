"""Tests for ``osmpq.update.osc`` (docs/m2-contracts.md section 1 and
section 5 step 2) on a hand-written ``.osc`` file covering create/modify/
delete for all three element types, and a node appearing twice."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from osmpq.update import osc as osc_mod

SAMPLE_OSC = """<osmChange version="0.6" generator="test">
<create>
<node id="100" version="1" timestamp="2026-01-01T00:00:00Z" uid="1" user="u" changeset="1" lat="10.0" lon="20.0"/>
</create>
<modify>
<node id="100" version="2" timestamp="2026-01-01T00:01:00Z" uid="1" user="u" changeset="2" lat="10.5" lon="20.5">
<tag k="amenity" v="cafe"/>
</node>
</modify>
<delete>
<node id="100" version="3" timestamp="2026-01-01T00:02:00Z" uid="1" user="u" changeset="3" lat="10.5" lon="20.5"/>
</delete>
<create>
<node id="200" version="1" timestamp="2026-01-01T00:03:00Z" uid="2" user="v" changeset="4" lat="11.0" lon="21.0"/>
<node id="201" version="1" timestamp="2026-01-01T00:03:00Z" uid="2" user="v" changeset="4" lat="11.1" lon="21.1"/>
<way id="300" version="1" timestamp="2026-01-01T00:03:00Z" uid="2" user="v" changeset="4">
<nd ref="200"/>
<nd ref="201"/>
<tag k="highway" v="residential"/>
</way>
</create>
<modify>
<way id="300" version="2" timestamp="2026-01-01T00:04:00Z" uid="2" user="v" changeset="5">
<nd ref="200"/>
<nd ref="201"/>
<tag k="highway" v="service"/>
</way>
</modify>
<delete>
<way id="301" version="2" timestamp="2026-01-01T00:05:00Z" uid="2" user="v" changeset="6"/>
</delete>
<create>
<relation id="400" version="1" timestamp="2026-01-01T00:06:00Z" uid="1" user="u" changeset="7">
<member type="node" ref="200" role="foo"/>
<member type="way" ref="300" role=""/>
<tag k="type" v="multipolygon"/>
</relation>
</create>
<modify>
<relation id="400" version="2" timestamp="2026-01-01T00:07:00Z" uid="1" user="u" changeset="8">
<member type="node" ref="201" role="bar"/>
</relation>
</modify>
<delete>
<relation id="401" version="5" timestamp="2026-01-01T00:08:00Z" uid="1" user="u" changeset="9"/>
</delete>
</osmChange>
"""


@pytest.fixture(scope="module")
def sample_path(tmp_path_factory) -> Path:
    d = tmp_path_factory.mktemp("osc")
    p = d / "sample.osc"
    p.write_text(SAMPLE_OSC)
    return p


@pytest.fixture(scope="module")
def batch(sample_path) -> osc_mod.BatchResult:
    return osc_mod.parse_batch([(42, sample_path)])


def test_last_occurrence_wins_for_repeated_node(batch):
    # node 100: create(v1) -> modify(v2) -> delete(v3); last occurrence (the
    # delete) must win.
    assert batch.node.num_rows == 3  # 100 (deleted), 200, 201
    rows = {r["id"]: r for r in batch.node.to_pylist()}
    assert rows[100]["deleted"] is True
    assert rows[100]["version"] == 3
    assert rows[100]["tags"] is None  # tags are not part of a delete block here
    assert rows[100]["seq"] == 42


def test_node_columns_and_tags(batch):
    rows = {r["id"]: r for r in batch.node.to_pylist()}
    n200 = rows[200]
    assert n200["deleted"] is False
    assert n200["version"] == 1
    assert n200["changeset"] == 4
    assert n200["uid"] == 2
    assert n200["user"] == "v"
    assert n200["tags"] is None
    assert n200["lat_e7"] == 110000000
    assert n200["lon_e7"] == 210000000


def test_way_columns_last_occurrence_and_refs(batch):
    assert batch.way.num_rows == 2  # 300 (modified), 301 (deleted)
    rows = {r["id"]: r for r in batch.way.to_pylist()}
    w300 = rows[300]
    assert w300["deleted"] is False
    assert w300["version"] == 2  # modify wins over the earlier create
    assert w300["refs"] == [200, 201]
    assert dict(w300["tags"]) == {"highway": "service"}
    w301 = rows[301]
    assert w301["deleted"] is True
    assert w301["version"] == 2


def test_relation_columns_last_occurrence_and_members(batch):
    assert batch.relation.num_rows == 2  # 400 (modified), 401 (deleted)
    rows = {r["id"]: r for r in batch.relation.to_pylist()}
    r400 = rows[400]
    assert r400["deleted"] is False
    assert r400["version"] == 2
    # the modify block replaced the member list wholesale
    assert r400["members"] == [{"type": "n", "ref": 201, "role": "bar"}]
    r401 = rows[401]
    assert r401["deleted"] is True
    assert r401["version"] == 5


def test_tags_column_is_a_native_duckdb_map(batch):
    con = duckdb.connect()
    con.register("way", batch.way)
    described = dict((c[0], c[1]) for c in con.execute("DESCRIBE SELECT * FROM way").fetchall())
    assert described["tags"] == "MAP(VARCHAR, VARCHAR)"
    row = con.execute("SELECT tags FROM way WHERE id = 300").fetchone()
    assert row[0] == {"highway": "service"}


def test_batch_seq_range(batch):
    assert batch.first_seq == 42
    assert batch.last_seq == 42
    assert batch.n_files == 1


def test_two_file_batch_last_occurrence_across_files(tmp_path):
    """The same id modified in two files of a batch: the later sequence wins,
    even though it's a separate file (not just a repeat within one file)."""
    f1 = tmp_path / "1.osc"
    f1.write_text(
        '<osmChange version="0.6" generator="t">'
        '<modify><node id="500" version="4" timestamp="2026-01-01T00:00:00Z" '
        'uid="1" user="u" changeset="1" lat="1.0" lon="2.0"/></modify>'
        "</osmChange>"
    )
    f2 = tmp_path / "2.osc"
    f2.write_text(
        '<osmChange version="0.6" generator="t">'
        '<modify><node id="500" version="5" timestamp="2026-01-01T00:01:00Z" '
        'uid="1" user="u" changeset="2" lat="3.0" lon="4.0"/></modify>'
        "</osmChange>"
    )
    result = osc_mod.parse_batch([(1, f1), (2, f2)])
    rows = {r["id"]: r for r in result.node.to_pylist()}
    assert rows[500]["version"] == 5
    assert rows[500]["lat_e7"] == 30000000
    assert rows[500]["seq"] == 2
    assert result.first_seq == 1 and result.last_seq == 2


def test_empty_osc_file(tmp_path):
    """A sequence with no changes (osmosis publishes these) parses to empty
    tables without error."""
    f = tmp_path / "empty.osc"
    f.write_text('<osmChange version="0.6" generator="xmlwriter">\n</osmChange>')
    result = osc_mod.parse_batch([(7292745, f)])
    assert result.node.num_rows == 0
    assert result.way.num_rows == 0
    assert result.relation.num_rows == 0
    assert result.first_seq == 7292745
    assert result.last_seq == 7292745
