"""`osmpq history init` (src/osmpq/history/init.py): the current tables
become the first state of every element, and the attic engine answers
`[date:]` at `since` identically to a plain query."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fixtures import history_v5, make_fixture  # noqa: E402

from osmpq.build import validate as validate_mod  # noqa: E402
from osmpq.engine.executor import Engine  # noqa: E402
from osmpq.history.init import HistoryInitOptions, history_init  # noqa: E402
from osmpq.layout import manifest as manifest_mod  # noqa: E402


@pytest.fixture(scope="module")
def root(tmp_path_factory) -> str:
    r = tmp_path_factory.mktemp("history-init") / "root"
    make_fixture.build(str(r), manifest_version=2)
    history_v5._add_updater_hilbert_columns(r)
    summary = history_init(HistoryInitOptions(root=str(r)))
    assert summary["rows"]["node"] > 0 and summary["rows"]["way"] > 0
    return str(r)


def test_manifest_is_v5_with_history(root):
    man = manifest_mod.load_latest(root)
    assert man.manifest_version == 5
    h = man.history
    assert h["since"] == man.timestamp_osm_base and h["tiers"] == {}
    assert h["stats"]["minor_rows"] == {"node": 0, "way": 0, "relation": 0}
    for typ in ("node", "way", "relation"):
        assert h["byid"][typ] and h["spatial"][typ]
        for p in h["byid"][typ]:
            assert (Path(root) / p["path"]).exists()


def test_init_refuses_twice(root):
    with pytest.raises(SystemExit):
        history_init(HistoryInitOptions(root=root))


def test_validate_passes(root):
    # The M0 fixture's own spatial files are not hilbert-sorted (it predates
    # hilbert), so only the history checks are asserted here.
    _ok, summary = validate_mod.validate(root)
    problems = [line for line in summary if line.strip().startswith("- ")]
    assert not [p for p in problems if "history" in p.lower()], problems
    assert any("history" in line.lower() and "ok:" in line for line in summary), summary


def test_date_at_since_equals_now(root):
    eng = Engine(root)
    since = eng.manifest.timestamp_osm_base
    q = "nwr(-90,-180,90,180);out meta geom;"
    now = eng.run("[out:json];" + q)
    then = eng.run(f'[out:json][date:"{since}"];' + q)
    assert then.remark is None
    key = lambda e: (e["type"], e["id"])  # noqa: E731
    now_nw = [e for e in now.elements if e["type"] != "relation"]
    then_nw = [e for e in then.elements if e["type"] != "relation"]
    assert now_nw == then_nw and len(now_nw) > 10
    # relations: the current path's exact member test drops a relation whose
    # only member is another relation (fixture 205); the snapshot path keeps
    # it (bbox test only, contract 3.1). Everything current is in the snapshot.
    now_r = {key(e) for e in now.elements if e["type"] == "relation"}
    then_r = {key(e) for e in then.elements if e["type"] == "relation"}
    assert now_r <= then_r and then_r - now_r <= {("relation", 205)}


def test_timeline_has_one_entry_per_element(root):
    eng = Engine(root)
    r = eng.run("[out:json];timeline(node,1);out;")
    assert len(r.elements) == 1
    assert "expired" not in r.elements[0]["tags"]
