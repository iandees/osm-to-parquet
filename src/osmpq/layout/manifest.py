"""Manifest dataclasses and I/O, per docs/m0-contracts.md section 3.

A dataset root is a local directory or an ``s3://bucket/prefix`` URL. All
paths recorded *inside* the manifest are relative to the root, forward
slashes, no leading slash (section 1). This module builds, writes and loads
``manifest/<n>.json`` and ``manifest/LATEST``.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

MANIFEST_VERSION = 1
CURRENT_MANIFEST_VERSION = 3
SCHEMA_VERSION = 1
COORDINATE_SCALE = 10_000_000

DEFAULT_PROMOTED_KEYS = [
    "amenity",
    "shop",
    "highway",
    "building",
    "name",
    "natural",
    "landuse",
    "leisure",
    "railway",
    "waterway",
    "place",
    "tourism",
]


def _is_remote(root: str) -> bool:
    return "://" in root


def _join(root: str, path: str) -> str:
    if _is_remote(root):
        return root.rstrip("/") + "/" + path
    return str(Path(root) / path)


@dataclass
class Manifest:
    """In-memory form of ``manifest/<n>.json``.

    ``tables``, ``byid`` and ``index`` are kept as plain nested dicts (rather
    than a deeper dataclass tree) because their shapes are heterogeneous per
    the contract (node cells split tagged/untagged; way/relation cells carry
    a bbox; byid parts carry min_id/max_id; member index parts do not) and
    the contract fixes their exact JSON shape directly.
    """

    generation: str
    timestamp_osm_base: str
    source: str
    extent: list[float]
    leaf_cells: list[str]
    tables: dict[str, Any] = field(default_factory=dict)
    byid: dict[str, Any] = field(default_factory=dict)
    index: dict[str, Any] = field(default_factory=dict)
    promoted_keys: list[str] = field(default_factory=lambda: list(DEFAULT_PROMOTED_KEYS))
    replication_sequence: Optional[int] = None
    manifest_version: int = MANIFEST_VERSION
    schema_version: int = SCHEMA_VERSION
    coordinate_scale: int = COORDINATE_SCALE
    # -- v2 fields (docs/m1-contracts.md section 5); None/empty when absent
    # from a v1 manifest, so v1 loading is unaffected. ------------------------
    ancestor_depths: Optional[list[int]] = None
    max_depth: Optional[int] = None
    rowgroup_index: dict[str, str] = field(default_factory=dict)
    producer: dict[str, str] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    # -- v3 fields (docs/m2-contracts.md section 3); None/empty when absent
    # from a v1/v2 manifest. ``replication_source`` is the osmosis-style
    # replication directory URL the updater last pulled from.
    # ``deltas`` maps tier name ("hour"/"day"/"week") -> that tier's current
    # version metadata; a tier absent from ``deltas`` is empty (base only).
    replication_source: Optional[str] = None
    deltas: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "manifest_version": self.manifest_version,
            "generation": self.generation,
            "schema_version": self.schema_version,
            "coordinate_scale": self.coordinate_scale,
            "promoted_keys": self.promoted_keys,
            "timestamp_osm_base": self.timestamp_osm_base,
            "replication_sequence": self.replication_sequence,
            "source": self.source,
            "extent": self.extent,
            "leaf_cells": self.leaf_cells,
            "tables": self.tables,
            "byid": self.byid,
            "index": self.index,
        }
        if self.manifest_version >= 2:
            d["ancestor_depths"] = self.ancestor_depths
            d["max_depth"] = self.max_depth
            d["rowgroup_index"] = self.rowgroup_index
            d["producer"] = self.producer
            d["stats"] = self.stats
        if self.manifest_version >= 3:
            d["replication_source"] = self.replication_source
            d["deltas"] = self.deltas
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Manifest":
        return cls(
            generation=d["generation"],
            timestamp_osm_base=d["timestamp_osm_base"],
            source=d["source"],
            extent=d["extent"],
            leaf_cells=d["leaf_cells"],
            tables=d.get("tables", {}),
            byid=d.get("byid", {}),
            index=d.get("index", {}),
            promoted_keys=d.get("promoted_keys", list(DEFAULT_PROMOTED_KEYS)),
            replication_sequence=d.get("replication_sequence"),
            manifest_version=d.get("manifest_version", MANIFEST_VERSION),
            schema_version=d.get("schema_version", SCHEMA_VERSION),
            coordinate_scale=d.get("coordinate_scale", COORDINATE_SCALE),
            ancestor_depths=d.get("ancestor_depths"),
            max_depth=d.get("max_depth"),
            rowgroup_index=d.get("rowgroup_index", {}),
            producer=d.get("producer", {}),
            stats=d.get("stats", {}),
            replication_source=d.get("replication_source"),
            deltas=d.get("deltas", {}),
        )

    # -- convenience accessors -------------------------------------------------

    def node_cell_files(self, cell: str) -> dict[str, Any]:
        return self.tables.get("node", {}).get("cells", {}).get(cell, {})

    def way_cell_file(self, cell: str) -> dict[str, Any] | None:
        return self.tables.get("way", {}).get("cells", {}).get(cell)

    def relation_cell_file(self, cell: str) -> dict[str, Any] | None:
        return self.tables.get("relation", {}).get("cells", {}).get(cell)

    def all_paths(self) -> list[str]:
        """Every file path this manifest references, for validation."""
        paths: list[str] = []
        for _table, spec in self.tables.items():
            for _cell, entry in spec.get("cells", {}).items():
                if "path" in entry:
                    paths.append(entry["path"])
                else:
                    for part in ("tagged", "untagged"):
                        if entry.get(part):
                            paths.append(entry[part]["path"])
        for _table, parts in self.byid.items():
            for part in parts:
                paths.append(part["path"])
        for _table, parts in self.index.items():
            for part in parts:
                paths.append(part["path"])
        for path in self.rowgroup_index.values():
            paths.append(path)
        for _tier, entry in self.deltas.items():
            files = entry.get("files", {})
            for _table, table_files in files.items():
                if isinstance(table_files, dict):
                    for _kind, p in table_files.items():
                        if p:
                            paths.append(p)
                elif table_files:
                    paths.append(table_files)
        return paths


def local_root_path(root: str) -> Path:
    if _is_remote(root):
        raise ValueError(f"not a local root: {root}")
    return Path(root)


def next_manifest_number(root: str) -> int:
    """The manifest number to use for a new build: 1, or LATEST + 1."""
    latest = _read_latest(root)
    return (latest + 1) if latest is not None else 1


def _read_latest(root: str) -> Optional[int]:
    if _is_remote(root):
        text = _s3_read_text(_join(root, "manifest/LATEST"))
        return int(text.strip()) if text is not None else None
    path = local_root_path(root) / "manifest" / "LATEST"
    if not path.exists():
        return None
    return int(path.read_text().strip())


def _atomic_write_text(path: Path, body: str) -> None:
    """Write ``body`` to ``path`` without mutating whatever inode ``path``
    currently names.

    A plain ``path.write_text()`` opens the existing file and truncates it
    in place, which is wrong here: dataset roots are routinely duplicated
    with ``cp -al`` (a hardlinked, near-free snapshot -- e.g. before a
    stateless ``osmpq update`` run, or by ``osmpq compact``/``gc`` between
    generations), and ``manifest/LATEST`` in particular is the same
    filename in every one of those snapshots. Truncating it in place would
    silently corrupt every other snapshot sharing that inode. Writing to a
    temp file in the same directory and ``os.replace``-ing it over the
    target instead always lands on a fresh inode, leaving any other
    hardlinked copy's file untouched -- the same pattern already used by
    ``osmpq.build.builder._place_file``'s hardlink mode.
    """
    import os
    import tempfile

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(body)
        os.replace(tmp_name, str(path))
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def write_manifest(root: str, manifest: Manifest, number: int) -> None:
    """Write ``manifest/<number>.json``, then update ``manifest/LATEST`` last."""
    body = json.dumps(manifest.to_dict(), indent=2, sort_keys=False)
    if _is_remote(root):
        _s3_write_text(_join(root, f"manifest/{number}.json"), body)
        _s3_write_text(_join(root, "manifest/LATEST"), str(number))
        return
    manifest_dir = local_root_path(root) / "manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(manifest_dir / f"{number}.json", body)
    # Written last: an engine that already loaded manifest n stays consistent
    # even while n+1 is being written.
    _atomic_write_text(manifest_dir / "LATEST", str(number))


def load(root: str, number: int) -> Manifest:
    if _is_remote(root):
        text = _s3_read_text(_join(root, f"manifest/{number}.json"))
        if text is None:
            raise FileNotFoundError(f"{root}/manifest/{number}.json")
        return Manifest.from_dict(json.loads(text))
    path = local_root_path(root) / "manifest" / f"{number}.json"
    return Manifest.from_dict(json.loads(path.read_text()))


def load_latest(root: str) -> Manifest:
    """Load the manifest named by ``manifest/LATEST`` under ``root``.

    ``root`` may be a local directory or an ``s3://`` URL (read via DuckDB's
    ``httpfs`` extension; s3 support is best-effort for M0).
    """
    number = _read_latest(root)
    if number is None:
        raise FileNotFoundError(f"{root}/manifest/LATEST not found")
    return load(root, number)


# --------------------------------------------------------------------------
# S3 access via DuckDB httpfs (best-effort for M0)
# --------------------------------------------------------------------------


def _s3_read_text(url: str) -> Optional[str]:
    # Round-tripped through DuckDB's CSV reader (not read_text, which returns
    # raw bytes) so arbitrary JSON content written by _s3_write_text below
    # comes back exactly, including embedded quotes/newlines.
    import duckdb

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        try:
            escaped_url = url.replace("'", "''")
            row = con.execute(
                f"SELECT content FROM read_csv('{escaped_url}', "
                "columns={'content': 'VARCHAR'}, header=false, quote='\"', escape='\"')"
            ).fetchone()
        except Exception:
            return None
        return row[0] if row else None
    finally:
        con.close()


def _s3_write_text(url: str, body: str) -> None:
    import duckdb

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        escaped_url = url.replace("'", "''")
        con.execute(
            f"COPY (SELECT ? AS content) TO '{escaped_url}' "
            "(FORMAT CSV, HEADER false, QUOTE '\"', ESCAPE '\"')",
            [body],
        )
    finally:
        con.close()
