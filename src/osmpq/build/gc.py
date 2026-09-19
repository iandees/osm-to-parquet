"""``osmpq gc <root> [--keep 2] [--dry-run]``: docs/m2-contracts.md section 6.

Deletes files under the root that no manifest among the newest ``--keep``
manifests references (paths collected from ``tables``, ``byid``, ``index``,
``rowgroup_index`` and ``deltas``), and deletes the older manifest JSON
files themselves. Reads manifests as plain JSON so this has no dependency
on ``osmpq.layout.manifest.Manifest`` gaining v3 fields.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional


def _referenced_paths(man: dict) -> set[str]:
    """Every relative path this (plain-dict) manifest references."""
    paths: set[str] = set()
    for spec in (man.get("tables") or {}).values():
        for entry in (spec.get("cells") or {}).values():
            if "path" in entry:
                paths.add(entry["path"])
            else:
                for part in ("tagged", "untagged"):
                    if entry.get(part):
                        paths.add(entry[part]["path"])
    for parts in (man.get("byid") or {}).values():
        for p in parts:
            paths.add(p["path"])
    for parts in (man.get("index") or {}).values():
        for p in parts:
            paths.add(p["path"])
    for p in (man.get("rowgroup_index") or {}).values():
        paths.add(p)
    # docs/m3-contracts.md section 4.2: manifest v4 `areas` (index file +
    # per-cell spatial files) is referenced like any other table.
    areas = man.get("areas") or {}
    if areas.get("index"):
        paths.add(areas["index"]["path"])
    if areas.get("way_index"):
        paths.add(areas["way_index"]["path"])
    for entry in (areas.get("cells") or {}).values():
        if "path" in entry:
            paths.add(entry["path"])
    for tier_info in (man.get("deltas") or {}).values():
        for kind, val in (tier_info.get("files") or {}).items():
            if isinstance(val, dict):
                paths.update(val.values())
            elif isinstance(val, str):
                paths.add(val)
    return paths


def _manifest_numbers(manifest_dir: Path) -> list[int]:
    out = []
    for f in manifest_dir.glob("*.json"):
        try:
            out.append(int(f.stem))
        except ValueError:
            continue
    return sorted(out)


def _all_files_under(root: Path) -> list[Path]:
    out = []
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root)
            if rel.parts and rel.parts[0] == "manifest":
                continue
            out.append(p)
    return out


def _prune_empty_dirs(root: Path) -> list[str]:
    """Remove now-empty directories (bottom-up), except the root itself and
    ``manifest/``. Returns the relative paths removed."""
    removed = []
    dirs = sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True)
    for d in dirs:
        rel = d.relative_to(root)
        if not rel.parts or rel.parts[0] == "manifest":
            continue
        try:
            next(d.iterdir())
        except StopIteration:
            d.rmdir()
            removed.append(str(rel).replace("\\", "/"))
        except FileNotFoundError:
            continue
    return removed


def gc(root: str, keep: int = 2, dry_run: bool = False) -> dict:
    root_path = Path(root)
    manifest_dir = root_path / "manifest"
    numbers = _manifest_numbers(manifest_dir)
    if not numbers:
        return {"removed_files": [], "removed_manifests": [], "removed_dirs": []}

    keep_numbers = set(numbers[-keep:]) if keep > 0 else set()
    referenced: set[str] = set()
    for n in keep_numbers:
        man = json.loads((manifest_dir / f"{n}.json").read_text())
        referenced |= _referenced_paths(man)

    all_files = _all_files_under(root_path)
    to_delete_files = []
    for p in all_files:
        rel = str(p.relative_to(root_path)).replace("\\", "/")
        if rel not in referenced:
            to_delete_files.append((p, rel))

    to_delete_manifests = [n for n in numbers if n not in keep_numbers]

    removed_files, removed_manifests = [], []
    if dry_run:
        removed_files = [rel for _p, rel in to_delete_files]
        removed_manifests = [f"manifest/{n}.json" for n in to_delete_manifests]
        removed_dirs: list[str] = []
    else:
        for p, rel in to_delete_files:
            p.unlink()
            removed_files.append(rel)
        for n in to_delete_manifests:
            (manifest_dir / f"{n}.json").unlink()
            removed_manifests.append(f"manifest/{n}.json")
        removed_dirs = _prune_empty_dirs(root_path)

    return {"removed_files": removed_files, "removed_manifests": removed_manifests, "removed_dirs": removed_dirs}


def gc_main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="osmpq gc", description=__doc__)
    p.add_argument("root")
    p.add_argument("--keep", type=int, default=2)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv if argv is not None else sys.argv[1:])

    result = gc(args.root, keep=args.keep, dry_run=args.dry_run)
    prefix = "(dry-run) would remove" if args.dry_run else "removed"
    for rel in result["removed_files"]:
        print(f"{prefix} file: {rel}")
    for rel in result["removed_manifests"]:
        print(f"{prefix} manifest: {rel}")
    for rel in result.get("removed_dirs", []):
        print(f"{prefix} empty dir: {rel}")
    total = len(result["removed_files"]) + len(result["removed_manifests"])
    print(f"osmpq gc: {'would remove' if args.dry_run else 'removed'} {total} file(s)/manifest(s)"
          f" (kept the newest {args.keep} manifest(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(gc_main())
