"""Ensures ``import osmpq`` resolves to *this worktree's* ``src/`` rather
than whatever path the shared environment's editable install happens to
point at (this repo's worktrees share one Python environment, and the
editable install is only ever repointed at one of them at a time). Mirrors
the same ``sys.path.insert`` pattern ``tests/fixtures/make_fixture.py``
already uses for itself.
"""
from __future__ import annotations

import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
