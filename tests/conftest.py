"""Make sure ``import osmpq`` resolves to *this worktree's* ``src/``.

The sandbox's global editable install of ``osmpq`` can point at a
different checkout/worktree's ``src/`` directory than the one pytest is
being run from here; whichever test module Python imports first "wins"
and caches ``osmpq`` (and every ``osmpq.*`` submodule) in ``sys.modules``
for the rest of the process, silently shadowing any change made in this
worktree for every test file that doesn't defend against it itself.

A root ``conftest.py`` is imported by pytest before it collects/imports
any test module in this directory, so inserting this worktree's own
``src/`` at the front of ``sys.path`` here -- before anything has had a
chance to import ``osmpq`` from somewhere else -- makes every test file
see this worktree's code, regardless of import order.
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKTREE_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _WORKTREE_SRC not in sys.path:
    sys.path.insert(0, _WORKTREE_SRC)
