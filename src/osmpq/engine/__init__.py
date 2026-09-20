"""The query engine: translates a parsed Overpass QL ``ast.Program`` into
SQL over the on-disk (or on-S3) Parquet layout, executes it against an
embedded DuckDB database, and renders the result in Overpass's JSON/XML/CSV
shapes. ``Engine`` (``executor.py``) is the package's main entry point;
``planner.py`` and ``hooks.py`` own statement dispatch and the tier-2
extension-point registry that the other modules in this package plug into.
See docs/development.md for the query pipeline and the extension points.
"""
from .executor import Engine
from .result import Result

__all__ = ["Engine", "Result"]
