"""Layout primitives shared by the build and update side of osmpq,
independent of the query engine: quadtree cell math (``cells``), Hilbert
curve indexing (``hilbert``), and manifest dataclasses plus their
read/write/``LATEST`` handling (``manifest``). ``osmpq.engine.catalog``
reimplements the cell/manifest logic the query engine needs rather than
depending on this package, so the two can evolve independently; see its
module docstring.
"""
