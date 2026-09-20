"""osmpq: an Overpass-QL-compatible query service over OpenStreetMap data
stored as Parquet on local disk or object storage (R2/S3), plus the
build/update/history tooling that produces and maintains that data. See
``osmpq.ql`` (the Overpass QL front end), ``osmpq.engine`` (the query
engine), ``osmpq.build``/``osmpq.update``/``osmpq.history`` (dataset
construction and maintenance), and ``osmpq.server`` (the HTTP API); the
``osmpq`` console script (``osmpq.cli``) is the command-line entry point
for all of them. See docs/development.md for the repository map.
"""
