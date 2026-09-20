"""Dataset construction: turning a PBF (or an existing ``raw/`` directory)
into a dataset root, and maintaining that root over time. ``raw.py`` and
``builder.py`` implement ``osmpq raw-py``/``osmpq build``; ``areas.py``,
``validate.py``, ``compact.py`` and ``gc.py`` implement the
correspondingly-named subcommands; ``common.py`` and ``rowgroups.py`` hold
helpers shared between the PBF-reading and the row-group-index-writing
stages. See docs/cli.md for each subcommand's flags and docs/development.md
for how these modules fit into the rest of the codebase.
"""
