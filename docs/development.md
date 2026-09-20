# Developer guide

How to set up a working copy, how the code is organized, how a query
flows through it, how to extend the query language, and how the test
suite and reference harness are structured. For the HTTP API and the
`osmpq`/`osmpq-raw` command-line tools themselves, see
[docs/api.md](api.md) and [docs/cli.md](cli.md). For milestone status and
what's currently in flight, see `docs/progress.md`.

## Setup

Requirements: Python >= 3.11, and optionally a Rust toolchain (only
needed to build the planet-scale `osmpq-raw` producer — everything else,
including running the full test suite against the small Bermuda fixture,
works in pure Python).

```sh
uv sync --extra dev              # creates .venv from pyproject.toml + uv.lock
uv run pytest tests -q           # run the test suite
uv run osmpq --help
```

`uv.lock` is committed, so `uv sync` reproduces the exact dependency set
used elsewhere. `pip install -e '.[dev]'` still works for anyone without
`uv` installed (it reads the same `pyproject.toml`, just without the
lockfile's pinned versions). Everything invoked as `uv run <cmd>` below
can equally be run directly (`pytest ...`, `osmpq ...`) inside an
activated `pip`-installed environment.

DuckDB's `spatial` and `httpfs` extensions are installed automatically on
first use (`Engine._setup_database` tries `LOAD` first, falls back to
`INSTALL ... LOAD`) — no separate install step in development, though the
service image installs them at build time so no network `INSTALL` happens
at container startup (`docs/m3-contracts.md` section 6.4).

`pyosmium` (the `osmium` package) is a regular dependency, used by the
`raw-py`/`osmpq-raw` alternative Python path, `tools/bbox_extract.py`, and
the history builder's ingest layer; `uv sync`/`pip install` pulls it in
like any other dependency.

The Rust producer, if you need it:

```sh
cd rust/osmpq-raw && cargo build --release
```

## Repository map

| Path | Owns |
| --- | --- |
| `src/osmpq/ql/` | Overpass QL front end: hand-written lexer (`lexer.py`), recursive-descent parser (`parser.py`) producing an `ast.Program`, the shared AST (`ast.py`), and a separate expression evaluator subset (`evaluator.py`) used by `(if:)`/`if`/`foreach`. `parse(text)` in `__init__.py` is the public entry point. |
| `src/osmpq/engine/` | The query engine. `executor.py` (`Engine`: one DuckDB database, one cursor per run, manifest refresh, timeouts, S3 secrets), `planner.py` (statement -> SQL, tier-1 core), `catalog.py` (manifest loading, cell/quadkey math, row-group index), `sources.py` (SQL builders: spatial/byid/set-sourced reads), `render.py` (materialized set -> Overpass JSON-shaped dicts), `result.py` (`Result.render()`: JSON/XML/CSV envelopes), `hooks.py` (the tier-2 extension-point registry), and the tier-2 modules that register into it: `geofilters.py` (`around`, `poly`), `areas.py` (area queries, `(area)`, `(pivot)`, `is_in`, `map_to_area`), `metafilters.py` (`newer`, `changed`, `user`, `uid`), `evalfilter.py`/`evalsql.py` (`(if:)`, `if`, `foreach`), `attic.py` (M4: snapshot reads, `[date:]`, `retro`, `timeline`, `[diff:]`/`[adiff:]`). Plus small shared pieces: `schema.py` (canonical set columns), `setops.py` (set materialization/algebra), `idset.py` (id-based lookups, recursion helpers), `recurse.py` (`>`,`>>`,`<`,`<<`), `tagsql.py` (tag filter -> SQL), `hilbert.py` (Hilbert key, registered as DuckDB UDFs). |
| `src/osmpq/build/` | Dataset construction. `raw.py` (`osmpq raw-py`: PBF -> `raw/` in pure DuckDB), `builder.py` (`osmpq build`: `raw/` -> a full dataset root — relations, byid/index tables, row-group index, manifest), `common.py` (shared helpers between the two), `areas.py` (`osmpq areas`), `rowgroups.py` (row-group index side files), `validate.py` (`osmpq validate`), `compact.py` (`osmpq compact`: fold delta tiers into a new base generation), `gc.py` (`osmpq gc`: delete unreferenced files/old manifests). |
| `src/osmpq/update/` | The stateless minutely updater. `updater.py` (the M2 update cycle: fetch diffs, re-resolve touched elements, rewrite rolling delta tiers), `osc.py` (OsmChange parsing via pyosmium), `replication.py` (osmosis-style replication client with retry/backoff), `server.py` (`osmpq updater-server`'s control API: `POST /run`, `GET /status`, `GET /healthz`). |
| `src/osmpq/layout/` | Layout primitives shared by the build/update side, independent of the query engine: `cells.py` (quadtree cell math), `hilbert.py` (Hilbert curve indexing), `manifest.py` (manifest dataclasses, read/write, `LATEST` pointer). `osmpq.engine.catalog` reimplements the cell/manifest logic it needs rather than depending on this package, so the engine can evolve independently (see its module docstring). |
| `src/osmpq/history/` | The history (attic) dataset: `schema.py` (shared history row definitions), `ingest.py` (raw object-version stream from a PBF+`.osc` set or a full-history `.osh.pbf`), `build.py` (`osmpq history build`: versions -> history states, minor versions, tombstones), `init.py` (`osmpq history init`: current tables -> first-known-state history), `writer.py` (writes the history layout: cell-partitioned spatial files, id-sorted byid files). |
| `src/osmpq/server.py` | The query API: `/api/interpreter`, `/api/status`, `/api/timestamp`, `/api/kill_my_queries`, `/healthz`. Thin — all query semantics live in `osmpq.engine`. See [docs/api.md](api.md). |
| `src/osmpq/store.py` | The object-store abstraction: `LocalStore`, `S3Store`, `MemoryStore`, `for_root(root)`, and `s3_secret_sql` (shared by the engine and the updater for `s3://` roots). |
| `src/osmpq/cli.py` | The `osmpq` command-line entry point: argument parsing for every subcommand in [docs/cli.md](cli.md); the actual work lives in the modules above. |
| `src/osmpq/errors.py` | The exception hierarchy: `ParseError` (-> HTTP 400 HTML), `UnsupportedError` (-> HTTP 400 HTML), `RuntimeQueryError` (-> Overpass `remark`, HTTP 200). |
| `rust/osmpq-raw/` | The planet-scale Rust producer: PBF -> `raw/` (histogram, nodes, ways, relations passes). See [docs/cli.md](cli.md#osmpq-raw-rust-producer). |
| `deploy/cloudflare/` | The Cloudflare Worker + Containers deployment: `src/index.ts` (routing, rate limiting, caching, attribution), `src/engine.ts`/`src/updater.ts` (Container/Durable Object classes), `src/logic.ts` (pure, unit-tested logic). `docs/m3-runbook.md` covers deploying it. |
| `tools/` | Harness and one-off scripts, not part of the installed package. See the table in [docs/cli.md](cli.md#tools). |
| `tests/` | The pytest suite plus `tests/corpus/` (the differential-test query corpus and its bboxes/self-test) and `tests/fixtures/` (synthetic dataset builders). See below. |

## How a query flows through the code

```
HTTP request (server.py)
  -> osmpq.ql.parse(text)                     lexer -> parser -> ast.Program
  -> Engine.run_program(program, ...)         executor.py
       -> planner.run_program                  checks settings, dispatches statements
            -> planner.execute_query            base SELECT (spatial/byid/set/recurse path)
                 -> hooks.FILTER_HOOKS[...]      tier-2 filters: implied_bbox() narrows cell/row-group
                                                  selection, predicate() adds a SQL WHERE clause
            -> hooks.STATEMENT_HOOKS[...]        tier-2 statements the core planner doesn't handle
            -> engine.setops / engine.recurse     set algebra, forward/backward recursion
       -> engine.render.build_elements           materialized set -> Overpass JSON-shaped dicts
  -> Result.render()                            result.py: JSON / XML / CSV envelope
  -> HTTP response
```

Concretely: `sources.py` builds the SQL that actually reads Parquet
(`read_parquet([...cell files...])` for a bbox scan, or an id lookup
against the byid parts), scoped by `catalog.py`'s manifest-driven cell and
row-group selection; `[date:]`/`retro`/`diff`/`adiff` redirect those same
reads through history files instead (`attic.py`, via the
`catalog.SNAPSHOT` context variable) without the rest of the pipeline
needing to change. Everything above `current_rows`/`byid_current_rows` —
hydration, `>`/`<` recursion, `out geom`, areas — works unchanged whether
it's reading current data or a snapshot.

## Extension points

New tier-2/tier-3 language features are added as a **new module**, never
by editing `planner.py` (`docs/m3-contracts.md` section 2, implemented in
`src/osmpq/engine/hooks.py`):

- **A new filter** (e.g. a new `(...)` predicate): implement
  `FilterHook.implied_bbox(ctx, q, f)` (return the bbox this filter can
  guarantee results are inside, in degrees, or `None` if it can't bound
  one) and `FilterHook.predicate(ctx, q, f, alias)` (a SQL boolean
  expression over `alias`'s canonical columns —
  `osmpq.engine.schema.CANONICAL_COLUMNS`), then call
  `hooks.register_filter(YourFilterAstClass, your_hook_instance)` at
  import time.
- **A new statement** (e.g. a new block or standalone statement the core
  planner doesn't know): implement a function
  `fn(ctx, stmt) -> None` that mutates `ctx` the way any other statement
  does (recursing into `planner.execute_statement` for nested bodies),
  then call `hooks.register_statement(YourStmtAstClass, fn)`.
- **A new `area`-typed query source**: call
  `hooks.set_area_query_hook(fn)`.
- Register your module in `hooks.HOOK_MODULES` so `planner` imports it
  lazily on first use; a module not yet merged simply means "unsupported"
  for its AST classes, never an import error.

Rules from `docs/m3-contracts.md` section 2: the planner computes the
**effective bbox** as the explicit/global bbox intersected with every
hooked filter's `implied_bbox` (an empty intersection short-circuits to
an empty result); it wraps the base SELECT as `SELECT * FROM (<base>)
__q WHERE p1 AND p2 ...` with each hook's `predicate` called with alias
`"__q"`. Predicates may create their own TEMP tables via
`ctx.fresh_name`. `geometry` is only populated for ways read from spatial
files (NULL from byid parts and for relations) — a hook must degrade
gracefully; `lat_e7`/`lon_e7` (nodes) and `xmin_e7..ymax_e7`
(ways/relations) are always present. `ctx.manifest`, `ctx.promoted_keys`,
`ctx.con` (this run's cursor), `ctx.files_read` and `ctx.warnings` are
available to any hook.

## Dataset layout and manifest versions

A dataset root (a local directory or `s3://bucket/prefix`) holds
cell-partitioned spatial Parquet (Hilbert-sorted, tagged/untagged split
for nodes), id-sorted byid parts, `node_way`/`member` reverse-membership
indexes, a row-group index side table, derived `area` tables, optional
rolling delta tiers (`delta/<generation>/{hour,day,week}/`) written by the
updater, an optional `history/<generation>/` attic dataset, and
`manifest/<n>.json` + `manifest/LATEST`. Manifest versions accumulate
fields as milestones added capability (v1: M0 base layout; v2: ancestor
depths + row-group index, M1; v3: `deltas`, M2; v4: `areas`, M3; v5:
`history`, M4) — `osmpq.layout.manifest` and `osmpq.engine.catalog` both
read/write it, and `osmpq manifest <root>` prints a live summary. The
exact schemas and rules live in the milestone contracts, in order:
`docs/m0-contracts.md` (layout, manifest v1, tier-1 API),
`docs/m1-contracts.md` (Rust producer, layout v2, caching),
`docs/m2-contracts.md` (delta tiers, updater, compaction/gc),
`docs/m3-contracts.md` (tier-2 language, service hardening, Cloudflare),
`docs/m4-contracts.md` (history/attic). `docs/design.md` explains the
reasoning behind the layout; the contracts say exactly what it is.

## Test suite

```sh
uv run pytest tests -q
```

`tests/conftest.py` puts this worktree's own `src/` first on `sys.path`
so the tests always exercise the code actually checked out here,
regardless of what editable `osmpq` install exists elsewhere.

Most tests run against **synthetic fixtures** built directly with DuckDB
(`tests/fixtures/make_fixture.py` and friends) rather than a real PBF —
tiny, hand-specified datasets (a handful of leaf cells, ~36 nodes, 10
ways, 3 relations, well-known ids returned as a `FixtureInfo` so tests
don't have to re-derive them) that exercise the layout rules
(`docs/m0-contracts.md` sections 1-4) without needing any external data.

A smaller set of end-to-end tests (`tests/test_build_e2e.py`,
`tests/test_compact.py`, `tests/test_gc.py`, `tests/test_update_e2e.py`,
`tests/test_raw_rust.py`) instead build a real dataset from
`data/bermuda-latest.osm.pbf` (a ~2 MB extract) and are **skipped** when
that file isn't present (`pytest.mark.skipif`). To run them, download the
Bermuda extract from Geofabrik and save it at that path:

```sh
mkdir -p data
curl -o data/bermuda-latest.osm.pbf https://download.geofabrik.de/central-america/bermuda-latest.osm.pbf
```

(`data/` is git-ignored — see `docs/m0-contracts.md` section 0. If
Geofabrik has reorganized its regions since this was written, search its
download index for "Bermuda".) `tests/test_raw_rust.py` additionally
needs the Rust binary built (`cargo build --release` in
`rust/osmpq-raw/`) and is skipped without it.

`tests/corpus/selftest.py` is a **plain script, not a pytest module** (so
other agents' test discovery doesn't pick it up) that exercises the
differential-test comparison logic (`compare()`,
`compare_diff_actions()`, `compare_timeline()`, `should_apply_date()`,
...) directly against hand-written JSON/XML fixtures, with no network
access:

```sh
uv run python tests/corpus/selftest.py
```

## Reference harness (`tools/difftest.py`)

`tools/difftest.py` runs the shared query corpus (`tests/corpus/*.overpassql`)
against a real Overpass instance and against a local `osmpq` server and
diffs the results — see `tools/README.md` for the full flag reference and
[docs/cli.md](cli.md#tools) for a one-line summary. The flags relevant to
attic (M4) grading:

| Flag | Meaning |
| --- | --- |
| `--reference-only` | Fetch and cache reference responses under `tests/corpus/.cache/`; don't call `--local`. Use this to front-load reference calls when the reference is rate-limited or briefly unavailable. |
| `--local-only` | Only call `--local`, comparing against previously cached reference responses — no network to the reference needed. |
| `--date DATE` | Pins corpus 01-49 (which know nothing about attic settings) to a fixed instant on the **reference side only**; skipped automatically for any query that already carries its own attic time setting (`[date:]`, `retro`, `timeline`, `[diff:]`/`[adiff:]`). |
| `--local-date DATE` | The same, but for the **local** side: lets a local dataset that has moved past the cached reference answers be asked for the state as of the reference's date (needs a history dataset). |
| `--history-since DATE` | ISO instant the local history starts at; reference `timeline` entries that expired at or before it are not expected locally and are excluded from the comparison. |

The reference response cache lives under `tests/corpus/.cache/` (one JSON
file per `(query file, bbox, dated query text)` cache key; gitignored,
not committed) — re-running `--local-only` or `--reference-only` reuses
it without re-hitting the reference.

Corpus queries are graded against a fixed set of **six bboxes**
(`tests/corpus/bboxes.json`): `downtown_minneapolis`, `suburb_edina`,
`farmland_southern_mn`, `lake_minnetonka`, `duluth_harbor`, and `tiny`.
Most queries run once, against the first bbox in that file; a handful of
geography-sensitive ones (`ALL_BBOX_PATTERNS` in `difftest.py`) run
against all six. See `tools/README.md`'s "Adding a corpus query" section
for how to add a new one.

## Linting

```sh
uv run ruff check src tools tests
```

The `ruff` configuration lives in `pyproject.toml` (owned by the project
coordinator).

## Continuous integration

`.github/workflows/ci.yml` (owned by the project coordinator) runs lint
and the test suite on every pull request, using `astral-sh/setup-uv` and
`uv sync --extra dev` to set up the same environment described above
before invoking `ruff check` and `pytest`.

## Conventions

- **Contracts before code, reports after.** Each milestone's exact
  interfaces are written down in `docs/mN-contracts.md` before the
  corresponding code is built; what was actually delivered, measured and
  found is written up afterwards in `docs/mN-report.md`. Change the
  contract first if it needs to change.
- **No model names in artifacts.** Nothing under `docs/`, commit
  messages' visible content, or code comments names a specific model.
- **Queries without attic settings stay byte-identical in SQL.** Adding
  `[date:]`/`retro`/`diff`/`adiff` support must not change the SQL a
  plain (non-attic) query compiles to — enforced by a no-regression test
  that asserts `catalog.SNAPSHOT` is never set and no history file is
  read for such a query (`docs/m4-contracts.md` section 3.3).
- **Tests on fixtures, plus a corpus entry per feature.** A new filter or
  statement gets unit/fixture-level tests in `tests/` *and* at least one
  `tests/corpus/NN_*.overpassql` entry that can be graded against the
  reference with `tools/difftest.py`.
- Milestone/workstream progress and what's currently in flight is tracked
  in `docs/progress.md` (owned by the project coordinator), not in this
  file.

## See also

- [docs/api.md](api.md) — the HTTP API.
- [docs/cli.md](cli.md) — every `osmpq`/`osmpq-raw` subcommand and the
  `tools/` scripts.
- [docs/overpass-ql-support.md](overpass-ql-support.md) — the Overpass QL
  feature matrix.
- `docs/design.md` — why the system is shaped this way.
- `docs/m0-contracts.md` through `docs/m4-contracts.md` and their
  matching `-report.md` files — the exact, milestone-by-milestone
  contracts and results.
