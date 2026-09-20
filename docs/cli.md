# Command-line tools

Two binaries build and run a dataset: the Rust producer `osmpq-raw`
(`rust/osmpq-raw`, one-time or occasional, planet-scale passes) and the
Python `osmpq` CLI (`src/osmpq/cli.py`, everything else — building without
Rust, deriving areas, updating, history, validating, serving). This
document lists every subcommand and flag as implemented, a typical
end-to-end sequence for building and serving a regional extract, and the
one-off scripts under `tools/`.

See [docs/api.md](api.md) for the HTTP surface `osmpq serve` exposes, and
[docs/development.md](development.md) for setup and the repository map.
Each subcommand also points at the contract section that specifies its
exact behavior (`docs/m0-contracts.md` through `docs/m4-contracts.md`).

## Setup

```sh
uv sync --extra dev                            # Python env from pyproject.toml/uv.lock
(cd rust/osmpq-raw && cargo build --release)   # optional: only needed for `osmpq-raw`
uv run osmpq --help
```

`pip install -e '.[dev]'` still works for anyone without `uv`; see
[docs/development.md](development.md#setup) for details. Every `osmpq ...`
invocation below can equally be run as `uv run osmpq ...` from a checkout
that hasn't activated its virtualenv.

## `osmpq` subcommands

### `osmpq raw-py <input> <rawdir>`

Produces the `raw/` layout from a PBF using DuckDB only — no Rust binary
needed (`src/osmpq/build/raw.py`, `docs/m1-contracts.md` sections 1 and
3). Slower than `osmpq-raw build` and untagged nodes get no metadata.

| Option | Default | Meaning |
| --- | --- | --- |
| `input` | *(required)* | Source `.osm.pbf`. |
| `rawdir` | *(required)* | Output `raw/` directory. |
| `--bbox S,W,N,E` | none | Cut the PBF to this bbox first. |
| `--max-nodes-per-cell` | `1000000` | Quadtree leaf split threshold. |
| `--max-depth` | `13` (`cells.DEFAULT_MAX_DEPTH_V2`) | Maximum quadtree depth. |
| `--promoted-keys` | the built-in 12-key list | Comma-separated tag keys to promote to columns. |
| `--threads` | DuckDB default | DuckDB `threads`. |
| `--memory-limit` | DuckDB default | DuckDB `memory_limit`. |
| `--tmpdir` | DuckDB default | DuckDB `temp_directory`. |

### `osmpq build <input> <root>`

Builds a full dataset root. Two modes:

- **From a PBF** (default): runs `raw-py` into a temp `raw/` directory,
  then the `--raw` stage below (`src/osmpq/build/builder.py:build`,
  `docs/m1-contracts.md` section 7 — this is the M0-compatible entry
  point).
- **`--raw`**: `input` is an existing `raw/` directory (from `raw-py` or
  the Rust `osmpq-raw build`); relations, byid/index tables, the
  row-group index and the manifest are built from it
  (`build_from_raw`).

| Option | Default | Meaning |
| --- | --- | --- |
| `input` | *(required)* | PBF path, or a `raw/` dir with `--raw`. |
| `root` | *(required)* | Output dataset root. |
| `--raw` | off | Treat `input` as a `raw/` directory. |
| `--bbox S,W,N,E` | none | Cut bbox (ignored with `--raw`; cut at the `raw-py`/`osmpq-raw` stage instead). |
| `--extent S,W,N,E` | data bbox from `raw/summary.json` | Intended coverage recorded in the manifest (with `--raw`); wider than the cut bbox since a kept way keeps all its nodes. Read by the updater to decide whether a new element belongs in this extract. |
| `--generation` | auto (`g0001`, ...) | Generation id to write. |
| `--max-nodes-per-cell` | `1000000` | Ignored with `--raw`. |
| `--max-depth` | `13` | Ignored with `--raw`. |
| `--promoted-keys` | built-in 12-key list | Ignored with `--raw`. |
| `--timestamp` | from the PBF/raw summary | `timestamp_osm_base` to record. |
| `--replication-sequence` | none | Starting replication sequence for the updater. |
| `--threads`, `--memory-limit`, `--tmpdir` | DuckDB defaults | Passed through to every DuckDB step. |
| `--link` / `--copy` / `--move` | `--link` | How `raw/` files are placed into `root` (hardlink, copy, or move; mutually exclusive). |
| `--no-areas` | off | Skip the `osmpq areas` step normally run at the end (`docs/m3-contracts.md` section 4.3). |

### `osmpq areas <root>`

Derives the area table for the current generation — closed ways and
multipolygon/boundary relations that qualify as areas
(`src/osmpq/build/areas.py`, `docs/m3-contracts.md` section 4, amended by
its section 9). Run automatically at the end of `osmpq build` unless
`--no-areas`; re-run standalone after any other change to current data
(e.g. after `osmpq compact`, though `compact` does this itself).

| Option | Default |
| --- | --- |
| `root` | *(required)* |
| `--threads`, `--memory-limit`, `--tmpdir` | DuckDB defaults |

### `osmpq manifest <root>`

Prints a human-readable summary of `manifest/LATEST` for `root`:
generation, schema/manifest version, `timestamp_osm_base`, replication
sequence, source, extent, promoted keys, leaf cell count, per-table row
and byte counts, byid/index part counts, the row-group index paths (v2+),
areas summary, and (if present) the history section's generation, since
timestamp, minor-version counts and tiers. Read-only; no options besides
`root`.

### `osmpq validate <root>`

Checks a dataset root against the manifest contract
(`src/osmpq/build/validate.py`, `docs/m1-contracts.md` section 7): every
manifest path exists, row counts match, byid parts are sorted with
non-overlapping id ranges, spatial files are sorted by `(hilbert, id)`,
every way/relation's cell contains its bbox under the v2 depth rule, the
row-group index covers every spatial file, and (when a `history` section
is present) the corresponding history checks. Prints a summary and exits
`0` on success, `1` if any check failed.

### `osmpq update <root>`

Applies pending minutely replication diffs (`src/osmpq/update/updater.py`,
`docs/m2-contracts.md` section 5). Stateless: everything it needs comes
from the dataset root and the replication source.

| Option | Default | Meaning |
| --- | --- | --- |
| `root` | *(required)* | Local directory or `s3://` root. |
| `--source` | the manifest's own `replication_source` | Override the replication source URL. |
| `--once` / `--follow` | `--once` | Apply one batch and exit, or poll the source and keep applying (mutually exclusive). |
| `--max-diffs` | `60` | Diffs applied per batch. |
| `--tmpdir` | `.osmpq-update-tmp` under cwd | Scratch DuckDB database + downloaded `.osc.gz`/`.state.txt` cache; deleted at the end of each run. |
| `--threads`, `--memory-limit` | DuckDB defaults | For the run's scratch database. |
| `--poll-interval` | `30.0` seconds | Sleep between polls under `--follow`. |

### `osmpq history init <root>`

Starts a history (attic) dataset from the root's **current** tables: every
current row becomes its first known state (`minor=0`, `valid_from =
timestamp_osm_base`); no PBF is re-read (`src/osmpq/history/init.py`,
`docs/m4-contracts.md` section 4). This is the practical way to add
history to an existing regional dataset — `osmpq update` then appends
every later version. Prints `since`, row count, byte count and elapsed
seconds.

| Option | Default |
| --- | --- |
| `root` | *(required)* |
| `--threads`, `--memory-limit`, `--tmpdir` | DuckDB defaults |

### `osmpq history build <root>`

Builds history from a raw object-version stream instead — either a base
PBF plus `.osc`/`.osc.gz` diffs, or a full-history `.osh.pbf`
(`src/osmpq/history/build.py`, `docs/m4-contracts.md` section 4). Writes
`history/<gen>/` and a manifest v5.

| Option | Default | Meaning |
| --- | --- | --- |
| `root` | *(required)* | |
| `--pbf` | none | Base extract PBF (first state of every element); mutually exclusive with `--osh`. |
| `--osc` | none | A `.osc`/`.osc.gz` file or a directory of them, sorted by sequence; repeatable. |
| `--osh` | none | A full-history `.osh.pbf`; mutually exclusive with `--pbf`/`--osc`. |
| `--threads`, `--memory-limit`, `--tmpdir` | DuckDB defaults | |

Exactly one of `--osh` or `--pbf` (with optional `--osc`) must be given.
As of `docs/m4-report.md`, this path is correct on its fixtures but its
node pass does not fit in a memory-constrained sandbox at Minnesota scale
— `history init` + `osmpq update` is what actually built the shipped
Minnesota history dataset; `history build` remains the path for a genuine
full-history `.osh.pbf` load.

### `osmpq compact <root>`

Folds every present delta tier into a new base generation
(`src/osmpq/build/compact.py`, `docs/m2-contracts.md` section 6): touched
spatial cells and byid parts are rewritten, untouched files are
hardlinked, `node_way`/`member` indexes and the row-group index are
rebuilt, and (when history is present) the history tiers are folded into
the base history with `valid_to` filled in. Writes a new manifest with
`deltas: {}`. Note this is dispatched specially in `osmpq`'s `main()`
(`argv[0] == "compact"`) rather than through the normal subparser, so its
own `--help` is `osmpq compact --help`, not listed under `osmpq --help`.

| Option | Default |
| --- | --- |
| `root` | *(required)* |
| `--generation` | auto |
| `--threads`, `--memory-limit`, `--tmpdir` | DuckDB defaults |

### `osmpq gc <root>`

Deletes files under `root` that no manifest among the newest `--keep`
manifests references, and deletes the older manifest JSON files
themselves (`src/osmpq/build/gc.py`, `docs/m2-contracts.md` section 6).
Also dispatched specially in `main()`, like `compact`.

| Option | Default |
| --- | --- |
| `root` | *(required)* |
| `--keep` | `2` | Number of newest manifests to keep. |
| `--dry-run` | off | Print what would be removed without deleting. |

### `osmpq serve`

Runs `osmpq.server:app` (the HTTP API — see [docs/api.md](api.md)) under
uvicorn (`docs/m3-contracts.md` section 6.1). The dataset root and every
behavior knob (rate limits, timeouts, S3 credentials, ...) come from
`OSMPQ_*` environment variables the app itself reads at request time —
this subcommand only owns the bind address, port and worker count.
**`OSMPQ_ROOT` must be set in the environment** before running it.

| Option | Default |
| --- | --- |
| `--host` | `0.0.0.0` |
| `--port` | `8080` |
| `--workers` | `1` |

```sh
OSMPQ_ROOT=root/ uv run osmpq serve --port 8080
```

### `osmpq updater-server`

Runs `osmpq.update.server:app` (the updater's control API) under uvicorn
(`docs/m3-contracts.md` section 6.3): `POST /run` executes one
`run_once`, `GET /status` reports whether a run is in progress plus the
last summary and the root's manifest number/`timestamp_osm_base`, `GET
/healthz` is a plain liveness check. A `POST /run` while one is already
running returns 409. Its own environment variables (`OSMPQ_ROOT`,
`OSMPQ_REPLICATION_SOURCE`, `OSMPQ_UPDATE_MAX_DIFFS` (60),
`OSMPQ_UPDATE_TMPDIR`, `OSMPQ_UPDATE_THREADS`,
`OSMPQ_UPDATE_MEMORY_LIMIT`) are documented in
`src/osmpq/update/server.py`'s module docstring.

| Option | Default |
| --- | --- |
| `--host` | `0.0.0.0` |
| `--port` | `8081` |
| `--workers` | `1` |

## `osmpq-raw` (Rust producer)

`rust/osmpq-raw` (`cargo build --release`, binary at
`rust/osmpq-raw/target/release/osmpq-raw`). Two subcommands
(`rust/osmpq-raw/src/main.rs`):

### `osmpq-raw build <input> <rawdir>`

The planet-scale `raw/` producer (`docs/m1-contracts.md` section 3,
passes 1-5: node histogram, nodes, ways, relations, summary). Faster than
`osmpq raw-py` and preserves metadata on untagged nodes.

| Option | Default | Meaning |
| --- | --- | --- |
| `input` | *(required)* | Source `.osm.pbf`. |
| `rawdir` | *(required)* | Output `raw/` directory. |
| `--max-nodes-per-cell` | `1000000` | Quadtree leaf split threshold. |
| `--max-depth` | `13` | Maximum quadtree depth. |
| `--threads` | all cores | Rayon thread pool size. |
| `--promoted-keys` | the built-in 12-key list | Comma-separated tag keys to promote. |
| `--node-store` | `auto` | `sorted-mem`, `dense-file`, or `auto` (picks `sorted-mem` under `--sorted-mem-max` nodes, else `dense-file`). |
| `--flat-nodes` | `<tmpdir>/nodes.flat` | Backing file for `dense-file` node storage. |
| `--tmpdir` | `<rawdir>/_tmp` | Scratch directory. |
| `--bbox` | none | `S,W,N,E` cut bbox. |
| `--sorted-mem-max` | `400000000` | Node-count threshold for the `auto` node-store choice. |

### `osmpq-raw node-way-index <rawdir>`

Optional separate pass: builds `rawdir/node_way/` from
`rawdir/way/*.parquet`, for when this index wasn't produced (or needs
rebuilding) as part of `build`.

| Option | Default |
| --- | --- |
| `rawdir` | *(required)* |
| `--threads` | all cores |
| `--tmpdir` | none |

## Typical end-to-end sequence: a regional extract

```sh
# 1. Rust producer: PBF -> raw/
osmpq-raw build extract.osm.pbf raw/

# 2. raw/ -> a dataset root (relations, byid/index tables, manifest, areas)
uv run osmpq build --raw raw/ root/

# 3. (areas already ran as part of build; re-run standalone if ever needed)
uv run osmpq areas root/

# 4. Start a history (attic) dataset from the current tables
uv run osmpq history init root/

# 5. Catch up on replication diffs since the base timestamp
uv run osmpq update root/ --once
# repeat, or use --follow to poll continuously

# 6. Fold delta tiers back into the base, then drop old generations
uv run osmpq compact root/
uv run osmpq gc root/ --keep 2

# 7. Check the result against the manifest contract
uv run osmpq validate root/

# 8. Upload to object storage
uv run python tools/upload_root.py root/ s3://<bucket>/<prefix>

# 9. Serve from local disk, or directly from s3://
OSMPQ_ROOT=root/ uv run osmpq serve --port 8080
# or, with OSMPQ_S3_KEY_ID/OSMPQ_S3_SECRET/OSMPQ_S3_ENDPOINT set:
OSMPQ_ROOT=s3://<bucket>/<prefix> uv run osmpq serve --port 8080
```

`osmpq build extract.osm.pbf root/` (no `--raw`) does steps 1-2 without
the Rust binary, using `osmpq raw-py` internally instead — slower, and
untagged nodes get no metadata. See `docs/m1-runbook.md` for the
planet-scale version of this sequence and `docs/m3-runbook.md` for taking
a built root all the way to a public Cloudflare endpoint with minutely
updates.

## `tools/`

One-off and harness scripts, not installed as part of the `osmpq` package
— run with `uv run python tools/<script>.py ...` (or `python tools/...`
inside an activated environment). `tools/README.md` covers `difftest.py`
in full detail.

| Tool | Purpose |
| --- | --- |
| `tools/difftest.py` | Differential test harness: runs a shared corpus of Overpass QL queries against a reference Overpass instance and a local `osmpq` server and compares results (`docs/m0-contracts.md` section 9, `tools/README.md`). Supports two-phase (reference-cache then local-only) runs, `[diff:]`/`[adiff:]` action-list comparison, `timeline` multiset comparison, and `--date`/`--local-date`/`--history-since` for attic queries (`docs/m4-report.md` section 2). |
| `tools/diffcheck.py` | Samples ids touched since the dataset's base (from delta byid files, or an `--ids` file), queries both a local endpoint and the reference at `[date:"T"]`, and compares existence/version/tags/coordinates/refs/members (`docs/m2-contracts.md` section 7.1). |
| `tools/remote_profile.py` | Runs corpus queries through the engine against an HTTP root and counts range requests and bytes per query, using `tools/range_server.py`'s request log. |
| `tools/range_server.py` | A static file server with HTTP Range support that logs one line per request (method, path, byte range), used as the HTTP root for `remote_profile.py`. |
| `tools/bbox_extract.py` | Cuts a reference-complete bbox extract out of a larger PBF (osmium `--strategy smart`-like semantics): ids selected in DuckDB, the copy done via pyosmium filters. |
| `tools/m4_dataset.sh` | Builds the Minnesota M4 history dataset end to end: `copy`, `fetch`, `history`, `update`, `curcheck` stages, or `all` (`docs/m4-contracts.md` section 7). |
| `tools/m4_fetch_diffs.py` | Fetches consecutive minutely replication diffs into a local cache, used by `m4_dataset.sh fetch`; safe to re-run to top up to the source's latest. |
| `tools/upload_root.py` | Uploads a dataset root to an S3-compatible bucket (R2), manifest last so a reader never sees a manifest before its files; skips files already present with the same size, so an interrupted run can resume. |

## See also

- [docs/api.md](api.md) — the HTTP API `osmpq serve` exposes.
- [docs/development.md](development.md) — setup, the repository map, the
  test suite, and how to extend the query engine.
- `docs/m0-contracts.md` through `docs/m4-contracts.md` — the exact
  contracts each subcommand implements.
- `docs/m1-runbook.md`, `docs/m3-runbook.md` — planet-scale build and
  Cloudflare deployment runbooks.
