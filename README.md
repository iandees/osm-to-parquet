# osm-to-parquet

An Overpass-API-compatible query service for OpenStreetMap whose data lives
on cheap object storage (Cloudflare R2, S3) instead of a large local
database, kept current with the minutely replication diffs, with history.

The public Overpass API is a great tool, but running your own instance means
600+ GB of fast local SSD per server and a single-writer update process,
repeated for every read replica. This project takes the opposite trade: the
data sits in cloud-optimized Parquet files on R2, DuckDB reads them with
HTTP range requests, and an Overpass QL front end translates each query into
SQL over that layout. Queries mostly wait on I/O, so the query engine runs
serverless (a Cloudflare Worker in front of Containers that sleep when
idle); the only stateful process is a small, stateless-by-design updater.

Status: milestones M0-M4 are done on a Minnesota extract; the planet build
and a real Cloudflare deploy have not been run yet. Details and what is
left: [docs/progress.md](docs/progress.md).

## What works

- **Overpass QL**: the tier-1 and tier-2 language — bbox, tag and id
  filters, recursion (`>`, `<`, `>>`, `<<`, `(w)`, `(bn)`, ...), unions
  and differences, `around`, `poly`, areas (`area[...]`, `(area)`,
  `(pivot)`, `is_in`, `map_to_area`), `newer`/`changed`/`user`/`uid`,
  `(if:)`, `foreach`, `if`, every `out` mode incl. `geom(bbox)`, JSON, XML
  and csv output. See [docs/overpass-ql-support.md](docs/overpass-ql-support.md).
- **History (attic)**: `[date:]`, `retro`, `timeline`, `[diff:]`/`[adiff:]`
  and an exact `(changed:)`, from a history dataset the updater keeps
  appending to.
- **Compatibility**: graded query by query against a public Overpass
  instance with [`tools/difftest.py`](tools/README.md); 77 of 89 corpus
  rows match on Minnesota, the rest are documented differences.
- **Operations**: `osmpq serve` with Overpass-shaped rate limits and status
  endpoints; a stateless updater that applies minutely diffs to a root on
  local disk or `s3://`; compaction and gc; a container image; a Cloudflare
  Worker + Containers deployment with a Durable Object scheduler.

## Quick start

Query the Minnesota dataset that already lives on R2 (you need read
credentials for the bucket; the engine needs nothing on local disk):

```
git clone https://github.com/iandees/osm-to-parquet.git && cd osm-to-parquet
uv sync                                   # Python 3.11+, https://docs.astral.sh/uv/
export OSMPQ_ROOT=s3://osm-parquet/minnesota
export OSMPQ_S3_KEY_ID=... OSMPQ_S3_SECRET=... OSMPQ_S3_ENDPOINT=<account>.r2.cloudflarestorage.com
uv run osmpq serve --port 8080
curl 'http://127.0.0.1:8080/api/interpreter' --data-urlencode \
  'data=[out:json];node["amenity"="cafe"](44.97,-93.28,44.985,-93.255);out;'
curl 'http://127.0.0.1:8080/api/interpreter' --data-urlencode \
  'data=[out:json][date:"2026-09-19T06:00:00Z"];node["amenity"="cafe"](44.97,-93.28,44.985,-93.255);out meta;'
```

Point overpass turbo at `http://127.0.0.1:8080/api/` (Settings → Overpass
API Server) and it works as a front end.

Build your own dataset from a Geofabrik extract:

```
(cd rust/osmpq-raw && cargo build --release)          # Rust producer (optional: `osmpq build extract.osm.pbf root/` works without it)
rust/osmpq-raw/target/release/osmpq-raw build minnesota-latest.osm.pbf raw/
uv run osmpq build --raw raw/ root/                   # relations, indexes, areas, manifest
uv run osmpq history init root/                       # start the history from the current tables
uv run osmpq update root/ --source https://download.openstreetmap.fr/replication/north-america/us-midwest/minute --max-diffs 60
uv run osmpq validate root/
uv run osmpq serve --port 8080                        # OSMPQ_ROOT=root/
```

`uv run python tools/upload_root.py root/ s3://bucket/prefix` puts it on
R2; [docs/m3-runbook.md](docs/m3-runbook.md) takes it from there to a
public endpoint with minutely updates.

## How it works

1. **One snapshot, two copies.** Nodes, ways and relations as Parquet in a
   spatial copy (adaptive quadtree cells, Hilbert-sorted, so a bbox query
   touches a few files and row groups) and an id-sorted copy for id lookups
   and for the updater. Ways and relations carry their resolved geometry
   and bbox, so `out geom`, `around` and areas never join back to nodes over
   the network; node references are kept so recursion works like Overpass.
2. **Minutely updates as rolling deltas.** A stateless container fetches
   the diffs, looks up the current state of what they touch from the
   id-sorted copy, re-resolves geometry and rewrites hour/day/week delta
   files that prune like the base. Deltas fold into a new base generation
   at compaction. The same run appends every new state to the history.
3. **Overpass QL → SQL.** A parser, a planner that turns each statement
   into SQL over the manifest's files, DuckDB with the spatial extension,
   and a renderer that produces the Overpass envelope. Attic queries flip
   one switch in the scan layer to read the history at a point in time.
4. **Serverless serving.** A Worker caches and rate-limits; containers run
   the engine and sleep when idle; a Durable Object schedules the updater.

The full design, cost model and roadmap: [docs/design.md](docs/design.md).

## Documentation

| | |
| --- | --- |
| [docs/api.md](docs/api.md) | The HTTP API: endpoints, formats, limits, configuration |
| [docs/cli.md](docs/cli.md) | Every `osmpq` and `osmpq-raw` command and the tools |
| [docs/development.md](docs/development.md) | Setup, code map, tests, harness, conventions |
| [docs/overpass-ql-support.md](docs/overpass-ql-support.md) | Overpass QL feature matrix and documented differences |
| [docs/design.md](docs/design.md) | Architecture: layout, updates, query translation, serving, costs, roadmap |
| [docs/prior-art.md](docs/prior-art.md) | Existing projects and what each contributes or lacks |
| [docs/progress.md](docs/progress.md) | Milestone status, measurements, what is left |
| [docs/m1-runbook.md](docs/m1-runbook.md), [docs/m3-runbook.md](docs/m3-runbook.md) | Building the planet; deploying to Cloudflare |
| `docs/m0-` … `m4-contracts.md`, `-report.md` | Per-milestone contracts and reports |

## Development

```
uv sync --extra dev
uv run ruff check src tools tests
uv run pytest tests -q
```

CI runs lint, the tests, the Rust build and the Worker's typecheck and
tests on every pull request. The repository name is historical: Parquet is
a means, not the goal.
