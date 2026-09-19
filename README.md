# osm-to-parquet

**Goal:** an Overpass-API-compatible query service for the full OpenStreetMap
planet whose data lives on cheap object storage (Cloudflare R2, S3) instead of
a large local NVMe database, kept current with the minutely replication diffs.

The public Overpass API is a great tool, but running your own instance means
provisioning roughly 600+ GB of fast local SSD per server plus a single-writer
update process, and every read replica repeats that cost. This project explores
the opposite trade: put the planet in cloud-optimized columnar files on
Cloudflare R2, use an embedded analytical engine (DuckDB) with HTTP range
reads as the execution layer, and translate Overpass QL into queries against
that layout. Queries mostly wait on I/O, so the query engine runs serverless
(Cloudflare Workers in front of Cloudflare Containers that sleep when idle);
the only always-on, stateful machine is the small updater.

Decisions so far: R2 for storage; serverless query compute with containers
that shut down quickly until there is real traffic; a public endpoint; the
initial load runs on a laptop or a rented node while minutely updates run in
a cloud container, which forces the updater to be stateless; "most real
queries work" rather than byte-identical Overpass compatibility; full history
back to 2012 and earlier as a real goal, prototyped on small regional history
extracts first. See the decisions table at the top of the design document.

Status: **M3 done on Minnesota.** A Rust producer (`rust/osmpq-raw`)
turns a PBF into the layout, a Python/DuckDB stage finishes it, a stateless
updater keeps it current from minutely diffs (rolling delta tiers, periodic
compaction), and a Python engine serves Overpass QL over it (local disk,
HTTP, or R2) with tier-1 and tier-2 language support: recursion, areas,
`around`, `poly`, `is_in`, meta filters, `foreach`/`if`, csv. The service
has Overpass-shaped rate limits and status endpoints, a container image,
and a Cloudflare Worker + Containers deployment with a Durable Object
scheduler for the updater (`deploy/cloudflare`, `docs/m3-runbook.md`). The
planet build has not been run yet (`docs/m1-runbook.md`). The repository
name is historical and Parquet is a means, not the goal.

## Documents

| Document | What it covers |
| --- | --- |
| [docs/prior-art.md](docs/prior-art.md) | Survey of existing projects (Overpass, Postpass, ohsome-planet, QLever, OSMExpress, GeoDesk, QuackOSM, osm-pds, DuckLake, R2 Data Catalog, ...) and what each one contributes or lacks |
| [docs/design.md](docs/design.md) | Proposed architecture: storage layout, update pipeline, query translation, serving tier, cost model, roadmap, open questions |
| [docs/overpass-ql-support.md](docs/overpass-ql-support.md) | Overpass QL feature matrix and the order we intend to implement it in |
| [docs/m0-contracts.md](docs/m0-contracts.md), [docs/m0-report.md](docs/m0-report.md) | M0: exact layout, schemas, API and the Minnesota results of the first prototype |
| [docs/m1-contracts.md](docs/m1-contracts.md), [docs/m1-report.md](docs/m1-report.md) | M1: Rust producer, layout v2 (restricted ancestor depths, row-group index, tuned encodings), engine caching, measurements |
| [docs/m1-runbook.md](docs/m1-runbook.md) | How to build the planet on your own machine and publish it to R2 |
| [docs/m2-contracts.md](docs/m2-contracts.md), [docs/m2-report.md](docs/m2-report.md) | M2: delta tiers, the stateless minutely updater, compaction, gc, diffcheck against the reference |
| [docs/m3-contracts.md](docs/m3-contracts.md), [docs/m3-report.md](docs/m3-report.md) | M3: tier-2 language (areas, around, poly, is_in, meta filters, evaluators), service limits, image, updater on R2, Cloudflare deployment |
| [docs/m3-runbook.md](docs/m3-runbook.md) | From a built dataset on R2 to a public endpoint with minutely updates |

## Running it

```
pip install -e '.[dev]'                       # Python side (DuckDB 1.5, pyarrow, FastAPI)
(cd rust/osmpq-raw && cargo build --release)  # Rust producer
osmpq-raw build extract.osm.pbf raw/          # PBF -> raw layout
osmpq build --raw raw/ root/                  # raw -> dataset root (relations, indexes, manifest)
osmpq validate root/
osmpq serve --port 8080                       # OSMPQ_ROOT=root/; /api/interpreter, /api/status, /healthz
osmpq update root/ --once                     # apply pending minutely diffs (see docs/m2-contracts.md)
curl 'http://127.0.0.1:8080/api/interpreter' --data-urlencode 'data=[out:json];node(44.97,-93.28,44.985,-93.255)["amenity"="cafe"];out;'
```

`osmpq build extract.osm.pbf root/` does the same without Rust (slower, and
untagged nodes get no metadata). `tools/difftest.py` compares a server
against a real Overpass instance over `tests/corpus`; `tools/remote_profile.py`
counts range requests and bytes per query over HTTP.

## Short version of the design

1. **Base snapshot, two copies.** Convert the planet PBF into Parquet tables
   for nodes, ways and relations in two sort orders: a spatial copy
   (partitioned by adaptive quadtree cell, Hilbert-sorted inside each file, so
   a bounding-box query touches a few files and a few row groups) and an
   id-sorted copy that serves id lookups and replaces the local replication
   store an updater would otherwise need. Plus reverse membership indexes and
   derived areas.
2. **Denormalized geometry.** Ways and relations carry their resolved
   geometry and bounding box so the common case (`out geom`, `area`, `around`)
   never has to join back to nodes over the network. Node references are kept
   too, so `>` / `<` recursion still works exactly like Overpass.
3. **Minutely updates as rolling deltas, from a stateless container.** A
   scheduled container fetches the `.osc`, looks up the current state of
   everything it touches from the id-sorted copy on R2, re-resolves geometry,
   and rewrites three rolling delta files (hour, day, week) that prune like
   the base. A cold reader needs one manifest fetch and then
   reads base cells plus at most three delta files; last version wins.
   Deltas fold into a new base generation weekly.
4. **Overpass QL front end, serverless.** A Worker handles caching and rate
   limits; a container runs the parser, a planner that turns each statement
   into SQL over the lake, and DuckDB. Output is Overpass-shaped JSON/XML so
   overpass turbo, JOSM and existing clients work for the common subset.
5. **History as an Iceberg table.** An append-only history dataset on R2 Data
   Catalog with `valid_from`/`valid_to` per element state (including minor
   versions caused by node moves) backs `date:` / `retro` / `timeline` /
   `diff` / `adiff`, loaded from the full-history planet in a later phase.

Nobody appears to have built exactly this. The closest existing pieces are
ohsome-planet (minutely-updated GeoParquet, no query language), Postpass
(Overpass-like service over PostGIS with SQL instead of Overpass QL), and
QLever's `osm-live-updates` (minutely planet updates into a SPARQL engine).
See the prior-art document for details and links.
