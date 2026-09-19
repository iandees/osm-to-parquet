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

Status: **design phase**. Nothing runs yet. The documents below are the
proposal; the repository name is historical and Parquet is a means, not the
goal.

## Documents

| Document | What it covers |
| --- | --- |
| [docs/prior-art.md](docs/prior-art.md) | Survey of existing projects (Overpass, Postpass, ohsome-planet, QLever, OSMExpress, GeoDesk, QuackOSM, osm-pds, DuckLake, R2 Data Catalog, ...) and what each one contributes or lacks |
| [docs/design.md](docs/design.md) | Proposed architecture: storage layout, update pipeline, query translation, serving tier, cost model, roadmap, open questions |
| [docs/overpass-ql-support.md](docs/overpass-ql-support.md) | Overpass QL feature matrix and the order we intend to implement it in |

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
