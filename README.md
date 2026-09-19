# osm-to-parquet

**Goal:** an Overpass-API-compatible query service for the full OpenStreetMap
planet whose data lives on cheap object storage (Cloudflare R2, S3) instead of
a large local NVMe database, kept current with the minutely replication diffs.

The public Overpass API is a great tool, but running your own instance means
provisioning roughly 600+ GB of fast local SSD per server plus a single-writer
update process, and every read replica repeats that cost. This project explores
the opposite trade: put the planet in cloud-optimized columnar files on a blob
store, use an embedded analytical engine (DuckDB) with HTTP range reads as the
execution layer, and translate Overpass QL into queries against that layout.
Query workers become stateless and disposable; the only stateful machine is
the small updater.

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

1. **Base snapshot.** Convert the weekly planet PBF into three families of
   Parquet tables (nodes, ways, relations) plus derived tables (areas, reverse
   membership index, id-to-cell index). Files are partitioned by a spatial cell
   key and sorted by a space-filling curve inside each file, so a bounding-box
   query touches a handful of files and a few row groups.
2. **Denormalized geometry.** Ways and relations carry their resolved
   geometry and bounding box so the common case (`out geom`, `area`, `around`)
   never has to join back to nodes over the network. Node references are kept
   too, so `>` / `<` recursion still works exactly like Overpass.
3. **Minutely updates as deltas.** A small updater applies `.osc` diffs to a
   local replication store, re-resolves geometry for touched ways and
   relations, and publishes small delta Parquet files. Readers see
   `base ⊕ deltas`, last version wins. Deltas are compacted hourly and daily,
   and folded into a new base periodically.
4. **Overpass QL front end.** A parser produces an AST; a planner turns each
   statement into SQL over the lake, with Overpass sets materialized as
   temporary tables inside a per-query DuckDB instance. Output is byte-for-byte
   compatible Overpass JSON/XML so overpass turbo, JOSM and existing clients
   keep working.
5. **Attic (history) later.** Keep an append-only history dataset alongside the
   current one; `date:` / `retro` / `timeline` are a later phase.

Nobody appears to have built exactly this. The closest existing pieces are
ohsome-planet (minutely-updated GeoParquet, no query language), Postpass
(Overpass-like service over PostGIS with SQL instead of Overpass QL), and
QLever's `osm-live-updates` (minutely planet updates into a SPARQL engine).
See the prior-art document for details and links.
