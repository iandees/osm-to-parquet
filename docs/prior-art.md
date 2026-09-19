# Prior art

Surveyed September 2026. The question was: has anyone built an Overpass-QL
query service over object-storage-resident data, and if not, which existing
pieces can we reuse or learn from?

**Conclusion:** no project implements Overpass QL over Parquet/object storage.
Several projects solve one slice of the problem each (converting the planet to
Parquet, keeping Parquet current with minutely diffs, serving an
Overpass-*like* API from a different engine, or applying minutely diffs to a
non-Overpass planet-scale store). The design in `design.md` borrows from each.

## Overpass-like services

| Project | What it is | Relevant to us | Gaps for our goal |
| --- | --- | --- | --- |
| [Overpass API](https://github.com/drolbr/Overpass-API) (drolbr) | The reference implementation. Custom C++ storage engine with quadtile-indexed block files, minutely updates, attic (history) support. | Defines the language and output formats we must be compatible with. Its quadtile spatial index and separate id index are the same two access patterns we need. | Needs the whole DB on fast local disk. A full planet with attic was reported at ~659 GB in late 2025 ([wiki install page](https://wiki.openstreetmap.org/wiki/Overpass_API/Installation)); the wiki recommends SSDs and warns that spinning disks cannot keep up with minutely updates. Every read replica is a full copy. |
| [Postpass](https://github.com/woodpeck/postpass) (Geofabrik) | "Approximately the same things that Overpass API does, just based on a PostGIS database." osm2pgsql flex schema, tags as jsonb, tables `postpass_point/line/polygon`. Public instance at postpass.geofabrik.de; overpass turbo has `{{data:sql,server=...}}` support. [SotM EU 2025 slides](https://www.geofabrik.de/media/2025-11-15-sotm-eu-postpass.pdf). | Validates demand for an alternative backend and shows that a pre-built geometry per element (point/line/polygon) is enough for most real queries. | Query language is SQL, not Overpass QL. Still a big PostGIS box on local disk. No node-ref recursion semantics. |
| [QLever osm-planet](https://qlever.dev/osm-planet) + [osm2rdf](https://github.com/ad-freiburg/osm2rdf) + [osm-live-updates](https://github.com/ad-freiburg/osm-live-updates) (Uni Freiburg) | SPARQL endpoint over the complete planet as RDF (~200 billion triples), with precomputed spatial relations. `osm-live-updates` turns minutely `.osc` files into SPARQL updates; they report generating a minute's update for the whole planet in under 7 seconds. | Proof that minutely planet updates on a non-Overpass engine are practical. Their update tool is a good reference for the "which ways/relations are affected by this node change" problem. | SPARQL, not Overpass QL; local disk; a very different data model. |
| [Overpass Ultra](https://wiki.openstreetmap.org/wiki/Overpass_Ultra) | A MapLibre-based overpass turbo re-imagining that can talk to Overpass, SPARQL (QLever), and other sources. | A likely first client for a new backend. | Front end only. |

## OSM to Parquet converters

| Project | What it is | Relevant to us | Gaps |
| --- | --- | --- | --- |
| [ohsome-planet](https://github.com/GIScience/ohsome-planet) (HeiGIT, Java, GPL-3) | Converts planet and full-history PBF into GeoParquet with resolved geometries, changeset metadata and country codes. The 2025 "Tajogaite" release adds continuous processing of minutely `.osc` files into "contributions" Parquet, using a RocksDB-based replication store holding the latest version of every element so way/relation geometries can be rebuilt ([announcement](https://heigit.org/new-release-of-ohsome-planet-analyse-osm-data-at-the-pace-it-is-produced-with-minutely-updates/)). Output schema has ~29 columns including `osm_type`, `osm_id`, `osm_version`, `valid_from/valid_to`, `tags`, `geometry`, `bbox`, `centroid`, `refs`, `members`, `xzcode`. Writes directly to S3. | The closest existing thing to our ingest/update pipeline. Their design decision (local key-value replication store + append-only contribution files) is the one we propose to copy. Their `valid_from`/`valid_to` model is a ready-made attic representation. | No query language, no service. Output is analysis-oriented (one row per contribution) rather than optimized for point lookups by id or by cell. GPL-3 matters if we want to vendor code. |
| [osm-pds on AWS](https://registry.opendata.aws/osm/) ([docs](https://github.com/awslabs/open-data-docs/tree/main/docs/osm-pds)) | OSMF/Pacific Atlas-maintained buckets: raw planet/history PBF and replication files (`osm-planet-eu-central-1`, `osm-planet-us-west-2`), plus cloud-native ORC tables (`osm-pds`) with `planet`, `planet-history`, `changesets` tables for Athena. | Public, credential-free source for the planet PBF and the minutely diffs, with S3 in-region reads. The ORC schema (id, type, tags map, lat/lon, nds, members, changeset, timestamp, uid, user, version, visible) is a sensible baseline. | Weekly snapshots only for the columnar tables. No geometry, no spatial partitioning. |
| [Open Planet Data](https://openplanetdata.com/) ([GitHub](https://github.com/openplanetdata)) | Daily planet snapshots in PBF, GeoDesk GOL and GeoParquet (~150 GB), hosted on Cloudflare R2, no API keys, MIT pipelines. | Existence proof that planet-scale Parquet on R2 is affordable and that daily rebuilds are feasible. Could be an alternative base-snapshot source. | Daily, not minutely. Not a query service. |
| [QuackOSM](https://github.com/kraina-ai/quackosm) (Python, DuckDB) | PBF to GeoParquet using DuckDB's spatial extension, with tag and geometry filters. | Good reference for doing geometry assembly *inside* DuckDB. | Extract-oriented; not designed for planet updates. |
| [duckdb-osmium](https://github.com/jake-low/duckdb-osmium) / [osmium community extension](https://duckdb.org/community_extensions/extensions/osmium) | DuckDB extension reading OSM PBF/XML via libosmium, builds geometries, exposes version/timestamp metadata (0.5+). | Lets the base build be a DuckDB `COPY (SELECT ... FROM osmium_read(...)) TO 'parquet'` pipeline instead of custom code, at least for prototyping. | Streaming reader; planet-scale geometry assembly still needs care. |
| [osm-parquetizer](https://wiki.openstreetmap.org/wiki/Osm-parquetizer) (Java) | Raw PBF to three Parquet files (nodes/ways/relations), Spark/Hadoop oriented. | Historical baseline; the "raw" schema approach. | Unmaintained, no geometry, no updates. |
| [openstreetmap_h3](https://github.com/igor-suhorukov/openstreetmap_h3) | Planet loader that partitions by H3 cells into PostGIS or Arrow/Parquet. | Example of H3-cell partitioning of the planet. | Bulk load only. |
| [layercake](https://github.com/osmus/layercake) (OSM US) | Opinionated thematic layers (OGC geometries) from OSM. | Shows what a "derived layers" product looks like; not our goal. | Different data model. |
| [GeoDesk / GOL 2.0](https://www.geodesk.com/2025/09/30/geodesk-2-0) | Single-file compact planet database (~100 GB) with the GOQL query language, C++20 rewrite, incremental updates planned. | Very compact planet encoding; GOQL is an interesting terse alternative to Overpass QL. | Local file, not object storage. |
| [OSMExpress](https://github.com/protomaps/OSMExpress) (protomaps) | LMDB + Cap'n Proto planet store with S2 spatial index, reverse membership indexes, and in-place minutely updates that do not block readers. ~1,500 LOC. | A candidate for the updater's local replication store: it already maintains node→way, node→relation, way→relation indexes and applies `.osc` files. | Not maintained very actively; C++; local file. |

## Storage and engine building blocks

| Piece | Why it matters |
| --- | --- |
| [DuckDB 1.5 built-in GEOMETRY](https://duckdb.org/docs/current/sql/data_types/geometry) (March 2026) | Geometry columns now carry per-row-group bounding-box statistics used by the optimizer to skip row groups, and uniform geometry columns are "shredded" into primitive columns for ~3x better compression. DuckDB 1.4 added writing native Parquet GEOMETRY. This removes most of the need for hand-made `bbox` struct columns, though flat `xmin/ymin/xmax/ymax` columns remain the most portable way to get pruning in other readers ([GeoParquet discussion](https://github.com/opengeospatial/geoparquet/discussions/192), [duckdb-spatial #723](https://github.com/duckdb/duckdb-spatial/discussions/723)). |
| DuckDB httpfs + [cache_httpfs](https://duckdb.org/community_extensions/extensions/cache_httpfs) | Range-request reads of Parquet from S3/R2 with metadata caching; `cache_httpfs` adds an on-disk/in-memory block cache with LRU eviction, reported at ~11x speedup on repeated remote reads. Cloudflare R2 works through the S3 API ([DuckDB R2 guide](https://duckdb.org/docs/stable/guides/network_cloud_storage/cloudflare_r2_import)). |
| Parquet bloom filters in DuckDB (1.2+) | DuckDB writes and reads Parquet bloom filters for equality predicates, which is what makes "promoted tag columns" (see design) prunable by value without a full scan. |
| [DuckLake](https://ducklake.select/) (v1.0, April 2026) | Lakehouse table format: Parquet data on blob storage, all metadata in a SQL database (DuckDB, SQLite, Postgres). Cheap snapshots (rows, not files), partial-file references, `ducklake_table_changes` for incremental reads, time travel. A strong candidate for managing our delta/compaction lifecycle and for attic queries. |
| [Cloudflare R2 Data Catalog](https://developers.cloudflare.com/r2-data-catalog/) + [R2 SQL](https://blog.cloudflare.com/r2-sql-deep-dive/) | Managed Iceberg REST catalog inside an R2 bucket. DuckDB 1.4+ can attach, read and write it ([DuckDB example](https://developers.cloudflare.com/r2-data-catalog/config-examples/duckdb/)), with the caveat that DuckDB cannot `DELETE` on partitioned Iceberg tables. [Automatic compaction](https://developers.cloudflare.com/r2-data-catalog/table-maintenance/) (64-512 MB target files) and snapshot expiration are managed. R2 SQL is a serverless engine at $2.50/TB scanned with joins/CTEs as of 2026. Chosen for the history dataset; the hot query path uses a hand-rolled manifest instead because Iceberg metadata needs several round trips before pruning. |
| [Cloudflare Containers](https://developers.cloudflare.com/containers/) | Serverless containers fronted by Workers, managed through Durable Objects, sleep after idle, billed per 10 ms active (memory $0.0000025/GiB-s, vCPU $0.00002/vCPU-s, disk $0.00000007/GB-s, with an allowance in the $5/month Workers Paid plan). Instance types up to `standard-4` (4 vCPU, 12 GiB, 20 GB ephemeral disk); account limits in the thousands of instances ([limits](https://developers.cloudflare.com/containers/platform-details/limits/), [Feb 2026 changelog](https://developers.cloudflare.com/changelog/post/2026-02-25-higher-container-resource-limits)). Still labelled beta. This is the intended home for the DuckDB query engine, next to R2. |
| DuckDB on AWS Lambda | Well-trodden fallback: DuckDB fits in an ~18 MB layer, cold starts of roughly 1-2 s, up to 10 GB memory ([example](https://dev.to/aws-builders/serverless-analytics-on-nas-data-for-000001query-duckdb-lambda-x-fsx-for-ontap-2o5o), [quack-reduce](https://github.com/BauplanLabs/quack-reduce)). Cross-cloud reads from R2 cost latency, not egress. |
| [pyosmium replication tools](https://docs.osmcode.org/pyosmium/latest/user_manual/10-Replication-Tools/) / osmium | Battle-tested minutely diff download and sequencing. |

## Overpass QL parsers

Searches for a reusable Overpass QL parser (Rust, Python, tree-sitter, Lark)
turned up only tooling *around* the language:
[overpass-syntax-checker](https://github.com/topics/overpass-ql) (Python
validator), [Overpass-Forge](https://github.com/topics/overpass-ql) (Python
query builder), [overpassify](https://pypi.org/project/overpassify/) (Python to
Overpass QL transpiler), [OverpassNL](https://github.com/raphael-sch/OverpassNL)
(NL to Overpass QL), and a [community syntax reference](https://osm-queries.ldodds.com/syntax-reference.html).
The reference grammar is Overpass's own hand-written C++ parser plus the
[wiki language reference](https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL).
We should expect to write the parser ourselves and treat the wiki and the
reference implementation's behavior on a test corpus as the spec.

## Sources

- https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL
- https://wiki.openstreetmap.org/wiki/Overpass_API/Installation
- https://wiki.openstreetmap.org/wiki/Overpass_API/Areas and https://github.com/drolbr/Overpass-API/blob/master/src/rules/areas.osm3s
- https://wiki.openstreetmap.org/wiki/Planet.osm (planet PBF 88.0 GB on 2026-09-01) and https://wiki.openstreetmap.org/wiki/Planet.osm/full (history PBF 150.1 GB, 2026-08-01)
- https://github.com/woodpeck/postpass and https://postpass.geofabrik.de/
- https://community.openstreetmap.org/t/osm-data-in-geoparquet-format/141690
- https://github.com/GIScience/ohsome-planet and https://heigit.org/new-release-of-ohsome-planet-analyse-osm-data-at-the-pace-it-is-produced-with-minutely-updates/
- https://github.com/ad-freiburg/osm-live-updates and https://github.com/ad-freiburg/osm2rdf
- https://registry.opendata.aws/osm/ and https://github.com/awslabs/open-data-docs/tree/main/docs/osm-pds
- https://openplanetdata.com/ and https://github.com/openplanetdata
- https://github.com/kraina-ai/quackosm, https://github.com/jake-low/duckdb-osmium
- https://github.com/protomaps/OSMExpress, https://www.geodesk.com/2025/09/30/geodesk-2-0
- https://spatialists.ch/posts/2026/03/22-duckdb-15-with-spatial-updates/ and https://duckdb.org/docs/current/sql/data_types/geometry
- https://github.com/duckdb/duckdb-spatial/discussions/723, https://github.com/opengeospatial/geoparquet/discussions/192
- https://ducklake.select/2026/04/13/ducklake-10/, https://motherduck.com/blog/ducklake-architecture-deep-dive/
- https://developers.cloudflare.com/r2-data-catalog/, https://blog.cloudflare.com/r2-sql-deep-dive/
- https://duckdb.org/community_extensions/extensions/cache_httpfs
- https://developers.cloudflare.com/containers/platform-details/limits/, https://developers.cloudflare.com/containers/pricing/
- https://developers.cloudflare.com/r2-data-catalog/table-maintenance/, https://developers.cloudflare.com/r2-data-catalog/config-examples/duckdb/
