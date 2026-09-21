# Progress and status

The project is developed in milestones. Each one starts with a contract
(`docs/mN-contracts.md`: the exact layout, semantics and division of work)
and ends with a report (`docs/mN-report.md`: what was built, measured
numbers, differences from the reference, open issues). Everything so far
was built and graded on a Minnesota extract; the planet build has not
been run.

| milestone | state | delivered |
| --- | --- | --- |
| M0 extract-scale prototype | done | Layout v1 on Parquet, Overpass QL parser and planner for the tier-1 language, engine over local disk/HTTP/R2, the differential harness against a public Overpass instance ([contract](m0-contracts.md), [report](m0-report.md)) |
| M1 planet-capable builder | done on Minnesota | Rust producer (`rust/osmpq-raw`) with a disk-backed node store, layout v2 (restricted ancestor depths, row-group index, tuned encodings), engine caching ([contract](m1-contracts.md), [report](m1-report.md), [planet runbook](m1-runbook.md)) |
| M2 minutely updates | done | Stateless updater, rolling hour/day/week delta tiers, compaction, gc, diffcheck against the reference ([contract](m2-contracts.md), [report](m2-report.md)) |
| M3 public beta | done | Tier-2 language (areas, `around`, `poly`, `is_in`, meta filters, evaluators, `foreach`/`if`, csv), rate limits and status endpoints, container image, updater on `s3://` roots, Cloudflare Worker + Containers + Durable Object scheduler ([contract](m3-contracts.md), [report](m3-report.md), [deploy runbook](m3-runbook.md)) |
| M4 history | done | History dataset kept current by the updater; `[date:]`, `retro`, `timeline`, `[diff:]`/`[adiff:]`, exact `(changed:)` ([contract](m4-contracts.md), [report](m4-report.md)) |
| M5 extras | not started | `compare`, `make`/`convert`, `popup`/`custom` outputs, DuckDB-Wasm browser mode ([roadmap](design.md#10-roadmap)) |

## Where things stand (September 2026)

- **Tests**: 683 passing (`uv run pytest tests -q`), lint clean, the Worker's
  30 tests passing. `.github/workflows/ci.yml` runs all of it on pull
  requests.
- **Compatibility on Minnesota** against the public reference
  (`tools/difftest.py`): 77 of 89 graded rows pass; the 12 that do not are
  documented in [the M4 report](m4-report.md#5-validation-targets) (queries
  the reference cannot answer, the Lake Superior extent, relation areas
  leaving the extract, history that starts at the base timestamp).
- **A live dataset on R2**: `s3://osm-parquet/minnesota` (5.3 GB with
  history), queried directly by the engine and updated once from the
  sandbox; see the M4 report for timings.
- **Not yet exercised**: a real Cloudflare deploy (no account in the
  development sandbox; `docs/m3-runbook.md` is written but untested), the
  Docker image build (no daemon in the sandbox), and the planet build.

## What is left

Code:

- **Planet-scale history builder.** `osmpq history build` (raw object
  stream → states with minor versions) materializes every node version in
  memory; it needs to work in bounded id ranges before a full-history
  planet load (`docs/m4-report.md` section 6). Until then, a planet build
  gets attic data the same way regional datasets do: `history init` right
  after the build, then the updater, accumulating real history forward
  from the build's own timestamp (`docs/m1-runbook.md` section 9 has the
  exact procedure) — full 2004-present backfill has to wait for the
  rewrite above. When it exists, backfilling a root that already has
  forward history running needs no splicing: `history build --osh`
  recomputes the whole base history in one pass from a dump that already
  contains everything the forward path captured meanwhile, so it is a
  wholesale replace-and-resume, not a merge.
- **Updater over object storage.** 224 s for a 30-diff batch against R2
  versus 6-12 s locally: profile the by-id reads of touched parents
  (DuckDB reads whole row groups over HTTP); cache byid parts in the
  container or add an id-to-row-group index.
- **`adiff` over very large result sets** renders both passes before
  diffing (58 s for 2.1M nodes per pass); diff the passes' set rows in SQL
  and render only the changed ids.
- **Row-group index for history files** so wide `[date:]` scans prune like
  current-table scans (4x cost today).
- **Way-area index by name** for planet-scale `area[name=...]` lookups on
  closed ways (M3 follow-up); relation areas at an extract's edge.
- ~~`osmpq areas` single-threaded, 99.7% of build wall time at 10x
  Minnesota scale~~ found and fixed benchmarking `us-midwest`: parallelism
  plus an O(hole-count) algorithmic fix took it from 6,518 s to 86.5 s, a
  75x speedup (`docs/m3-report.md` section 5).
- **Rust producer**: compaction rewrites the way-area index in full; the
  chunked sort for a root-level way cell (M1 caveat) is implemented but
  untested at actual planet scale (no real planet-scale run has happened
  yet -- see docs/m1-report.md).
- ~~`--node-store auto` picked `dense-file` for a country-scale extract and
  tried to allocate ~113.6 GB~~ found and fixed attempting a real whole-US
  build (Geofabrik's combined US extract, 1.596B nodes, max id 14.2B, so
  only ~11.2% id density): `dense-file` sizes its file by `max_id`, not
  actual node count, and -- a separate finding -- isn't meaningfully sparse
  on disk at any density measured here (planet's ~70% density still leaves
  every 4 KiB block touched); it filled a 107 GB-free disk in ~14 minutes.
  Added a third node-store mode, `sorted-file` (an mmap-backed sorted
  array sized by actual node count, ~25.6 GB for this input, vs.
  `sorted-mem`'s same-sized hard RAM commitment or `dense-file`'s
  oversized disk file); `--node-store auto` now picks between the three
  by comparing real disk cost (`docs/m1-contracts.md` section 3.1).
  Validated against the real whole-US PBF: the node store itself
  populated correctly and reached its exact target size across several
  attempts, once the machine had enough free RAM (the same Mac had ~30 GB
  already committed to other running applications at one point, which is
  a real-world constraint, not a flaw in the fix). Locally, per-cell node
  spill (same spill-then-sort mechanism as the way-cell fix above) reached
  78+ GB and was still growing at 84% of the node pass, and didn't fit this
  laptop's available disk. That was resolved by completing the build on a
  rented instance instead (`i4i.2xlarge`, 8 vCPU / 61 GiB / 1.7 TB NVMe,
  `us-west-2`, $0.686/hr): the real observed peak during the node pass was
  ~92 GB, below the ~130-150 GB extrapolated locally, and per-cell spill
  cleanup worked as designed once given room -- the laptop run's spill
  growth wasn't a leak, just a large transient that hadn't finished
  clearing when the disk ran out. **The whole-US raw + build pass now
  completes and validates cleanly end to end**: `osmpq-raw build` in
  3580 s (1.596B nodes, 161.7M ways, 1.607M relations, 4176 leaves), then
  `osmpq build --raw` in 942.6 s (relations + 186,440 relation areas +
  2.77M way areas indexed), `osmpq validate` passing every check, 59 GB
  root output. Two more real bugs turned up only at this scale and are
  now fixed: Ubuntu's default 1024 open-file soft limit crashes
  `osmpq-raw`'s spill mechanism (4176 leaf cells each open a spill file)
  with `TooManyOpenFiles` -- raise it (`ulimit -n 1048576`, comfortably
  under Ubuntu's 1M hard limit) before running on Linux; and relation/way
  bbox-centroid SQL in the Python build path
  (`src/osmpq/build/builder.py`, `src/osmpq/build/raw.py`) summed two
  `INT32` `*_e7` columns before dividing, which overflows for any bbox
  spanning far enough into negative (western) longitudes -- true for much
  of the western US and Alaska, never triggered by Minnesota or
  us-midwest's narrower extents. Fixed by widening to `BIGINT` before the
  add, matching how the Rust producer already did this for ways
  (`rust/osmpq-raw/src/ways.rs`).
- **Language**: `compare`, `for`, `complete`, `make`/`convert`, `local`,
  `popup`/`custom` outputs, `way_cnt`/`way_link`; `retro` with evaluator
  arguments beyond the M3 subset.
- A license file (the design allows Apache-2/MIT; nothing is copied from
  Overpass or ohsome-planet).

Deployment (`docs/m3-runbook.md`):

1. `wrangler deploy` on a real account: image build and push, the two
   container apps, the Durable Object migration, the rate-limit namespace
   id (a placeholder is committed).
2. Secrets (`OSMPQ_S3_*`, `ADMIN_TOKEN`), then start the scheduler and
   watch a day of minutely runs: per-run wall clock, lag, R2 operation
   counts, the container's memory at the updater's peak.
3. Compaction schedule and where it runs (a bigger scheduled container or
   the load machine), once the weekly touched-cell ratio is known.
4. The planet: build on a rented node (`docs/m1-runbook.md`), sync to R2,
   `history init`, switch the updater to the planet's minutely diffs, then
   the numbers that replace the extrapolations (cold/warm latency, cost per
   query, storage).
5. Decide whether the bucket is public so others can run the engine (or the
   future Wasm mode) against it.
