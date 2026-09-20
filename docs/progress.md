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
  planet load. Regional datasets use `osmpq history init` + the updater.
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
- **Rust producer**: compaction rewrites the way-area index in full; the
  chunked sort for a root-level way cell (M1 caveat) is implemented but
  untested at actual planet scale (no real planet-scale run has happened
  yet -- see docs/m1-report.md).
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
