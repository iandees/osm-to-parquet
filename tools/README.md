# tools/difftest.py

A differential test harness that runs the same Overpass-QL query, from a
shared corpus, against a reference Overpass instance and against the local
`osmpq` server, and compares the results. See `docs/m0-contracts.md`
section 9 for the contract this implements.

## Quick start

Once the local server is running (`OSMPQ_ROOT=... uvicorn osmpq.server:app`):

```
python tools/difftest.py \
  --reference https://maps.mail.ru/osm/tools/overpass/api/interpreter \
  --local http://127.0.0.1:8080/api/interpreter \
  --corpus tests/corpus \
  --date 2026-09-19T00:21:52Z
```

(`overpass-api.de` is not reachable from this sandbox; the `maps.mail.ru`
mirror is a full Overpass instance that supports `[date:]` and is used as
the reference. `--date` should match the manifest's `timestamp_osm_base`
so the reference answers as of the extract's timestamp, not "now".)

This prints one row per (query, bbox) pair — `PASS` or `FAIL` with counts
of missing/extra elements, tag mismatches, and both sides' wall-clock
times — and exits non-zero if anything failed. Add `--json report.json` to
also write the full report (including up to 5 example missing/extra ids
and mismatch details per query) as JSON.

## Two-phase runs (reference not always available)

Because the reference is rate-limited and sometimes slow/unavailable, you
can split fetching the reference from comparing against it:

```
# Phase 1: hit the reference once, cache its responses under
# tests/corpus/.cache/<hash>.json. No --local needed.
python tools/difftest.py --reference https://maps.mail.ru/osm/tools/overpass/api/interpreter \
  --corpus tests/corpus --date 2026-09-19T00:21:52Z --reference-only

# Phase 2 (any time later, no network to the reference): hit only --local
# and compare against the cached reference responses.
python tools/difftest.py --local http://127.0.0.1:8080/api/interpreter \
  --corpus tests/corpus --local-only
```

The cache key covers the query file, the bbox name, and the exact
substituted+dated query text, so changing a corpus query or the `--date`
naturally invalidates stale cache entries.

## Flags

| Flag | Meaning |
| --- | --- |
| `--reference URL` | Reference Overpass endpoint. Required unless `--local-only`. |
| `--local URL` | Local server's `/api/interpreter` endpoint. Required unless `--reference-only`. |
| `--corpus DIR` | Directory of `*.overpassql` files (default `tests/corpus`). |
| `--bboxes FILE` | `{name: [south, west, north, east]}` JSON (default `tests/corpus/bboxes.json`). |
| `--date DATE` | Value for `[date:"DATE"]`, inserted on the **reference side only**, so the reference answers as of the extract's timestamp. |
| `--only GLOB` | Only run corpus files matching this glob, e.g. `--only '0[1-7]_*'`. |
| `--bbox-name NAME` | Run every selected query against just this one bbox, instead of the default mix (see below). |
| `--json FILE` | Write the full report as JSON. |
| `--timeout SECONDS` | Per-request HTTP timeout (default 60). |
| `--sleep SECONDS` | Sleep between reference calls, and base backoff unit for 429/504 retries (default 2). |
| `--retries N` | Retries on 429/504/timeout, with exponential backoff (default 5). |
| `--reference-only` | Fetch and cache reference responses; don't call `--local`. |
| `--local-only` | Only call `--local`; compare against previously cached reference responses. |

Without `--bbox-name`, a handful of the most geography-sensitive queries
(the overpass-turbo-wizard-shaped ones for cafe/building/highway/shop/
water/park — see `ALL_BBOX_PATTERNS` in `difftest.py`) run against every
bbox in `bboxes.json`; every other query runs once, against the first bbox
listed in that file.

## What gets compared

For every corpus query, both sides are asked for `[out:json]` (the harness
rewrites `[out:xml]` to `[out:json]` before sending, on both sides, purely
for comparison purposes — but for a corpus entry that declared
`[out:xml]`, the harness *also* fetches the real XML from `--local` and
checks it parses as XML, reported as a separate `<file> [xml-parse-check]`
row).

- If the query is `out count`, the two `count` tag dicts are compared
  directly.
- Otherwise, elements are keyed by `(type, id)`. The harness compares:
  - the *set* of keys (missing = in reference but not local, extra =
    the reverse),
  - for elements present on both sides: `tags` equality, node
    `lat`/`lon` within 1e-7 degrees, way `nodes` list equality, and
    `geometry` point lists (when present on either side) within 1e-6
    degrees.
- An Overpass error response (an HTML parse-error body, or a 200 with a
  `remark`) is treated as a `FAIL` with that message, on whichever side
  produced it.

Rate-limiting: reference calls are made one at a time with `--sleep`
seconds between them, and any 429/504 (or a timeout) is retried with
exponential backoff up to `--retries` times.

## Adding a corpus query

1. Add `tests/corpus/NN_short_description.overpassql`, numbered after the
   existing files, using `{{bbox}}` wherever a bbox filter or `[bbox:...]`
   setting goes. Stick to the tier-1 language subset in
   `docs/m0-contracts.md` section 8 — the local server won't parse
   anything else.
2. If the query is a static id lookup (no bbox needed at all, e.g.
   `node(id:...)`/`way(id:...)`), it's fine to skip `{{bbox}}` entirely;
   just verify the id(s) actually exist at the reference first. Leading
   `//` comments explaining a static id lookup are fine — the harness
   skips them when inserting `[date:...]`.
3. If the query is one you want exercised against every bbox rather than
   just the first, add its filename to `ALL_BBOX_PATTERNS` in
   `difftest.py`.
4. Sanity-check it against the reference alone before relying on it:
   `python tools/difftest.py --reference <ref-url> --corpus tests/corpus --only 'NN_*' --date <date> --reference-only`.
5. Add any new bboxes needed to `tests/corpus/bboxes.json` as
   `"name": [south, west, north, east]`, keeping them small (well under
   0.1 degrees on a side) so the reference answers quickly.

## Unit-testing the comparison logic

`tests/corpus/selftest.py` exercises `compare()` directly against two
hand-written JSON fixtures (`tests/corpus/fixture_ref.json` and
`tests/corpus/fixture_local.json`), without any network access. It's a
plain script, not a pytest module (so it won't be picked up by other
agents' test discovery) — run it directly:

```
python tests/corpus/selftest.py
```
