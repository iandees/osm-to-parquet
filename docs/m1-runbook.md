# M1 runbook: planet build and R2 sync

This is the procedure for building the full-planet dataset on your own
machine and publishing it to R2. The sandbox that developed the code cannot
run it (4 cores, 25 GB disk), so treat the numbers as estimates from the
Minnesota extrapolation in `docs/m1-report.md` and correct them after the
first run.

## 1. Machine

| Resource | Minimum | Comfortable |
| --- | --- | --- |
| RAM | 32 GB (dense-file node store lives on disk; page cache does the rest) | 128 GB or more (most of the 110 GB flat-node file stays cached, ways pass is then CPU-bound) |
| Disk | 1.5 TB NVMe | 2 TB NVMe |
| Cores | 8 | 16-32 (PBF decoding and per-cell sorting parallelize; DuckDB stages too) |

### Suggested cloud instance

Not yet run at planet scale; this is an estimate, not a measurement. Primary
pick: `i4i.8xlarge` in `us-west-2` (32 vCPU, 256 GiB RAM, 3.75 TB local NVMe,
one volume). 256 GiB clears the "comfortable" RAM line with margin, so the
~110 GB flat-nodes file plus OS page cache should mostly keep the way pass
CPU-bound rather than disk-latency-bound; 32 cores sits at the top of the
comfortable range for the parallelized PBF-decode/per-cell-sort and DuckDB
stages; the single 3.75 TB NVMe volume covers the full transient disk budget
(up to ~1.5 TB without the optional node_way index) on one filesystem, which
`--link` requires for `/fast/raw` and `/fast/root`. On-demand price in
`us-west-2` is $2.746/hr (AWS EC2 pricing via cloudprice.net, checked
2026-09-19). For an assumed 12-24h job (download + raw pass + build pass +
validate + R2 sync): ~$33-66 in instance-hours, plus ~$40-54 for the ~450-600
GB upload to R2 over the internet at AWS's $0.09/GB data-transfer-out rate
(same egress cost regardless of instance choice; pulling the planet PBF from
`s3://osm-planet-us-west-2` costs nothing extra since it's a same-region
S3-to-EC2 transfer). Instance-store data is ephemeral: a stop or reboot
loses everything under `/fast` mid-build. Given the job is a few hours and
cheaply restartable from the source PBF, that risk is accepted rather than
engineered around — just don't stop/hibernate the instance while it's
running. The NVMe volume comes pre-initialized (no TRIM step) but still
needs `mkfs`+mount at first boot.

Fallbacks: `i4i.4xlarge` (16 vCPU, 128 GiB RAM, 3.75 TB NVMe, $1.373/hr
on-demand) meets "comfortable" RAM/disk exactly but sits at the bottom of
the comfortable core range, and has less page-cache headroom if the OS and
DuckDB working set eat into the 128 GB budget — roughly half the compute
cost of the 8xlarge. `r7i.4xlarge` + a 2 TB `gp3` EBS volume (16 vCPU, 128
GiB RAM, $1.0584/hr on-demand, gp3 at ~$0.08/GB-month) survives a
stop/reboot, removing the ephemeral-storage risk, but gp3's baseline
throughput (125 MB/s/volume unless extra IOPS/throughput is provisioned) is
well below local NVMe and risks making the mmap-heavy way pass more
disk-bound than the RAM-driven page-cache story above assumes; pick this
only if restart-safety matters more than raw speed, and provision extra gp3
throughput/IOPS (or use `io2`) if so. Spot pricing for `i4i.8xlarge` runs
roughly half of on-demand but isn't worth the interruption risk for a
one-time, non-checkpointed job.

Disk budget for a planet run (rough): planet PBF 90 GB, flat-node file up to
110 GB (sparse; allocated pages depend on id density), node spill ~250 GB,
way spill ~250 GB (deleted after their pass), raw output ~450 GB, plus the
node_way index pass (~200 GB temp + ~150 GB output) if you build it. Run
`df -h` before each stage.

## 2. Software

```
# Rust toolchain (rustup), then:
cd rust/osmpq-raw && cargo build --release && cp target/release/osmpq-raw ~/bin/
# Python side
python3 -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'
# object storage tooling
rclone version   # https://rclone.org/install/
```

## 3. Get the planet

The OSMF buckets are public and fast:

```
aws s3 cp --no-sign-request s3://osm-planet-us-west-2/planet/pbf/2026/planet-260914.osm.pbf . 
# or: curl -O https://osm-planet-us-west-2.s3.amazonaws.com/planet/pbf/2026/planet-260914.osm.pbf
curl -O https://osm-planet-us-west-2.s3.amazonaws.com/planet/pbf/2026/planet-260914.osm.pbf.md5 && md5sum -c planet-260914.osm.pbf.md5
```

Record the replication state that matches the file so minutely updates
(M2) can start from it: `osmium fileinfo -e planet-260914.osm.pbf` prints
`osmosis_replication_sequence_number` and `_timestamp` from the PBF header.

## 4. Raw pass (Rust)

```
export TMP=/fast/tmp && mkdir -p $TMP
osmpq-raw build planet-260914.osm.pbf /fast/raw \
    --threads $(nproc) --max-nodes-per-cell 1000000 --max-depth 13 \
    --node-store dense-file --flat-nodes $TMP/nodes.flat --tmpdir $TMP \
    2>&1 | tee raw.log
```

Expected order of magnitude (to be corrected from the Minnesota numbers in
the report): histogram pass and node pass each bounded by PBF decode speed,
tens of minutes to an hour on 16+ cores; the way pass is bounded by node
lookups against the flat file, one to a few hours depending on how much of
it the page cache holds; per-cell sorts run in parallel afterwards.

Optional, only needed by the updater (M2):

```
osmpq-raw node-way-index /fast/raw 2>&1 | tee node-way.log     # ~13B rows, ~200 GB temp
```

Check `/fast/raw/summary.json` and `leaves.json` (expect a few thousand leaves).

## 5. Build pass (Python + DuckDB)

```
osmpq build --raw /fast/raw /fast/root --generation g0001 \
    --timestamp <osmosis_replication_timestamp> --replication-sequence <sequence> \
    --threads $(nproc) --memory-limit 48GB --tmpdir $TMP --link 2>&1 | tee build.log
osmpq validate /fast/root
osmpq manifest /fast/root
```

`--link` hardlinks the raw node and way files into the root instead of
copying them, so keep `/fast/raw` and `/fast/root` on the same filesystem.

## 6. Sync to R2

Create a bucket (say `osm-planet`) and an API token with object read/write.

```
rclone config create r2 s3 provider=Cloudflare access_key_id=$R2_KEY secret_access_key=$R2_SECRET \
    endpoint=https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com acl=private
rclone sync /fast/root r2:osm-planet/current --transfers 32 --checkers 64 --s3-chunk-size 64M --fast-list -P
rclone check /fast/root r2:osm-planet/current --one-way --size-only
```

`manifest/LATEST` must be uploaded last if you sync incrementally into a
bucket that is already being read; `rclone sync` orders by path, so upload
`manifest/` in a second pass or exclude it from the first
(`--exclude 'manifest/**'`, then `rclone copy /fast/root/manifest r2:osm-planet/current/manifest`).

Storage cost at R2 list price: about $0.015 per GB-month, so ~450 GB is
~$7/month before the node_way index; Class A operations for the upload are
a few tens of thousands (cents).

## 7. Serve from R2

DuckDB reads R2 through the S3 API. The engine takes the root as an `s3://`
URL and the credentials from a DuckDB secret created from the environment:

```
export OSMPQ_ROOT=s3://osm-planet/current
export OSMPQ_S3_ENDPOINT=$R2_ACCOUNT_ID.r2.cloudflarestorage.com
export OSMPQ_S3_KEY_ID=$R2_KEY OSMPQ_S3_SECRET=$R2_SECRET OSMPQ_S3_REGION=auto
uvicorn osmpq.server:app --port 8080
curl 'http://127.0.0.1:8080/api/interpreter' --data-urlencode 'data=[out:json];node(44.970,-93.280,44.985,-93.255)["amenity"="cafe"];out;'
```

(If the engine does not yet read those variables, run the equivalent
`CREATE SECRET (TYPE S3, KEY_ID ..., SECRET ..., ENDPOINT ..., REGION 'auto', URL_STYLE 'path')`
through `duckdb_config`; see `src/osmpq/engine/executor.py`.)

Then run the harness and the profiler against it to get the first real
cold-latency numbers per request over R2:

```
python tools/difftest.py --reference https://overpass-api.de/api/interpreter --local http://127.0.0.1:8080/api/interpreter \
    --corpus tests/corpus --date <osmosis_replication_timestamp>
```

## 8. What to record for the report

Per pass wall time and peak RSS, leaf count and depth distribution, file
count and bytes per table, `validate` output, R2 upload time, and the
harness table over R2 with `local_ms`. Those numbers replace the
extrapolations in `docs/m1-report.md`.

## 9. Attic (history) data after the build

Sections 1-8 build the *current*-state dataset only. This project's history
subsystem (`docs/m4-contracts.md`) can add attic data on top of it, but the
two ways to do that are at very different levels of readiness.

### Forward path — implemented, do this now

This is exactly the machinery M4 validated on Minnesota
(`docs/m4-report.md`), unchanged for planet scale:

1. Right after `osmpq build --raw` (section 5) — before any `osmpq update`
   run, while the root is still one generation with no delta tiers —
   turn the current tables into history:
   ```
   osmpq history init /fast/root --threads $(nproc) --memory-limit 48GB --tmpdir $TMP
   ```
   Every current row becomes its own state (`minor=0`, `valid_from` = the
   row's own `timestamp`, `visible=true`) — exactly the dataset's history
   at the instant the planet extract was taken. It streams one Parquet
   file at a time and never materializes a full table, so cost is flat
   regardless of extract size: 70s on Minnesota's ~55M nodes
   (`docs/m4-report.md`). Correct that estimate once you've run it at
   planet scale.
2. Point `osmpq update` at the planet's own minutely diffs, starting from
   the sequence recorded in section 3 (already carried in the manifest via
   section 5's `--replication-sequence`/`--timestamp`):
   ```
   osmpq update /fast/root --source https://planet.openstreetmap.org/replication/minute/ \
       --max-diffs 60 --threads $(nproc) --memory-limit 48GB --tmpdir $TMP --once
   ```
   (`--source` is saved into the manifest on first use, so later runs and
   the scheduled container can omit it.) From here every `osmpq update` run
   appends real, complete history to the rolling hour/day/week tiers
   (`docs/m4-contracts.md` section 5.1); `osmpq compact` folds them into
   the base history on whatever schedule you use for the current-state
   deltas (section 5.2).
3. `[date:]`, `retro`, `timeline`, `[diff:]`/`[adiff:]` and exact
   `(changed:)` now work for any date from the build's own timestamp
   onward. Dates before it are unavailable — this is the one gap the
   forward path can't close (see below).

### Backfill path (OSM's full 2004-present history) — not planet-ready

`osmpq history build --osh <full-history.osh.pbf>` recomputes every state
from a raw object-version stream instead of starting from "now", so it is
the only path that can produce true retroactive history back through OSM's
whole edit history rather than just from the build's own timestamp
forward. It does not fit even Minnesota's ~55M nodes today: the
node-version cell-assignment pass materializes every node version in
memory and hits a 14 GB cgroup ceiling regardless of the memory fixes
tried so far (`docs/m4-report.md`, "History build: what worked and what
did not"). A full planet history is dramatically larger than Minnesota's:
OSM's full-history planet file is about 150 GB compressed / on the order
of 3.7 TB uncompressed (vs. a ~90 GB current-state planet PBF), covering
a planet with on the order of 10 billion nodes, 1.1 billion ways and 13
million relations (`docs/design.md` section 1) — and, because it carries
every version of every element back to 2004, a multiple of that object
count in object-versions (an OSHDB paper measured about 8.4 billion
versions for 6.1 billion entities on an earlier, smaller planet; today's
planet is roughly twice that entity count, so tens of billions of object
versions is the right order of magnitude — "a few times the current
planet's row count", per `docs/design.md` section 4.5).

Making the builder fit is the same *class* of fix already used for the
Rust way-cell sort (`rust/osmpq-raw/src/ways.rs`, `docs/m1-contracts.md`
section 3.2): chunk oversized work into bounded ranges and spill/merge
instead of holding it all in memory. Applied here that means
`history/build.py`'s node-state computation (`_compute_node_states`) and
the way/relation minor-version joins (`_compute_way_states`,
`_compute_relation_states`) processing node ids in bounded ranges instead
of materializing `node_states`/`multi_version_node_ids` whole — the
`common.range_bounds`/`range_cond` id-chunking idiom this codebase already
uses elsewhere (e.g. `history/writer.py`'s `write_byid`) is the natural
fit, though the way/relation minor-version join would need reworking to
run per range rather than against one in-memory node table. This is
unscoped, unscheduled future work (`docs/progress.md`, `docs/m4-report.md`
section 6), not something to attempt as part of a planet deployment today.

**Reconciling a later backfill with an already-running forward history:**
no splicing is needed. `history build` recomputes the *entire* base
history from its input in one pass, and a full-history dump taken later
necessarily already contains every version the forward path captured in
the meantime — they're the same real OSM edit history, just read from a
file that goes back further. So once the bounded-range rewrite exists,
backfilling is: fetch a full-history dump covering the extent through
"now", run `osmpq history build --osh` against it once (this wholesale
replaces whatever `history init` + the updater had built, not merges with
it), then resume `osmpq update` from that dump's own replication sequence
exactly as in step 2 above. One sharp edge in the code as it stands today:
`history_build` writes into `history/<gen>/...` for whatever generation
the root is currently on and does not refuse to run when `man.history` is
already set (unlike `history_init`, which does refuse) — so do this into a
new generation/copy and swap the manifest, not in place, until that gets a
guard.

### Recommendation

For a planet build today: run `history init` and start `osmpq update`
immediately (steps 1-2 above), and accept that dates before the build's
own timestamp are unavailable. Defer full 2004-present backfill until the
bounded-id-range rewrite of `osmpq history build` exists — there is
currently no tested, working way to backfill true historical attic data
at planet scale.
