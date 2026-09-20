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
