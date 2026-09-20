#!/usr/bin/env bash
# tools/m4_dataset.sh -- builds the Minnesota M4 history dataset
# (docs/m4-contracts.md section 7).
#
# Stages (run one with `tools/m4_dataset.sh <stage>`, or the whole
# pipeline with `all` -- the default):
#
#   copy      hardlink-copy the M3 base dataset (minnesota-rs3-areas2) to
#             a working root (minnesota-h1)
#   fetch     fetch osm.fr north-america/us-midwest/minute diffs from the
#             base sequence to the source's latest, into osc-midwest/
#             (skips files already present; safe to re-run to top up)
#   history   `osmpq history build minnesota-h1 --pbf data/minnesota.osm.pbf
#             --osc osc-midwest` -- needs W1's history builder
#             (src/osmpq/history/build.py); not runnable until that lands
#   update    catch `minnesota-h1` up with `osmpq update --max-diffs 60`,
#             looped until its manifest's replication_sequence reaches the
#             last fetched sequence, then `osmpq validate`
#   curcheck  the same catch-up loop on a *plain* hardlink copy
#             (minnesota-cur1, no history) to time the M2 updater alone,
#             for comparison in the report
#
# All scratch state lives under $SCRATCH (below); nothing is written
# under the repo. Every stage appends timestamped lines to
# $SCRATCH/m4-dataset-logs/timings.log.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"
# Use this worktree's own src/, not whatever editable install of `osmpq`
# sys.path would otherwise resolve to.
export PYTHONPATH="$HERE/src${PYTHONPATH:+:$PYTHONPATH}"

SCRATCH="/tmp/claude-0/-home-user-osm-to-parquet/d9d8896c-c4a1-5798-bd34-4be59080fbe3/scratchpad"
SRC_DATASET="$SCRATCH/minnesota-rs3-areas2"
H1_ROOT="$SCRATCH/minnesota-h1"
CUR1_ROOT="$SCRATCH/minnesota-cur1"
OSC_DIR="$SCRATCH/osc-midwest"
LOG_DIR="$SCRATCH/m4-dataset-logs"
mkdir -p "$LOG_DIR"
TIMING_LOG="$LOG_DIR/timings.log"

REPL_SOURCE="https://download.openstreetmap.fr/replication/north-america/us-midwest/minute"
FROM_SEQ=7292745
BASE_PBF="data/minnesota.osm.pbf"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$TIMING_LOG"; }

timed() {
    # timed <label> <command...> -- runs the command, logs start/end and
    # elapsed seconds to $TIMING_LOG regardless of success, then re-raises
    # a non-zero exit.
    local label="$1"; shift
    local t0 t1 rc
    t0=$(date +%s)
    log "START $label: $*"
    set +e
    "$@"
    rc=$?
    set -e
    t1=$(date +%s)
    log "END   $label rc=$rc elapsed=$((t1 - t0))s"
    return $rc
}

last_fetched_seq() {
    ls "$OSC_DIR"/*.state.txt 2>/dev/null | sed -E 's#.*/([0-9]+)\.state\.txt$#\1#' | sort -n | tail -1
}

manifest_seq() {
    osmpq manifest "$1" | sed -nE 's/^replication_sequence: ([0-9]+)$/\1/p'
}

stage_copy() {
    if [ -d "$H1_ROOT" ]; then
        log "skip copy: $H1_ROOT already exists"
        return 0
    fi
    timed copy cp -al "$SRC_DATASET" "$H1_ROOT"
}

stage_fetch() {
    mkdir -p "$OSC_DIR"
    timed fetch python3 tools/m4_fetch_diffs.py \
        --source "$REPL_SOURCE" --from-seq "$FROM_SEQ" --dest "$OSC_DIR"
}

stage_history() {
    # docs/m4-contracts.md sections 2 and 5: start the history from the
    # root's current tables (one Parquet file at a time, flat memory) and
    # let the M2 updater append every later version with its minor
    # versions and tombstones. `osmpq history build --pbf ... --osc ...`
    # (section 4) recomputes the same states from the raw object stream
    # and needs more memory than this sandbox has for a 55M-node extract.
    timed history osmpq history init "$H1_ROOT" --threads 4 --memory-limit 6GB --tmpdir "$SCRATCH/m4-init-tmp"
}

# catch_up <root> <label> -- loops `osmpq update <root> --max-diffs 60`
# until its manifest's replication_sequence reaches the last fetched
# sequence in $OSC_DIR, logging each batch and the total.
catch_up() {
    local root="$1" label="$2"
    local target cur t0 t1
    target=$(last_fetched_seq)
    if [ -z "$target" ]; then
        echo "no diffs found in $OSC_DIR; run the fetch stage first" >&2
        return 1
    fi
    cur=$(manifest_seq "$root")
    log "$label: catching up $root from seq $cur to $target (--max-diffs 60 per batch: fewer versions collapse inside a batch)"
    t0=$(date +%s)
    while [ "$cur" -lt "$target" ]; do
        timed "$label(seq=$cur)" osmpq update "$root" --source "$REPL_SOURCE" \
            --max-diffs 60 --tmpdir "$OSC_DIR"
        cur=$(manifest_seq "$root")
    done
    t1=$(date +%s)
    log "$label: DONE seq $(manifest_seq "$root") (target $target) total=$((t1 - t0))s"
}

stage_update() {
    catch_up "$H1_ROOT" "update-h1"
    timed validate osmpq validate "$H1_ROOT"
}

stage_curcheck() {
    if [ ! -d "$CUR1_ROOT" ]; then
        timed copy_cur cp -al "$SRC_DATASET" "$CUR1_ROOT"
    fi
    catch_up "$CUR1_ROOT" "update-cur1"
    timed validate_cur osmpq validate "$CUR1_ROOT"
}

STAGE="${1:-all}"
case "$STAGE" in
    copy) stage_copy ;;
    fetch) stage_fetch ;;
    history) stage_history ;;
    update) stage_update ;;
    curcheck) stage_curcheck ;;
    all)
        stage_copy
        stage_fetch
        stage_history
        stage_update
        ;;
    *)
        echo "unknown stage: $STAGE (want copy|fetch|history|update|curcheck|all)" >&2
        exit 2
        ;;
esac
