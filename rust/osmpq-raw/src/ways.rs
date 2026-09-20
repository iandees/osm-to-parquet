//! Pass 3 (docs/m1-contracts.md section 3): way byid + spatial (loose cell
//! placement, geometry assembly).

use crate::cells::{self, LeafIndex};
use crate::pbfutil::{self, BBox};
use crate::rows::{self, WayByIdBuilder, WaySpatialBuilder};
use crate::schema;
use crate::spill::{Meta, SpillSet, WaySpillRow};
use crate::store::NodeStore;
use crate::writer::{PartInfo, PartWriter, RowGroupSizing, SingleFileWriter, TableKind};
use anyhow::Result;
use osmpbf::{Element, ElementReader};
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub const WAY_BYID_PART_ROWS: usize = 1_000_000;
pub const BATCH_ROWS: usize = 64_000;

/// Above this on-disk spill size, sort a cell's ways in bounded-memory
/// chunks (external merge sort) instead of loading the whole cell into
/// memory (docs/m1-contracts.md section 3.2: "if a way cell exceeds a few
/// GB ... sort in chunks and merge" -- this is that threshold). It's
/// measured against *encoded* spill bytes, but a decoded `WaySpillRow` runs
/// several times larger: `refs`/`tags` become owned `Vec`/`String`
/// allocations (heap header + capacity overhead per allocation, not just
/// the raw bytes) and `geometry_wkb` becomes an owned `Vec<u8>` copy of
/// bytes that were a borrowed slice on disk. 512 MiB of encoded bytes
/// keeps a single chunk's decoded `Vec<WaySpillRow>` in the low single-digit
/// GB even under a generous (4x) blowup estimate -- comfortably inside one
/// rayon worker's share of memory when cells are processed in parallel
/// across `--threads` workers, while staying well above the size of the
/// vast majority of cells (only a handful of root/near-root cells at
/// planet scale are expected to ever cross it; see docs/m1-report.md).
/// Reused below as the target size of each sorted run, too -- see
/// `spill_to_sorted_runs`.
const WAY_CHUNK_SORT_THRESHOLD_BYTES: u64 = 512 * 1024 * 1024;

/// Row groups by target compressed bytes (docs/m1-contracts.md section 3 /
/// M1 tuning brief: "byid ways and spatial ways ~= 2 MB compressed"),
/// resolved per file from a real sample batch (see
/// `writer::RowGroupSizing`). Way byid rows carry no geometry; spatial way
/// rows do (WKB), so bytes/row differ a lot between the two -- each gets
/// its own resolved row count even though they share the same target.
const WAY_BYID_ROW_GROUP_SIZING: RowGroupSizing = RowGroupSizing::AdaptiveBytes {
    target_bytes: 2_000_000,
    min_rows: 4_000,
    max_rows: 100_000,
};
const WAY_SPATIAL_ROW_GROUP_SIZING: RowGroupSizing = RowGroupSizing::AdaptiveBytes {
    target_bytes: 4_000_000,
    min_rows: 2_000,
    max_rows: 100_000,
};

pub struct WayPassResult {
    pub byid_parts: Vec<PartInfo>,
    pub way_count: u64,
    pub spatial_rows: u64,
    pub spatial_bytes: u64,
    /// Populated only when `--bbox` is set (used by the relation pass'
    /// simplified "smart" keep rule); see the README note in main.rs.
    pub kept_way_ids: Option<std::collections::HashSet<i64>>,
    pub max_timestamp_us: Option<i64>,
}

#[allow(clippy::too_many_arguments)]
pub fn way_pass(
    pbf_path: &Path,
    rawdir: &Path,
    tmpdir: &Path,
    leaf_index: &LeafIndex,
    node_store: &NodeStore,
    promoted_keys: &[String],
    bbox: Option<BBox>,
    threads: usize,
) -> Result<WayPassResult> {
    let byid_dir = rawdir.join("way");
    let spill_dir = tmpdir.join("spill").join("way");
    let byid_schema = schema::way_byid_schema(promoted_keys);
    let mut byid_writer = PartWriter::new(
        &byid_dir,
        byid_schema.clone(),
        WAY_BYID_PART_ROWS,
        TableKind::WayById,
        WAY_BYID_ROW_GROUP_SIZING,
    )?;
    let mut spill = SpillSet::new(&spill_dir)?;

    let mut batch = WayByIdBuilder::new(promoted_keys);
    let mut batch_lo = i64::MAX;
    let mut batch_hi = i64::MIN;
    let mut way_count: u64 = 0;
    let mut kept_way_ids: Option<std::collections::HashSet<i64>> =
        if bbox.is_some() { Some(std::collections::HashSet::new()) } else { None };
    let mut max_timestamp_us: Option<i64> = None;

    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|el| {
        let way = match &el {
            Element::Way(w) => w,
            _ => return,
        };
        let id = way.id();
        let refs: Vec<i64> = way.refs().collect();
        let tags = pbfutil::tags_owned(way.tags());
        let meta: Meta = pbfutil::info_meta(&way.info());

        let mut xmin: Option<i32> = None;
        let mut xmax: Option<i32> = None;
        let mut ymin: Option<i32> = None;
        let mut ymax: Option<i32> = None;
        let mut resolved_points: Vec<(f64, f64)> = Vec::with_capacity(refs.len());
        let mut any_resolved = false;
        for r in &refs {
            if let Some((lat_e7, lon_e7)) = node_store.get(*r) {
                any_resolved = true;
                xmin = Some(xmin.map_or(lon_e7, |v| v.min(lon_e7)));
                xmax = Some(xmax.map_or(lon_e7, |v| v.max(lon_e7)));
                ymin = Some(ymin.map_or(lat_e7, |v| v.min(lat_e7)));
                ymax = Some(ymax.map_or(lat_e7, |v| v.max(lat_e7)));
                resolved_points.push((lon_e7 as f64 / 1e7, lat_e7 as f64 / 1e7));
            }
        }

        if bbox.is_some() && !any_resolved {
            // "smart" bbox semantics, simplified: a way is kept when at
            // least one of its nodes resolves -- and with --bbox, the node
            // store only holds in-bbox nodes (see cells.rs/nodes.rs), so
            // "resolves" here already means "has an in-bbox node". See the
            // README note in main.rs about this simplification.
            return;
        }

        way_count += 1;
        if let Some(set) = kept_way_ids.as_mut() {
            set.insert(id);
        }
        if let Some(ts) = meta.timestamp_us {
            max_timestamp_us = Some(max_timestamp_us.map_or(ts, |m| m.max(ts)));
        }
        let is_closed = refs.len() >= 4 && refs.first() == refs.last();
        let is_area = rows::compute_is_area(&tags, is_closed);
        let geometry_wkb = if resolved_points.len() >= 2 {
            Some(rows::wkb_linestring(&resolved_points))
        } else {
            None
        };
        let (centroid_lat_e7, centroid_lon_e7) = match (ymin, ymax, xmin, xmax) {
            (Some(a), Some(b), Some(c), Some(d)) => (
                Some(((a as i64 + b as i64) as f64 / 2.0).round() as i32),
                Some(((c as i64 + d as i64) as f64 / 2.0).round() as i32),
            ),
            _ => (None, None),
        };
        let (cell, hilbert) = match (ymin, ymax, xmin, xmax) {
            (Some(a), Some(b), Some(c), Some(d)) => {
                let south = a as f64 / 1e7;
                let north = b as f64 / 1e7;
                let west = c as f64 / 1e7;
                let east = d as f64 / 1e7;
                let cell = leaf_index.containing_cell((south, west, north, east));
                let h = cells::hilbert_key(centroid_lat_e7.unwrap(), centroid_lon_e7.unwrap());
                (cell, h)
            }
            _ => (cells::ROOT.to_string(), 0u64),
        };

        batch.append(
            id,
            &refs,
            &tags,
            &meta,
            (xmin, ymin, xmax, ymax),
            is_closed,
            is_area,
            &cell,
            hilbert,
        );
        batch_lo = batch_lo.min(id);
        batch_hi = batch_hi.max(id);
        if batch.len() >= BATCH_ROWS {
            let full = std::mem::replace(&mut batch, WayByIdBuilder::new(promoted_keys));
            byid_writer
                .write_batch(full.finish(byid_schema.clone()), Some((batch_lo, batch_hi)))
                .expect("write way byid batch");
            batch_lo = i64::MAX;
            batch_hi = i64::MIN;
        }

        let spill_row = WaySpillRow {
            id,
            refs,
            tags,
            meta,
            xmin_e7: xmin,
            ymin_e7: ymin,
            xmax_e7: xmax,
            ymax_e7: ymax,
            geometry_wkb,
            is_closed,
            is_area,
            centroid_lat_e7,
            centroid_lon_e7,
            hilbert,
        };
        spill.append(&cell, &spill_row.encode()).expect("append way spill record");
    })?;

    if batch.len() > 0 {
        byid_writer.write_batch(batch.finish(byid_schema.clone()), Some((batch_lo, batch_hi)))?;
    }
    let byid_parts = byid_writer.finish()?;
    let spill_files = spill.finish()?;

    if threads > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().ok();
    }
    let promoted_keys_owned = promoted_keys.to_vec();
    let geo_meta = crate::writer::geo_metadata(&["LineString"]);
    let results: Vec<(u64, u64)> = spill_files
        .par_iter()
        .map(|(cell, path)| -> Result<(u64, u64)> {
            let spill_bytes = std::fs::metadata(path)?.len();
            let (rows_n, bytes_n) = if spill_bytes < WAY_CHUNK_SORT_THRESHOLD_BYTES {
                let mut rows_v = decode_and_sort_in_memory(path)?;
                write_sorted_way_cell(cell, rows_v.drain(..), rawdir, &promoted_keys_owned, &geo_meta)?
            } else {
                // Root-level cells at planet scale can be several GB once
                // WKB + tags are included (docs/m1-contracts.md section
                // 3.2) -- reading+decoding the whole file at once risks
                // OOM, so fall back to an external merge sort: chunk the
                // spill file into sorted runs on disk, then k-way merge
                // them into the same fully-sorted stream the in-memory
                // path would have produced in one shot.
                let run_dir = path.parent().expect("spill path has a parent directory");
                let run_paths =
                    spill_to_sorted_runs(path, run_dir, cell, WAY_CHUNK_SORT_THRESHOLD_BYTES)?;
                let merged = WayRunMerge::new(run_paths)?;
                write_sorted_way_cell(cell, merged, rawdir, &promoted_keys_owned, &geo_meta)?
            };
            std::fs::remove_file(path).ok();
            Ok((rows_n, bytes_n))
        })
        .collect::<Result<Vec<_>>>()?;

    let spatial_rows: u64 = results.iter().map(|r| r.0).sum();
    let spatial_bytes: u64 = results.iter().map(|r| r.1).sum();

    Ok(WayPassResult {
        byid_parts,
        way_count,
        spatial_rows,
        spatial_bytes,
        kept_way_ids,
        max_timestamp_us,
    })
}

// ---- per-cell sort: in-memory (small cells) and chunked (large cells) -----

/// Today's path for the vast majority of cells: read the whole spill file,
/// decode every record, sort in memory. Simple and fast; unsafe to use once
/// a cell's spill file is large enough that its *decoded* rows could
/// exhaust memory (see `WAY_CHUNK_SORT_THRESHOLD_BYTES`).
fn decode_and_sort_in_memory(path: &Path) -> Result<Vec<WaySpillRow>> {
    let records = crate::spill::read_records(path)?;
    let mut rows_v: Vec<WaySpillRow> = records.into_iter().map(|b| WaySpillRow::decode(&b)).collect();
    rows_v.sort_by_key(|a| (a.hilbert, a.id));
    Ok(rows_v)
}

/// Streams `path` sequentially (never materializing more than one chunk's
/// worth of decoded rows at a time), sorting and flushing each chunk to its
/// own "run" file on disk once the buffered *encoded* bytes reach
/// `chunk_bytes`. Returns the run file paths, each already sorted by
/// `(hilbert, id)` -- the caller k-way merges them (`WayRunMerge`).
fn spill_to_sorted_runs(path: &Path, run_dir: &Path, cell: &str, chunk_bytes: u64) -> Result<Vec<PathBuf>> {
    let f = File::open(path)?;
    let mut r = BufReader::with_capacity(crate::spill::SPILL_BUF_SIZE, f);
    let mut run_paths = Vec::new();
    let mut buf: Vec<WaySpillRow> = Vec::new();
    let mut buf_bytes: u64 = 0;
    let mut run_idx: usize = 0;
    let mut len_buf = [0u8; 4];
    loop {
        match r.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        r.read_exact(&mut payload)?;
        buf_bytes += len as u64;
        buf.push(WaySpillRow::decode(&payload));
        if buf_bytes >= chunk_bytes {
            run_paths.push(write_sorted_run(&mut buf, run_dir, cell, run_idx)?);
            run_idx += 1;
            buf_bytes = 0;
        }
    }
    if !buf.is_empty() {
        run_paths.push(write_sorted_run(&mut buf, run_dir, cell, run_idx)?);
    }
    Ok(run_paths)
}

/// Sorts `buf` (draining it) and writes it as one run file, same
/// length-prefixed `WaySpillRow::encode` format the source spill file uses
/// -- `WayRunMerge` reads runs with the same decoder.
fn write_sorted_run(buf: &mut Vec<WaySpillRow>, run_dir: &Path, cell: &str, run_idx: usize) -> Result<PathBuf> {
    buf.sort_by_key(|a| (a.hilbert, a.id));
    let path = run_dir.join(format!("{cell}.run-{run_idx}"));
    let f = File::create(&path)?;
    let mut w = BufWriter::with_capacity(crate::spill::SPILL_BUF_SIZE, f);
    for row in buf.drain(..) {
        let enc = row.encode();
        w.write_all(&(enc.len() as u32).to_le_bytes())?;
        w.write_all(&enc)?;
    }
    w.flush()?;
    Ok(path)
}

fn read_one_record(r: &mut BufReader<File>) -> Result<Option<WaySpillRow>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok(Some(WaySpillRow::decode(&payload)))
}

/// One entry in `WayRunMerge`'s heap: the next not-yet-emitted row from one
/// run, ordered by the same `(hilbert, id)` key the runs are sorted by.
struct RunHeapItem {
    hilbert: u64,
    id: i64,
    run_idx: usize,
    row: WaySpillRow,
}

impl PartialEq for RunHeapItem {
    fn eq(&self, other: &Self) -> bool {
        (self.hilbert, self.id) == (other.hilbert, other.id)
    }
}
impl Eq for RunHeapItem {}
impl PartialOrd for RunHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RunHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.hilbert, self.id).cmp(&(other.hilbert, other.id))
    }
}

/// K-way merge over already-sorted run files, yielding the same globally
/// sorted `(hilbert, id)` order the in-memory path produces -- but holding
/// only one decoded row per run at a time (a `BinaryHeap` of `Reverse` items
/// turns the max-heap `BinaryHeap` into the min-heap this needs), not the
/// whole cell.
struct WayRunMerge {
    readers: Vec<Option<BufReader<File>>>,
    run_paths: Vec<PathBuf>,
    heap: BinaryHeap<Reverse<RunHeapItem>>,
}

impl WayRunMerge {
    fn new(run_paths: Vec<PathBuf>) -> Result<Self> {
        let mut readers: Vec<Option<BufReader<File>>> = Vec::with_capacity(run_paths.len());
        let mut heap = BinaryHeap::new();
        for (idx, p) in run_paths.iter().enumerate() {
            let f = File::open(p)?;
            let mut r = BufReader::with_capacity(crate::spill::SPILL_BUF_SIZE, f);
            match read_one_record(&mut r)? {
                Some(row) => {
                    heap.push(Reverse(RunHeapItem {
                        hilbert: row.hilbert,
                        id: row.id,
                        run_idx: idx,
                        row,
                    }));
                    readers.push(Some(r));
                }
                None => readers.push(None), // an empty run (shouldn't happen, but harmless)
            }
        }
        Ok(WayRunMerge { readers, run_paths, heap })
    }
}

impl Iterator for WayRunMerge {
    type Item = WaySpillRow;

    fn next(&mut self) -> Option<WaySpillRow> {
        let Reverse(item) = self.heap.pop()?;
        let idx = item.run_idx;
        if let Some(reader) = self.readers[idx].as_mut() {
            // Decode errors here would mean a corrupt run file we just
            // wrote ourselves -- treated the same as the rest of this
            // module treats spill I/O, with `expect` rather than
            // threading a `Result` through the `Iterator` trait.
            match read_one_record(reader).expect("read way run record") {
                Some(next_row) => self.heap.push(Reverse(RunHeapItem {
                    hilbert: next_row.hilbert,
                    id: next_row.id,
                    run_idx: idx,
                    row: next_row,
                })),
                None => {
                    self.readers[idx] = None;
                    std::fs::remove_file(&self.run_paths[idx]).ok();
                }
            }
        }
        Some(item.row)
    }
}

impl Drop for WayRunMerge {
    /// Belt-and-suspenders cleanup for runs the merge never finished
    /// reading (e.g. the caller's write loop returned early on error) --
    /// `next()` already deletes each run as it's exhausted in the normal
    /// (fully-drained) case.
    fn drop(&mut self) {
        for (idx, reader) in self.readers.iter().enumerate() {
            if reader.is_some() {
                std::fs::remove_file(&self.run_paths[idx]).ok();
            }
        }
    }
}

/// Shared tail of the per-cell way pass: given a (fully sorted) stream of
/// `WaySpillRow`s from either sort path, resolve row-group sizing from a
/// real sample, then batch the rows through `WaySpatialBuilder` into the
/// cell's `SingleFileWriter`. Buffering the first `BATCH_ROWS` rows up
/// front serves double duty -- it's the row-group-size sample (a real
/// prefix of the final sorted output, per `writer::RowGroupSizing`) and the
/// first batch(es) written -- and it's the only place this function holds
/// more than one `BATCH_ROWS`-sized batch in memory, regardless of which
/// sort path produced `rows_iter`.
fn write_sorted_way_cell(
    cell: &str,
    rows_iter: impl Iterator<Item = WaySpillRow>,
    rawdir: &Path,
    promoted_keys: &[String],
    geo_meta: &str,
) -> Result<(u64, u64)> {
    let spatial_schema = schema::way_spatial_schema(promoted_keys);
    let out_path = rawdir
        .join("spatial")
        .join("way")
        .join(format!("cell={cell}"))
        .join("part-0.parquet");

    let mut rows_iter = rows_iter;
    let mut sample_buf: Vec<WaySpillRow> = Vec::new();
    while sample_buf.len() < BATCH_ROWS {
        match rows_iter.next() {
            Some(row) => sample_buf.push(row),
            None => break,
        }
    }

    let sample = if sample_buf.is_empty() {
        None
    } else {
        let mut sb = WaySpatialBuilder::new(promoted_keys);
        for row in &sample_buf {
            sb.append(
                row.id,
                &row.refs,
                &row.tags,
                &row.meta,
                (row.xmin_e7, row.ymin_e7, row.xmax_e7, row.ymax_e7),
                row.geometry_wkb.as_deref(),
                row.is_closed,
                row.is_area,
                (row.centroid_lat_e7, row.centroid_lon_e7),
                cell,
                row.hilbert,
            );
        }
        Some(sb.finish(spatial_schema.clone()))
    };
    let mut writer = SingleFileWriter::create(
        &out_path,
        spatial_schema.clone(),
        TableKind::WaySpatial,
        WAY_SPATIAL_ROW_GROUP_SIZING,
        sample.as_ref(),
        Some(geo_meta.to_string()),
    )?;
    let mut b = WaySpatialBuilder::new(promoted_keys);
    for row in sample_buf.into_iter().chain(rows_iter) {
        b.append(
            row.id,
            &row.refs,
            &row.tags,
            &row.meta,
            (row.xmin_e7, row.ymin_e7, row.xmax_e7, row.ymax_e7),
            row.geometry_wkb.as_deref(),
            row.is_closed,
            row.is_area,
            (row.centroid_lat_e7, row.centroid_lon_e7),
            cell,
            row.hilbert,
        );
        if b.len() >= BATCH_ROWS {
            let full = std::mem::replace(&mut b, WaySpatialBuilder::new(promoted_keys));
            writer.write(&full.finish(spatial_schema.clone()))?;
        }
    }
    if b.len() > 0 {
        writer.write(&b.finish(spatial_schema.clone()))?;
    }
    writer.finish(&out_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spill::{Meta, SpillSet};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A fresh scratch directory under the system temp dir, unique per call
    /// (no `tempfile` crate dependency needed for a handful of tests) --
    /// the caller removes it when done.
    fn test_dir(name: &str) -> PathBuf {
        let n = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("osmpq-raw-ways-test-{name}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A way spill row with no resolved nodes -- same shape `way_pass`
    /// produces for a way none of whose refs resolved (root cell,
    /// `hilbert = 0`, every bbox/geometry field `None`); tests that don't
    /// care about geometry pass their own `hilbert` in anyway to exercise
    /// sort ordering.
    fn minimal_row(id: i64, hilbert: u64) -> WaySpillRow {
        WaySpillRow {
            id,
            refs: vec![],
            tags: vec![],
            meta: Meta::default(),
            xmin_e7: None,
            ymin_e7: None,
            xmax_e7: None,
            ymax_e7: None,
            geometry_wkb: None,
            is_closed: false,
            is_area: false,
            centroid_lat_e7: None,
            centroid_lon_e7: None,
            hilbert,
        }
    }

    /// Writes `rows` to a spill file the same way `way_pass` does (via
    /// `SpillSet`) and returns its path. `SpillSet` only creates a cell's
    /// file lazily on first `append`, so an all-empty `rows` falls back to
    /// creating the (empty) file directly.
    fn write_spill_file(dir: &Path, cell: &str, rows: &[WaySpillRow]) -> PathBuf {
        let mut spill = SpillSet::new(dir).unwrap();
        for row in rows {
            spill.append(cell, &row.encode()).unwrap();
        }
        let files = spill.finish().unwrap();
        files
            .into_iter()
            .find(|(c, _)| c == cell)
            .map(|(_, p)| p)
            .unwrap_or_else(|| {
                let p = dir.join(format!("{cell}.spill"));
                std::fs::File::create(&p).unwrap();
                p
            })
    }

    fn ids_via_in_memory(path: &Path) -> Vec<i64> {
        decode_and_sort_in_memory(path).unwrap().into_iter().map(|r| r.id).collect()
    }

    fn ids_via_chunked(path: &Path, run_dir: &Path, cell: &str, chunk_bytes: u64) -> (Vec<i64>, Vec<PathBuf>) {
        let run_paths = spill_to_sorted_runs(path, run_dir, cell, chunk_bytes).unwrap();
        let merge = WayRunMerge::new(run_paths.clone()).unwrap();
        (merge.map(|r| r.id).collect(), run_paths)
    }

    #[test]
    fn chunked_matches_in_memory_order() {
        let dir = test_dir("matches");
        let cell = "root";
        // Deliberately out-of-order insertion, with duplicate hilbert
        // values that must tie-break on id.
        let rows = vec![
            minimal_row(30, 5),
            minimal_row(10, 5),
            minimal_row(20, 1),
            minimal_row(1, 100),
            minimal_row(2, 100),
            minimal_row(99, 0),
            minimal_row(15, 5),
            minimal_row(7, 2),
            minimal_row(8, 2),
            minimal_row(3, 3),
        ];
        let n = rows.len();
        let path = write_spill_file(&dir, cell, &rows);

        let expected = ids_via_in_memory(&path);
        // A tiny threshold forces several chunks/runs out of only 10 rows.
        let (actual, run_paths) = ids_via_chunked(&path, &dir, cell, 200);
        assert!(run_paths.len() > 1, "expected the tiny threshold to force multiple runs");
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), n);
        for p in &run_paths {
            assert!(!p.exists(), "run file should be deleted once fully merged");
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn chunked_handles_zero_and_one_record() {
        let dir = test_dir("edges-zero-one");

        // Zero records.
        let empty_path = dir.join("empty.spill");
        std::fs::File::create(&empty_path).unwrap();
        assert!(decode_and_sort_in_memory(&empty_path).unwrap().is_empty());
        let empty_run_paths = spill_to_sorted_runs(&empty_path, &dir, "empty", 200).unwrap();
        assert!(empty_run_paths.is_empty(), "no records should produce no run files");
        let merged: Vec<WaySpillRow> = WayRunMerge::new(empty_run_paths).unwrap().collect();
        assert!(merged.is_empty());

        // One record.
        let one_path = write_spill_file(&dir, "one", &[minimal_row(42, 7)]);
        let (ids, _) = ids_via_chunked(&one_path, &dir, "one", 200);
        assert_eq!(ids, vec![42]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn chunk_boundary_lands_exactly_on_a_record_boundary() {
        let dir = test_dir("boundary");
        // Every minimal row here encodes to the same size (fixed-width
        // fields only), so a threshold of exactly 2 records' worth of
        // bytes should produce runs of exactly 2 records each, with no
        // partial/overflow run.
        let rows: Vec<WaySpillRow> = (0..6u32).map(|i| minimal_row(i as i64, i as u64)).collect();
        let record_len = rows[0].encode().len() as u64;
        let path = write_spill_file(&dir, "boundary", &rows);

        let run_paths = spill_to_sorted_runs(&path, &dir, "boundary", record_len * 2).unwrap();
        assert_eq!(run_paths.len(), 3, "6 equal-size records at a 2-record threshold should make exactly 3 runs");
        let ids: Vec<i64> = WayRunMerge::new(run_paths).unwrap().map(|r| r.id).collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn chunked_handles_unresolved_root_cell_rows() {
        // Ways with no resolved nodes land in the root cell with
        // `hilbert = 0` and every bbox/geometry field `None` (see
        // `way_pass`'s `(cell, hilbert)` match on `(ymin, ymax, xmin,
        // xmax)`) -- confirm the chunked path round-trips that shape.
        let dir = test_dir("root-unresolved");
        let rows = vec![minimal_row(5, 0), minimal_row(1, 0), minimal_row(3, 0)];
        let path = write_spill_file(&dir, "root", &rows);

        let expected = ids_via_in_memory(&path);
        let (actual, _run_paths) = ids_via_chunked(&path, &dir, "root", 50);
        assert_eq!(actual, expected);
        assert_eq!(actual, vec![1, 3, 5]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_sorted_way_cell_matches_between_sort_paths() {
        // Exercise the full tail (sample + SingleFileWriter + batching)
        // with both sort paths and confirm they write byte-identical
        // Parquet output, not just that the sort primitives agree on
        // order.
        let dir = test_dir("write-e2e");
        let cell = "root";
        let rows: Vec<WaySpillRow> = (0..25i64).map(|i| minimal_row(100 - i, (i % 4) as u64)).collect();
        let path = write_spill_file(&dir, cell, &rows);
        let promoted_keys: Vec<String> = vec![];
        let geo_meta = crate::writer::geo_metadata(&["LineString"]);

        let mem_rawdir = dir.join("rawdir_mem");
        let mut mem_rows = decode_and_sort_in_memory(&path).unwrap();
        write_sorted_way_cell(cell, mem_rows.drain(..), &mem_rawdir, &promoted_keys, &geo_meta).unwrap();

        let chunk_rawdir = dir.join("rawdir_chunk");
        let run_paths = spill_to_sorted_runs(&path, &dir, cell, 100).unwrap();
        assert!(run_paths.len() > 1, "expected the tiny threshold to force multiple runs");
        let merged = WayRunMerge::new(run_paths).unwrap();
        write_sorted_way_cell(cell, merged, &chunk_rawdir, &promoted_keys, &geo_meta).unwrap();

        let part_rel = Path::new("spatial").join("way").join(format!("cell={cell}")).join("part-0.parquet");
        let mem_bytes = std::fs::read(mem_rawdir.join(&part_rel)).unwrap();
        let chunk_bytes_out = std::fs::read(chunk_rawdir.join(&part_rel)).unwrap();
        assert_eq!(
            mem_bytes, chunk_bytes_out,
            "in-memory and chunked paths should produce byte-identical parquet output for the same input rows"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
