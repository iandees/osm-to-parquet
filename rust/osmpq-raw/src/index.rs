//! `osmpq-raw node-way-index` (docs/m1-contracts.md section 3.3).

use crate::rows::NodeWayBuilder;
use crate::schema;
use crate::writer::{PartWriter, RowGroupSizing, TableKind};
use anyhow::Result;
use arrow::array::{Array, Int64Array, ListArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

const N_BUCKETS: usize = 256;
const BUCKET_SHIFT: u32 = 26;
pub const NODE_WAY_PART_ROWS: usize = 8_000_000;

fn bucket_for(node_id: i64) -> usize {
    (((node_id as u64) >> BUCKET_SHIFT) as usize) & (N_BUCKETS - 1)
}

pub struct NodeWayIndexResult {
    pub rows: u64,
    pub bytes: u64,
    pub parts: usize,
    pub way_parts_read: usize,
}

pub fn build_node_way_index(rawdir: &Path, tmpdir: &Path, threads: usize) -> Result<NodeWayIndexResult> {
    if threads > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().ok();
    }
    let way_dir = rawdir.join("way");
    let mut way_parts: Vec<_> = std::fs::read_dir(&way_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
        .collect();
    way_parts.sort();

    let bucket_dir = tmpdir.join("spill").join("node_way_buckets");
    std::fs::create_dir_all(&bucket_dir)?;
    let mut bucket_writers: Vec<BufWriter<File>> = (0..N_BUCKETS)
        .map(|b| {
            let path = bucket_dir.join(format!("bucket-{b:03}.bin"));
            BufWriter::with_capacity(256 * 1024, File::create(path).expect("create bucket file"))
        })
        .collect();

    for way_path in &way_parts {
        let file = File::open(way_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        for batch in reader {
            let batch = batch?;
            let id_col = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let refs_col = batch
                .column_by_name("refs")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let way_id = id_col.value(row);
                if refs_col.is_null(row) {
                    continue;
                }
                let refs_any = refs_col.value(row);
                let refs = refs_any.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..refs.len() {
                    if refs.is_null(i) {
                        continue;
                    }
                    let node_id = refs.value(i);
                    let b = bucket_for(node_id);
                    let mut rec = [0u8; 16];
                    rec[0..8].copy_from_slice(&node_id.to_le_bytes());
                    rec[8..16].copy_from_slice(&way_id.to_le_bytes());
                    bucket_writers[b].write_all(&rec)?;
                }
            }
        }
    }
    for w in bucket_writers.iter_mut() {
        w.flush()?;
    }
    drop(bucket_writers);

    // Sort each bucket in parallel; buckets are non-overlapping, increasing
    // ranges of node_id, so concatenating them in order 0..255 yields a
    // fully (node_id, way_id) sorted sequence.
    let sorted_buckets: Vec<Vec<(i64, i64)>> = (0..N_BUCKETS)
        .into_par_iter()
        .map(|b| -> Result<Vec<(i64, i64)>> {
            let path = bucket_dir.join(format!("bucket-{b:03}.bin"));
            let mut f = BufReader::new(File::open(&path)?);
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            let n = buf.len() / 16;
            let mut pairs = Vec::with_capacity(n);
            for i in 0..n {
                let off = i * 16;
                let node_id = i64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
                let way_id = i64::from_le_bytes(buf[off + 8..off + 16].try_into().unwrap());
                pairs.push((node_id, way_id));
            }
            pairs.sort_unstable();
            std::fs::remove_file(&path).ok();
            Ok(pairs)
        })
        .collect::<Result<Vec<_>>>()?;

    let out_dir = rawdir.join("node_way");
    let schema = schema::node_way_schema();
    let mut writer = PartWriter::new(
        &out_dir,
        schema.clone(),
        NODE_WAY_PART_ROWS,
        TableKind::NodeWay,
        RowGroupSizing::Fixed(100_000),
    )?;
    let mut batch = NodeWayBuilder::new();
    let mut batch_lo = i64::MAX;
    let mut batch_hi = i64::MIN;
    let mut total_rows: u64 = 0;
    for pairs in &sorted_buckets {
        for &(node_id, way_id) in pairs {
            batch.append(node_id, way_id);
            batch_lo = batch_lo.min(node_id);
            batch_hi = batch_hi.max(node_id);
            total_rows += 1;
            if batch.len() >= 64_000 {
                let full = std::mem::replace(&mut batch, NodeWayBuilder::new());
                writer.write_batch(full.finish(schema.clone()), Some((batch_lo, batch_hi)))?;
                batch_lo = i64::MAX;
                batch_hi = i64::MIN;
            }
        }
    }
    if batch.len() > 0 {
        writer.write_batch(batch.finish(schema.clone()), Some((batch_lo, batch_hi)))?;
    }
    let parts = writer.finish()?;
    let total_bytes: u64 = parts.iter().map(|p| p.bytes).sum();

    let parts_json: Vec<serde_json::Value> = parts
        .iter()
        .map(|p| {
            serde_json::json!({
                "path": format!("index/node_way/{}", p.path.file_name().unwrap().to_string_lossy()),
                "rows": p.rows,
                "bytes": p.bytes,
                "min_id": p.min_id,
                "max_id": p.max_id,
            })
        })
        .collect();
    std::fs::write(out_dir.join("parts.json"), serde_json::to_string_pretty(&parts_json)?)?;
    std::fs::remove_dir_all(&bucket_dir).ok();

    Ok(NodeWayIndexResult {
        rows: total_rows,
        bytes: total_bytes,
        parts: parts_json.len(),
        way_parts_read: way_parts.len(),
    })
}
