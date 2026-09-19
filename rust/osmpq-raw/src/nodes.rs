//! Passes 1 and 2 (docs/m1-contracts.md section 3): the node histogram /
//! leaf selection, and the node byid + spatial + location-store pass.

use crate::cells::{self, LeafIndex};
use crate::pbfutil::{self, BBox};
use crate::rows::{NodeByIdBuilder, NodeSpatialBuilder};
use crate::schema;
use crate::spill::{Meta, NodeSpillRow, SpillSet};
use crate::store::{NodeStore, NodeStoreBuilder};
use crate::writer::{PartInfo, PartWriter, SingleFileWriter};
use anyhow::Result;
use osmpbf::{Element, ElementReader};
use rayon::prelude::*;
use std::path::Path;

pub const NODE_BYID_PART_ROWS: usize = 4_000_000;
pub const NODE_ROW_GROUP_ROWS: usize = 64_000;
pub const BATCH_ROWS: usize = 64_000;

pub struct HistogramResult {
    pub counts: Vec<u32>,
    pub max_id: i64,
    pub node_count: u64,
}

/// Pass 1: read nodes once, count per depth-`max_depth` quadkey.
pub fn histogram_pass(pbf_path: &Path, max_depth: u32, bbox: Option<BBox>) -> Result<HistogramResult> {
    let size = 1usize << (2 * max_depth);
    let mut counts = vec![0u32; size];
    let mut max_id: i64 = 0;
    let mut node_count: u64 = 0;
    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|el| {
        let (id, lat_e7, lon_e7) = match &el {
            Element::Node(n) => (n.id(), n.decimicro_lat(), n.decimicro_lon()),
            Element::DenseNode(n) => (n.id(), n.decimicro_lat(), n.decimicro_lon()),
            _ => return,
        };
        if let Some(bb) = bbox {
            if !pbfutil::point_in_bbox(lat_e7, lon_e7, bb) {
                return;
            }
        }
        let code = cells::qk_code(lat_e7 as i64, lon_e7 as i64, max_depth);
        counts[code as usize] = counts[code as usize].saturating_add(1);
        if id > max_id {
            max_id = id;
        }
        node_count += 1;
    })?;
    Ok(HistogramResult {
        counts,
        max_id,
        node_count,
    })
}

pub struct NodePassResult {
    pub byid_parts: Vec<PartInfo>,
    pub node_store: NodeStore,
    pub node_count: u64,
    pub tagged_count: u64,
    pub spatial_rows: u64,
    pub spatial_bytes: u64,
    /// (south, west, north, east) in degrees, over kept nodes.
    pub extent: Option<(f64, f64, f64, f64)>,
    pub max_timestamp_us: Option<i64>,
}

#[allow(clippy::too_many_arguments)]
pub fn node_pass(
    pbf_path: &Path,
    rawdir: &Path,
    tmpdir: &Path,
    leaf_index: &LeafIndex,
    promoted_keys: &[String],
    bbox: Option<BBox>,
    mut store_builder: NodeStoreBuilder,
    threads: usize,
) -> Result<NodePassResult> {
    let byid_dir = rawdir.join("node");
    let spill_dir = tmpdir.join("spill").join("node");
    let mut byid_writer = PartWriter::new(
        &byid_dir,
        schema::node_byid_schema(promoted_keys),
        NODE_BYID_PART_ROWS,
        NODE_ROW_GROUP_ROWS,
    )?;
    let mut spill = SpillSet::new(&spill_dir)?;

    let mut batch = NodeByIdBuilder::new(promoted_keys);
    let mut batch_lo: i64 = i64::MAX;
    let mut batch_hi: i64 = i64::MIN;
    let mut node_count: u64 = 0;
    let mut tagged_count: u64 = 0;
    let mut ext_min_lat: i32 = i32::MAX;
    let mut ext_max_lat: i32 = i32::MIN;
    let mut ext_min_lon: i32 = i32::MAX;
    let mut ext_max_lon: i32 = i32::MIN;
    let mut max_timestamp_us: Option<i64> = None;

    let byid_schema = schema::node_byid_schema(promoted_keys);

    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|el| {
        let (id, lat_e7, lon_e7, tags, meta): (i64, i32, i32, Vec<(String, String)>, Meta) = match &el {
            Element::DenseNode(n) => (
                n.id(),
                n.decimicro_lat(),
                n.decimicro_lon(),
                pbfutil::tags_owned(n.tags()),
                pbfutil::dense_meta(n.info()),
            ),
            Element::Node(n) => (
                n.id(),
                n.decimicro_lat(),
                n.decimicro_lon(),
                pbfutil::tags_owned(n.tags()),
                pbfutil::info_meta(&n.info()),
            ),
            _ => return,
        };
        if let Some(bb) = bbox {
            if !pbfutil::point_in_bbox(lat_e7, lon_e7, bb) {
                return;
            }
        }
        let hilbert = cells::hilbert_key(lat_e7, lon_e7);
        let leaf_idx = leaf_index.leaf_idx_for_point(lat_e7, lon_e7);
        let cell = leaf_index.key_at(leaf_idx);

        node_count += 1;
        if !tags.is_empty() {
            tagged_count += 1;
        }
        ext_min_lat = ext_min_lat.min(lat_e7);
        ext_max_lat = ext_max_lat.max(lat_e7);
        ext_min_lon = ext_min_lon.min(lon_e7);
        ext_max_lon = ext_max_lon.max(lon_e7);
        if let Some(ts) = meta.timestamp_us {
            max_timestamp_us = Some(max_timestamp_us.map_or(ts, |m| m.max(ts)));
        }

        store_builder.put(id, lat_e7, lon_e7);

        batch.append(id, lat_e7, lon_e7, &tags, &meta, cell, hilbert);
        batch_lo = batch_lo.min(id);
        batch_hi = batch_hi.max(id);
        if batch.len() >= BATCH_ROWS {
            let full = std::mem::replace(&mut batch, NodeByIdBuilder::new(promoted_keys));
            let rb = full.finish(byid_schema.clone());
            byid_writer
                .write_batch(rb, Some((batch_lo, batch_hi)))
                .expect("write node byid batch");
            batch_lo = i64::MAX;
            batch_hi = i64::MIN;
        }

        let spill_row = NodeSpillRow {
            id,
            lat_e7,
            lon_e7,
            hilbert,
            tags,
            meta,
        };
        spill
            .append(cell, &spill_row.encode())
            .expect("append node spill record");
    })?;

    if batch.len() > 0 {
        let rb = batch.finish(byid_schema.clone());
        byid_writer.write_batch(rb, Some((batch_lo, batch_hi)))?;
    }
    let byid_parts = byid_writer.finish()?;
    let node_store = store_builder.finish()?;
    let spill_files = spill.finish()?;

    // Per-leaf (parallel): sort by (hilbert, id), split tagged/untagged,
    // write the two spatial partitions.
    let promoted_keys_owned = promoted_keys.to_vec();
    if threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .ok();
    }
    let results: Vec<(u64, u64)> = spill_files
        .par_iter()
        .map(|(cell, path)| -> Result<(u64, u64)> {
            let records = crate::spill::read_records(path)?;
            let mut rows: Vec<NodeSpillRow> = records.into_iter().map(|b| NodeSpillRow::decode(&b)).collect();
            rows.sort_by(|a, b| (a.hilbert, a.id).cmp(&(b.hilbert, b.id)));

            let spatial_schema = schema::node_spatial_schema(&promoted_keys_owned);
            let tagged_path = rawdir
                .join("spatial")
                .join("node")
                .join(format!("cell={cell}"))
                .join("tagged=true")
                .join("part-0.parquet");
            let untagged_path = rawdir
                .join("spatial")
                .join("node")
                .join(format!("cell={cell}"))
                .join("tagged=false")
                .join("part-0.parquet");

            let n_tagged = rows.iter().filter(|r| !r.tags.is_empty()).count();
            let n_untagged = rows.len() - n_tagged;

            let mut tagged_batch = NodeSpatialBuilder::new(&promoted_keys_owned);
            let mut untagged_batch = NodeSpatialBuilder::new(&promoted_keys_owned);
            let mut tagged_writer = if n_tagged > 0 {
                Some(SingleFileWriter::create(
                    &tagged_path,
                    spatial_schema.clone(),
                    NODE_ROW_GROUP_ROWS,
                    None,
                )?)
            } else {
                None
            };
            let mut untagged_writer = if n_untagged > 0 {
                Some(SingleFileWriter::create(
                    &untagged_path,
                    spatial_schema.clone(),
                    NODE_ROW_GROUP_ROWS,
                    None,
                )?)
            } else {
                None
            };
            for row in rows.drain(..) {
                if row.tags.is_empty() {
                    untagged_batch.append(row.id, row.lat_e7, row.lon_e7, &row.tags, &row.meta, row.hilbert);
                    if untagged_batch.len() >= BATCH_ROWS {
                        let full = std::mem::replace(&mut untagged_batch, NodeSpatialBuilder::new(&promoted_keys_owned));
                        untagged_writer.as_mut().unwrap().write(&full.finish(spatial_schema.clone()))?;
                    }
                } else {
                    tagged_batch.append(row.id, row.lat_e7, row.lon_e7, &row.tags, &row.meta, row.hilbert);
                    if tagged_batch.len() >= BATCH_ROWS {
                        let full = std::mem::replace(&mut tagged_batch, NodeSpatialBuilder::new(&promoted_keys_owned));
                        tagged_writer.as_mut().unwrap().write(&full.finish(spatial_schema.clone()))?;
                    }
                }
            }
            if tagged_batch.len() > 0 {
                tagged_writer.as_mut().unwrap().write(&tagged_batch.finish(spatial_schema.clone()))?;
            }
            if untagged_batch.len() > 0 {
                untagged_writer
                    .as_mut()
                    .unwrap()
                    .write(&untagged_batch.finish(spatial_schema.clone()))?;
            }
            let (tagged_rows, tagged_bytes) = match tagged_writer {
                Some(w) => w.finish(&tagged_path)?,
                None => (0, 0),
            };
            let (untagged_rows, untagged_bytes) = match untagged_writer {
                Some(w) => w.finish(&untagged_path)?,
                None => (0, 0),
            };
            std::fs::remove_file(path).ok();
            Ok((tagged_rows + untagged_rows, tagged_bytes + untagged_bytes))
        })
        .collect::<Result<Vec<_>>>()?;

    let spatial_rows: u64 = results.iter().map(|r| r.0).sum();
    let spatial_bytes: u64 = results.iter().map(|r| r.1).sum();

    let extent = if node_count > 0 {
        Some((
            ext_min_lat as f64 / 1e7,
            ext_min_lon as f64 / 1e7,
            ext_max_lat as f64 / 1e7,
            ext_max_lon as f64 / 1e7,
        ))
    } else {
        None
    };

    Ok(NodePassResult {
        byid_parts,
        node_store,
        node_count,
        tagged_count,
        spatial_rows,
        spatial_bytes,
        extent,
        max_timestamp_us,
    })
}
