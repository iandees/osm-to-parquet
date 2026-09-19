//! Pass 3 (docs/m1-contracts.md section 3): way byid + spatial (loose cell
//! placement, geometry assembly).

use crate::cells::{self, LeafIndex};
use crate::pbfutil::{self, BBox};
use crate::rows::{self, WayByIdBuilder, WaySpatialBuilder};
use crate::schema;
use crate::spill::{Meta, SpillSet, WaySpillRow};
use crate::store::NodeStore;
use crate::writer::{PartInfo, PartWriter, SingleFileWriter};
use anyhow::Result;
use osmpbf::{Element, ElementReader};
use rayon::prelude::*;
use std::path::Path;

pub const WAY_BYID_PART_ROWS: usize = 1_000_000;
pub const WAY_BYID_ROW_GROUP_ROWS: usize = 8_000;
pub const WAY_SPATIAL_ROW_GROUP_ROWS: usize = 10_000;
pub const BATCH_ROWS: usize = 64_000;

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
    let mut byid_writer = PartWriter::new(&byid_dir, byid_schema.clone(), WAY_BYID_PART_ROWS, WAY_BYID_ROW_GROUP_ROWS)?;
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
            let records = crate::spill::read_records(path)?;
            let mut rows_v: Vec<WaySpillRow> = records.into_iter().map(|b| WaySpillRow::decode(&b)).collect();
            rows_v.sort_by(|a, b| (a.hilbert, a.id).cmp(&(b.hilbert, b.id)));

            let spatial_schema = schema::way_spatial_schema(&promoted_keys_owned);
            let out_path = rawdir
                .join("spatial")
                .join("way")
                .join(format!("cell={cell}"))
                .join("part-0.parquet");
            let mut writer = SingleFileWriter::create(
                &out_path,
                spatial_schema.clone(),
                WAY_SPATIAL_ROW_GROUP_ROWS,
                Some(geo_meta.clone()),
            )?;
            let mut b = WaySpatialBuilder::new(&promoted_keys_owned);
            for row in rows_v.drain(..) {
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
                    let full = std::mem::replace(&mut b, WaySpatialBuilder::new(&promoted_keys_owned));
                    writer.write(&full.finish(spatial_schema.clone()))?;
                }
            }
            if b.len() > 0 {
                writer.write(&b.finish(spatial_schema.clone()))?;
            }
            let (rows_n, bytes_n) = writer.finish(&out_path)?;
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
