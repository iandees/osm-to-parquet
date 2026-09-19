//! Parquet writer helpers: per-column `WriterProperties` (dictionary /
//! encoding tuned per column, see `apply_column_properties`), row-group
//! sizing by target compressed bytes rather than a fixed row count (see
//! `RowGroupSizing`), and part-file splitting for byid/index tables /
//! single-file writing for spatial cell files (docs/m1-contracts.md
//! section 3; tuning rationale in docs/m1-report.md).

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{
    EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
};
use parquet::schema::types::ColumnPath;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// ZSTD level 3 is DuckDB's default (see docs/m1-report.md); matching it
/// keeps the comparison in the M1 tuning brief apples-to-apples.
const ZSTD_LEVEL: i32 = 3;

/// Which raw table a `WriterProperties` is being built for -- controls the
/// column-level dictionary/encoding overrides in `apply_column_properties`,
/// since the same logical column (`id`, `hilbert`) is sorted in one table
/// and effectively random in another, and the best encoding depends on it
/// (measured with real Minnesota data; see docs/m1-report.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKind {
    /// `node/part-*.parquet`: id-ordered (as in the PBF).
    NodeById,
    /// `spatial/node/cell=*/tagged=*/part-0.parquet`: (hilbert, id)-ordered.
    NodeSpatial,
    /// `way/part-*.parquet`: id-ordered.
    WayById,
    /// `spatial/way/cell=*/part-0.parquet`: (hilbert, id)-ordered.
    WaySpatial,
    /// `relation/part-*.parquet`: id-ordered.
    Relation,
    /// `node_way/part-*.parquet`: (node_id, way_id)-ordered.
    NodeWay,
}

fn col(parts: &[&str]) -> ColumnPath {
    // NB: `ColumnPath::from(&str)` treats the whole string as a single path
    // segment (no splitting on '.'), which would silently miss every
    // nested column (list/map/struct members) below the top level -- build
    // multi-segment paths from parts explicitly instead.
    ColumnPath::new(parts.iter().map(|s| s.to_string()).collect())
}

/// Disables dictionary encoding for `path` and pins its encoding.
fn no_dict(b: WriterPropertiesBuilder, path: &[&str], encoding: Encoding) -> WriterPropertiesBuilder {
    let p = col(path);
    b.set_column_dictionary_enabled(p.clone(), false)
        .set_column_encoding(p, encoding)
}

/// Per-column dictionary/encoding overrides (docs/m1-report.md tuning
/// notes below). Dictionary encoding is left at its default (enabled) for
/// `user`, `cell`, tag keys/values, promoted string columns, and the
/// `members` struct's `type`/`role` strings -- all low-cardinality,
/// frequently-repeated strings that dictionary-encode well. Everything
/// else handled here is an integer, float or binary column, disabled per
/// the M1 tuning brief ("disable dictionary encoding for all
/// integer/float/binary columns").
///
/// `id` and `hilbert` are context-dependent: `id` is DELTA-encoded in the
/// id-ordered byid/relation tables (near-zero deltas) and PLAIN in the
/// hilbert-ordered spatial tables (measured larger under DELTA there,
/// since id has no relationship to hilbert order); `hilbert` is the
/// mirror image. `changeset`/`timestamp`/`uid` are measured *worse* under
/// DELTA_BINARY_PACKED in every table sampled on Minnesota (not
/// correlated with either sort order -- deltas are as large and as
/// randomly signed as the raw values, and parquet-rs's zig-zag varint
/// packing of that is bigger than PLAIN + ZSTD), so they use PLAIN
/// despite being in the M1 brief's suggested DELTA list; see
/// docs/m1-report.md for the measurements. `version` is a coin flip
/// either way (tiny column) so it follows the brief's suggestion of DELTA.
/// Coordinate/bbox/centroid `*_e7` columns measure smaller under DELTA in
/// every table sampled, so those follow the brief as given. `refs`
/// (way node ids) and `member.ref` do not -- a referenced id has no
/// relation to its position in the list or to the containing way's/
/// relation's id, so PLAIN measures smaller there too (verified against
/// DuckDB's own choice of PLAIN for the same column); see the per-kind
/// comments below and docs/m1-report.md.
fn apply_column_properties(mut b: WriterPropertiesBuilder, kind: TableKind) -> WriterPropertiesBuilder {
    let id_encoding = match kind {
        TableKind::NodeById | TableKind::WayById | TableKind::Relation | TableKind::NodeWay => {
            Encoding::DELTA_BINARY_PACKED
        }
        TableKind::NodeSpatial | TableKind::WaySpatial => Encoding::PLAIN,
    };
    b = no_dict(b, &["id"], id_encoding);

    if matches!(
        kind,
        TableKind::NodeById | TableKind::NodeSpatial | TableKind::WayById | TableKind::WaySpatial
    ) {
        let hilbert_encoding = match kind {
            TableKind::NodeSpatial | TableKind::WaySpatial => Encoding::DELTA_BINARY_PACKED,
            _ => Encoding::PLAIN,
        };
        b = no_dict(b, &["hilbert"], hilbert_encoding);
    }

    if matches!(
        kind,
        TableKind::NodeById
            | TableKind::NodeSpatial
            | TableKind::WayById
            | TableKind::WaySpatial
            | TableKind::Relation
    ) {
        // Metadata columns common to every table that carries them.
        b = no_dict(b, &["version"], Encoding::DELTA_BINARY_PACKED);
        b = no_dict(b, &["changeset"], Encoding::PLAIN);
        b = no_dict(b, &["timestamp"], Encoding::PLAIN);
        b = no_dict(b, &["uid"], Encoding::PLAIN);
    }

    match kind {
        TableKind::NodeById | TableKind::NodeSpatial => {
            b = no_dict(b, &["lat_e7"], Encoding::DELTA_BINARY_PACKED);
            b = no_dict(b, &["lon_e7"], Encoding::DELTA_BINARY_PACKED);
        }
        TableKind::WayById | TableKind::WaySpatial => {
            for f in ["xmin_e7", "ymin_e7", "xmax_e7", "ymax_e7"] {
                b = no_dict(b, &[f], Encoding::DELTA_BINARY_PACKED);
            }
            // `refs` node ids: measured *worse* under DELTA_BINARY_PACKED
            // on the full Minnesota way tables (a node id has no relation
            // to its position within a way's ref list, so deltas are as
            // large/random as the raw ids -- 26.1-26.2 B/row PLAIN vs
            // 27.2-32.5 B/row DELTA measured on the real byid/spatial way
            // files; matches DuckDB's own PLAIN choice here). This is one
            // of the few places this module disagrees with the M1 tuning
            // brief's suggested column list; see docs/m1-report.md.
            b = no_dict(b, &["refs", "list", "item"], Encoding::PLAIN);
            if kind == TableKind::WaySpatial {
                b = no_dict(b, &["centroid_lat_e7"], Encoding::DELTA_BINARY_PACKED);
                b = no_dict(b, &["centroid_lon_e7"], Encoding::DELTA_BINARY_PACKED);
                // WKB geometry: effectively unique per row, so dictionary
                // encoding is wasted effort; PLAIN measured marginally
                // smaller than the PARQUET_2_0 default BYTE_ARRAY fallback
                // (DELTA_BYTE_ARRAY) on real way geometries.
                b = no_dict(b, &["geometry"], Encoding::PLAIN);
            }
        }
        TableKind::Relation => {
            // Same reasoning as `refs` above: a member ref has no relation
            // to its position in the member list or to the relation's id;
            // PLAIN measured smaller (30.1 vs 41.2 B/row) on the real
            // Minnesota relation table.
            b = no_dict(b, &["members", "list", "item", "ref"], Encoding::PLAIN);
            // members.type / members.role keep dictionary encoding
            // (default): low-cardinality strings ("n"/"w"/"r", common
            // role names).
        }
        TableKind::NodeWay => {
            // way_id has no relationship to the (node_id, way_id) sort
            // order; leave it PLAIN (still no dictionary -- huge
            // cardinality, one-off values).
            b = no_dict(b, &["way_id"], Encoding::PLAIN);
        }
    }

    b
}

pub fn writer_properties(row_group_size: usize, geo_metadata: Option<String>, kind: TableKind) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(ZSTD_LEVEL).expect("zstd level 3 is valid"),
        ))
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(1024 * 1024)
        .set_write_batch_size(row_group_size.max(1))
        .set_max_row_group_size(row_group_size.max(1));
    builder = apply_column_properties(builder, kind);
    if let Some(geo) = geo_metadata {
        builder = builder.set_key_value_metadata(Some(vec![KeyValue::new(
            "geo".to_string(),
            Some(geo),
        )]));
    }
    builder.build()
}

/// GeoParquet-ish `geo` footer metadata for a WKB geometry column, the
/// fallback path from docs/m1-contracts.md section 3 ("otherwise, write a
/// BYTE_ARRAY column named geometry holding WKB and add the GeoParquet geo
/// key-value metadata"). We always take this path: the `arrow`/`parquet`
/// crates in use (56.2.1) know the `LogicalType::Geometry` *Parquet*
/// concept, but the Arrow<->Parquet schema converter that `ArrowWriter`
/// uses has no Arrow `DataType` for it, so there's no way to ask
/// `ArrowWriter` for a native GEOMETRY leaf; verified below (and by the
/// DuckDB check in tests/test_raw_rust.py) that this metadata is
/// sufficient for `DESCRIBE` to report `GEOMETRY`.
pub fn geo_metadata(geometry_types: &[&str]) -> String {
    let types_json = geometry_types
        .iter()
        .map(|t| format!("\"{t}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{{\"geometry\":{{\"encoding\":\"WKB\",\"geometry_types\":[{types_json}]}}}}}}"
    )
}

/// Row-group sizing policy (docs/m1-contracts.md section 3 / M1 tuning
/// brief: "row-group sizing by target bytes, not a fixed row count").
#[derive(Clone, Copy)]
pub enum RowGroupSizing {
    /// A fixed row count, used as-is.
    Fixed(usize),
    /// Resolved once (from the true compressed size of a sample batch,
    /// measured with the same column properties as the real file) to the
    /// row count that lands close to `target_bytes` per row group, clamped
    /// to `[min_rows, max_rows]`.
    AdaptiveBytes {
        target_bytes: usize,
        min_rows: usize,
        max_rows: usize,
    },
}

impl RowGroupSizing {
    /// Resolves to a concrete row-group row count. For `AdaptiveBytes`,
    /// `sample` (typically the first batch written to this table/part) is
    /// compressed on its own with `max_row_group_size` set to its own row
    /// count, so the measurement reflects one real, complete row group's
    /// compressed bytes -- not an in-progress/uncompressed estimate.
    pub fn resolve(self, schema: &Arc<Schema>, kind: TableKind, sample: Option<&RecordBatch>) -> Result<usize> {
        match self {
            RowGroupSizing::Fixed(n) => Ok(n.max(1)),
            RowGroupSizing::AdaptiveBytes {
                target_bytes,
                min_rows,
                max_rows,
            } => {
                let rows = sample.map(|b| b.num_rows()).unwrap_or(0);
                if rows == 0 {
                    return Ok(min_rows.max(1));
                }
                let probe_props = writer_properties(rows, None, kind);
                let mut w = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(probe_props))?;
                w.write(sample.unwrap())?;
                let buf = w.into_inner()?;
                let bytes_per_row = (buf.len() as f64 / rows as f64).max(1.0);
                let target_rows = (target_bytes as f64 / bytes_per_row).round() as usize;
                Ok(target_rows.clamp(min_rows.max(1), max_rows.max(min_rows.max(1))))
            }
        }
    }
}

/// Writes a single Parquet file (one or more row groups) from a sequence of
/// batches -- used for spatial cell files, which are always exactly one
/// `part-0.parquet` per cell regardless of size.
pub struct SingleFileWriter {
    writer: ArrowWriter<File>,
    rows: u64,
}

impl SingleFileWriter {
    /// `sizing` is resolved against `sample` (pass the first batch you are
    /// about to write, or a representative prefix of it -- e.g. for a
    /// spill-sorted cell already fully buffered in memory, a prefix of the
    /// sorted rows) before the file is created, so the real file is opened
    /// with its final, concrete row-group row count from the start.
    pub fn create(
        path: &Path,
        schema: Arc<Schema>,
        kind: TableKind,
        sizing: RowGroupSizing,
        sample: Option<&RecordBatch>,
        geo_metadata: Option<String>,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let row_group_size = sizing.resolve(&schema, kind, sample)?;
        let file = File::create(path)?;
        let props = writer_properties(row_group_size, geo_metadata, kind);
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        Ok(SingleFileWriter { writer, rows: 0 })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows += batch.num_rows() as u64;
        self.writer.write(batch)?;
        Ok(())
    }

    /// Returns (rows, bytes).
    pub fn finish(self, path: &Path) -> Result<(u64, u64)> {
        self.writer.close()?;
        let bytes = std::fs::metadata(path)?.len();
        Ok((self.rows, bytes))
    }
}

/// Writes a sequence of `part-NNNNN.parquet` files, starting a new part once
/// `max_rows_per_part` is reached -- used for byid and index tables.
pub struct PartWriter {
    dir: PathBuf,
    schema: Arc<Schema>,
    kind: TableKind,
    max_rows_per_part: usize,
    sizing: RowGroupSizing,
    part_idx: usize,
    rows_in_part: usize,
    writer: Option<ArrowWriter<File>>,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub parts: Vec<PartInfo>,
    part_min_id: Option<i64>,
    part_max_id: Option<i64>,
}

pub struct PartInfo {
    pub path: PathBuf,
    pub rows: u64,
    pub bytes: u64,
    pub min_id: Option<i64>,
    pub max_id: Option<i64>,
}

impl PartWriter {
    pub fn new(
        dir: &Path,
        schema: Arc<Schema>,
        max_rows_per_part: usize,
        kind: TableKind,
        sizing: RowGroupSizing,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(PartWriter {
            dir: dir.to_path_buf(),
            schema,
            kind,
            max_rows_per_part,
            sizing,
            part_idx: 0,
            rows_in_part: 0,
            writer: None,
            total_rows: 0,
            total_bytes: 0,
            parts: Vec::new(),
            part_min_id: None,
            part_max_id: None,
        })
    }

    fn part_path(&self) -> PathBuf {
        self.dir.join(format!("part-{:05}.parquet", self.part_idx))
    }

    fn open(&mut self, first_batch: Option<&RecordBatch>) -> Result<()> {
        // Resolved fresh for every part (not cached): byte density can
        // drift across a table's id range (e.g. tag/ref density changes
        // over the id-ordered history of a way byid file), so each part
        // gets its own row-group row count from its own first batch.
        let row_group_size = self.sizing.resolve(&self.schema, self.kind, first_batch)?;
        let path = self.part_path();
        let file = File::create(&path)?;
        let props = writer_properties(row_group_size, None, self.kind);
        self.writer = Some(ArrowWriter::try_new(file, self.schema.clone(), Some(props))?);
        Ok(())
    }

    /// `id_range` is the (min, max) id in this batch, if the caller tracks
    /// ids (used for `min_id`/`max_id` in parts.json / manifest).
    pub fn write_batch(&mut self, batch: RecordBatch, id_range: Option<(i64, i64)>) -> Result<()> {
        if self.writer.is_none() {
            self.open(Some(&batch))?;
        }
        if let Some((lo, hi)) = id_range {
            self.part_min_id = Some(self.part_min_id.map_or(lo, |v| v.min(lo)));
            self.part_max_id = Some(self.part_max_id.map_or(hi, |v| v.max(hi)));
        }
        let n = batch.num_rows();
        self.writer.as_mut().unwrap().write(&batch)?;
        self.rows_in_part += n;
        self.total_rows += n as u64;
        if self.rows_in_part >= self.max_rows_per_part {
            self.close_part()?;
        }
        Ok(())
    }

    fn close_part(&mut self) -> Result<()> {
        if let Some(w) = self.writer.take() {
            w.close()?;
            let path = self.part_path();
            let bytes = std::fs::metadata(&path)?.len();
            self.total_bytes += bytes;
            self.parts.push(PartInfo {
                path,
                rows: self.rows_in_part as u64,
                bytes,
                min_id: self.part_min_id.take(),
                max_id: self.part_max_id.take(),
            });
            self.rows_in_part = 0;
            self.part_idx += 1;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<Vec<PartInfo>> {
        self.close_part()?;
        Ok(self.parts)
    }
}
