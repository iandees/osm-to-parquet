//! Parquet writer helpers: part-file splitting for byid/index tables, and
//! single-file writing for spatial cell files. ZSTD, dictionary encoding,
//! statistics on, everywhere (docs/m1-contracts.md section 3).

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn writer_properties(row_group_size: usize, geo_metadata: Option<String>) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(row_group_size);
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

/// Writes a single Parquet file (one or more row groups) from a sequence of
/// batches -- used for spatial cell files, which are always exactly one
/// `part-0.parquet` per cell regardless of size.
pub struct SingleFileWriter {
    writer: ArrowWriter<File>,
    rows: u64,
}

impl SingleFileWriter {
    pub fn create(
        path: &Path,
        schema: Arc<Schema>,
        row_group_size: usize,
        geo_metadata: Option<String>,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        let props = writer_properties(row_group_size, geo_metadata);
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
    max_rows_per_part: usize,
    row_group_size: usize,
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
        row_group_size: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(PartWriter {
            dir: dir.to_path_buf(),
            schema,
            max_rows_per_part,
            row_group_size,
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

    fn open(&mut self) -> Result<()> {
        let path = self.part_path();
        let file = File::create(&path)?;
        let props = writer_properties(self.row_group_size, None);
        self.writer = Some(ArrowWriter::try_new(file, self.schema.clone(), Some(props))?);
        Ok(())
    }

    /// `id_range` is the (min, max) id in this batch, if the caller tracks
    /// ids (used for `min_id`/`max_id` in parts.json / manifest).
    pub fn write_batch(&mut self, batch: RecordBatch, id_range: Option<(i64, i64)>) -> Result<()> {
        if self.writer.is_none() {
            self.open()?;
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
