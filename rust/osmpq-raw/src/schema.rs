//! Arrow schemas for the raw tables, per docs/m0-contracts.md section 4 and
//! docs/m1-contracts.md section 3. Column names and order exactly as the M0
//! schemas, plus the M1 `hilbert` column on byid/spatial node/way.

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use std::sync::Arc;

/// Field for the `tags MAP(VARCHAR, VARCHAR)` column, matching exactly what
/// `arrow::array::builder::MapBuilder::new(None, StringBuilder, StringBuilder)`
/// produces (so a `RecordBatch` built with that builder validates against
/// this schema without adjustment).
pub fn tags_field() -> Field {
    let keys_field = Arc::new(Field::new("keys", DataType::Utf8, false));
    let values_field = Arc::new(Field::new("values", DataType::Utf8, true));
    let entries_struct = DataType::Struct(Fields::from(vec![keys_field, values_field]));
    let entries_field = Arc::new(Field::new("entries", entries_struct, false));
    Field::new("tags", DataType::Map(entries_field, false), true)
}

pub fn promoted_fields(promoted_keys: &[String]) -> Vec<Field> {
    promoted_keys
        .iter()
        .map(|k| Field::new(k.as_str(), DataType::Utf8, true))
        .collect()
}

pub fn meta_fields() -> Vec<Field> {
    vec![
        Field::new("version", DataType::Int32, true),
        Field::new("changeset", DataType::Int64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("uid", DataType::Int32, true),
        Field::new("user", DataType::Utf8, true),
    ]
}

pub fn refs_field() -> Field {
    Field::new(
        "refs",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )
}

pub fn member_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("ref", DataType::Int64, false),
        Field::new("role", DataType::Utf8, false),
    ])
}

pub fn members_field() -> Field {
    Field::new(
        "members",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(member_struct_fields()),
            true,
        ))),
        true,
    )
}

/// `rawdir/node/part-NNNNN.parquet` (byid copy).
pub fn node_byid_schema(promoted_keys: &[String]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lat_e7", DataType::Int32, true),
        Field::new("lon_e7", DataType::Int32, true),
        tags_field(),
    ];
    fields.extend(promoted_fields(promoted_keys));
    fields.extend(meta_fields());
    fields.push(Field::new("cell", DataType::Utf8, true));
    fields.push(Field::new("hilbert", DataType::UInt64, true));
    Arc::new(Schema::new(fields))
}

/// `rawdir/spatial/node/cell=<cell>/tagged={true,false}/part-0.parquet`.
pub fn node_spatial_schema(promoted_keys: &[String]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lat_e7", DataType::Int32, true),
        Field::new("lon_e7", DataType::Int32, true),
        tags_field(),
    ];
    fields.extend(promoted_fields(promoted_keys));
    fields.extend(meta_fields());
    fields.push(Field::new("hilbert", DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

/// `rawdir/way/part-NNNNN.parquet` (byid copy, no geometry).
pub fn way_byid_schema(promoted_keys: &[String]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        refs_field(),
        tags_field(),
    ];
    fields.extend(promoted_fields(promoted_keys));
    fields.extend(meta_fields());
    fields.push(Field::new("xmin_e7", DataType::Int32, true));
    fields.push(Field::new("ymin_e7", DataType::Int32, true));
    fields.push(Field::new("xmax_e7", DataType::Int32, true));
    fields.push(Field::new("ymax_e7", DataType::Int32, true));
    fields.push(Field::new("is_closed", DataType::Boolean, true));
    fields.push(Field::new("is_area", DataType::Boolean, true));
    fields.push(Field::new("cell", DataType::Utf8, true));
    fields.push(Field::new("hilbert", DataType::UInt64, true));
    Arc::new(Schema::new(fields))
}

/// `rawdir/spatial/way/cell=<cell>/part-0.parquet`. `geometry` is written as
/// a plain BYTE_ARRAY (Arrow `Binary`) WKB column here; the GeoParquet
/// `geo` footer key-value metadata (added by the writer) is what tells
/// DuckDB to present it as `GEOMETRY` (see writer.rs / README in ways.rs).
pub fn way_spatial_schema(promoted_keys: &[String]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        refs_field(),
        tags_field(),
    ];
    fields.extend(promoted_fields(promoted_keys));
    fields.extend(meta_fields());
    fields.push(Field::new("xmin_e7", DataType::Int32, true));
    fields.push(Field::new("ymin_e7", DataType::Int32, true));
    fields.push(Field::new("xmax_e7", DataType::Int32, true));
    fields.push(Field::new("ymax_e7", DataType::Int32, true));
    fields.push(Field::new("geometry", DataType::Binary, true));
    fields.push(Field::new("is_closed", DataType::Boolean, true));
    fields.push(Field::new("is_area", DataType::Boolean, true));
    fields.push(Field::new("centroid_lat_e7", DataType::Int32, true));
    fields.push(Field::new("centroid_lon_e7", DataType::Int32, true));
    fields.push(Field::new("cell", DataType::Utf8, true));
    fields.push(Field::new("hilbert", DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

/// `rawdir/relation/part-NNNNN.parquet`: id order, no bbox/cell (the Python
/// stage computes those).
pub fn relation_schema(promoted_keys: &[String]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        members_field(),
        tags_field(),
    ];
    fields.extend(promoted_fields(promoted_keys));
    fields.extend(meta_fields());
    Arc::new(Schema::new(fields))
}

/// `rawdir/node_way/part-NNNNN.parquet`.
pub fn node_way_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("node_id", DataType::Int64, false),
        Field::new("way_id", DataType::Int64, false),
    ]))
}
