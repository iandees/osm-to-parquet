//! RecordBatch builders for each raw table, filled a row at a time from
//! decoded PBF elements or spill records.

use crate::schema;
use crate::spill::Meta;
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Int32Builder, Int64Builder, ListBuilder,
    MapBuilder, RecordBatch, StringBuilder, StructBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::Schema;
use std::sync::Arc;

/// `tags MAP(VARCHAR,VARCHAR)` + one nullable VARCHAR column per promoted
/// key.
pub struct TagsCols {
    promoted_keys: Vec<String>,
    tags: MapBuilder<StringBuilder, StringBuilder>,
    promoted: Vec<StringBuilder>,
}

impl TagsCols {
    pub fn new(promoted_keys: &[String]) -> Self {
        TagsCols {
            promoted_keys: promoted_keys.to_vec(),
            tags: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
            promoted: promoted_keys.iter().map(|_| StringBuilder::new()).collect(),
        }
    }

    pub fn append(&mut self, tags: &[(String, String)]) {
        if tags.is_empty() {
            self.tags.append(false).unwrap();
        } else {
            for (k, v) in tags {
                self.tags.keys().append_value(k);
                self.tags.values().append_value(v);
            }
            self.tags.append(true).unwrap();
        }
        for (i, key) in self.promoted_keys.iter().enumerate() {
            match tags.iter().find(|(k, _)| k == key) {
                Some((_, v)) => self.promoted[i].append_value(v),
                None => self.promoted[i].append_null(),
            }
        }
    }

    pub fn finish_into(&mut self, cols: &mut Vec<ArrayRef>) {
        cols.push(Arc::new(self.tags.finish()));
        for b in self.promoted.iter_mut() {
            cols.push(Arc::new(b.finish()));
        }
    }
}

/// `version, changeset, timestamp, uid, "user"`.
pub struct MetaCols {
    version: Int32Builder,
    changeset: Int64Builder,
    timestamp: TimestampMicrosecondBuilder,
    uid: Int32Builder,
    user: StringBuilder,
}

impl MetaCols {
    pub fn new() -> Self {
        MetaCols {
            version: Int32Builder::new(),
            changeset: Int64Builder::new(),
            timestamp: TimestampMicrosecondBuilder::new(),
            uid: Int32Builder::new(),
            user: StringBuilder::new(),
        }
    }

    pub fn append(&mut self, meta: &Meta) {
        match meta.version {
            Some(v) => self.version.append_value(v),
            None => self.version.append_null(),
        }
        match meta.changeset {
            Some(v) => self.changeset.append_value(v),
            None => self.changeset.append_null(),
        }
        match meta.timestamp_us {
            Some(v) => self.timestamp.append_value(v),
            None => self.timestamp.append_null(),
        }
        match meta.uid {
            Some(v) => self.uid.append_value(v),
            None => self.uid.append_null(),
        }
        match &meta.user {
            Some(v) => self.user.append_value(v),
            None => self.user.append_null(),
        }
    }

    pub fn finish_into(&mut self, cols: &mut Vec<ArrayRef>) {
        cols.push(Arc::new(self.version.finish()));
        cols.push(Arc::new(self.changeset.finish()));
        cols.push(Arc::new(self.timestamp.finish()));
        cols.push(Arc::new(self.uid.finish()));
        cols.push(Arc::new(self.user.finish()));
    }
}

// ---- node byid --------------------------------------------------------------

pub struct NodeByIdBuilder {
    id: Int64Builder,
    lat_e7: Int32Builder,
    lon_e7: Int32Builder,
    tags: TagsCols,
    meta: MetaCols,
    cell: StringBuilder,
    hilbert: UInt64Builder,
}

impl NodeByIdBuilder {
    pub fn new(promoted_keys: &[String]) -> Self {
        NodeByIdBuilder {
            id: Int64Builder::new(),
            lat_e7: Int32Builder::new(),
            lon_e7: Int32Builder::new(),
            tags: TagsCols::new(promoted_keys),
            meta: MetaCols::new(),
            cell: StringBuilder::new(),
            hilbert: UInt64Builder::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        id: i64,
        lat_e7: i32,
        lon_e7: i32,
        tags: &[(String, String)],
        meta: &Meta,
        cell: &str,
        hilbert: u64,
    ) {
        self.id.append_value(id);
        self.lat_e7.append_value(lat_e7);
        self.lon_e7.append_value(lon_e7);
        self.tags.append(tags);
        self.meta.append(meta);
        self.cell.append_value(cell);
        self.hilbert.append_value(hilbert);
    }

    pub fn len(&self) -> usize {
        self.id.len()
    }

    pub fn finish(mut self, schema: Arc<Schema>) -> RecordBatch {
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.lat_e7.finish()),
            Arc::new(self.lon_e7.finish()),
        ];
        self.tags.finish_into(&mut cols);
        self.meta.finish_into(&mut cols);
        cols.push(Arc::new(self.cell.finish()));
        cols.push(Arc::new(self.hilbert.finish()));
        RecordBatch::try_new(schema, cols).expect("node byid batch")
    }
}

// ---- node spatial -------------------------------------------------------------

pub struct NodeSpatialBuilder {
    id: Int64Builder,
    lat_e7: Int32Builder,
    lon_e7: Int32Builder,
    tags: TagsCols,
    meta: MetaCols,
    hilbert: UInt64Builder,
}

impl NodeSpatialBuilder {
    pub fn new(promoted_keys: &[String]) -> Self {
        NodeSpatialBuilder {
            id: Int64Builder::new(),
            lat_e7: Int32Builder::new(),
            lon_e7: Int32Builder::new(),
            tags: TagsCols::new(promoted_keys),
            meta: MetaCols::new(),
            hilbert: UInt64Builder::new(),
        }
    }

    pub fn append(
        &mut self,
        id: i64,
        lat_e7: i32,
        lon_e7: i32,
        tags: &[(String, String)],
        meta: &Meta,
        hilbert: u64,
    ) {
        self.id.append_value(id);
        self.lat_e7.append_value(lat_e7);
        self.lon_e7.append_value(lon_e7);
        self.tags.append(tags);
        self.meta.append(meta);
        self.hilbert.append_value(hilbert);
    }

    pub fn len(&self) -> usize {
        self.id.len()
    }

    pub fn finish(mut self, schema: Arc<Schema>) -> RecordBatch {
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.lat_e7.finish()),
            Arc::new(self.lon_e7.finish()),
        ];
        self.tags.finish_into(&mut cols);
        self.meta.finish_into(&mut cols);
        cols.push(Arc::new(self.hilbert.finish()));
        RecordBatch::try_new(schema, cols).expect("node spatial batch")
    }
}

// ---- way byid -----------------------------------------------------------------

fn refs_builder() -> ListBuilder<Int64Builder> {
    ListBuilder::new(Int64Builder::new())
}

fn append_refs(b: &mut ListBuilder<Int64Builder>, refs: &[i64]) {
    for r in refs {
        b.values().append_value(*r);
    }
    b.append(true);
}

pub struct WayByIdBuilder {
    id: Int64Builder,
    refs: ListBuilder<Int64Builder>,
    tags: TagsCols,
    meta: MetaCols,
    xmin_e7: Int32Builder,
    ymin_e7: Int32Builder,
    xmax_e7: Int32Builder,
    ymax_e7: Int32Builder,
    is_closed: BooleanBuilder,
    is_area: BooleanBuilder,
    cell: StringBuilder,
    hilbert: UInt64Builder,
}

impl WayByIdBuilder {
    pub fn new(promoted_keys: &[String]) -> Self {
        WayByIdBuilder {
            id: Int64Builder::new(),
            refs: refs_builder(),
            tags: TagsCols::new(promoted_keys),
            meta: MetaCols::new(),
            xmin_e7: Int32Builder::new(),
            ymin_e7: Int32Builder::new(),
            xmax_e7: Int32Builder::new(),
            ymax_e7: Int32Builder::new(),
            is_closed: BooleanBuilder::new(),
            is_area: BooleanBuilder::new(),
            cell: StringBuilder::new(),
            hilbert: UInt64Builder::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        id: i64,
        refs: &[i64],
        tags: &[(String, String)],
        meta: &Meta,
        bbox: (Option<i32>, Option<i32>, Option<i32>, Option<i32>),
        is_closed: bool,
        is_area: bool,
        cell: &str,
        hilbert: u64,
    ) {
        self.id.append_value(id);
        append_refs(&mut self.refs, refs);
        self.tags.append(tags);
        self.meta.append(meta);
        macro_rules! opt_i32 {
            ($b:expr, $v:expr) => {
                match $v {
                    Some(x) => $b.append_value(x),
                    None => $b.append_null(),
                }
            };
        }
        opt_i32!(self.xmin_e7, bbox.0);
        opt_i32!(self.ymin_e7, bbox.1);
        opt_i32!(self.xmax_e7, bbox.2);
        opt_i32!(self.ymax_e7, bbox.3);
        self.is_closed.append_value(is_closed);
        self.is_area.append_value(is_area);
        self.cell.append_value(cell);
        self.hilbert.append_value(hilbert);
    }

    pub fn len(&self) -> usize {
        self.id.len()
    }

    pub fn finish(mut self, schema: Arc<Schema>) -> RecordBatch {
        let mut cols: Vec<ArrayRef> = vec![Arc::new(self.id.finish()), Arc::new(self.refs.finish())];
        self.tags.finish_into(&mut cols);
        self.meta.finish_into(&mut cols);
        cols.push(Arc::new(self.xmin_e7.finish()));
        cols.push(Arc::new(self.ymin_e7.finish()));
        cols.push(Arc::new(self.xmax_e7.finish()));
        cols.push(Arc::new(self.ymax_e7.finish()));
        cols.push(Arc::new(self.is_closed.finish()));
        cols.push(Arc::new(self.is_area.finish()));
        cols.push(Arc::new(self.cell.finish()));
        cols.push(Arc::new(self.hilbert.finish()));
        RecordBatch::try_new(schema, cols).expect("way byid batch")
    }
}

// ---- way spatial ----------------------------------------------------------------

pub struct WaySpatialBuilder {
    id: Int64Builder,
    refs: ListBuilder<Int64Builder>,
    tags: TagsCols,
    meta: MetaCols,
    xmin_e7: Int32Builder,
    ymin_e7: Int32Builder,
    xmax_e7: Int32Builder,
    ymax_e7: Int32Builder,
    geometry: BinaryBuilder,
    is_closed: BooleanBuilder,
    is_area: BooleanBuilder,
    centroid_lat_e7: Int32Builder,
    centroid_lon_e7: Int32Builder,
    cell: StringBuilder,
    hilbert: UInt64Builder,
}

impl WaySpatialBuilder {
    pub fn new(promoted_keys: &[String]) -> Self {
        WaySpatialBuilder {
            id: Int64Builder::new(),
            refs: refs_builder(),
            tags: TagsCols::new(promoted_keys),
            meta: MetaCols::new(),
            xmin_e7: Int32Builder::new(),
            ymin_e7: Int32Builder::new(),
            xmax_e7: Int32Builder::new(),
            ymax_e7: Int32Builder::new(),
            geometry: BinaryBuilder::new(),
            is_closed: BooleanBuilder::new(),
            is_area: BooleanBuilder::new(),
            centroid_lat_e7: Int32Builder::new(),
            centroid_lon_e7: Int32Builder::new(),
            cell: StringBuilder::new(),
            hilbert: UInt64Builder::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        id: i64,
        refs: &[i64],
        tags: &[(String, String)],
        meta: &Meta,
        bbox: (Option<i32>, Option<i32>, Option<i32>, Option<i32>),
        geometry_wkb: Option<&[u8]>,
        is_closed: bool,
        is_area: bool,
        centroid: (Option<i32>, Option<i32>),
        cell: &str,
        hilbert: u64,
    ) {
        self.id.append_value(id);
        append_refs(&mut self.refs, refs);
        self.tags.append(tags);
        self.meta.append(meta);
        macro_rules! opt_i32 {
            ($b:expr, $v:expr) => {
                match $v {
                    Some(x) => $b.append_value(x),
                    None => $b.append_null(),
                }
            };
        }
        opt_i32!(self.xmin_e7, bbox.0);
        opt_i32!(self.ymin_e7, bbox.1);
        opt_i32!(self.xmax_e7, bbox.2);
        opt_i32!(self.ymax_e7, bbox.3);
        match geometry_wkb {
            Some(g) => self.geometry.append_value(g),
            None => self.geometry.append_null(),
        }
        self.is_closed.append_value(is_closed);
        self.is_area.append_value(is_area);
        opt_i32!(self.centroid_lat_e7, centroid.0);
        opt_i32!(self.centroid_lon_e7, centroid.1);
        self.cell.append_value(cell);
        self.hilbert.append_value(hilbert);
    }

    pub fn len(&self) -> usize {
        self.id.len()
    }

    pub fn finish(mut self, schema: Arc<Schema>) -> RecordBatch {
        let mut cols: Vec<ArrayRef> = vec![Arc::new(self.id.finish()), Arc::new(self.refs.finish())];
        self.tags.finish_into(&mut cols);
        self.meta.finish_into(&mut cols);
        cols.push(Arc::new(self.xmin_e7.finish()));
        cols.push(Arc::new(self.ymin_e7.finish()));
        cols.push(Arc::new(self.xmax_e7.finish()));
        cols.push(Arc::new(self.ymax_e7.finish()));
        cols.push(Arc::new(self.geometry.finish()));
        cols.push(Arc::new(self.is_closed.finish()));
        cols.push(Arc::new(self.is_area.finish()));
        cols.push(Arc::new(self.centroid_lat_e7.finish()));
        cols.push(Arc::new(self.centroid_lon_e7.finish()));
        cols.push(Arc::new(self.cell.finish()));
        cols.push(Arc::new(self.hilbert.finish()));
        RecordBatch::try_new(schema, cols).expect("way spatial batch")
    }
}

// ---- relation ---------------------------------------------------------------------

pub struct RelationMember {
    pub mtype: &'static str,
    pub mref: i64,
    pub role: String,
}

pub struct RelationBuilder {
    id: Int64Builder,
    members: ListBuilder<StructBuilder>,
    tags: TagsCols,
    meta: MetaCols,
}

impl RelationBuilder {
    pub fn new(promoted_keys: &[String]) -> Self {
        let struct_builder = StructBuilder::from_fields(schema::member_struct_fields(), 0);
        RelationBuilder {
            id: Int64Builder::new(),
            members: ListBuilder::new(struct_builder),
            tags: TagsCols::new(promoted_keys),
            meta: MetaCols::new(),
        }
    }

    pub fn append(&mut self, id: i64, members: &[RelationMember], tags: &[(String, String)], meta: &Meta) {
        self.id.append_value(id);
        {
            let sb = self.members.values();
            for m in members {
                sb.field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(m.mtype);
                sb.field_builder::<Int64Builder>(1).unwrap().append_value(m.mref);
                sb.field_builder::<StringBuilder>(2)
                    .unwrap()
                    .append_value(&m.role);
                sb.append(true);
            }
        }
        self.members.append(true);
        self.tags.append(tags);
        self.meta.append(meta);
    }

    pub fn len(&self) -> usize {
        self.id.len()
    }

    pub fn finish(mut self, schema: Arc<Schema>) -> RecordBatch {
        let mut cols: Vec<ArrayRef> = vec![Arc::new(self.id.finish()), Arc::new(self.members.finish())];
        self.tags.finish_into(&mut cols);
        self.meta.finish_into(&mut cols);
        RecordBatch::try_new(schema, cols).expect("relation batch")
    }
}

// ---- node_way index -----------------------------------------------------------------

pub struct NodeWayBuilder {
    node_id: Int64Builder,
    way_id: Int64Builder,
}

impl NodeWayBuilder {
    pub fn new() -> Self {
        NodeWayBuilder {
            node_id: Int64Builder::new(),
            way_id: Int64Builder::new(),
        }
    }

    pub fn append(&mut self, node_id: i64, way_id: i64) {
        self.node_id.append_value(node_id);
        self.way_id.append_value(way_id);
    }

    pub fn len(&self) -> usize {
        self.node_id.len()
    }

    pub fn finish(mut self, schema: Arc<Schema>) -> RecordBatch {
        let cols: Vec<ArrayRef> = vec![Arc::new(self.node_id.finish()), Arc::new(self.way_id.finish())];
        RecordBatch::try_new(schema, cols).expect("node_way batch")
    }
}

/// WKB (little-endian) LineString from (lon, lat) points in degrees.
pub fn wkb_linestring(points: &[(f64, f64)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9 + points.len() * 16);
    buf.push(1u8); // little endian
    buf.extend_from_slice(&2u32.to_le_bytes()); // LineString
    buf.extend_from_slice(&(points.len() as u32).to_le_bytes());
    for (lon, lat) in points {
        buf.extend_from_slice(&lon.to_le_bytes());
        buf.extend_from_slice(&lat.to_le_bytes());
    }
    buf
}

/// M0 `is_area` rule (docs/m0-contracts.md section 4).
pub fn compute_is_area(tags: &[(String, String)], is_closed: bool) -> bool {
    if !is_closed {
        return false;
    }
    let get = |k: &str| tags.iter().find(|(tk, _)| tk == k).map(|(_, v)| v.as_str());
    let area_no = get("area") == Some("no");
    if area_no {
        return false;
    }
    let has_highway_or_barrier = get("highway").is_some() || get("barrier").is_some();
    let area_yes = get("area") == Some("yes");
    if has_highway_or_barrier && !area_yes {
        return false;
    }
    true
}
