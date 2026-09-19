//! Pass 4 (docs/m1-contracts.md section 3): relations, id order, no
//! bbox/cell/hilbert (the Python `build --raw` stage computes those).

use crate::pbfutil::{self, BBox};
use crate::rows::{RelationBuilder, RelationMember};
use crate::schema;
use crate::store::NodeStore;
use crate::writer::{PartInfo, PartWriter, RowGroupSizing, TableKind};
use anyhow::Result;
use osmpbf::{Element, ElementReader, RelMemberType};
use std::collections::HashSet;
use std::path::Path;

pub const RELATION_PART_ROWS: usize = 5_000_000;
/// M1 tuning brief: "relations ~= 8k rows" (a fixed row count is fine here
/// -- relations are a small table, not part of the byte-regression numbers
/// this pass was tuned against).
pub const RELATION_ROW_GROUP_ROWS: usize = 8_000;
pub const BATCH_ROWS: usize = 64_000;

pub struct RelationPassResult {
    pub byid_parts: Vec<PartInfo>,
    pub relation_count: u64,
    pub max_timestamp_us: Option<i64>,
}

pub fn relation_pass(
    pbf_path: &Path,
    rawdir: &Path,
    promoted_keys: &[String],
    bbox: Option<BBox>,
    node_store: Option<&NodeStore>,
    kept_way_ids: Option<&HashSet<i64>>,
) -> Result<RelationPassResult> {
    let dir = rawdir.join("relation");
    let schema = schema::relation_schema(promoted_keys);
    let mut writer = PartWriter::new(
        &dir,
        schema.clone(),
        RELATION_PART_ROWS,
        TableKind::Relation,
        RowGroupSizing::Fixed(RELATION_ROW_GROUP_ROWS),
    )?;

    let mut batch = RelationBuilder::new(promoted_keys);
    let mut batch_lo = i64::MAX;
    let mut batch_hi = i64::MIN;
    let mut relation_count: u64 = 0;
    let mut max_timestamp_us: Option<i64> = None;

    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|el| {
        let rel = match &el {
            Element::Relation(r) => r,
            _ => return,
        };
        let id = rel.id();
        let members: Vec<RelationMember> = rel
            .members()
            .map(|m| {
                let mtype = match m.member_type {
                    RelMemberType::Node => "n",
                    RelMemberType::Way => "w",
                    RelMemberType::Relation => "r",
                };
                let role = m.role().unwrap_or("").to_string();
                RelationMember {
                    mtype,
                    mref: m.member_id,
                    role,
                }
            })
            .collect();
        let tags = pbfutil::tags_owned(rel.tags());
        let meta = pbfutil::info_meta(&rel.info());

        if let Some(bb) = bbox {
            let _ = bb;
            let any_kept = members.iter().any(|m| match m.mtype {
                "n" => node_store.map(|s| s.get(m.mref).is_some()).unwrap_or(false),
                "w" => kept_way_ids.map(|s| s.contains(&m.mref)).unwrap_or(false),
                // Member relations: conservatively not treated as
                // contributing (see the README note in main.rs on the
                // simplified --bbox "smart" semantics).
                _ => false,
            });
            if !any_kept {
                return;
            }
        }

        relation_count += 1;
        if let Some(ts) = meta.timestamp_us {
            max_timestamp_us = Some(max_timestamp_us.map_or(ts, |m| m.max(ts)));
        }
        batch.append(id, &members, &tags, &meta);
        batch_lo = batch_lo.min(id);
        batch_hi = batch_hi.max(id);
        if batch.len() >= BATCH_ROWS {
            let full = std::mem::replace(&mut batch, RelationBuilder::new(promoted_keys));
            writer
                .write_batch(full.finish(schema.clone()), Some((batch_lo, batch_hi)))
                .expect("write relation batch");
            batch_lo = i64::MAX;
            batch_hi = i64::MIN;
        }
    })?;

    if batch.len() > 0 {
        writer.write_batch(batch.finish(schema.clone()), Some((batch_lo, batch_hi)))?;
    }
    let byid_parts = writer.finish()?;
    Ok(RelationPassResult {
        byid_parts,
        relation_count,
        max_timestamp_us,
    })
}
