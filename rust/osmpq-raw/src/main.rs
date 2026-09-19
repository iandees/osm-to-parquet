mod cells;
mod index;
mod nodes;
mod pbfutil;
mod relations;
mod rows;
mod schema;
mod spill;
mod store;
mod ways;
mod writer;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use std::time::Instant;

const DEFAULT_PROMOTED_KEYS: &[&str] = &[
    "amenity", "shop", "highway", "building", "name", "natural", "landuse", "leisure", "railway",
    "waterway", "place", "tourism",
];

#[derive(Parser)]
#[command(name = "osmpq-raw", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// PBF -> rawdir (docs/m1-contracts.md section 3, passes 1-5).
    Build(BuildArgs),
    /// Optional separate pass: rawdir/way/*.parquet -> rawdir/node_way/.
    NodeWayIndex(NodeWayIndexArgs),
}

#[derive(Args)]
struct BuildArgs {
    input: PathBuf,
    rawdir: PathBuf,
    #[arg(long, default_value_t = 1_000_000)]
    max_nodes_per_cell: u64,
    #[arg(long, default_value_t = 13)]
    max_depth: u32,
    #[arg(long)]
    threads: Option<usize>,
    #[arg(long)]
    promoted_keys: Option<String>,
    #[arg(long, default_value = "auto")]
    node_store: String,
    #[arg(long)]
    flat_nodes: Option<PathBuf>,
    #[arg(long)]
    tmpdir: Option<PathBuf>,
    #[arg(long)]
    bbox: Option<String>,
    #[arg(long, default_value_t = 400_000_000)]
    sorted_mem_max: u64,
}

#[derive(Args)]
struct NodeWayIndexArgs {
    rawdir: PathBuf,
    #[arg(long)]
    threads: Option<usize>,
    #[arg(long)]
    tmpdir: Option<PathBuf>,
}

fn log(msg: &str) {
    eprintln!("[osmpq-raw] {msg}");
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Build(args) => run_build(args),
        Command::NodeWayIndex(args) => run_node_way_index(args),
    }
}

fn run_build(args: BuildArgs) -> Result<()> {
    let t_start = Instant::now();
    std::fs::create_dir_all(&args.rawdir)
        .with_context(|| format!("creating rawdir {}", args.rawdir.display()))?;
    let tmpdir = args.tmpdir.clone().unwrap_or_else(|| args.rawdir.join("_tmp"));
    std::fs::create_dir_all(&tmpdir)?;

    let threads = args.threads.unwrap_or(0);
    if threads > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().ok();
    }

    let promoted_keys: Vec<String> = match &args.promoted_keys {
        Some(s) => s.split(',').map(|k| k.trim().to_string()).filter(|k| !k.is_empty()).collect(),
        None => DEFAULT_PROMOTED_KEYS.iter().map(|s| s.to_string()).collect(),
    };
    let bbox = args.bbox.as_deref().map(pbfutil::parse_bbox).transpose()?;

    let mut timings: Vec<(String, f64)> = Vec::new();

    // ---- pass 1: histogram / leaf selection --------------------------------
    let t0 = Instant::now();
    log("pass 1: node histogram ...");
    let hist = nodes::histogram_pass(&args.input, args.max_depth, bbox)?;
    let leaves = cells::select_leaves(&hist.counts, args.max_nodes_per_cell, args.max_depth);
    log(&format!(
        "pass 1: {} nodes, {} leaves, max_id={} ({:.1}s)",
        hist.node_count,
        leaves.len(),
        hist.max_id,
        t0.elapsed().as_secs_f64()
    ));
    timings.push(("histogram".to_string(), t0.elapsed().as_secs_f64()));

    let leaves_json = serde_json::json!({
        "max_nodes_per_cell": args.max_nodes_per_cell,
        "max_depth": args.max_depth,
        "ancestor_depths": cells::ANCESTOR_DEPTHS,
        "leaves": leaves,
    });
    std::fs::write(args.rawdir.join("leaves.json"), serde_json::to_string_pretty(&leaves_json)?)?;

    let leaf_index = cells::LeafIndex::new(&leaves, args.max_depth);

    // ---- node location store selection -------------------------------------
    let node_store_kind = match args.node_store.as_str() {
        "sorted-mem" => "sorted-mem",
        "dense-file" => "dense-file",
        "auto" => {
            if hist.node_count < args.sorted_mem_max {
                "sorted-mem"
            } else {
                "dense-file"
            }
        }
        other => anyhow::bail!("unknown --node-store {other}"),
    };
    log(&format!("node store: {node_store_kind}"));
    let store_builder = match node_store_kind {
        "sorted-mem" => store::NodeStoreBuilder::sorted_mem(hist.node_count as usize),
        "dense-file" => {
            let path = args.flat_nodes.clone().unwrap_or_else(|| tmpdir.join("nodes.flat"));
            store::NodeStoreBuilder::dense_file(&path, hist.max_id)?
        }
        _ => unreachable!(),
    };

    // ---- pass 2: nodes -------------------------------------------------------
    let t0 = Instant::now();
    log("pass 2: nodes ...");
    let node_result = nodes::node_pass(
        &args.input,
        &args.rawdir,
        &tmpdir,
        &leaf_index,
        &promoted_keys,
        bbox,
        store_builder,
        threads,
    )?;
    log(&format!(
        "pass 2: {} nodes ({} tagged), {:.1}s, {:.0} nodes/s",
        node_result.node_count,
        node_result.tagged_count,
        t0.elapsed().as_secs_f64(),
        node_result.node_count as f64 / t0.elapsed().as_secs_f64().max(1e-9)
    ));
    timings.push(("nodes".to_string(), t0.elapsed().as_secs_f64()));

    // ---- pass 3: ways ----------------------------------------------------------
    let t0 = Instant::now();
    log("pass 3: ways ...");
    let way_result = ways::way_pass(
        &args.input,
        &args.rawdir,
        &tmpdir,
        &leaf_index,
        &node_result.node_store,
        &promoted_keys,
        bbox,
        threads,
    )?;
    log(&format!(
        "pass 3: {} ways, {:.1}s, {:.0} ways/s",
        way_result.way_count,
        t0.elapsed().as_secs_f64(),
        way_result.way_count as f64 / t0.elapsed().as_secs_f64().max(1e-9)
    ));
    timings.push(("ways".to_string(), t0.elapsed().as_secs_f64()));

    // ---- pass 4: relations -------------------------------------------------------
    let t0 = Instant::now();
    log("pass 4: relations ...");
    let relation_result = relations::relation_pass(
        &args.input,
        &args.rawdir,
        &promoted_keys,
        bbox,
        Some(&node_result.node_store),
        way_result.kept_way_ids.as_ref(),
    )?;
    log(&format!(
        "pass 4: {} relations, {:.1}s",
        relation_result.relation_count,
        t0.elapsed().as_secs_f64()
    ));
    timings.push(("relations".to_string(), t0.elapsed().as_secs_f64()));

    // ---- pass 5: summary.json -------------------------------------------------------
    let max_timestamp_us = [
        node_result.max_timestamp_us,
        way_result.max_timestamp_us,
        relation_result.max_timestamp_us,
    ]
    .into_iter()
    .flatten()
    .max();
    let max_timestamp = max_timestamp_us.map(|us| {
        let secs = us.div_euclid(1_000_000);
        let dt = chrono_like_format(secs);
        dt
    });

    let byid_node_bytes: u64 = node_result.byid_parts.iter().map(|p| p.bytes).sum();
    let byid_way_bytes: u64 = way_result.byid_parts.iter().map(|p| p.bytes).sum();
    let byid_relation_bytes: u64 = relation_result.byid_parts.iter().map(|p| p.bytes).sum();

    let summary = serde_json::json!({
        "producer": "osmpq-raw 0.1.0",
        "source": args.input.file_name().map(|n| n.to_string_lossy().to_string()),
        "bbox": bbox.map(|(s,w,n,e)| vec![s,w,n,e]),
        "counts": {
            "nodes": node_result.node_count,
            "tagged_nodes": node_result.tagged_count,
            "ways": way_result.way_count,
            "relations": relation_result.relation_count,
            "leaf_cells": leaves.len(),
        },
        "max_timestamp": max_timestamp,
        "extent": node_result.extent.map(|(s,w,n,e)| vec![s,w,n,e]),
        "promoted_keys": promoted_keys,
        "node_store": node_store_kind,
        "max_nodes_per_cell": args.max_nodes_per_cell,
        "max_depth": args.max_depth,
        "ancestor_depths": cells::ANCESTOR_DEPTHS,
        "timings_seconds": timings.iter().cloned().collect::<std::collections::BTreeMap<_,_>>(),
        "bytes": {
            "byid_node": byid_node_bytes,
            "byid_way": byid_way_bytes,
            "byid_relation": byid_relation_bytes,
            "spatial_node": node_result.spatial_bytes,
            "spatial_way": way_result.spatial_bytes,
        },
        "rows": {
            "byid_node": node_result.byid_parts.iter().map(|p| p.rows).sum::<u64>(),
            "byid_way": way_result.byid_parts.iter().map(|p| p.rows).sum::<u64>(),
            "byid_relation": relation_result.byid_parts.iter().map(|p| p.rows).sum::<u64>(),
            "spatial_node": node_result.spatial_rows,
            "spatial_way": way_result.spatial_rows,
        },
        "total_seconds": t_start.elapsed().as_secs_f64(),
    });
    std::fs::write(args.rawdir.join("summary.json"), serde_json::to_string_pretty(&summary)?)?;
    log(&format!("done in {:.1}s total", t_start.elapsed().as_secs_f64()));
    Ok(())
}

fn run_node_way_index(args: NodeWayIndexArgs) -> Result<()> {
    let tmpdir = args.tmpdir.clone().unwrap_or_else(|| args.rawdir.join("_tmp"));
    std::fs::create_dir_all(&tmpdir)?;
    let t0 = Instant::now();
    let result = index::build_node_way_index(&args.rawdir, &tmpdir, args.threads.unwrap_or(0))?;
    log(&format!(
        "node_way index: {} rows from {} way parts into {} parts, {:.1}s",
        result.rows,
        result.way_parts_read,
        result.parts,
        t0.elapsed().as_secs_f64()
    ));
    Ok(())
}

/// Minimal seconds-since-epoch -> "YYYY-MM-DDTHH:MM:SSZ" formatter (avoids
/// pulling in a chrono dependency for one field).
fn chrono_like_format(epoch_secs: i64) -> String {
    const DAYS_IN_MONTH: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let days_total = epoch_secs.div_euclid(86400);
    let secs_of_day = epoch_secs.rem_euclid(86400);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    let mut year = 1970i64;
    let mut days = days_total;
    loop {
        let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
        let year_len = if leap { 366 } else { 365 };
        if days >= year_len {
            days -= year_len;
            year += 1;
        } else if days < 0 {
            year -= 1;
            let leap2 = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            days += if leap2 { 366 } else { 365 };
        } else {
            break;
        }
    }
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let mut month = 0usize;
    let mut day = days;
    for (i, &dm) in DAYS_IN_MONTH.iter().enumerate() {
        let dm = if i == 1 && leap { 29 } else { dm };
        if day < dm {
            month = i;
            break;
        }
        day -= dm;
    }
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year,
        month + 1,
        day + 1,
        hour,
        minute,
        second
    )
}
