//! Per-cell spill-then-sort (docs/m1-contracts.md section 3.2).
//!
//! Deviation from the letter of the contract: it describes node spill
//! records as "fixed-size binary rows" (contrasted with way spills, which
//! it says are variable-length because they carry WKB + tags). We use the
//! same length-prefixed variable-length record format for both nodes and
//! ways, so a node's tags/user string travel with it in the spill file
//! instead of needing a second, position-addressed side file. Memory stays
//! bounded exactly the same way (a leaf holds <= max-nodes-per-cell nodes,
//! buffered through a small `BufWriter` per open spill file); this is
//! purely a simpler on-disk encoding, not a behavior change.

use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub const SPILL_BUF_SIZE: usize = 64 * 1024;

// ---- primitive encode/decode helpers --------------------------------------

pub fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

pub fn get_str(cur: &mut &[u8]) -> String {
    let len = u32::from_le_bytes(cur[0..4].try_into().unwrap()) as usize;
    let s = String::from_utf8_lossy(&cur[4..4 + len]).into_owned();
    *cur = &cur[4 + len..];
    s
}

pub fn put_i32_opt(buf: &mut Vec<u8>, v: Option<i32>) {
    buf.extend_from_slice(&v.unwrap_or(i32::MIN).to_le_bytes());
}

pub fn get_i32_opt(cur: &mut &[u8]) -> Option<i32> {
    let v = i32::from_le_bytes(cur[0..4].try_into().unwrap());
    *cur = &cur[4..];
    if v == i32::MIN {
        None
    } else {
        Some(v)
    }
}

pub fn put_i64_opt(buf: &mut Vec<u8>, v: Option<i64>) {
    buf.extend_from_slice(&v.unwrap_or(i64::MIN).to_le_bytes());
}

pub fn get_i64_opt(cur: &mut &[u8]) -> Option<i64> {
    let v = i64::from_le_bytes(cur[0..8].try_into().unwrap());
    *cur = &cur[8..];
    if v == i64::MIN {
        None
    } else {
        Some(v)
    }
}

pub fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn get_i64(cur: &mut &[u8]) -> i64 {
    let v = i64::from_le_bytes(cur[0..8].try_into().unwrap());
    *cur = &cur[8..];
    v
}

pub fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn get_u64(cur: &mut &[u8]) -> u64 {
    let v = u64::from_le_bytes(cur[0..8].try_into().unwrap());
    *cur = &cur[8..];
    v
}

pub fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

pub fn get_u8(cur: &mut &[u8]) -> u8 {
    let v = cur[0];
    *cur = &cur[1..];
    v
}

pub fn put_bytes_opt(buf: &mut Vec<u8>, v: Option<&[u8]>) {
    match v {
        None => buf.extend_from_slice(&0u32.to_le_bytes()),
        Some(b) => {
            buf.extend_from_slice(&((b.len() as u32) + 1).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

pub fn get_bytes_opt(cur: &mut &[u8]) -> Option<Vec<u8>> {
    let len_plus_1 = u32::from_le_bytes(cur[0..4].try_into().unwrap()) as usize;
    *cur = &cur[4..];
    if len_plus_1 == 0 {
        None
    } else {
        let len = len_plus_1 - 1;
        let v = cur[..len].to_vec();
        *cur = &cur[len..];
        Some(v)
    }
}

/// Common element metadata (docs/m0-contracts.md section 4).
#[derive(Clone, Debug, Default)]
pub struct Meta {
    pub version: Option<i32>,
    pub changeset: Option<i64>,
    pub timestamp_us: Option<i64>,
    pub uid: Option<i32>,
    pub user: Option<String>,
}

fn put_tags(buf: &mut Vec<u8>, tags: &[(String, String)]) {
    buf.extend_from_slice(&(tags.len() as u32).to_le_bytes());
    for (k, v) in tags {
        put_str(buf, k);
        put_str(buf, v);
    }
}

fn get_tags(cur: &mut &[u8]) -> Vec<(String, String)> {
    let n = u32::from_le_bytes(cur[0..4].try_into().unwrap()) as usize;
    *cur = &cur[4..];
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let k = get_str(cur);
        let v = get_str(cur);
        out.push((k, v));
    }
    out
}

// ---- node spill row --------------------------------------------------------

pub struct NodeSpillRow {
    pub id: i64,
    pub lat_e7: i32,
    pub lon_e7: i32,
    pub hilbert: u64,
    pub tags: Vec<(String, String)>,
    pub meta: Meta,
}

impl NodeSpillRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        put_i64(&mut buf, self.id);
        buf.extend_from_slice(&self.lat_e7.to_le_bytes());
        buf.extend_from_slice(&self.lon_e7.to_le_bytes());
        put_u64(&mut buf, self.hilbert);
        put_tags(&mut buf, &self.tags);
        encode_meta_with_flag(&mut buf, &self.meta);
        buf
    }

    pub fn decode(mut cur: &[u8]) -> NodeSpillRow {
        let id = get_i64(&mut cur);
        let lat_e7 = i32::from_le_bytes(cur[0..4].try_into().unwrap());
        cur = &cur[4..];
        let lon_e7 = i32::from_le_bytes(cur[0..4].try_into().unwrap());
        cur = &cur[4..];
        let hilbert = get_u64(&mut cur);
        let tags = get_tags(&mut cur);
        let meta = decode_meta_with_flag(&mut cur);
        NodeSpillRow {
            id,
            lat_e7,
            lon_e7,
            hilbert,
            tags,
            meta,
        }
    }
}

// Meta encode/decode with an explicit "has user" flag byte (Meta::encode
// above writes the user string but not the flag byte -- keep the flag next
// to the string here to keep call sites simple).
fn encode_meta_with_flag(buf: &mut Vec<u8>, meta: &Meta) {
    put_i32_opt(buf, meta.version);
    put_i64_opt(buf, meta.changeset);
    put_i64_opt(buf, meta.timestamp_us);
    put_i32_opt(buf, meta.uid);
    match &meta.user {
        None => {
            put_u8(buf, 0);
            put_str(buf, "");
        }
        Some(u) => {
            put_u8(buf, 1);
            put_str(buf, u);
        }
    }
}

fn decode_meta_with_flag(cur: &mut &[u8]) -> Meta {
    let version = get_i32_opt(cur);
    let changeset = get_i64_opt(cur);
    let timestamp_us = get_i64_opt(cur);
    let uid = get_i32_opt(cur);
    let flag = get_u8(cur);
    let s = get_str(cur);
    let user = if flag == 0 { None } else { Some(s) };
    Meta {
        version,
        changeset,
        timestamp_us,
        uid,
        user,
    }
}

// ---- way spill row ----------------------------------------------------------

pub struct WaySpillRow {
    pub id: i64,
    pub refs: Vec<i64>,
    pub tags: Vec<(String, String)>,
    pub meta: Meta,
    pub xmin_e7: Option<i32>,
    pub ymin_e7: Option<i32>,
    pub xmax_e7: Option<i32>,
    pub ymax_e7: Option<i32>,
    pub geometry_wkb: Option<Vec<u8>>,
    pub is_closed: bool,
    pub is_area: bool,
    pub centroid_lat_e7: Option<i32>,
    pub centroid_lon_e7: Option<i32>,
    pub hilbert: u64,
}

impl WaySpillRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        put_i64(&mut buf, self.id);
        buf.extend_from_slice(&(self.refs.len() as u32).to_le_bytes());
        for r in &self.refs {
            put_i64(&mut buf, *r);
        }
        put_tags(&mut buf, &self.tags);
        encode_meta_with_flag(&mut buf, &self.meta);
        put_i32_opt(&mut buf, self.xmin_e7);
        put_i32_opt(&mut buf, self.ymin_e7);
        put_i32_opt(&mut buf, self.xmax_e7);
        put_i32_opt(&mut buf, self.ymax_e7);
        put_bytes_opt(&mut buf, self.geometry_wkb.as_deref());
        put_u8(&mut buf, self.is_closed as u8);
        put_u8(&mut buf, self.is_area as u8);
        put_i32_opt(&mut buf, self.centroid_lat_e7);
        put_i32_opt(&mut buf, self.centroid_lon_e7);
        put_u64(&mut buf, self.hilbert);
        buf
    }

    pub fn decode(mut cur: &[u8]) -> WaySpillRow {
        let id = get_i64(&mut cur);
        let n_refs = u32::from_le_bytes(cur[0..4].try_into().unwrap()) as usize;
        cur = &cur[4..];
        let mut refs = Vec::with_capacity(n_refs);
        for _ in 0..n_refs {
            refs.push(get_i64(&mut cur));
        }
        let tags = get_tags(&mut cur);
        let meta = decode_meta_with_flag(&mut cur);
        let xmin_e7 = get_i32_opt(&mut cur);
        let ymin_e7 = get_i32_opt(&mut cur);
        let xmax_e7 = get_i32_opt(&mut cur);
        let ymax_e7 = get_i32_opt(&mut cur);
        let geometry_wkb = get_bytes_opt(&mut cur);
        let is_closed = get_u8(&mut cur) != 0;
        let is_area = get_u8(&mut cur) != 0;
        let centroid_lat_e7 = get_i32_opt(&mut cur);
        let centroid_lon_e7 = get_i32_opt(&mut cur);
        let hilbert = get_u64(&mut cur);
        WaySpillRow {
            id,
            refs,
            tags,
            meta,
            xmin_e7,
            ymin_e7,
            xmax_e7,
            ymax_e7,
            geometry_wkb,
            is_closed,
            is_area,
            centroid_lat_e7,
            centroid_lon_e7,
            hilbert,
        }
    }
}

// ---- spill file set (one append-only file per cell) ------------------------

pub struct SpillSet {
    dir: PathBuf,
    writers: HashMap<String, BufWriter<File>>,
}

impl SpillSet {
    pub fn new(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(SpillSet {
            dir: dir.to_path_buf(),
            writers: HashMap::new(),
        })
    }

    pub fn append(&mut self, cell: &str, payload: &[u8]) -> Result<()> {
        let dir = &self.dir;
        let w = self.writers.entry(cell.to_string()).or_insert_with(|| {
            let path = dir.join(format!("{cell}.spill"));
            let f = File::create(&path).expect("create spill file");
            BufWriter::with_capacity(SPILL_BUF_SIZE, f)
        });
        w.write_all(&(payload.len() as u32).to_le_bytes())?;
        w.write_all(payload)?;
        Ok(())
    }

    /// Flush and return every cell's spill file path.
    pub fn finish(self) -> Result<Vec<(String, PathBuf)>> {
        let mut out = Vec::with_capacity(self.writers.len());
        for (cell, mut w) in self.writers {
            w.flush()?;
            out.push((cell.clone(), self.dir.join(format!("{cell}.spill"))));
        }
        out.sort();
        Ok(out)
    }
}

/// Read every length-prefixed record out of a spill file.
pub fn read_records(path: &Path) -> Result<Vec<Vec<u8>>> {
    let f = File::open(path)?;
    let mut r = BufReader::with_capacity(SPILL_BUF_SIZE, f);
    let mut out = Vec::new();
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
        out.push(payload);
    }
    Ok(out)
}
