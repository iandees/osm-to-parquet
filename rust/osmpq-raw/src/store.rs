//! Node location stores (docs/m1-contracts.md section 3.1).

use anyhow::{Context, Result};
use memmap2::{Mmap, MmapMut};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

pub enum NodeStoreKind {
    SortedMem,
    DenseFile,
    SortedFile,
}

/// Record size of the `sorted-file` store: `i64` id + `i32` lat_e7 + `i32`
/// lon_e7, all little-endian.
const SORTED_FILE_RECORD_BYTES: u64 = 16;

/// Builder used during pass 2 (node reading): appends `(id, lat_e7, lon_e7)`
/// for every node, in id order.
pub enum NodeStoreBuilder {
    SortedMem(Vec<(i64, i32, i32)>),
    DenseFile { path: PathBuf, mmap: MmapMut, max_id: i64 },
    /// `mmap` is pre-sized to `node_count * 16` bytes (from the pass-1
    /// histogram); `written` counts records appended so far via `put`, and
    /// is also the write cursor (`put` writes at `written * 16` then
    /// increments). Pass 2 calls `put` exactly once per node, strictly in
    /// ascending id order (single sequential call site in `nodes.rs`'s
    /// `node_pass`), so no sort step is needed -- same assumption
    /// `sorted_mem` already relies on.
    SortedFile { path: PathBuf, mmap: MmapMut, written: u64, capacity: u64 },
}

impl NodeStoreBuilder {
    pub fn sorted_mem(capacity: usize) -> Self {
        NodeStoreBuilder::SortedMem(Vec::with_capacity(capacity))
    }

    /// `max_id` bounds the sparse file size (id*8 + 8 bytes); an unset entry
    /// reads back as `(0, 0)`, one of the two conventions the contract
    /// allows for "missing" (the sparse file's holes read as zero, so no
    /// large upfront initialization write is needed).
    pub fn dense_file(path: &Path, max_id: i64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("creating flat-nodes file {}", path.display()))?;
        let len = ((max_id.max(0) as u64) + 1) * 8;
        file.set_len(len)
            .with_context(|| format!("sizing flat-nodes file to {len} bytes"))?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(NodeStoreBuilder::DenseFile {
            path: path.to_path_buf(),
            mmap,
            max_id,
        })
    }

    /// `node_count` is the exact expected record count from pass 1's
    /// histogram (`hist.node_count`), used to pre-size the backing file at
    /// `node_count * 16` bytes -- disk cost scales with actual node count,
    /// not the id range, unlike `dense_file`.
    pub fn sorted_file(path: &Path, node_count: u64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("creating sorted-file node store {}", path.display()))?;
        let len = node_count * SORTED_FILE_RECORD_BYTES;
        // mmap requires a non-empty file; a zero-node build never calls
        // `put`, so a 1-byte placeholder is fine (finish() truncates to the
        // actual written length regardless).
        file.set_len(len.max(1))
            .with_context(|| format!("sizing sorted-file node store to {len} bytes"))?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(NodeStoreBuilder::SortedFile {
            path: path.to_path_buf(),
            mmap,
            written: 0,
            capacity: node_count,
        })
    }

    #[inline]
    pub fn put(&mut self, id: i64, lat_e7: i32, lon_e7: i32) {
        match self {
            NodeStoreBuilder::SortedMem(v) => v.push((id, lat_e7, lon_e7)),
            NodeStoreBuilder::DenseFile { mmap, max_id, .. } => {
                if id < 0 || id > *max_id {
                    return;
                }
                let off = (id as u64 * 8) as usize;
                mmap[off..off + 4].copy_from_slice(&lat_e7.to_le_bytes());
                mmap[off + 4..off + 8].copy_from_slice(&lon_e7.to_le_bytes());
            }
            NodeStoreBuilder::SortedFile { mmap, written, capacity, .. } => {
                // Defensive only: pass 1 and pass 2 read the identical PBF
                // node stream, so `written` should never exceed `capacity`
                // (the same assumption `sorted_mem`'s `Vec::with_capacity`
                // relies on -- there it would just reallocate; here the
                // file is fixed-size, so guard instead of panicking on a
                // wild write past the mmap's end).
                if *written >= *capacity {
                    return;
                }
                let off = (*written * SORTED_FILE_RECORD_BYTES) as usize;
                mmap[off..off + 8].copy_from_slice(&id.to_le_bytes());
                mmap[off + 8..off + 12].copy_from_slice(&lat_e7.to_le_bytes());
                mmap[off + 12..off + 16].copy_from_slice(&lon_e7.to_le_bytes());
                *written += 1;
            }
        }
    }

    pub fn finish(self) -> Result<NodeStore> {
        match self {
            NodeStoreBuilder::SortedMem(v) => Ok(NodeStore::SortedMem(v)),
            NodeStoreBuilder::DenseFile { path, mmap, .. } => {
                mmap.flush()?;
                drop(mmap);
                let file = OpenOptions::new().read(true).open(&path)?;
                let mmap = unsafe { Mmap::map(&file)? };
                Ok(NodeStore::DenseFile(mmap))
            }
            NodeStoreBuilder::SortedFile { path, mmap, written, .. } => {
                mmap.flush()?;
                drop(mmap);
                // Truncate to the actual written length in case pass 1's
                // histogram count and pass 2's actual node stream ever
                // disagreed (see the guard in `put` above) -- keeps the
                // file's record count exact rather than trailing zero
                // bytes that would decode as a bogus (id=0, 0, 0) record.
                let actual_len = written * SORTED_FILE_RECORD_BYTES;
                let file = OpenOptions::new().read(true).write(true).open(&path)?;
                file.set_len(actual_len.max(1))?;
                let file = OpenOptions::new().read(true).open(&path)?;
                let mmap = unsafe { Mmap::map(&file)? };
                Ok(NodeStore::SortedFile { mmap, len: actual_len })
            }
        }
    }
}

/// Read-only, shared across threads (used in pass 3+ to resolve way/relation
/// node refs).
pub enum NodeStore {
    SortedMem(Vec<(i64, i32, i32)>),
    DenseFile(Mmap),
    /// `len` is the actual record region in bytes (`record_count * 16`);
    /// stored separately from `mmap.len()` because an empty store still
    /// needs a real (1-byte) mmap under the hood (see `finish`'s `.max(1)`).
    SortedFile { mmap: Mmap, len: u64 },
}

impl NodeStore {
    #[inline]
    pub fn get(&self, id: i64) -> Option<(i32, i32)> {
        match self {
            NodeStore::SortedMem(v) => v
                .binary_search_by_key(&id, |&(nid, _, _)| nid)
                .ok()
                .map(|i| (v[i].1, v[i].2)),
            NodeStore::DenseFile(mmap) => {
                if id < 0 {
                    return None;
                }
                let off = (id as u64 * 8) as usize;
                if off + 8 > mmap.len() {
                    return None;
                }
                let lat = i32::from_le_bytes(mmap[off..off + 4].try_into().unwrap());
                let lon = i32::from_le_bytes(mmap[off + 4..off + 8].try_into().unwrap());
                if lat == 0 && lon == 0 {
                    None
                } else {
                    Some((lat, lon))
                }
            }
            NodeStore::SortedFile { mmap, len } => {
                let n = (*len / SORTED_FILE_RECORD_BYTES) as usize;
                let record = |i: usize| -> i64 {
                    let off = i * SORTED_FILE_RECORD_BYTES as usize;
                    i64::from_le_bytes(mmap[off..off + 8].try_into().unwrap())
                };
                let mut lo = 0usize;
                let mut hi = n;
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let mid_id = record(mid);
                    match mid_id.cmp(&id) {
                        std::cmp::Ordering::Less => lo = mid + 1,
                        std::cmp::Ordering::Greater => hi = mid,
                        std::cmp::Ordering::Equal => {
                            let off = mid * SORTED_FILE_RECORD_BYTES as usize;
                            let lat = i32::from_le_bytes(mmap[off + 8..off + 12].try_into().unwrap());
                            let lon = i32::from_le_bytes(mmap[off + 12..off + 16].try_into().unwrap());
                            return Some((lat, lon));
                        }
                    }
                }
                None
            }
        }
    }

    pub fn kind(&self) -> NodeStoreKind {
        match self {
            NodeStore::SortedMem(_) => NodeStoreKind::SortedMem,
            NodeStore::DenseFile(_) => NodeStoreKind::DenseFile,
            NodeStore::SortedFile { .. } => NodeStoreKind::SortedFile,
        }
    }
}

impl std::fmt::Display for NodeStoreKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStoreKind::SortedMem => write!(f, "sorted-mem"),
            NodeStoreKind::DenseFile => write!(f, "dense-file"),
            NodeStoreKind::SortedFile => write!(f, "sorted-file"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A fresh scratch directory under the system temp dir, unique per call.
    fn test_dir(name: &str) -> PathBuf {
        let n = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("osmpq-raw-store-test-{name}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn build_sorted_file(dir: &Path, rows: &[(i64, i32, i32)]) -> NodeStore {
        let path = dir.join("nodes.sorted");
        let mut b = NodeStoreBuilder::sorted_file(&path, rows.len() as u64).unwrap();
        for &(id, lat, lon) in rows {
            b.put(id, lat, lon);
        }
        b.finish().unwrap()
    }

    fn build_sorted_mem(rows: &[(i64, i32, i32)]) -> NodeStore {
        let mut b = NodeStoreBuilder::sorted_mem(rows.len());
        for &(id, lat, lon) in rows {
            b.put(id, lat, lon);
        }
        b.finish().unwrap()
    }

    #[test]
    fn sorted_file_empty_store() {
        let dir = test_dir("empty");
        let store = build_sorted_file(&dir, &[]);
        assert_eq!(store.get(0), None);
        assert_eq!(store.get(42), None);
        assert_eq!(store.get(-1), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sorted_file_single_entry() {
        let dir = test_dir("single");
        let store = build_sorted_file(&dir, &[(5, 10, 20)]);
        assert_eq!(store.get(5), Some((10, 20)));
        assert_eq!(store.get(4), None);
        assert_eq!(store.get(6), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sorted_file_edge_case_ids() {
        let dir = test_dir("edges");
        // id 0, a large id, and non-contiguous ids in ascending order.
        let rows: Vec<(i64, i32, i32)> = vec![
            (0, 1, 2),
            (3, 30, 40),
            (1_000, 100, 200),
            (14_197_504_504, -700_000_000, 1_800_000_000),
        ];
        let store = build_sorted_file(&dir, &rows);
        assert_eq!(store.get(0), Some((1, 2)));
        assert_eq!(store.get(3), Some((30, 40)));
        assert_eq!(store.get(1_000), Some((100, 200)));
        assert_eq!(store.get(14_197_504_504), Some((-700_000_000, 1_800_000_000)));
        // absent ids, including ones between present ones and past the end.
        assert_eq!(store.get(1), None);
        assert_eq!(store.get(2), None);
        assert_eq!(store.get(999), None);
        assert_eq!(store.get(1_001), None);
        assert_eq!(store.get(14_197_504_505), None);
        assert_eq!(store.get(-5), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sorted_file_matches_sorted_mem_across_probe_range() {
        let dir = test_dir("cross-check");
        // Ascending, non-contiguous ids spanning a wide range, as pass 2
        // would produce from a real PBF (strictly increasing, gaps common).
        let rows: Vec<(i64, i32, i32)> = (0..500)
            .map(|i: i64| (i * 7 + 3, (i as i32) * 11 - 1000, (i as i32) * 13 + 500))
            .collect();
        let file_store = build_sorted_file(&dir, &rows);
        let mem_store = build_sorted_mem(&rows);

        // Probe every id in the covered range plus some past both ends --
        // present and absent ids should agree exactly between the two
        // store implementations.
        let max_id = rows.last().unwrap().0;
        for probe in -10..=(max_id + 10) {
            assert_eq!(
                file_store.get(probe),
                mem_store.get(probe),
                "mismatch at probe id {probe}"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sorted_file_kind_display() {
        let dir = test_dir("kind");
        let store = build_sorted_file(&dir, &[(1, 2, 3)]);
        assert!(matches!(store.kind(), NodeStoreKind::SortedFile));
        assert_eq!(store.kind().to_string(), "sorted-file");
        std::fs::remove_dir_all(&dir).ok();
    }
}
