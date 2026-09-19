//! Node location stores (docs/m1-contracts.md section 3.1).

use anyhow::{Context, Result};
use memmap2::{Mmap, MmapMut};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

pub enum NodeStoreKind {
    SortedMem,
    DenseFile,
}

/// Builder used during pass 2 (node reading): appends `(id, lat_e7, lon_e7)`
/// for every node, in id order.
pub enum NodeStoreBuilder {
    SortedMem(Vec<(i64, i32, i32)>),
    DenseFile { path: PathBuf, mmap: MmapMut, max_id: i64 },
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
        }
    }
}

/// Read-only, shared across threads (used in pass 3+ to resolve way/relation
/// node refs).
pub enum NodeStore {
    SortedMem(Vec<(i64, i32, i32)>),
    DenseFile(Mmap),
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
        }
    }

    pub fn kind(&self) -> NodeStoreKind {
        match self {
            NodeStore::SortedMem(_) => NodeStoreKind::SortedMem,
            NodeStore::DenseFile(_) => NodeStoreKind::DenseFile,
        }
    }
}

impl std::fmt::Display for NodeStoreKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStoreKind::SortedMem => write!(f, "sorted-mem"),
            NodeStoreKind::DenseFile => write!(f, "dense-file"),
        }
    }
}
