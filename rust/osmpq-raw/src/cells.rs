//! Quadtree cells + Hilbert keys, matching `src/osmpq/layout/cells.py` and
//! `src/osmpq/layout/hilbert.py` bit-for-bit (see docs/m0-contracts.md
//! section 2 and docs/m1-contracts.md section 2).
//!
//! Cell keys are digit strings ("0213", ...), root = "root". Splits produce
//! four children in Bing quadkey order: 0=NW, 1=NE, 2=SW, 3=SE. A point
//! belongs to the cell where `lon < east` and `lat < north` are strict on
//! the high side (half-open), which is exactly what the bit-interleaved
//! "quadkey code" construction below gives you for free.

pub const ROOT: &str = "root";
pub const HILBERT_ORDER: u32 = 20;

/// ancestor_depths from docs/m1-contracts.md section 2.
pub const ANCESTOR_DEPTHS: [u32; 5] = [0, 3, 6, 9, 12];

/// Grid (x, y) coordinates at a given order, matching
/// `osmpq.layout.cells._grid_xy` / `point_to_qk_np` exactly (same floor/clip
/// arithmetic in f64).
#[inline]
pub fn grid_xy(lat_e7: i64, lon_e7: i64, order: u32) -> (u64, u64) {
    let n = (1u64 << order) as f64;
    let lat = lat_e7 as f64 / 1e7;
    let lon = lon_e7 as f64 / 1e7;
    let mut x = ((lon + 180.0) / 360.0 * n).floor() as i64;
    let mut y = ((lat + 90.0) / 180.0 * n).floor() as i64;
    let max_i = (1i64 << order) - 1;
    if x < 0 {
        x = 0;
    } else if x > max_i {
        x = max_i;
    }
    if y < 0 {
        y = 0;
    } else if y > max_i {
        y = max_i;
    }
    (x as u64, y as u64)
}

/// Depth-`order` quadkey code (2*order bits, MSB-first digits), matching
/// `point_to_qk` / `point_to_qk_np`.
#[inline]
pub fn qk_code(lat_e7: i64, lon_e7: i64, order: u32) -> u64 {
    let (x, y) = grid_xy(lat_e7, lon_e7, order);
    let mask = if order == 64 { u64::MAX } else { (1u64 << order) - 1 };
    let inv_y = mask ^ y;
    let mut code: u64 = 0;
    for i in (0..order).rev() {
        let bx = (x >> i) & 1;
        let biy = (inv_y >> i) & 1;
        let digit = (biy << 1) | bx;
        code = (code << 2) | digit;
    }
    code
}

/// Render a depth-`order` code as a cell key string ("" digits => "root").
pub fn code_to_key(code: u64, depth: u32) -> String {
    if depth == 0 {
        return ROOT.to_string();
    }
    let mut s = String::with_capacity(depth as usize);
    for i in (0..depth).rev() {
        let digit = ((code >> (2 * i)) & 0b11) as u8;
        s.push((b'0' + digit) as char);
    }
    s
}

/// The four child codes (in digit order 0,1,2,3) of a code at `depth`,
/// expressed at `depth+1`.
#[inline]
pub fn child_codes(code_at_depth: u64) -> [u64; 4] {
    let base = code_at_depth << 2;
    [base, base | 1, base | 2, base | 3]
}

/// Inclusive [lo, hi] range, at `max_depth` resolution, covered by a cell
/// whose code at `depth` is `value` (depth <= max_depth). Matches
/// `qk_range` generalized to a configurable resolution instead of the fixed
/// order-20 grid (the M1 histogram builds cells directly at `--max-depth`).
#[inline]
pub fn code_range_at(value: u64, depth: u32, max_depth: u32) -> (u64, u64) {
    let shift = 2 * (max_depth - depth);
    let lo = value << shift;
    let span = 1u64 << shift;
    (lo, lo + span - 1)
}

/// Classic Wikipedia `xy2d`: Hilbert curve index of (x, y) at `order`,
/// matching `hilbert_xy2d` exactly.
#[inline]
pub fn hilbert_xy2d(order: u32, mut x: u64, mut y: u64) -> u64 {
    let n: u64 = 1u64 << order;
    let mut d: u64 = 0;
    let mut s: u64 = n / 2;
    while s > 0 {
        let rx: u64 = if (x & s) > 0 { 1 } else { 0 };
        let ry: u64 = if (y & s) > 0 { 1 } else { 0 };
        d += s * s * ((3 * rx) ^ ry);
        if ry == 0 {
            if rx == 1 {
                x = n - 1 - x;
                y = n - 1 - y;
            }
            std::mem::swap(&mut x, &mut y);
        }
        s /= 2;
    }
    d
}

/// Hilbert key for a point given as INT32 e7 coordinates, matching
/// `hilbert_key` (fixed order 20, independent of the leaf max-depth).
#[inline]
pub fn hilbert_key(lat_e7: i32, lon_e7: i32) -> u64 {
    let (x, y) = grid_xy(lat_e7 as i64, lon_e7 as i64, HILBERT_ORDER);
    hilbert_xy2d(HILBERT_ORDER, x, y)
}

/// (south, west, north, east) world bbox.
const WORLD: (f64, f64, f64, f64) = (-90.0, -180.0, 90.0, 180.0);

/// One bisection step of `cur_bbox` toward digit `d` (0=NW,1=NE,2=SW,3=SE),
/// matching `cells_mod.cell_bbox` / `cells_mod.containing_cell` exactly
/// (same float ops, same order, so results match Python bit-for-bit).
#[inline]
fn bisect(cur: (f64, f64, f64, f64), digit: u8) -> (f64, f64, f64, f64) {
    let (south, west, north, east) = cur;
    let mid_lon = (west + east) / 2.0;
    let mid_lat = (south + north) / 2.0;
    match digit {
        0 => (mid_lat, west, north, mid_lon),
        1 => (mid_lat, mid_lon, north, east),
        2 => (south, west, mid_lat, mid_lon),
        3 => (south, mid_lon, mid_lat, east),
        _ => unreachable!(),
    }
}

/// outer fully contains inner (both (south, west, north, east)).
#[inline]
fn bbox_contains(outer: (f64, f64, f64, f64), inner: (f64, f64, f64, f64)) -> bool {
    let (o_south, o_west, o_north, o_east) = outer;
    let (i_south, i_west, i_north, i_east) = inner;
    i_south >= o_south && i_west >= o_west && i_north <= o_north && i_east <= o_east
}

/// A leaf cell set, indexed for fast lookups: leaves partition the whole
/// world into ranges of the depth-`max_depth` code space (same trick as
/// `osmpq.layout.cells.LeafIndex`).
pub struct LeafIndex {
    pub max_depth: u32,
    /// Sorted by `lo`.
    los: Vec<u64>,
    keys_by_lo: Vec<String>,
    depths_by_lo: Vec<u32>,
    leafset: std::collections::HashSet<String>,
}

impl LeafIndex {
    pub fn new(leaves: &[String], max_depth: u32) -> Self {
        let mut ranges: Vec<(u64, u64, String, u32)> = leaves
            .iter()
            .map(|k| {
                let depth = if k == ROOT { 0 } else { k.len() as u32 };
                let value = key_to_value(k);
                let (lo, hi) = code_range_at(value, depth, max_depth);
                (lo, hi, k.clone(), depth)
            })
            .collect();
        ranges.sort_by_key(|r| r.0);
        let los = ranges.iter().map(|r| r.0).collect();
        let keys_by_lo = ranges.iter().map(|r| r.2.clone()).collect();
        let depths_by_lo = ranges.iter().map(|r| r.3).collect();
        let leafset = leaves.iter().cloned().collect();
        LeafIndex {
            max_depth,
            los,
            keys_by_lo,
            depths_by_lo,
            leafset,
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        self.leafset.contains(key)
    }

    pub fn leaves(&self) -> impl Iterator<Item = &str> {
        self.keys_by_lo.iter().map(|s| s.as_str())
    }

    /// The leaf cell (as an index into `keys_by_lo`) containing a
    /// depth-`max_depth` code.
    #[inline]
    fn leaf_idx_for_code(&self, code: u64) -> usize {
        // greatest lo <= code
        match self.los.binary_search(&code) {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        }
    }

    #[inline]
    pub fn leaf_for_point(&self, lat_e7: i32, lon_e7: i32) -> &str {
        let code = qk_code(lat_e7 as i64, lon_e7 as i64, self.max_depth);
        let idx = self.leaf_idx_for_code(code);
        &self.keys_by_lo[idx]
    }

    #[inline]
    pub fn leaf_idx_for_point(&self, lat_e7: i32, lon_e7: i32) -> usize {
        let code = qk_code(lat_e7 as i64, lon_e7 as i64, self.max_depth);
        self.leaf_idx_for_code(code)
    }

    pub fn key_at(&self, idx: usize) -> &str {
        &self.keys_by_lo[idx]
    }

    pub fn depth_at(&self, idx: usize) -> u32 {
        self.depths_by_lo[idx]
    }

    /// Smallest cell (leaf or ancestor, including root) fully containing
    /// `bbox = (south, west, north, east)`, restricted (M1 v2 rule) to leaf
    /// cells and depths in `ANCESTOR_DEPTHS`.
    ///
    /// Descends from the root while exactly one child fully contains the
    /// whole bbox and the current cell is not a leaf; call the result C at
    /// depth d. If C is a leaf, use C. Otherwise use the ancestor of C at
    /// the greatest depth in `ANCESTOR_DEPTHS` that is <= d.
    pub fn containing_cell(&self, bbox: (f64, f64, f64, f64)) -> String {
        let mut key = ROOT.to_string();
        let mut cur_bbox = WORLD;
        let mut depth: u32 = 0;
        loop {
            if self.contains(&key) {
                return key;
            }
            if depth >= self.max_depth {
                return snap_to_ancestor_depth(&key, depth);
            }
            let mut contained_digit: Option<u8> = None;
            let mut n_contained = 0u32;
            let mut contained_bbox = cur_bbox;
            for digit in 0u8..4 {
                let child_bbox = bisect(cur_bbox, digit);
                if bbox_contains(child_bbox, bbox) {
                    contained_digit = Some(digit);
                    contained_bbox = child_bbox;
                    n_contained += 1;
                }
            }
            if n_contained != 1 {
                // Not exactly one child fully contains the bbox: C = key at
                // this depth.
                return snap_to_ancestor_depth(&key, depth);
            }
            let digit = contained_digit.unwrap();
            let digit_char = (digit + b'0') as char;
            if key == ROOT {
                key = digit_char.to_string();
            } else {
                key.push(digit_char);
            }
            cur_bbox = contained_bbox;
            depth += 1;
        }
    }
}

fn snap_to_ancestor_depth(key_at_stop: &str, depth_at_stop: u32) -> String {
    // `key_at_stop` is C (the cell we stopped at, non-leaf, depth
    // `depth_at_stop`). Use the ancestor at the greatest depth in
    // ANCESTOR_DEPTHS that is <= depth_at_stop.
    let target_depth = ANCESTOR_DEPTHS
        .iter()
        .copied()
        .filter(|&d| d <= depth_at_stop)
        .max()
        .unwrap_or(0);
    if target_depth == 0 || key_at_stop == ROOT {
        return ROOT.to_string();
    }
    key_at_stop[..target_depth as usize].to_string()
}

/// Select leaf cells from a dense histogram of depth-`max_depth` codes,
/// exactly as `osmpq.build.builder._select_leaf_cells` does (recursive
/// split-while-over-threshold), generalized to a configurable max depth
/// (the M1 change; M0 always used depth 20).
pub fn select_leaves(counts: &[u32], max_nodes_per_cell: u64, max_depth: u32) -> Vec<String> {
    let mut leaves = Vec::new();
    // cum[i] = sum(counts[0..i]); counts.len() == 4^max_depth
    let mut cum = vec![0u64; counts.len() + 1];
    for (i, c) in counts.iter().enumerate() {
        cum[i + 1] = cum[i] + *c as u64;
    }
    let range_count = |lo: u64, hi: u64| -> u64 { cum[(hi + 1) as usize] - cum[lo as usize] };

    fn recurse(
        value: u64,
        depth: u32,
        max_depth: u32,
        max_nodes: u64,
        range_count: &dyn Fn(u64, u64) -> u64,
        leaves: &mut Vec<String>,
    ) {
        let (lo, hi) = code_range_at(value, depth, max_depth);
        let n = range_count(lo, hi);
        if n <= max_nodes || depth >= max_depth {
            if n > 0 || (depth == 0 && value == 0) {
                leaves.push(code_to_key(value, depth));
            }
            return;
        }
        for cv in child_codes(value) {
            recurse(cv, depth + 1, max_depth, max_nodes, range_count, leaves);
        }
    }

    recurse(0, 0, max_depth, max_nodes_per_cell, &range_count, &mut leaves);
    if leaves.is_empty() {
        leaves.push(ROOT.to_string());
    }
    leaves
}

/// The base-4 digit value of a key (0 for root).
pub fn key_to_value(key: &str) -> u64 {
    if key == ROOT {
        return 0;
    }
    let mut value: u64 = 0;
    for ch in key.chars() {
        value = value * 4 + (ch as u64 - '0' as u64);
    }
    value
}
