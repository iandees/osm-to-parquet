//! Small helpers shared by nodes.rs/ways.rs/relations.rs for pulling
//! metadata and tags out of osmpbf elements.

use crate::spill::Meta;

pub fn dense_meta(info: Option<&osmpbf::DenseNodeInfo>) -> Meta {
    match info {
        None => Meta::default(),
        Some(i) => Meta {
            version: Some(i.version()),
            changeset: Some(i.changeset()),
            timestamp_us: Some(i.milli_timestamp().saturating_mul(1000)),
            uid: Some(i.uid()),
            user: i.user().ok().map(|s| s.to_string()),
        },
    }
}

pub fn info_meta(info: &osmpbf::Info) -> Meta {
    Meta {
        version: info.version(),
        changeset: info.changeset(),
        timestamp_us: info.milli_timestamp().map(|ms| ms.saturating_mul(1000)),
        uid: info.uid(),
        user: info.user().and_then(|r| r.ok()).map(|s| s.to_string()),
    }
}

pub fn tags_owned<'a>(iter: impl Iterator<Item = (&'a str, &'a str)>) -> Vec<(String, String)> {
    iter.map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

pub type BBox = (f64, f64, f64, f64); // (south, west, north, east)

pub fn point_in_bbox(lat_e7: i32, lon_e7: i32, bbox: BBox) -> bool {
    let lat = lat_e7 as f64 / 1e7;
    let lon = lon_e7 as f64 / 1e7;
    let (south, west, north, east) = bbox;
    lat >= south && lat <= north && lon >= west && lon <= east
}

pub fn parse_bbox(s: &str) -> anyhow::Result<BBox> {
    let parts: Vec<f64> = s
        .split(',')
        .map(|p| p.trim().parse::<f64>())
        .collect::<Result<_, _>>()?;
    anyhow::ensure!(parts.len() == 4, "--bbox needs S,W,N,E");
    Ok((parts[0], parts[1], parts[2], parts[3]))
}
