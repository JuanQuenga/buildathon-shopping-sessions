//! manifest.json, products.csv, campaigns.csv, and directory listing.
use super::csv;
use super::ts;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub fn read_manifest(data_dir: &Path) -> (i64, i64) {
    let buf = std::fs::read(data_dir.join("manifest.json")).expect("read manifest.json");
    let get = |key: &str| -> i64 {
        let k = format!("\"{key}\"");
        let pos = find(&buf, k.as_bytes()).unwrap_or_else(|| panic!("manifest missing {key}"));
        let rest = &buf[pos + k.len()..];
        let colon = rest.iter().position(|&b| b == b':').expect("manifest colon");
        let rest = &rest[colon + 1..];
        let q1 = rest.iter().position(|&b| b == b'"').expect("manifest quote");
        let rest = &rest[q1 + 1..];
        let q2 = rest.iter().position(|&b| b == b'"').expect("manifest quote");
        ts::parse_ts(&rest[..q2]).unwrap_or_else(|| panic!("manifest bad ts {key}"))
    };
    (get("window_start"), get("window_end"))
}

fn find(h: &[u8], n: &[u8]) -> Option<usize> {
    h.windows(n.len()).position(|w| w == n)
}

/// Sorted, unique categories (incl. `unknown`) and product_id -> category index.
pub struct Products {
    pub categories: Vec<Vec<u8>>,
    pub unknown: u32,
    pub map: HashMap<Vec<u8>, u32>,
}

pub fn read_products(data_dir: &Path) -> Products {
    let buf = std::fs::read(data_dir.join("products.csv")).unwrap_or_default();
    let mut rows: Vec<(&[u8], &[u8])> = Vec::new();
    let mut f = [&b""[..]; 2];
    for line in csv::data_lines(&buf) {
        if csv::split(line, &mut f) && !f[0].is_empty() {
            rows.push((f[0], f[1]));
        }
    }
    let mut categories: Vec<Vec<u8>> = rows.iter().map(|r| r.1.to_vec()).collect();
    categories.push(b"unknown".to_vec());
    categories.sort_unstable();
    categories.dedup();
    let idx: HashMap<&[u8], u32> = categories.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u32)).collect();
    let unknown = idx[&b"unknown"[..]];
    let mut map = HashMap::with_capacity(rows.len());
    for (p, c) in rows {
        map.entry(p.to_vec()).or_insert(idx[c]);
    }
    Products { categories, unknown, map }
}

/// Sorted by campaign_id; and campaign_id -> index.
pub fn read_campaigns(data_dir: &Path) -> (Vec<(Vec<u8>, Vec<u8>)>, HashMap<Vec<u8>, u32>) {
    let buf = std::fs::read(data_dir.join("campaigns.csv")).unwrap_or_default();
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut f = [&b""[..]; 2];
    for line in csv::data_lines(&buf) {
        if csv::split(line, &mut f) && !f[0].is_empty() {
            rows.push((f[0].to_vec(), f[1].to_vec()));
        }
    }
    rows.sort();
    rows.dedup_by(|a, b| a.0 == b.0);
    let map = rows.iter().enumerate().map(|(i, r)| (r.0.clone(), i as u32)).collect();
    (rows, map)
}

/// Regular `*.csv` files directly in `data_dir/sub`, sorted by name.
pub fn list_csv(data_dir: &Path, sub: &str) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(data_dir.join(sub)) {
        for e in rd.flatten() {
            let p = e.path();
            let is_csv = p
                .file_name()
                .map(|n| n.as_encoded_bytes().ends_with(b".csv"))
                .unwrap_or(false);
            if is_csv && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}
