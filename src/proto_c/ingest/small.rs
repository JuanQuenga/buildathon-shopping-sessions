//! manifest.json, products.csv, campaigns.csv, and directory listing.
use super::csv;
use super::key::IdLookup;
use super::ts;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub fn read_manifest(data_dir: &Path) -> (i64, i64) {
    let buf = std::fs::read(data_dir.join("manifest.json")).expect("read manifest.json");
    let get = |key: &str| -> i64 {
        let k = format!("\"{key}\"");
        let pos = find(&buf, k.as_bytes()).unwrap_or_else(|| panic!("manifest missing {key}"));
        let rest = &buf[pos + k.len()..];
        let colon = rest
            .iter()
            .position(|&b| b == b':')
            .expect("manifest colon");
        let rest = &rest[colon + 1..];
        let q1 = rest
            .iter()
            .position(|&b| b == b'"')
            .expect("manifest quote");
        let rest = &rest[q1 + 1..];
        let q2 = rest
            .iter()
            .position(|&b| b == b'"')
            .expect("manifest quote");
        ts::parse_ts(&rest[..q2]).unwrap_or_else(|| panic!("manifest bad ts {key}"))
    };
    (get("window_start"), get("window_end"))
}

fn find(h: &[u8], n: &[u8]) -> Option<usize> {
    h.windows(n.len()).position(|w| w == n)
}

pub fn read_products_lookup(data_dir: &Path) -> (Vec<Vec<u8>>, u32, IdLookup) {
    let buf = std::fs::read(data_dir.join("products.csv")).unwrap_or_default();
    let unknown_name = &b"unknown"[..];
    let mut categories = vec![unknown_name];
    let mut indices = HashMap::from([(unknown_name, 0)]);
    let mut rows = Vec::new();
    let mut f = [&b""[..]; 2];
    for line in csv::data_lines(&buf) {
        if csv::split(line, &mut f) && !f[0].is_empty() {
            let category = *indices.entry(f[1]).or_insert_with(|| {
                let i = categories.len();
                categories.push(f[1]);
                i
            });
            rows.push((f[0], category));
        }
    }

    categories.sort_unstable();
    let mut sorted_indices = vec![0; categories.len()];
    for (i, &category) in categories.iter().enumerate() {
        sorted_indices[indices[category]] = i as u32;
    }
    let unknown = sorted_indices[0];
    // Rows stay in file order so IdLookup keeps the first value for each product.
    let lookup = IdLookup::build(
        rows.iter()
            .map(|&(id, category)| (id, sorted_indices[category])),
        unknown,
    );
    (
        categories.into_iter().map(<[u8]>::to_vec).collect(),
        unknown,
        lookup,
    )
}

pub fn read_campaigns_lookup(data_dir: &Path) -> (Vec<(Vec<u8>, Vec<u8>)>, IdLookup) {
    let rows = campaign_rows(data_dir);
    let lookup = IdLookup::build(
        rows.iter()
            .enumerate()
            .map(|(i, r)| (r.0.as_slice(), i as u32)),
        u32::MAX,
    );
    (rows, lookup)
}

fn campaign_rows(data_dir: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
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
    rows
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
            if is_csv
                && e.file_type()
                    .is_ok_and(|ft| ft.is_file() || (ft.is_symlink() && p.is_file()))
            {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}
