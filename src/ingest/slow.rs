//! Straightforward exact ingest: byte-string hash maps, single-threaded.
//! Used as the generic fallback (any id shape) and as a differential check
//! (`INGEST_SLOW=1`).
use super::{csv, small, ts};
use crate::types::*;
use std::collections::HashMap;
use std::path::Path;

const WEEK_MS: i64 = 7 * ts::DAY_MS;

pub fn load(data_dir: &Path) -> Loaded {
    let (ws, we) = small::read_manifest(data_dir);
    let products = small::read_products(data_dir);
    let (campaigns, campaign_map) = small::read_campaigns(data_dir);

    // Links: union-find over interned ids.
    let mut ids: Vec<Vec<u8>> = Vec::new();
    let mut id_ix: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    let mut intern = |s: &[u8], ids: &mut Vec<Vec<u8>>, parent: &mut Vec<u32>| -> u32 {
        if let Some(&i) = id_ix.get(s) {
            return i;
        }
        let i = ids.len() as u32;
        ids.push(s.to_vec());
        parent.push(i);
        id_ix.insert(s.to_vec(), i);
        i
    };
    for path in small::list_csv(data_dir, "links") {
        let buf = std::fs::read(&path).expect("read links");
        let mut f2 = [&b""[..]; 2];
        for line in csv::data_lines(&buf) {
            if !csv::split(line, &mut f2) || f2[0].is_empty() || f2[1].is_empty() {
                continue;
            }
            let a = intern(f2[0], &mut ids, &mut parent);
            let b = intern(f2[1], &mut ids, &mut parent);
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    // canonical (smallest) id per root
    let mut best: HashMap<u32, u32> = HashMap::new();
    for i in 0..ids.len() as u32 {
        let r = find(&mut parent, i);
        let e = best.entry(r).or_insert(i);
        if ids[i as usize] < ids[*e as usize] {
            *e = i;
        }
    }
    let mut canon: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for i in 0..ids.len() as u32 {
        let r = find(&mut parent, i);
        let c = best[&r];
        canon.insert(ids[i as usize].clone(), ids[c as usize].clone());
    }
    let canonical = |u: &[u8]| -> Vec<u8> { canon.get(u).cloned().unwrap_or_else(|| u.to_vec()) };

    // Events: dedup by event_id keeping min ingest_ts (first on ties).
    struct Cand {
        ingest: i64,
        payload: Option<(Vec<u8>, Event)>,
    }
    let mut ev: HashMap<Vec<u8>, Cand> = HashMap::new();
    for path in small::list_csv(data_dir, "events") {
        let buf = std::fs::read(&path).expect("read events");
        let mut f7 = [&b""[..]; 7];
        for line in csv::data_lines(&buf) {
            if !csv::split(line, &mut f7) || f7[0].is_empty() {
                continue;
            }
            let ingest = ts::parse_ts(f7[3]).unwrap_or(i64::MIN);
            if let Some(c) = ev.get(f7[0]) {
                if c.ingest <= ingest {
                    continue;
                }
            }
            let payload = validate_event(&f7, ingest, ws, we, &products);
            ev.insert(f7[0].to_vec(), Cand { ingest, payload });
        }
    }

    // Clicks: dedup by click_id (keep first), validate.
    let mut ck: HashMap<Vec<u8>, Option<(Vec<u8>, Click)>> = HashMap::new();
    for path in small::list_csv(data_dir, "clicks") {
        let buf = std::fs::read(&path).expect("read clicks");
        let mut f4 = [&b""[..]; 4];
        for line in csv::data_lines(&buf) {
            if !csv::split(line, &mut f4) || f4[0].is_empty() {
                continue;
            }
            if ck.contains_key(f4[0]) {
                continue;
            }
            let v = (|| {
                if f4[1].is_empty() {
                    return None;
                }
                let camp = *campaign_map.get(f4[2])?;
                let t = ts::ts_in(f4[3], ws - WEEK_MS, we)?;
                Some((f4[1].to_vec(), Click { ts_ms: t, campaign: camp, reserved: 0 }))
            })();
            ck.insert(f4[0].to_vec(), v);
        }
    }

    // Group by person.
    let mut person_ix: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut person_ids: Vec<Vec<u8>> = Vec::new();
    let mut pev: Vec<Vec<Event>> = Vec::new();
    for (_, c) in ev {
        if let Some((user, e)) = c.payload {
            let p = canonical(&user);
            let i = *person_ix.entry(p.clone()).or_insert_with(|| {
                person_ids.push(p);
                pev.push(Vec::new());
                (person_ids.len() - 1) as u32
            });
            pev[i as usize].push(e);
        }
    }
    let mut pck: Vec<Vec<(Vec<u8>, Click)>> = vec![Vec::new(); person_ids.len()];
    for (cid, c) in ck {
        if let Some((user, c)) = c {
            if let Some(&i) = person_ix.get(&canonical(&user)) {
                pck[i as usize].push((cid, c));
            }
        }
    }
    let mut out = Loaded {
        window_start_ms: ws,
        window_end_ms: we,
        person_ids: StringTable::new(),
        categories: products.categories.clone(),
        campaigns,
        events: Vec::new(),
        event_offsets: vec![0],
        clicks: Vec::new(),
        click_offsets: vec![0],
    };
    for (i, p) in person_ids.iter().enumerate() {
        out.person_ids.push(p);
        let mut e = std::mem::take(&mut pev[i]);
        e.sort_by_key(|e| e.ts_ms);
        out.events.extend_from_slice(&e);
        out.event_offsets.push(out.events.len() as u32);
        let mut c = std::mem::take(&mut pck[i]);
        c.sort_by(|a, b| (a.1.ts_ms, &a.0).cmp(&(b.1.ts_ms, &b.0)));
        out.clicks.extend(c.into_iter().map(|x| x.1));
        out.click_offsets.push(out.clicks.len() as u32);
    }
    out
}

/// Full per-row event validation (rules "Events" step 3 and 4). Returns the
/// raw user id and event payload if the row would survive as the kept copy.
pub fn validate_event(
    f: &[&[u8]; 7],
    ingest: i64,
    ws: i64,
    we: i64,
    products: &small::Products,
) -> Option<(Vec<u8>, Event)> {
    if f[1].is_empty() || f[5].is_empty() {
        return None;
    }
    let t = ts::ts_in(f[2], ws, we)?;
    let cents = match f[4] {
        b"view" | b"cart" => 0u32,
        b"purchase" => parse_price(f[6])?,
        _ => return None,
    };
    if ingest - t > ts::DAY_MS {
        return None;
    }
    let category = products.map.get(f[5]).copied().unwrap_or(products.unknown);
    Some((f[1].to_vec(), Event { ts_ms: t, category, cents }))
}

/// Digits only, value in 1..=10_000_000 (leading zeros allowed).
#[inline]
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
        if v > 10_000_000 {
            // keep scanning for non-digits? value already too big: invalid either way
            return None;
        }
    }
    if v >= 1 {
        Some(v as u32)
    } else {
        None
    }
}
