//! Parallel exact ingest. Ids are carried as exact inline u128 keys (<= 15
//! bytes); any id that does not fit makes `load` return `None` and the caller
//! falls back to the generic path. See docs/ingest.md.
use super::key::{self, IdLookup, Map};
use super::par::{self, SyncPtr};
use super::scan::{self, Chunk, FileInfo, Kind};
use super::{slow, small, ts, tsfast};
use crate::types::*;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Instant;

const WEEK_MS: i64 = 7 * ts::DAY_MS;
const CHUNK: u64 = 1 << 20;
/// Event dedup partitions (by event_id hash) and person partitions.
const P1_BITS: u32 = 10;
const P1: usize = 1 << P1_BITS;
const P2_BITS: u32 = 10;
const P2: usize = 1 << P2_BITS;
const INVALID: u32 = u32::MAX;

/// Event candidate before dedup.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct RecA {
    eid: u128,
    user: u128,
    ingest: i64,
    ts: i64,
    cat: u32,
    /// INVALID if the row fails validation as a kept copy.
    cents: u32,
}

/// Surviving event keyed by person.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct RecB {
    person: u128,
    ts: i64,
    cat: u32,
    cents: u32,
}

/// Valid click keyed by person.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct RecC {
    person: u128,
    cid: u128,
    ts: i64,
    campaign: u32,
}

static PROF: std::sync::LazyLock<u32> = std::sync::LazyLock::new(|| std::env::var("INGEST_PROF").ok().and_then(|v| v.parse().ok()).unwrap_or(0));
/// Append with non-temporal stores (records are whole 32/64-byte aligned units:
/// no read-for-ownership of the destination line). Callers `sfence` before
/// other threads read the data (see `fence`).
#[inline(always)]
fn push_nt<T: Copy>(v: &mut Vec<T>, x: T) {
    const { assert!(std::mem::size_of::<T>() % 32 == 0 && std::mem::align_of::<T>() >= 32) };
    if v.len() == v.capacity() {
        fence();
        v.reserve(v.capacity().max(256));
    }
    unsafe {
        use std::arch::x86_64::*;
        let dst = v.as_mut_ptr().add(v.len()) as *mut __m256i;
        let src = &x as *const T as *const __m256i;
        for i in 0..std::mem::size_of::<T>() / 32 {
            _mm256_stream_si256(dst.add(i), _mm256_loadu_si256(src.add(i)));
        }
        v.set_len(v.len() + 1);
    }
}

#[inline(always)]
fn fence() {
    unsafe { std::arch::x86_64::_mm_sfence() }
}

struct Timer(Instant);
impl Timer {
    fn lap(&mut self, name: &str) {
        let now = Instant::now();
        eprintln!("[ingest] {name:<12} {:.3}s", (now - self.0).as_secs_f64());
        self.0 = now;
    }
}

#[inline(always)]
fn field<'a>(b: &'a [u8], f: (u32, u32)) -> &'a [u8] {
    unsafe { b.get_unchecked(f.0 as usize..f.1 as usize) }
}

/// Exact key for a field already known to be <= 15 bytes. Safe: scan buffers
/// have >= PAD readable bytes after any field.
#[inline(always)]
fn fkey(b: &[u8], f: (u32, u32)) -> u128 {
    unsafe { key::pack_at(b.as_ptr().add(f.0 as usize), (f.1 - f.0) as usize) }
}

#[inline(always)]
fn fts(b: &[u8], f: (u32, u32)) -> Option<i64> {
    unsafe { tsfast::parse(b.as_ptr().add(f.0 as usize), flen(f)) }
}

#[inline(always)]
fn flen(f: (u32, u32)) -> usize {
    (f.1 - f.0) as usize
}

pub fn load(data_dir: &Path, threads: usize) -> Option<Loaded> {
    let mut tm = Timer(Instant::now());
    let (ws, we) = small::read_manifest(data_dir);
    let products = small::read_products(data_dir);
    let (campaigns, campaign_map) = small::read_campaigns(data_dir);
    let unknown = products.unknown;
    let prod = IdLookup::build(products.map.iter().map(|(k, &v)| (k.as_slice(), v)), unknown)?;
    let camp = IdLookup::build(campaign_map.iter().map(|(k, &v)| (k.as_slice(), v)), INVALID)?;

    let mut files: Vec<FileInfo> = Vec::new();
    for (sub, kind) in [("events", Kind::Events), ("clicks", Kind::Clicks), ("links", Kind::Links)] {
        for p in small::list_csv(data_dir, sub) {
            let len = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            files.push(FileInfo { path: p, len, kind });
        }
    }
    let handles: Vec<File> = files.iter().map(|f| File::open(&f.path).expect("open csv")).collect();
    let chunks = scan::plan(&files, CHUNK);
    let link_chunks: Vec<Chunk> = chunks.iter().copied().filter(|c| files[c.file as usize].kind == Kind::Links).collect();
    let mut main_chunks: Vec<Chunk> = chunks.iter().copied().filter(|c| files[c.file as usize].kind != Kind::Links).collect();
    // Bigger-first is not needed: chunks are uniform. Interleave nothing.
    main_chunks.sort_by_key(|c| (c.file, c.start));
    let fallback = AtomicBool::new(false);
    tm.lap("small");

    // ---------------- Links ----------------
    let link_pairs: Vec<Vec<(u128, u128)>> = par::run(
        threads,
        link_chunks.len(),
        |_| (Vec::<(u128, u128)>::new(), Vec::<u8>::new()),
        |(pairs, buf), i| {
            let c = link_chunks[i];
            let (lo, hi) = scan::read_chunk(&handles[c.file as usize], files[c.file as usize].len, c, buf);
            scan::scan::<2, _>(buf, lo, hi, |b, f| {
                let (a, bb) = (f[0], f[1]);
                if flen(a) == 0 || flen(bb) == 0 {
                    return;
                }
                if flen(a) > key::MAX_INLINE || flen(bb) > key::MAX_INLINE {
                    fallback.store(true, Ordering::Relaxed);
                    return;
                }
                pairs.push((fkey(b, a), fkey(b, bb)));
            });
        },
    )
    .into_iter()
    .map(|s| s.0)
    .collect();
    if fallback.load(Ordering::Relaxed) {
        return None;
    }
    let nlinks: usize = link_pairs.iter().map(|v| v.len()).sum();
    tm.lap("links-parse");
    let links = Links::build(&link_pairs, nlinks, threads);
    drop(link_pairs);
    tm.lap("links-uf");

    // ---------------- Events + clicks parse ----------------
    let total_event_bytes: u64 = files.iter().filter(|f| f.kind == Kind::Events).map(|f| f.len).sum();
    let est_rows = (total_event_bytes / 80) as usize;
    let per_sink = (est_rows / threads.max(1) / P1) * 5 / 4 + 16;
    struct ParseState {
        ev: Vec<Vec<RecA>>,
        ck: Vec<Vec<RecC>>,
        buf: Vec<u8>,
        t_read: f64,
        t_scan: f64,
    }
    let states = par::run(
        threads,
        main_chunks.len(),
        |_| ParseState {
            ev: (0..P1).map(|_| Vec::with_capacity(per_sink)).collect(),
            ck: (0..P2).map(|_| Vec::new()).collect(),
            buf: Vec::with_capacity(CHUNK as usize + 8192),
            t_read: 0.0,
            t_scan: 0.0,
        },
        |st, i| {
            if fallback.load(Ordering::Relaxed) {
                return;
            }
            let c = main_chunks[i];
            let fi = &files[c.file as usize];
            let t0 = par::thread_cpu();
            let (lo, hi) = scan::read_chunk(&handles[c.file as usize], fi.len, c, &mut st.buf);
            st.t_read += par::thread_cpu() - t0;
            let t1 = par::thread_cpu();
            let buf = std::mem::take(&mut st.buf);
            let mode = *PROF;
            match fi.kind {
                Kind::Events if mode == 1 => scan::scan::<7, _>(&buf, lo, hi, |_, f| {
                    st.t_read += f[0].0 as f64 * 1e-30;
                }),
                Kind::Events if mode == 2 => scan::scan::<7, _>(&buf, lo, hi, |b, f| {
                    let x = ts::parse_ts(field(b, f[2])).unwrap_or(0) + ts::parse_ts(field(b, f[3])).unwrap_or(0);
                    st.t_read += x as f64 * 1e-30;
                }),
                Kind::Events => scan::scan::<7, _>(&buf, lo, hi, |b, f| {
                    if !parse_event(b, f, ws, we, &prod, &mut st.ev) {
                        fallback.store(true, Ordering::Relaxed);
                    }
                }),
                Kind::Clicks => scan::scan::<4, _>(&buf, lo, hi, |b, f| {
                    if !parse_click(b, f, ws, we, &camp, &links, &mut st.ck) {
                        fallback.store(true, Ordering::Relaxed);
                    }
                }),
                Kind::Links => unreachable!(),
            }
            fence();
            st.t_scan += par::thread_cpu() - t1;
            st.buf = buf;
        },
    );
    if fallback.load(Ordering::Relaxed) {
        return None;
    }
    let tr: f64 = states.iter().map(|s| s.t_read).sum();
    let tsn: f64 = states.iter().map(|s| s.t_scan).sum();
    eprintln!("[ingest] parse cpu: read {tr:.3}s scan+parse {tsn:.3}s");
    let mut ev_sinks: Vec<Vec<Vec<RecA>>> = Vec::with_capacity(threads);
    let mut ck_sinks: Vec<Vec<Vec<RecC>>> = Vec::with_capacity(threads);
    for s in states {
        ev_sinks.push(s.ev);
        ck_sinks.push(s.ck);
    }
    tm.lap("parse");

    // ---------------- Event dedup ----------------
    let max_part = (0..P1).map(|p| ev_sinks.iter().map(|t| t[p].len()).sum::<usize>()).max().unwrap_or(0);
    let tcap = (max_part * 2).next_power_of_two().max(64);
    let ev_sinks_ref = &ev_sinks;
    let links_ref = &links;
    let b_states: Vec<Vec<Vec<RecB>>> = par::run(
        threads,
        P1,
        |_| (vec![0u64; tcap], (0..P2).map(|_| Vec::<RecB>::new()).collect::<Vec<_>>()),
        |(table, out), p| {
            let parts: Vec<&[RecA]> = ev_sinks_ref.iter().map(|t| t[p].as_slice()).collect();
            let n: usize = parts.iter().map(|s| s.len()).sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two();
            let mask = cap - 1;
            let table = &mut table[..cap];
            table.fill(0);
            let mut starts = [0usize; 65];
            debug_assert!(parts.len() <= 64);
            for (i, s) in parts.iter().enumerate() {
                starts[i + 1] = starts[i] + s.len();
            }
            let get = |j: usize| -> &RecA {
                let mut t = 0;
                while starts[t + 1] <= j {
                    t += 1;
                }
                &parts[t][j - starts[t]]
            };
            let mut j = 0usize;
            for s in &parts {
                for r in s.iter() {
                    let h = key::hash(r.eid);
                    let fp = (h >> 32) as u32;
                    let mut slot = (h as usize) & mask;
                    loop {
                        let v = table[slot];
                        if v == 0 {
                            table[slot] = ((fp as u64) << 32) | (j as u64 + 1);
                            break;
                        }
                        if (v >> 32) as u32 == fp {
                            let w = (v as u32 - 1) as usize;
                            let wr = get(w);
                            if wr.eid == r.eid {
                                if r.ingest < wr.ingest {
                                    table[slot] = ((fp as u64) << 32) | (j as u64 + 1);
                                }
                                break;
                            }
                        }
                        slot = (slot + 1) & mask;
                    }
                    j += 1;
                }
            }
            for &v in table.iter() {
                if v == 0 {
                    continue;
                }
                let r = get((v as u32 - 1) as usize);
                if r.cents == INVALID {
                    continue;
                }
                let person = links_ref.canonical(r.user);
                let q = (key::hash(person) >> (64 - P2_BITS)) as usize;
                push_nt(&mut out[q], RecB { person, ts: r.ts, cat: r.cat, cents: r.cents });
            }
            fence();
        },
    )
    .into_iter()
    .map(|s| s.1)
    .collect();
    drop(ev_sinks);
    tm.lap("dedup");

    // ---------------- Person grouping ----------------
    struct Part {
        persons: Vec<u128>,
        events: Vec<Event>,
        ev_off: Vec<u32>,
        clicks: Vec<Click>,
        ck_off: Vec<u32>,
    }
    let b_ref = &b_states;
    let c_ref = &ck_sinks;
    let parts_out: Vec<Vec<(usize, Part)>> = par::run(
        threads,
        P2,
        |_| Vec::new(),
        |res, q| {
            let n: usize = b_ref.iter().map(|t| t[q].len()).sum();
            let mut map = Map::with_capacity(n.max(1));
            let mut persons: Vec<u128> = Vec::new();
            let mut counts: Vec<u32> = Vec::new();
            let mut pid: Vec<u32> = Vec::with_capacity(n);
            for t in b_ref.iter() {
                for r in &t[q] {
                    let next = persons.len() as u32;
                    let id = map.insert(r.person, next);
                    if id == next {
                        persons.push(r.person);
                        counts.push(0);
                    }
                    counts[id as usize] += 1;
                    pid.push(id);
                }
            }
            let np = persons.len();
            let mut ev_off = vec![0u32; np + 1];
            for i in 0..np {
                ev_off[i + 1] = ev_off[i] + counts[i];
            }
            let mut fill: Vec<u32> = ev_off[..np].to_vec();
            let mut events = vec![Event::default(); n];
            let mut j = 0;
            for t in b_ref.iter() {
                for r in &t[q] {
                    let id = pid[j] as usize;
                    events[fill[id] as usize] = Event { ts_ms: r.ts, category: r.cat, cents: r.cents };
                    fill[id] += 1;
                    j += 1;
                }
            }
            for i in 0..np {
                events[ev_off[i] as usize..ev_off[i + 1] as usize].sort_unstable_by_key(|e| e.ts_ms);
            }
            // clicks
            let mut cl: Vec<(u32, i64, u128, u32)> = Vec::new();
            for t in c_ref.iter() {
                for r in &t[q] {
                    if let Some(id) = map.get(r.person) {
                        cl.push((id, r.ts, r.cid, r.campaign));
                    }
                }
            }
            cl.sort_unstable_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
            cl.dedup_by(|a, b| a.0 == b.0 && a.2 == b.2);
            let mut ck_off = vec![0u32; np + 1];
            for c in &cl {
                ck_off[c.0 as usize + 1] += 1;
            }
            for i in 0..np {
                ck_off[i + 1] += ck_off[i];
            }
            let clicks = cl.iter().map(|c| Click { ts_ms: c.1, campaign: c.3, reserved: 0 }).collect();
            res.push((q, Part { persons, events, ev_off, clicks, ck_off }));
        },
    );
    drop(b_states);
    drop(ck_sinks);
    let mut parts: Vec<Option<Part>> = (0..P2).map(|_| None).collect();
    for v in parts_out {
        for (q, p) in v {
            parts[q] = Some(p);
        }
    }
    let parts: Vec<Part> = parts.into_iter().map(|p| p.unwrap()).collect();
    tm.lap("group");

    // ---------------- Assemble ----------------
    let np: usize = parts.iter().map(|p| p.persons.len()).sum();
    let ne: usize = parts.iter().map(|p| p.events.len()).sum();
    let nc: usize = parts.iter().map(|p| p.clicks.len()).sum();
    let mut base = vec![(0usize, 0usize, 0usize); P2 + 1];
    for q in 0..P2 {
        let p = &parts[q];
        base[q + 1] = (base[q].0 + p.persons.len(), base[q].1 + p.events.len(), base[q].2 + p.clicks.len());
    }
    let mut events: Vec<Event> = Vec::with_capacity(ne);
    let mut clicks: Vec<Click> = Vec::with_capacity(nc);
    let mut event_offsets: Vec<u32> = Vec::with_capacity(np + 1);
    let mut click_offsets: Vec<u32> = Vec::with_capacity(np + 1);
    let mut pkeys: Vec<u128> = Vec::with_capacity(np);
    {
        let (ep, cp, eo, co, pk) = (
            SyncPtr(events.as_mut_ptr()),
            SyncPtr(clicks.as_mut_ptr()),
            SyncPtr(event_offsets.as_mut_ptr()),
            SyncPtr(click_offsets.as_mut_ptr()),
            SyncPtr(pkeys.as_mut_ptr()),
        );
        let (parts, base) = (&parts, &base);
        par::run(threads, P2, |_| (), move |_, q| {
            let p = &parts[q];
            let (pb, eb, cb) = base[q];
            unsafe {
                ep.slice(eb, p.events.len()).copy_from_slice(&p.events);
                cp.slice(cb, p.clicks.len()).copy_from_slice(&p.clicks);
                pk.slice(pb, p.persons.len()).copy_from_slice(&p.persons);
                for i in 0..p.persons.len() {
                    eo.write(pb + i, eb as u32 + p.ev_off[i]);
                    co.write(pb + i, cb as u32 + p.ck_off[i]);
                }
            }
        });
        unsafe {
            events.set_len(ne);
            clicks.set_len(nc);
            pkeys.set_len(np);
            event_offsets.set_len(np);
            click_offsets.set_len(np);
        }
        event_offsets.push(ne as u32);
        click_offsets.push(nc as u32);
    }
    drop(parts);
    let mut person_ids = StringTable { bytes: Vec::with_capacity(np * 10), offsets: Vec::with_capacity(np + 1) };
    person_ids.offsets.push(0);
    for &k in &pkeys {
        key::unpack(k, &mut person_ids.bytes);
        person_ids.offsets.push(person_ids.bytes.len() as u64);
    }
    tm.lap("assemble");
    eprintln!("[ingest] links={nlinks} persons={np} events={ne} clicks={nc}");
    Some(Loaded {
        window_start_ms: ws,
        window_end_ms: we,
        person_ids,
        categories: products.categories,
        campaigns,
        events,
        event_offsets,
        clicks,
        click_offsets,
    })
}

/// Returns false if the fast path cannot represent the row exactly.
#[inline(always)]
fn parse_event(
    b: &[u8],
    f: &[(u32, u32); 7],
    ws: i64,
    we: i64,
    prod: &IdLookup,
    sinks: &mut [Vec<RecA>],
) -> bool {
    let le = flen(f[0]);
    if le == 0 {
        return true;
    }
    if le > key::MAX_INLINE {
        return false;
    }
    let eid = fkey(b, f[0]);
    let ingest = fts(b, f[3]).unwrap_or(i64::MIN);
    let mut rec = RecA { eid, user: 0, ingest, ts: 0, cat: 0, cents: INVALID };
    let lu = flen(f[1]);
    if lu > key::MAX_INLINE {
        return false;
    }
    'v: {
        if lu == 0 || flen(f[5]) == 0 {
            break 'v;
        }
        let t = match fts(b, f[2]) {
            Some(t) if t >= ws && t < we => t,
            _ => break 'v,
        };
        let ty = f[4];
        let cents = if flen(ty) == 4 {
            let w = unsafe { std::ptr::read_unaligned(b.as_ptr().add(ty.0 as usize) as *const u32) };
            if w == u32::from_le_bytes(*b"view") || w == u32::from_le_bytes(*b"cart") {
                0
            } else {
                break 'v;
            }
        } else if flen(ty) == 8
            && unsafe { std::ptr::read_unaligned(b.as_ptr().add(ty.0 as usize) as *const u64) } == u64::from_le_bytes(*b"purchase")
        {
            let pr = f[6];
            let lp = flen(pr);
            let v = if (1..=8).contains(&lp) {
                unsafe { tsfast::digits8(b.as_ptr().add(pr.0 as usize), lp) }.map(|v| v as u32)
            } else {
                slow::parse_price(field(b, pr))
            };
            match v {
                Some(c) if (1..=10_000_000).contains(&c) => c,
                _ => break 'v,
            }
        } else {
            break 'v;
        };
        if ingest - t > ts::DAY_MS {
            break 'v;
        }
        let cat = unsafe { prod.get(b.as_ptr().add(f[5].0 as usize), flen(f[5])) };
        rec.user = fkey(b, f[1]);
        rec.ts = t;
        rec.cat = cat;
        rec.cents = cents;
    }
    let p = (key::hash(eid) >> (64 - P1_BITS)) as usize;
    if *PROF == 3 {
        if rec.cents == 7 && rec.ts == 5 { sinks[p].push(rec); }
        return true;
    }
    push_nt(unsafe { sinks.get_unchecked_mut(p) }, rec);
    true
}

#[inline(always)]
fn parse_click(
    b: &[u8],
    f: &[(u32, u32); 4],
    ws: i64,
    we: i64,
    camp: &IdLookup,
    links: &Links,
    sinks: &mut [Vec<RecC>],
) -> bool {
    let lc = flen(f[0]);
    if lc == 0 {
        return true;
    }
    let lu = flen(f[1]);
    if lu == 0 {
        return true;
    }
    let campaign = unsafe { camp.get(b.as_ptr().add(f[2].0 as usize), flen(f[2])) };
    if campaign == INVALID {
        return true;
    }
    let t = match fts(b, f[3]) {
        Some(t) if t >= ws - WEEK_MS && t < we => t,
        _ => return true,
    };
    if lc > key::MAX_INLINE || lu > key::MAX_INLINE {
        return false;
    }
    let person = links.canonical(fkey(b, f[1]));
    let q = (key::hash(person) >> (64 - P2_BITS)) as usize;
    push_nt(unsafe { sinks.get_unchecked_mut(q) }, RecC { person, cid: fkey(b, f[0]), ts: t, campaign });
    true
}

/// Linked-id resolution: id key -> canonical id key.
struct Links {
    map: Map,
    canon: Vec<u128>,
}

impl Links {
    fn build(pairs: &[Vec<(u128, u128)>], n: usize, threads: usize) -> Links {
        let mut map = Map::with_capacity(n * 2);
        let mut keys: Vec<u128> = Vec::with_capacity(n * 2);
        let mut edges: Vec<(u32, u32)> = Vec::with_capacity(n);
        for v in pairs {
            for &(a, b) in v {
                let ia = map.insert(a, keys.len() as u32);
                if ia as usize == keys.len() {
                    keys.push(a);
                }
                let ib = map.insert(b, keys.len() as u32);
                if ib as usize == keys.len() {
                    keys.push(b);
                }
                if ia != ib {
                    edges.push((ia, ib));
                }
            }
        }
        let nn = keys.len();
        let parent: Vec<AtomicU32> = (0..nn as u32).map(AtomicU32::new).collect();
        let edges_ref = &edges;
        let parent_ref = &parent;
        let step = 1 << 16;
        let ntasks = edges.len().div_ceil(step);
        par::run(threads, ntasks, |_| (), |_, t| {
            for &(a, b) in &edges_ref[t * step..((t + 1) * step).min(edges_ref.len())] {
                uf_union(parent_ref, a, b);
            }
        });
        // best (smallest key) node per root
        let best: Vec<AtomicU32> = (0..nn as u32).map(AtomicU32::new).collect();
        let keys_ref = &keys;
        let best_ref = &best;
        let ntasks = nn.div_ceil(step);
        par::run(threads, ntasks, |_| (), |_, t| {
            for i in t * step..((t + 1) * step).min(nn) {
                let r = uf_find(parent_ref, i as u32) as usize;
                let mut cur = best_ref[r].load(Ordering::Relaxed);
                while keys_ref[i] < keys_ref[cur as usize] {
                    match best_ref[r].compare_exchange_weak(cur, i as u32, Ordering::Relaxed, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(c) => cur = c,
                    }
                }
            }
        });
        let mut canon = vec![0u128; nn];
        {
            let cp = SyncPtr(canon.as_mut_ptr());
            par::run(threads, ntasks, |_| (), move |_, t| {
                for i in t * step..((t + 1) * step).min(nn) {
                    let r = uf_find(parent_ref, i as u32) as usize;
                    let b = best_ref[r].load(Ordering::Relaxed) as usize;
                    unsafe { cp.write(i, keys_ref[b]) };
                }
            });
        }
        Links { map, canon }
    }

    #[inline(always)]
    fn canonical(&self, k: u128) -> u128 {
        match self.map.get(k) {
            Some(i) => unsafe { *self.canon.get_unchecked(i as usize) },
            None => k,
        }
    }
}

fn uf_find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = p[x as usize].load(Ordering::Relaxed);
        if px == x {
            return x;
        }
        let gp = p[px as usize].load(Ordering::Relaxed);
        if gp != px {
            // path halving (benign race: only ever points to an ancestor)
            let _ = p[x as usize].compare_exchange_weak(px, gp, Ordering::Relaxed, Ordering::Relaxed);
        }
        x = gp;
    }
}

fn uf_union(p: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = uf_find(p, a);
        b = uf_find(p, b);
        if a == b {
            return;
        }
        // link larger index under smaller (deterministic, acyclic)
        let (hi, lo) = if a > b { (a, b) } else { (b, a) };
        if p[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

/// Single-thread per-row cost breakdown on one events file (INGEST_BENCH2=<file>, DATA_DIR for products).
pub fn bench_rows(path: &Path, data_dir: &Path) {
    let (ws, we) = small::read_manifest(data_dir);
    let products = small::read_products(data_dir);
    let prod = IdLookup::build(products.map.iter().map(|(k, &v)| (k.as_slice(), v)), products.unknown).unwrap();
    let data = std::fs::read(path).unwrap();
    let mut buf = data.clone();
    let hi = buf.len();
    buf.resize(hi + scan::PAD + 1, 0);
    let lo = data.iter().position(|&b| b == b'\n').unwrap() + 1;
    let mb = (hi - lo) as f64 / 1e6;
    let mut sinks: Vec<Vec<RecA>> = (0..P1).map(|_| Vec::new()).collect();
    for round in 0..2 {
        let mut line = String::new();
        let mut rows = 0usize;
        for stage in 0..6 {
            let t = par::thread_cpu();
            let mut acc = 0i64;
            scan::scan::<7, _>(&buf, lo, hi, |b, f| {
                rows += (stage == 0) as usize;
                match stage {
                    0 => acc += f[1].0 as i64,
                    1 => acc += fts(b, f[2]).unwrap_or(0) ^ fts(b, f[3]).unwrap_or(0),
                    2 => acc += fkey(b, f[0]) as i64 ^ fkey(b, f[1]) as i64,
                    3 => acc += unsafe { prod.get(b.as_ptr().add(f[5].0 as usize), flen(f[5])) } as i64,
                    4 => {
                        parse_event(b, f, ws, we, &prod, &mut sinks);
                    }
                    _ => {
                        parse_event(b, f, ws, we, &prod, &mut sinks);
                    }
                }
            });
            if stage == 4 {
                for s in sinks.iter_mut() {
                    s.clear();
                }
            }
            let d = par::thread_cpu() - t;
            line += &format!(" s{stage} {:.1}ns/row ({:.0}MB/s){}", d * 1e9 / rows.max(1) as f64, mb / d, if acc == 1 { "!" } else { "" });
        }
        eprintln!("round {round} rows {rows}:{line}");
    }
}
