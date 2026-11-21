//! Parallel exact ingest, generic over the id key width (see keys.rs).
//! `load` tries compact u64 keys, then wide u128 keys, and returns `None` (the
//! caller then uses the generic slow path) if neither can carry the input exactly.
//! Pipeline (docs/ingest.md):
//!   links  : parse -> partitioned interning -> parallel union-find -> canonical maps
//!   parse  : chunked pread + SIMD scan; events -> P1 sinks by hash(event_id);
//!            valid clicks -> P2 sinks by hash(canonical person)
//!   dedup  : per P1 partition, min ingest per event_id; valid winners -> P2 sinks
//!   group  : per P2 partition, person map, CSR, sort by ts; written in place
use super::key::IdLookup;
use super::keys::{Key, Map};
use super::par::{self, Sinks, SyncPtr};
use super::scan::{self, Chunk, FileInfo, Kind};
use super::{slow, small, ts, tsfast};
use crate::types::*;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::time::Instant;

const WEEK_MS: i64 = 7 * ts::DAY_MS;
const CHUNK: u64 = 1 << 20;
const P1_BITS: u32 = 11;
const P1: usize = 1 << P1_BITS;
const P2_BITS: u32 = 10;
const P2: usize = 1 << P2_BITS;
const LP_BITS: u32 = 8;
const LP: usize = 1 << LP_BITS;
const INVALID: u32 = u32::MAX;
const MAX_THREADS: usize = 32;

/// Why the fast path gave up.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Need {
    Wide = 1,
    Slow = 2,
}

struct Flag(AtomicU8);
impl Flag {
    fn set(&self, n: Need) {
        self.0.fetch_max(n as u8, Ordering::Relaxed);
    }
    fn get(&self) -> Option<Need> {
        match self.0.load(Ordering::Relaxed) {
            0 => None,
            1 => Some(Need::Wide),
            _ => Some(Need::Slow),
        }
    }
}

/// Event candidate before dedup.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct RecA<K: Key> {
    eid: K,
    user: K,
    /// ms since window_start (only meaningful when valid)
    ts: u32,
    /// ingest seconds since epoch + 1 (0 = unparseable)
    ingest: u32,
    cat: u32,
    /// INVALID if the row fails validation as a kept copy
    cents: u32,
}

/// Surviving event keyed by person.
#[derive(Clone, Copy)]
#[repr(C, align(16))]
struct RecB<K: Key> {
    person: K,
    ts: u32,
    /// category << 24 | cents
    cc: u32,
}

/// Valid click keyed by person.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct RecC<K: Key> {
    person: K,
    cid: K,
    /// ms since window_start - 7 days
    ts: u32,
    campaign: u32,
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
#[inline(always)]
fn fptr(b: &[u8], f: (u32, u32)) -> *const u8 {
    unsafe { b.as_ptr().add(f.0 as usize) }
}
#[inline(always)]
fn flen(f: (u32, u32)) -> usize {
    (f.1 - f.0) as usize
}
#[inline(always)]
fn fts(b: &[u8], f: (u32, u32)) -> Option<i64> {
    unsafe { tsfast::parse(fptr(b, f), flen(f)) }
}
#[inline(always)]
fn fkey<K: Key>(b: &[u8], f: (u32, u32)) -> Option<K> {
    unsafe { K::enc(fptr(b, f), flen(f)) }
}

/// Everything independent of the key width.
struct Ctx {
    ws: i64,
    we: i64,
    categories: Vec<Vec<u8>>,
    campaigns: Vec<(Vec<u8>, Vec<u8>)>,
    prod: IdLookup,
    camp: IdLookup,
    files: Vec<FileInfo>,
    handles: Vec<File>,
    link_chunks: Vec<Chunk>,
    main_chunks: Vec<Chunk>,
    event_bytes: u64,
    click_bytes: u64,
}

pub fn load(data_dir: &Path, threads: usize) -> Option<Loaded> {
    let threads = threads.clamp(1, MAX_THREADS);
    let mut tm = Timer(Instant::now());
    let (ws, we) = small::read_manifest(data_dir);
    let products = small::read_products(data_dir);
    let (campaigns, campaign_map) = small::read_campaigns(data_dir);
    // Representation limits of the compact records (else: generic path).
    if we < ws || we - ws > u32::MAX as i64 - WEEK_MS || products.categories.len() > 256 {
        return None;
    }
    let prod = IdLookup::build(products.map.iter().map(|(k, &v)| (k.as_slice(), v)), products.unknown)?;
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
    let link_chunks = chunks.iter().copied().filter(|c| files[c.file as usize].kind == Kind::Links).collect();
    // Largest files first is irrelevant (uniform chunks); keep file order.
    let main_chunks = chunks.iter().copied().filter(|c| files[c.file as usize].kind != Kind::Links).collect();
    let event_bytes = files.iter().filter(|f| f.kind == Kind::Events).map(|f| f.len).sum();
    let click_bytes = files.iter().filter(|f| f.kind == Kind::Clicks).map(|f| f.len).sum();
    let ctx = Ctx {
        ws,
        we,
        categories: products.categories,
        campaigns,
        prod,
        camp,
        files,
        handles,
        link_chunks,
        main_chunks,
        event_bytes,
        click_bytes,
    };
    tm.lap("small");
    match run::<u64>(&ctx, threads, &mut tm) {
        Ok(l) => Some(l),
        Err(Need::Wide) => {
            eprintln!("[ingest] compact keys not applicable: retrying with wide keys");
            run::<u128>(&ctx, threads, &mut tm).ok()
        }
        Err(Need::Slow) => None,
    }
}

fn run<K: Key>(ctx: &Ctx, threads: usize, tm: &mut Timer) -> Result<Loaded, Need> {
    let flag = Flag(AtomicU8::new(0));
    let (ws, we) = (ctx.ws, ctx.we);

    // ---------------- Links ----------------
    let link_pairs: Vec<Vec<(K, K)>> = par::run(
        threads,
        ctx.link_chunks.len(),
        |_| (Vec::<(K, K)>::new(), Vec::<u8>::new()),
        |(pairs, buf), i| {
            let c = ctx.link_chunks[i];
            let (lo, hi) = scan::read_chunk(&ctx.handles[c.file as usize], ctx.files[c.file as usize].len, c, buf);
            scan::scan::<2, _>(buf, lo, hi, |b, f| {
                if flen(f[0]) == 0 || flen(f[1]) == 0 {
                    return;
                }
                match (fkey::<K>(b, f[0]), fkey::<K>(b, f[1])) {
                    (Some(a), Some(c)) => pairs.push((a, c)),
                    _ => flag.set(Need::Wide),
                }
            });
        },
    )
    .into_iter()
    .map(|s| s.0)
    .collect();
    if let Some(n) = flag.get() {
        return Err(n);
    }
    tm.lap("links-parse");
    let links = Links::build(&link_pairs, threads);
    drop(link_pairs);
    tm.lap("links-uf");

    // ---------------- Events + clicks parse ----------------
    let est_ev = (ctx.event_bytes / 75) as usize + 1024;
    let est_ck = (ctx.click_bytes / 40) as usize + 1024;
    let cap_a = est_ev * 3 / 2 / threads / P1 + 16;
    let cap_c = est_ck * 3 / 2 / threads / P2 + 16;
    struct ParseState<K: Key> {
        ev: Sinks<RecA<K>>,
        ck: Sinks<RecC<K>>,
        buf: Vec<u8>,
    }
    let states: Vec<ParseState<K>> = par::run(
        threads,
        ctx.main_chunks.len(),
        |_| ParseState { ev: Sinks::new(P1, cap_a), ck: Sinks::new(P2, cap_c), buf: Vec::with_capacity(CHUNK as usize + 8192) },
        |st, i| {
            if flag.get().is_some() {
                return;
            }
            let c = ctx.main_chunks[i];
            let fi = &ctx.files[c.file as usize];
            let (lo, hi) = scan::read_chunk(&ctx.handles[c.file as usize], fi.len, c, &mut st.buf);
            let buf = std::mem::take(&mut st.buf);
            match fi.kind {
                Kind::Events => scan::scan::<7, _>(&buf, lo, hi, |b, f| {
                    if let Err(n) = parse_event(b, f, ws, we, &ctx.prod, &mut st.ev) {
                        flag.set(n);
                    }
                }),
                Kind::Clicks => scan::scan::<4, _>(&buf, lo, hi, |b, f| {
                    if let Err(n) = parse_click(b, f, ws, we, &ctx.camp, &links, &mut st.ck) {
                        flag.set(n);
                    }
                }),
                Kind::Links => unreachable!(),
            }
            st.ev.fence();
            st.buf = buf;
        },
    );
    if let Some(n) = flag.get() {
        return Err(n);
    }
    let mut ev_sinks: Vec<Sinks<RecA<K>>> = Vec::with_capacity(threads);
    let mut ck_sinks: Vec<Sinks<RecC<K>>> = Vec::with_capacity(threads);
    for s in states {
        ev_sinks.push(s.ev);
        ck_sinks.push(s.ck);
    }
    tm.lap("parse");

    // ---------------- Event dedup ----------------
    let ne_raw: usize = (0..P1).map(|p| ev_sinks.iter().map(|s| s.count(p)).sum::<usize>()).sum();
    let cap_b = ne_raw * 5 / 4 / threads / P2 + 64;
    let b_sinks: Vec<Sinks<RecB<K>>> = par::run(
        threads,
        P1,
        |_| (Vec::<u64>::new(), Sinks::<RecB<K>>::new(P2, cap_b)),
        |(table, out), p| {
            // Record j is addressed as (thread << 27 | index) across the thread-local
            // arena slice `a` (index < cap) and overflow slice `o` (index >= cap).
            let parts: Vec<(&[RecA<K>], &[RecA<K>])> = ev_sinks.iter().map(|s| s.part(p)).collect();
            let n: usize = parts.iter().map(|(a, o)| a.len() + o.len()).sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two();
            if table.len() < cap {
                table.resize(cap, 0);
            }
            let mask = cap - 1;
            let table = &mut table[..cap];
            table.fill(0);
            let get = |j: u32| -> &RecA<K> {
                let (a, o) = parts[(j >> 27) as usize];
                let i = (j & ((1 << 27) - 1)) as usize;
                if i < a.len() {
                    unsafe { a.get_unchecked(i) }
                } else {
                    unsafe { o.get_unchecked(i - a.len()) }
                }
            };
            for (t, (a, o)) in parts.iter().enumerate() {
                for (i, r) in a.iter().chain(o.iter()).enumerate() {
                    let j = ((t as u32) << 27) | i as u32;
                    let h = r.eid.h();
                    let fp = (h >> 32) as u32;
                    let mut slot = (h as usize) & mask;
                    loop {
                        let v = unsafe { *table.get_unchecked(slot) };
                        if v == 0 {
                            unsafe { *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (j as u64 + 1) };
                            break;
                        }
                        if (v >> 32) as u32 == fp {
                            let w = get((v as u32).wrapping_sub(1));
                            if w.eid == r.eid {
                                if r.ingest < w.ingest {
                                    unsafe { *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (j as u64 + 1) };
                                }
                                break;
                            }
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
            // Winners: resolve person with batched prefetch of the link maps.
            const B: usize = 32;
            let mut batch = [0u32; B];
            let mut nb = 0;
            let flush = |batch: &[u32], out: &mut Sinks<RecB<K>>| {
                for &j in batch {
                    let r = get(j);
                    let person = links.canonical(r.user);
                    let q = (person.h() >> (64 - P2_BITS)) as usize;
                    out.push(q, RecB { person, ts: r.ts, cc: (r.cat << 24) | r.cents });
                }
            };
            for &v in table.iter() {
                if v == 0 {
                    continue;
                }
                let j = (v as u32).wrapping_sub(1);
                let r = get(j);
                if r.cents == INVALID {
                    continue;
                }
                links.prefetch(r.user);
                batch[nb] = j;
                nb += 1;
                if nb == B {
                    flush(&batch, out);
                    nb = 0;
                }
            }
            flush(&batch[..nb], out);
            out.fence();
        },
    )
    .into_iter()
    .map(|s| s.1)
    .collect();
    drop(ev_sinks);
    tm.lap("dedup");

    // ---------------- Person grouping (written in place) ----------------
    let mut ev_base = vec![0usize; P2 + 1];
    for q in 0..P2 {
        ev_base[q + 1] = ev_base[q] + b_sinks.iter().map(|s| s.count(q)).sum::<usize>();
    }
    let ne = ev_base[P2];
    let mut events: Vec<Event> = Vec::with_capacity(ne);
    struct Part<K> {
        persons: Vec<K>,
        ev_cnt: Vec<u32>,
        clicks: Vec<Click>,
        ck_cnt: Vec<u32>,
    }
    let evp = SyncPtr(events.as_mut_ptr());
    let ck_base_ms = ws - WEEK_MS;
    let parts_out: Vec<Vec<(usize, Part<K>)>> = par::run(
        threads,
        P2,
        |_| (Vec::new(), Map::<K, u32>::with_capacity(1024), Vec::<u32>::new(), Vec::<(u32, u32, K, u32)>::new()),
        |(res, map, pid, cl), q| {
            let n = ev_base[q + 1] - ev_base[q];
            map.reset(n / 8 + 16);
            pid.clear();
            let mut persons: Vec<K> = Vec::new();
            let mut counts: Vec<u32> = Vec::new();
            for s in b_sinks.iter() {
                let (a, o) = s.part(q);
                for r in a.iter().chain(o.iter()) {
                    let next = persons.len() as u32;
                    let (id, ins) = map.insert(r.person, next);
                    if ins {
                        persons.push(r.person);
                        counts.push(0);
                        if map.len() * 2 > map_cap(map) {
                            grow(map, &persons);
                        }
                    }
                    counts[id as usize] += 1;
                    pid.push(id);
                }
            }
            let np = persons.len();
            let mut fill = vec![0u32; np];
            let mut acc = 0u32;
            for i in 0..np {
                fill[i] = acc;
                acc += counts[i];
            }
            let out = unsafe { evp.slice(ev_base[q], n) };
            let mut j = 0;
            for s in b_sinks.iter() {
                let (a, o) = s.part(q);
                for r in a.iter().chain(o.iter()) {
                    let id = pid[j] as usize;
                    out[fill[id] as usize] =
                        Event { ts_ms: ws + r.ts as i64, category: r.cc >> 24, cents: r.cc & 0x00FF_FFFF };
                    fill[id] += 1;
                    j += 1;
                }
            }
            let mut st = 0usize;
            for &c in &counts {
                let e = st + c as usize;
                if c > 1 {
                    out[st..e].sort_unstable_by_key(|e| e.ts_ms);
                }
                st = e;
            }
            // clicks
            cl.clear();
            for s in ck_sinks.iter() {
                let (a, o) = s.part(q);
                for r in a.iter().chain(o.iter()) {
                    if let Some(id) = map.get(r.person) {
                        cl.push((id, r.ts, r.cid, r.campaign));
                    }
                }
            }
            cl.sort_unstable_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
            cl.dedup_by(|a, b| a.0 == b.0 && a.2 == b.2);
            let mut ck_cnt = vec![0u32; np];
            for c in cl.iter() {
                ck_cnt[c.0 as usize] += 1;
            }
            let clicks = cl.iter().map(|c| Click { ts_ms: ck_base_ms + c.1 as i64, campaign: c.3, reserved: 0 }).collect();
            res.push((q, Part { persons, ev_cnt: counts, clicks, ck_cnt }));
        },
    )
    .into_iter()
    .map(|s| s.0)
    .collect();
    unsafe { events.set_len(ne) };
    drop(b_sinks);
    drop(ck_sinks);
    let mut parts: Vec<Option<Part<K>>> = (0..P2).map(|_| None).collect();
    for v in parts_out {
        for (q, p) in v {
            parts[q] = Some(p);
        }
    }
    let parts: Vec<Part<K>> = parts.into_iter().map(|p| p.unwrap()).collect();
    tm.lap("group");

    // ---------------- Offsets, clicks, person ids ----------------
    let mut pbase = vec![0usize; P2 + 1];
    let mut cbase = vec![0usize; P2 + 1];
    for q in 0..P2 {
        pbase[q + 1] = pbase[q] + parts[q].persons.len();
        cbase[q + 1] = cbase[q] + parts[q].clicks.len();
    }
    let (np, nc) = (pbase[P2], cbase[P2]);
    let mut clicks: Vec<Click> = Vec::with_capacity(nc);
    let mut event_offsets: Vec<u32> = Vec::with_capacity(np + 1);
    let mut click_offsets: Vec<u32> = Vec::with_capacity(np + 1);
    let mut name_len: Vec<u8> = Vec::with_capacity(np);
    {
        let (cp, eo, co, nl) = (
            SyncPtr(clicks.as_mut_ptr()),
            SyncPtr(event_offsets.as_mut_ptr()),
            SyncPtr(click_offsets.as_mut_ptr()),
            SyncPtr(name_len.as_mut_ptr()),
        );
        let (parts, pbase, cbase, ev_base) = (&parts, &pbase, &cbase, &ev_base);
        let names: Vec<Vec<(usize, Vec<u8>)>> = par::run(threads, P2, |_| Vec::new(), move |res, q| {
            let p = &parts[q];
            let (pb, cb) = (pbase[q], cbase[q]);
            let mut bytes = Vec::with_capacity(p.persons.len() * 10);
            unsafe {
                cp.slice(cb, p.clicks.len()).copy_from_slice(&p.clicks);
                let (mut e, mut c) = (ev_base[q] as u32, cb as u32);
                for i in 0..p.persons.len() {
                    eo.write(pb + i, e);
                    co.write(pb + i, c);
                    e += p.ev_cnt[i];
                    c += p.ck_cnt[i];
                    let before = bytes.len();
                    p.persons[i].unpack(&mut bytes);
                    nl.write(pb + i, (bytes.len() - before) as u8);
                }
            }
            res.push((q, bytes));
        });
        unsafe {
            clicks.set_len(nc);
            event_offsets.set_len(np);
            click_offsets.set_len(np);
            name_len.set_len(np);
        }
        event_offsets.push(ne as u32);
        click_offsets.push(nc as u32);
        let mut nm: Vec<(usize, Vec<u8>)> = names.into_iter().flatten().collect();
        nm.sort_unstable_by_key(|x| x.0);
        let total: usize = nm.iter().map(|x| x.1.len()).sum();
        let mut person_ids = StringTable { bytes: Vec::with_capacity(total), offsets: Vec::with_capacity(np + 1) };
        for (_, b) in &nm {
            person_ids.bytes.extend_from_slice(b);
        }
        person_ids.offsets.push(0);
        let mut acc = 0u64;
        for &l in &name_len {
            acc += l as u64;
            person_ids.offsets.push(acc);
        }
        tm.lap("assemble");
        eprintln!("[ingest] key={}B persons={np} events={ne} (raw {ne_raw}) clicks={nc}", std::mem::size_of::<K>());
        Ok(Loaded {
            window_start_ms: ws,
            window_end_ms: we,
            person_ids,
            categories: ctx.categories.clone(),
            campaigns: ctx.campaigns.clone(),
            events,
            event_offsets,
            clicks,
            click_offsets,
        })
    }
}

fn map_cap<K: Key>(m: &Map<K, u32>) -> usize {
    m.capacity()
}

fn grow<K: Key>(m: &mut Map<K, u32>, persons: &[K]) {
    let mut n = Map::with_capacity(persons.len() * 2);
    for (i, &k) in persons.iter().enumerate() {
        n.insert(k, i as u32);
    }
    *m = n;
}

/// Parses one event row into the P1 sinks.
#[inline(always)]
fn parse_event<K: Key>(
    b: &[u8],
    f: &[(u32, u32); 7],
    ws: i64,
    we: i64,
    prod: &IdLookup,
    sinks: &mut Sinks<RecA<K>>,
) -> Result<(), Need> {
    if flen(f[0]) == 0 {
        return Ok(());
    }
    let eid: K = fkey(b, f[0]).ok_or(Need::Wide)?;
    let ingest_ms = fts(b, f[3]);
    let ingest = match ingest_ms {
        Some(ms) if ms >= 0 && ms % 1000 == 0 && ms / 1000 < u32::MAX as i64 => (ms / 1000 + 1) as u32,
        Some(_) => return Err(Need::Slow),
        None => 0,
    };
    let mut rec = RecA { eid, user: K::default(), ts: 0, ingest, cat: 0, cents: INVALID };
    'v: {
        if flen(f[1]) == 0 || flen(f[5]) == 0 {
            break 'v;
        }
        let t = match fts(b, f[2]) {
            Some(t) if t >= ws && t < we => t,
            _ => break 'v,
        };
        let ty = f[4];
        let cents = if flen(ty) == 4 {
            let w = unsafe { std::ptr::read_unaligned(fptr(b, ty) as *const u32) };
            if w == u32::from_le_bytes(*b"view") || w == u32::from_le_bytes(*b"cart") {
                0
            } else {
                break 'v;
            }
        } else if flen(ty) == 8 && unsafe { std::ptr::read_unaligned(fptr(b, ty) as *const u64) } == u64::from_le_bytes(*b"purchase") {
            let pr = f[6];
            let lp = flen(pr);
            let v = if (1..=8).contains(&lp) {
                unsafe { tsfast::digits8(fptr(b, pr), lp) }.map(|v| v as u32)
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
        // late: ingest - ts > 24h (ingest unparseable = i64::MIN never late)
        if let Some(ims) = ingest_ms {
            if ims - t > ts::DAY_MS {
                break 'v;
            }
        }
        rec.user = fkey(b, f[1]).ok_or(Need::Wide)?;
        rec.ts = (t - ws) as u32;
        rec.cat = unsafe { prod.get(fptr(b, f[5]), flen(f[5])) };
        rec.cents = cents;
    }
    let p = (eid.h() >> (64 - P1_BITS)) as usize;
    sinks.push(p, rec);
    Ok(())
}

#[inline(always)]
fn parse_click<K: Key>(
    b: &[u8],
    f: &[(u32, u32); 4],
    ws: i64,
    we: i64,
    camp: &IdLookup,
    links: &Links<K>,
    sinks: &mut Sinks<RecC<K>>,
) -> Result<(), Need> {
    if flen(f[0]) == 0 || flen(f[1]) == 0 {
        return Ok(());
    }
    let campaign = unsafe { camp.get(fptr(b, f[2]), flen(f[2])) };
    if campaign == INVALID {
        return Ok(());
    }
    let t = match fts(b, f[3]) {
        Some(t) if t >= ws - WEEK_MS && t < we => t,
        _ => return Ok(()),
    };
    let cid: K = fkey(b, f[0]).ok_or(Need::Wide)?;
    let user: K = fkey(b, f[1]).ok_or(Need::Wide)?;
    let person = links.canonical(user);
    let q = (person.h() >> (64 - P2_BITS)) as usize;
    sinks.push(q, RecC { person, cid, ts: (t - (ws - WEEK_MS)) as u32, campaign });
    Ok(())
}

/// Linked-id resolution: id key -> canonical id key. Partitioned by the top
/// hash bits; only ids whose canonical differs from themselves are stored.
struct Links<K: Key> {
    maps: Vec<Map<K, K>>,
}

impl<K: Key> Links<K> {
    fn build(pairs: &[Vec<(K, K)>], threads: usize) -> Links<K> {
        // 1. scatter endpoints by partition: (key, endpoint index)
        let mut off = vec![0usize; pairs.len() + 1];
        for (i, v) in pairs.iter().enumerate() {
            off[i + 1] = off[i] + v.len();
        }
        let nl = off[pairs.len()];
        let off = &off;
        let scattered: Vec<Vec<Vec<(K, u32)>>> = par::run(threads, pairs.len(), |_| Vec::new(), |res, t| {
            let mut parts: Vec<Vec<(K, u32)>> = (0..LP).map(|_| Vec::new()).collect();
            for (i, &(a, b)) in pairs[t].iter().enumerate() {
                let e = 2 * (off[t] + i) as u32;
                parts[(a.h() >> (64 - LP_BITS)) as usize].push((a, e));
                parts[(b.h() >> (64 - LP_BITS)) as usize].push((b, e + 1));
            }
            res.push(parts);
        })
        .into_iter()
        .flatten()
        .collect();
        // 2. per partition: intern; endpoint -> (partition, local id)
        let mut ep: Vec<u64> = vec![0; 2 * nl];
        let epp = SyncPtr(ep.as_mut_ptr());
        let scattered = &scattered;
        let mut keys_parts: Vec<(usize, Vec<K>)> = par::run(threads, LP, |_| Vec::new(), move |res, p| {
            let n: usize = scattered.iter().map(|s| s[p].len()).sum();
            let mut m: Map<K, u32> = Map::with_capacity(n);
            let mut keys: Vec<K> = Vec::new();
            for s in scattered.iter() {
                for &(k, e) in &s[p] {
                    let (id, ins) = m.insert(k, keys.len() as u32);
                    if ins {
                        keys.push(k);
                    }
                    unsafe { epp.write(e as usize, ((p as u64) << 32) | id as u64) };
                }
            }
            res.push((p, keys));
        })
        .into_iter()
        .flatten()
        .collect();
        keys_parts.sort_unstable_by_key(|x| x.0);
        let mut base = vec![0u32; LP + 1];
        for p in 0..LP {
            base[p + 1] = base[p] + keys_parts[p].1.len() as u32;
        }
        let nn = base[LP] as usize;
        let keys: Vec<K> = keys_parts.into_iter().flat_map(|x| x.1).collect();
        // 3. parallel union-find
        let parent: Vec<AtomicU32> = (0..nn as u32).map(AtomicU32::new).collect();
        let node = |code: u64| base[(code >> 32) as usize] + code as u32;
        let step = 1 << 16;
        let (parent_ref, ep_ref, keys_ref) = (&parent, &ep, &keys);
        par::run(threads, nl.div_ceil(step), |_| (), |_, t| {
            for i in t * step..((t + 1) * step).min(nl) {
                let (a, b) = (node(ep_ref[2 * i]), node(ep_ref[2 * i + 1]));
                if a != b {
                    uf_union(parent_ref, a, b);
                }
            }
        });
        // 4. smallest key per root
        let best: Vec<AtomicU32> = (0..nn as u32).map(AtomicU32::new).collect();
        let best_ref = &best;
        par::run(threads, nn.div_ceil(step), |_| (), |_, t| {
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
        // 5. per partition map: id -> canonical (only where different)
        let base_ref = &base;
        let mut maps: Vec<(usize, Map<K, K>)> = par::run(threads, LP, |_| Vec::new(), |res, p| {
            let (s, e) = (base_ref[p] as usize, base_ref[p + 1] as usize);
            let mut canon: Vec<(K, K)> = Vec::new();
            for i in s..e {
                let r = uf_find(parent_ref, i as u32) as usize;
                let c = keys_ref[best_ref[r].load(Ordering::Relaxed) as usize];
                if c != keys_ref[i] {
                    canon.push((keys_ref[i], c));
                }
            }
            let mut m: Map<K, K> = Map::with_capacity(canon.len());
            for (k, c) in canon {
                m.insert(k, c);
            }
            res.push((p, m));
        })
        .into_iter()
        .flatten()
        .collect();
        maps.sort_unstable_by_key(|x| x.0);
        eprintln!("[ingest] links={nl} linked_ids={nn}");
        Links { maps: maps.into_iter().map(|x| x.1).collect() }
    }

    #[inline(always)]
    fn canonical(&self, k: K) -> K {
        let p = (k.h() >> (64 - LP_BITS)) as usize;
        unsafe { self.maps.get_unchecked(p) }.get(k).unwrap_or(k)
    }

    #[inline(always)]
    fn prefetch(&self, k: K) {
        let p = (k.h() >> (64 - LP_BITS)) as usize;
        unsafe { self.maps.get_unchecked(p) }.prefetch(k)
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
            // path halving; only ever re-points to an ancestor (smaller index)
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
        // link larger index under smaller: parent index < child index, so acyclic
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
    let mut sinks: Sinks<RecA<u64>> = Sinks::new(P1, 4_000_000 / P1 * 2);
    for round in 0..2 {
        let mut line = String::new();
        let mut rows = 0usize;
        for stage in 0..5 {
            for l in sinks.len.iter_mut() {
                *l = 0;
            }
            let t = par::thread_cpu();
            let mut acc = 0i64;
            scan::scan::<7, _>(&buf, lo, hi, |b, f| {
                rows += (stage == 0) as usize;
                match stage {
                    0 => acc += f[1].0 as i64,
                    1 => acc += fts(b, f[2]).unwrap_or(0) ^ fts(b, f[3]).unwrap_or(0),
                    2 => acc += fkey::<u64>(b, f[0]).unwrap_or(0) as i64 ^ fkey::<u64>(b, f[1]).unwrap_or(0) as i64,
                    3 => acc += unsafe { prod.get(fptr(b, f[5]), flen(f[5])) } as i64,
                    _ => {
                        let _ = parse_event::<u64>(b, f, ws, we, &prod, &mut sinks);
                    }
                }
            });
            let d = par::thread_cpu() - t;
            line += &format!(" s{stage} {:.1}ns/row ({:.0}MB/s){}", d * 1e9 / rows.max(1) as f64, mb / d, if acc == 1 { "!" } else { "" });
        }
        eprintln!("round {round} rows {rows}:{line}");
    }
}
