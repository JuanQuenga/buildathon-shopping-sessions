//! Parallel exact ingest, generic over the id key width (see keys.rs).
//! `load` tries compact u64 keys, then wide u128 keys, and returns `None` (the
//! caller then uses the generic slow path) if neither can carry the input exactly.
//! Pipeline (docs/ingest.md):
//!   links  : parse -> flat parallel scatter/intern -> parallel union-find -> canonical maps + L3 Bloom
//!   parse  : chunked pread + L1/L2-tiled SIMD scan; events -> P1 SWCB sinks by hash(event_id);
//!            valid clicks -> batched Bloom/prefetched canonical -> P2 SWCB sinks
//!   dedup  : per P1 partition, compact u32 table + L1 dead bitset + sequential Bloom/prefetched pass -> P2 SWCB sinks
//!   group  : per P2 partition, person map, CSR, sort by ts; written in place
use super::key::IdLookup;
use super::keys::{Key, Map};
use super::par::{self, Sinks, SyncPtr};
use super::scan::{self, Chunk, FileInfo, Kind};
use super::tsfast::{self, DateTable};
use super::{slow, small, ts};
use crate::aggregate::{self, output, CampStore, NO_CATEGORY};
use crate::types::*;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

const WEEK_MS: i64 = 7 * ts::DAY_MS;
const WEEK_MS_U32: u32 = WEEK_MS as u32;
const GAP_MS_U32: u32 = 1_800_000;
const DAY_MS_U64: u64 = ts::DAY_MS as u64;
const CHUNK: u64 = 256 << 10;
const LP_BITS: u32 = 8;
const LP: usize = 1 << LP_BITS;
const INVALID_CAMP: u32 = u32::MAX;
const INVALID_CC: u32 = u32::MAX;
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

/// Event candidate before dedup. 32 bytes for u64 keys, 64 bytes for u128 keys.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct RecA<K: Key> {
    eid: K,
    user: K,
    /// ms since window_start (only meaningful when valid)
    ts: u32,
    /// ingest seconds since epoch + 1 (u32::MAX = unparseable, matching ref.py +inf)
    ingest: u32,
    /// (cat << 24) | cents; INVALID_CC if row fails validation as a kept copy
    cc: u32,
    /// Low 32 bits of eid.h() (independent of top p1_bits), avoiding re-hashing in dedup Pass 1.
    eid_hash: u32,
}

/// Surviving event keyed by person. 16 bytes for u64 keys, 32 bytes for u128 keys.
#[derive(Clone, Copy)]
#[repr(C, align(16))]
struct RecB<K: Key> {
    person: K,
    ts: u32,
    /// category << 24 | cents
    cc: u32,
}

/// Valid click keyed by person. 32 bytes for u64 keys, 64 bytes for u128 keys.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct RecC<K: Key> {
    person: K,
    cid: K,
    /// ms since window_start - 7 days
    ts: u32,
    campaign: u32,
}

struct Timer(Instant, f64);
impl Timer {
    fn lap(&mut self, name: &str) {
        let now = Instant::now();
        let main_cpu = par::thread_cpu();
        let cpu = par::CPU_NS.swap(0, Ordering::Relaxed) as f64 * 1e-9 + (main_cpu - self.1);
        eprintln!("[ingest] {name:<12} {:.3}s cpu {:.3}s", (now - self.0).as_secs_f64(), cpu);
        self.0 = now;
        self.1 = par::thread_cpu();
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
fn fts(dt: &DateTable, b: &[u8], f: (u32, u32)) -> Option<i64> {
    unsafe { dt.parse(fptr(b, f), flen(f)) }
}
#[inline(always)]
fn fkey<K: Key>(b: &[u8], f: (u32, u32)) -> Option<K> {
    unsafe { K::enc(fptr(b, f), flen(f)) }
}
#[inline(always)]
fn key_need(f: (u32, u32)) -> Need {
    if flen(f) > super::key::MAX_INLINE { Need::Slow } else { Need::Wide }
}

#[derive(Clone, Copy)]
struct MappedFile(*const u8, usize);
unsafe impl Send for MappedFile {}
unsafe impl Sync for MappedFile {}

/// Everything independent of the key width.
struct Ctx {
    ws: i64,
    we: i64,
    dt: DateTable,
    categories: Vec<Vec<u8>>,
    campaigns: Vec<(Vec<u8>, Vec<u8>)>,
    prod: IdLookup,
    camp: IdLookup,
    files: Vec<FileInfo>,
    handles: Vec<File>,
    mmaps: Vec<Option<MappedFile>>,
    pref_stop: Arc<AtomicBool>,
    link_chunks: Vec<Chunk>,
    main_chunks: Vec<Chunk>,
    event_bytes: u64,
    click_bytes: u64,
    parse_threads: usize,
    p1_bits_override: Option<u32>,
}

impl Drop for Ctx {
    fn drop(&mut self) {
        self.pref_stop.store(true, Ordering::Relaxed);
        for mf in &self.mmaps {
            if let Some(MappedFile(p, len)) = *mf {
                par::huge_free(p as *mut u8, len);
            }
        }
    }
}

#[inline(always)]
fn scan_chunk_ctx<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(
    ctx: &Ctx,
    c: Chunk,
    buf: &mut Vec<u8>,
    f: F,
) {
    let fi_idx = c.file as usize;
    if let Some(MappedFile(ptr, flen)) = ctx.mmaps[fi_idx] {
        let data = unsafe { std::slice::from_raw_parts(ptr, flen) };
        let lo = if c.start == 0 {
            match scan::memchr(b'\n', data) {
                Some(p) => p + 1,
                None => return,
            }
        } else {
            let s = (c.start - 1) as usize;
            match scan::memchr(b'\n', &data[s..]) {
                Some(p) => s + p + 1,
                None => return,
            }
        };
        let own_end = c.end as usize;
        if lo >= own_end {
            return;
        }
        let (hi, has_lf) = match scan::memchr(b'\n', &data[own_end - 1..]) {
            Some(p) => (own_end - 1 + p + 1, true),
            None => (data.len(), false),
        };
        if lo >= hi {
            return;
        }
        if has_lf && hi + scan::PAD <= data.len() && (hi - lo) < (u32::MAX as usize - 256) {
            let base = lo.saturating_sub(1);
            scan::scan::<N, F>(&data[base..hi + scan::PAD], lo - base, hi - base, f);
        } else {
            let len = hi - lo;
            let extra = usize::from(!has_lf);
            buf.clear();
            buf.reserve(len + extra + scan::PAD + 1);
            buf.extend_from_slice(&data[lo..hi]);
            if !has_lf {
                buf.push(b'\n');
            }
            let end_pos = buf.len();
            buf.resize(end_pos + scan::PAD, 0);
            scan::scan::<N, F>(buf, 0, end_pos, f);
        }
        return;
    }
    let fi = &ctx.files[fi_idx];
    let (lo, hi) = scan::read_chunk(&ctx.handles[fi_idx], fi.len, c, buf);
    if lo < hi {
        scan::scan::<N, F>(buf, lo, hi, f);
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse::<usize>().ok())
}

fn build_ctx(data_dir: &Path, tm: &mut Timer) -> Option<Ctx> {
    let (ws, we) = small::read_manifest(data_dir);
    let products = small::read_products(data_dir);
    let (campaigns, campaign_map) = small::read_campaigns(data_dir);
    // Representation limits of the compact records (else: generic path).
    if we < ws || we - ws > u32::MAX as i64 - WEEK_MS || products.categories.len() > 256 {
        return None;
    }
    let dt = DateTable::build(ws, we);
    let prod = IdLookup::build(products.map.iter().map(|(k, &v)| (k.as_slice(), v)), products.unknown);
    let camp = IdLookup::build(campaign_map.iter().map(|(k, &v)| (k.as_slice(), v)), INVALID_CAMP);
    let mut files: Vec<FileInfo> = Vec::new();
    for (sub, kind) in [("events", Kind::Events), ("clicks", Kind::Clicks), ("links", Kind::Links)] {
        for p in small::list_csv(data_dir, sub) {
            let len = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            files.push(FileInfo { path: p, len, kind });
        }
    }
    let handles: Vec<File> = files.iter().map(|f| File::open(&f.path).expect("open csv")).collect();
    let total_csv_bytes: u64 = files.iter().map(|f| f.len).sum();
    let use_mmap = total_csv_bytes <= (10u64 << 30) && std::env::var("INGEST_PREAD").is_err();
    let mmaps: Vec<Option<MappedFile>> = if use_mmap {
        use std::os::fd::AsRawFd;
        handles
            .iter()
            .zip(files.iter())
            .map(|(h, f)| par::mmap_ro(h.as_raw_fd(), f.len as usize).map(|p| MappedFile(p, f.len as usize)))
            .collect()
    } else {
        vec![None; files.len()]
    };
    let pref_stop = Arc::new(AtomicBool::new(false));
    let pref_threads = if std::env::var("INGEST_NO_PREFETCH").is_ok() {
        0
    } else {
        env_usize("INGEST_PREFETCH").unwrap_or(4).min(8)
    };
    if pref_threads > 0 {
        let mut segs: Vec<(std::path::PathBuf, u64, u64)> = Vec::new();
        let seg_sz: u64 = 16 << 20;
        for f in files.iter().filter(|f| f.kind == Kind::Links).chain(files.iter().filter(|f| f.kind != Kind::Links)) {
            let mut off = 0u64;
            while off < f.len {
                let len = seg_sz.min(f.len - off);
                segs.push((f.path.clone(), off, len));
                off += len;
            }
        }
        let n_pref = segs.len().min(pref_threads);
        if n_pref > 0 {
            let segs = Arc::new(segs);
            let next = Arc::new(AtomicUsize::new(0));
            for _ in 0..n_pref {
                let segs = Arc::clone(&segs);
                let next = Arc::clone(&next);
                let stop = Arc::clone(&pref_stop);
                std::thread::spawn(move || {
                    use std::os::fd::AsRawFd;
                    let mut cached: Option<(std::path::PathBuf, File)> = None;
                    loop {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= segs.len() {
                            break;
                        }
                        let (ref path, off, len) = segs[i];
                        let fd = match cached.as_ref() {
                            Some((p, h)) if p == path => h.as_raw_fd(),
                            _ => match File::open(path) {
                                Ok(h) => {
                                    let fd = h.as_raw_fd();
                                    cached = Some((path.clone(), h));
                                    fd
                                }
                                Err(_) => continue,
                            },
                        };
                        par::fadvise_range(fd, off, len);
                    }
                });
            }
        }
    }
    let default_chunk = if use_mmap { 512 << 10 } else { CHUNK };
    let chunk_bytes = env_usize("INGEST_CHUNK_KB").map(|kb| (kb.clamp(32, 4096) as u64) << 10).unwrap_or(default_chunk);
    let chunks = scan::plan(&files, chunk_bytes);
    let link_chunks = chunks.iter().copied().filter(|c| files[c.file as usize].kind == Kind::Links).collect();
    let main_chunks = chunks.iter().copied().filter(|c| files[c.file as usize].kind != Kind::Links).collect();
    let event_bytes = files.iter().filter(|f| f.kind == Kind::Events).map(|f| f.len).sum();
    let click_bytes = files.iter().filter(|f| f.kind == Kind::Clicks).map(|f| f.len).sum();
    let parse_threads = env_usize("INGEST_PARSE_THREADS").unwrap_or(0);
    let p1_bits_override = env_usize("INGEST_P1_BITS").map(|b| (b as u32).clamp(6, 12));
    let ctx = Ctx {
        ws,
        we,
        dt,
        categories: products.categories,
        campaigns,
        prod,
        camp,
        files,
        handles,
        mmaps,
        pref_stop,
        link_chunks,
        main_chunks,
        event_bytes,
        click_bytes,
        parse_threads,
        p1_bits_override,
    };
    tm.lap("small");
    Some(ctx)
}

/// Fused end-to-end fast path: reads CSVs, deduplicates, groups+aggregates per P2
/// partition in L2-resident compact buffers, and writes Parquet directly without
/// materializing `Loaded`. Returns `false` if the generic slow path is needed.
pub fn run_to_output(data_dir: &Path, output_dir: &Path, threads: usize) -> bool {
    let threads = threads.clamp(1, MAX_THREADS);
    let mut tm = Timer(Instant::now(), par::thread_cpu());
    let Some(ctx) = build_ctx(data_dir, &mut tm) else {
        return false;
    };
    let ok = match run_fused::<u64>(&ctx, output_dir, threads, &mut tm) {
        Ok(()) => true,
        Err(Need::Wide) => {
            eprintln!("[ingest] compact keys not applicable: retrying with wide keys");
            run_fused::<u128>(&ctx, output_dir, threads, &mut tm).is_ok()
        }
        Err(Need::Slow) => false,
    };
    if ok {
        // Prefetch threads were already stopped after `parse`; skip `munmap` TLB shootdowns right before exit(0).
        ctx.pref_stop.store(true, Ordering::Relaxed);
        std::mem::forget(ctx);
    }
    ok
}

pub fn load(data_dir: &Path, threads: usize) -> Option<Loaded> {
    let threads = threads.clamp(1, MAX_THREADS);
    let mut tm = Timer(Instant::now(), par::thread_cpu());
    let ctx = build_ctx(data_dir, &mut tm)?;
    match run::<u64>(&ctx, threads, &mut tm) {
        Ok(l) => Some(l),
        Err(Need::Wide) => {
            eprintln!("[ingest] compact keys not applicable: retrying with wide keys");
            run::<u128>(&ctx, threads, &mut tm).ok()
        }
        Err(Need::Slow) => None,
    }
}

struct Deduped<K: Key> {
    b_sinks: Vec<Sinks<RecB<K>>>,
    ck_sinks: Vec<Sinks<RecC<K>>>,
    p2: usize,
    ne_raw: usize,
}

fn ingest_to_sinks<const ALL_CAT: bool, K: Key>(ctx: &Ctx, threads: usize, tm: &mut Timer) -> Result<Deduped<K>, Need> {
    let flag = Flag(AtomicU8::new(0));
    let (ws, we) = (ctx.ws, ctx.we);
    let dt = &ctx.dt;

    // ---------------- Links ----------------
    let link_pairs: Vec<Vec<(K, K)>> = par::run(
        threads,
        ctx.link_chunks.len(),
        |_| (Vec::<(K, K)>::new(), Vec::<u8>::new()),
        |(pairs, buf), i| {
            if flag.get().is_some() {
                return;
            }
            let c = ctx.link_chunks[i];
            scan_chunk_ctx::<2, _>(ctx, c, buf, |b, f| {
                if flen(f[0]) == 0 || flen(f[1]) == 0 {
                    return;
                }
                match (fkey::<K>(b, f[0]), fkey::<K>(b, f[1])) {
                    (Some(a), Some(c)) => pairs.push((a, c)),
                    (None, _) => flag.set(key_need(f[0])),
                    (_, None) => flag.set(key_need(f[1])),
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
    let parse_threads = if ctx.parse_threads > 0 { ctx.parse_threads.clamp(1, MAX_THREADS) } else { threads };
    // Dynamic partition counts: keep SWCB (p1 * 256B = 128-512 KB) + 256 KB chunk buffer in L2,
    // AND keep dedup u32 table (128-256 KB) deep in L2.
    let p1_bits: u32 = ctx.p1_bits_override.unwrap_or(if est_ev <= 4_000_000 { 8 } else if est_ev <= 30_000_000 { 10 } else { 11 });
    let p1: usize = 1 << p1_bits;
    let p2_bits: u32 = if est_ev <= 4_000_000 { 8 } else if est_ev <= 30_000_000 { 9 } else { 10 };
    let p2: usize = 1 << p2_bits;
    let cap_a = est_ev * 112 / 100 / parse_threads / p1 + 24;
    let cap_c = est_ck * 112 / 100 / parse_threads / p2 + 24;

    #[derive(Clone, Copy)]
    struct PendingClick<K: Key> {
        user: K,
        uh: u64,
        cid: K,
        ts: u32,
        campaign: u32,
    }
    const CK_RING: usize = 32;
    struct ParseState<K: Key> {
        ev: Sinks<RecA<K>>,
        ck: Sinks<RecC<K>>,
        ck_ring: [PendingClick<K>; CK_RING],
        ck_cnt: usize,
        buf: Vec<u8>,
    }
    let states: Vec<ParseState<K>> = par::run_fini(
        parse_threads,
        ctx.main_chunks.len(),
        |_| ParseState {
            ev: Sinks::new(p1, cap_a),
            ck: Sinks::new(p2, cap_c),
            ck_ring: [PendingClick { user: K::default(), uh: 0, cid: K::default(), ts: 0, campaign: 0 }; CK_RING],
            ck_cnt: 0,
            buf: Vec::with_capacity(CHUNK as usize + 8192),
        },
        |st, i| {
            if flag.get().is_some() {
                return;
            }
            let c = ctx.main_chunks[i];
            let fi = &ctx.files[c.file as usize];
            let mut buf = std::mem::take(&mut st.buf);
            match fi.kind {
                Kind::Events => scan_chunk_ctx::<7, _>(ctx, c, &mut buf, |b, f| {
                    if let Err(n) = parse_event::<ALL_CAT, K>(dt, b, f, ws, we, &ctx.prod, p1_bits, &mut st.ev) {
                        flag.set(n);
                    }
                }),
                Kind::Clicks => {
                    let ck_base = ws - WEEK_MS;
                    let has_links = links.has_links;
                    scan_chunk_ctx::<4, _>(ctx, c, &mut buf, |b, f| {
                        if flen(f[0]) == 0 || flen(f[1]) == 0 {
                            return;
                        }
                        let campaign = unsafe { ctx.camp.get(fptr(b, f[2]), flen(f[2])) };
                        if campaign == INVALID_CAMP {
                            return;
                        }
                        let t = match fts(dt, b, f[3]) {
                            Some(t) if t >= ck_base && t < we => t,
                            _ => return,
                        };
                        let cid: K = match fkey(b, f[0]) {
                            Some(v) => v,
                            None => {
                                flag.set(key_need(f[0]));
                                return;
                            }
                        };
                        let user: K = match fkey(b, f[1]) {
                            Some(v) => v,
                            None => {
                                flag.set(key_need(f[1]));
                                return;
                            }
                        };
                        let uh = user.h();
                        let ts_rel = (t - ck_base) as u32;
                        if has_links {
                            links.prefetch_map_h(uh);
                            let slot = st.ck_cnt & (CK_RING - 1);
                            if st.ck_cnt >= CK_RING {
                                let pc = unsafe { *st.ck_ring.get_unchecked(slot) };
                                let (person, ph) = links.canonical_h_hash(pc.user, pc.uh);
                                let q = (ph >> (64 - p2_bits)) as usize;
                                st.ck.push(q, RecC { person, cid: pc.cid, ts: pc.ts, campaign: pc.campaign });
                            }
                            unsafe {
                                *st.ck_ring.get_unchecked_mut(slot) =
                                    PendingClick { user, uh, cid, ts: ts_rel, campaign };
                            }
                            st.ck_cnt += 1;
                        } else {
                            let q = (uh >> (64 - p2_bits)) as usize;
                            st.ck.push(q, RecC { person: user, cid, ts: ts_rel, campaign });
                        }
                    });
                }
                Kind::Links => unreachable!(),
            }
            st.buf = buf;
        },
        |st| {
            let n_rem = st.ck_cnt.min(CK_RING);
            let start = st.ck_cnt - n_rem;
            for k in start..st.ck_cnt {
                let pc = unsafe { *st.ck_ring.get_unchecked(k & (CK_RING - 1)) };
                let (person, ph) = links.canonical_h_hash(pc.user, pc.uh);
                let q = (ph >> (64 - p2_bits)) as usize;
                st.ck.push(q, RecC { person, cid: pc.cid, ts: pc.ts, campaign: pc.campaign });
            }
            st.ck_cnt = 0;
            st.ev.finish();
            st.ck.finish();
        },
    );
    if let Some(n) = flag.get() {
        return Err(n);
    }
    ctx.pref_stop.store(true, Ordering::Relaxed);
    let mut ev_sinks: Vec<Sinks<RecA<K>>> = Vec::with_capacity(parse_threads);
    let mut ck_sinks: Vec<Sinks<RecC<K>>> = Vec::with_capacity(parse_threads);
    for s in states {
        ev_sinks.push(s.ev);
        ck_sinks.push(s.ck);
    }
    tm.lap("parse");

    // ---------------- Event dedup ----------------
    let ne_raw: usize = (0..p1).map(|p| ev_sinks.iter().map(|s| s.count(p)).sum::<usize>()).sum();
    let cap_b = ne_raw * 112 / 100 / threads / p2 + 32;
    struct DedupState<K: Key> {
        t32: Vec<u32>,
        t64: Vec<u64>,
        dead: Vec<u64>,
        out: Sinks<RecB<K>>,
    }
    let b_sinks: Vec<Sinks<RecB<K>>> = par::run_fini(
        threads,
        p1,
        |_| DedupState {
            t32: Vec::new(),
            t64: Vec::new(),
            dead: Vec::new(),
            out: Sinks::<RecB<K>>::new(p2, cap_b),
        },
        |st, p| {
            // Flatten non-empty arena + overflow slices for partition `p`.
            let mut slices: [(&[RecA<K>], u32); MAX_THREADS * 2] = [(&[], 0); MAX_THREADS * 2];
            let mut nsl = 0usize;
            let mut total = 0usize;
            for s in ev_sinks.iter() {
                let (a, o) = s.part(p);
                if !a.is_empty() {
                    slices[nsl] = (a, total as u32);
                    total += a.len();
                    nsl += 1;
                }
                if !o.is_empty() {
                    slices[nsl] = (o, total as u32);
                    total += o.len();
                    nsl += 1;
                }
            }
            if total == 0 {
                return;
            }
            let active = &slices[..nsl];
            let get_flat = |idx: u32| -> &RecA<K> {
                // Usually <= 8 slices; linear scan with cumulative base is tiny and branch-predictable.
                let mut i = nsl - 1;
                while i > 0 && idx < active[i].1 {
                    i -= 1;
                }
                let (sl, base) = active[i];
                unsafe { sl.get_unchecked((idx - base) as usize) }
            };
            let dead_words = total.div_ceil(64);
            if st.dead.len() < dead_words {
                st.dead.resize(dead_words, 0);
            }
            let dead = &mut st.dead[..dead_words];
            dead.fill(0);
            let cap = (total * 2).next_power_of_two();
            let mask = cap - 1;

            // Pass 1: sequential insert; mark duplicate losers in the L1-resident `dead` bitset.
            if total < (1 << 18) - 1 {
                if st.t32.len() < cap {
                    st.t32.resize(cap, 0);
                }
                let table = &mut st.t32[..cap];
                table.fill(0);
                let mut flat = 0u32;
                for &(sl, _) in active {
                    for r in sl {
                        // Reuse low 32 bits of eid.h() computed during parse (zero hashing in dedup Pass 1!).
                        let h = r.eid_hash;
                        let fp = h >> 18; // 14-bit fingerprint
                        let mut slot = (h as usize) & mask;
                        loop {
                            let v = unsafe { *table.get_unchecked(slot) };
                            if v == 0 {
                                unsafe { *table.get_unchecked_mut(slot) = (fp << 18) | (flat + 1) };
                                break;
                            }
                            if (v >> 18) == fp {
                                let prev = (v & ((1 << 18) - 1)) - 1;
                                let w = get_flat(prev);
                                if w.eid == r.eid {
                                    if r.ingest < w.ingest {
                                        unsafe {
                                            *dead.get_unchecked_mut((prev >> 6) as usize) |= 1u64 << (prev & 63);
                                            *table.get_unchecked_mut(slot) = (fp << 18) | (flat + 1);
                                        }
                                    } else {
                                        unsafe {
                                            *dead.get_unchecked_mut((flat >> 6) as usize) |= 1u64 << (flat & 63);
                                        }
                                    }
                                    break;
                                }
                            }
                            slot = (slot + 1) & mask;
                        }
                        flat += 1;
                    }
                }
            } else {
                // Skew fallback: >= 262k records in one partition -> u64 table (fp32 | idx32+1).
                if st.t64.len() < cap {
                    st.t64.resize(cap, 0);
                }
                let table = &mut st.t64[..cap];
                table.fill(0);
                let mut flat = 0u32;
                for &(sl, _) in active {
                    for r in sl {
                        let h = r.eid.h();
                        let fp = (h >> 32) as u32;
                        let mut slot = (h as usize) & mask;
                        loop {
                            let v = unsafe { *table.get_unchecked(slot) };
                            if v == 0 {
                                unsafe { *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (flat as u64 + 1) };
                                break;
                            }
                            if (v >> 32) as u32 == fp {
                                let prev = (v as u32).wrapping_sub(1);
                                let w = get_flat(prev);
                                if w.eid == r.eid {
                                    if r.ingest < w.ingest {
                                        unsafe {
                                            *dead.get_unchecked_mut((prev >> 6) as usize) |= 1u64 << (prev & 63);
                                            *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (flat as u64 + 1);
                                        }
                                    } else {
                                        unsafe {
                                            *dead.get_unchecked_mut((flat >> 6) as usize) |= 1u64 << (flat & 63);
                                        }
                                    }
                                    break;
                                }
                            }
                            slot = (slot + 1) & mask;
                        }
                        flat += 1;
                    }
                }
            }

            // Pass 2: 100% SEQUENTIAL scan over winners with continuous ring-32 prefetch into Links flat huge-page table.
            #[derive(Clone, Copy)]
            struct PendingEv<K: Key> {
                user: K,
                uh: u64,
                ts: u32,
                cc: u32,
            }
            const EV_RING: usize = 32;
            let mut ring = [PendingEv { user: K::default(), uh: 0, ts: 0, cc: 0 }; EV_RING];
            let mut cnt = 0usize;
            let has_links = links.has_links;
            let mut flat = 0usize;
            for &(sl, _) in active {
                for r in sl {
                    let is_dead = unsafe { (*dead.get_unchecked(flat >> 6) >> (flat & 63)) & 1 != 0 };
                    flat += 1;
                    if is_dead || r.cc == INVALID_CC {
                        continue;
                    }
                    let uh = r.user.h();
                    if has_links {
                        links.prefetch_map_h(uh);
                        let slot = cnt & (EV_RING - 1);
                        if cnt >= EV_RING {
                            let pe = unsafe { *ring.get_unchecked(slot) };
                            let (person, ph) = links.canonical_h_hash(pe.user, pe.uh);
                            let q = (ph >> (64 - p2_bits)) as usize;
                            st.out.push(q, RecB { person, ts: pe.ts, cc: pe.cc });
                        }
                        unsafe {
                            *ring.get_unchecked_mut(slot) = PendingEv { user: r.user, uh, ts: r.ts, cc: r.cc };
                        }
                        cnt += 1;
                    } else {
                        let q = (uh >> (64 - p2_bits)) as usize;
                        st.out.push(q, RecB { person: r.user, ts: r.ts, cc: r.cc });
                    }
                }
            }
            let n_rem = cnt.min(EV_RING);
            let start = cnt - n_rem;
            for k in start..cnt {
                let pe = unsafe { *ring.get_unchecked(k & (EV_RING - 1)) };
                let (person, ph) = links.canonical_h_hash(pe.user, pe.uh);
                let q = (ph >> (64 - p2_bits)) as usize;
                st.out.push(q, RecB { person, ts: pe.ts, cc: pe.cc });
            }
        },
        |st| {
            st.out.finish();
        },
    )
    .into_iter()
    .map(|s| s.out)
    .collect();
    drop(ev_sinks);
    tm.lap("dedup");
    Ok(Deduped { b_sinks, ck_sinks, p2, ne_raw })
}

/// 8-byte compact surviving event inside a P2 partition: `(ts << 32) | cc`.
/// Single-instruction `u64` ordering sorts primarily by `ts` ascending.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct CompactEv(u64);

impl CompactEv {
    #[inline(always)]
    fn new(ts: u32, cc: u32) -> Self {
        CompactEv(((ts as u64) << 32) | (cc as u64))
    }
    #[inline(always)]
    fn ts(self) -> u32 {
        (self.0 >> 32) as u32
    }
    #[inline(always)]
    fn cc(self) -> u32 {
        self.0 as u32
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct CompactClick<K: Key> {
    ts: u32,
    campaign: u32,
    cid: K,
}

#[inline]
fn insertion_sort_ev(a: &mut [CompactEv]) {
    for i in 1..a.len() {
        let v = unsafe { *a.get_unchecked(i) };
        let mut j = i;
        while j > 0 && unsafe { a.get_unchecked(j - 1).0 } > v.0 {
            unsafe { *a.get_unchecked_mut(j) = *a.get_unchecked(j - 1) };
            j -= 1;
        }
        unsafe { *a.get_unchecked_mut(j) = v };
    }
}

/// 3-pass (11+11+10 bit) LSB radix sort on `CompactEv::ts()` (skips trivial passes).
fn radix_sort_ev(slice: &mut [CompactEv], tmp: &mut Vec<CompactEv>) {
    let n = slice.len();
    if tmp.len() < n {
        tmp.resize(n, CompactEv(0));
    }
    let buf = &mut tmp[..n];
    let mut c0 = [0u32; 2048];
    let mut c1 = [0u32; 2048];
    let mut c2 = [0u32; 1024];
    for e in slice.iter() {
        let t = e.ts();
        c0[(t & 2047) as usize] += 1;
        c1[((t >> 11) & 2047) as usize] += 1;
        c2[(t >> 22) as usize] += 1;
    }
    let t0 = slice[0].ts();
    let skip0 = c0[(t0 & 2047) as usize] == n as u32;
    let skip1 = c1[((t0 >> 11) & 2047) as usize] == n as u32;
    let skip2 = c2[(t0 >> 22) as usize] == n as u32;
    let mut in_tmp = false;

    if !skip0 {
        let mut acc = 0u32;
        for c in c0.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        for &e in slice.iter() {
            let b = (e.ts() & 2047) as usize;
            let dst = unsafe { c0.get_unchecked_mut(b) };
            unsafe { *buf.get_unchecked_mut(*dst as usize) = e };
            *dst += 1;
        }
        in_tmp = true;
    }
    if !skip1 {
        let mut acc = 0u32;
        for c in c1.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        if in_tmp {
            for &e in buf.iter() {
                let b = ((e.ts() >> 11) & 2047) as usize;
                let dst = unsafe { c1.get_unchecked_mut(b) };
                unsafe { *slice.get_unchecked_mut(*dst as usize) = e };
                *dst += 1;
            }
        } else {
            for &e in slice.iter() {
                let b = ((e.ts() >> 11) & 2047) as usize;
                let dst = unsafe { c1.get_unchecked_mut(b) };
                unsafe { *buf.get_unchecked_mut(*dst as usize) = e };
                *dst += 1;
            }
        }
        in_tmp = !in_tmp;
    }
    if !skip2 {
        let mut acc = 0u32;
        for c in c2.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        if in_tmp {
            for &e in buf.iter() {
                let b = (e.ts() >> 22) as usize;
                let dst = unsafe { c2.get_unchecked_mut(b) };
                unsafe { *slice.get_unchecked_mut(*dst as usize) = e };
                *dst += 1;
            }
        } else {
            for &e in slice.iter() {
                let b = (e.ts() >> 22) as usize;
                let dst = unsafe { c2.get_unchecked_mut(b) };
                unsafe { *buf.get_unchecked_mut(*dst as usize) = e };
                *dst += 1;
            }
        }
        in_tmp = !in_tmp;
    }
    if in_tmp {
        slice.copy_from_slice(buf);
    }
}

#[inline]
fn sort_ev_slice(slice: &mut [CompactEv], tmp: &mut Vec<CompactEv>) {
    let n = slice.len();
    if n <= 1 {
        return;
    }
    if n <= 16 {
        insertion_sort_ev(slice);
    } else if n <= 256 {
        slice.sort_unstable();
    } else {
        radix_sort_ev(slice, tmp);
    }
}

#[inline]
fn sort_and_dedup_clicks<K: Key>(cl: &mut [CompactClick<K>]) -> usize {
    let n = cl.len();
    if n <= 1 {
        return n;
    }
    if n <= 16 {
        for i in 1..n {
            let v = cl[i];
            let mut j = i;
            while j > 0 && (cl[j - 1].ts > v.ts || (cl[j - 1].ts == v.ts && cl[j - 1].cid > v.cid)) {
                cl[j] = cl[j - 1];
                j -= 1;
            }
            cl[j] = v;
        }
    } else {
        cl.sort_unstable_by(|a, b| (a.ts, a.cid).cmp(&(b.ts, b.cid)));
    }
    let mut w = 1usize;
    for r in 1..n {
        if cl[r].cid != cl[w - 1].cid {
            cl[w] = cl[r];
            w += 1;
        }
    }
    w
}

/// Fused group + sessionize + attribute + assists + direct user_daily Parquet output.
fn run_fused<K: Key>(ctx: &Ctx, output_dir: &Path, threads: usize, tm: &mut Timer) -> Result<(), Need> {
    // Non-purchase events (view/cart, ~97%) never use `category` in daily outputs; skip `prod.get` for them!
    let Deduped { b_sinks, ck_sinks, p2, ne_raw } = ingest_to_sinks::<false, K>(ctx, threads, tm)?;

    let ud = output_dir.join("user_daily");
    let cd = output_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");

    let mut ev_cnt_part = vec![0usize; p2];
    let mut ne = 0usize;
    for q in 0..p2 {
        let c: usize = b_sinks.iter().map(|s| s.count(q)).sum();
        ev_cnt_part[q] = c;
        ne += c;
    }
    let mut q_order: Vec<usize> = (0..p2).collect();
    q_order.sort_unstable_by_key(|&q| std::cmp::Reverse(ev_cnt_part[q]));

    let ws = ctx.ws;
    let we = ctx.we;
    let day0 = aggregate::day_of(ws);
    let ws_rem = ws.rem_euclid(ts::DAY_MS) as u64;
    let ndays = if we > ws {
        (aggregate::day_of(we - 1) as i64 - day0 as i64 + 1).min(u32::MAX as i64) as u32
    } else {
        0
    };
    let ncamp = ctx.campaigns.len();
    let organic = ncamp as u32;
    let ncat = ctx.categories.len();
    let categories = &ctx.categories;

    struct FusedWorker<'a, K: Key> {
        // Grouping scratch (reused across P2 partitions -> stays L2-hot!)
        map: Map<K, u32>,
        persons: Vec<K>,
        counts: Vec<u32>,
        fill: Vec<u32>,
        pid: Vec<u32>,
        ev_buf: Vec<CompactEv>,
        ev_tmp: Vec<CompactEv>,
        cl_raw: Vec<(u32, CompactClick<K>)>,
        ck_cnt: Vec<u32>,
        ck_off: Vec<u32>,
        cl_buf: Vec<CompactClick<K>>,
        // Aggregate state
        camp: CampStore,
        writer: output::UserWriter<'a>,
        cat_sum: Vec<u64>,
        cat_touched: Vec<u32>,
        s_start: Vec<u32>,
        day_first: Vec<(i32, u32)>,
        stamp: u32,
        c_stamp: Vec<u32>,
        c_lo: Vec<u32>,
        c_hi: Vec<u32>,
        active: Vec<u32>,
        // Counters
        np: usize,
        nc: usize,
        written: usize,
    }

    impl<'a, K: Key> FusedWorker<'a, K> {
        #[inline(always)]
        fn flush_user_day(
            &mut self,
            id_bytes: &[u8],
            categories: &[Vec<u8>],
            day: i32,
            sessions: u32,
            events: u32,
            revenue: i64,
        ) {
            let mut top = NO_CATEGORY;
            let mut best = 0u64;
            for &c in &self.cat_touched {
                let v = unsafe { *self.cat_sum.get_unchecked(c as usize) };
                if v > best || (v == best && c < top) {
                    best = v;
                    top = c;
                }
                unsafe { *self.cat_sum.get_unchecked_mut(c as usize) = 0 };
            }
            self.cat_touched.clear();
            let cat = if top == NO_CATEGORY {
                None
            } else {
                Some(unsafe { categories.get_unchecked(top as usize) }.as_slice())
            };
            self.writer.push(id_bytes, day, sessions, events, revenue, cat);
        }

        #[inline(always)]
        fn count_interval(&mut self, camp: u32, lo: u32, hi: u32) {
            let st = &self.s_start;
            let df = &self.day_first;
            if st.len() == 1 {
                let s0 = unsafe { *st.get_unchecked(0) };
                if lo <= s0 && s0 <= hi {
                    self.camp.cell(camp, unsafe { df.get_unchecked(0).0 }).assisted += 1;
                }
                return;
            }
            let a = st.partition_point(|&x| x < lo);
            let b = st.partition_point(|&x| x <= hi);
            if a >= b {
                return;
            }
            if df.len() == 1 {
                self.camp.cell(camp, unsafe { df.get_unchecked(0).0 }).assisted += (b - a) as i64;
                return;
            }
            let mut di = df.partition_point(|d| (d.1 as usize) <= a) - 1;
            let mut idx = a;
            while idx < b {
                let end = if di + 1 < df.len() {
                    (df[di + 1].1 as usize).min(b)
                } else {
                    b
                };
                let day = df[di].0;
                self.camp.cell(camp, day).assisted += (end - idx) as i64;
                idx = end;
                di += 1;
            }
        }

        #[inline]
        fn assists(&mut self, pcl: &[CompactClick<K>]) {
            let first = self.s_start[0];
            let last = *unsafe { self.s_start.last().unwrap_unchecked() };
            if pcl.len() == 1 {
                let c = unsafe { *pcl.get_unchecked(0) };
                let lo = c.ts.saturating_sub(WEEK_MS_U32);
                if lo <= last && c.ts >= first {
                    self.count_interval(c.campaign, lo, c.ts);
                }
                return;
            }
            self.stamp = self.stamp.wrapping_add(1);
            if self.stamp == 0 {
                self.c_stamp.fill(0);
                self.stamp = 1;
            }
            let stamp = self.stamp;
            for c in pcl {
                let t = c.ts;
                let lo = t.saturating_sub(WEEK_MS_U32);
                if lo > last {
                    break;
                }
                if t < first {
                    continue;
                }
                let ci = c.campaign as usize;
                if unsafe { *self.c_stamp.get_unchecked(ci) } == stamp {
                    let prev_hi = unsafe { *self.c_hi.get_unchecked(ci) };
                    if lo <= prev_hi {
                        unsafe { *self.c_hi.get_unchecked_mut(ci) = t };
                        continue;
                    }
                    let lo0 = unsafe { *self.c_lo.get_unchecked(ci) };
                    self.count_interval(c.campaign, lo0, prev_hi);
                } else {
                    unsafe { *self.c_stamp.get_unchecked_mut(ci) = stamp };
                    self.active.push(c.campaign);
                }
                unsafe {
                    *self.c_lo.get_unchecked_mut(ci) = lo;
                    *self.c_hi.get_unchecked_mut(ci) = t;
                }
            }
            let active = std::mem::take(&mut self.active);
            for &c in &active {
                let lo = unsafe { *self.c_lo.get_unchecked(c as usize) };
                let hi = unsafe { *self.c_hi.get_unchecked(c as usize) };
                self.count_interval(c, lo, hi);
            }
            self.active = active;
            self.active.clear();
        }
    }

    let q_order = &q_order;
    let ev_cnt_part = &ev_cnt_part;
    let ud_ref = &ud;
    let est_user_rows = ne / threads / 8 + 1024;
    let workers: Vec<FusedWorker<'_, K>> = par::run_fini(
        threads,
        p2,
        |tid| FusedWorker {
            map: Map::<K, u32>::with_capacity(1024),
            persons: Vec::with_capacity(2048),
            counts: Vec::with_capacity(2048),
            fill: Vec::with_capacity(2048),
            pid: Vec::with_capacity(65536),
            ev_buf: Vec::with_capacity(65536),
            ev_tmp: Vec::new(),
            cl_raw: Vec::with_capacity(16384),
            ck_cnt: Vec::with_capacity(2048),
            ck_off: Vec::with_capacity(2048),
            cl_buf: Vec::with_capacity(16384),
            camp: CampStore::new(ncamp + 1, day0, ndays),
            writer: output::UserWriter::new(ud_ref, tid, est_user_rows),
            cat_sum: vec![0u64; ncat],
            cat_touched: Vec::with_capacity(32),
            s_start: Vec::with_capacity(256),
            day_first: Vec::with_capacity(32),
            stamp: 0,
            c_stamp: vec![0u32; ncamp],
            c_lo: vec![0u32; ncamp],
            c_hi: vec![0u32; ncamp],
            active: Vec::with_capacity(64),
            np: 0,
            nc: 0,
            written: 0,
        },
        |w, idx| {
            let q = q_order[idx];
            let n = ev_cnt_part[q];
            if n == 0 {
                return;
            }
            w.map.reset(n / 20 + 16);
            w.persons.clear();
            w.counts.clear();
            if w.pid.capacity() < n {
                w.pid.reserve(n - w.pid.len());
            }
            unsafe { w.pid.set_len(n) };
            let pid_ptr = w.pid.as_mut_ptr();
            let mut j = 0usize;
            for s in b_sinks.iter() {
                let (a, o) = s.part(q);
                for sl in [a, o] {
                    for r in sl {
                        let next = w.persons.len() as u32;
                        let (id, ins) = w.map.insert(r.person, next);
                        if ins {
                            w.persons.push(r.person);
                            w.counts.push(0);
                            if w.map.len() * 2 > map_cap(&w.map) {
                                grow(&mut w.map, &w.persons);
                            }
                        }
                        unsafe {
                            *w.counts.get_unchecked_mut(id as usize) += 1;
                            pid_ptr.add(j).write(id);
                        }
                        j += 1;
                    }
                }
            }
            let np = w.persons.len();
            w.np += np;
            w.fill.clear();
            w.fill.reserve(np);
            let mut acc = 0u32;
            for i in 0..np {
                w.fill.push(acc);
                acc += w.counts[i];
            }
            if w.ev_buf.capacity() < n {
                w.ev_buf.reserve(n - w.ev_buf.len());
            }
            unsafe { w.ev_buf.set_len(n) };
            let ev_ptr = w.ev_buf.as_mut_ptr();
            let mut j = 0usize;
            for s in b_sinks.iter() {
                let (a, o) = s.part(q);
                for sl in [a, o] {
                    for r in sl {
                        let id = unsafe { *pid_ptr.add(j) } as usize;
                        let pos = unsafe { *w.fill.get_unchecked(id) } as usize;
                        unsafe {
                            ev_ptr.add(pos).write(CompactEv::new(r.ts, r.cc));
                            *w.fill.get_unchecked_mut(id) = (pos + 1) as u32;
                        }
                        j += 1;
                    }
                }
            }

            // Collect & count-scatter clicks for persons present in partition `q`.
            w.cl_raw.clear();
            w.ck_cnt.clear();
            w.ck_cnt.resize(np, 0);
            for s in ck_sinks.iter() {
                let (a, o) = s.part(q);
                for sl in [a, o] {
                    for r in sl {
                        if let Some(id) = w.map.get(r.person) {
                            w.cl_raw.push((id, CompactClick { ts: r.ts, campaign: r.campaign, cid: r.cid }));
                            unsafe { *w.ck_cnt.get_unchecked_mut(id as usize) += 1 };
                        }
                    }
                }
            }
            let ncl_raw = w.cl_raw.len();
            w.ck_off.clear();
            if ncl_raw > 0 {
                w.ck_off.reserve(np + 1);
                let mut cacc = 0u32;
                for i in 0..np {
                    w.ck_off.push(cacc);
                    cacc += w.ck_cnt[i];
                }
                w.ck_off.push(cacc);
                w.fill.clear();
                w.fill.extend_from_slice(&w.ck_off[..np]);
                if w.cl_buf.capacity() < ncl_raw {
                    w.cl_buf.reserve(ncl_raw - w.cl_buf.len());
                }
                unsafe { w.cl_buf.set_len(ncl_raw) };
                let cl_ptr = w.cl_buf.as_mut_ptr();
                for &(id, ck) in &w.cl_raw {
                    let pos = unsafe { *w.fill.get_unchecked(id as usize) } as usize;
                    unsafe {
                        cl_ptr.add(pos).write(ck);
                        *w.fill.get_unchecked_mut(id as usize) = (pos + 1) as u32;
                    }
                }
            }

            // Per-person sort + sessionize + credit + assists while hot in L1/L2 cache!
            let mut ev_st = 0usize;
            let mut id_buf = [0u8; 16];
            for pi in 0..np {
                let ec = w.counts[pi] as usize;
                let ev_en = ev_st + ec;
                sort_ev_slice(&mut w.ev_buf[ev_st..ev_en], &mut w.ev_tmp);
                let (cl_st, cl_len) = if ncl_raw > 0 {
                    let cs = w.ck_off[pi] as usize;
                    let ce = w.ck_off[pi + 1] as usize;
                    let len = sort_and_dedup_clicks(&mut w.cl_buf[cs..ce]);
                    w.nc += len;
                    (cs, len)
                } else {
                    (0, 0)
                };
                let id_len = w.persons[pi].unpack_buf(&mut id_buf);
                let id_bytes = &id_buf[..id_len];

                w.s_start.clear();
                w.day_first.clear();
                let mut i = ev_st;
                let mut cj = cl_st;
                let cl_end = cl_st + cl_len;
                let mut grp_first = cl_st;
                let mut last_ts = 0u32;
                let mut have_click = false;
                let mut ud_day = i32::MIN;
                let mut next_day_s = 0u64;
                let (mut ud_sessions, mut ud_events, mut ud_revenue) = (0u32, 0u32, 0i64);

                while i < ev_en {
                    let s = unsafe { w.ev_buf.get_unchecked(i).ts() };
                    let day = if (s as u64) < next_day_s {
                        ud_day
                    } else {
                        let rel_day = (ws_rem + s as u64) / DAY_MS_U64;
                        next_day_s = (rel_day + 1) * DAY_MS_U64 - ws_rem;
                        day0 + rel_day as i32
                    };
                    if day != ud_day {
                        if ud_sessions != 0 {
                            w.flush_user_day(id_bytes, categories, ud_day, ud_sessions, ud_events, ud_revenue);
                        }
                        ud_day = day;
                        ud_sessions = 0;
                        ud_events = 0;
                        ud_revenue = 0;
                        w.day_first.push((day, w.s_start.len() as u32));
                    }
                    let mut prev = s;
                    let mut k = i;
                    let mut rev = 0u64;
                    while k < ev_en {
                        let e = unsafe { *w.ev_buf.get_unchecked(k) };
                        let ets = e.ts();
                        if ets - prev > GAP_MS_U32 {
                            break;
                        }
                        prev = ets;
                        let cc = e.cc();
                        let cents = cc & 0x00FF_FFFF;
                        if cents != 0 {
                            rev += cents as u64;
                            let cat = cc >> 24;
                            let slot = unsafe { w.cat_sum.get_unchecked_mut(cat as usize) };
                            if *slot == 0 {
                                w.cat_touched.push(cat);
                            }
                            *slot += cents as u64;
                        }
                        k += 1;
                    }
                    let nev = (k - i) as u32;
                    i = k;

                    let s_ck = s + WEEK_MS_U32;
                    while cj < cl_end {
                        let ct = unsafe { w.cl_buf.get_unchecked(cj).ts };
                        if ct > s_ck {
                            break;
                        }
                        if !have_click || ct != last_ts {
                            last_ts = ct;
                            grp_first = cj;
                            have_click = true;
                        }
                        cj += 1;
                    }
                    let credit = if have_click && last_ts >= s {
                        unsafe { w.cl_buf.get_unchecked(grp_first).campaign }
                    } else {
                        organic
                    };
                    let cell = w.camp.cell(credit, day);
                    cell.sessions += 1;
                    cell.purchasing += (rev != 0) as i64;
                    cell.revenue += rev as i64;

                    ud_sessions += 1;
                    ud_events += nev;
                    ud_revenue += rev as i64;
                    w.s_start.push(s);
                }
                w.flush_user_day(id_bytes, categories, ud_day, ud_sessions, ud_events, ud_revenue);

                if cl_len > 0 {
                    // SAFETY: `cl_buf` is not mutated by `assists`.
                    let cl_ptr = w.cl_buf.as_ptr();
                    let pcl = unsafe { std::slice::from_raw_parts(cl_ptr.add(cl_st), cl_len) };
                    w.assists(pcl);
                }
                ev_st = ev_en;
            }
        },
        |w| {
            w.written = w.writer.finish();
        },
    );
    // Skip unmapping b_sinks/ck_sinks right before exit(0) (avoids 8-core munmap TLB shootdowns).
    std::mem::forget(b_sinks);
    std::mem::forget(ck_sinks);
    let np: usize = workers.iter().map(|w| w.np).sum();
    let nc: usize = workers.iter().map(|w| w.nc).sum();
    let user_rows_written: usize = workers.iter().map(|w| w.written).sum();
    if user_rows_written == 0 {
        output::write_empty_user(&ud).expect("writing empty user_daily");
    }
    let camp_rows = aggregate::merge_camps(workers.into_iter().map(|w| w.camp));
    tm.lap("group+agg");
    let t_cw = Instant::now();
    output::write_campaign_raw(&cd, &camp_rows, &ctx.campaigns).expect("writing campaign_daily");
    eprintln!(
        "[stage] campaign_write {:.3}s (user rows {user_rows_written}, campaign rows {})",
        t_cw.elapsed().as_secs_f64(),
        camp_rows.len()
    );
    eprintln!("[ingest] key={}B persons={np} events={ne} (raw {ne_raw}) clicks={nc}", std::mem::size_of::<K>());
    Ok(())
}

fn run<K: Key>(ctx: &Ctx, threads: usize, tm: &mut Timer) -> Result<Loaded, Need> {
    let Deduped { b_sinks, ck_sinks, p2, ne_raw } = ingest_to_sinks::<true, K>(ctx, threads, tm)?;
    let (ws, we) = (ctx.ws, ctx.we);

    // ---------------- Person grouping (written in place) ----------------
    let mut ev_base = vec![0usize; p2 + 1];
    for q in 0..p2 {
        ev_base[q + 1] = ev_base[q] + b_sinks.iter().map(|s| s.count(q)).sum::<usize>();
    }
    let ne = ev_base[p2];
    let mut events: Vec<Event> = Vec::with_capacity(ne);
    struct Part<K> {
        persons: Vec<K>,
        ev_cnt: Vec<u32>,
        clicks: Vec<Click>,
        ck_cnt: Vec<u32>,
    }
    let evp = SyncPtr(events.as_mut_ptr());
    let ck_base_ms = ws - WEEK_MS;
    // Dispatch largest P2 partitions first so whale/kiosk partitions don't form a tail.
    let mut q_order: Vec<usize> = (0..p2).collect();
    q_order.sort_unstable_by_key(|&q| std::cmp::Reverse(ev_base[q + 1] - ev_base[q]));
    let q_order = &q_order;
    let parts_out: Vec<Vec<(usize, Part<K>)>> = par::run(
        threads,
        p2,
        |_| (Vec::new(), Map::<K, u32>::with_capacity(1024), Vec::<u32>::new(), Vec::<(u32, u32, K, u32)>::new()),
        |(res, map, pid, cl), idx| {
            let q = q_order[idx];
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
    let mut parts: Vec<Option<Part<K>>> = (0..p2).map(|_| None).collect();
    for v in parts_out {
        for (q, p) in v {
            parts[q] = Some(p);
        }
    }
    let parts: Vec<Part<K>> = parts.into_iter().map(|p| p.unwrap()).collect();
    tm.lap("group");

    // ---------------- Offsets, clicks, person ids ----------------
    let mut pbase = vec![0usize; p2 + 1];
    let mut cbase = vec![0usize; p2 + 1];
    for q in 0..p2 {
        pbase[q + 1] = pbase[q] + parts[q].persons.len();
        cbase[q + 1] = cbase[q] + parts[q].clicks.len();
    }
    let (np, nc) = (pbase[p2], cbase[p2]);
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
        let names: Vec<Vec<(usize, Vec<u8>)>> = par::run(threads, p2, |_| Vec::new(), move |res, q| {
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
/// When `ALL_CAT == false`, `prod.get` is skipped for non-purchases (`view`/`cart`, ~97% of rows)
/// because their category is never used by daily outputs (`product_id` non-emptiness is still enforced).
#[inline(always)]
fn parse_event<const ALL_CAT: bool, K: Key>(
    dt: &DateTable,
    b: &[u8],
    f: &[(u32, u32); 7],
    ws: i64,
    we: i64,
    prod: &IdLookup,
    p1_bits: u32,
    sinks: &mut Sinks<RecA<K>>,
) -> Result<(), Need> {
    if flen(f[0]) == 0 {
        return Ok(());
    }
    let eid: K = fkey(b, f[0]).ok_or_else(|| key_need(f[0]))?;
    // Fast path: ingest_ts is guaranteed valid F1 (whole seconds).
    let (ingest, ingest_ms): (u32, Option<i64>) = match unsafe { dt.parse_f1_sec(fptr(b, f[3]), flen(f[3])) } {
        Some(sec) if sec >= 0 && sec < (u32::MAX as i64 - 1) => ((sec + 1) as u32, Some(sec * 1000)),
        Some(_) => return Err(Need::Slow),
        None => match fts(dt, b, f[3]) {
            Some(ms) if ms >= 0 && ms % 1000 == 0 && ms / 1000 < (u32::MAX as i64 - 1) => {
                ((ms / 1000 + 1) as u32, Some(ms))
            }
            Some(_) => return Err(Need::Slow),
            // Unparseable ingest_ts: match ref.py (+inf -> loses dedup, dropped as late if sole copy).
            None => (u32::MAX, None),
        },
    };
    let eid_h = eid.h();
    let mut rec = RecA { eid, user: K::default(), ts: 0, ingest, cc: INVALID_CC, eid_hash: eid_h as u32 };
    'v: {
        let ims = match ingest_ms {
            Some(v) => v,
            None => break 'v,
        };
        if flen(f[1]) == 0 || flen(f[5]) == 0 {
            break 'v;
        }
        let t = match fts(dt, b, f[2]) {
            Some(t) if t >= ws && t < we => t,
            _ => break 'v,
        };
        // Late check: ingest_ts - ts > 24h.
        if ims - t > ts::DAY_MS {
            break 'v;
        }
        let ty = f[4];
        let cents = if flen(ty) == 4 {
            let w = unsafe { std::ptr::read_unaligned(fptr(b, ty) as *const u32) };
            if (w == u32::from_le_bytes(*b"view")) | (w == u32::from_le_bytes(*b"cart")) {
                0
            } else {
                break 'v;
            }
        } else if flen(ty) == 8
            && unsafe { std::ptr::read_unaligned(fptr(b, ty) as *const u64) } == u64::from_le_bytes(*b"purchase")
        {
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
        rec.user = fkey(b, f[1]).ok_or_else(|| key_need(f[1]))?;
        rec.ts = (t - ws) as u32;
        let cat = if ALL_CAT || cents != 0 {
            unsafe { prod.get(fptr(b, f[5]), flen(f[5])) }
        } else {
            0
        };
        rec.cc = (cat << 24) | cents;
    }
    let p = (eid_h >> (64 - p1_bits)) as usize;
    sinks.push(p, rec);
    Ok(())
}

/// Linked-id resolution: id key -> canonical id key (only ids whose canonical differs).
/// Stored in ONE 2 MiB huge-page-backed flat partitioned table (zero `maps[p]` pointer
/// indirection, ~100% 2M dTLB hit rate even at 128 MB / 60M events!). Direct ring-32
/// prefetch beats a Bloom filter because ~55% of user IDs are linked (hits would suffer
/// unprefetched DRAM misses behind a Bloom check).
struct Links<K: Key> {
    has_links: bool,
    base: *mut (K, K),
    raw: (*mut u8, usize),
    part_bits: u32,
    part_mask: usize,
}
unsafe impl<K: Key> Send for Links<K> {}
unsafe impl<K: Key> Sync for Links<K> {}
impl<K: Key> Drop for Links<K> {
    fn drop(&mut self) {
        if self.raw.1 != 0 {
            par::huge_free(self.raw.0, self.raw.1);
        }
    }
}

impl<K: Key> Links<K> {
    fn empty() -> Links<K> {
        Links {
            has_links: false,
            base: std::ptr::null_mut(),
            raw: (std::ptr::null_mut(), 0),
            part_bits: 0,
            part_mask: 0,
        }
    }

    fn build(pairs: &[Vec<(K, K)>], threads: usize) -> Links<K> {
        let mut off = vec![0usize; pairs.len() + 1];
        for (i, v) in pairs.iter().enumerate() {
            off[i + 1] = off[i] + v.len();
        }
        let nl = off[pairs.len()];
        if nl == 0 {
            eprintln!("[ingest] links=0 linked_ids=0");
            return Links::empty();
        }
        let off = &off;

        // 1. Flat 2-pass count + scatter per thread (1 allocation per thread instead of 256 Vecs).
        struct Scattered<K> {
            flat: Vec<(K, u32)>,
            pos: [usize; LP + 1],
        }
        let scattered: Vec<Scattered<K>> = par::run(threads, pairs.len(), |_| Vec::new(), |res, t| {
            let mut cnt = [0usize; LP];
            for &(a, b) in &pairs[t] {
                cnt[(a.h() >> (64 - LP_BITS)) as usize] += 1;
                cnt[(b.h() >> (64 - LP_BITS)) as usize] += 1;
            }
            let mut pos = [0usize; LP + 1];
            for p in 0..LP {
                pos[p + 1] = pos[p] + cnt[p];
            }
            let mut cur = [0usize; LP];
            cur.copy_from_slice(&pos[..LP]);
            let mut flat: Vec<(K, u32)> = Vec::with_capacity(pos[LP]);
            unsafe { flat.set_len(pos[LP]) };
            let ptr = flat.as_mut_ptr();
            for (i, &(a, b)) in pairs[t].iter().enumerate() {
                let e = 2 * (off[t] + i) as u32;
                let pa = (a.h() >> (64 - LP_BITS)) as usize;
                let pb = (b.h() >> (64 - LP_BITS)) as usize;
                unsafe {
                    ptr.add(cur[pa]).write((a, e));
                    cur[pa] += 1;
                    ptr.add(cur[pb]).write((b, e + 1));
                    cur[pb] += 1;
                }
            }
            res.push(Scattered { flat, pos });
        })
        .into_iter()
        .flatten()
        .collect();

        // 2. Per partition: intern; write endpoint -> (partition, local id) into uninit `ep`.
        let mut ep: Vec<u64> = Vec::with_capacity(2 * nl);
        unsafe { ep.set_len(2 * nl) };
        let epp = SyncPtr(ep.as_mut_ptr());
        let scattered_ref = &scattered;
        let mut keys_parts: Vec<(usize, Vec<K>)> = par::run(
            threads,
            LP,
            |_| (Vec::new(), Map::<K, u32>::with_capacity(1024)),
            move |(res, m), p| {
                let n: usize = scattered_ref.iter().map(|s| s.pos[p + 1] - s.pos[p]).sum();
                m.reset(n);
                let mut keys: Vec<K> = Vec::with_capacity(n / 2 + 8);
                for s in scattered_ref.iter() {
                    for &(k, e) in &s.flat[s.pos[p]..s.pos[p + 1]] {
                        let (id, ins) = m.insert(k, keys.len() as u32);
                        if ins {
                            keys.push(k);
                        }
                        unsafe { epp.write(e as usize, ((p as u64) << 32) | id as u64) };
                    }
                }
                res.push((p, keys));
            },
        )
        .into_iter()
        .flat_map(|x| x.0)
        .collect();
        drop(scattered);
        keys_parts.sort_unstable_by_key(|x| x.0);
        let mut base = vec![0u32; LP + 1];
        for p in 0..LP {
            base[p + 1] = base[p] + keys_parts[p].1.len() as u32;
        }
        let nn = base[LP] as usize;

        // 3. Parallel copy of `keys` + parallel init of `parent` and `best` (zero serial overhead).
        let mut keys: Vec<K> = Vec::with_capacity(nn);
        let mut parent: Vec<AtomicU32> = Vec::with_capacity(nn);
        let mut best: Vec<AtomicU32> = Vec::with_capacity(nn);
        unsafe {
            keys.set_len(nn);
            parent.set_len(nn);
            best.set_len(nn);
        }
        {
            let (kp, pp, bp) = (SyncPtr(keys.as_mut_ptr()), SyncPtr(parent.as_mut_ptr()), SyncPtr(best.as_mut_ptr()));
            let (keys_parts_ref, base_ref) = (&keys_parts, &base);
            par::run(threads, LP, |_| (), move |_, p| {
                let s = base_ref[p] as usize;
                let src = &keys_parts_ref[p].1;
                unsafe {
                    kp.slice(s, src.len()).copy_from_slice(src);
                    for i in s..s + src.len() {
                        pp.write(i, AtomicU32::new(i as u32));
                        bp.write(i, AtomicU32::new(i as u32));
                    }
                }
            });
        }
        drop(keys_parts);

        // 4. Parallel lock-free union-find + smallest key per root.
        let node = |code: u64| base[(code >> 32) as usize] + code as u32;
        let step = 1 << 16;
        let (parent_ref, ep_ref, keys_ref, best_ref) = (&parent, &ep, &keys, &best);
        par::run(threads, nl.div_ceil(step), |_| (), |_, t| {
            for i in t * step..((t + 1) * step).min(nl) {
                let (a, b) = (node(ep_ref[2 * i]), node(ep_ref[2 * i + 1]));
                if a != b {
                    uf_union(parent_ref, a, b);
                }
            }
        });
        drop(ep);
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

        // 5. Collect (key, canonical) per partition (only id != canonical), then populate
        //    ONE contiguous 2 MiB huge-page flat partitioned table in parallel.
        let base_ref = &base;
        let canon_parts: Vec<(usize, Vec<(K, K)>)> = par::run(threads, LP, |_| Vec::new(), move |res, p| {
            let (s, e) = (base_ref[p] as usize, base_ref[p + 1] as usize);
            let mut canon: Vec<(K, K)> = Vec::with_capacity((e - s) * 3 / 4 + 8);
            for i in s..e {
                let r = uf_find(parent_ref, i as u32) as usize;
                let c = keys_ref[best_ref[r].load(Ordering::Relaxed) as usize];
                let k = keys_ref[i];
                if c != k {
                    canon.push((k, c));
                }
            }
            res.push((p, canon));
        })
        .into_iter()
        .flatten()
        .collect();
        let total_mapped: usize = canon_parts.iter().map(|x| x.1.len()).sum();
        eprintln!("[ingest] links={nl} linked_ids={nn} mapped={total_mapped}");
        if total_mapped == 0 {
            return Links::empty();
        }
        let max_cnt = canon_parts.iter().map(|x| x.1.len()).max().unwrap_or(0);
        // Keep load <= 50% (typically 29-39%) so probes almost never spill across a 64B cache line.
        let part_cap = (max_cnt * 2).next_power_of_two().max(16);
        let part_bits = part_cap.trailing_zeros();
        let part_mask = part_cap - 1;
        // Anonymous mmap is zero-filled by the OS, which equals (K::default(), K::default()) = (0, 0).
        let (a, raw_p, raw_l) = par::huge_alloc(LP * part_cap * std::mem::size_of::<(K, K)>());
        let flat_ptr = SyncPtr(a as *mut (K, K));
        let canon_ref = &canon_parts;
        par::run(threads, canon_ref.len(), |_| (), move |_, idx| {
            let (p, ref canon) = canon_ref[idx];
            let sl = unsafe { flat_ptr.slice(p << part_bits, part_cap) };
            for &(k, c) in canon {
                let mut s = (k.h() as usize) & part_mask;
                loop {
                    let dst = unsafe { sl.get_unchecked_mut(s) };
                    if dst.0.is_zero() {
                        *dst = (k, c);
                        break;
                    }
                    s = (s + 1) & part_mask;
                }
            }
        });
        Links { has_links: true, base: a as *mut (K, K), raw: (raw_p, raw_l), part_bits, part_mask }
    }

    #[inline(always)]
    fn canonical_h_hash(&self, k: K, h: u64) -> (K, u64) {
        let p = (h >> (64 - LP_BITS)) as usize;
        let pbase = p << self.part_bits;
        let mask = self.part_mask;
        let mut s = (h as usize) & mask;
        loop {
            let slot = unsafe { *self.base.add(pbase | s) };
            if slot.0 == k {
                return (slot.1, slot.1.h());
            }
            if slot.0.is_zero() {
                return (k, h);
            }
            s = (s + 1) & mask;
        }
    }

    #[inline(always)]
    fn prefetch_map_h(&self, h: u64) {
        let p = (h >> (64 - LP_BITS)) as usize;
        let idx = (p << self.part_bits) | ((h as usize) & self.part_mask);
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(self.base.add(idx) as *const i8);
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
        let (hi, lo) = if a > b { (a, b) } else { (b, a) };
        if p[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

/// Single-thread per-row cost breakdown on one events file (INGEST_BENCH2=<file>, DATA_DIR for products).
pub fn bench_rows(path: &Path, data_dir: &Path) {
    let (ws, we) = small::read_manifest(data_dir);
    let dt = DateTable::build(ws, we);
    let products = small::read_products(data_dir);
    let prod = IdLookup::build(products.map.iter().map(|(k, &v)| (k.as_slice(), v)), products.unknown);
    let data = std::fs::read(path).unwrap();
    let mut buf = data.clone();
    let hi = buf.len();
    buf.resize(hi + scan::PAD + 1, 0);
    let lo = data.iter().position(|&b| b == b'\n').unwrap() + 1;
    let mb = (hi - lo) as f64 / 1e6;
    let p1_bits = 10u32;
    let p1 = 1usize << p1_bits;
    let mut sinks: Sinks<RecA<u64>> = Sinks::new(p1, 4_500_000 / p1 * 2);
    let mut pieces_g = Vec::new();
    {
        let mut st = lo;
        while st < hi {
            let mut e = (st + (1 << 20)).min(hi);
            while e < hi && buf[e - 1] != b'\n' {
                e += 1;
            }
            pieces_g.push((st, e));
            st = e;
        }
    }
    for round in 0..2 {
        let mut line = String::new();
        let mut rows = 0usize;
        for stage in 0..5 {
            for l in sinks.len.iter_mut() {
                *l = 0;
            }
            let t = par::thread_cpu();
            let mut acc = 0i64;
            for &(plo, phi) in &pieces_g {
                scan::scan::<7, _>(&buf, plo, phi, |b, f| {
                    rows += (stage == 0) as usize;
                    match stage {
                        0 => acc += f[1].0 as i64,
                        1 => acc += fts(&dt, b, f[2]).unwrap_or(0) ^ unsafe { dt.parse_f1_sec(fptr(b, f[3]), flen(f[3])) }.unwrap_or(0),
                        2 => acc += fkey::<u64>(b, f[0]).unwrap_or(0) as i64 ^ fkey::<u64>(b, f[1]).unwrap_or(0) as i64,
                        3 => acc += unsafe { prod.get(fptr(b, f[5]), flen(f[5])) } as i64,
                        _ => {
                            let _ = parse_event::<false, u64>(&dt, b, f, ws, we, &prod, p1_bits, &mut sinks);
                        }
                    }
                });
            }
            let d = par::thread_cpu() - t;
            line += &format!(" s{stage} {:.1}ns/row ({:.0}MB/s){}", d * 1e9 / rows.max(1) as f64, mb / d, if acc == 1 { "!" } else { "" });
        }
        eprintln!("round {round} rows {rows}:{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radix_sort_matches_std() {
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut tmp = Vec::new();
        for &n in &[0usize, 1, 2, 15, 16, 17, 256, 257, 4096, 100_000] {
            let mut a: Vec<CompactEv> = (0..n)
                .map(|i| CompactEv::new((next() as u32) % 1_209_600_000, i as u32))
                .collect();
            let mut b = a.clone();
            sort_ev_slice(&mut a, &mut tmp);
            b.sort_by_key(|e| e.ts());
            assert!(a.windows(2).all(|w| w[0].ts() <= w[1].ts()));
            assert_eq!(a.iter().map(|e| e.ts()).collect::<Vec<_>>(), b.iter().map(|e| e.ts()).collect::<Vec<_>>());
            // Also test identical timestamps (all passes skipped) and full u32 range
            if n > 0 {
                let mut same = vec![CompactEv::new(42, 1); n];
                sort_ev_slice(&mut same, &mut tmp);
                assert!(same.iter().all(|e| e.ts() == 42));
                let mut full: Vec<CompactEv> = (0..n).map(|i| CompactEv::new(next() as u32, i as u32)).collect();
                let mut full_b = full.clone();
                sort_ev_slice(&mut full, &mut tmp);
                full_b.sort_unstable_by_key(|e| e.ts());
                assert_eq!(
                    full.iter().map(|e| e.ts()).collect::<Vec<_>>(),
                    full_b.iter().map(|e| e.ts()).collect::<Vec<_>>()
                );
            }
        }
    }
}
