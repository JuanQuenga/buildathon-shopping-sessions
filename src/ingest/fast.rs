//! Parallel exact ingest, generic over the id key width (see keys.rs).
//! `load` tries compact u64 keys, then wide u128 keys, and returns `None` (the
//! caller then uses the generic slow path) if neither can carry the input exactly.
//! Pipeline (docs/ingest.md):
//!   links  : parse -> partitioned interning -> parallel union-find -> canonical pairs
//!   parse  : chunked pread + direct compact row parsing; event tokens -> P1 by
//!            event id; valid events and clicks -> P2 by raw person
//!   dedup  : per P1 partition, exact open addressing marks loser locations in P2
//!   relocate: move surviving linked rows to their canonical P2 partition
//!   group  : per P2 partition, process live rows, build CSR and sort by timestamp
//! The wide-key and Loaded paths use a two-pass dedup with canonicalization after
//! winner selection.
use super::key::IdLookup;
use super::keys::{Key, Map};
use super::par::{self, ArenaVec, Scratch, SinkArena, Sinks, SyncPtr, Timer};
use super::scan::{self, Chunk, FileInfo, Kind};
use super::tsfast::{self, DateTable};
use super::{slow, small, ts};
use crate::aggregate::{self, output, CampStore, NO_CATEGORY};
use crate::types::*;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, Scope, ScopedJoinHandle};
use std::time::Instant;

#[path = "direct.rs"]
mod direct;

const WEEK_MS: i64 = 7 * ts::DAY_MS;
const WEEK_MS_U32: u32 = WEEK_MS as u32;
const GAP_MS_U32: u32 = 1_800_000;
const DAY_MS_U32: u32 = ts::DAY_MS as u32;
const CHUNK: u64 = 256 << 10;
const LP_BITS: u32 = 8;
const LP: usize = 1 << LP_BITS;
const INVALID_CAMP: u32 = u32::MAX;
const INVALID_CC: u32 = u32::MAX;
const MAX_THREADS: usize = 32;

/// Why the fast path gave up.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Need {
    TwoPass = 1,
    Wide = 2,
    Slow = 3,
}

struct Flag(AtomicU8);
impl Flag {
    fn set(&self, n: Need) {
        self.0.fetch_max(n as u8, Ordering::Relaxed);
    }
    fn get(&self) -> Option<Need> {
        match self.0.load(Ordering::Relaxed) {
            0 => None,
            1 => Some(Need::TwoPass),
            2 => Some(Need::Wide),
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

struct Lookups {
    categories: Vec<Vec<u8>>,
    campaigns: Vec<(Vec<u8>, Vec<u8>)>,
    prod: IdLookup,
    camp: IdLookup,
}

enum LookupTask<'scope> {
    Pending(Option<ScopedJoinHandle<'scope, Lookups>>),
    Ready(Lookups),
}

impl LookupTask<'_> {
    fn wait(&mut self) -> &Lookups {
        if let Self::Pending(worker) = self {
            *self = Self::Ready(worker.take().unwrap().join().expect("read lookup tables"));
        }
        self.get()
    }

    fn get(&self) -> &Lookups {
        match self {
            Self::Ready(lookups) => lookups,
            Self::Pending(_) => unreachable!("lookup tables must be joined before parsing"),
        }
    }
}

/// Everything independent of the key width.
struct Ctx<'scope> {
    ws: i64,
    we: i64,
    dt: DateTable,
    lookups: LookupTask<'scope>,
    files: Arc<Vec<FileInfo>>,
    prefetch: par::Prefetch,
    link_chunks: Vec<Chunk>,
    main_chunks: Vec<Chunk>,
    event_bytes: u64,
    click_bytes: u64,
    readers: Vec<Mutex<Vec<u8>>>,
    parse_threads: usize,
    p1_bits_override: Option<u32>,
}

#[inline(always)]
fn scan_chunk<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(
    files: &[FileInfo],
    c: Chunk,
    buf: &mut Vec<u8>,
    f: F,
) {
    let fi = &files[c.file as usize];
    let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, buf);
    if lo < hi {
        scan::scan::<N, F>(buf, lo, hi, f);
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse::<usize>().ok())
}

fn build_ctx<'scope, 'env>(
    data_dir: &'env Path,
    threads: usize,
    scope: &'scope Scope<'scope, 'env>,
    tm: &mut Timer,
) -> Option<Ctx<'scope>> {
    let (ws, we) = small::read_manifest(data_dir);
    // Representation limits of the compact records (else: generic path).
    if we < ws || we.checked_sub(ws)? > u32::MAX as i64 - WEEK_MS {
        return None;
    }
    let files = Arc::new(scan::open_csv_files(data_dir));
    let dt = DateTable::build(ws, we);
    let lookups = LookupTask::Pending(Some(scope.spawn(|| {
        let (categories, _, prod) = small::read_products_lookup(data_dir);
        let (campaigns, camp) = small::read_campaigns_lookup(data_dir);
        Lookups { categories, campaigns, prod, camp }
    })));
    let prefetch = par::Prefetch::start(&files, threads);
    let chunk_bytes = env_usize("INGEST_CHUNK_KB")
        .map(|kb| (kb.clamp(32, 4096) as u64) << 10)
        .unwrap_or(CHUNK);
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
        lookups,
        files,
        prefetch,
        link_chunks,
        main_chunks,
        event_bytes,
        click_bytes,
        readers: (0..threads.max(parse_threads.clamp(1, MAX_THREADS)))
            .map(|_| Mutex::new(Vec::with_capacity(chunk_bytes as usize + 8192))).collect(),
        parse_threads,
        p1_bits_override,
    };
    tm.lap("small");
    Some(ctx)
}

/// Fused end-to-end fast path: reads CSVs, deduplicates, groups+aggregates per P2
/// partition in L2-resident compact buffers, and writes Parquet directly without
/// materializing `Loaded`. The caller keeps any returned parse mapping alive
/// through the exit diagnostics. An error selects the generic path.
pub fn run_to_output(data_dir: &Path, output_dir: &Path, threads: usize) -> Result<Option<SinkArena>, ()> {
    let threads = threads.clamp(1, MAX_THREADS);
    thread::scope(|scope| {
        let mut tm = Timer::new("ingest");
        let Some(mut ctx) = build_ctx(data_dir, threads, scope, &mut tm) else {
            return Err(());
        };

        let plan = PartitionPlan::new(&ctx, threads);
        match ingest_compact(&mut ctx, threads, &plan, &mut tm) {
            Ok(deduped) => Ok(run_fused(&ctx, deduped, output_dir, threads, &mut tm)),
            Err(Need::Wide) => {
                eprintln!("[ingest] compact keys not applicable: retrying with wide keys");
                match ingest_to_sinks::<false, u128>(&mut ctx, threads, &plan, &mut tm) {
                    Ok(deduped) => Ok(run_fused(&ctx, deduped, output_dir, threads, &mut tm)),
                    Err(_) => Err(()),
                }
            }
            Err(Need::Slow | Need::TwoPass) => Err(()),
        }
    })
}

fn ingest_compact(
    ctx: &mut Ctx<'_>,
    threads: usize,
    plan: &PartitionPlan,
    tm: &mut Timer,
) -> Result<Deduped<u64>, Need> {
    if threads <= 16 && plan.parse_threads <= 16 && plan.p2_bits <= 10 {
        match ingest_single_pass(ctx, threads, plan, tm) {
            Err(Need::TwoPass) => {
                eprintln!("[ingest] event location overflow: retrying with two passes");
            }
            result => return result,
        }
    }
    ingest_to_sinks::<false, u64>(ctx, threads, plan, tm)
}

pub fn load(data_dir: &Path, threads: usize) -> Option<Loaded> {
    let threads = threads.clamp(1, MAX_THREADS);
    thread::scope(|scope| {
        let mut tm = Timer::new("ingest");
        let mut ctx = build_ctx(data_dir, threads, scope, &mut tm)?;
        match run::<u64>(&mut ctx, threads, &mut tm) {
            Ok(l) => Some(l),
            Err(Need::Wide) => {
                eprintln!("[ingest] compact keys not applicable: retrying with wide keys");
                run::<u128>(&mut ctx, threads, &mut tm).ok()
            }
            Err(Need::Slow | Need::TwoPass) => None,
        }
    })
}

struct Deduped<K: Key> {
    b_sinks: Vec<Sinks<RecB<K>>>,
    ck_sinks: Vec<Sinks<RecC<K>>>,
    p2: usize,
    ne_raw: usize,
    dead: Option<DeadEvents>,
    relocated: Option<Relocated<K>>,
    parse_mapping: Option<SinkArena>,
    group_scratch: Vec<SinkArena>,
}

type CanonicalPart<'a, K> = (usize, ArenaVec<'a, (K, K)>);

fn read_links<'a, K: Key, L>(
    ctx: &Ctx<'_>, threads: usize, tm: &mut Timer, scratch: &mut Scratch<'a>,
    build: impl FnOnce(Vec<CanonicalPart<'a, K>>) -> L,
) -> Result<L, Need> {
    let flag = Flag(AtomicU8::new(0));
    let files = &ctx.files;
    let link_chunks = &ctx.link_chunks;
    let link_pairs: Vec<Vec<(K, K)>> = par::run(
        threads,
        link_chunks.len(),
        |tid| (Vec::<(K, K)>::new(), std::mem::take(&mut *ctx.readers[tid].lock().unwrap())),
        |(pairs, buf), i| {
            if flag.get().is_some() {
                return;
            }
            let c = link_chunks[i];
            scan_chunk::<2, _>(files, c, buf, |b, f| {
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
    .enumerate()
    .map(|(tid, (pairs, buf))| {
        *ctx.readers[tid].lock().unwrap() = buf;
        pairs
    })
    .collect();
    if let Some(n) = flag.get() {
        return Err(n);
    }
    tm.lap("links-parse");
    let canonical = Links::canonical_pairs(&link_pairs, threads, scratch);
    drop(link_pairs);
    let links = build(canonical);
    tm.lap("links-uf");

    // ---------------- Events + clicks parse ----------------
    Ok(links)
}

struct PartitionPlan {
    parse_threads: usize,
    p1_bits: u32,
    p2_bits: u32,
    p1: usize,
    p2: usize,
    cap_ev: usize,
    cap_ck: usize,
}

impl PartitionPlan {
    fn new(ctx: &Ctx<'_>, threads: usize) -> Self {
        let est_ev = (ctx.event_bytes / 75) as usize + 1024;
        let est_ck = (ctx.click_bytes / 40) as usize + 1024;
        let parse_threads = if ctx.parse_threads > 0 { ctx.parse_threads.clamp(1, MAX_THREADS) } else { threads };
        // Keep the write buffers small enough for sibling threads sharing L2.
        let p1_bits = ctx.p1_bits_override.unwrap_or(if est_ev <= 4_000_000 { 8 } else if est_ev <= 30_000_000 { 9 } else { 11 });
        let p2_bits = env_usize("INGEST_P2_BITS").map(|b| b.clamp(8, 10) as u32)
            .unwrap_or(if est_ev <= 30_000_000 { 8 } else { 10 });
        let p1 = 1 << p1_bits;
        let p2 = 1 << p2_bits;
        Self {
            parse_threads, p1_bits, p2_bits, p1, p2,
            cap_ev: est_ev * 112 / 100 / parse_threads / p1 + 24,
            cap_ck: est_ck * 112 / 100 / parse_threads / p2 + 24,
        }
    }
}

struct ParseRows<'a, K: Key> {
    dt: &'a DateTable,
    ws: i64,
    we: i64,
    lookups: &'a Lookups,
    plan: &'a PartitionPlan,
    flag: &'a Flag,
    links: Option<&'a Links<K>>,
}

impl<K: Key> ParseRows<'_, K> {
    #[inline(always)]
    fn clicks_base(&self) -> i64 {
        self.ws - WEEK_MS
    }

    #[inline(always)]
    fn p2_shift(&self) -> u32 {
        64 - self.plan.p2_bits
    }
}

trait EventSink<K: Key>: Send {
    type Clicks: ClickSink<K>;
    fn finish(&mut self);
    fn parse_rows<const ALL_CAT: bool>(
        &mut self, clicks: &mut Self::Clicks, rows: &ParseRows<'_, K>,
        kind: Kind, buf: &[u8], lo: usize, hi: usize,
    );
}

impl<K: Key> EventSink<K> for Sinks<RecA<K>> {
    type Clicks = LinkedClicks<K>;

    fn finish(&mut self) {
        Sinks::finish(self);
    }

    fn parse_rows<const ALL_CAT: bool>(
        &mut self, clicks: &mut Self::Clicks, rows: &ParseRows<'_, K>,
        kind: Kind, buf: &[u8], lo: usize, hi: usize,
    ) {
        match kind {
            Kind::Events => scan::scan::<7, _>(buf, lo, hi, |b, f| {
                if let Err(n) = parse_event::<ALL_CAT, K>(
                    rows.dt, b, f, rows.ws, rows.we, &rows.lookups.prod,
                    |eid, h, ingest, user, ts, cc| {
                        let p = (h >> (64 - rows.plan.p1_bits)) as usize;
                        self.push(p, RecA { eid, user, ts, ingest, cc, eid_hash: h as u32 });
                    },
                ) {
                    rows.flag.set(n);
                }
            }),
            Kind::Clicks => scan::scan::<4, _>(buf, lo, hi, |b, f| {
                match parse_click(rows, b, f) {
                    Ok(Some(rec)) => clicks.push_click(rec, rows.links, rows.p2_shift()),
                    Ok(None) => {}
                    Err(n) => rows.flag.set(n),
                }
            }),
            Kind::Links => unreachable!(),
        }
    }
}

trait ClickSink<K: Key>: Send {
    fn push_click(&mut self, rec: RecC<K>, links: Option<&Links<K>>, p2_shift: u32);
    fn finish(&mut self, links: Option<&Links<K>>, p2_shift: u32);
    fn into_sinks(self) -> Sinks<RecC<K>>;
}

impl<K: Key> ClickSink<K> for Sinks<RecC<K>> {
    #[inline(always)]
    fn push_click(&mut self, rec: RecC<K>, _: Option<&Links<K>>, p2_shift: u32) {
        self.push((rec.person.h() >> p2_shift) as usize, rec);
    }

    fn finish(&mut self, _: Option<&Links<K>>, _: u32) {
        Sinks::finish(self);
    }

    fn into_sinks(self) -> Sinks<RecC<K>> {
        self
    }
}

const CK_RING: usize = 32;

struct LinkedClicks<K: Key> {
    out: Sinks<RecC<K>>,
    ring: [(RecC<K>, u64); CK_RING],
    count: usize,
}

impl<K: Key> LinkedClicks<K> {
    #[inline(always)]
    fn resolve(&mut self, (mut rec, hash): (RecC<K>, u64), links: &Links<K>, shift: u32) {
        let (person, ph) = links.canonical_h_hash(rec.person, hash);
        rec.person = person;
        self.out.push((ph >> shift) as usize, rec);
    }
}

impl<K: Key> LinkedClicks<K> {
    fn new(plan: &PartitionPlan) -> Self {
        let blank = RecC { person: K::default(), cid: K::default(), ts: 0, campaign: 0 };
        Self { out: Sinks::new(plan.p2, plan.cap_ck), ring: [(blank, 0); CK_RING], count: 0 }
    }
}

impl<K: Key> ClickSink<K> for LinkedClicks<K> {

    #[inline(always)]
    fn push_click(&mut self, rec: RecC<K>, links: Option<&Links<K>>, p2_shift: u32) {
        let hash = rec.person.h();
        if let Some(links) = links {
            links.prefetch_map_h(hash);
            let slot = self.count & (CK_RING - 1);
            if self.count >= CK_RING {
                self.resolve(self.ring[slot], links, p2_shift);
            }
            self.ring[slot] = (rec, hash);
            self.count += 1;
        } else {
            self.out.push((hash >> p2_shift) as usize, rec);
        }
    }

    fn finish(&mut self, links: Option<&Links<K>>, p2_shift: u32) {
        if let Some(links) = links {
            for i in self.count.saturating_sub(CK_RING)..self.count {
                self.resolve(self.ring[i & (CK_RING - 1)], links, p2_shift);
            }
        }
        self.out.finish();
    }

    fn into_sinks(self) -> Sinks<RecC<K>> {
        self.out
    }
}

type Parsed<K, E> = Vec<(E, Sinks<RecC<K>>)>;

fn parse_sinks<const ALL_CAT: bool, K: Key, E: EventSink<K>>(
    ctx: &mut Ctx<'_>,
    plan: &PartitionPlan,
    links: Option<&Links<K>>,
    init: impl Fn(usize) -> (E, E::Clicks) + Sync,
    tm: &mut Timer,
) -> Result<Parsed<K, E>, Need> {
    let flag = Flag(AtomicU8::new(0));
    let (ws, we) = (ctx.ws, ctx.we);
    let dt = &ctx.dt;
    let files = &ctx.files;
    let lookups = ctx.lookups.wait();
    if lookups.categories.len() > 256 {
        return Err(Need::Slow);
    }
    tm.lap("small-wait");
    let main_chunks = &ctx.main_chunks;
    let p2_shift = 64 - plan.p2_bits;
    let parse_threads = plan.parse_threads;
    let links = links.filter(|l| l.has_links);
    let rows = ParseRows { dt, ws, we, lookups, plan, flag: &flag, links };

    struct ParseState<E, C> {
        ev: E,
        ck: C,
        buf: Vec<u8>,
    }
    let mut states = par::run_fini(
        parse_threads,
        main_chunks.len(),
        |tid| {
            let (ev, ck) = init(tid);
            ParseState { ev, ck, buf: std::mem::take(&mut *ctx.readers[tid].lock().unwrap()) }
        },
        |st, i| {
            if flag.get().is_some() {
                return;
            }
            let c = main_chunks[i];
            let fi = &files[c.file as usize];
            let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, &mut st.buf);
            st.ev.parse_rows::<ALL_CAT>(&mut st.ck, &rows, fi.kind, &st.buf, lo, hi);
        },
        |st| {
            st.ev.finish();
            st.ck.finish(links, p2_shift);
        },
    );
    ctx.prefetch.stop();
    for (tid, state) in states.iter_mut().enumerate() {
        *ctx.readers[tid].lock().unwrap() = std::mem::take(&mut state.buf);
    }
    if let Some(n) = flag.get() {
        par::drop_par(parse_threads, states);
        return Err(n);
    }
    Ok(states.into_iter().map(|s| (s.ev, s.ck.into_sinks())).collect())
}

fn ingest_to_sinks<const ALL_CAT: bool, K: Key>(
    ctx: &mut Ctx<'_>,
    threads: usize,
    plan: &PartitionPlan,
    tm: &mut Timer,
) -> Result<Deduped<K>, Need> {
    let links = read_links(ctx, threads, tm, &mut Scratch::default(), |pairs| Links::<K>::from_pairs(&pairs, threads))?;
    let parsed = parse_sinks::<ALL_CAT, K, Sinks<RecA<K>>>(ctx, plan, Some(&links), |_| {
        (Sinks::new(plan.p1, plan.cap_ev), LinkedClicks::new(plan))
    }, tm);
    tm.lap("parse");
    let (ev_sinks, ck_sinks): (Vec<_>, Vec<_>) = parsed?.into_iter().unzip();
    let PartitionPlan { p1, p2, p2_bits, .. } = *plan;

    // ---------------- Event dedup ----------------
    let ne_raw: usize = (0..p1).map(|p| ev_sinks.iter().map(|s| s.count(p)).sum::<usize>()).sum();
    let cap_b = ne_raw * 112 / 100 / threads / p2 + 32;
    struct DedupState<K: Key> {
        table: DedupTable,
        dead: Vec<u64>,
        out: Sinks<RecB<K>>,
    }
    let b_sinks: Vec<Sinks<RecB<K>>> = par::run_fini(
        threads,
        p1,
        |_| DedupState {
            table: DedupTable::default(),
            dead: Vec::new(),
            out: Sinks::<RecB<K>>::new(p2, cap_b),
        },
        |st, p| {
            let mut slices = [(&[][..], 0); MAX_THREADS * 2];
            let (active, total) = partition_slices(&ev_sinks, p, &mut slices);
            let dead_words = total.div_ceil(64);
            if st.dead.len() < dead_words {
                st.dead.resize(dead_words, 0);
            }
            let dead = &mut st.dead[..dead_words];
            dead.fill(0);
            st.table.mark_losers(active, total, |idx, _| {
                dead[(idx >> 6) as usize] |= 1u64 << (idx & 63);
            });

            if total == 0 {
                return;
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
    par::drop_par(threads, ev_sinks);
    drop(links);
    tm.lap("dedup");
    Ok(Deduped { b_sinks, ck_sinks, p2, ne_raw, dead: None, relocated: None, parse_mapping: None, group_scratch: Vec::new() })
}

fn partition_slices<'a, 's, R: Copy>(
    sinks: &'a [Sinks<R>], p: usize, slices: &'s mut [(&'a [R], u32); MAX_THREADS * 2],
) -> (&'s [(&'a [R], u32)], usize) {
    let mut n = 0;
    let mut total = 0;
    for sink in sinks {
        let (arena, overflow) = sink.part(p);
        for slice in [arena, overflow] {
            if !slice.is_empty() {
                slices[n] = (slice, total as u32);
                total += slice.len();
                n += 1;
            }
        }
    }
    (&slices[..n], total)
}

trait Candidate {
    type Id: Key;
    fn id(&self) -> Self::Id;
    fn hash32(&self) -> u32;
    fn ingest(&self) -> u32;
}

impl<K: Key> Candidate for RecA<K> {
    type Id = K;
    #[inline(always)]
    fn id(&self) -> K { self.eid }
    #[inline(always)]
    fn hash32(&self) -> u32 { self.eid_hash }
    #[inline(always)]
    fn ingest(&self) -> u32 { self.ingest }
}

#[derive(Default)]
struct DedupTable {
    t32: Vec<u32>,
    t64: Vec<u64>,
}

impl DedupTable {
    fn mark_losers<R: Candidate>(
        &mut self, active: &[(&[R], u32)], total: usize, mut lose: impl FnMut(u32, &R),
    ) {
        if total == 0 {
            return;
        }
        let nsl = active.len();
        let get_flat = |idx: u32| -> &R {
            // Usually <= 8 slices; linear scan with cumulative base is tiny and branch-predictable.
            let mut i = nsl - 1;
            while i > 0 && idx < active[i].1 {
                i -= 1;
            }
            let (sl, base) = active[i];
            unsafe { sl.get_unchecked((idx - base) as usize) }
        };
        let cap = (total * 2).next_power_of_two();
        let mask = cap - 1;

        if total < (1 << 18) - 1 {
            if self.t32.len() < cap {
                self.t32.resize(cap, 0);
            }
            let table = &mut self.t32[..cap];
            table.fill(0);
            let mut flat = 0u32;
            for &(sl, _) in active {
                for r in sl {
                    let h = r.hash32();
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
                            if w.id() == r.id() {
                                if r.ingest() < w.ingest() {
                                    lose(prev, w);
                                    unsafe {
                                        *table.get_unchecked_mut(slot) = (fp << 18) | (flat + 1);
                                    }
                                } else {
                                    lose(flat, r);
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
            if self.t64.len() < cap {
                self.t64.resize(cap, 0);
            }
            let table = &mut self.t64[..cap];
            table.fill(0);
            let mut flat = 0u32;
            for &(sl, _) in active {
                for r in sl {
                    let h = r.id().h();
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
                            if w.id() == r.id() {
                                if r.ingest() < w.ingest() {
                                    lose(prev, w);
                                    unsafe {
                                        *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (flat as u64 + 1);
                                    }
                                } else {
                                    lose(flat, r);
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
    }
}

// A location uses 4 worker bits, 10 person-partition bits, and 18 index bits.
// Reserving the largest index keeps the all-ones invalid-row sentinel distinct.
const MAX_LOC_IDX: usize = (1 << 18) - 1;
const NO_REC: u32 = u32::MAX;
#[repr(C, align(16))]
#[derive(Clone, Copy)]
struct Tok {
    eid: u64,
    ingest: u32,
    loc: u32,
}

impl Candidate for Tok {
    type Id = u64;
    #[inline(always)]
    fn id(&self) -> u64 { self.eid }
    #[inline(always)]
    fn hash32(&self) -> u32 { self.eid.h() as u32 }
    #[inline(always)]
    fn ingest(&self) -> u32 { self.ingest }
}

struct SinglePass {
    tok: Sinks<Tok>,
    b: Sinks<RecB<u64>>,
    tid_bits: u32,
}

struct SinglePassEvents<'a> {
    out: &'a mut SinglePass,
    plan: &'a PartitionPlan,
    flag: &'a Flag,
}

impl direct::EventOutput for SinglePassEvents<'_> {
    #[inline(always)]
    fn valid(&mut self, eid: u64, ingest: u32, user: u64, ts: u32, cc: u32) {
        let p = (eid.h() >> (64 - self.plan.p1_bits)) as usize;
        let q = (user.h() >> (64 - self.plan.p2_bits)) as usize;
        let idx = self.out.b.push_idx(q, RecB { person: user, ts, cc });
        if idx >= MAX_LOC_IDX {
            self.flag.set(Need::TwoPass);
            return;
        }
        let loc = self.out.tid_bits | ((q as u32) << 18) | idx as u32;
        self.out.tok.push(p, Tok { eid, ingest, loc });
    }

    #[inline(always)]
    fn invalid(&mut self, eid: u64, ingest: u32) {
        let p = (eid.h() >> (64 - self.plan.p1_bits)) as usize;
        self.out.tok.push(p, Tok { eid, ingest, loc: NO_REC });
    }
}

impl EventSink<u64> for SinglePass {
    type Clicks = Sinks<RecC<u64>>;

    fn parse_rows<const ALL_CAT: bool>(
        &mut self, clicks: &mut Self::Clicks, rows: &ParseRows<'_, u64>,
        kind: Kind, buf: &[u8], lo: usize, hi: usize,
    ) {
        debug_assert!(!ALL_CAT && rows.links.is_none());
        match kind {
            Kind::Events => direct::events(rows, buf, lo, hi, &mut SinglePassEvents {
                out: self, plan: rows.plan, flag: rows.flag,
            }),
            Kind::Clicks => direct::clicks(rows, buf, lo, hi, |rec| {
                clicks.push_click(rec, None, rows.p2_shift());
            }),
            Kind::Links => unreachable!(),
        }
    }

    fn finish(&mut self) {
        self.tok.finish();
        self.b.finish();
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
struct LinkEntries {
    keys: [u64; 4],
    canon: [u64; 4],
}

#[derive(Default)]
struct RelocationMap {
    buckets: Vec<LinkEntries>,
    mask: usize,
}

impl RelocationMap {
    fn reset(&mut self, n: usize) {
        let len = n.div_ceil(2).next_power_of_two();
        if self.buckets.len() < len {
            self.buckets.resize(len, LinkEntries::default());
        }
        self.buckets[..len].fill(LinkEntries::default());
        self.mask = len - 1;
    }

    fn insert(&mut self, key: u64, person: u64) {
        let mut slot = key.h() as usize & self.mask;
        loop {
            let bucket = &mut self.buckets[slot];
            if let Some(lane) = bucket.keys.iter().position(|&k| k == 0) {
                bucket.keys[lane] = key;
                bucket.canon[lane] = person;
                return;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn prefetch_slot(&self, key: u64) -> usize {
        let slot = key.h() as usize & self.mask;
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T1 }>(
                self.buckets.as_ptr().add(slot).cast(),
            );
        }
        slot
    }

    #[inline(always)]
    fn get(&self, key: u64) -> Option<u64> {
        self.get_from_slot(key, key.h() as usize & self.mask)
    }

    #[inline(always)]
    fn get_from_slot(&self, key: u64, mut slot: usize) -> Option<u64> {
        use std::arch::x86_64::*;
        let needle = unsafe { _mm256_set1_epi64x(key as i64) };
        loop {
            let bucket = unsafe { self.buckets.get_unchecked(slot) };
            let keys = unsafe { _mm256_load_si256(bucket.keys.as_ptr().cast()) };
            let hits = unsafe { _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(keys, needle))) } as u32;
            if hits != 0 {
                return Some(unsafe { *bucket.canon.get_unchecked(hits.trailing_zeros() as usize) });
            }
            if unsafe { _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(keys, _mm256_setzero_si256()))) } != 0 {
                return None;
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

struct BucketLinks<'a> {
    entries: ArenaVec<'a, (u64, u64)>,
    offsets: Vec<usize>,
}

impl<'a> BucketLinks<'a> {
    fn build(mut parts: Vec<CanonicalPart<'_, u64>>, p2_bits: u32, threads: usize, scratch: &mut Scratch<'a>) -> Self {
        let split = 1 << (p2_bits - LP_BITS);
        let shift = 64 - p2_bits;
        let mut offsets = vec![0; (1 << p2_bits) + 1];
        let mut entries = ArenaVec::default();
        if !parts.is_empty() {
            parts.sort_unstable_by_key(|part| part.0);
            for (p, pairs) in &parts {
                if split == 1 {
                    offsets[p + 1] = pairs.len();
                } else {
                    for &(key, _) in pairs.iter() {
                        offsets[((key.h() >> shift) as usize) + 1] += 1;
                    }
                }
            }
            for q in 0..offsets.len() - 1 {
                offsets[q + 1] += offsets[q];
            }
            let len = *offsets.last().unwrap();
            entries = scratch.buffer(len);
            let dst = SyncPtr(entries.as_mut_ptr());
            par::run(threads, parts.len(), |_| (), |_, i| {
                let (p, pairs) = &parts[i];
                let first = p * split;
                if split == 1 {
                    for (i, &pair) in pairs.iter().enumerate() {
                        unsafe { dst.write(offsets[first] + i, pair) };
                    }
                } else {
                    // The union-find already grouped keys by the high LP_BITS.
                    let mut next = [0; 4];
                    next[..split].copy_from_slice(&offsets[first..first + split]);
                    for &(key, canon) in pairs.iter() {
                        let bin = ((key.h() >> shift) as usize) & (split - 1);
                        unsafe { dst.write(next[bin], (key, canon)) };
                        next[bin] += 1;
                    }
                }
            });
            // Each worker filled its disjoint part of the array before returning.
            unsafe { entries.set_len(len) };
        }
        Self { entries, offsets }
    }

    fn bucket(&self, q: usize) -> &[(u64, u64)] {
        &self.entries[self.offsets[q]..self.offsets[q + 1]]
    }
}

trait DeadWord: Default + Send + Sync {
    fn load(&self) -> u64;
    fn exclusive(&mut self) -> &mut u64;
}

impl DeadWord for AtomicU64 {
    #[inline(always)]
    fn load(&self) -> u64 { self.load(Ordering::Relaxed) }
    fn exclusive(&mut self) -> &mut u64 { self.get_mut() }
}

impl DeadWord for u64 {
    #[inline(always)]
    fn load(&self) -> u64 { *self }
    fn exclusive(&mut self) -> &mut u64 { self }
}

struct DeadRows<W> {
    sinks: usize,
    offsets: Vec<usize>,
    bits: Vec<W>,
}

type DeadEvents = DeadRows<AtomicU64>;
type DeadClicks = DeadRows<u64>;

impl<W: DeadWord> DeadRows<W> {
    fn new<T: Copy>(sinks: &[Sinks<T>], p2: usize, threads: usize) -> Self {
        let mut offsets = Vec::with_capacity(sinks.len() * p2 + 1);
        let mut len = 0;
        for q in 0..p2 {
            for sink in sinks {
                offsets.push(len);
                len += sink.count(q).div_ceil(64);
            }
        }
        offsets.push(len);
        let mut bits = Vec::with_capacity(len);
        let ptr = SyncPtr(bits.as_mut_ptr());
        const WORDS_PER_TASK: usize = 8192;
        par::run(threads, len.div_ceil(WORDS_PER_TASK), |_| (), |_, i| {
            let start = i * WORDS_PER_TASK;
            for j in start..(start + WORDS_PER_TASK).min(len) {
                unsafe { ptr.write(j, W::default()) };
            }
        });
        unsafe { bits.set_len(len) };
        Self { sinks: sinks.len(), offsets, bits }
    }

    fn words(&self, tid: usize, q: usize) -> &[W] {
        let part = q * self.sinks + tid;
        &self.bits[self.offsets[part]..self.offsets[part + 1]]
    }
}

impl DeadEvents {
    #[inline]
    fn mark(&self, loc: u32) {
        if loc != NO_REC {
            let tid = (loc >> 28) as usize;
            let q = ((loc >> 18) & 1023) as usize;
            let idx = loc as usize & MAX_LOC_IDX;
            let off = self.offsets[q * self.sinks + tid];
            self.bits[off + (idx >> 6)].fetch_or(1u64 << (idx & 63), Ordering::Relaxed);
        }
    }
}

struct Relocated<K: Key> {
    events: Vec<Sinks<RecB<K>>>,
    clicks: Vec<Sinks<RecC<K>>>,
    dead_clicks: DeadClicks,
}

fn ingest_single_pass(
    ctx: &mut Ctx<'_>, threads: usize, plan: &PartitionPlan, tm: &mut Timer,
) -> Result<Deduped<u64>, Need> {
    let cap_b = (plan.cap_ev - 24) * plan.p1 / plan.p2 + 32;
    let tok_bytes = Sinks::<Tok>::arena_bytes(plan.p1, plan.cap_ev);
    let event_bytes = Sinks::<RecB<u64>>::arena_bytes(plan.p2, cap_b);
    let click_bytes = Sinks::<RecC<u64>>::arena_bytes(plan.p2, plan.cap_ck);
    let workers = plan.parse_threads;
    let parse_bytes = (tok_bytes + event_bytes + click_bytes) * workers;
    // A typical compact link occupies about as many CSV bytes as its
    // canonical pair. Unusually dense files can fall back to the heap.
    let link_bytes = ctx.files.iter().filter(|f| f.kind == Kind::Links)
        .map(|f| f.len as usize).sum::<usize>().next_multiple_of(64);
    let total = parse_bytes + link_bytes;
    let (arena, parse_mapping) = SinkArena::new(total).split_at(total);
    let (mut bucket_space, arena) = arena.split_at(link_bytes);
    let (mut tokens, mut arena) = arena.split_at(tok_bytes * workers);
    let links = read_links(ctx, threads, tm, &mut tokens.scratch(), |pairs| {
        BucketLinks::build(pairs, plan.p2_bits, threads, &mut bucket_space.scratch())
    })?;
    let mut slices = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tok, rest) = tokens.split_at(tok_bytes);
        tokens = rest;
        let (events, rest) = arena.split_at(event_bytes);
        let (clicks, rest) = rest.split_at(click_bytes);
        slices.push(Mutex::new(Some((tok, events, clicks))));
        arena = rest;
    }
    let result = parse_sinks::<false, u64, SinglePass>(ctx, plan, None, |tid| {
        let (tok, events, clicks) = slices[tid].lock().unwrap().take().unwrap();
        let events = SinglePass {
            tok: Sinks::in_arena(plan.p1, plan.cap_ev, tok),
            b: Sinks::in_arena(plan.p2, cap_b, events),
            tid_bits: (tid as u32) << 28,
        };
        (events, Sinks::in_arena(plan.p2, plan.cap_ck, clicks))
    }, tm);
    tm.lap("parse");
    let states = result?;
    let mut tok_sinks = Vec::with_capacity(states.len());
    let mut b_sinks = Vec::with_capacity(states.len());
    let mut ck_sinks = Vec::with_capacity(states.len());
    let mut ne_raw = 0;
    for (ev, ck) in states {
        ne_raw += ev.tok.len.iter().sum::<usize>();
        tok_sinks.push(ev.tok);
        b_sinks.push(ev.b);
        ck_sinks.push(ck);
    }
    let p2 = plan.p2;
    let mut dead = DeadEvents::new(&b_sinks, p2, threads);
    dedup_tokens(&tok_sinks, &dead, plan.p1, threads);
    let (relocated, group_scratch) = if links.entries.is_empty() {
        let scratch = tok_sinks.into_iter().map(Sinks::into_arena).collect();
        tm.lap("dedup");
        (None, scratch)
    } else {
        let arenas: Vec<_> = tok_sinks.into_iter()
            .map(|sink| Mutex::new(Some(sink.into_arena())))
            .collect();
        tm.lap("dedup");
        let (relocated, mut scratch) = relocate_linked(links, &b_sinks, &ck_sinks, &mut dead, plan, ne_raw, threads, &arenas);
        scratch.extend(arenas.into_iter().filter_map(|a| a.into_inner().unwrap()));
        tm.lap("relocate");
        (Some(relocated), scratch)
    };
    Ok(Deduped { b_sinks, ck_sinks, p2, ne_raw, dead: Some(dead), relocated,
        parse_mapping: Some(parse_mapping), group_scratch })
}

fn dedup_tokens(sinks: &[Sinks<Tok>], dead: &DeadEvents, p1: usize, threads: usize) {
    par::run(threads, p1, |_| DedupTable::default(), |table, p| {
        let mut slices = [(&[][..], 0); MAX_THREADS * 2];
        let (active, total) = partition_slices(sinks, p, &mut slices);
        table.mark_losers(active, total, |_, loser| dead.mark(loser.loc));
    });
}

fn relocate_linked(
    links: BucketLinks<'_>, events: &[Sinks<RecB<u64>>], clicks: &[Sinks<RecC<u64>>],
    dead: &mut DeadEvents, plan: &PartitionPlan, ne_raw: usize, threads: usize,
    arenas: &[Mutex<Option<par::SinkArena>>],
) -> (Relocated<u64>, Vec<SinkArena>) {
    let mut dead_clicks = DeadClicks::new(clicks, plan.p2, threads);
    let event_offsets = &dead.offsets;
    let click_offsets = &dead_clicks.offsets;
    let event_bits = SyncPtr(dead.bits.as_mut_ptr());
    let click_bits = SyncPtr(dead_clicks.bits.as_mut_ptr());
    let stride = events.len();
    let shift = 64 - plan.p2_bits;
    let cap_b = ne_raw * 45 / 100 / threads / plan.p2 + 16;
    let cap_c = (plan.cap_ck - 24) * plan.parse_threads * 45 / 100 / threads + 16;

    let mut workers = par::run_fini(
        threads,
        plan.p2,
        |tid| {
            let arena = arenas.get(tid).and_then(|slot| slot.lock().unwrap().take());
            let (b, c, tail) = Sinks::reuse_pair(plan.p2, cap_b, cap_c, arena);
            (RelocationMap::default(), b, c, tail)
        },
        |(map, out_b, out_c, _), q| {
            let entries = links.bucket(q);
            if entries.is_empty() {
                return;
            }
            map.reset(entries.len());
            for &(key, canon) in entries {
                map.insert(key, canon);
            }
            let start = q * stride;
            for (tid, (events, clicks)) in events.iter().zip(clicks).enumerate() {
                let part = start + tid;
                let (e, c) = (event_offsets[part], click_offsets[part]);
                // Each task owns a whole raw partition. Word ranges are aligned
                // to record groups of 64 and never shared by two tasks.
                let eb = unsafe { event_bits.slice(e, event_offsets[part + 1] - e) };
                let cb = unsafe { click_bits.slice(c, click_offsets[part + 1] - c) };
                relocate_rows(events.part(q), eb, map, |r| r.person, |r, person| {
                    out_b.push((person.h() >> shift) as usize, RecB { person, ..*r });
                });
                relocate_rows(clicks.part(q), cb, map, |r| r.person, |r, person| {
                    out_c.push((person.h() >> shift) as usize, RecC { person, ..*r });
                });
            }
        },
        |(_, b, c, _)| {
            b.finish();
            c.finish();
        },
    );
    let mut reloc_b = Vec::with_capacity(workers.len());
    let mut reloc_c = Vec::with_capacity(workers.len());
    let mut scratch = Vec::with_capacity(workers.len());
    for (_, b, c, tail) in workers.drain(..) {
        scratch.extend(tail);
        reloc_b.push(b);
        reloc_c.push(c);
    }
    (Relocated { events: reloc_b, clicks: reloc_c, dead_clicks }, scratch)
}

#[inline(always)]
fn relocate_rows<T, W: DeadWord>(
    (arena, overflow): (&[T], &[T]), dead: &mut [W], map: &RelocationMap,
    key: impl Fn(&T) -> u64, mut move_row: impl FnMut(&T, u64),
) {
    let len = arena.len() + overflow.len();
    for (i, word) in dead.iter_mut().enumerate() {
        let start = i * 64;
        let end = (start + 64).min(len);
        let word = word.exclusive();
        let mut mask = *word;
        let moved = if end <= arena.len() {
            relocate_block(&arena[start..end], &mut mask, map, &key, &mut move_row)
        } else if start >= arena.len() {
            relocate_block(&overflow[start - arena.len()..end - arena.len()], &mut mask, map, &key, &mut move_row)
        } else {
            let first = &arena[start..];
            let a = relocate_block(first, &mut mask, map, &key, &mut move_row);
            let b = relocate_block(&overflow[..end - arena.len()], &mut mask, map, &key, &mut move_row);
            a | (b << first.len())
        };
        *word |= moved;
    }
}

#[inline(always)]
fn relocate_block<T>(
    rows: &[T], mask: &mut u64, map: &RelocationMap,
    key: &impl Fn(&T) -> u64, move_row: &mut impl FnMut(&T, u64),
) -> u64 {
    let mut moved = 0;
    let mut done = 0;
    for block in rows.chunks_exact(8) {
        // Probe the slots we prefetched so each row only hashes its key once.
        let slots: [usize; 8] = std::array::from_fn(|i| map.prefetch_slot(key(&block[i])));
        for (i, (row, slot)) in block.iter().zip(slots).enumerate() {
            if *mask & 1 == 0 {
                if let Some(person) = map.get_from_slot(key(row), slot) {
                    move_row(row, person);
                    moved |= 1u64 << (done + i);
                }
            }
            *mask >>= 1;
        }
        done += 8;
    }
    for (i, row) in rows[done..].iter().enumerate() {
        if *mask & 1 == 0 {
            if let Some(person) = map.get(key(row)) {
                move_row(row, person);
                moved |= 1u64 << (done + i);
            }
        }
        *mask >>= 1;
    }
    moved
}

#[inline(always)]
fn for_each_live<T: Copy, W: DeadWord>(
    sinks: &[Sinks<T>], dead: Option<&DeadRows<W>>, relocated: &[Sinks<T>], q: usize,
    mut f: impl FnMut(&T),
) {
    for (tid, sink) in sinks.iter().enumerate() {
        let (arena, overflow) = sink.part(q);
        if let Some(dead) = dead {
            let words = dead.words(tid, q);
            let mut start = 0;
            for slice in [arena, overflow] {
                let mut done = 0;
                while done < slice.len() {
                    let idx = start + done;
                    let bit = idx & 63;
                    let n = (64 - bit).min(slice.len() - done);
                    let mask = words[idx >> 6].load() >> bit;
                    let rows = &slice[done..done + n];
                    if mask == 0 {
                        for r in rows {
                            f(r);
                        }
                    } else {
                        let mut live = !mask & (u64::MAX >> (64 - n));
                        while live != 0 {
                            f(unsafe { rows.get_unchecked(live.trailing_zeros() as usize) });
                            live &= live - 1;
                        }
                    }
                    done += n;
                }
                start += slice.len();
            }
        } else {
            for r in arena.iter().chain(overflow) {
                f(r);
            }
        }
    }
    for sink in relocated {
        let (arena, overflow) = sink.part(q);
        for r in arena.iter().chain(overflow) {
            f(r);
        }
    }
}

#[derive(Clone, Copy)]
struct GroupEntry<K> {
    key: K,
    id: u32,
    count: u32,
}

impl<K: Key> Default for GroupEntry<K> {
    fn default() -> Self {
        Self { key: K::default(), id: 0, count: 0 }
    }
}

// Compact event counts share the probed slot with the person key and ID.
struct GroupMap<K> {
    entries: Vec<GroupEntry<K>>,
    person_slots: Vec<usize>,
    mask: usize,
}

impl<K: Key> GroupMap<K> {
    fn new() -> Self {
        Self { entries: Vec::new(), person_slots: Vec::new(), mask: 0 }
    }

    fn reset(&mut self, keys: usize) {
        let cap = (keys * 2).next_power_of_two().max(16);
        self.entries.clear();
        self.entries.resize(cap, GroupEntry::default());
        self.person_slots.clear();
        self.mask = cap - 1;
    }

    #[cold]
    fn grow(&mut self) {
        let cap = self.entries.len() * 2;
        let old = std::mem::replace(&mut self.entries, vec![GroupEntry::default(); cap]);
        self.mask = cap - 1;
        for pos in &mut self.person_slots {
            let entry = old[*pos];
            let mut i = entry.key.h() as usize & self.mask;
            while !self.entries[i].key.is_zero() {
                i = (i + 1) & self.mask;
            }
            self.entries[i] = entry;
            *pos = i;
        }
    }

    #[inline(always)]
    fn count(&mut self, key: K) -> u32 {
        debug_assert!(!key.is_zero());
        let mut i = key.h() as usize & self.mask;
        loop {
            let entry = unsafe { self.entries.get_unchecked_mut(i) };
            if entry.key == key {
                entry.count += 1;
                return entry.id;
            }
            if entry.key.is_zero() {
                let id = self.person_slots.len() as u32;
                *entry = GroupEntry { key, id, count: 1 };
                self.person_slots.push(i);
                if self.person_slots.len() * 2 > self.entries.len() {
                    self.grow();
                }
                return id;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, key: K) -> Option<u32> {
        let mut i = key.h() as usize & self.mask;
        loop {
            let entry = unsafe { self.entries.get_unchecked(i) };
            if entry.key == key {
                return Some(entry.id);
            }
            if entry.key.is_zero() {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn person(&self, id: usize) -> GroupEntry<K> {
        self.entries[self.person_slots[id]]
    }
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
fn radix_sort_ev(slice: &mut [CompactEv], tmp: &mut ArenaVec<'_, CompactEv>) {
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
fn sort_ev_slice(slice: &mut [CompactEv], tmp: &mut ArenaVec<'_, CompactEv>) {
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
fn run_fused<K: Key>(ctx: &Ctx<'_>, mut deduped: Deduped<K>, output_dir: &Path, threads: usize, tm: &mut Timer) -> Option<SinkArena> {
    let parse_mapping = deduped.parse_mapping.take();
    let prof = std::env::var("INGEST_PROF").as_deref() == Ok("1");
    if prof {
        group_fused::<K, GroupTimings>(ctx, deduped, output_dir, threads, tm);
    } else {
        group_fused::<K, ()>(ctx, deduped, output_dir, threads, tm);
    }
    parse_mapping
}

#[derive(Clone, Copy)]
enum GroupPhase {
    Csr,
    Sort,
    Aggregate,
    Write,
}

// Specialize the default path with () so it stores no timing state and
// compiles out all phase marks in the worker loops.
trait GroupProfile: Default + Send {
    const ENABLED: bool = false;
    fn start(&mut self) {}
    fn end(&mut self, _: GroupPhase) {}
    fn totals(&self) -> [f64; 4] { [0.0; 4] }
}

impl GroupProfile for () {}

#[derive(Default)]
struct GroupTimings {
    last: f64,
    totals: [f64; 4],
}

impl GroupProfile for GroupTimings {
    const ENABLED: bool = true;

    fn start(&mut self) {
        self.last = par::thread_cpu();
    }

    fn end(&mut self, phase: GroupPhase) {
        let now = par::thread_cpu();
        self.totals[phase as usize] += now - self.last;
        self.last = now;
    }

    fn totals(&self) -> [f64; 4] {
        self.totals
    }
}

struct FusedAssists {
    s_start: Vec<u32>,
    s_day: Vec<u16>,
    stamp: u32,
    campaigns: Vec<(u32, u32)>,
}

impl FusedAssists {
    fn new(ncamp: usize) -> Self {
        Self {
            s_start: Vec::with_capacity(256),
            s_day: Vec::with_capacity(256),
            stamp: 0,
            campaigns: vec![(0, 0); ncamp],
        }
    }

    #[inline]
    fn count<K: Key>(&mut self, camp: &mut CampStore, clicks: &[CompactClick<K>]) {
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            self.campaigns.fill((0, 0));
            self.stamp = 1;
        }
        let stamp = self.stamp;
        let starts = &self.s_start;
        let (mut first, mut end) = (0, 0);
        for click in clicks {
            let lo = click.ts.saturating_sub(WEEK_MS_U32);
            while first < starts.len() && starts[first] < lo {
                first += 1;
            }
            if first == starts.len() {
                break;
            }
            end = end.max(first);
            while end < starts.len() && starts[end] <= click.ts {
                end += 1;
            }

            // Both bounds advance monotonically. For this campaign, only
            // the tail beyond the previous click's window is new.
            let campaign = click.campaign as usize;
            let seen = unsafe { self.campaigns.get_unchecked_mut(campaign) };
            let begin = if seen.0 == stamp { first.max(seen.1 as usize) } else { first };
            if begin >= end {
                continue;
            }
            *seen = (stamp, end as u32);
            let days = &self.s_day[begin..end];
            if let Some(row) = camp.dense_row(click.campaign) {
                for &day in days {
                    // Session starts are always inside the window.
                    unsafe { row.get_unchecked_mut(day as usize) }.assisted += 1;
                }
            } else {
                for &day in days {
                    let day = camp.day0.wrapping_add(i32::from(day));
                    camp.cell(click.campaign, day).assisted += 1;
                }
            }
        }
    }
}

fn group_fused<K: Key, P: GroupProfile>(
    ctx: &Ctx<'_>,
    deduped: Deduped<K>,
    output_dir: &Path,
    threads: usize,
    tm: &mut Timer,
) {
    let Deduped { b_sinks, ck_sinks, p2, ne_raw, dead, relocated, mut group_scratch, .. } = deduped;
    let (reloc_b, reloc_c, dead_ck) = relocated.as_ref().map_or(
        (&[][..], &[][..], None),
        |r| (&r.events[..], &r.clicks[..], Some(&r.dead_clicks)),
    );
    let ud = output_dir.join("user_daily");
    let cd = output_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");

    let mut ev_cnt_part = vec![0usize; p2];
    let mut ne = 0usize;
    for q in 0..p2 {
        let c: usize = b_sinks.iter().enumerate().map(|(tid, s)| {
            let raw = s.count(q);
            let removed = dead.as_ref().map_or(0, |d| d.words(tid, q)
                .iter().map(|w| w.load(Ordering::Relaxed).count_ones() as usize).sum::<usize>());
            raw - removed
        }).sum();
        let c = c + reloc_b.iter().map(|s| s.count(q)).sum::<usize>();
        ev_cnt_part[q] = c;
        ne += c;
    }
    let mut q_order: Vec<usize> = (0..p2).collect();
    q_order.sort_unstable_by_key(|&q| std::cmp::Reverse(ev_cnt_part[q]));

    let ws = ctx.ws;
    let we = ctx.we;
    let day0 = aggregate::day_of(ws);
    let ws_rem = ws.rem_euclid(ts::DAY_MS) as u32;
    let ndays = if we > ws {
        (aggregate::day_of(we - 1) as i64 - day0 as i64 + 1).min(u32::MAX as i64) as u32
    } else {
        0
    };
    let lookups = ctx.lookups.get();
    let ncamp = lookups.campaigns.len();
    let organic = ncamp as u32;
    let ncat = lookups.categories.len();
    let categories = &lookups.categories;

    struct FusedWorker<'a, K: Key, P> {
        // Grouping scratch (reused across P2 partitions -> stays L2-hot!)
        map: GroupMap<K>,
        fill: Vec<u32>,
        pid: ArenaVec<'a, u32>,
        ev_flat: ArenaVec<'a, CompactEv>,
        ev_buf: ArenaVec<'a, CompactEv>,
        ev_tmp: ArenaVec<'a, CompactEv>,
        cl_raw: ArenaVec<'a, (u32, CompactClick<K>)>,
        ck_cnt: Vec<u32>,
        ck_off: Vec<u32>,
        cl_buf: ArenaVec<'a, CompactClick<K>>,
        // Aggregate state
        camp: CampStore,
        writer: output::UserWriter<'a>,
        cat_sum: Vec<u64>,
        cat_touched: Vec<u32>,
        assists: FusedAssists,
        // Counters
        np: usize,
        nc: usize,
        written: usize,
        profile: P,
    }

    impl<K: Key, P: GroupProfile> FusedWorker<'_, K, P> {
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
            let cat = if revenue == 0 {
                None
            } else {
                self.take_top_category(categories)
            };
            if self.writer.push_group_row(id_bytes, day, sessions, events, revenue, cat) {
                self.profile.end(GroupPhase::Aggregate);
                self.writer.flush_group();
                self.profile.end(GroupPhase::Write);
            }
        }

        #[cold]
        fn take_top_category<'a>(&mut self, categories: &'a [Vec<u8>]) -> Option<&'a [u8]> {
            let mut top = NO_CATEGORY;
            let mut best = 0u64;
            for c in self.cat_touched.drain(..) {
                let v = unsafe { self.cat_sum.get_unchecked_mut(c as usize) };
                if *v > best || (*v == best && c < top) {
                    best = *v;
                    top = c;
                }
                *v = 0;
            }
            categories.get(top as usize).map(Vec::as_slice)
        }
    }

    let q_order = &q_order;
    let ev_cnt_part = &ev_cnt_part;
    let ud_ref = &ud;
    let est_user_rows = ne / threads / 8 + 1024;
    let scratch: Vec<_> = group_scratch.iter_mut().map(|a| Mutex::new(a.scratch())).collect();
    let group_scratch_available = !scratch.is_empty();
    let event_cap = if group_scratch_available {
        ev_cnt_part.iter().copied().max().unwrap_or(0).max(65536)
    } else {
        65536
    };
    let click_cap = if group_scratch_available {
        let raw_clicks: usize = ck_sinks.iter().map(|s| s.len.iter().sum::<usize>()).sum();
        (raw_clicks / p2 * 2).max(16384)
    } else {
        16384
    };
    let workers: Vec<FusedWorker<'_, K, P>> = par::run_fini(
        threads,
        p2,
        |tid| {
            let mut scratch = scratch.get(tid)
                .map(|slot| std::mem::take(&mut *slot.lock().unwrap()))
                .unwrap_or_default();
            FusedWorker {
                map: GroupMap::new(),
                fill: Vec::with_capacity(2048),
                pid: scratch.buffer(event_cap),
                ev_flat: scratch.buffer(event_cap),
                ev_buf: scratch.buffer(event_cap),
                ev_tmp: if group_scratch_available { scratch.buffer(8192) } else { ArenaVec::default() },
                cl_raw: scratch.buffer(click_cap),
                ck_cnt: Vec::with_capacity(2048),
                ck_off: Vec::with_capacity(2048),
                cl_buf: scratch.buffer(click_cap),
                camp: CampStore::new(ncamp + 1, day0, ndays),
                writer: output::UserWriter::new(ud_ref, tid, est_user_rows),
                cat_sum: vec![0u64; ncat],
                cat_touched: Vec::with_capacity(32),
                assists: FusedAssists::new(ncamp),
                np: 0,
                nc: 0,
                written: 0,
                profile: P::default(),
            }
        },
        |w, idx| {
            let q = q_order[idx];
            let n = ev_cnt_part[q];
            if n == 0 {
                return;
            }
            w.profile.start();
            w.map.reset(n / 25 + 16);
            w.pid.clear();
            w.pid.reserve(n);
            w.ev_flat.clear();
            w.ev_flat.reserve(n);
            let pid = w.pid.as_mut_ptr();
            let ev_flat = w.ev_flat.as_mut_ptr();
            let mut live = 0;
            for_each_live(&b_sinks, dead.as_ref(), reloc_b, q, |r| {
                let id = w.map.count(r.person);
                // The count pass reserved one slot per live event.
                unsafe {
                    pid.add(live).write(id);
                    ev_flat.add(live).write(CompactEv::new(r.ts, r.cc));
                }
                live += 1;
            });
            debug_assert_eq!(live, n);
            unsafe {
                w.pid.set_len(live);
                w.ev_flat.set_len(live);
            }
            let np = w.map.person_slots.len();
            w.np += np;
            w.fill.clear();
            w.fill.reserve(np);
            let mut acc = 0u32;
            for i in 0..np {
                w.fill.push(acc);
                acc += w.map.person(i).count;
            }
            w.ev_buf.clear();
            w.ev_buf.reserve(n);
            let ev_ptr = w.ev_buf.as_mut_ptr();
            for (&id, &ev) in w.pid.iter().zip(w.ev_flat.iter()) {
                let pos = unsafe { w.fill.get_unchecked_mut(id as usize) };
                unsafe { ev_ptr.add(*pos as usize).write(ev) };
                *pos += 1;
            }
            // The prefix counts assigned each live event one distinct slot.
            unsafe { w.ev_buf.set_len(n) };

            // Collect & count-scatter clicks for persons present in partition `q`.
            w.cl_raw.clear();
            w.ck_cnt.clear();
            w.ck_cnt.resize(np, 0);
            for_each_live(&ck_sinks, dead_ck, reloc_c, q, |r| {
                if let Some(id) = w.map.get(r.person) {
                    w.cl_raw.push((id, CompactClick { ts: r.ts, campaign: r.campaign, cid: r.cid }));
                    unsafe { *w.ck_cnt.get_unchecked_mut(id as usize) += 1 };
                }
            });
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
                w.cl_buf.clear();
                w.cl_buf.reserve(ncl_raw);
                let cl_ptr = w.cl_buf.as_mut_ptr();
                for &(id, ck) in w.cl_raw.iter() {
                    let pos = unsafe { *w.fill.get_unchecked(id as usize) } as usize;
                    unsafe {
                        cl_ptr.add(pos).write(ck);
                        *w.fill.get_unchecked_mut(id as usize) = (pos + 1) as u32;
                    }
                }
                // Each of the ncl_raw clicks initialized one distinct offset.
                unsafe { w.cl_buf.set_len(ncl_raw) };
            }

            w.profile.end(GroupPhase::Csr);

            // Per-person sort + sessionize + credit + assists while hot in L1/L2 cache!
            let mut ev_st = 0usize;
            let mut id_buf = [0u8; 16];
            for pi in 0..np {
                let person = w.map.person(pi);
                let ev_en = ev_st + person.count as usize;
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
                w.profile.end(GroupPhase::Sort);
                let id_len = person.key.unpack_buf(&mut id_buf);
                let id_bytes = &id_buf[..id_len];

                w.assists.s_start.clear();
                w.assists.s_day.clear();
                let mut i = ev_st;
                let mut cj = cl_st;
                let cl_end = cl_st + cl_len;
                let mut grp_first = cl_end;
                // The exclusive window end reserves u32::MAX as a sentinel.
                let mut last_ts = u32::MAX;
                let mut rel_day = 0u32;
                let mut next_day_s = DAY_MS_U32 - ws_rem;
                let (mut ud_sessions, mut ud_events, mut ud_revenue) = (0u32, 0u32, 0i64);

                while i < ev_en {
                    let first = unsafe { *w.ev_buf.get_unchecked(i) };
                    let s = first.ts();
                    if s >= next_day_s {
                        if ud_sessions != 0 {
                            w.flush_user_day(id_bytes, categories, day0.wrapping_add(rel_day as i32), ud_sessions, ud_events, ud_revenue);
                            ud_sessions = 0;
                            ud_events = 0;
                            ud_revenue = 0;
                        }
                        // The compact window is at most u32::MAX - WEEK_MS long,
                        // leaving room to add the window's time of day.
                        rel_day = (ws_rem + s) / DAY_MS_U32;
                        next_day_s = (rel_day + 1) * DAY_MS_U32 - ws_rem;
                    }
                    let mut prev = s;
                    let mut k = i + 1;
                    let mut cc = first.cc();
                    let mut rev = 0u64;
                    loop {
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
                        if k == ev_en {
                            break;
                        }
                        let e = unsafe { *w.ev_buf.get_unchecked(k) };
                        let ets = e.ts();
                        if ets - prev > GAP_MS_U32 {
                            break;
                        }
                        prev = ets;
                        cc = e.cc();
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
                        if ct != last_ts {
                            last_ts = ct;
                            grp_first = cj;
                        }
                        cj += 1;
                    }
                    let credit = if grp_first != cl_end && last_ts >= s {
                        unsafe { w.cl_buf.get_unchecked(grp_first).campaign }
                    } else {
                        organic
                    };
                    // Credit is a known campaign (or organic) and the start is in the window.
                    let cell = unsafe { w.camp.cell_in_window(credit, rel_day as usize) };
                    cell.sessions += 1;
                    if rev != 0 {
                        cell.purchasing += 1;
                        cell.revenue += rev as i64;
                    }

                    ud_sessions += 1;
                    ud_events += nev;
                    ud_revenue += rev as i64;
                    if cl_len > 0 {
                        w.assists.s_start.push(s);
                        w.assists.s_day.push(rel_day as u16);
                    }
                }
                w.flush_user_day(id_bytes, categories, day0.wrapping_add(rel_day as i32), ud_sessions, ud_events, ud_revenue);

                if cl_len > 0 {
                    w.assists.count(&mut w.camp, &w.cl_buf[cl_st..cl_end]);
                }
                ev_st = ev_en;
                w.profile.end(GroupPhase::Aggregate);
            }
        },
        |w| {
            w.profile.start();
            w.written = w.writer.finish();
            w.profile.end(GroupPhase::Write);
            w.writer.release_buffers();
            drop(std::mem::take(&mut w.ev_buf));
            drop(std::mem::take(&mut w.ev_tmp));
            drop(std::mem::take(&mut w.cl_buf));
            drop(std::mem::take(&mut w.cl_raw));
            drop(std::mem::take(&mut w.pid));
            drop(std::mem::take(&mut w.ev_flat));
        },
    );
    if group_scratch_available {
        // The parse mapping stays alive through the exit diagnostics.
        drop((b_sinks, ck_sinks));
    } else {
        let arenas = b_sinks.into_iter().map(Ok).chain(ck_sinks.into_iter().map(Err));
        par::drop_par(threads, arenas.collect());
    }
    let np: usize = workers.iter().map(|w| w.np).sum();
    let nc: usize = workers.iter().map(|w| w.nc).sum();
    let user_rows_written: usize = workers.iter().map(|w| w.written).sum();
    if user_rows_written == 0 {
        output::write_empty_user(&ud).expect("writing empty user_daily");
    }
    if P::ENABLED {
        let mut totals = [0.0; 4];
        for w in &workers {
            for (total, cpu) in totals.iter_mut().zip(w.profile.totals()) {
                *total += cpu;
            }
        }
        let [csr, sort, agg, write] = totals;
        eprintln!("[ingest-prof] cpu csr {csr:.6}s sort {sort:.6}s sessionize+agg {agg:.6}s user-finish {write:.6}s");
    }
    let camp_rows = aggregate::merge_camps(workers.into_iter().map(|w| w.camp));
    tm.lap("group+agg");
    let t_cw = Instant::now();
    output::write_campaign_raw(&cd, &camp_rows, &lookups.campaigns).expect("writing campaign_daily");
    tm.lap("campaign-write");
    eprintln!(
        "[stage] campaign_write {:.3}s (user rows {user_rows_written}, campaign rows {})",
        t_cw.elapsed().as_secs_f64(),
        camp_rows.len()
    );
    eprintln!("[ingest] key={}B persons={np} events={ne} (raw {ne_raw}) clicks={nc}", std::mem::size_of::<K>());
}

fn run<K: Key>(ctx: &mut Ctx<'_>, threads: usize, tm: &mut Timer) -> Result<Loaded, Need> {
    let plan = PartitionPlan::new(ctx, threads);
    let Deduped { b_sinks, ck_sinks, p2, ne_raw, .. } = ingest_to_sinks::<true, K>(ctx, threads, &plan, tm)?;
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
    let arenas = b_sinks.into_iter().map(Ok).chain(ck_sinks.into_iter().map(Err));
    par::drop_par(threads, arenas.collect());
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
            categories: ctx.lookups.get().categories.clone(),
            campaigns: ctx.lookups.get().campaigns.clone(),
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
    emit: impl FnOnce(K, u64, u32, K, u32, u32),
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
    let (mut user, mut ts, mut cc) = (K::default(), 0, INVALID_CC);
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
        user = fkey(b, f[1]).ok_or_else(|| key_need(f[1]))?;
        ts = (t - ws) as u32;
        let cat = if ALL_CAT || cents != 0 {
            unsafe { prod.get(fptr(b, f[5]), flen(f[5])) }
        } else {
            0
        };
        cc = (cat << 24) | cents;
    }
    emit(eid, eid_h, ingest, user, ts, cc);
    Ok(())
}

#[inline(always)]
fn parse_click<K: Key>(
    rows: &ParseRows<'_, K>, b: &[u8], f: &[(u32, u32); 4],
) -> Result<Option<RecC<K>>, Need> {
    if flen(f[0]) == 0 || flen(f[1]) == 0 {
        return Ok(None);
    }
    let campaign = unsafe { rows.lookups.camp.get(fptr(b, f[2]), flen(f[2])) };
    if campaign == INVALID_CAMP {
        return Ok(None);
    }
    let base = rows.clicks_base();
    let t = match fts(rows.dt, b, f[3]) {
        Some(t) if t >= base && t < rows.we => t,
        _ => return Ok(None),
    };
    let cid = fkey(b, f[0]).ok_or_else(|| key_need(f[0]))?;
    let person = fkey(b, f[1]).ok_or_else(|| key_need(f[1]))?;
    Ok(Some(RecC { person, cid, ts: (t - base) as u32, campaign }))
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

    fn canonical_pairs<'a>(
        pairs: &[Vec<(K, K)>], threads: usize, scratch: &mut Scratch<'a>,
    ) -> Vec<CanonicalPart<'a, K>> {
        let mut off = vec![0usize; pairs.len() + 1];
        for (i, v) in pairs.iter().enumerate() {
            off[i + 1] = off[i] + v.len();
        }
        let nl = off[pairs.len()];
        if nl == 0 {
            eprintln!("[ingest] links=0 linked_ids=0");
            return Vec::new();
        }
        let off = &off;

        // 1. Flat 2-pass count + scatter per thread (1 allocation per thread instead of 256 Vecs).
        struct Scattered<'a, K> {
            flat: ArenaVec<'a, (K, u32)>,
            pos: [usize; LP + 1],
        }
        let scatter_space: Vec<_> = pairs.iter()
            .map(|p| Mutex::new(Some(scratch.buffer::<(K, u32)>(2 * p.len()))))
            .collect();
        let scattered: Vec<Scattered<'_, K>> = par::run(threads, pairs.len(), |_| Vec::new(), |res, t| {
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
            let mut flat = scatter_space[t].lock().unwrap().take().unwrap();
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
            unsafe { flat.set_len(pos[LP]) };
            res.push(Scattered { flat, pos });
        })
        .into_iter()
        .flatten()
        .collect();

        // 2. Per partition: intern; write endpoint -> (partition, local id) into uninit `ep`.
        let mut ep = scratch.buffer::<u64>(2 * nl);
        let epp = SyncPtr(ep.as_mut_ptr());
        let scattered_ref = &scattered;
        let key_space = Mutex::new(&mut *scratch);
        let key_space_ref = &key_space;
        let mut keys_parts: Vec<(usize, ArenaVec<'_, K>)> = par::run(
            threads,
            LP,
            |_| (Vec::new(), Map::<K, u32>::with_capacity(1024)),
            move |(res, m), p| {
                let n: usize = scattered_ref.iter().map(|s| s.pos[p + 1] - s.pos[p]).sum();
                m.reset(n);
                let mut keys = key_space_ref.lock().unwrap().buffer::<K>(n / 2 + 8);
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
        unsafe { ep.set_len(2 * nl) };
        drop(scattered);
        keys_parts.sort_unstable_by_key(|x| x.0);
        let mut base = vec![0u32; LP + 1];
        for p in 0..LP {
            base[p + 1] = base[p] + keys_parts[p].1.len() as u32;
        }
        let nn = base[LP] as usize;

        // 3. Parallel copy of `keys` + parallel init of `parent` and `best` (zero serial overhead).
        let mut keys = scratch.buffer::<K>(nn);
        let mut parent = scratch.buffer::<AtomicU32>(nn);
        let mut best = scratch.buffer::<AtomicU32>(nn);
        {
            let (kp, pp, bp) = (SyncPtr(keys.as_mut_ptr()), SyncPtr(parent.as_mut_ptr()), SyncPtr(best.as_mut_ptr()));
            let (keys_parts_ref, base_ref) = (&keys_parts, &base);
            par::run(threads, LP, |_| (), move |_, p| {
                let s = base_ref[p] as usize;
                let src = &keys_parts_ref[p].1;
                unsafe {
                    kp.copy_from(s, src);
                    for i in s..s + src.len() {
                        pp.write(i, AtomicU32::new(i as u32));
                        bp.write(i, AtomicU32::new(i as u32));
                    }
                }
            });
        }
        unsafe {
            keys.set_len(nn);
            parent.set_len(nn);
            best.set_len(nn);
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

        // 5. Collect only keys whose canonical key differs, preserving raw partitions.
        let base_ref = &base;
        let canon_space = Mutex::new(scratch);
        let canon_space_ref = &canon_space;
        let canon_parts: Vec<CanonicalPart<'_, K>> = par::run(threads, LP, |_| Vec::new(), move |res, p| {
            let (s, e) = (base_ref[p] as usize, base_ref[p + 1] as usize);
            let mut canon = canon_space_ref.lock().unwrap().buffer::<(K, K)>((e - s) * 3 / 4 + 8);
            for i in s..e {
                let r = uf_find(parent_ref, i as u32) as usize;
                let c = keys_ref[best_ref[r].load(Ordering::Relaxed) as usize];
                let k = keys_ref[i];
                if c != k {
                    canon.push((k, c));
                }
            }
            if !canon.is_empty() {
                res.push((p, canon));
            }
        })
        .into_iter()
        .flatten()
        .collect();
        let total_mapped: usize = canon_parts.iter().map(|x| x.1.len()).sum();
        eprintln!("[ingest] links={nl} linked_ids={nn} mapped={total_mapped}");
        canon_parts
    }

    fn from_pairs(canon_parts: &[CanonicalPart<'_, K>], threads: usize) -> Self {
        let Some(max_cnt) = canon_parts.iter().map(|x| x.1.len()).max() else {
            return Self::empty();
        };
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
            for &(k, c) in canon.iter() {
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
    let (_, _, prod) = small::read_products_lookup(data_dir);
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
                            let _ = parse_event::<false, u64>(&dt, b, f, ws, we, &prod, |eid, h, ingest, user, ts, cc| {
                                let p = (h >> (64 - p1_bits)) as usize;
                                sinks.push(p, RecA { eid, user, ts, ingest, cc, eid_hash: h as u32 });
                            });
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
    fn single_pass_dedup_and_location_overflow() {
        struct TestInput(std::path::PathBuf);
        impl Drop for TestInput {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let input = TestInput(std::env::temp_dir().join(format!("shopping-sessions-dedup-{}", std::process::id())));
        let root = &input.0;
        std::fs::create_dir_all(root.join("events")).unwrap();
        std::fs::create_dir_all(root.join("links")).unwrap();
        std::fs::write(root.join("manifest.json"), r#"{
            "window_start": "2026-10-01T00:00:00Z",
            "window_end": "2026-10-15T00:00:00Z"
        }"#).unwrap();
        std::fs::write(root.join("links/part.csv"), "user_id,linked_user_id\na,a0\n").unwrap();
        let path = root.join("events/part.csv");
        let header = b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n";
        let mut events = header.to_vec();
        events.extend_from_slice(concat!(
            "a,a0,2026-10-01T00:00:01Z,2026-10-01T00:00:02Z,view,p,\n",
            "a,b,2026-10-01T00:00:00Z,2026-10-01T00:00:02Z,view,p,\n",
            "b,b,2026-10-01T00:00:00Z,2026-10-01T00:00:03Z,view,p,\n",
            "b,unencodable-user,bad,2026-10-01T00:00:02Z,view,p,\n",
            "c,c,2026-10-01T00:00:00Z,2026-10-01T00:00:02Z,view,p,\n",
            "c,unencodable-user,bad,2026-10-01T00:00:03Z,view,p,\n",
            "d,d,2026-10-01T00:00:00Z,bad,view,p,\n",
        ).as_bytes());
        std::fs::write(&path, events).unwrap();

        let run = || thread::scope(|scope| {
            let mut tm = Timer::new("test");
            let mut ctx = build_ctx(root, 1, scope, &mut tm).unwrap();
            let plan = PartitionPlan::new(&ctx, 1);
            let Deduped { b_sinks, p2, ne_raw, dead, relocated, .. } = ingest_compact(&mut ctx, 1, &plan, &mut tm).unwrap();
            let reloc_b = relocated.as_ref().map_or(&[][..], |r| &r.events[..]);
            let mut live = Vec::new();
            for q in 0..p2 {
                for_each_live(&b_sinks, dead.as_ref(), reloc_b, q, |r| live.push((r.person, r.ts)));
            }
            live.sort_unstable();
            (dead.is_some(), ne_raw, live)
        });
        let user = |id: &[u8]| u64::enc_slice(id).unwrap();
        assert_eq!(run(), (true, 7, vec![(user(b"a"), 1000), (user(b"c"), 0)]));

        let row = b"x,c,2026-10-01T00:00:00Z,2026-10-01T00:00:02Z,view,p,\n";
        let mut events = header.to_vec();
        events.extend_from_slice(&row.repeat(MAX_LOC_IDX + 1));
        std::fs::write(path, events).unwrap();
        assert_eq!(run(), (false, MAX_LOC_IDX + 1, vec![(user(b"c"), 0)]));
    }

    #[test]
    fn relocation_preserves_dead_bits_across_sink_overflow() {
        let mut map = RelocationMap::default();
        map.reset(1);
        map.insert(13, 7);
        let keys: Vec<u64> = (0..147).map(|i| if i % 3 == 0 { 13 } else { 5 }).collect();
        let initially_dead = [1 << 4, 1 << 2, 1 << 2];
        let mut dead = initially_dead;
        let mut moved = Vec::new();
        relocate_rows(keys.split_at(69), &mut dead, &map, |key| *key, |key, person| {
            assert_eq!((*key, person), (13, 7));
            moved.push(person);
        });
        let mut expected = initially_dead;
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            let bit = 1 << (i % 64);
            if *key == 13 && (expected[i / 64] & bit) == 0 {
                expected[i / 64] |= bit;
                count += 1;
            }
        }
        assert_eq!(moved.len(), count);
        assert_eq!(dead, expected);
    }

    #[test]
    fn radix_sort_matches_std() {
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut tmp = ArenaVec::default();
        for &n in &[0usize, 1, 2, 3, 4, 15, 16, 17, 256, 257, 4096, 100_000] {
            let mut a: Vec<CompactEv> = (0..n)
                .map(|i| CompactEv::new((next() as u32) % 1_209_600_000, i as u32))
                .collect();
            let mut b = a.clone();
            sort_ev_slice(&mut a, &mut tmp);
            b.sort_by_key(|e| e.ts());
            assert!(a.windows(2).all(|w| w[0].ts() <= w[1].ts()));
            assert_eq!(a.iter().map(|e| e.ts()).collect::<Vec<_>>(), b.iter().map(|e| e.ts()).collect::<Vec<_>>());
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(a, b);
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
