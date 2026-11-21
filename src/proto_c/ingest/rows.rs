use super::direct::{self, EventOutput, RawClick};
use super::links::{partition, Links};
use super::{par, read, Chunks, Kind, ParseRows, ReadBuffers, Result, PARTITIONS};
use crate::proto_c::arena::{Bucket, Region, WriteLine, BLOCK};
use crate::proto_c::packed::{self, hash, RecC, StoredEvent, StoredTok, INVALID_LOC};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const TOKEN_PARTITIONS: usize = 512;
const ROW_BITS: u32 = 18;
const ROW_MASK: u32 = (1 << ROW_BITS) - 1;
const EVENT_BLOCK: usize = 128;
pub const TOKEN_BYTES: usize = std::mem::size_of::<StoredTok>();
pub const EVENT_BYTES: usize = std::mem::size_of::<StoredEvent>();

#[derive(Default)]
struct EventExtras<'a> {
    extras: Bucket<'a, u64, 32>,
    offsets: Vec<usize>,
}

pub struct LiveEvent {
    pub key: u64,
    pub ts: u32,
    pub cc: u32,
}

pub struct Rows<'a> {
    pub events: Vec<Bucket<'a, StoredEvent, EVENT_BLOCK>>,
    event_extras: Vec<EventExtras<'a>>,
    pub clicks: Vec<Bucket<'a, RecC>>,
}

impl<'a> Rows<'a> {
    fn new(buckets: usize) -> Self {
        Self {
            events: (0..buckets).map(|_| Bucket::default()).collect(),
            event_extras: (0..buckets).map(|_| EventExtras::default()).collect(),
            clicks: (0..buckets).map(|_| Bucket::default()).collect(),
        }
    }

    #[inline(always)]
    fn extra_of(&self, q: usize, block: usize, side: usize) -> u64 {
        let ex = unsafe { self.event_extras.get_unchecked(q) };
        *ex.extras
            .get(unsafe { *ex.offsets.get_unchecked(block) } + side - 1)
            .expect("initialized event extra")
    }

    #[inline(always)]
    fn decode_event(&self, q: usize, block: usize, row: StoredEvent) -> LiveEvent {
        let side = row.side();
        let extra = if side != 0 {
            self.extra_of(q, block, side)
        } else {
            0
        };
        LiveEvent {
            key: row.person(),
            ts: row.seconds() * 1000 + (extra >> 32) as u32,
            cc: extra as u32,
        }
    }

    fn event_at(&self, q: usize, offset: usize) -> (u64, u32, u32) {
        let block = offset / EVENT_BLOCK;
        let row = *self.events[q].get(offset).expect("valid event location");
        let ev = self.decode_event(q, block, row);
        (packed::restore_person(q, ev.key), ev.ts, ev.cc)
    }

    pub fn for_events(&self, q: usize, mut visit: impl FnMut(LiveEvent)) {
        for (block, rows) in self.events[q].blocks().enumerate() {
            for &row in rows {
                visit(self.decode_event(q, block, row));
            }
        }
    }

    #[inline(always)]
    fn push_event_extra(
        &mut self,
        region: &'a Region<'_>,
        q: usize,
        person: u64,
        seconds: u32,
        extra: u64,
        staging: Option<&mut WriteLine<64>>,
    ) -> Result<usize> {
        let raw_bucket = unsafe { self.events.get_unchecked_mut(q) };
        let len = raw_bucket.len();
        if (len & (EVENT_BLOCK - 1)) == 0 {
            let ex = unsafe { self.event_extras.get_unchecked_mut(q) };
            ex.offsets.push(ex.extras.len());
        }
        let side = if extra == 0 {
            0
        } else {
            let ex = unsafe { self.event_extras.get_unchecked_mut(q) };
            let index = ex
                .extras
                .push(region, extra)
                .ok_or("event extras arena exhausted")?;
            (index - *ex.offsets.last().unwrap() + 1) as u8
        };
        let record = StoredEvent::new(person, seconds, side);
        match staging {
            Some(line) => unsafe { raw_bucket.push_staged(region, record, line) },
            None => raw_bucket.push(region, record),
        }
        .ok_or("event arena exhausted")
    }

    #[inline(always)]
    fn push_event(
        &mut self,
        region: &'a Region<'_>,
        q: usize,
        person: u64,
        ts: u32,
        cc: u32,
        staging: Option<&mut WriteLine<64>>,
    ) -> Result<usize> {
        let seconds = ts / 1000;
        let millis = ts - seconds * 1000;
        let extra = if (cc | millis) == 0 {
            0
        } else {
            u64::from(cc) | (u64::from(millis) << 32)
        };
        self.push_event_extra(region, q, person, seconds, extra, staging)
    }

    #[inline(always)]
    fn push_click(
        &mut self,
        region: &'a Region<'_>,
        q: usize,
        row: RawClick,
        staging: Option<&mut WriteLine<64>>,
    ) -> Result<()> {
        let record = RecC::new(row.person, row.cid, row.ts, row.campaign);
        let bucket = unsafe { self.clicks.get_unchecked_mut(q) };
        match staging {
            Some(line) => unsafe { bucket.push_staged(region, record, line) },
            None => bucket.push(region, record),
        }
        .map(|_| ())
        .ok_or("click arena exhausted")
    }
}

#[derive(Default)]
pub struct TokenBucket<'a> {
    raw: Bucket<'a, StoredTok>,
    overflow: Vec<(usize, u32)>,
}

impl TokenBucket<'_> {
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    pub fn overflow_count(&self) -> usize {
        self.overflow.len()
    }
}

struct TokenBlock<'a> {
    rows: &'a [StoredTok],
    tid: u32,
    offset: usize,
    overflow: &'a [(usize, u32)],
}

impl TokenBlock<'_> {
    #[inline(always)]
    fn location(&self, index: usize, row: StoredTok) -> u32 {
        let loc = row.location(self.tid);
        if loc != packed::OVERFLOW_LOC {
            loc
        } else {
            let index = self.offset + index;
            let slot = self
                .overflow
                .binary_search_by_key(&index, |&(i, _)| i)
                .expect("overflow marker has a full location");
            self.overflow[slot].1
        }
    }
}

pub struct Parsed<'r, 't> {
    pub live: Vec<Rows<'r>>,
    pub tokens: Vec<Vec<TokenBucket<'t>>>,
}

struct Worker<'r, 't> {
    tid: u32,
    live: Rows<'r>,
    tokens: Vec<TokenBucket<'t>>,
    token_lines: Vec<WriteLine<320>>,
    event_lines: Vec<WriteLine<64>>,
    click_lines: Vec<WriteLine<64>>,
    error: Result<()>,
}

struct Emit<'w, 'r, 't> {
    worker: &'w mut Worker<'r, 't>,
    records: &'r Region<'r>,
    tokens: &'t Region<'t>,
}

impl<'r, 't> Emit<'_, 'r, 't> {
    #[inline(always)]
    fn token(&mut self, eid: u64, ingest: u32, loc: u32) -> Result<()> {
        let h = hash(eid);
        let p = (h >> (64 - TOKEN_PARTITIONS.trailing_zeros())) as usize;
        let tok =
            StoredTok::pack(eid, h, ingest, loc).ok_or("ingest exceeds the narrow token window")?;
        let w = &mut self.worker;
        let bucket = unsafe { w.tokens.get_unchecked_mut(p) };
        let inserted = if cfg!(feature = "swcb") {
            unsafe {
                bucket
                    .raw
                    .push_staged(self.tokens, tok, w.token_lines.get_unchecked_mut(p))
            }
        } else {
            bucket.raw.push(self.tokens, tok)
        };
        let index = inserted.ok_or("token arena exhausted")?;
        if packed::needs_loc_side(loc) {
            bucket.overflow.push((index, loc));
        }
        Ok(())
    }

    #[inline(always)]
    fn add_event(&mut self, eid: u64, ingest: u32, user: u64, ts: u32, cc: u32) -> Result<()> {
        let h = hash(user);
        let q = partition(h);
        let person = packed::record_person(user, h);
        let w = &mut self.worker;
        let line = if cfg!(feature = "swcb") {
            Some(unsafe { w.event_lines.get_unchecked_mut(q) })
        } else {
            None
        };
        let idx = w.live.push_event(self.records, q, person, ts, cc, line)?;
        if idx >= ROW_MASK as usize {
            return Err("event row count exceeds the worker partition location's range");
        }
        // Loc = tid:3 | raw partition:8 | 0:1 | index:18.
        let loc = (w.tid << 27) | ((q as u32) << 19) | idx as u32;
        self.token(eid, ingest, loc)
    }

    #[inline(always)]
    fn add_click(&mut self, mut row: RawClick) -> Result<()> {
        let h = hash(row.person);
        let q = partition(h);
        row.person = packed::record_person(row.person, h);
        let w = &mut self.worker;
        let line = if cfg!(feature = "swcb") {
            Some(unsafe { w.click_lines.get_unchecked_mut(q) })
        } else {
            None
        };
        w.live.push_click(self.records, q, row, line)
    }
}

impl EventOutput for Emit<'_, '_, '_> {
    #[inline(always)]
    fn valid(&mut self, eid: u64, ingest: u32, person: u64, ts: u32, cc: u32) {
        if let Err(limit) = self.add_event(eid, ingest, person, ts, cc) {
            self.worker.error = Err(limit);
        }
    }

    #[inline(always)]
    fn invalid(&mut self, eid: u64, ingest: u32) {
        if let Err(limit) = self.token(eid, ingest, INVALID_LOC) {
            self.worker.error = Err(limit);
        }
    }

    fn unsupported(&mut self, limit: &'static str) {
        self.worker.error = Err(limit);
    }
}

pub fn parse<'r, 't>(
    ctx: &ParseRows,
    chunks: &Chunks,
    buffers: &ReadBuffers<'_>,
    records: &'r Region<'r>,
    tokens: &'t Region<'t>,
    threads: usize,
) -> Result<Parsed<'r, 't>> {
    let workers = par::run_fini(
        threads,
        chunks.main.len(),
        |tid| Worker {
            tid: tid as u32,
            live: Rows::new(PARTITIONS),
            tokens: (0..TOKEN_PARTITIONS)
                .map(|_| TokenBucket::default())
                .collect(),
            token_lines: if cfg!(feature = "swcb") {
                (0..TOKEN_PARTITIONS).map(|_| WriteLine::default()).collect()
            } else {
                Vec::new()
            },
            event_lines: if cfg!(feature = "swcb") {
                (0..PARTITIONS).map(|_| WriteLine::default()).collect()
            } else {
                Vec::new()
            },
            click_lines: if cfg!(feature = "swcb") {
                (0..PARTITIONS).map(|_| WriteLine::default()).collect()
            } else {
                Vec::new()
            },
            error: Ok(()),
        },
        |worker, i| {
            if worker.error.is_err() {
                return;
            }
            let chunk = chunks.main[i];
            let (buf, lo, hi) = match unsafe { read(chunks, chunk, buffers, worker.tid as usize) } {
                Ok(chunk) => chunk,
                Err(err) => {
                    worker.error = Err(err);
                    return;
                }
            };
            let mut emit = Emit {
                worker,
                records,
                tokens,
            };
            match chunks.files[chunk.file as usize].kind {
                Kind::Events => direct::events(ctx, buf, lo, hi, &mut emit),
                Kind::Clicks => {
                    let result = direct::clicks(ctx, buf, lo, hi, |row| {
                        if let Err(limit) = emit.add_click(row) {
                            emit.worker.error = Err(limit);
                        }
                    });
                    if result.is_err() {
                        emit.worker.error = result;
                    }
                }
                Kind::Links => unreachable!(),
            }
        },
        |worker| {
            if cfg!(feature = "swcb") {
                unsafe {
                    for (bucket, line) in worker.tokens.iter_mut().zip(&worker.token_lines) {
                        bucket.raw.finish_staged(line);
                    }
                    for (bucket, line) in worker.live.events.iter_mut().zip(&worker.event_lines) {
                        bucket.finish_staged(line);
                    }
                    for (bucket, line) in worker.live.clicks.iter_mut().zip(&worker.click_lines) {
                        bucket.finish_staged(line);
                    }
                    std::arch::x86_64::_mm_sfence();
                }
            }
        },
    );
    let mut parsed = Parsed {
        live: Vec::with_capacity(threads),
        tokens: Vec::with_capacity(threads),
    };
    for wi in workers {
        wi.error?;
        parsed.live.push(wi.live);
        parsed.tokens.push(wi.tokens);
    }
    Ok(parsed)
}

pub struct Dead<'a> {
    workers: Vec<Vec<&'a [AtomicU64]>>,
}

impl<'a> Dead<'a> {
    fn build_events(live: &[Rows<'_>], region: &'a Region<'_>) -> Result<Self> {
        let workers = live
            .iter()
            .map(|w| {
                w.events
                    .iter()
                    .map(|b| {
                        region
                            .init(b.len().div_ceil(64), |_| AtomicU64::new(0))
                            .map(|w| &*w)
                            .ok_or("dedup loser bitmap arena exhausted")
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { workers })
    }

    fn build_clicks(live: &[Rows<'_>], region: &'a Region<'_>) -> Result<Self> {
        let workers = live
            .iter()
            .map(|w| {
                w.clicks
                    .iter()
                    .map(|b| {
                        region
                            .init(b.len().div_ceil(64), |_| AtomicU64::new(0))
                            .map(|w| &*w)
                            .ok_or("click loser bitmap arena exhausted")
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { workers })
    }

    #[inline(always)]
    fn mark(&self, loc: u32) {
        if loc == INVALID_LOC {
            return;
        }
        let tid = (loc >> 27) as usize;
        let q = ((loc >> 19) & 255) as usize;
        let offset = (loc & ROW_MASK) as usize;
        self.workers[tid][q][offset / 64].fetch_or(1 << (offset & 63), Relaxed);
    }

    pub fn count(&self, tid: usize, q: usize) -> usize {
        self.workers[tid][q]
            .iter()
            .map(|w| w.load(Relaxed).count_ones() as usize)
            .sum()
    }

    #[inline]
    pub fn for_live(
        &self,
        rows: &Rows<'_>,
        tid: usize,
        q: usize,
        mut visit: impl FnMut(LiveEvent),
    ) {
        let mut words = self.workers[tid][q].iter();
        for (bi, block) in rows.events[q].blocks().enumerate() {
            for slice in block.chunks(64) {
                let dead = words.next().unwrap().load(Relaxed);
                if dead == 0 {
                    for &row in slice {
                        visit(rows.decode_event(q, bi, row));
                    }
                } else {
                    let mut keep = !dead & (u64::MAX >> (64 - slice.len()));
                    while keep != 0 {
                        let i = keep.trailing_zeros() as usize;
                        visit(rows.decode_event(q, bi, unsafe { *slice.get_unchecked(i) }));
                        keep &= keep - 1;
                    }
                }
            }
        }
    }

    #[inline]
    pub fn for_live_clicks(
        &self,
        rows: &Rows<'_>,
        tid: usize,
        q: usize,
        mut visit: impl FnMut(RecC),
    ) {
        let mut words = self.workers[tid][q].iter();
        for block in rows.clicks[q].blocks() {
            for slice in block.chunks(64) {
                let dead = words.next().unwrap().load(Relaxed);
                if dead == 0 {
                    for &row in slice {
                        visit(row);
                    }
                } else {
                    let mut keep = !dead & (u64::MAX >> (64 - slice.len()));
                    while keep != 0 {
                        let i = keep.trailing_zeros() as usize;
                        visit(unsafe { *slice.get_unchecked(i) });
                        keep &= keep - 1;
                    }
                }
            }
        }
    }
}

fn decode_loc(live: &[Rows<'_>], loc: u32) -> Option<(u64, u32, u32)> {
    if loc == INVALID_LOC {
        return None;
    }
    let tid = (loc >> 27) as usize;
    let q = ((loc >> 19) & 255) as usize;
    let offset = (loc & ROW_MASK) as usize;
    Some(live[tid].event_at(q, offset))
}

pub fn dedup<'a>(
    tokens: &[Vec<TokenBucket<'_>>],
    live: &[Rows<'_>],
    phase: &Region<'_>,
    persistent: &'a Region<'_>,
    threads: usize,
) -> Result<Dead<'a>> {
    let dead = Dead::build_events(live, persistent)?;
    let max = (0..TOKEN_PARTITIONS)
        .map(|p| tokens.iter().map(|t| t[p].len()).sum::<usize>())
        .max()
        .unwrap_or(0);
    let blocks_max = (0..TOKEN_PARTITIONS)
        .map(|p| {
            tokens
                .iter()
                .map(|w| w[p].len().div_ceil(BLOCK))
                .sum::<usize>()
        })
        .max()
        .unwrap_or(0);
    const INDEX_BITS: u32 = 21;
    const INDEX_MASK: u32 = (1 << INDEX_BITS) - 1;
    if blocks_max * BLOCK >= 1 << INDEX_BITS {
        return Err("more than 2,097,152 token slots in one partition");
    }
    let cap = (max * 2).next_power_of_two();
    let errors = par::run(
        threads,
        TOKEN_PARTITIONS,
        |_| {
            (
                phase.slice(cap, 0u32),
                Vec::<TokenBlock<'_>>::with_capacity(blocks_max),
                false,
            )
        },
        |(table, blocks, tie_conflict), p| {
            let Some(table) = table else { return };
            if *tie_conflict {
                return;
            }
            blocks.clear();
            blocks.extend(tokens.iter().enumerate().flat_map(|(tid, w)| {
                w[p].raw
                    .blocks()
                    .enumerate()
                    .map(move |(bi, rows)| TokenBlock {
                        rows,
                        tid: tid as u32,
                        offset: bi * BLOCK,
                        overflow: &w[p].overflow,
                    })
            }));
            let count = blocks.iter().map(|b| b.rows.len()).sum::<usize>();
            let cap = (count * 2).next_power_of_two();
            let table = &mut table[..cap];
            table.fill(0);
            let mask = cap - 1;
            for (bi, block) in blocks.iter().enumerate() {
                for (i, &row) in block.rows.iter().enumerate() {
                    let eid = row.key();
                    let h = eid.wrapping_mul(0x9e37_79b9_7f4a_7c15);
                    let fingerprint = (h >> 53) as u32;
                    let here = (fingerprint << INDEX_BITS) | (bi * BLOCK + i + 1) as u32;
                    let mut slot = h as usize & mask;
                    loop {
                        let old = unsafe { *table.get_unchecked(slot) };
                        if old == 0 {
                            unsafe { *table.get_unchecked_mut(slot) = here };
                            break;
                        }
                        if old >> INDEX_BITS == fingerprint {
                            let offset = (old & INDEX_MASK) as usize - 1;
                            let winner_block = unsafe { blocks.get_unchecked(offset / BLOCK) };
                            let winner_index = offset & (BLOCK - 1);
                            let winner = unsafe { *winner_block.rows.get_unchecked(winner_index) };
                            if winner.key() == eid {
                                let w_loc = winner_block.location(winner_index, winner);
                                let r_loc = block.location(i, row);
                                if row.ingest() < winner.ingest() {
                                    dead.mark(w_loc);
                                    unsafe { *table.get_unchecked_mut(slot) = here };
                                } else {
                                    if row.ingest() == winner.ingest()
                                        && decode_loc(live, w_loc) != decode_loc(live, r_loc)
                                    {
                                        *tie_conflict = true;
                                        return;
                                    }
                                    dead.mark(r_loc);
                                }
                                break;
                            }
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
        },
    );
    if errors.iter().any(|(table, _, _)| table.is_none()) {
        return Err("event dedup hash table arena exhausted");
    }
    if errors.iter().any(|(_, _, tie)| *tie) {
        return Err("exact-order: equal-ingest event tie with differing payload");
    }
    Ok(dead)
}

pub fn relocate<'a, 'b>(
    live: &[Rows<'_>],
    dead: &Dead<'_>,
    links: &Links<'_>,
    phase: &'a Region<'_>,
    persistent: &'b Region<'_>,
    threads: usize,
) -> Result<(Vec<Rows<'a>>, Dead<'b>)> {
    let dead_clicks = Dead::build_clicks(live, persistent)?;
    let workers = par::run(
        threads,
        PARTITIONS,
        |_| (Rows::new(PARTITIONS), Ok(())),
        |(out, error), q| {
            if error.is_err() || links.shard_empty(q) {
                return;
            }
            for (tid, rows) in live.iter().enumerate() {
                // Events: 8-wide prefetch into L2-hot shard q; move only linked rows.
                let mut ewords = dead.workers[tid][q].iter();
                for (bi, block) in rows.events[q].blocks().enumerate() {
                    for slice in block.chunks(64) {
                        let word = ewords.next().unwrap();
                        let mask = word.load(Relaxed);
                        let mut moved = 0u64;
                        let mut done = 0usize;
                        for grp in slice.chunks_exact(8) {
                            let slots: [usize; 8] =
                                std::array::from_fn(|i| links.prefetch_perm(q, grp[i].person()));
                            let m8 = (mask >> done) as u8;
                            if m8 == 0 {
                                for (i, (&row, slot)) in grp.iter().zip(slots).enumerate() {
                                    if let Some((cq, cperm)) =
                                        links.resolve_perm_slot(q, row.person(), slot)
                                    {
                                        let extra = if row.side() != 0 {
                                            rows.extra_of(q, bi, row.side())
                                        } else {
                                            0
                                        };
                                        if let Err(err) = out.push_event_extra(
                                            phase,
                                            cq,
                                            cperm,
                                            row.seconds(),
                                            extra,
                                            None,
                                        ) {
                                            *error = Err(err);
                                            return;
                                        }
                                        moved |= 1u64 << (done + i);
                                    }
                                }
                            } else {
                                for (i, (&row, slot)) in grp.iter().zip(slots).enumerate() {
                                    if (m8 & (1 << i)) == 0 {
                                        if let Some((cq, cperm)) =
                                            links.resolve_perm_slot(q, row.person(), slot)
                                        {
                                            let extra = if row.side() != 0 {
                                                rows.extra_of(q, bi, row.side())
                                            } else {
                                                0
                                            };
                                            if let Err(err) = out.push_event_extra(
                                                phase,
                                                cq,
                                                cperm,
                                                row.seconds(),
                                                extra,
                                                None,
                                            ) {
                                                *error = Err(err);
                                                return;
                                            }
                                            moved |= 1u64 << (done + i);
                                        }
                                    }
                                }
                            }
                            done += 8;
                        }
                        for (i, &row) in slice[done..].iter().enumerate() {
                            if (mask & (1u64 << (done + i))) == 0 {
                                let slot = links.prefetch_perm(q, row.person());
                                if let Some((cq, cperm)) =
                                    links.resolve_perm_slot(q, row.person(), slot)
                                {
                                    let extra = if row.side() != 0 {
                                        rows.extra_of(q, bi, row.side())
                                    } else {
                                        0
                                    };
                                    if let Err(err) = out.push_event_extra(
                                        phase,
                                        cq,
                                        cperm,
                                        row.seconds(),
                                        extra,
                                        None,
                                    ) {
                                        *error = Err(err);
                                        return;
                                    }
                                    moved |= 1u64 << (done + i);
                                }
                            }
                        }
                        if moved != 0 {
                            word.store(mask | moved, Relaxed);
                        }
                    }
                }

                // Clicks: 8-wide prefetch into L2-hot shard q; move only linked clicks.
                let mut cwords = dead_clicks.workers[tid][q].iter();
                for block in rows.clicks[q].blocks() {
                    for slice in block.chunks(64) {
                        let word = cwords.next().unwrap();
                        let mut moved = 0u64;
                        let mut done = 0usize;
                        for grp in slice.chunks_exact(8) {
                            let slots: [usize; 8] =
                                std::array::from_fn(|i| links.prefetch_perm(q, grp[i].person()));
                            for (i, (&row, slot)) in grp.iter().zip(slots).enumerate() {
                                if let Some((cq, cperm)) =
                                    links.resolve_perm_slot(q, row.person(), slot)
                                {
                                    let click = RawClick {
                                        person: cperm,
                                        cid: row.cid(),
                                        ts: row.ts(),
                                        campaign: row.campaign(),
                                    };
                                    if let Err(err) = out.push_click(phase, cq, click, None) {
                                        *error = Err(err);
                                        return;
                                    }
                                    moved |= 1u64 << (done + i);
                                }
                            }
                            done += 8;
                        }
                        for (i, &row) in slice[done..].iter().enumerate() {
                            let slot = links.prefetch_perm(q, row.person());
                            if let Some((cq, cperm)) =
                                links.resolve_perm_slot(q, row.person(), slot)
                            {
                                let click = RawClick {
                                    person: cperm,
                                    cid: row.cid(),
                                    ts: row.ts(),
                                    campaign: row.campaign(),
                                };
                                if let Err(err) = out.push_click(phase, cq, click, None) {
                                    *error = Err(err);
                                    return;
                                }
                                moved |= 1u64 << (done + i);
                            }
                        }
                        if moved != 0 {
                            word.store(moved, Relaxed);
                        }
                    }
                }
            }
        },
    );
    let relocated = workers
        .into_iter()
        .map(|(rows, status)| status.map(|_| rows))
        .collect::<Result<Vec<_>>>()?;
    Ok((relocated, dead_clicks))
}
