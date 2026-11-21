use super::direct::{self, EventOutput, RawClick};
use super::links::{partition, Links};
use super::{par, read, Chunks, Kind, ParseRows, ReadBuffers, Result, PARTITIONS};
use crate::proto_c::arena::{Bucket, Region, WriteLine, BLOCK};
#[cfg(not(feature = "narrow"))]
use crate::proto_c::packed::PURCHASE_LIMIT;
use crate::proto_c::packed::{self, hash, RecC, StoredEvent, StoredTok, INVALID_LOC};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const TOKEN_PARTITIONS: usize = 512;
const ROW_BITS: u32 = 18;
const ROW_MASK: u32 = (1 << ROW_BITS) - 1;
const RING: usize = 32;
const EVENT_BLOCK: usize = if cfg!(feature = "narrow") { 128 } else { BLOCK };
pub const TOKEN_BYTES: usize = std::mem::size_of::<StoredTok>();
pub const EVENT_BYTES: usize = std::mem::size_of::<StoredEvent>();

#[derive(Default)]
pub struct EventBucket<'a> {
    raw: Bucket<'a, StoredEvent, EVENT_BLOCK>,
    #[cfg(feature = "narrow")]
    extras: Bucket<'a, u64, 32>,
    #[cfg(feature = "narrow")]
    extras_offset: Vec<usize>,
}

impl EventBucket<'_> {
    pub fn len(&self) -> usize {
        self.raw.len()
    }
}

pub struct LiveEvent {
    pub key: u64,
    pub ts: u32,
    pub cc: u32,
}

pub struct Rows<'a> {
    pub events: Vec<EventBucket<'a>>,
    pub clicks: Vec<Bucket<'a, RecC>>,
    #[cfg(not(feature = "narrow"))]
    purchases: Bucket<'a, u32>,
}

impl<'a> Rows<'a> {
    fn new(buckets: usize) -> Self {
        Self {
            events: (0..buckets).map(|_| EventBucket::default()).collect(),
            clicks: (0..buckets).map(|_| Bucket::default()).collect(),
            #[cfg(not(feature = "narrow"))]
            purchases: Bucket::default(),
        }
    }

    #[inline(always)]
    fn decode_event(&self, q: usize, block: usize, row: StoredEvent) -> LiveEvent {
        #[cfg(feature = "narrow")]
        {
            let side = row.side();
            let extra = if side != 0 {
                let bucket = &self.events[q];
                *bucket
                    .extras
                    .get(bucket.extras_offset[block] + side - 1)
                    .expect("initialized event extra")
            } else {
                0
            };
            LiveEvent {
                key: row.person(),
                ts: row.seconds() * 1000 + (extra >> 32) as u32,
                cc: extra as u32,
            }
        }
        #[cfg(not(feature = "narrow"))]
        {
            let _ = (q, block);
            let purchase = row.purchase_idx();
            let cc = if purchase == 0 {
                0
            } else {
                *self
                    .purchases
                    .get(purchase - 1)
                    .expect("initialized purchase")
            };
            LiveEvent {
                key: row.person(),
                ts: row.ts(),
                cc,
            }
        }
    }

    fn event_at(&self, q: usize, offset: usize) -> (u64, u32, u32) {
        let block = offset / EVENT_BLOCK;
        let row = *self.events[q].raw.get(offset).expect("valid event location");
        let ev = self.decode_event(q, block, row);
        (packed::restore_person(q, ev.key), ev.ts, ev.cc)
    }

    pub fn for_events(&self, q: usize, mut visit: impl FnMut(LiveEvent)) {
        for (block, rows) in self.events[q].raw.blocks().enumerate() {
            for &row in rows {
                visit(self.decode_event(q, block, row));
            }
        }
    }

    #[inline(always)]
    fn push_event(
        &mut self,
        region: &'a Region<'_>,
        q: usize,
        person: u64,
        ts: u32,
        cc: u32,
        staging: Option<&mut WriteLine>,
    ) -> Result<usize> {
        let bucket = &mut self.events[q];
        #[cfg(feature = "narrow")]
        let record = {
            if bucket.len() & (EVENT_BLOCK - 1) == 0 {
                bucket.extras_offset.push(bucket.extras.len());
            }
            let seconds = ts / 1000;
            let millis = ts - seconds * 1000;
            let side = if (cc | millis) == 0 {
                0
            } else {
                let index = bucket
                    .extras
                    .push(region, u64::from(cc) | (u64::from(millis) << 32))
                    .ok_or("event extras arena exhausted")?;
                // A block has at most 128 entries and at most one extra per entry.
                (index - bucket.extras_offset[bucket.extras_offset.len() - 1] + 1) as u8
            };
            StoredEvent::new(person, seconds, side)
        };
        #[cfg(not(feature = "narrow"))]
        let record = {
            let purchase = if cc == 0 {
                0
            } else {
                let n = self
                    .purchases
                    .push(region, cc)
                    .ok_or("purchase arena exhausted")?;
                if n + 1 >= PURCHASE_LIMIT as usize {
                    return Err("more than 2,097,151 purchases on one worker");
                }
                (n + 1) as u32
            };
            StoredEvent::new(person, ts, purchase)
        };
        match staging {
            Some(line) => unsafe { bucket.raw.push_staged(region, record, line) },
            None => bucket.raw.push(region, record),
        }
        .ok_or("event arena exhausted")
    }

    #[inline(always)]
    fn push_click(
        &mut self,
        region: &'a Region<'_>,
        q: usize,
        row: RawClick,
        staging: Option<&mut WriteLine>,
    ) -> Result<()> {
        let record = RecC::new(row.person, row.cid, row.ts, row.campaign);
        match staging {
            Some(line) => unsafe { self.clicks[q].push_staged(region, record, line) },
            None => self.clicks[q].push(region, record),
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

#[derive(Clone, Copy, Default)]
struct Event {
    eid: u64,
    ingest: u32,
    person: u64,
    ts: u32,
    cc: u32,
}

struct Worker<'r, 't> {
    tid: u32,
    live: Rows<'r>,
    tokens: Vec<TokenBucket<'t>>,
    token_lines: Vec<WriteLine>,
    event_lines: Vec<WriteLine>,
    click_lines: Vec<WriteLine>,
    event_ring: [(Event, u64); RING],
    click_ring: [(RawClick, u64); RING],
    er: usize,
    cr: usize,
    error: Result<()>,
}

struct Emit<'w, 'r, 't, const INLINE: bool> {
    worker: &'w mut Worker<'r, 't>,
    links: &'w Links<'w>,
    records: &'r Region<'r>,
    tokens: &'t Region<'t>,
}

impl<'r, 't, const INLINE: bool> Emit<'_, 'r, 't, INLINE> {
    #[inline(always)]
    fn token(&mut self, eid: u64, ingest: u32, loc: u32) -> Result<()> {
        let h = hash(eid);
        let p = (h >> (64 - TOKEN_PARTITIONS.trailing_zeros())) as usize;
        let tok =
            StoredTok::pack(eid, h, ingest, loc).ok_or("ingest exceeds the narrow token window")?;
        let w = &mut self.worker;
        // Each token bucket has a distinct line indexed by its partition.
        let bucket = &mut w.tokens[p];
        let inserted = if cfg!(feature = "swcb") {
            unsafe {
                bucket
                    .raw
                    .push_staged(self.tokens, tok, &mut w.token_lines[p])
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
    fn event_row(&mut self, ev: Event, q: usize, h: u64) -> Result<()> {
        let person = packed::record_person(ev.person, h);
        let w = &mut self.worker;
        let line = if cfg!(feature = "swcb") {
            Some(&mut w.event_lines[q])
        } else {
            None
        };
        let idx = w
            .live
            .push_event(self.records, q, person, ev.ts, ev.cc, line)?;
        if idx >= ROW_MASK as usize {
            return Err("event row count exceeds the worker partition location's range");
        }
        // Loc = tid:3 | raw partition:8 | possible-link flag:1 | index:18.
        let loc = (self.worker.tid << 27)
            | (((q & (PARTITIONS - 1)) as u32) << 19)
            | (((q >= PARTITIONS) as u32) << ROW_BITS)
            | idx as u32;
        self.token(ev.eid, ev.ingest, loc)
    }

    #[inline(always)]
    fn resolve_event(&mut self, mut ev: Event, original_hash: u64) -> Result<()> {
        let person = self.links.resolve(ev.person, original_hash);
        let h = if person == ev.person {
            original_hash
        } else {
            hash(person)
        };
        ev.person = person;
        self.event_row(ev, partition(h), h)
    }

    #[inline(always)]
    fn store_click(&mut self, mut row: RawClick, q: usize, h: u64) -> Result<()> {
        row.person = packed::record_person(row.person, h);
        let w = &mut self.worker;
        let line = if cfg!(feature = "swcb") {
            Some(&mut w.click_lines[q])
        } else {
            None
        };
        w.live.push_click(self.records, q, row, line)
    }

    #[inline(always)]
    fn resolve_click(&mut self, mut row: RawClick, original_hash: u64) -> Result<()> {
        let person = self.links.resolve(row.person, original_hash);
        let h = if person == row.person {
            original_hash
        } else {
            hash(person)
        };
        row.person = person;
        self.store_click(row, partition(h), h)
    }

    #[inline(always)]
    fn add_event(&mut self, ev: Event) -> Result<()> {
        let h = hash(ev.person);
        if !self.links.possible(h) {
            return self.event_row(ev, partition(h), h);
        }
        if !INLINE {
            return self.event_row(ev, partition(h) + PARTITIONS, h);
        }

        self.links.prefetch(h);
        let i = self.worker.er & (RING - 1);
        let old = std::mem::replace(&mut self.worker.event_ring[i], (ev, h));
        self.worker.er += 1;
        if self.worker.er > RING {
            self.resolve_event(old.0, old.1)?;
        }
        Ok(())
    }

    #[inline(always)]
    fn add_click(&mut self, row: RawClick) -> Result<()> {
        let h = hash(row.person);
        if !self.links.possible(h) {
            return self.store_click(row, partition(h), h);
        }
        if !INLINE {
            return self.store_click(row, partition(h) + PARTITIONS, h);
        }

        self.links.prefetch(h);
        let i = self.worker.cr & (RING - 1);
        let old = std::mem::replace(&mut self.worker.click_ring[i], (row, h));
        self.worker.cr += 1;
        if self.worker.cr > RING {
            self.resolve_click(old.0, old.1)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.worker.error?;
        if INLINE {
            let er = self.worker.er;
            for i in er.saturating_sub(RING)..er {
                let (ev, h) = self.worker.event_ring[i & (RING - 1)];
                self.resolve_event(ev, h)?;
            }
            let cr = self.worker.cr;
            for i in cr.saturating_sub(RING)..cr {
                let (row, h) = self.worker.click_ring[i & (RING - 1)];
                self.resolve_click(row, h)?;
            }
        }
        Ok(())
    }
}

impl<const INLINE: bool> EventOutput for Emit<'_, '_, '_, INLINE> {
    #[inline(always)]
    fn valid(&mut self, eid: u64, ingest: u32, person: u64, ts: u32, cc: u32) {
        if let Err(limit) = self.add_event(Event {
            eid,
            ingest,
            person,
            ts,
            cc,
        }) {
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

pub fn parse<'r, 't, const INLINE: bool>(
    ctx: &ParseRows,
    chunks: &Chunks,
    buffers: &ReadBuffers<'_>,
    links: &Links<'_>,
    records: &'r Region<'r>,
    tokens: &'t Region<'t>,
    threads: usize,
) -> Result<Parsed<'r, 't>> {
    let buckets = if INLINE { PARTITIONS } else { 2 * PARTITIONS };
    let lines = |cap| {
        if cfg!(feature = "swcb") {
            (0..cap).map(|_| WriteLine::default()).collect()
        } else {
            Vec::new()
        }
    };
    let workers = par::run_fini(
        threads,
        chunks.main.len(),
        |tid| Worker {
            tid: tid as u32,
            live: Rows::new(buckets),
            tokens: (0..TOKEN_PARTITIONS)
                .map(|_| TokenBucket::default())
                .collect(),
            token_lines: lines(TOKEN_PARTITIONS),
            event_lines: lines(buckets),
            click_lines: lines(buckets),
            event_ring: [(Event::default(), 0); RING],
            click_ring: [(RawClick::default(), 0); RING],
            er: 0,
            cr: 0,
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
            let mut emit = Emit::<INLINE> {
                worker,
                links,
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
            let mut emit = Emit::<INLINE> {
                worker,
                links,
                records,
                tokens,
            };
            emit.worker.error = emit.finish();
            if cfg!(feature = "swcb") {
                // The same partition-indexed line was used for every push.
                unsafe {
                    for (bucket, line) in worker.tokens.iter_mut().zip(&worker.token_lines) {
                        bucket.raw.finish_staged(line);
                    }
                    for (bucket, line) in worker.live.events.iter_mut().zip(&worker.event_lines) {
                        bucket.raw.finish_staged(line);
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
    fn build(live: &[Rows<'_>], region: &'a Region<'_>) -> Result<Self> {
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

    #[inline(always)]
    fn mark(&self, loc: u32) {
        if loc == INVALID_LOC {
            return;
        }
        let tid = (loc >> 27) as usize;
        let p = ((loc >> 19) & 255) as usize;
        let bucket = p + if loc & (1 << ROW_BITS) != 0 {
            PARTITIONS
        } else {
            0
        };
        let offset = (loc & ROW_MASK) as usize;
        self.workers[tid][bucket][offset / 64].fetch_or(1 << (offset & 63), Relaxed);
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
        for (bi, block) in rows.events[q].raw.blocks().enumerate() {
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
}

fn decode_loc(live: &[Rows<'_>], loc: u32) -> Option<(u64, u32, u32)> {
    if loc == INVALID_LOC {
        return None;
    }
    let tid = (loc >> 27) as usize;
    let p = ((loc >> 19) & 255) as usize;
    let bucket = p + if loc & (1 << ROW_BITS) != 0 {
        PARTITIONS
    } else {
        0
    };
    let offset = (loc & ROW_MASK) as usize;
    Some(live[tid].event_at(bucket, offset))
}

pub fn dedup<'a>(
    tokens: &[Vec<TokenBucket<'_>>],
    live: &[Rows<'_>],
    phase: &Region<'_>,
    persistent: &'a Region<'_>,
    threads: usize,
) -> Result<Dead<'a>> {
    let dead = Dead::build(live, persistent)?;
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
                            // This offset was inserted while visiting a live
                            // slice of this partition, and is always valid.
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

pub fn relocate<'a>(
    live: &[Rows<'_>],
    dead: &Dead<'_>,
    links: &Links<'_>,
    region: &'a Region<'_>,
    threads: usize,
) -> Result<Vec<Rows<'a>>> {
    let workers = par::run(
        threads,
        PARTITIONS,
        |_| (Rows::new(PARTITIONS), Ok(())),
        |(out, error), p| {
            if error.is_err() {
                return;
            }
            let q = PARTITIONS + p;
            for (tid, rows) in live.iter().enumerate() {
                dead.for_live(rows, tid, q, |row| {
                    let raw = packed::restore_person(q, row.key);
                    let raw_hash = hash(raw);
                    let canon = links.resolve(raw, raw_hash);
                    let h = if canon == raw { raw_hash } else { hash(canon) };
                    if let Err(err) = out.push_event(
                        region,
                        partition(h),
                        packed::record_person(canon, h),
                        row.ts,
                        row.cc,
                        None,
                    ) {
                        *error = Err(err);
                    }
                });
                for &row in rows.clicks[q].rows() {
                    let raw = packed::restore_person(q, row.person());
                    let raw_hash = hash(raw);
                    let person = links.resolve(raw, raw_hash);
                    let h = if person == raw {
                        raw_hash
                    } else {
                        hash(person)
                    };
                    let bucket = partition(h);
                    let click = RawClick {
                        person: packed::record_person(person, h),
                        cid: row.cid(),
                        ts: row.ts(),
                        campaign: row.campaign(),
                    };
                    if let Err(err) = out.push_click(region, bucket, click, None) {
                        *error = Err(err);
                        return;
                    }
                }
            }
        },
    );
    workers
        .into_iter()
        .map(|(rows, status)| status.map(|_| rows))
        .collect()
}
