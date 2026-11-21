//! CSR, sessionization and campaign rollups adapted from candidate-12a.
use super::par::{self, Timer};
use super::rows::{Dead, LiveEvent, Rows};
use super::{ParseRows, Result, PARTITIONS};
use crate::proto_c::aggregate::{self, output, CampStore, NO_CATEGORY};
use crate::proto_c::arena::{Region, Scratch};
use crate::proto_c::packed::{decode, hash, restore_person};
use std::path::Path;

const WEEK_MS_U32: u32 = super::WEEK as u32;
const GAP_MS_U32: u32 = 1_800_000;
const DAY_MS_U32: u32 = super::ts::DAY_MS as u32;

#[derive(Clone, Copy)]
struct GroupEntry {
    key: u64,
    id: u32,
    count: u32,
}

impl Default for GroupEntry {
    fn default() -> Self {
        Self {
            key: u64::default(),
            id: 0,
            count: 0,
        }
    }
}

// Compact event counts share the probed slot with the person key and ID.
#[inline(always)]
fn group_hash(key: u64) -> usize {
    if cfg!(feature = "narrow") {
        key as usize
    } else {
        hash(key) as usize
    }
}

struct GroupMap {
    entries: Vec<GroupEntry>,
    person_slots: Vec<usize>,
    mask: usize,
}

impl GroupMap {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            person_slots: Vec::new(),
            mask: 0,
        }
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
            let mut i = group_hash(entry.key) & self.mask;
            while self.entries[i].count != 0 {
                i = (i + 1) & self.mask;
            }
            self.entries[i] = entry;
            *pos = i;
        }
    }

    #[inline(always)]
    fn count(&mut self, key: u64) -> u32 {
        let mut i = group_hash(key) & self.mask;
        loop {
            let entry = unsafe { self.entries.get_unchecked_mut(i) };
            if (entry.key == key) & (entry.count != 0) {
                entry.count += 1;
                return entry.id;
            }
            if entry.count == 0 {
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
    fn get(&self, key: u64) -> Option<u32> {
        let mut i = group_hash(key) & self.mask;
        loop {
            let entry = unsafe { self.entries.get_unchecked(i) };
            if (entry.key == key) & (entry.count != 0) {
                return Some(entry.id);
            }
            if entry.count == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn person(&self, id: usize) -> GroupEntry {
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
struct CompactClick {
    ts: u32,
    campaign: u32,
    cid: u64,
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
fn sort_and_dedup_clicks(cl: &mut [CompactClick]) -> usize {
    let n = cl.len();
    if n <= 1 {
        return n;
    }
    if n <= 16 {
        for i in 1..n {
            let v = cl[i];
            let mut j = i;
            while j > 0 && (cl[j - 1].ts > v.ts || (cl[j - 1].ts == v.ts && cl[j - 1].cid > v.cid))
            {
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
    fn count(&mut self, camp: &mut CampStore, clicks: &[CompactClick]) {
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
            let begin = if seen.0 == stamp {
                first.max(seen.1 as usize)
            } else {
                first
            };
            if begin >= end {
                continue;
            }
            *seen = (stamp, end as u32);
            let days = &self.s_day[begin..end];
            let row = camp.row(click.campaign);
            for &day in days {
                // Session starts are always inside the window.
                unsafe { row.get_unchecked_mut(day as usize) }.assisted += 1;
            }
        }
    }
}

struct FusedWorker<'a> {
    map: GroupMap,
    fill: Vec<u32>,
    pid: Scratch<'a, u32>,
    ev_flat: Scratch<'a, CompactEv>,
    ev_buf: Scratch<'a, CompactEv>,
    ev_tmp: Vec<CompactEv>,
    cl_raw: Scratch<'a, (u32, CompactClick)>,
    ck_cnt: Vec<u32>,
    ck_off: Vec<u32>,
    cl_buf: Scratch<'a, CompactClick>,
    camp: CampStore<'a>,
    writer: output::UserWriter<'a>,
    cat_sum: Vec<u64>,
    cat_touched: Vec<u32>,
    assists: FusedAssists,
    np: usize,
    nc: usize,
    written: usize,
}

impl FusedWorker<'_> {
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
        if self
            .writer
            .push_group_row(id_bytes, day, sessions, events, revenue, cat)
        {
            self.writer.flush_group();
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

pub fn run(
    ctx: &ParseRows,
    input: &[Rows<'_>],
    dead: &Dead<'_>,
    dead_clicks: Option<&Dead<'_>>,
    relocated: &[Rows<'_>],
    region: &Region<'_>,
    output_dir: &Path,
    ne_raw: usize,
    threads: usize,
    tm: &mut Timer,
) -> Result<()> {
    let ud = output_dir.join("user_daily");
    let cd = output_dir.join("campaign_daily");
    let mut ev_cnt_part = [0; PARTITIONS];
    let mut click_cnt_part = [0; PARTITIONS];
    for (q, count) in ev_cnt_part.iter_mut().enumerate() {
        *count = input
            .iter()
            .enumerate()
            .map(|(tid, w)| w.events[q].len() - dead.count(tid, q))
            .sum::<usize>()
            + relocated.iter().map(|w| w.events[q].len()).sum::<usize>();
        if *count > 0 {
            click_cnt_part[q] = input
                .iter()
                .enumerate()
                .map(|(tid, w)| {
                    w.clicks[q].len() - dead_clicks.map_or(0, |dc| dc.count(tid, q))
                })
                .sum::<usize>()
                + relocated.iter().map(|w| w.clicks[q].len()).sum::<usize>();
        }
    }
    let ne = ev_cnt_part.iter().sum::<usize>();
    let cost = |q: usize| ev_cnt_part[q] + 2 * click_cnt_part[q];
    let mut q_order: [usize; PARTITIONS] = std::array::from_fn(|i| i);
    q_order.sort_unstable_by_key(|&q| std::cmp::Reverse(cost(q)));

    let day0 = aggregate::day_of(ctx.ws);
    let ws_rem = ctx.ws.rem_euclid(super::ts::DAY_MS) as u32;
    let ndays = if ctx.we > ctx.ws {
        (aggregate::day_of(ctx.we - 1) - day0 + 1) as usize
    } else {
        0
    };
    let campaigns = &ctx.lookups.campaigns;
    let organic = campaigns.len() as u32;
    let categories = &ctx.lookups.categories;
    let max_cat = categories.iter().map(Vec::len).max().unwrap_or(0);
    let est_user_rows = ne / threads / 8 + 1024;
    let file_cap = (est_user_rows * 24).clamp(512 << 10, 2 << 20);
    let campaign_file = region
        .uninit(8 << 20)
        .ok_or("campaign output arena exhausted")?;
    let max_events = ev_cnt_part.iter().copied().max().unwrap_or(0);
    let max_clicks = click_cnt_part.iter().copied().max().unwrap_or(0);

    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");

    let q_order = &q_order;
    let ev_cnt_part = &ev_cnt_part;
    let active_parts = q_order
        .iter()
        .take_while(|&&q| ev_cnt_part[q] > 0)
        .count();

    let workers = par::run_fini(
        threads,
        active_parts,
        |tid| -> Result<FusedWorker<'_>> {
            let scratch = "grouping scratch arena exhausted";
            Ok(FusedWorker {
                map: GroupMap::new(),
                fill: Vec::with_capacity(2048),
                pid: Scratch::new(region, max_events).ok_or(scratch)?,
                ev_flat: Scratch::new(region, max_events).ok_or(scratch)?,
                ev_buf: Scratch::new(region, max_events).ok_or(scratch)?,
                ev_tmp: Vec::new(),
                cl_raw: Scratch::new(region, max_clicks).ok_or(scratch)?,
                ck_cnt: Vec::with_capacity(2048),
                ck_off: Vec::with_capacity(2048),
                cl_buf: Scratch::new(region, max_clicks).ok_or(scratch)?,
                camp: CampStore::new(region, campaigns.len() + 1, day0, ndays).ok_or(scratch)?,
                writer: output::UserWriter::new(&ud, tid, region, max_cat, file_cap)
                    .ok_or("user output arena exhausted")?,
                cat_sum: vec![0; categories.len()],
                cat_touched: Vec::with_capacity(32),
                assists: FusedAssists::new(campaigns.len()),
                np: 0,
                nc: 0,
                written: 0,
            })
        },
        |worker, idx| {
            let Ok(w) = worker.as_mut() else {
                return;
            };
            let q = q_order[idx];
            let n = ev_cnt_part[q];
            w.map.reset(n / 25 + 16);
            w.pid.clear();
            w.pid.reserve(n);
            w.ev_flat.clear();
            w.ev_flat.reserve(n);
            let pid = w.pid.as_mut_ptr();
            let ev_flat = w.ev_flat.as_mut_ptr();
            let mut live = 0;
            let mut visit = |r: LiveEvent| {
                let id = w.map.count(r.key);
                let ev = CompactEv::new(r.ts, r.cc);
                // The count pass reserved exactly one slot per live event.
                unsafe {
                    pid.add(live).write(id);
                    ev_flat.add(live).write(ev);
                }
                live += 1;
            };
            for (tid, rows) in input.iter().enumerate() {
                dead.for_live(rows, tid, q, &mut visit);
            }
            for rows in relocated {
                rows.for_events(q, &mut visit);
            }
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
            let mut visit_ck = |r: crate::proto_c::packed::RecC| {
                if let Some(id) = w.map.get(r.person()) {
                    w.cl_raw.push((
                        id,
                        CompactClick {
                            ts: r.ts(),
                            campaign: r.campaign(),
                            cid: r.cid(),
                        },
                    ));
                    w.ck_cnt[id as usize] += 1;
                }
            };
            if let Some(dc) = dead_clicks {
                for (tid, rows) in input.iter().enumerate() {
                    dc.for_live_clicks(rows, tid, q, &mut visit_ck);
                }
            } else {
                for rows in input {
                    for &r in rows.clicks[q].rows() {
                        visit_ck(r);
                    }
                }
            }
            for rows in relocated {
                for &r in rows.clicks[q].rows() {
                    visit_ck(r);
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
                let id_len = decode(restore_person(q, person.key), b'u', &mut id_buf);
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
                            w.flush_user_day(
                                id_bytes,
                                categories,
                                day0.wrapping_add(rel_day as i32),
                                ud_sessions,
                                ud_events,
                                ud_revenue,
                            );
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
                w.flush_user_day(
                    id_bytes,
                    categories,
                    day0.wrapping_add(rel_day as i32),
                    ud_sessions,
                    ud_events,
                    ud_revenue,
                );

                if cl_len > 0 {
                    w.assists.count(&mut w.camp, &w.cl_buf[cl_st..cl_end]);
                }
                ev_st = ev_en;
            }
        },
        |worker| {
            if let Ok(w) = worker.as_mut() {
                w.written = w.writer.finish();
            }
        },
    )
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    let np = workers.iter().map(|w| w.np).sum::<usize>();
    let nc = workers.iter().map(|w| w.nc).sum::<usize>();
    let user_rows = workers.iter().map(|w| w.written).sum::<usize>();
    if user_rows == 0 {
        output::write_empty_user(&ud).expect("writing empty user_daily");
    }
    let camp_rows = aggregate::merge_camps(workers.into_iter().map(|w| w.camp));
    tm.lap("group+agg");
    output::write_campaign_raw(&cd, &camp_rows, campaigns, campaign_file)
        .expect("writing campaign_daily");
    tm.lap("campaign-write");
    eprintln!("[group] persons={np} events={ne} raw_event_tokens={ne_raw} clicks={nc} user_rows={user_rows} campaign_rows={}", camp_rows.len());
    Ok(())
}
