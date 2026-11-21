//! Sessionization, attribution, assists and daily rollups. Owner: aggregate agent.
//!
//! Input: person-grouped CSR from ingest (see docs/INTERFACE.md). Each person's events
//! are sorted by ts and clicks by (ts, click_id). Persons are processed in parallel
//! (chunks of persons balanced by event count, largest first, dynamic dispatch).
//! Every thread keeps its own user_daily rows and a dense (campaign x day) accumulator;
//! accumulators are merged at the end.
//!
//! Assists use interval merging per (person, campaign): campaign `c` assists a session
//! starting at `s` iff `s` lies in the union of `[t, t + 7d]` over `c`'s clicks `t`.
//! Clicks arrive in ts order, so the union is built incrementally; each merged interval
//! is intersected with the sorted session starts by binary search and counted per day.
//! Cost is O(clicks * log sessions), independent of the number of active campaigns.

#[path = "../output/mod.rs"]
pub mod output;

use crate::types::{Click, Event, Loaded};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub const DAY_MS: i64 = 86_400_000;
pub const GAP_MS: i64 = 1_800_000;
pub const WIN_MS: i64 = 604_800_000;
pub const NO_CATEGORY: u32 = u32::MAX;

/// One user_daily row. `top == NO_CATEGORY` means null.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UserRow {
    pub person: u32,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: i64,
    pub top: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CampCell {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

/// One campaign_daily row. `campaign == campaigns.len()` means `organic`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CampRow {
    pub campaign: u32,
    pub day: i32,
    pub cell: CampCell,
}

pub struct Tables {
    /// One vector per worker; each becomes one or more Parquet files.
    pub user_rows: Vec<Vec<UserRow>>,
    /// Rows already written to disk by the workers (see `aggregate_with`).
    pub user_rows_written: usize,
    pub camp_rows: Vec<CampRow>,
}

/// Entry point called by main.rs.
pub fn run(loaded: Loaded, output_dir: &Path, threads: usize) {
    let t = Instant::now();
    let ud = output_dir.join("user_daily");
    let cd = output_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");
    // Workers write their own user_daily files as soon as they run out of work.
    let tables = aggregate_with(&loaded, threads, Some(&ud));
    eprintln!(
        "[stage] aggregate+user_write {:.3}s (user rows {}, campaign rows {})",
        t.elapsed().as_secs_f64(),
        tables.user_rows_written,
        tables.camp_rows.len()
    );
    let t = Instant::now();
    output::write_campaign(&cd, &tables.camp_rows, &loaded).expect("writing campaign_daily");
    eprintln!("[stage] campaign_write {:.3}s", t.elapsed().as_secs_f64());
    if let Ok(st) = std::fs::read_to_string("/proc/self/status") {
        if let Some(kb) = st.lines().find_map(|l| l.strip_prefix("VmHWM:")) {
            let kb: u64 = kb.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
            eprintln!("[stage] peak_rss_mb={}", kb / 1024);
        }
    }
}

#[inline]
fn day_of(ts: i64) -> i32 {
    ts.div_euclid(DAY_MS) as i32
}

/// Campaign x day accumulator: dense over the window's days, sparse overflow otherwise.
struct CampStore {
    day0: i32,
    ndays: u32,
    dense: Vec<CampCell>,
    sparse: HashMap<(u32, i32), CampCell>,
}

impl CampStore {
    fn new(ncamp_total: usize, day0: i32, ndays: u32) -> Self {
        // Cap dense size (~32 MB per worker); beyond that everything goes sparse.
        let (ndays, dense) = match ncamp_total.checked_mul(ndays as usize) {
            Some(n) if n <= (1 << 20) => (ndays, vec![CampCell::default(); n]),
            _ => (0, Vec::new()),
        };
        CampStore { day0, ndays, dense, sparse: HashMap::new() }
    }
    #[inline]
    fn cell(&mut self, camp: u32, day: i32) -> &mut CampCell {
        let di = day.wrapping_sub(self.day0) as u32;
        if di < self.ndays {
            &mut self.dense[camp as usize * self.ndays as usize + di as usize]
        } else {
            self.sparse.entry((camp, day)).or_default()
        }
    }
}

struct Worker<'a> {
    loaded: &'a Loaded,
    organic: u32,
    camp: CampStore,
    rows: Vec<UserRow>,
    written: usize,
    cat_sum: Vec<u64>,
    cat_touched: Vec<u32>,
    // Per-person session scratch.
    s_start: Vec<i64>,
    /// (day, index of first session of that day) for the current person.
    day_first: Vec<(i32, u32)>,
    // Per-person assist scratch, indexed by campaign.
    stamp: u32,
    c_stamp: Vec<u32>,
    c_lo: Vec<i64>,
    c_hi: Vec<i64>,
    active: Vec<u32>,
}

impl<'a> Worker<'a> {
    fn new(loaded: &'a Loaded, day0: i32, ndays: u32) -> Self {
        let ncamp = loaded.campaigns.len();
        Worker {
            loaded,
            organic: ncamp as u32,
            camp: CampStore::new(ncamp + 1, day0, ndays),
            rows: Vec::new(),
            written: 0,
            cat_sum: vec![0; loaded.categories.len()],
            cat_touched: Vec::new(),
            s_start: Vec::new(),
            day_first: Vec::new(),
            stamp: 0,
            c_stamp: vec![0; ncamp],
            c_lo: vec![0; ncamp],
            c_hi: vec![0; ncamp],
            active: Vec::new(),
        }
    }

    #[inline]
    fn flush_user_day(&mut self, person: u32, day: i32, sessions: u32, events: u32, revenue: i64) {
        let mut top = NO_CATEGORY;
        let mut best = 0u64;
        for &c in &self.cat_touched {
            let v = self.cat_sum[c as usize];
            // Categories are sorted byte-wise, so the smaller index wins ties.
            if v > best || (v == best && c < top) {
                best = v;
                top = c;
            }
            self.cat_sum[c as usize] = 0;
        }
        self.cat_touched.clear();
        self.rows.push(UserRow { person, day, sessions, events, revenue, top });
    }

    fn person(&mut self, p: usize) {
        let ev: &[Event] = self.loaded.person_events(p);
        let cl: &[Click] = self.loaded.person_clicks(p);
        if ev.is_empty() {
            return;
        }
        self.s_start.clear();
        self.day_first.clear();

        let n = ev.len();
        let mut i = 0usize;
        // Attribution pointer state.
        let mut j = 0usize;
        let mut grp_first = 0usize;
        let mut last_ts = i64::MIN;
        // Current user-day.
        let mut ud_day = i32::MIN;
        let (mut ud_sessions, mut ud_events, mut ud_revenue) = (0u32, 0u32, 0i64);

        while i < n {
            let s = ev[i].ts_ms;
            let day = day_of(s);
            if day != ud_day {
                if ud_sessions != 0 {
                    self.flush_user_day(p as u32, ud_day, ud_sessions, ud_events, ud_revenue);
                }
                ud_day = day;
                ud_sessions = 0;
                ud_events = 0;
                ud_revenue = 0;
                self.day_first.push((day, self.s_start.len() as u32));
            }
            let mut prev = s;
            let mut k = i;
            let mut rev = 0u64;
            while k < n {
                let e = ev[k];
                if e.ts_ms - prev > GAP_MS {
                    break;
                }
                prev = e.ts_ms;
                if e.cents != 0 {
                    rev += e.cents as u64;
                    let slot = &mut self.cat_sum[e.category as usize];
                    if *slot == 0 {
                        self.cat_touched.push(e.category);
                    }
                    *slot += e.cents as u64;
                }
                k += 1;
            }
            let nev = (k - i) as u32;
            i = k;

            // Credit: latest click with ts <= s within 7 days; first of its ts group.
            while j < cl.len() && cl[j].ts_ms <= s {
                if cl[j].ts_ms != last_ts {
                    last_ts = cl[j].ts_ms;
                    grp_first = j;
                }
                j += 1;
            }
            let credit = if j > 0 && s - last_ts <= WIN_MS { cl[grp_first].campaign } else { self.organic };
            let cell = self.camp.cell(credit, day);
            cell.sessions += 1;
            cell.purchasing += (rev != 0) as i64;
            cell.revenue += rev as i64;

            ud_sessions += 1;
            ud_events += nev;
            ud_revenue += rev as i64;
            self.s_start.push(s);
        }
        self.flush_user_day(p as u32, ud_day, ud_sessions, ud_events, ud_revenue);

        if !cl.is_empty() {
            self.assists(cl);
        }
    }

    fn assists(&mut self, cl: &[Click]) {
        let first = self.s_start[0];
        let last = *self.s_start.last().unwrap();
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            self.c_stamp.iter_mut().for_each(|x| *x = 0);
            self.stamp = 1;
        }
        let stamp = self.stamp;
        for c in cl {
            let t = c.ts_ms;
            if t > last {
                break; // clicks sorted by ts; later clicks assist nothing
            }
            let hi = t + WIN_MS;
            if hi < first {
                continue;
            }
            let ci = c.campaign as usize;
            if self.c_stamp[ci] == stamp {
                if t <= self.c_hi[ci] {
                    self.c_hi[ci] = hi;
                    continue;
                }
                let (lo0, hi0) = (self.c_lo[ci], self.c_hi[ci]);
                self.count_interval(c.campaign, lo0, hi0);
            } else {
                self.c_stamp[ci] = stamp;
                self.active.push(c.campaign);
            }
            self.c_lo[ci] = t;
            self.c_hi[ci] = hi;
        }
        let active = std::mem::take(&mut self.active);
        for &c in &active {
            let (lo, hi) = (self.c_lo[c as usize], self.c_hi[c as usize]);
            self.count_interval(c, lo, hi);
        }
        self.active = active;
        self.active.clear();
    }

    /// Adds one assist per session with start in [lo, hi], grouped by day.
    #[inline]
    fn count_interval(&mut self, camp: u32, lo: i64, hi: i64) {
        let st = &self.s_start;
        let a = st.partition_point(|&x| x < lo);
        let b = st.partition_point(|&x| x <= hi);
        if a >= b {
            return;
        }
        let df = &self.day_first;
        let mut di = df.partition_point(|d| (d.1 as usize) <= a) - 1;
        let mut idx = a;
        while idx < b {
            let end = if di + 1 < df.len() { (df[di + 1].1 as usize).min(b) } else { b };
            let day = df[di].0;
            self.camp.cell(camp, day).assisted += (end - idx) as i64;
            idx = end;
            di += 1;
        }
    }
}

/// Splits persons into contiguous chunks of roughly `target` events.
fn chunks(loaded: &Loaded, target: usize) -> Vec<(usize, usize)> {
    let np = loaded.persons();
    let off = &loaded.event_offsets;
    let mut out = Vec::new();
    let mut p = 0;
    while p < np {
        let base = off[p] as usize;
        // First person q > p with off[q] >= base + target.
        let lim = (base + target).min(u32::MAX as usize) as u32;
        let q = (p + 1 + off[p + 1..=np].partition_point(|&o| o < lim)).min(np);
        let q = q.max(p + 1);
        out.push((p, q));
        p = q;
    }
    // Largest first so whales do not form the tail.
    out.sort_by_key(|&(a, b)| std::cmp::Reverse(off[b] - off[a]));
    out
}

#[allow(dead_code)]
pub fn aggregate(loaded: &Loaded, threads: usize) -> Tables {
    aggregate_with(loaded, threads, None)
}

/// With `user_dir`, each worker writes its user_daily rows there (one or more files,
/// at least one even when empty) and `Tables::user_rows` comes back empty.
pub fn aggregate_with(loaded: &Loaded, threads: usize, user_dir: Option<&Path>) -> Tables {
    let threads = threads.max(1);
    let (day0, ndays) = if loaded.window_end_ms > loaded.window_start_ms {
        let d0 = day_of(loaded.window_start_ms);
        let d1 = day_of(loaded.window_end_ms - 1);
        (d0, (d1 as i64 - d0 as i64 + 1).min(u32::MAX as i64) as u32)
    } else {
        (0, 0)
    };
    let total = loaded.events.len();
    let target = (total / (threads * 64)).clamp(4096, 1 << 20);
    let work = chunks(loaded, target);
    let next = AtomicUsize::new(0);

    let workers: Vec<Worker> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|tid| {
                let work = &work;
                let next = &next;
                sc.spawn(move || {
                    let mut w = Worker::new(loaded, day0, ndays);
                    loop {
                        let k = next.fetch_add(1, Ordering::Relaxed);
                        if k >= work.len() {
                            break;
                        }
                        let (a, b) = work[k];
                        for p in a..b {
                            w.person(p);
                        }
                    }
                    if let Some(dir) = user_dir {
                        output::write_user_rows(dir, tid, &w.rows, loaded).expect("writing user_daily");
                        w.written = w.rows.len();
                        w.rows = Vec::new();
                    }
                    w
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // Merge campaign accumulators.
    let mut workers = workers;
    let mut user_rows = Vec::with_capacity(workers.len());
    let user_rows_written = workers.iter().map(|w| w.written).sum();
    let mut merged: Option<CampStore> = None;
    for w in workers.drain(..) {
        user_rows.push(w.rows);
        match merged.as_mut() {
            None => merged = Some(w.camp),
            Some(m) => {
                for (d, s) in m.dense.iter_mut().zip(w.camp.dense.iter()) {
                    d.sessions += s.sessions;
                    d.purchasing += s.purchasing;
                    d.revenue += s.revenue;
                    d.assisted += s.assisted;
                }
                for (k, s) in w.camp.sparse {
                    let d = m.sparse.entry(k).or_default();
                    d.sessions += s.sessions;
                    d.purchasing += s.purchasing;
                    d.revenue += s.revenue;
                    d.assisted += s.assisted;
                }
            }
        }
    }
    let mut camp_rows = Vec::new();
    if let Some(m) = merged {
        let nd = m.ndays as usize;
        if nd > 0 {
            for (i, cell) in m.dense.iter().enumerate() {
                if cell.sessions != 0 || cell.assisted != 0 {
                    camp_rows.push(CampRow {
                        campaign: (i / nd) as u32,
                        day: m.day0 + (i % nd) as i32,
                        cell: *cell,
                    });
                }
            }
        }
        for ((c, d), cell) in m.sparse {
            if cell.sessions != 0 || cell.assisted != 0 {
                camp_rows.push(CampRow { campaign: c, day: d, cell });
            }
        }
    }
    Tables { user_rows, user_rows_written, camp_rows }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StringTable;

    fn mk(persons: Vec<(&str, Vec<(i64, u32, u32)>, Vec<(i64, u32)>)>, ncamp: usize) -> Loaded {
        let mut ids = StringTable::new();
        let mut events = Vec::new();
        let mut clicks = Vec::new();
        let mut eo = vec![0u32];
        let mut co = vec![0u32];
        for (id, ev, cl) in persons {
            ids.push(id.as_bytes());
            for (ts, cat, cents) in ev {
                events.push(Event { ts_ms: ts, category: cat, cents });
            }
            for (ts, camp) in cl {
                clicks.push(Click { ts_ms: ts, campaign: camp, reserved: 0 });
            }
            eo.push(events.len() as u32);
            co.push(clicks.len() as u32);
        }
        Loaded {
            window_start_ms: 0,
            window_end_ms: 30 * DAY_MS,
            person_ids: ids,
            categories: vec![b"a".to_vec(), b"b".to_vec(), b"unknown".to_vec()],
            campaigns: (0..ncamp).map(|i| (format!("c{i}").into_bytes(), b"x".to_vec())).collect(),
            events,
            event_offsets: eo,
            clicks,
            click_offsets: co,
        }
    }

    #[test]
    fn sessions_and_assists() {
        let d = DAY_MS;
        let l = mk(
            vec![(
                "u",
                vec![
                    (10 * d, 0, 0),
                    (10 * d + GAP_MS, 1, 5),     // same session (exactly 30 min)
                    (10 * d + 2 * GAP_MS + 1, 0, 5), // new session
                    (11 * d - 1, 0, 0),
                    (11 * d + 10, 1, 3), // same session, crosses midnight
                ],
                vec![(3 * d, 0), (10 * d - WIN_MS, 1), (10 * d, 1), (10 * d, 0)],
            )],
            2,
        );
        let t = aggregate(&l, 2);
        let rows: Vec<UserRow> = t.user_rows.into_iter().flatten().collect();
        assert_eq!(
            rows,
            vec![UserRow { person: 0, day: 10, sessions: 3, events: 5, revenue: 13, top: 1 }]
        );
        let mut c = t.camp_rows;
        c.sort_by_key(|r| (r.campaign, r.day));
        // Session starts: 10d (credit: click group at 10d -> first = campaign 1), 10d+2G+1, 11d-1.
        // Campaign 1 clicks at 3d..10d intervals cover all three starts? [3d,10d] (from 10d-W=3d) and [10d, 17d].
        // Campaign 0 clicks at 3d and 10d: same.
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].campaign, 0);
        assert_eq!(c[0].cell, CampCell { sessions: 0, purchasing: 0, revenue: 0, assisted: 3 });
        assert_eq!(c[1].cell, CampCell { sessions: 3, purchasing: 3, revenue: 13, assisted: 3 });
    }

    #[test]
    fn top_category_per_day() {
        let d = DAY_MS;
        // Day 5: purchase cat 0 (10). Day 6: view, then purchase cat 1 (7). Day 7: no purchase.
        let l = mk(
            vec![("u", vec![(5 * d, 0, 10), (6 * d, 2, 0), (6 * d + 5, 1, 7), (7 * d, 0, 0)], vec![])],
            1,
        );
        let rows: Vec<UserRow> = aggregate(&l, 1).user_rows.into_iter().flatten().collect();
        let tops: Vec<(i32, u32)> = rows.iter().map(|r| (r.day, r.top)).collect();
        assert_eq!(tops, vec![(5, 0), (6, 1), (7, NO_CATEGORY)]);
        // Tie on cents: smaller category index wins.
        let l = mk(vec![("u", vec![(5 * d, 1, 4), (5 * d + 1, 0, 4)], vec![])], 1);
        let rows: Vec<UserRow> = aggregate(&l, 1).user_rows.into_iter().flatten().collect();
        assert_eq!(rows[0].top, 0);
    }

    #[test]
    fn sparse_matches_dense() {
        let d = DAY_MS;
        let mut l = mk(
            vec![
                ("a", vec![(10 * d, 0, 3), (12 * d + 7, 1, 0), (20 * d, 0, 9)], vec![(9 * d, 0), (11 * d, 1), (19 * d, 2)]),
                ("b", vec![(10 * d + 5, 2, 0)], vec![(10 * d + 5, 2), (10 * d + 5, 1)]),
            ],
            3,
        );
        let norm = |t: Tables| {
            let mut c = t.camp_rows;
            c.sort_by_key(|r| (r.campaign, r.day));
            let mut u: Vec<UserRow> = t.user_rows.into_iter().flatten().collect();
            u.sort_by_key(|r| (r.person, r.day));
            (u, c)
        };
        let dense = norm(aggregate(&l, 2));
        // A window this long forces the sparse accumulator.
        l.window_start_ms = -200_000 * d;
        l.window_end_ms = 200_000 * d;
        assert_eq!(CampStore::new(4, 0, 400_000).ndays, 0);
        let sparse = norm(aggregate(&l, 2));
        assert_eq!(dense, sparse);
        assert!(dense.1.iter().any(|r| r.cell.assisted > 0 && r.cell.sessions == 0));
    }
}
