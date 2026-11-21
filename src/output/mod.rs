//! Parquet output for both tables. Owner: aggregate agent.
//!
//! user_daily: one file per worker chunk (split above MAX_ROWS), written in parallel.
//! campaign_daily: one file. Both directories always get at least one file, even with
//! zero rows.

pub mod parquet;

use super::{CampRow, Tables, UserRow, NO_CATEGORY};
use crate::types::Loaded;
use parquet::FileBuilder;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

const MAX_ROWS: usize = 1 << 21;
const MAX_COL_BYTES: usize = 1 << 29; // 512 MiB cap per page (well below Parquet i32::MAX limit)

#[cfg(not(target_endian = "little"))]
compile_error!("Parquet PLAIN buffers are built in native byte order; little-endian only");

#[inline]
fn bytes_of<T: Copy>(v: &[T]) -> &[u8] {
    // SAFETY: plain integer slices viewed as bytes (little-endian target asserted above).
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// user_daily rows accumulated directly as PLAIN Parquet column page bodies.
pub struct UserCols {
    n: usize,
    ids: Vec<u8>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    defs: Vec<u8>,
    top: Vec<u8>,
}

impl UserCols {
    pub fn with_capacity(rows: usize) -> Self {
        let cap = rows.min(MAX_ROWS);
        UserCols {
            n: 0,
            ids: Vec::with_capacity(cap * 14),
            day: Vec::with_capacity(cap),
            sessions: Vec::with_capacity(cap),
            events: Vec::with_capacity(cap),
            revenue: Vec::with_capacity(cap),
            defs: Vec::with_capacity(cap.div_ceil(8) + 1),
            top: Vec::with_capacity(cap * 4),
        }
    }
    pub fn len(&self) -> usize {
        self.n
    }
    pub fn clear(&mut self) {
        self.n = 0;
        self.ids.clear();
        self.day.clear();
        self.sessions.clear();
        self.events.clear();
        self.revenue.clear();
        self.defs.clear();
        self.top.clear();
    }
    #[cold]
    #[inline(never)]
    fn grow_fixed(&mut self) {
        let extra = self.n.max(4096);
        self.day.reserve(extra);
        let target = self.day.capacity();
        if self.sessions.capacity() < target {
            self.sessions.reserve(target - self.sessions.len());
        }
        if self.events.capacity() < target {
            self.events.reserve(target - self.events.len());
        }
        if self.revenue.capacity() < target {
            self.revenue.reserve(target - self.revenue.len());
        }
        let dneed = target.div_ceil(8) + 1;
        if self.defs.capacity() < dneed {
            self.defs.reserve(dneed - self.defs.len());
        }
    }
    #[inline(always)]
    pub fn needs_flush(&self) -> bool {
        self.n >= MAX_ROWS || self.ids.len() >= MAX_COL_BYTES || self.top.len() >= MAX_COL_BYTES
    }
    #[inline(always)]
    pub fn push(&mut self, id: &[u8], day: i32, sessions: u32, events: u32, revenue: i64, top: Option<&[u8]>) {
        let n = self.n;
        if n == self.day.capacity() {
            self.grow_fixed();
        }
        let ilen = id.len();
        if self.ids.capacity() - self.ids.len() < 4 + ilen {
            self.ids.reserve(4 + ilen);
        }
        unsafe {
            let cur = self.ids.len();
            let dst = self.ids.as_mut_ptr().add(cur);
            std::ptr::copy_nonoverlapping((ilen as u32).to_le_bytes().as_ptr(), dst, 4);
            std::ptr::copy_nonoverlapping(id.as_ptr(), dst.add(4), ilen);
            self.ids.set_len(cur + 4 + ilen);

            self.day.as_mut_ptr().add(n).write(day);
            self.sessions.as_mut_ptr().add(n).write(sessions as i64);
            self.events.as_mut_ptr().add(n).write(events as i64);
            self.revenue.as_mut_ptr().add(n).write(revenue);
            let n1 = n + 1;
            self.day.set_len(n1);
            self.sessions.set_len(n1);
            self.events.set_len(n1);
            self.revenue.set_len(n1);

            let di = n >> 3;
            if n & 7 == 0 {
                self.defs.as_mut_ptr().add(di).write(0);
                self.defs.set_len(di + 1);
            }
            if let Some(t) = top {
                *self.defs.get_unchecked_mut(di) |= 1 << (n & 7);
                let tlen = t.len();
                if self.top.capacity() - self.top.len() < 4 + tlen {
                    self.top.reserve(4 + tlen);
                }
                let cur = self.top.len();
                let dst = self.top.as_mut_ptr().add(cur);
                std::ptr::copy_nonoverlapping((tlen as u32).to_le_bytes().as_ptr(), dst, 4);
                std::ptr::copy_nonoverlapping(t.as_ptr(), dst.add(4), tlen);
                self.top.set_len(cur + 4 + tlen);
            }
        }
        self.n = n + 1;
    }
}

/// Streaming per-worker `user_daily` writer that splits parts before hitting `MAX_ROWS`
/// or `MAX_COL_BYTES` so Parquet pages never overflow `i32::MAX`.
pub struct UserWriter<'a> {
    dir: &'a Path,
    worker: usize,
    seq: usize,
    pub cols: UserCols,
    pub total_written: usize,
}

impl<'a> UserWriter<'a> {
    pub fn new(dir: &'a Path, worker: usize, est_rows: usize) -> Self {
        UserWriter {
            dir,
            worker,
            seq: 0,
            cols: UserCols::with_capacity(est_rows),
            total_written: 0,
        }
    }
    #[inline(always)]
    pub fn push(&mut self, id: &[u8], day: i32, sessions: u32, events: u32, revenue: i64, top: Option<&[u8]>) {
        self.cols.push(id, day, sessions, events, revenue, top);
        if self.cols.needs_flush() {
            self.flush_part();
        }
    }
    #[cold]
    fn flush_part(&mut self) {
        if self.cols.len() == 0 {
            return;
        }
        let path = self.dir.join(format!("part-{}-{}.parquet", self.worker, self.seq));
        write_user_cols_to(&path, &self.cols).expect("writing user_daily part");
        self.total_written += self.cols.len();
        self.seq += 1;
        self.cols.clear();
    }
    pub fn finish(&mut self) -> usize {
        if self.cols.len() > 0 {
            let path = if self.seq == 0 {
                self.dir.join(format!("part-{}.parquet", self.worker))
            } else {
                self.dir.join(format!("part-{}-{}.parquet", self.worker, self.seq))
            };
            write_user_cols_to(&path, &self.cols).expect("writing user_daily");
            self.total_written += self.cols.len();
            self.seq += 1;
            self.cols.clear();
        }
        self.total_written
    }
}

/// Writes a single clean 0-row `part-0.parquet` when no worker emitted any `user_daily` rows.
pub fn write_empty_user(dir: &Path) -> io::Result<()> {
    let empty = UserCols::with_capacity(0);
    write_user_cols_to(&dir.join("part-0.parquet"), &empty)
}

/// Writes column buffers to an explicit Parquet file path.
pub fn write_user_cols_to(path: &Path, c: &UserCols) -> io::Result<()> {
    use parquet::{Column, Kind};
    let prefix = parquet::def_levels_prefix(c.n);
    let cols = [
        Column {
            name: "user_id",
            kind: Kind::Utf8,
            parts: vec![&c.ids],
        },
        Column {
            name: "day",
            kind: Kind::Date,
            parts: vec![bytes_of(&c.day)],
        },
        Column {
            name: "sessions",
            kind: Kind::Int64,
            parts: vec![bytes_of(&c.sessions)],
        },
        Column {
            name: "events",
            kind: Kind::Int64,
            parts: vec![bytes_of(&c.events)],
        },
        Column {
            name: "revenue_cents",
            kind: Kind::Int64,
            parts: vec![bytes_of(&c.revenue)],
        },
        Column {
            name: "top_category",
            kind: Kind::OptUtf8,
            parts: vec![&prefix, &c.defs, &c.top],
        },
    ];
    parquet::write_columns(path, c.n, &cols)
}

/// Writes `campaign_daily` from raw campaign dictionary + rows (no `Loaded` required).
pub fn write_campaign_raw(dir: &Path, rows: &[CampRow], campaigns: &[(Vec<u8>, Vec<u8>)]) -> io::Result<()> {
    let id = |c: u32| -> &[u8] { campaigns.get(c as usize).map(|x| x.0.as_slice()).unwrap_or(b"organic") };
    let ch = |c: u32| -> &[u8] { campaigns.get(c as usize).map(|x| x.1.as_slice()).unwrap_or(b"organic") };
    let write_chunk = |path: &Path, chunk: &[CampRow]| -> io::Result<()> {
        let n = chunk.len();
        let idb: usize = chunk.iter().map(|r| id(r.campaign).len()).sum();
        let chb: usize = chunk.iter().map(|r| ch(r.campaign).len()).sum();
        let mut f = FileBuilder::new(n, 8 * n + idb + chb + 36 * n);
        f.col_utf8("campaign_id", idb, chunk.iter().map(|r| id(r.campaign)));
        f.col_utf8("channel", chb, chunk.iter().map(|r| ch(r.campaign)));
        f.col_date("day", chunk.iter().map(|r| r.day));
        f.col_i64("sessions", chunk.iter().map(|r| r.cell.sessions));
        f.col_i64("purchasing_sessions", chunk.iter().map(|r| r.cell.purchasing));
        f.col_i64("revenue_cents", chunk.iter().map(|r| r.cell.revenue));
        f.col_i64("assisted_sessions", chunk.iter().map(|r| r.cell.assisted));
        std::fs::write(path, f.finish())
    };
    if rows.len() <= MAX_ROWS {
        return write_chunk(&dir.join("part-0.parquet"), rows);
    }
    for (k, chunk) in rows.chunks(MAX_ROWS).enumerate() {
        write_chunk(&dir.join(format!("part-{k}.parquet")), chunk)?;
    }
    Ok(())
}

fn user_file(rows: &[UserRow], loaded: &Loaded) -> Vec<u8> {
    let ids = &loaded.person_ids;
    let cats = &loaded.categories;
    let n = rows.len();
    let id_bytes: usize = rows.iter().map(|r| ids.get(r.person as usize).len()).sum();
    let (mut nonnull, mut cat_bytes) = (0usize, 0usize);
    for r in rows {
        if r.top != NO_CATEGORY {
            nonnull += 1;
            cat_bytes += cats[r.top as usize].len();
        }
    }
    let cap = 4 * n + id_bytes + 4 * n + 24 * n + n / 8 + 4 * nonnull + cat_bytes + 64;
    let mut f = FileBuilder::new(n, cap);
    f.col_utf8("user_id", id_bytes, rows.iter().map(|r| ids.get(r.person as usize)));
    f.col_date("day", rows.iter().map(|r| r.day));
    f.col_i64("sessions", rows.iter().map(|r| r.sessions as i64));
    f.col_i64("events", rows.iter().map(|r| r.events as i64));
    f.col_i64("revenue_cents", rows.iter().map(|r| r.revenue));
    f.col_opt_utf8(
        "top_category",
        nonnull,
        cat_bytes,
        rows.iter().map(|r| {
            if r.top == NO_CATEGORY {
                None
            } else {
                Some(cats[r.top as usize].as_slice())
            }
        }),
    );
    f.finish()
}

#[allow(dead_code)]
pub fn user_file_bytes(rows: &[UserRow], loaded: &Loaded) -> Vec<u8> {
    user_file(rows, loaded)
}

fn camp_file(rows: &[CampRow], loaded: &Loaded) -> Vec<u8> {
    let camps = &loaded.campaigns;
    let id = |c: u32| -> &[u8] { camps.get(c as usize).map(|x| x.0.as_slice()).unwrap_or(b"organic") };
    let ch = |c: u32| -> &[u8] { camps.get(c as usize).map(|x| x.1.as_slice()).unwrap_or(b"organic") };
    let n = rows.len();
    let idb: usize = rows.iter().map(|r| id(r.campaign).len()).sum();
    let chb: usize = rows.iter().map(|r| ch(r.campaign).len()).sum();
    let mut f = FileBuilder::new(n, 8 * n + idb + chb + 36 * n);
    f.col_utf8("campaign_id", idb, rows.iter().map(|r| id(r.campaign)));
    f.col_utf8("channel", chb, rows.iter().map(|r| ch(r.campaign)));
    f.col_date("day", rows.iter().map(|r| r.day));
    f.col_i64("sessions", rows.iter().map(|r| r.cell.sessions));
    f.col_i64("purchasing_sessions", rows.iter().map(|r| r.cell.purchasing));
    f.col_i64("revenue_cents", rows.iter().map(|r| r.cell.revenue));
    f.col_i64("assisted_sessions", rows.iter().map(|r| r.cell.assisted));
    f.finish()
}

/// Writes one worker's user_daily rows as `part-{worker}[-k].parquet` (always >= 1 file).
#[allow(dead_code)]
pub fn write_user_rows(dir: &Path, worker: usize, rows: &[UserRow], loaded: &Loaded) -> io::Result<()> {
    if rows.len() <= MAX_ROWS {
        return std::fs::write(dir.join(format!("part-{worker}.parquet")), user_file(rows, loaded));
    }
    for (k, c) in rows.chunks(MAX_ROWS).enumerate() {
        std::fs::write(dir.join(format!("part-{worker}-{k}.parquet")), user_file(c, loaded))?;
    }
    Ok(())
}

pub fn write_campaign(dir: &Path, rows: &[CampRow], loaded: &Loaded) -> io::Result<()> {
    std::fs::write(dir.join("part-0.parquet"), camp_file(rows, loaded))
}

/// Writes both tables from in-memory rows (used when rows were not written by workers).
#[allow(dead_code)]
pub fn write(tables: &Tables, loaded: &Loaded, out: &Path, threads: usize) -> io::Result<()> {
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud)?;
    std::fs::create_dir_all(&cd)?;

    // Work items: None = campaign file, Some(slice) = user file.
    let mut items: Vec<Option<&[UserRow]>> = vec![None];
    for v in &tables.user_rows {
        for c in v.chunks(MAX_ROWS) {
            items.push(Some(c));
        }
    }
    if items.len() == 1 {
        items.push(Some(&[]));
    }
    let next = AtomicUsize::new(0);
    let threads = threads.clamp(1, items.len());
    std::thread::scope(|sc| -> io::Result<()> {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                let (items, next, ud, cd) = (&items, &next, &ud, &cd);
                sc.spawn(move || -> io::Result<()> {
                    loop {
                        let k = next.fetch_add(1, Ordering::Relaxed);
                        if k >= items.len() {
                            return Ok(());
                        }
                        match items[k] {
                            None => std::fs::write(cd.join("part-0.parquet"), camp_file(&tables.camp_rows, loaded))?,
                            Some(rows) => {
                                std::fs::write(ud.join(format!("part-{k}.parquet")), user_file(rows, loaded))?
                            }
                        }
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap()?;
        }
        Ok(())
    })
}
