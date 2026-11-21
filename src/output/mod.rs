//! Parquet output for both tables. Owner: aggregate agent.
//!
//! user_daily: row groups written in parallel into per-worker files.
//! campaign_daily: one file. Both directories always get at least one file, even with
//! zero rows.

pub mod parquet;

use super::{CampRow, Tables, UserRow, NO_CATEGORY};
use crate::types::Loaded;
use parquet::{FileBuilder, RowGroupFile};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

const USER_GROUP_ROWS: usize = 1 << 14;
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
#[derive(Default)]
pub struct UserCols {
    n: usize,
    ids: Vec<u8>,
    day: Vec<i32>,
    sessions: Vec<u32>,
    events: Vec<u32>,
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
    fn sync_lens(&mut self) {
        // Each push initialized all four fixed-width columns through row n.
        unsafe {
            self.day.set_len(self.n);
            self.sessions.set_len(self.n);
            self.events.set_len(self.n);
            self.revenue.set_len(self.n);
        }
    }

    #[cold]
    #[inline(never)]
    fn grow_fixed(&mut self) {
        // Reserve may relocate existing entries, so expose all initialized rows first.
        self.sync_lens();
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
        self.n >= USER_GROUP_ROWS || self.ids.len() >= MAX_COL_BYTES || self.top.len() >= MAX_COL_BYTES
    }
    #[inline(always)]
    pub fn push(
        &mut self,
        id: &[u8],
        day: i32,
        sessions: u32,
        events: u32,
        revenue: i64,
        top: Option<&[u8]>,
    ) {
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
            self.sessions.as_mut_ptr().add(n).write(sessions);
            self.events.as_mut_ptr().add(n).write(events);
            self.revenue.as_mut_ptr().add(n).write(revenue);

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

/// Streams bounded row groups into one `user_daily` file per worker.
pub struct UserWriter<'a> {
    dir: &'a Path,
    worker: usize,
    cols: UserCols,
    file: FileBuilder,
    stream: Option<RowGroupFile>,
    total_written: usize,
}

impl<'a> UserWriter<'a> {
    pub fn new(dir: &'a Path, worker: usize, est_rows: usize) -> Self {
        UserWriter {
            dir,
            worker,
            cols: UserCols::with_capacity(est_rows.min(USER_GROUP_ROWS)),
            file: FileBuilder::default(),
            stream: None,
            total_written: 0,
        }
    }
    #[inline(always)]
    pub fn push(
        &mut self,
        id: &[u8],
        day: i32,
        sessions: u32,
        events: u32,
        revenue: i64,
        top: Option<&[u8]>,
    ) {
        if self.push_group_row(id, day, sessions, events, revenue, top) {
            self.flush_group();
        }
    }

    #[inline(always)]
    pub fn push_group_row(
        &mut self,
        id: &[u8],
        day: i32,
        sessions: u32,
        events: u32,
        revenue: i64,
        top: Option<&[u8]>,
    ) -> bool {
        self.cols.push(id, day, sessions, events, revenue, top);
        self.cols.needs_flush()
    }

    #[cold]
    pub fn flush_group(&mut self) {
        debug_assert!(self.cols.len() > 0);
        encode_user_cols(&mut self.cols, &mut self.file);
        let stream = self.stream.get_or_insert_with(|| {
            let name = format!("part-{}.parquet", self.worker);
            RowGroupFile::create(&self.dir.join(name)).expect("creating user_daily part")
        });
        stream.append(&self.file).expect("writing user_daily row group");
        self.total_written += self.cols.len();
        self.cols.clear();
    }

    /// Release completed column buffers on the thread that wrote the parts.
    pub fn release_buffers(&mut self) {
        debug_assert_eq!(self.cols.len(), 0);
        self.cols = UserCols::default();
    }

    pub fn finish(&mut self) -> usize {
        if self.cols.len() > 0 {
            self.flush_group();
        }
        if let Some(stream) = self.stream.take() {
            stream.finish().expect("writing user_daily footer");
        }
        self.file = FileBuilder::default();
        self.total_written
    }
}

/// Writes a single clean 0-row `part-0.parquet` when no worker emitted any `user_daily` rows.
pub fn write_empty_user(dir: &Path) -> io::Result<()> {
    write_user_cols_to(
        &dir.join("part-0.parquet"),
        &mut UserCols::default(),
        &mut FileBuilder::default(),
    )
}

fn encode_user_cols(cols: &mut UserCols, file: &mut FileBuilder) {
    cols.sync_lens();
    file.reset(cols.n);
    file.col_user_id(&cols.ids);
    file.col_date(&cols.day);
    file.col_u32("sessions", &cols.sessions);
    file.col_u32("events", &cols.events);
    file.col_user_revenue(&cols.revenue);
    file.col_category(&cols.defs, &cols.top);
}

fn write_user_cols_to(path: &Path, cols: &mut UserCols, file: &mut FileBuilder) -> io::Result<()> {
    encode_user_cols(cols, file);
    std::fs::write(path, file.finish())
}

/// Writes `campaign_daily` from raw campaign dictionary + rows (no `Loaded` required).
pub fn write_campaign_raw(
    dir: &Path,
    rows: &[CampRow],
    campaigns: &[(Vec<u8>, Vec<u8>)],
) -> io::Result<()> {
    let mut file = FileBuilder::default();
    if rows.is_empty() {
        encode_campaign(rows, campaigns, &mut file);
        return std::fs::write(dir.join("part-0.parquet"), file.finish());
    }
    for (part, chunk) in rows.chunks(MAX_ROWS).enumerate() {
        encode_campaign(chunk, campaigns, &mut file);
        std::fs::write(dir.join(format!("part-{part}.parquet")), file.finish())?;
    }
    Ok(())
}

fn encode_campaign(rows: &[CampRow], campaigns: &[(Vec<u8>, Vec<u8>)], file: &mut FileBuilder) {
    let id = |c: u32| -> &[u8] {
        campaigns
            .get(c as usize)
            .map(|x| x.0.as_slice())
            .unwrap_or(b"organic")
    };
    let ch = |c: u32| -> &[u8] {
        campaigns
            .get(c as usize)
            .map(|x| x.1.as_slice())
            .unwrap_or(b"organic")
    };
    file.reset(rows.len());
    file.col_utf8("campaign_id", rows.iter().map(|row| id(row.campaign)));
    file.col_utf8("channel", rows.iter().map(|row| ch(row.campaign)));
    file.col_date(&rows.iter().map(|row| row.day).collect::<Vec<_>>());
    let mut integers = Vec::with_capacity(rows.len());
    integers.extend(rows.iter().map(|row| row.cell.sessions));
    file.col_i64("sessions", &integers);
    integers.clear();
    integers.extend(rows.iter().map(|row| row.cell.purchasing));
    file.col_i64("purchasing_sessions", &integers);
    integers.clear();
    integers.extend(rows.iter().map(|row| row.cell.revenue));
    file.col_i64("revenue_cents", &integers);
    integers.clear();
    integers.extend(rows.iter().map(|row| row.cell.assisted));
    file.col_i64("assisted_sessions", &integers);
}

fn user_file(rows: &[UserRow], loaded: &Loaded) -> Vec<u8> {
    let mut cols = UserCols::with_capacity(rows.len());
    for row in rows {
        cols.push(
            loaded.person_ids.get(row.person as usize),
            row.day,
            row.sessions,
            row.events,
            row.revenue,
            (row.top != NO_CATEGORY).then(|| loaded.categories[row.top as usize].as_slice()),
        );
    }
    let mut file = FileBuilder::default();
    encode_user_cols(&mut cols, &mut file);
    file.into_bytes()
}

#[allow(dead_code)]
pub fn user_file_bytes(rows: &[UserRow], loaded: &Loaded) -> Vec<u8> {
    user_file(rows, loaded)
}

fn camp_file(rows: &[CampRow], loaded: &Loaded) -> Vec<u8> {
    let mut file = FileBuilder::default();
    encode_campaign(rows, &loaded.campaigns, &mut file);
    file.into_bytes()
}

/// Writes one worker's user_daily rows as `part-{worker}[-k].parquet` (always >= 1 file).
#[allow(dead_code)]
pub fn write_user_rows(
    dir: &Path,
    worker: usize,
    rows: &[UserRow],
    loaded: &Loaded,
) -> io::Result<()> {
    if rows.len() <= MAX_ROWS {
        return std::fs::write(
            dir.join(format!("part-{worker}.parquet")),
            user_file(rows, loaded),
        );
    }
    for (k, c) in rows.chunks(MAX_ROWS).enumerate() {
        std::fs::write(
            dir.join(format!("part-{worker}-{k}.parquet")),
            user_file(c, loaded),
        )?;
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
                            None => std::fs::write(
                                cd.join("part-0.parquet"),
                                camp_file(&tables.camp_rows, loaded),
                            )?,
                            Some(rows) => std::fs::write(
                                ud.join(format!("part-{k}.parquet")),
                                user_file(rows, loaded),
                            )?,
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

#[cfg(test)]
mod tests {
    use super::UserCols;

    #[test]
    fn fixed_columns_keep_initialized_rows_when_growing() {
        let mut cols = UserCols::with_capacity(1);
        cols.push(b"first", 10, 2, 3, 40, Some(b"cat"));
        // The next row grows the fixed columns while their Vec lengths are stale.
        cols.push(b"second", 11, 5, 6, 70, None);
        cols.sync_lens();
        assert_eq!(cols.day, [10, 11]);
        assert_eq!(cols.sessions, [2, 5]);
        assert_eq!(cols.events, [3, 6]);
        assert_eq!(cols.revenue, [40, 70]);

        cols.clear();
        cols.push(b"third", 12, 8, 9, 100, None);
        cols.sync_lens();
        assert_eq!(cols.day, [12]);
        assert_eq!(cols.sessions, [8]);
        assert_eq!(cols.events, [9]);
        assert_eq!(cols.revenue, [100]);
    }
}
