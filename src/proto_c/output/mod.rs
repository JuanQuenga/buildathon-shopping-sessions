//! Parquet output for both tables. Owner: aggregate agent.
//!
//! user_daily: row groups written in parallel into per-worker files.
//! campaign_daily: one file. Both directories always get at least one file, even with
//! zero rows.

pub mod parquet;

use crate::proto_c::aggregate::CampRow;
use crate::proto_c::arena::{Region, Scratch};
use parquet::{FileBuilder, RowGroupBuffer};
use std::io;
use std::mem::MaybeUninit;
use std::path::Path;

const USER_GROUP_ROWS: usize = 1 << 14;
const MAX_ROWS: usize = 1 << 21;

#[cfg(not(target_endian = "little"))]
compile_error!("Parquet PLAIN buffers are built in native byte order; little-endian only");

#[inline]
fn bytes_of<T: Copy>(v: &[T]) -> &[u8] {
    // SAFETY: plain integer slices viewed as bytes (little-endian target asserted above).
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// Reuses one fixed row group's column values in the worker's arena pages.
struct UserCols<'a> {
    n: usize,
    ids: Scratch<'a, u8>,
    day: &'a mut [MaybeUninit<i32>],
    sessions: &'a mut [MaybeUninit<u32>],
    events: &'a mut [MaybeUninit<u32>],
    revenue: &'a mut [MaybeUninit<i64>],
    defs: &'a mut [MaybeUninit<u8>],
    top: Scratch<'a, u8>,
}

impl<'a> UserCols<'a> {
    fn new(region: &'a Region<'_>, max_cat: usize) -> Option<Self> {
        let rows = USER_GROUP_ROWS;
        let top_bytes = rows.checked_mul(max_cat.checked_add(4)?)?;
        let ids = Scratch::new(region, rows * 13)?;
        let top = Scratch::new(region, top_bytes)?;
        Some(Self {
            n: 0,
            ids,
            top,
            day: region.uninit(rows)?,
            sessions: region.uninit(rows)?,
            events: region.uninit(rows)?,
            revenue: region.uninit(rows)?,
            defs: region.uninit(rows.div_ceil(8))?,
        })
    }

    fn clear(&mut self) {
        self.n = 0;
        self.ids.clear();
        self.top.clear();
    }

    #[inline(always)]
    fn push(
        &mut self,
        id: &[u8],
        day: i32,
        sessions: u32,
        events: u32,
        revenue: i64,
        top: Option<&[u8]>,
    ) {
        let n = self.n;
        debug_assert!(n < USER_GROUP_ROWS && id.len() <= 9);
        self.ids.extend_from_slice(&(id.len() as u32).to_le_bytes());
        self.ids.extend_from_slice(id);
        // Every worker flushes at USER_GROUP_ROWS. All columns were allocated
        // with exactly that capacity, and a row initializes them together.
        unsafe {
            self.day.get_unchecked_mut(n).write(day);
            self.sessions.get_unchecked_mut(n).write(sessions);
            self.events.get_unchecked_mut(n).write(events);
            self.revenue.get_unchecked_mut(n).write(revenue);
            let def = self.defs.get_unchecked_mut(n / 8);
            let bit = n & 7;
            if bit == 0 {
                def.write(0);
            }
            if let Some(top) = top {
                *def.assume_init_mut() |= 1 << bit;
                self.top
                    .extend_from_slice(&(top.len() as u32).to_le_bytes());
                self.top.extend_from_slice(top);
            }
        }
        self.n = n + 1;
    }
}

/// # Safety
/// The first count entries must have been initialized.
unsafe fn prefix<T>(column: &[MaybeUninit<T>], count: usize) -> &[T] {
    debug_assert!(count <= column.len());
    unsafe { std::slice::from_raw_parts(column.as_ptr().cast(), count) }
}

/// Encodes bounded row groups and writes complete Parquet parts from the arena.
pub struct UserWriter<'a> {
    dir: &'a Path,
    worker: usize,
    part: usize,
    cols: UserCols<'a>,
    file: FileBuilder,
    stream: RowGroupBuffer<'a>,
    total_written: usize,
}

impl<'a> UserWriter<'a> {
    pub fn new(
        dir: &'a Path,
        worker: usize,
        region: &'a Region<'_>,
        max_cat: usize,
        file_cap: usize,
    ) -> Option<Self> {
        Some(Self {
            dir,
            worker,
            part: 0,
            cols: UserCols::new(region, max_cat)?,
            file: FileBuilder::default(),
            stream: RowGroupBuffer::new(region.uninit(file_cap)?),
            total_written: 0,
        })
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
        self.cols.n == USER_GROUP_ROWS
    }

    fn part_path(&self) -> std::path::PathBuf {
        self.dir
            .join(format!("part-{}-{}.parquet", self.worker, self.part))
    }

    fn write_part(&mut self) {
        if !self.stream.is_empty() {
            self.stream
                .write(&self.part_path())
                .expect("writing user_daily part");
            self.part += 1;
        }
    }

    #[cold]
    pub fn flush_group(&mut self) {
        debug_assert!(self.cols.n > 0);
        encode_user_cols(&self.cols, &mut self.file);
        if !self.stream.fits(&self.file) {
            self.write_part();
        }
        if self.stream.fits(&self.file) {
            self.stream.append(&self.file);
        } else {
            // A single row group with very long category values may exceed
            // an entire part buffer. Its existing encoder buffer is complete.
            let path = self.part_path();
            std::fs::write(path, self.file.finish()).expect("writing oversized user_daily part");
            self.part += 1;
        }
        self.total_written += self.cols.n;
        self.cols.clear();
    }

    pub fn finish(&mut self) -> usize {
        if self.cols.n > 0 {
            self.flush_group();
        }
        self.write_part();
        self.total_written
    }
}

/// Writes the user schema when no worker emitted any user_daily rows.
pub fn write_empty_user(dir: &Path) -> io::Result<()> {
    let mut file = FileBuilder::default();
    file.reset(0);
    file.col_user_id(&[]);
    file.col_date(&[]);
    file.col_u32("sessions", &[]);
    file.col_u32("events", &[]);
    file.col_user_revenue(&[]);
    file.col_category(&[], &[]);
    std::fs::write(dir.join("part-0.parquet"), file.finish())
}

fn encode_user_cols(cols: &UserCols<'_>, file: &mut FileBuilder) {
    let n = cols.n;
    file.reset(n);
    file.col_user_id(&cols.ids);
    // Each call to push initializes a full row before advancing n.
    unsafe {
        file.col_date(prefix(cols.day, n));
        file.col_u32("sessions", prefix(cols.sessions, n));
        file.col_u32("events", prefix(cols.events, n));
        file.col_user_revenue(prefix(cols.revenue, n));
        file.col_category(prefix(cols.defs, n.div_ceil(8)), &cols.top);
    }
}

/// Writes `campaign_daily` from raw campaign dictionary + rows (no `Loaded` required).
pub fn write_campaign_raw(
    dir: &Path,
    rows: &[CampRow],
    campaigns: &[(Vec<u8>, Vec<u8>)],
    storage: &mut [MaybeUninit<u8>],
) -> io::Result<()> {
    let mut file = FileBuilder::default();
    if rows.is_empty() {
        encode_campaign(rows, campaigns, &mut file);
        return write_arena(&dir.join("part-0.parquet"), file.finish(), storage);
    }
    for (part, chunk) in rows.chunks(MAX_ROWS).enumerate() {
        encode_campaign(chunk, campaigns, &mut file);
        write_arena(
            &dir.join(format!("part-{part}.parquet")),
            file.finish(),
            storage,
        )?;
    }
    Ok(())
}

fn write_arena(path: &Path, bytes: &[u8], storage: &mut [MaybeUninit<u8>]) -> io::Result<()> {
    if let Some(dst) = storage.get_mut(..bytes.len()) {
        std::fs::write(path, dst.write_copy_of_slice(bytes))
    } else {
        std::fs::write(path, bytes)
    }
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
