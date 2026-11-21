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
        UserCols {
            n: 0,
            ids: Vec::with_capacity(rows * 16),
            day: Vec::with_capacity(rows),
            sessions: Vec::with_capacity(rows),
            events: Vec::with_capacity(rows),
            revenue: Vec::with_capacity(rows),
            defs: Vec::with_capacity(rows / 8 + 1),
            top: Vec::with_capacity(rows * 4),
        }
    }
    pub fn len(&self) -> usize {
        self.n
    }
    #[inline]
    pub fn push(&mut self, id: &[u8], day: i32, sessions: u32, events: u32, revenue: i64, top: Option<&[u8]>) {
        self.ids.extend_from_slice(&(id.len() as u32).to_le_bytes());
        self.ids.extend_from_slice(id);
        self.day.push(day);
        self.sessions.push(sessions as i64);
        self.events.push(events as i64);
        self.revenue.push(revenue);
        if self.n % 8 == 0 {
            self.defs.push(0);
        }
        if let Some(t) = top {
            *self.defs.last_mut().unwrap() |= 1 << (self.n % 8);
            self.top.extend_from_slice(&(t.len() as u32).to_le_bytes());
            self.top.extend_from_slice(t);
        }
        self.n += 1;
    }
}

/// Writes one worker's column buffers as `part-{worker}.parquet`.
pub fn write_user_cols(dir: &Path, worker: usize, c: &UserCols) -> io::Result<()> {
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
    parquet::write_columns(&dir.join(format!("part-{worker}.parquet")), c.n, &cols)
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
