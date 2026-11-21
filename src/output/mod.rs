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
        rows.iter().map(|r| if r.top == NO_CATEGORY { None } else { Some(cats[r.top as usize].as_slice()) }),
    );
    f.finish()
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
