mod csv;
mod direct;
mod group;
mod key;
mod links;
mod par;
mod rows;
mod scan;
mod small;
mod ts;
mod tsfast;

use self::key::IdLookup;
use self::par::{SyncPtr, Timer};
use self::scan::{Chunk, FileInfo, Kind};
use self::tsfast::DateTable;
use crate::proto_c::{arena::Arena, packed};
use std::path::Path;

type Result<T> = std::result::Result<T, &'static str>;
const PARTITIONS: usize = 1 << packed::PERSON_PART_BITS;
const MAX_THREADS: usize = 32;
const CHUNK: usize = 256 << 10;
const WEEK: i64 = 7 * ts::DAY_MS;

struct Chunks {
    files: Vec<FileInfo>,
    links: Vec<Chunk>,
    main: Vec<Chunk>,
}

struct ReadBuffers {
    _storage: Vec<u8>,
    base: SyncPtr<u8>,
    stride: usize,
}

/// # Safety
/// There can only be one active borrower per worker ID.
unsafe fn read<'a>(
    chunks: &Chunks,
    chunk: Chunk,
    buffers: &'a ReadBuffers,
    tid: usize,
) -> Result<(&'a [u8], usize, usize)> {
    let file = &chunks.files[chunk.file as usize];
    let buf = unsafe { buffers.base.slice(tid * buffers.stride, buffers.stride) };
    let (lo, hi) = scan::read_chunk(&file.handle, file.len, chunk, buf)
        .ok_or("CSV row exceeds the per-worker input buffer")?;
    Ok((buf, lo, hi))
}

struct Lookups {
    categories: Vec<Vec<u8>>,
    campaigns: Vec<(Vec<u8>, Vec<u8>)>,
    prod: IdLookup,
    camp: IdLookup,
}

struct ParseRows {
    ws: i64,
    we: i64,
    ws_sec: i64,
    we_sec: i64,
    ws_whole_sec: bool,
    ingest_base_s: i64,
    dt: DateTable,
    lookups: Lookups,
}

impl ParseRows {
    #[inline(always)]
    fn clicks_base(&self) -> i64 {
        self.ws - WEEK
    }

    #[inline(always)]
    fn ingest_rel(&self, sec: i64) -> Option<u32> {
        let rel = sec.checked_sub(self.ingest_base_s)? as u64;
        (rel < u64::from(packed::INVALID_INGEST)).then_some(rel as u32)
    }
}

fn arena_sizes(
    events: usize,
    clicks: usize,
    links: usize,
) -> [usize; 3] {
    // Reserve virtual address space for denser-than-typical files and skewed
    // partitions. Only appended blocks and initialized slices fault in pages.
    let persistent = (2 << 20) + links * 2 + events / 300;
    let phase = (events / 70 * rows::TOKEN_BYTES + (64 << 20)).max(4 * links + (8 << 20));
    let records = events / 70 * rows::EVENT_BYTES + clicks / 37 * 16 + (64 << 20);
    [persistent, phase, records]
}

fn prescan_compatible(files: &[FileInfo]) -> Result<()> {
    use std::os::unix::fs::FileExt;
    const CAP: usize = 16384;
    let mut buf = [0u8; CAP + scan::PAD];
    for kind in [Kind::Links, Kind::Events, Kind::Clicks] {
        let Some(fi) = files.iter().find(|f| f.kind == kind && f.len > 0) else {
            continue;
        };
        let n = (fi.len as usize).min(CAP);
        if fi.handle.read_at(&mut buf[..n], 0).is_err() {
            continue;
        }
        buf[n..n + scan::PAD].fill(0);
        let slice = &buf[..n + scan::PAD];
        let Some(first_nl) = scan::memchr(b'\n', &buf[..n]) else {
            continue;
        };
        let mut pos = first_nl + 1;
        let mut checked = 0usize;
        while pos < n && checked < 64 {
            let Some(rel_nl) = scan::memchr(b'\n', &buf[pos..n]) else {
                break;
            };
            let end = pos + rel_nl;
            match kind {
                Kind::Links => {
                    if let Some([a, b]) = scan::row_fields::<2>(slice, pos, end) {
                        let a = &slice[a.0 as usize..a.1 as usize];
                        let b = &slice[b.0 as usize..b.1 as usize];
                        if !a.is_empty() && !b.is_empty() {
                            if packed::encode(a, b'u').is_none() || packed::encode(b, b'u').is_none()
                            {
                                return Err("prescan: link user ID outside 43-bit alphabet");
                            }
                            checked += 1;
                        }
                    }
                }
                Kind::Events => {
                    if let Some(f) = scan::row_fields::<7>(slice, pos, end) {
                        let eid = &slice[f[0].0 as usize..f[0].1 as usize];
                        if !eid.is_empty() {
                            if packed::encode_eid(eid).is_none() {
                                return Err("prescan: event ID outside 43-bit alphabet");
                            }
                            checked += 1;
                        }
                    }
                }
                Kind::Clicks => {
                    if let Some(f) = scan::row_fields::<4>(slice, pos, end) {
                        let cid = &slice[f[0].0 as usize..f[0].1 as usize];
                        let uid = &slice[f[1].0 as usize..f[1].1 as usize];
                        if !cid.is_empty() && !uid.is_empty() {
                            if packed::encode(cid, b'k').is_none()
                                || packed::encode(uid, b'u').is_none()
                            {
                                return Err("prescan: click ID outside 43-bit alphabet");
                            }
                            checked += 1;
                        }
                    }
                }
            }
            pos = end + 1;
        }
    }
    Ok(())
}

pub fn run(data_dir: &Path, out: &Path, threads: usize) -> Result<Arena> {
    let threads = threads.clamp(1, MAX_THREADS);
    let mut timer = Timer::new("stage");
    let (ws, we) = small::read_manifest(data_dir);
    let span = we.checked_sub(ws).ok_or("manifest window overflow")?;
    if span < 0 || span > i64::from(packed::CLICK_TS_LIMIT) - WEEK || ws.checked_sub(WEEK).is_none()
    {
        return Err("event window plus seven-day lookback exceeds the 31-bit click timestamp");
    }
    let files = scan::open_csv_files(data_dir);
    prescan_compatible(&files)?;
    let byte_len = |kind| {
        files
            .iter()
            .filter(|f| f.kind == kind)
            .map(|f| f.len as usize)
            .sum()
    };
    let chunk_bytes = std::env::var("PROTO_C_CHUNK_KIB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map_or(CHUNK, |kb| kb.clamp(64, 2048) << 10);
    let stride = (2 * chunk_bytes + 16 * 1024).next_multiple_of(64);
    let event_bytes = byte_len(Kind::Events);
    let click_bytes = byte_len(Kind::Clicks);
    let sizes = arena_sizes(event_bytes, click_bytes, byte_len(Kind::Links));
    let (link_chunks, main_chunks): (Vec<Chunk>, Vec<Chunk>) = scan::plan(&files, chunk_bytes as u64)
        .into_iter()
        .partition(|c| files[c.file as usize].kind == Kind::Links);
    let chunks = Chunks {
        files,
        links: link_chunks,
        main: main_chunks,
    };
    let _prefetch = start_prefetch(&chunks.files, threads);
    let lookup_dir = data_dir.to_path_buf();
    let lookups_handle = std::thread::spawn(move || -> Result<Lookups> {
        let (categories, _, prod) = small::read_products_lookup(&lookup_dir);
        if categories.len() > 256 {
            return Err("more than 256 categories");
        }
        let (campaigns, camp) = small::read_campaigns_lookup(&lookup_dir);
        if campaigns.len() > packed::CAMPAIGN_LIMIT {
            return Err("more than 2,048 campaigns");
        }
        Ok(Lookups {
            categories,
            campaigns,
            prod,
            camp,
        })
    });
    let mut arena = Arena::new(&sizes).map_err(|_| "failed to map the process arena")?;
    eprintln!(
        "[arena] reserved_bytes={} chunk_bytes={chunk_bytes} workers={threads} token_bytes_per_row={} event_bytes_per_row={}",
        arena.len(), rows::TOKEN_BYTES, rows::EVENT_BYTES
    );
    let [persistent, mut phase, records] = arena.regions(sizes);
    // Keep pread buffers on the heap (outside the Arena VMA) so copy_to_user never
    // contends with concurrent Arena page faults or madvise calls.
    let mut storage = vec![0u8; threads * stride];
    let buffers = ReadBuffers {
        base: SyncPtr(storage.as_mut_ptr()),
        _storage: storage,
        stride,
    };
    timer.lap("small");

    let links = links::build(&chunks, &buffers, &persistent, &phase, threads, &mut timer)?;
    let link_scratch = phase.used();
    phase.rewind();
    let lookups = lookups_handle
        .join()
        .map_err(|_| "lookup loader thread panicked")??;
    let ctx = ParseRows {
        ws,
        we,
        ws_sec: ws.div_euclid(1000),
        we_sec: we.div_euclid(1000),
        ws_whole_sec: ws.rem_euclid(1000) == 0 && we.rem_euclid(1000) == 0,
        ingest_base_s: ws.div_euclid(1000) - 2 * (ts::DAY_MS / 1000),
        dt: DateTable::build(ws, we),
        lookups,
    };
    let populate = std::env::var("PROTO_C_POPULATE_WRITE").unwrap_or_default();
    if populate == "cursor" {
        phase.start_cursor_population(link_scratch);
        records.start_cursor_population(0);
    }
    if matches!(populate.as_str(), "whole" | "1") {
        let tokens = event_bytes / 77 * rows::TOKEN_BYTES;
        let parsed = event_bytes / 80 * rows::EVENT_BYTES + click_bytes / 49 * 16;
        let status = par::run(
            2.min(threads),
            2,
            |_| Ok(()),
            |error, i| {
                let outcome = if i == 0 {
                    phase.populate_write(tokens)
                } else {
                    records.populate_write(parsed)
                };
                if outcome.is_err() {
                    *error = outcome;
                }
            },
        );
        eprintln!(
            "[arena] populate_write tokens_bytes={tokens} records_bytes={parsed} applied={}",
            status.iter().all(std::result::Result::is_ok)
        );
        timer.lap("populate");
    }
    let rows::Parsed { live, tokens } =
        rows::parse(&ctx, &chunks, &buffers, &links, &records, &phase, threads)?;
    if populate == "cursor" {
        let tokens = phase.stop_cursor_population();
        let rows = records.stop_cursor_population();
        eprintln!(
            "[arena] cursor_populate_bytes={} token_bytes={tokens} record_bytes={rows}",
            tokens + rows
        );
    }
    let token_bytes = phase.used();
    let ne_raw = tokens.iter().flatten().map(|b| b.len()).sum::<usize>();
    let loc_overflows = tokens
        .iter()
        .flatten()
        .map(|b| b.overflow_count())
        .sum::<usize>();
    timer.lap("parse");
    let dead = rows::dedup(&tokens, &live, &phase, &persistent, threads)?;
    drop(tokens);
    let token_and_dedup_bytes = phase.used();
    timer.lap("dedup");
    phase.rewind();

    eprintln!(
        "[arena] links_scratch_bytes={link_scratch} token_bytes={token_bytes} token_and_dedup_bytes={token_and_dedup_bytes} token_loc_overflows={loc_overflows} records_bytes={} persistent_bytes={}",
        records.used(), persistent.used()
    );
    group::run(
        &ctx,
        &live,
        &dead,
        &phase,
        out,
        ne_raw,
        threads,
        &mut timer,
    )?;
    eprintln!("[arena] group_phase_high_water_bytes={}", phase.used());
    eprintln!("[ingest] path=proto-c");
    Ok(arena)
}

struct PrefetchGuard {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl Drop for PrefetchGuard {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}

fn start_prefetch(files: &[FileInfo], _threads: usize) -> Option<PrefetchGuard> {
    use std::os::fd::AsRawFd;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    if std::env::var_os("INGEST_NO_PREFETCH").is_some() {
        return None;
    }
    let sample = files
        .iter()
        .filter(|f| f.kind == Kind::Events && f.len != 0)
        .max_by_key(|f| f.len)
        .and_then(|f| crate::ingest::par::is_file_cached(f.handle.as_raw_fd(), f.len));
    if sample == Some(true) {
        return None;
    }
    const SEG: u64 = 16 << 20;
    let mut jobs = Vec::new();
    for kind in [Kind::Links, Kind::Events, Kind::Clicks] {
        for f in files.iter().filter(|f| f.kind == kind) {
            let fd = f.handle.as_raw_fd();
            for off in (0..f.len).step_by(SEG as usize) {
                jobs.push((fd, off, SEG.min(f.len - off)));
            }
        }
    }
    let count = 4usize.min(jobs.len());
    if count == 0 {
        return None;
    }
    let jobs = Arc::new(jobs);
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicUsize::new(0));
    let workers = (0..count)
        .map(|_| {
            let (jobs, stop, next) = (jobs.clone(), stop.clone(), next.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&(fd, off, len)) = jobs.get(i) else {
                        break;
                    };
                    crate::ingest::par::fadvise_range(fd, off, len);
                }
            })
        })
        .collect();
    Some(PrefetchGuard { stop, workers })
}

pub fn usage() -> (f64, f64, i64) {
    par::rusage_self()
}
