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
const PARTITIONS: usize = 256;
const MAX_THREADS: usize = 8;
const CHUNK: usize = 256 << 10;
const WEEK: i64 = 7 * ts::DAY_MS;

struct Chunks {
    files: Vec<FileInfo>,
    links: Vec<Chunk>,
    main: Vec<Chunk>,
}

struct ReadBuffers<'a> {
    _storage: &'a mut [u8],
    base: SyncPtr<u8>,
    stride: usize,
}

/// # Safety
/// There can only be one active borrower per worker ID.
unsafe fn read<'a>(
    chunks: &Chunks,
    chunk: Chunk,
    buffers: &'a ReadBuffers<'_>,
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
    threads: usize,
    stride: usize,
) -> [usize; 3] {
    // Reserve virtual address space for denser-than-typical files and skewed
    // partitions. Only appended blocks and initialized slices fault in pages.
    let persistent = threads * stride + (2 << 20) + links * 2 + events / 300;
    let phase = (events / 70 * rows::TOKEN_BYTES + (48 << 20)).max(4 * links + (8 << 20));
    let records = events / 70 * rows::EVENT_BYTES + clicks / 37 * 16 + (24 << 20);
    [persistent, phase, records]
}

pub fn run(data_dir: &Path, out: &Path, threads: usize, inline: bool) -> Result<Arena> {
    let threads = threads.clamp(1, MAX_THREADS);
    let mut timer = Timer::new("stage");
    let (ws, we) = small::read_manifest(data_dir);
    let span = we.checked_sub(ws).ok_or("manifest window overflow")?;
    if span < 0 || span > i64::from(packed::CLICK_TS_LIMIT) - WEEK || ws.checked_sub(WEEK).is_none()
    {
        return Err("event window plus seven-day lookback exceeds the 31-bit click timestamp");
    }
    let files = scan::open_csv_files(data_dir);
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
    let sizes = arena_sizes(
        event_bytes,
        click_bytes,
        byte_len(Kind::Links),
        threads,
        stride,
    );
    let (link_chunks, main_chunks) = scan::plan(&files, chunk_bytes as u64)
        .into_iter()
        .partition(|c| files[c.file as usize].kind == Kind::Links);
    let chunks = Chunks {
        files,
        links: link_chunks,
        main: main_chunks,
    };
    let (categories, _, prod) = small::read_products_lookup(data_dir);
    if categories.len() > 256 {
        return Err("more than 256 categories");
    }
    let (campaigns, camp) = small::read_campaigns_lookup(data_dir);
    if campaigns.len() > packed::CAMPAIGN_LIMIT {
        return Err("more than 2,048 campaigns");
    }
    let ctx = ParseRows {
        ws,
        we,
        ingest_base_s: ws.div_euclid(1000) - 2 * (ts::DAY_MS / 1000),
        dt: DateTable::build(ws, we),
        lookups: Lookups {
            categories,
            campaigns,
            prod,
            camp,
        },
    };
    let mut arena = Arena::new(&sizes).map_err(|_| "failed to map the process arena")?;
    eprintln!(
        "[arena] reserved_bytes={} chunk_bytes={chunk_bytes} workers={threads} token_bytes_per_row={} event_bytes_per_row={}",
        arena.len(), rows::TOKEN_BYTES, rows::EVENT_BYTES
    );
    let [persistent, mut phase, records] = arena.regions(sizes);
    let storage = persistent
        .slice(threads * stride, 0u8)
        .ok_or("pread buffer arena exhausted")?;
    let buffers = ReadBuffers {
        base: SyncPtr(storage.as_mut_ptr()),
        _storage: storage,
        stride,
    };
    timer.lap("small");

    let links = links::build(&chunks, &buffers, &persistent, &phase, threads, &mut timer)?;
    let link_scratch = phase.used();
    phase.rewind();
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
    let rows::Parsed { live, tokens } = if inline {
        rows::parse::<true>(&ctx, &chunks, &buffers, &links, &records, &phase, threads)?
    } else {
        rows::parse::<false>(&ctx, &chunks, &buffers, &links, &records, &phase, threads)?
    };
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

    let relocated = if inline {
        Vec::new()
    } else {
        let linked_events = live
            .iter()
            .map(|w| {
                w.events[PARTITIONS..]
                    .iter()
                    .map(|b| b.len())
                    .sum::<usize>()
            })
            .sum::<usize>();
        let linked_clicks = live
            .iter()
            .map(|w| {
                w.clicks[PARTITIONS..]
                    .iter()
                    .map(|b| b.len())
                    .sum::<usize>()
            })
            .sum::<usize>();
        let rows = rows::relocate(&live, &dead, &links, &phase, threads)?;
        eprintln!("[relocate] bloom_events={linked_events} bloom_clicks={linked_clicks}");
        timer.lap("relocate");
        rows
    };
    eprintln!(
        "[arena] links_scratch_bytes={link_scratch} token_bytes={token_bytes} token_and_dedup_bytes={token_and_dedup_bytes} token_loc_overflows={loc_overflows} records_bytes={} persistent_bytes={} relocated_bytes={}",
        records.used(), persistent.used(), phase.used()
    );
    group::run(
        &ctx, &live, &dead, &relocated, &phase, out, ne_raw, threads, &mut timer,
    )?;
    eprintln!("[arena] group_phase_high_water_bytes={}", phase.used());
    eprintln!("[ingest] path=proto-c");
    Ok(arena)
}

pub fn usage() -> (f64, f64, i64) {
    par::rusage_self()
}
