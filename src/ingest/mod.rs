//! Bytes on disk -> `Loaded` or direct Parquet output. Owner: ingest agent. See docs/ingest.md.
//!
//! `fast` is the compact parallel exact path (`u64`/`u128` inline keys, <=256 categories, <=42d
//! window). Whenever an input exceeds `fast`'s compact limits, `run_pipeline` automatically runs
//! `generic::run_to_output` — a parallel, memory-bounded exact path supporting arbitrary ID lengths,
//! category counts, and window spans. `INGEST_GENERIC=1` forces the parallel generic path;
//! `INGEST_SLOW=1` forces single-threaded `slow::load` (in-repo differential oracle);
//! `INGEST_DUMP=<file>` writes a canonical text dump of `Loaded`.
pub mod csv;
pub mod fast;
pub mod generic;
pub mod key;
pub mod keys;
pub mod par;
pub mod scan;
pub mod slow;
pub mod small;
pub mod ts;
pub mod tsfast;

use crate::aggregate;
use crate::types::*;
use std::io::Write;
use std::path::Path;

#[allow(dead_code)]
pub enum PipelineArena {
    Fast(par::SinkArena),
    ProtoC(crate::proto_c::arena::Arena),
}

/// End-to-end pipeline entry point: uses the packed Prototype C fast path first,
/// then the fused u64/u128 fast path, falling back to the parallel memory-bounded
/// generic path (`generic::run_to_output`) otherwise. `INGEST_SLOW=1` or `INGEST_DUMP`
/// routes through `Loaded` + `aggregate::run` for differential verification.
pub fn run_pipeline(data_dir: &Path, output_dir: &Path, threads: usize) -> Option<PipelineArena> {
    let slow_forced = std::env::var("INGEST_SLOW").map(|v| v == "1").unwrap_or(false);
    let generic_forced = std::env::var("INGEST_GENERIC").map(|v| v == "1").unwrap_or(false);
    let no_proto_c = std::env::var("INGEST_NO_PROTO_C").map(|v| v == "1").unwrap_or(false);
    let dump_path = std::env::var("INGEST_DUMP").ok();
    if !slow_forced && dump_path.is_none() {
        if !generic_forced {
            if !no_proto_c {
                match crate::proto_c::run_to_output(data_dir, output_dir, threads) {
                    Ok(arena) => return Some(PipelineArena::ProtoC(arena)),
                    Err(reason) if reason.starts_with("exact-order:") => {
                        eprintln!("[ingest] proto-c fallback ({reason}): using exact-order slow path");
                        let loaded = slow::load(data_dir);
                        aggregate::run(loaded, output_dir, threads);
                        return None;
                    }
                    Err(reason) => {
                        eprintln!("[ingest] proto-c fallback ({reason}): using fast u64/u128 path");
                    }
                }
            }
            if let Ok(mapping) = fast::run_to_output(data_dir, output_dir, threads) {
                return mapping.map(PipelineArena::Fast);
            }
            eprintln!("[ingest] fast path not applicable: using parallel generic path");
        }
        generic::run_to_output(data_dir, output_dir, threads);
        return None;
    }
    let loaded = if slow_forced {
        par::diagnose(threads, None, 0);
        slow::load(data_dir)
    } else {
        match fast::load(data_dir, threads) {
            Some(l) => l,
            None => {
                par::diagnose(threads, None, 0);
                eprintln!("[ingest] fast path not applicable (long id): using generic path");
                slow::load(data_dir)
            }
        }
    };
    if let Some(p) = dump_path {
        dump(&loaded, Path::new(&p));
    }
    aggregate::run(loaded, output_dir, threads);
    None
}

#[allow(dead_code)]
pub fn load(data_dir: &Path, threads: usize) -> Loaded {
    let slow_forced = std::env::var("INGEST_SLOW").map(|v| v == "1").unwrap_or(false);
    let loaded = if slow_forced {
        slow::load(data_dir)
    } else {
        match fast::load(data_dir, threads) {
            Some(l) => l,
            None => {
                eprintln!("[ingest] fast path not applicable (long id): using generic path");
                slow::load(data_dir)
            }
        }
    };
    if let Ok(p) = std::env::var("INGEST_DUMP") {
        dump(&loaded, Path::new(&p));
    }
    loaded
}

/// Canonical text dump: persons sorted by id; events sorted by (ts, category, cents);
/// clicks in delivered order (which the contract fixes up to identical clicks).
pub fn dump(l: &Loaded, path: &Path) {
    let mut order: Vec<usize> = (0..l.persons()).collect();
    order.sort_by(|&a, &b| l.person_ids.get(a).cmp(l.person_ids.get(b)));
    let mut w = std::io::BufWriter::new(std::fs::File::create(path).expect("dump file"));
    writeln!(w, "window {} {}", l.window_start_ms, l.window_end_ms).unwrap();
    for c in &l.categories {
        writeln!(w, "category {}", String::from_utf8_lossy(c).escape_debug()).unwrap();
    }
    for (c, ch) in &l.campaigns {
        writeln!(w, "campaign {} {}", String::from_utf8_lossy(c).escape_debug(), String::from_utf8_lossy(ch).escape_debug()).unwrap();
    }
    for p in order {
        writeln!(w, "P {:?}", String::from_utf8_lossy(l.person_ids.get(p))).unwrap();
        let mut ev: Vec<Event> = l.person_events(p).to_vec();
        assert!(ev.windows(2).all(|x| x[0].ts_ms <= x[1].ts_ms), "events not sorted");
        ev.sort_by_key(|e| (e.ts_ms, e.category, e.cents));
        for e in ev {
            writeln!(w, "E {} {} {}", e.ts_ms, e.category, e.cents).unwrap();
        }
        let ck = l.person_clicks(p);
        assert!(ck.windows(2).all(|x| x[0].ts_ms <= x[1].ts_ms), "clicks not sorted");
        for c in ck {
            writeln!(w, "C {} {}", c.ts_ms, c.campaign).unwrap();
        }
    }
}

/// Microbenchmark hook: `INGEST_BENCH=<csv file>` (events file) prints single-thread
/// throughput of scanner and parsing pieces on an in-memory copy.
pub fn bench(path: &Path) {
    let data = std::fs::read(path).unwrap();
    let mut buf = data.clone();
    let hi = buf.len();
    buf.resize(hi + scan::PAD + 1, 0);
    let lo = data.iter().position(|&b| b == b'\n').unwrap() + 1;
    let mb = (hi - lo) as f64 / 1e6;
    for round in 0..3 {
        let t = par::thread_cpu();
        let mut n = 0u64;
        scan::scan::<7, _>(&buf, lo, hi, |_, f| n += f[1].0 as u64);
        let d = par::thread_cpu() - t;
        let t2 = par::thread_cpu();
        let mut x = 0i64;
        scan::scan::<7, _>(&buf, lo, hi, |b, f| {
            x += ts::parse_ts(&b[f[2].0 as usize..f[2].1 as usize]).unwrap_or(0);
            x += ts::parse_ts(&b[f[3].0 as usize..f[3].1 as usize]).unwrap_or(0);
        });
        let d2 = par::thread_cpu() - t2;
        let t3 = par::thread_cpu();
        let mut lines = 0u64;
        for l in csv::data_lines(&data) {
            let mut f = [&b""[..]; 7];
            if csv::split(l, &mut f) {
                lines += f[1].len() as u64;
            }
        }
        let d3 = par::thread_cpu() - t3;
        eprintln!("round {round}: scan {:.0} MB/s ({n}) | scan+2ts {:.0} MB/s ({x}) | scalar split {:.0} MB/s ({lines})", mb / d, mb / d2, mb / d3);
    }
}
