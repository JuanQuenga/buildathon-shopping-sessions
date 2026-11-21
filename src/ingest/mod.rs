//! Bytes on disk -> `Loaded`. Owner: ingest agent. See docs/ingest.md.
//!
//! `fast` is the parallel exact path; it returns `None` (and we fall back to the
//! generic `slow` path) whenever the input contains an id it cannot carry exactly
//! (> 15 bytes). `INGEST_SLOW=1` forces the slow path; `INGEST_DUMP=<file>` writes
//! a canonical text dump of the result (for differential checks).
pub mod csv;
pub mod fast;
pub mod key;
pub mod par;
pub mod scan;
pub mod slow;
pub mod small;
pub mod ts;

use crate::types::*;
use std::io::Write;
use std::path::Path;

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
