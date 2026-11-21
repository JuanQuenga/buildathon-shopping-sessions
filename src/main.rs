mod aggregate;
mod ingest;
mod probe;
mod types;

use std::path::PathBuf;
use std::time::Instant;

fn main() {
    // The fixture runner passes DATA_DIR/OUTPUT_DIR env plus trailing args; ignore args.
    let t0 = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let output_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    if let Ok(p) = std::env::var("INGEST_BENCH") {
        ingest::bench(std::path::Path::new(&p));
        return;
    }
    if let Ok(p) = std::env::var("INGEST_BENCH2") {
        ingest::fast::bench_rows(std::path::Path::new(&p), &data_dir);
        return;
    }
    probe::run(&data_dir, &output_dir, threads).expect("write probe output");
    let hwm = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.split_whitespace().nth(1).unwrap_or("0").to_string()))
        .unwrap_or_default();
    let hwm_mb: u64 = hwm.parse::<u64>().unwrap_or(0) / 1024;
    eprintln!("[stage] peak_rss_mb={hwm_mb}");
    eprintln!("[main] total {:.3}s peak_rss_kb {hwm}", t0.elapsed().as_secs_f64());
    // Skip destructors of large buffers.
    std::process::exit(0);
}
