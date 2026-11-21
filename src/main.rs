mod aggregate;
mod ingest;
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
    let loaded = ingest::load(&data_dir, threads);
    eprintln!("[main] ingest done {:.3}s", t0.elapsed().as_secs_f64());
    aggregate::run(loaded, &output_dir, threads);
    eprintln!("[main] total {:.3}s", t0.elapsed().as_secs_f64());
    // Skip destructors of large buffers.
    std::process::exit(0);
}
