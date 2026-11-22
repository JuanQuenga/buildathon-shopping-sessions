mod aggregate;
mod ingest;
mod types;

use std::path::PathBuf;
use std::time::Instant;

fn main() {
    // The fixture runner passes DATA_DIR/OUTPUT_DIR env plus trailing args; ignore args.
    let t0 = Instant::now();
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    tune_malloc();
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
    let _parse_mapping = ingest::run_pipeline(&data_dir, &output_dir, threads);
    if std::env::var_os("INGEST_DIAG").is_some() {
        let hwm = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.split_whitespace().nth(1).unwrap_or("0").to_string()))
            .unwrap_or_default();
        let hwm_mb: u64 = hwm.parse::<u64>().unwrap_or(0) / 1024;
        if let Ok(rollup) = std::fs::read_to_string("/proc/self/smaps_rollup") {
            let kib = |field: &str| {
                rollup.lines().find_map(|line| line.strip_prefix(field))
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok()).unwrap_or(0)
            };
            let rss_kb = kib("Rss:");
            let anon_huge_kb = kib("AnonHugePages:");
            eprintln!("[stage] peak_rss_mb={hwm_mb} rss_kb={rss_kb} anon_huge_kb={anon_huge_kb}");
        } else {
            eprintln!("[stage] peak_rss_mb={hwm_mb}");
        }
    }
    if let Some(ref arena) = _parse_mapping {
        arena.release_par(threads);
    }
    let (user, sys, minflt) = ingest::par::rusage_self();
    let wall = t0.elapsed().as_secs_f64();
    eprintln!("[main] total wall={wall:.6}s user={user:.6}s sys={sys:.6}s minflt={minflt}");
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn tune_malloc() {
    extern "C" {
        fn mallopt(param: std::ffi::c_int, value: std::ffi::c_int) -> std::ffi::c_int;
    }
    const M_TRIM_THRESHOLD: i32 = -1;
    const M_TOP_PAD: i32 = -2;
    const M_MMAP_THRESHOLD: i32 = -3;
    unsafe {
        mallopt(M_MMAP_THRESHOLD, 32 << 20);
        mallopt(M_TRIM_THRESHOLD, -1);
        mallopt(M_TOP_PAD, 16 << 20);
    }
}
