//! Local and scorer diagnostics for the probe branch.
use crate::aggregate::output::write_empty_user;
use crate::ingest::{
    self,
    keys::Key,
    scan::{ProbeScanner, Scratch, PAD},
    small,
    tsfast::DateTable,
};
use std::ffi::{c_int, c_long, c_ulong};
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::time::{Duration, Instant};

const MIB: usize = 1024 * 1024;
const HUGE: usize = 2 * MIB;
const PAGE: usize = 4096;
const REPEATS: usize = 4;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Timeval {
    sec: c_long,
    usec: c_long,
}

impl Timeval {
    fn ms(self) -> f64 {
        self.sec as f64 * 1000.0 + self.usec as f64 / 1000.0
    }
}

#[repr(C)]
#[derive(Default)]
struct Rusage {
    user: Timeval,
    sys: Timeval,
    maxrss: c_long,
    unused: [c_long; 3],
    minflt: c_long,
    majflt: c_long,
    tail: [c_long; 8],
}

#[derive(Clone, Copy, Default)]
struct Usage {
    user_ms: f64,
    sys_ms: f64,
    minflt: c_long,
    majflt: c_long,
    maxrss_kb: c_long,
}

impl Usage {
    fn read() -> Self {
        let mut r = Rusage::default();
        if unsafe { getrusage(0, &mut r) } != 0 {
            eprintln!("[probe] getrusage error={}", io::Error::last_os_error());
        }
        Self {
            user_ms: r.user.ms(),
            sys_ms: r.sys.ms(),
            minflt: r.minflt,
            majflt: r.majflt,
            maxrss_kb: r.maxrss,
        }
    }

    fn since(self, before: Self) -> Self {
        Self {
            user_ms: self.user_ms - before.user_ms,
            sys_ms: self.sys_ms - before.sys_ms,
            minflt: self.minflt - before.minflt,
            majflt: self.majflt - before.majflt,
            maxrss_kb: self.maxrss_kb,
        }
    }
}

struct Timer {
    started: Instant,
    usage: Usage,
}

impl Timer {
    fn start() -> Self {
        let usage = Usage::read();
        Self {
            started: Instant::now(),
            usage,
        }
    }

    fn finish(self) -> (f64, Usage) {
        let wall_ms = self.started.elapsed().as_secs_f64() * 1000.0;
        (wall_ms, Usage::read().since(self.usage))
    }
}

#[repr(C)]
struct IoVec {
    base: *mut u8,
    len: usize,
}

extern "C" {
    fn getrusage(who: c_int, usage: *mut Rusage) -> c_int;
    fn syscall(number: c_long, ...) -> c_long;
    fn mmap(addr: *mut u8, len: usize, prot: c_int, flags: c_int, fd: c_int, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: c_int) -> c_int;
    fn munmap(addr: *mut u8, len: usize) -> c_int;
}

fn read(path: &str) -> Option<String> {
    fs::read_to_string(path).ok().map(|s| s.trim().to_owned())
}

fn value<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key.trim() == name).then(|| value.trim())
    })
}

fn system(threads: usize) {
    eprintln!(
        "[probe] system kernel={}",
        read("/proc/version").as_deref().unwrap_or("n/a")
    );
    let cpu = read("/proc/cpuinfo").unwrap_or_default();
    let status = read("/proc/self/status").unwrap_or_default();
    let cpus = value(&status, "Cpus_allowed_list").unwrap_or("n/a");
    let model = value(&cpu, "model name").unwrap_or("n/a");
    let mhz = value(&cpu, "cpu MHz").unwrap_or("n/a");
    let cache = value(&cpu, "cache size").unwrap_or("n/a");
    eprintln!("[probe] cpu model={model:?} first_cpu_mhz={mhz} cache={cache:?} available_parallelism={threads} Cpus_allowed_list={cpus}");
    let flags = value(&cpu, "flags");
    let flags = [
        "avx2",
        "bmi2",
        "avx512f",
        "avx512bw",
        "avx512vl",
        "avx512dq",
        "avx512cd",
        "avx512vnni",
        "avx512vbmi",
        "avx512vpopcntdq",
    ]
    .map(|flag| {
        let present = flags.map_or("n/a", |flags| {
            if flags.split_whitespace().any(|f| f.replace('_', "") == flag) {
                "1"
            } else {
                "0"
            }
        });
        format!("{flag}={present}")
    });
    eprintln!("[probe] cpu_flags {}", flags.join(" "));

    let cgroup = "/sys/fs/cgroup";
    let controller = |file: &str, v1: &str| {
        read(&format!("{cgroup}/{file}"))
            .map(|v| format!("{file}={v:?}"))
            .or_else(|| read(&format!("{cgroup}/{v1}")).map(|v| format!("{v1}={v:?}")))
            .unwrap_or_else(|| format!("{file}=n/a"))
    };
    let cpu_limit = read("/sys/fs/cgroup/cpu.max").map_or_else(
        || {
            [
                controller("cpu.cfs_quota_us", "cpu/cpu.cfs_quota_us"),
                controller("cpu.cfs_period_us", "cpu/cpu.cfs_period_us"),
            ]
            .join(" ")
        },
        |v| format!("cpu.max={v:?}"),
    );
    let mem_limit = controller("memory.max", "memory/memory.limit_in_bytes");
    let cpuset = controller("cpuset.cpus.effective", "cpuset/cpuset.cpus");
    eprintln!("[probe] cgroup {cpu_limit} {mem_limit} {cpuset}");

    let mem = read("/proc/meminfo").unwrap_or_default();
    let memory = [
        "MemTotal",
        "MemFree",
        "MemAvailable",
        "Cached",
        "AnonHugePages",
        "HugePages_Total",
    ]
    .map(|k| format!("{k}={:?}", value(&mem, k).unwrap_or("n/a")));
    eprintln!("[probe] meminfo {}", memory.join(" "));
    let thp = |file| {
        read(&format!("/sys/kernel/mm/transparent_hugepage/{file}")).unwrap_or_else(|| "n/a".into())
    };
    eprintln!(
        "[probe] thp enabled={:?} defrag={:?}",
        thp("enabled"),
        thp("defrag")
    );

    let mounts = read("/proc/mounts").unwrap_or_default();
    for target in ["/", "/data", "/output", "/scratch", "/tmp"] {
        let entry = mounts
            .lines()
            .filter_map(|line| {
                let mut fields = line.split_whitespace();
                fields.next()?;
                (fields.next()? == target).then_some((fields.next()?, fields.next()?))
            })
            .last();
        if let Some((fs_type, options)) = entry {
            let options = options
                .split(',')
                .map(|opt| match opt.split_once('=') {
                    Some((key, val)) if val.len() > 96 => format!("{key}=<{}-bytes>", val.len()),
                    _ => opt.to_owned(),
                })
                .collect::<Vec<_>>()
                .join(",");
            eprintln!("[probe] mount path={target} fs={fs_type} options={options}");
        } else {
            eprintln!("[probe] mount path={target} fs=n/a");
        }
    }
}

fn dataset(data_dir: &Path) -> Vec<(PathBuf, u64)> {
    let files = |sub| {
        let mut files: Vec<_> = fs::read_dir(data_dir.join(sub))
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                let meta = path.metadata().ok()?;
                (meta.is_file() && path.extension()?.eq("csv")).then_some((path, meta.len()))
            })
            .collect();
        files.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let bytes: u64 = files.iter().map(|(_, len)| len).sum();
        eprintln!(
            "[probe] dataset table={sub} files={} bytes={bytes}",
            files.len()
        );
        files
    };
    let events = files("events");
    files("clicks");
    files("links");
    for name in ["products.csv", "campaigns.csv"] {
        match data_dir.join(name).metadata() {
            Ok(m) => eprintln!("[probe] dataset file={name} bytes={}", m.len()),
            Err(e) => eprintln!("[probe] dataset file={name} error={e}"),
        }
    }

    if let Some((path, _)) = events.first() {
        let result = match nowait(path) {
            Ok(0) => "EMPTY bytes=0".into(),
            Ok(n) => format!("WARM bytes={n}"),
            Err(e) if e.raw_os_error() == Some(11) => "COLD (EAGAIN) errno=11".into(),
            Err(e) => format!("ERROR errno={:?} detail={e}", e.raw_os_error()),
        };
        eprintln!(
            "[probe] cache file={} requested_bytes=262144 result={result}",
            path.display()
        );
    } else {
        eprintln!("[probe] cache n/a (no event CSVs)");
    }
    events
}

fn nowait(path: &Path) -> io::Result<c_long> {
    const SYS_PREADV2: c_long = 327; // Linux x86-64. 295 is preadv without flags.
    const RWF_NOWAIT: c_int = 0x8;
    let file = File::open(path)?;
    let mut bytes = vec![0; 256 * 1024];
    let iov = IoVec {
        base: bytes.as_mut_ptr(),
        len: bytes.len(),
    };
    // Raw preadv2 takes the offset in two arguments, followed by flags.
    let got = unsafe {
        syscall(
            SYS_PREADV2,
            file.as_raw_fd(),
            &iov as *const IoVec,
            1 as c_int,
            0 as c_ulong,
            0 as c_ulong,
            RWF_NOWAIT,
        )
    };
    if got < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(got)
    }
}

#[derive(Clone, Copy)]
enum Advice {
    Default,
    Huge,
    NoHuge,
}

impl Advice {
    fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Huge => "MADV_HUGEPAGE",
            Self::NoHuge => "MADV_NOHUGEPAGE",
        }
    }
}

struct Arena {
    addr: *mut u8,
    len: usize,
}

fn unmap(addr: *mut u8, len: usize) -> io::Result<()> {
    if len != 0 && unsafe { munmap(addr, len) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

impl Arena {
    fn new(len: usize, advice: Advice) -> io::Result<Self> {
        let total = len + HUGE;
        // PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS.
        let raw = unsafe { mmap(std::ptr::null_mut(), total, 3, 0x22, -1, 0) };
        if raw as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        let prefix = raw.addr().next_multiple_of(HUGE) - raw.addr();
        // SAFETY: both offsets lie within the live mapping.
        let addr = unsafe { raw.add(prefix) };
        let suffix_addr = unsafe { addr.add(len) };
        let mut arena = Self {
            addr: raw,
            len: total,
        };
        unmap(raw, prefix)?;
        arena.addr = addr;
        arena.len -= prefix;
        unmap(suffix_addr, total - prefix - len)?;
        arena.len = len;

        let code = match advice {
            Advice::Default => return Ok(arena),
            Advice::Huge => 14,
            Advice::NoHuge => 15,
        };
        if unsafe { madvise(addr, len, code) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(arena)
    }

    fn touch(&mut self) {
        for offset in (0..self.len).step_by(PAGE) {
            // SAFETY: the arena uniquely owns this writable mapping.
            unsafe { self.addr.add(offset).write_volatile(1) };
        }
    }

    fn unmap(mut self) -> io::Result<()> {
        unmap(self.addr, self.len)?;
        self.len = 0;
        Ok(())
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let _ = unmap(self.addr, self.len);
    }
}

fn anon_huge_kb() -> Option<i64> {
    value(&read("/proc/self/smaps_rollup")?, "AnonHugePages")?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Time the work after every worker has prepared its own state.
fn measure_workers<S, R: Send>(
    threads: usize,
    prepare: impl Fn(usize) -> S + Sync,
    work: impl Fn(S) -> R + Sync,
) -> (f64, Usage, Vec<R>) {
    if threads == 1 {
        let state = prepare(0);
        let timer = Timer::start();
        let result = work(state);
        let (wall, usage) = timer.finish();
        return (wall, usage, vec![result]);
    }
    let ready = Barrier::new(threads + 1);
    let start = Barrier::new(threads + 1);
    let finish = Barrier::new(threads + 1);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|id| {
                let (ready, start, finish, prepare, work) =
                    (&ready, &start, &finish, &prepare, &work);
                scope.spawn(move || {
                    let state = prepare(id);
                    ready.wait();
                    start.wait();
                    let result = work(state);
                    finish.wait();
                    finish.wait();
                    result
                })
            })
            .collect();
        ready.wait();
        let timer = Timer::start();
        start.wait();
        finish.wait();
        let (wall, usage) = timer.finish();
        finish.wait();
        let results = workers.into_iter().map(|t| t.join().unwrap()).collect();
        (wall, usage, results)
    })
}

fn fault_and_unmap(advice: Advice) -> io::Result<()> {
    const LEN: usize = 32 * MIB;
    let mut arena = Arena::new(LEN, advice)?;
    let huge_before = anon_huge_kb();
    let timer = Timer::start();
    arena.touch();
    let (wall, usage) = timer.finish();
    let huge_delta = huge_before
        .zip(anon_huge_kb())
        .map(|(a, b)| (b - a).to_string());
    let huge_delta = huge_delta.as_deref().unwrap_or("n/a");
    let timer = Timer::start();
    arena.unmap()?;
    let (unmap_wall, unmap_usage) = timer.finish();
    let mode = advice.name();
    let us_per_2mb = wall * 1000.0 / (LEN / HUGE) as f64;
    eprintln!("[probe] fault mode={mode} mib=32 wall_ms={wall:.3} us_per_2mb={us_per_2mb:.2} sys_ms={:.3} minflt={} anon_huge_kb_delta={huge_delta} unmap_wall_ms={unmap_wall:.3} unmap_sys_ms={:.3}", usage.sys_ms, usage.minflt, unmap_usage.sys_ms);
    Ok(())
}

fn unmap_bench(advice: Advice, threads: usize) -> io::Result<()> {
    const HUGE_PAGES: usize = 128 * MIB / HUGE;
    let threads = threads.min(HUGE_PAGES);
    let (wall, usage, results) = measure_workers(
        threads,
        |id| {
            let pages = HUGE_PAGES / threads + usize::from(id < HUGE_PAGES % threads);
            let mut arena = Arena::new(pages * HUGE, advice)?;
            arena.touch();
            Ok(arena)
        },
        |arena: io::Result<Arena>| arena?.unmap(),
    );
    for result in results {
        result?;
    }
    eprintln!(
        "[probe] unmap mode={} mib=128 threads={threads} wall_ms={wall:.3} sys_ms={:.3}",
        advice.name(),
        usage.sys_ms
    );
    Ok(())
}

fn sample_events(events: &[(PathBuf, u64)]) -> io::Result<(Vec<u8>, usize)> {
    const SAMPLE: usize = 4 * MIB;
    const TARGET: usize = 16 * MIB;
    let mut sample = Vec::with_capacity(TARGET + PAD);
    let mut part = Vec::new();
    for (path, size) in events {
        let remaining = SAMPLE - sample.len();
        if remaining == 0 {
            break;
        }
        part.clear();
        File::open(path)?
            .take(remaining as u64)
            .read_to_end(&mut part)?;
        let Some(header) = part.iter().position(|&b| b == b'\n') else {
            continue;
        };
        let body = &part[header + 1..];
        if part.len() as u64 == *size
            && !body.is_empty()
            && body.last() != Some(&b'\n')
            && part.len() < remaining
        {
            sample.extend_from_slice(body);
            sample.push(b'\n');
        } else if let Some(end) = body.iter().rposition(|&b| b == b'\n') {
            sample.extend_from_slice(&body[..=end]);
        }
    }
    let source_bytes = sample.len();
    if source_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no complete event rows",
        ));
    }
    while sample.len() < TARGET {
        let take = sample.len().min(TARGET - sample.len());
        sample.extend_from_within(..take);
    }
    let end = sample.iter().rposition(|&b| b == b'\n').unwrap() + 1;
    sample.resize(end + PAD, 0);
    Ok((sample, source_bytes))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ScanResult {
    rows: u64,
    fields: u64,
    parsed: u64,
    ts_ok: u64,
    key_ok: u64,
}

fn scan_pass<const PARSE: bool>(
    scanner: ProbeScanner,
    sample: &[u8],
    dt: &DateTable,
    mut scratch: Scratch,
) -> ScanResult {
    let mut result = ScanResult::default();
    for _ in 0..REPEATS {
        scanner.scan(sample, sample.len() - PAD, &mut scratch, |buf, fields| {
            result.rows += 1;
            // All offsets refer to the same tiles of the same input in each variant.
            let widths_and_offsets = fields.iter().fold(0u64, |sum, &(start, end)| {
                sum.wrapping_add((u64::from(start) << 16) | u64::from(end))
            });
            result.fields = result.fields.rotate_left(7) ^ widths_and_offsets;
            if PARSE {
                let (ts_start, ts_end) = fields[2];
                let (id_start, id_end) = fields[1];
                // SAFETY: the scanner reports ranges in buf, with at least PAD bytes after hi.
                unsafe {
                    let ts = dt.parse(
                        buf.as_ptr().add(ts_start as usize),
                        (ts_end - ts_start) as usize,
                    );
                    let id = u64::enc(
                        buf.as_ptr().add(id_start as usize),
                        (id_end - id_start) as usize,
                    );
                    result.ts_ok += u64::from(ts.is_some());
                    result.key_ok += u64::from(id.is_some());
                    result.parsed = result
                        .parsed
                        .wrapping_add(ts.unwrap_or_default() as u64 ^ id.unwrap_or_default());
                }
            }
        });
    }
    result
}

fn scan_bench(data_dir: &Path, events: &[(PathBuf, u64)], threads: usize) -> io::Result<()> {
    let (sample, source_bytes) = sample_events(events)?;
    let sample_bytes = sample.len() - PAD;
    let (ws, we) = small::read_manifest(data_dir);
    let dt = DateTable::build(ws, we);
    eprintln!(
        "[probe] scan sample_bytes={sample_bytes} source_bytes={source_bytes} repeats={REPEATS}"
    );
    let mut expected = [None, None];
    let mut checked = 0;
    let mut matches = true;
    for (name, scanner) in ProbeScanner::variants() {
        let Some(scanner) = scanner else {
            eprintln!("[probe] scan variant={name} status=UNSUPPORTED");
            continue;
        };
        checked += 1;
        for n in [1, threads] {
            for (parse, mode) in ["scan", "scan+parse"].into_iter().enumerate() {
                // Give the previous SIMD run time to leave any elevated frequency license.
                std::thread::sleep(Duration::from_millis(3));
                let (wall, usage, results) = measure_workers(
                    n,
                    |_| Scratch::default(),
                    |scratch| {
                        if parse == 0 {
                            scan_pass::<false>(scanner, &sample, &dt, scratch)
                        } else {
                            scan_pass::<true>(scanner, &sample, &dt, scratch)
                        }
                    },
                );
                let first = results[0];
                let reference = *expected[parse].get_or_insert(first);
                let equal = first.rows != 0 && results.iter().all(|r| *r == reference);
                matches &= equal;
                let rows: u64 = results.iter().map(|r| r.rows).sum();
                let ns_per_row = wall * 1e6 / rows as f64;
                eprintln!("[probe] scan variant={name} mode={mode} threads={n} rows={rows} fields={:016x} parsed={:016x} ts_ok={} key_ok={} ns_per_row={ns_per_row:.2} wall_ms={wall:.3} user_ms={:.3} match={equal}", first.fields, first.parsed, first.ts_ok, first.key_ok, usage.user_ms);
            }
            if threads == 1 {
                break;
            }
        }
    }
    let status = if !matches {
        "MISMATCH"
    } else if checked == 3 {
        "MATCH"
    } else {
        "INCOMPLETE"
    };
    eprintln!("[probe] scan verification variants={checked}/3 status={status}");
    Ok(())
}

pub fn run(data_dir: &Path, output_dir: &Path, pipeline_threads: usize) -> io::Result<()> {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    system(threads);
    let events = dataset(data_dir);
    for advice in [Advice::Default, Advice::Huge, Advice::NoHuge] {
        if let Err(e) = fault_and_unmap(advice) {
            eprintln!("[probe] fault mode={} error={e}", advice.name());
        }
    }
    for advice in [Advice::Default, Advice::Huge] {
        for n in [1, threads] {
            if let Err(e) = unmap_bench(advice, n) {
                eprintln!("[probe] unmap mode={} threads={n} error={e}", advice.name());
            }
            if threads == 1 {
                break;
            }
        }
    }
    if let Err(e) = scan_bench(data_dir, &events, threads) {
        eprintln!("[probe] scan error={e}");
    }

    let timer = Timer::start();
    let before = timer.usage;
    ingest::run_pipeline(data_dir, output_dir, pipeline_threads);
    let (wall, delta) = timer.finish();
    eprintln!(
        "[probe] pipeline before user_ms={:.3} sys_ms={:.3} minflt={} majflt={} maxrss_kb={}",
        before.user_ms, before.sys_ms, before.minflt, before.majflt, before.maxrss_kb
    );
    eprintln!("[probe] pipeline threads={pipeline_threads} wall_ms={wall:.3} delta_user_ms={:.3} delta_sys_ms={:.3} delta_minflt={} delta_majflt={} maxrss_kb={}", delta.user_ms, delta.sys_ms, delta.minflt, delta.majflt, delta.maxrss_kb);
    let user = output_dir.join("user_daily");
    for entry in fs::read_dir(&user)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.as_encoded_bytes();
        if name.starts_with(b"part-") && name.ends_with(b".parquet") && entry.file_type()?.is_file()
        {
            fs::remove_file(entry.path())?;
        }
    }
    write_empty_user(&user)?;
    eprintln!("[probe] done — deliberate 0-row user_daily mismatch written");
    Ok(())
}
