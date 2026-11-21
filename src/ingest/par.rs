//! Minimal persistent worker-pool work-stealing helpers and software write-combining sinks (no external deps).
use super::scan::{self, FileInfo, Kind};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Once, OnceLock};
use std::thread::JoinHandle;
use std::time::Instant;

/// Persistent spin-then-park worker pool: calling thread runs worker 0, background threads run
/// workers `1..threads`. Eliminates 11x OS thread spawn/join (`clone3` + stack mmap) per run!
struct Job {
    func: fn(*const u8, usize),
    data: *const u8,
    active_workers: usize,
    done: AtomicUsize,
}

struct PoolShared {
    epoch: AtomicU64,
    job: AtomicPtr<Job>,
    parked: AtomicUsize,
    mu: Mutex<u64>,
    cv: Condvar,
}

struct Pool {
    shared: &'static PoolShared,
    spawned: AtomicUsize,
    spawn_mu: Mutex<()>,
}

static POOL: OnceLock<Pool> = OnceLock::new();
static POOL_BUSY: AtomicBool = AtomicBool::new(false);

fn get_pool() -> &'static Pool {
    POOL.get_or_init(|| {
        let shared = Box::leak(Box::new(PoolShared {
            epoch: AtomicU64::new(0),
            job: AtomicPtr::new(std::ptr::null_mut()),
            parked: AtomicUsize::new(0),
            mu: Mutex::new(0),
            cv: Condvar::new(),
        }));
        Pool {
            shared,
            spawned: AtomicUsize::new(0),
            spawn_mu: Mutex::new(()),
        }
    })
}

fn ensure_workers(pool: &Pool, need_bg: usize) -> usize {
    let cur = pool.spawned.load(Ordering::Acquire);
    if cur >= need_bg {
        return cur;
    }
    let _g = pool.spawn_mu.lock().unwrap();
    let cur = pool.spawned.load(Ordering::Relaxed);
    if cur >= need_bg {
        return cur;
    }
    let init_epoch = pool.shared.epoch.load(Ordering::SeqCst);
    for wid in (cur + 1)..=need_bg {
        let shared = pool.shared;
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || {
                let mut seen = init_epoch;
                loop {
                    // Short spin catches back-to-back stages in < 1 us without burning CPU when idle.
                    let mut spins = 0u32;
                    let cur_ep = loop {
                        let e = shared.epoch.load(Ordering::Acquire);
                        if e != seen {
                            break e;
                        }
                        spins += 1;
                        if spins >= 512 {
                            shared.parked.fetch_add(1, Ordering::SeqCst);
                            let mut g = shared.mu.lock().unwrap();
                            while shared.epoch.load(Ordering::SeqCst) == seen {
                                g = shared.cv.wait(g).unwrap();
                            }
                            let e = shared.epoch.load(Ordering::Acquire);
                            drop(g);
                            shared.parked.fetch_sub(1, Ordering::AcqRel);
                            break e;
                        }
                        std::hint::spin_loop();
                    };
                    seen = cur_ep;
                    let jp = shared.job.load(Ordering::Acquire);
                    let job = unsafe { &*jp };
                    if wid < job.active_workers {
                        (job.func)(job.data, wid);
                    }
                    job.done.fetch_add(1, Ordering::Release);
                }
            })
            .expect("spawn pool worker");
    }
    pool.spawned.store(need_bg, Ordering::Release);
    need_bg
}

/// Runs `ntasks` tasks over `threads` threads; each thread owns a state from
/// `init(thread_index)`, and the states are returned in thread order.
pub fn run<S: Send, I, T>(threads: usize, ntasks: usize, init: I, task: T) -> Vec<S>
where
    I: Fn(usize) -> S + Sync,
    T: Fn(&mut S, usize) + Sync,
{
    run_fini(threads, ntasks, init, task, |_| {})
}

/// Like `run`, plus a per-thread `fini(&mut state)` hook executed on the worker
/// thread once all tasks are exhausted (e.g., flushing SWCBs + sfence in parallel).
pub fn run_fini<S: Send, I, T, F>(threads: usize, ntasks: usize, init: I, task: T, fini: F) -> Vec<S>
where
    I: Fn(usize) -> S + Sync,
    T: Fn(&mut S, usize) + Sync,
    F: Fn(&mut S) + Sync,
{
    let threads = threads.max(1);
    if threads == 1 {
        let mut s = init(0);
        for i in 0..ntasks {
            task(&mut s, i);
        }
        fini(&mut s);
        return vec![s];
    }

    // Acquire persistent global pool; fall back to thread::scope only if another test thread holds it.
    if POOL_BUSY
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        return run_fini_scoped(threads, ntasks, init, task, fini);
    }

    let pool = get_pool();
    let total_bg = ensure_workers(pool, threads - 1);

    let mut out: Vec<MaybeUninit<S>> = (0..threads).map(|_| MaybeUninit::uninit()).collect();
    let next = AtomicUsize::new(0);

    struct Ctx<'a, S, I, T, F> {
        next: &'a AtomicUsize,
        ntasks: usize,
        init: &'a I,
        task: &'a T,
        fini: &'a F,
        out_ptr: *mut MaybeUninit<S>,
    }
    fn tramp<S, I, T, F>(raw: *const u8, tid: usize)
    where
        I: Fn(usize) -> S + Sync,
        T: Fn(&mut S, usize) + Sync,
        F: Fn(&mut S) + Sync,
    {
        let ctx = unsafe { &*(raw as *const Ctx<'_, S, I, T, F>) };
        let mut s = (ctx.init)(tid);
        loop {
            let i = ctx.next.fetch_add(1, Ordering::Relaxed);
            if i >= ctx.ntasks {
                break;
            }
            (ctx.task)(&mut s, i);
        }
        (ctx.fini)(&mut s);
        unsafe { (*ctx.out_ptr.add(tid)).write(s) };
    }

    let ctx = Ctx {
        next: &next,
        ntasks,
        init: &init,
        task: &task,
        fini: &fini,
        out_ptr: out.as_mut_ptr(),
    };
    let job = Job {
        func: tramp::<S, I, T, F>,
        data: (&ctx as *const Ctx<'_, S, I, T, F>) as *const u8,
        active_workers: threads,
        done: AtomicUsize::new(0),
    };

    let shared = pool.shared;
    shared.job.store(&job as *const Job as *mut Job, Ordering::Release);
    shared.epoch.fetch_add(1, Ordering::SeqCst);
    if shared.parked.load(Ordering::SeqCst) > 0 {
        let _g = shared.mu.lock().unwrap();
        shared.cv.notify_all();
    }

    // Calling thread runs worker 0 directly.
    tramp::<S, I, T, F>(job.data, 0);

    // Wait for ALL spawned background workers before releasing stack `job`/`ctx`.
    let mut w_spins = 0u32;
    while job.done.load(Ordering::Acquire) < total_bg {
        if w_spins < 256 {
            std::hint::spin_loop();
            w_spins += 1;
        } else {
            std::thread::yield_now();
        }
    }
    shared.job.store(std::ptr::null_mut(), Ordering::Release);
    POOL_BUSY.store(false, Ordering::Release);

    out.into_iter().map(|m| unsafe { m.assume_init() }).collect()
}

fn run_fini_scoped<S: Send, I, T, F>(threads: usize, ntasks: usize, init: I, task: T, fini: F) -> Vec<S>
where
    I: Fn(usize) -> S + Sync,
    T: Fn(&mut S, usize) + Sync,
    F: Fn(&mut S) + Sync,
{
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let (next, init, task, fini) = (&next, &init, &task, &fini);
                sc.spawn(move || {
                    let mut s = init(t);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        task(&mut s, i);
                    }
                    fini(&mut s);
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// Drop one item per task so independent arenas are unmapped concurrently.
pub fn drop_par<T: Send>(threads: usize, items: Vec<T>) {
    let items: Vec<_> = items.into_iter().map(|item| Mutex::new(Some(item))).collect();
    run(threads, items.len(), |_| (), |_, i| {
        let item = items[i].lock().unwrap().take();
        drop(item);
    });
}

/// Raw pointer that may be shared across threads for disjoint writes.
pub struct SyncPtr<T>(pub *mut T);
impl<T> Clone for SyncPtr<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for SyncPtr<T> {}
unsafe impl<T> Send for SyncPtr<T> {}
unsafe impl<T> Sync for SyncPtr<T> {}
impl<T> SyncPtr<T> {
    /// # Safety: caller guarantees disjoint, in-bounds writes.
    #[inline(always)]
    pub unsafe fn write(self, i: usize, v: T) {
        self.0.add(i).write(v)
    }
    #[inline(always)]
    pub(super) unsafe fn copy_from(self, start: usize, src: &[T]) {
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.0.add(start), src.len()) };
    }

    #[inline(always)]
    pub unsafe fn slice<'a>(self, start: usize, len: usize) -> &'a mut [T] {
        std::slice::from_raw_parts_mut(self.0.add(start), len)
    }
}

#[repr(C)]
struct Timespec {
    sec: i64,
    nsec: i64,
}
extern "C" {
    fn clock_gettime(clk: i32, ts: *mut Timespec) -> i32;
}
/// CPU time of the calling thread, seconds.
pub fn thread_cpu() -> f64 {
    let mut t = Timespec { sec: 0, nsec: 0 };
    unsafe { clock_gettime(3, &mut t) }; // CLOCK_THREAD_CPUTIME_ID
    t.sec as f64 + t.nsec as f64 * 1e-9
}

#[repr(C)]
struct Timeval {
    sec: std::ffi::c_long,
    usec: std::ffi::c_long,
}

impl Timeval {
    fn seconds(&self) -> f64 {
        self.sec as f64 + self.usec as f64 * 1e-6
    }
}

#[repr(C)]
struct Rusage {
    utime: Timeval,
    stime: Timeval,
    maxrss: std::ffi::c_long,
    ixrss: std::ffi::c_long,
    idrss: std::ffi::c_long,
    isrss: std::ffi::c_long,
    minflt: std::ffi::c_long,
    rest: [std::ffi::c_long; 9], // Linux struct rusage fields after ru_minflt
}

extern "C" {
    fn getrusage(who: i32, usage: *mut Rusage) -> i32;
}

/// Whole-process user seconds, system seconds and minor faults, including workers.
pub fn rusage_self() -> (f64, f64, i64) {
    let mut usage = MaybeUninit::<Rusage>::uninit();
    // RUSAGE_SELF includes all threads on Linux.
    assert_eq!(unsafe { getrusage(0, usage.as_mut_ptr()) }, 0);
    let usage = unsafe { usage.assume_init() };
    (usage.utime.seconds(), usage.stime.seconds(), usage.minflt)
}

pub struct Timer {
    prefix: &'static str,
    wall: Instant,
    usage: (f64, f64, i64),
}

impl Timer {
    pub fn new(prefix: &'static str) -> Self {
        Self { prefix, wall: Instant::now(), usage: rusage_self() }
    }

    pub fn lap(&mut self, name: &str) {
        let now = Instant::now();
        let (user, sys, minflt) = rusage_self();
        let wall = (now - self.wall).as_secs_f64();
        let du = user - self.usage.0;
        let ds = sys - self.usage.1;
        let df = minflt - self.usage.2;
        eprintln!("[{}] {name:<12} wall={wall:.6}s user={du:.6}s sys={ds:.6}s minflt={df}", self.prefix);
        self.wall = now;
        self.usage = (user, sys, minflt);
    }
}

#[repr(C)]
struct Iovec {
    base: *mut u8,
    len: usize,
}

extern "C" {
    fn syscall(number: std::ffi::c_long, ...) -> std::ffi::c_long;
}

/// Sample the first 256 KiB without waiting for disk I/O. Some filesystems
/// don't implement RWF_NOWAIT, so an inconclusive probe leaves prefetch enabled.
pub fn is_file_cached(fd: i32, len: u64) -> Option<bool> {
    const SYS_PREADV2: std::ffi::c_long = 327; // Linux x86-64
    const RWF_NOWAIT: i32 = 0x8;
    const SAMPLE: usize = 256 << 10;
    let mut buf = MaybeUninit::<[u8; SAMPLE]>::uninit();
    let iov = Iovec { base: buf.as_mut_ptr().cast(), len: len.min(SAMPLE as u64) as usize };
    // preadv2 takes the low and high offset words as separate syscall arguments.
    let ret = unsafe { syscall(SYS_PREADV2, fd, &iov as *const Iovec, 1usize, 0usize, 0usize, RWF_NOWAIT) };
    if ret > 0 {
        Some(true)
    } else if ret == -1 && std::io::Error::last_os_error().raw_os_error() == Some(11) {
        Some(false)
    } else {
        None
    }
}

fn thp_mode(setting: &str) -> String {
    std::fs::read_to_string(setting)
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .find_map(|mode| Some(mode.strip_prefix('[')?.strip_suffix(']')?.to_owned()))
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

fn thp_enabled() -> &'static str {
    static ENABLED: OnceLock<String> = OnceLock::new();
    ENABLED.get_or_init(|| thp_mode("/sys/kernel/mm/transparent_hugepage/enabled"))
}

fn sink_thp() -> bool {
    static USE_THP: OnceLock<bool> = OnceLock::new();
    *USE_THP.get_or_init(|| {
        std::env::var("INGEST_SINK_THP").as_deref() != Ok("0") && thp_enabled() != "never"
    })
}

pub fn diagnose(threads: usize, cache: Option<bool>, prefetch: usize) {
    static REPORTED: Once = Once::new();
    REPORTED.call_once(|| {
        let cache = match cache {
            Some(true) => "warm",
            Some(false) => "cold",
            None => "unknown",
        };
        let scanner = scan::scanner().name();
        eprintln!(
            "[diag] threads={threads} cache={cache} thp={}/{} scanner={scanner} prefetch={prefetch} sink_thp={}",
            thp_enabled(),
            thp_mode("/sys/kernel/mm/transparent_hugepage/defrag"),
            u8::from(sink_thp()),
        );
    });
}

pub struct Prefetch {
    stop: Option<Arc<AtomicBool>>,
    workers: Vec<JoinHandle<()>>,
}

impl Prefetch {
    pub fn start(files: &Arc<Vec<FileInfo>>, threads: usize) -> Self {
        let cache = files.iter().filter(|f| f.kind == Kind::Events && f.len != 0)
            .max_by_key(|f| f.len)
            .and_then(|f| is_file_cached(f.handle.as_raw_fd(), f.len));
        let limit = if std::env::var_os("INGEST_NO_PREFETCH").is_some() {
            0
        } else if let Ok(n) = std::env::var("INGEST_PREFETCH").unwrap_or_default().parse::<usize>() {
            n.min(8)
        } else if cache == Some(true) {
            0
        } else {
            4
        };

        let mut segments = Vec::new();
        if limit > 0 {
            const SEGMENT: u64 = 16 << 20;
            for kind in [Kind::Links, Kind::Events, Kind::Clicks] {
                for (i, file) in files.iter().enumerate().filter(|(_, f)| f.kind == kind) {
                    for offset in (0..file.len).step_by(SEGMENT as usize) {
                        segments.push((i, offset, SEGMENT.min(file.len - offset)));
                    }
                }
            }
        }
        let count = limit.min(segments.len());
        diagnose(threads, cache, count);
        if count == 0 {
            return Self { stop: None, workers: Vec::new() };
        }
        let segments = Arc::new(segments);
        let stop = Arc::new(AtomicBool::new(false));
        let next = Arc::new(AtomicUsize::new(0));
        let workers = (0..count)
            .map(|_| {
                let (files, segments, stop, next) = (files.clone(), segments.clone(), stop.clone(), next.clone());
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(file, offset, len)) = segments.get(i) else { break };
                        fadvise_range(files[file].handle.as_raw_fd(), offset, len);
                    }
                })
            })
            .collect();
        Self { stop: Some(stop), workers }
    }

    pub fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.store(true, Ordering::Relaxed);
            for worker in self.workers.drain(..) {
                let _ = worker.join();
            }
        }
    }
}

impl Drop for Prefetch {
    fn drop(&mut self) {
        self.stop();
    }
}

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn posix_fadvise(fd: i32, off: i64, len: i64, advice: i32) -> i32;
}

/// Start asynchronous readahead of a specific file range.
#[inline]
pub fn fadvise_range(fd: i32, off: u64, len: u64) {
    if len > 0 {
        unsafe { posix_fadvise(fd, off as i64, len as i64, 3) };
    }
}

/// Anonymous memory region aligned to 2 MiB with optional MADV_HUGEPAGE.
/// Returns (aligned pointer, raw mapping pointer, raw length) for `huge_free`.
pub fn huge_alloc(bytes: usize) -> (*mut u8, *mut u8, usize) {
    huge_alloc_ex(bytes, sink_thp())
}

const HUGE_PAGE_BYTES: usize = 2 << 20;

fn map_aligned(bytes: usize) -> Option<(*mut u8, *mut u8, usize)> {
    let hp = HUGE_PAGE_BYTES;
    let len = bytes.max(1).next_multiple_of(hp);
    let reserved = len.checked_add(hp)?;
    unsafe {
        let raw = mmap(std::ptr::null_mut(), reserved, 3, 0x22, -1, 0);
        if raw as isize == -1 || raw.is_null() {
            return None;
        }
        let prefix = raw.align_offset(hp);
        let aligned = raw.add(prefix);
        if prefix != 0 {
            assert_eq!(munmap(raw, prefix), 0);
        }
        assert_eq!(munmap(aligned.add(len), hp - prefix), 0);
        Some((aligned, aligned, len))
    }
}

fn huge_alloc_ex(bytes: usize, use_thp: bool) -> (*mut u8, *mut u8, usize) {
    let (a, p, len) = map_aligned(bytes).expect("mmap failed");
    unsafe {
        if use_thp && bytes >= HUGE_PAGE_BYTES {
            madvise(a, len, 14); // MADV_HUGEPAGE
        }
        if std::env::var("INGEST_POPULATE").is_ok() {
            madvise(a, len, 23); // MADV_POPULATE_WRITE (5.14+); failure is harmless
        }
    }
    (a, p, len)
}

pub fn huge_free(raw: *mut u8, len: usize) {
    unsafe { munmap(raw, len) };
}

/// Two cache lines amortize each flush at the compact path's partition counts.
const SWCB_BYTES: usize = 128;
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SwcbLine([u8; SWCB_BYTES]);

/// Fixed-capacity per-partition append regions inside one huge-page arena,
/// fronted by L2-resident software write-combining buffers (SWCBs) that emit
/// full cache lines of non-temporal stores.
pub struct Sinks<T: Copy> {
    base: *mut T,
    arena: SinkArena,
    cap: usize,
    swcb: Vec<SwcbLine>,
    pub len: Vec<usize>,
    pub over: Vec<Vec<T>>,
}
unsafe impl<T: Copy + Send> Send for Sinks<T> {}
unsafe impl<T: Copy + Sync> Sync for Sinks<T> {}

struct SinkMapping {
    raw: *mut u8,
    len: usize,
}

// Mapping ownership is shared. Only the disjoint ranges in SinkArena
// provide access to the bytes.
unsafe impl Send for SinkMapping {}
unsafe impl Sync for SinkMapping {}

impl Drop for SinkMapping {
    fn drop(&mut self) {
        huge_free(self.raw, self.len);
    }
}

pub(crate) struct SinkArena {
    base: *mut u8,
    bytes: usize,
    mapping: Arc<SinkMapping>,
}

unsafe impl Send for SinkArena {}

impl SinkArena {
    pub(super) fn new(bytes: usize) -> Self {
        let (base, raw, len) = huge_alloc_ex(bytes, sink_thp());
        Self { base, bytes, mapping: Arc::new(SinkMapping { raw, len }) }
    }

    pub(super) fn split_at(self, offset: usize) -> (Self, Self) {
        assert!(offset <= self.bytes && offset % 64 == 0);
        let rest = Self {
            base: unsafe { self.base.add(offset) },
            bytes: self.bytes - offset,
            mapping: Arc::clone(&self.mapping),
        };
        (Self { bytes: offset, ..self }, rest)
    }
}

/// The unallocated tail of an exclusively borrowed arena. Typed allocations
/// are disjoint and can outlive the cursor, up to the arena borrow.
#[derive(Default)]
pub(super) struct Scratch<'a>(&'a mut [MaybeUninit<u8>]);

impl SinkArena {
    pub(super) fn scratch(&mut self) -> Scratch<'_> {
        Scratch(unsafe { std::slice::from_raw_parts_mut(self.base.cast(), self.bytes) })
    }
}

impl<'a> Scratch<'a> {
    pub(super) fn buffer<T>(&mut self, capacity: usize) -> ArenaVec<'a, T> {
        let align = std::mem::align_of::<T>().max(64);
        let pad = self.0.as_ptr().align_offset(align);
        let bytes = capacity.checked_mul(std::mem::size_of::<T>()).unwrap();
        if bytes == 0 || pad > self.0.len() || bytes > self.0.len() - pad {
            return ArenaVec::Heap(Vec::with_capacity(capacity));
        }
        let (area, rest) = std::mem::take(&mut self.0).split_at_mut(pad + bytes);
        self.0 = rest;
        let ptr = area[pad..].as_mut_ptr().cast();
        let slots = unsafe { std::slice::from_raw_parts_mut(ptr, capacity) };
        ArenaVec::Borrowed(ArenaBuffer { slots, len: 0 })
    }
}

pub(super) struct ArenaBuffer<'a, T> {
    slots: &'a mut [MaybeUninit<T>],
    len: usize,
}

impl<T> Drop for ArenaBuffer<'_, T> {
    fn drop(&mut self) {
        let initialized = std::ptr::slice_from_raw_parts_mut(self.slots.as_mut_ptr().cast::<T>(), self.len);
        unsafe { std::ptr::drop_in_place(initialized) };
    }
}

/// A vector using scratch space until it needs more capacity. Growth uses
/// the heap so skewed partitions do not need fixed arena capacity limits.
pub(super) enum ArenaVec<'a, T> {
    Borrowed(ArenaBuffer<'a, T>),
    Heap(Vec<T>),
}

impl<T> Default for ArenaVec<'_, T> {
    fn default() -> Self {
        Self::Heap(Vec::new())
    }
}

impl<T> ArenaVec<'_, T> {
    #[inline]
    pub(super) fn as_mut_ptr(&mut self) -> *mut T {
        match self {
            Self::Borrowed(a) => a.slots.as_mut_ptr().cast(),
            Self::Heap(v) => v.as_mut_ptr(),
        }
    }

    /// All elements in the new initialized range must be valid `T`s.
    #[inline]
    pub(super) unsafe fn set_len(&mut self, len: usize) {
        match self {
            Self::Borrowed(a) => {
                debug_assert!(len <= a.slots.len());
                a.len = len;
            }
            Self::Heap(v) => unsafe { v.set_len(len) },
        }
    }

    pub(super) fn reserve(&mut self, additional: usize) {
        match self {
            Self::Borrowed(a) if additional > a.slots.len() - a.len => {
                let mut heap = Vec::with_capacity((a.len + additional).max(a.slots.len() * 2));
                // Transfer initialized values to the heap and retire the
                // borrowed slots without dropping their previous contents.
                unsafe {
                    std::ptr::copy_nonoverlapping(a.slots.as_ptr().cast(), heap.as_mut_ptr(), a.len);
                    heap.set_len(a.len);
                }
                a.len = 0;
                *self = Self::Heap(heap);
            }
            Self::Heap(v) => v.reserve(additional),
            Self::Borrowed(_) => {}
        }
    }

    pub(super) fn clear(&mut self) {
        match self {
            Self::Borrowed(a) => {
                let initialized = std::ptr::slice_from_raw_parts_mut(a.slots.as_mut_ptr().cast::<T>(), a.len);
                a.len = 0;
                unsafe { std::ptr::drop_in_place(initialized) };
            }
            Self::Heap(v) => v.clear(),
        }
    }

    #[inline]
    pub(super) fn push(&mut self, value: T) {
        self.reserve(1);
        let len = self.len();
        unsafe {
            self.as_mut_ptr().add(len).write(value);
            self.set_len(len + 1);
        }
    }

    pub(super) fn resize(&mut self, len: usize, value: T) where T: Copy {
        let old = self.len();
        self.reserve(len.saturating_sub(old));
        for i in old..len {
            unsafe { self.as_mut_ptr().add(i).write(value) };
        }
        unsafe { self.set_len(len) };
    }
}

impl<T> std::ops::Deref for ArenaVec<'_, T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        match self {
            Self::Borrowed(a) => unsafe { std::slice::from_raw_parts(a.slots.as_ptr().cast(), a.len) },
            Self::Heap(v) => v,
        }
    }
}

impl<T> std::ops::DerefMut for ArenaVec<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        match self {
            Self::Borrowed(a) => unsafe { std::slice::from_raw_parts_mut(a.slots.as_mut_ptr().cast(), a.len) },
            Self::Heap(v) => v,
        }
    }
}

impl<T: Copy> Sinks<T> {
    #[inline(always)]
    const fn per_line() -> usize {
        let sz = std::mem::size_of::<T>();
        assert!(sz == 16 || sz == 32 || sz == 64);
        assert!(std::mem::align_of::<T>() >= 16);
        SWCB_BYTES / sz
    }

    fn aligned_cap(cap: usize) -> usize {
        cap.max(Self::per_line()).next_multiple_of(Self::per_line())
    }

    pub(super) fn in_arena(parts: usize, cap: usize, arena: SinkArena) -> Self {
        assert_eq!(arena.bytes, Self::arena_bytes(parts, cap));
        let cap = Self::aligned_cap(cap);
        Sinks {
            base: arena.base.cast(),
            arena,
            cap,
            swcb: vec![SwcbLine([0u8; SWCB_BYTES]); parts],
            len: vec![0; parts],
            over: (0..parts).map(|_| Vec::new()).collect(),
        }
    }

    pub(super) fn arena_bytes(parts: usize, cap: usize) -> usize {
        parts * Self::aligned_cap(cap) * std::mem::size_of::<T>()
    }

    pub fn new(parts: usize, cap: usize) -> Self {
        Self::in_arena(parts, cap, SinkArena::new(Self::arena_bytes(parts, cap)))
    }

    pub(super) fn into_arena(self) -> SinkArena {
        self.arena
    }

    pub(super) fn reuse_pair<U: Copy>(
        parts: usize, cap: usize, other_cap: usize, arena: Option<SinkArena>,
    ) -> (Self, Sinks<U>, Option<SinkArena>) {
        let bytes = Self::arena_bytes(parts, cap);
        let other_bytes = Sinks::<U>::arena_bytes(parts, other_cap);
        if let Some(arena) = arena {
            if bytes + other_bytes <= arena.bytes {
                let (first, rest) = arena.split_at(bytes);
                let (second, tail) = rest.split_at(other_bytes);
                return (Self::in_arena(parts, cap, first), Sinks::in_arena(parts, other_cap, second), Some(tail));
            }
            return (Self::new(parts, cap), Sinks::new(parts, other_cap), Some(arena));
        }
        (Self::new(parts, cap), Sinks::new(parts, other_cap), None)
    }

    /// Append to partition `p`'s SWCB. When full, stream it to the arena or
    /// copy it to the overflow vector.
    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        self.push_idx(p, x);
    }

    /// Append and return the logical index, including records in overflow.
    #[inline(always)]
    pub fn push_idx(&mut self, p: usize, x: T) -> usize {
        let pl = Self::per_line();
        let l = unsafe { *self.len.get_unchecked(p) };
        let slot = l & (pl - 1);
        let line_ptr = unsafe { self.swcb.as_mut_ptr().add(p) as *mut T };
        unsafe { line_ptr.add(slot).write(x) };
        let nl = l + 1;
        unsafe { *self.len.get_unchecked_mut(p) = nl };
        if (nl & (pl - 1)) == 0 {
            self.flush_full(p, nl, line_ptr);
        }
        l
    }

    #[inline(always)]
    fn flush_full(&mut self, p: usize, nl: usize, line_ptr: *const T) {
        let pl = Self::per_line();
        let base_idx = nl - pl;
        if nl <= self.cap {
            unsafe {
                use std::arch::x86_64::*;
                let dst = self.base.add(p * self.cap + base_idx) as *mut __m256i;
                let src = line_ptr as *const __m256i;
                debug_assert_eq!(dst as usize & 63, 0);
                debug_assert_eq!(src as usize & 63, 0);
                for i in 0..(SWCB_BYTES / 32) {
                    _mm256_stream_si256(dst.add(i), _mm256_load_si256(src.add(i)));
                }
            }
        } else {
            self.flush_overflow(p, line_ptr);
        }
    }

    #[cold]
    #[inline(never)]
    fn flush_overflow(&mut self, p: usize, line_ptr: *const T) {
        let slice = unsafe { std::slice::from_raw_parts(line_ptr, Self::per_line()) };
        self.over[p].extend_from_slice(slice);
    }

    /// Flush all partially-filled SWCB lines into the arena/overflow and issue sfence.
    /// Must be called once by the writing thread after all pushes finish.
    pub fn finish(&mut self) {
        let pl = Self::per_line();
        for p in 0..self.len.len() {
            let l = self.len[p];
            let rem = l & (pl - 1);
            if rem != 0 {
                let base_idx = l - rem;
                let src = unsafe { std::slice::from_raw_parts(self.swcb.as_ptr().add(p) as *const T, rem) };
                if l <= self.cap {
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), self.base.add(p * self.cap + base_idx), rem);
                    }
                } else {
                    self.over[p].extend_from_slice(src);
                }
            }
        }
        unsafe { std::arch::x86_64::_mm_sfence() };
    }

    #[inline]
    pub fn part(&self, p: usize) -> (&[T], &[T]) {
        let a = unsafe { std::slice::from_raw_parts(self.base.add(p * self.cap), self.len[p].min(self.cap)) };
        (a, &self.over[p])
    }

    pub fn count(&self, p: usize) -> usize {
        self.len[p].min(self.cap) + self.over[p].len()
    }
}

/// Slabs >= 64 KiB use direct anonymous `mmap`/`munmap` so freed partition streams
/// return pages to the OS immediately instead of lingering in glibc heap arenas.
const MMAP_SLAB_MIN: usize = 64 << 10;

struct Slab {
    ptr: *mut u8,
    len: usize,
    cap: usize,
    is_mmap: bool,
}
unsafe impl Send for Slab {}
unsafe impl Sync for Slab {}

impl Drop for Slab {
    fn drop(&mut self) {
        if self.cap == 0 {
            return;
        }
        if self.is_mmap {
            unsafe { munmap(self.ptr, self.cap) };
        } else {
            unsafe {
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.ptr, self.cap)));
            }
        }
    }
}

impl Slab {
    fn new(cap: usize) -> Self {
        if cap >= MMAP_SLAB_MIN {
            let cap = cap.next_multiple_of(4096);
            let ptr = unsafe { mmap(std::ptr::null_mut(), cap, 3, 0x22, -1, 0) };
            assert!(ptr as isize != -1 && !ptr.is_null(), "slab mmap({cap}) failed");
            Slab { ptr, len: 0, cap, is_mmap: true }
        } else {
            let raw = Box::into_raw(vec![0u8; cap].into_boxed_slice()) as *mut u8;
            Slab { ptr: raw, len: 0, cap, is_mmap: false }
        }
    }

    #[inline(always)]
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Append-only variable-length record buffer backed by pinned mmap (or small heap) slabs.
/// Memory never reallocates/copies on growth and is returned to the OS immediately on `Drop`.
pub struct SlabBuf {
    slabs: Vec<Slab>,
    slab_cap: usize,
    pub count: usize,
    pub total_bytes: usize,
}

impl SlabBuf {
    pub fn new(slab_cap: usize) -> Self {
        SlabBuf {
            slabs: Vec::new(),
            slab_cap: slab_cap.max(4096),
            count: 0,
            total_bytes: 0,
        }
    }

    /// Reserve `n` contiguous bytes for one record, returning a writable pointer.
    #[inline(always)]
    pub fn alloc_raw(&mut self, n: usize) -> *mut u8 {
        self.count += 1;
        self.total_bytes += n;
        if let Some(last) = self.slabs.last_mut() {
            if last.cap - last.len >= n {
                let p = unsafe { last.ptr.add(last.len) };
                last.len += n;
                return p;
            }
        }
        self.alloc_slow(n)
    }

    #[cold]
    #[inline(never)]
    fn alloc_slow(&mut self, n: usize) -> *mut u8 {
        let cap = self.slab_cap.max(n);
        let mut s = Slab::new(cap);
        let p = s.ptr;
        s.len = n;
        self.slabs.push(s);
        p
    }

    #[inline]
    pub fn slabs(&self) -> impl Iterator<Item = &[u8]> {
        self.slabs.iter().map(|s| s.as_slice())
    }
}
