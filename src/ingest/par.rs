//! Minimal persistent worker-pool work-stealing helpers and software write-combining sinks (no external deps).
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};

/// Sum of worker-thread CPU nanoseconds spent inside `run` (for stage reports).
pub static CPU_NS: AtomicU64 = AtomicU64::new(0);

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
        let c0 = thread_cpu();
        let mut s = (ctx.init)(tid);
        loop {
            let i = ctx.next.fetch_add(1, Ordering::Relaxed);
            if i >= ctx.ntasks {
                break;
            }
            (ctx.task)(&mut s, i);
        }
        (ctx.fini)(&mut s);
        if tid != 0 {
            CPU_NS.fetch_add(((thread_cpu() - c0) * 1e9) as u64, Ordering::Relaxed);
        }
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

    // Calling thread runs worker 0 directly (zero wake latency; its CPU is accounted by Timer).
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
                    let c0 = thread_cpu();
                    let mut s = init(t);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        task(&mut s, i);
                    }
                    fini(&mut s);
                    CPU_NS.fetch_add(((thread_cpu() - c0) * 1e9) as u64, Ordering::Relaxed);
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
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
    huge_alloc_ex(bytes, true)
}

pub fn huge_alloc_ex(bytes: usize, use_thp: bool) -> (*mut u8, *mut u8, usize) {
    const HP: usize = 2 << 20;
    let len = bytes.div_ceil(HP) * HP + HP;
    unsafe {
        let p = mmap(std::ptr::null_mut(), len, 3, 0x22, -1, 0);
        assert!(p as isize != -1 && !p.is_null(), "mmap failed");
        let a = ((p as usize + HP - 1) & !(HP - 1)) as *mut u8;
        if use_thp {
            madvise(a, len - HP, 14);
        }
        if std::env::var("INGEST_POPULATE").is_ok() {
            madvise(a, len - HP, 23); // MADV_POPULATE_WRITE (5.14+); failure is harmless
        }
        (a, p, len)
    }
}

pub fn huge_free(raw: *mut u8, len: usize) {
    unsafe { munmap(raw, len) };
}

/// Map a read-only file with `MADV_SEQUENTIAL` for zero-copy scanning.
pub fn mmap_ro(fd: i32, len: usize) -> Option<*const u8> {
    if len == 0 {
        return None;
    }
    unsafe {
        let p = mmap(std::ptr::null_mut(), len, 1, 2, fd, 0); // PROT_READ=1, MAP_PRIVATE=2
        if p as isize == -1 || p.is_null() {
            return None;
        }
        madvise(p, len, 2); // MADV_SEQUENTIAL=2
        Some(p as *const u8)
    }
}

/// One 256-byte (4 cache lines) cache-line-aligned software write-combining buffer slot.
/// Flushing 256 bytes at a time (8x 32B RecA, 16x 16B RecB) keeps the flush branch
/// highly predictable (>87.5% / >93.75% not-taken) and cuts dTLB touches by 8x-16x.
const SWCB_BYTES: usize = 256;
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SwcbLine([u8; SWCB_BYTES]);

/// Fixed-capacity per-partition append regions inside one huge-page arena,
/// fronted by L2-resident 256-byte software write-combining buffers (SWCBs)
/// that emit 4 full cache lines of non-temporal stores (8x `_mm256_stream_si256`).
pub struct Sinks<T: Copy> {
    base: *mut T,
    raw: (*mut u8, usize),
    cap: usize,
    swcb: Vec<SwcbLine>,
    pub len: Vec<usize>,
    pub over: Vec<Vec<T>>,
}
unsafe impl<T: Copy + Send> Send for Sinks<T> {}
unsafe impl<T: Copy + Sync> Sync for Sinks<T> {}

impl<T: Copy> Drop for Sinks<T> {
    fn drop(&mut self) {
        huge_free(self.raw.0, self.raw.1);
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

    pub fn new(parts: usize, cap: usize) -> Self {
        let pl = Self::per_line();
        let cap = cap.max(pl).div_ceil(pl) * pl;
        // Skip MADV_HUGEPAGE on streaming Sinks by default: avoids synchronous THP compaction stalls
        // when kernel defrag=[madvise], while 2M-aligned mmap still gets opportunistic THP under always.
        let use_thp = std::env::var("INGEST_SINK_THP").is_ok();
        let (a, p, l) = huge_alloc_ex(parts * cap * std::mem::size_of::<T>(), use_thp);
        Sinks {
            base: a as *mut T,
            raw: (p, l),
            cap,
            swcb: vec![SwcbLine([0u8; SWCB_BYTES]); parts],
            len: vec![0; parts],
            over: (0..parts).map(|_| Vec::new()).collect(),
        }
    }

    /// Append one item into partition `p`'s 256-byte SWCB; when full, stream all
    /// 4 cache lines non-temporally to the arena (or overflow vector).
    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
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
    }

    #[cold]
    #[inline(never)]
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
            let slice = unsafe { std::slice::from_raw_parts(line_ptr, pl) };
            self.over[p].extend_from_slice(slice);
        }
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
