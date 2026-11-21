//! Minimal scoped work-stealing helpers and software write-combining sinks (no external deps).
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Sum of worker-thread CPU nanoseconds spent inside `run` (for stage reports).
pub static CPU_NS: AtomicU64 = AtomicU64::new(0);

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
    let next = AtomicUsize::new(0);
    let threads = threads.max(1);
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

/// Anonymous memory region aligned to 2 MiB with MADV_HUGEPAGE (best effort).
/// Returns (aligned pointer, raw mapping pointer, raw length) for `huge_free`.
pub fn huge_alloc(bytes: usize) -> (*mut u8, *mut u8, usize) {
    const HP: usize = 2 << 20;
    let len = bytes.div_ceil(HP) * HP + HP;
    unsafe {
        let p = mmap(std::ptr::null_mut(), len, 3, 0x22, -1, 0);
        assert!(p as isize != -1 && !p.is_null(), "mmap failed");
        let a = ((p as usize + HP - 1) & !(HP - 1)) as *mut u8;
        madvise(a, len - HP, 14);
        if std::env::var("INGEST_POPULATE").is_ok() {
            madvise(a, len - HP, 23); // MADV_POPULATE_WRITE (5.14+); failure is harmless
        }
        (a, p, len)
    }
}

pub fn huge_free(raw: *mut u8, len: usize) {
    unsafe { munmap(raw, len) };
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
        let (a, p, l) = huge_alloc(parts * cap * std::mem::size_of::<T>());
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
