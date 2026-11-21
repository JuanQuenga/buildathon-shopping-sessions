//! Minimal scoped work-stealing helpers (no external deps).
use std::sync::atomic::{AtomicUsize, Ordering};

/// Runs `ntasks` tasks over `threads` threads; each thread owns a state from
/// `init(thread_index)`, and the states are returned in thread order.
pub fn run<S: Send, I, T>(threads: usize, ntasks: usize, init: I, task: T) -> Vec<S>
where
    I: Fn(usize) -> S + Sync,
    T: Fn(&mut S, usize) + Sync,
{
    let next = AtomicUsize::new(0);
    let threads = threads.max(1);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let (next, init, task) = (&next, &init, &task);
                sc.spawn(move || {
                    let mut s = init(t);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        task(&mut s, i);
                    }
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// Raw pointer that may be shared across threads for disjoint writes.
#[derive(Clone, Copy)]
pub struct SyncPtr<T>(pub *mut T);
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
}

/// Anonymous memory region aligned to 2 MiB with MADV_HUGEPAGE (best effort).
/// Never unmapped: the process exits without destructors.
pub fn huge_alloc(bytes: usize) -> *mut u8 {
    const HP: usize = 2 << 20;
    let len = bytes.div_ceil(HP) * HP + HP;
    unsafe {
        let p = mmap(std::ptr::null_mut(), len, 3, 0x22, -1, 0);
        assert!(p as isize != -1 && !p.is_null(), "mmap failed");
        let a = ((p as usize + HP - 1) & !(HP - 1)) as *mut u8;
        madvise(a, len - HP, 14);
        a
    }
}

/// Fixed-capacity per-partition append regions inside one huge-page arena,
/// with a heap overflow vector per partition.
pub struct Sinks<T: Copy> {
    base: *mut T,
    cap: usize,
    pub len: Vec<usize>,
    pub over: Vec<Vec<T>>,
}
unsafe impl<T: Copy + Send> Send for Sinks<T> {}
unsafe impl<T: Copy + Sync> Sync for Sinks<T> {}

impl<T: Copy> Sinks<T> {
    pub fn new(parts: usize, cap: usize) -> Self {
        let cap = cap.max(4);
        let base = huge_alloc(parts * cap * std::mem::size_of::<T>()) as *mut T;
        Sinks { base, cap, len: vec![0; parts], over: (0..parts).map(|_| Vec::new()).collect() }
    }
    /// Append with non-temporal 16-byte stores (no read-for-ownership).
    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        const { assert!(std::mem::size_of::<T>() % 16 == 0 && std::mem::align_of::<T>() >= 16) };
        let l = unsafe { *self.len.get_unchecked(p) };
        if l < self.cap {
            unsafe {
                use std::arch::x86_64::*;
                let dst = self.base.add(p * self.cap + l) as *mut __m128i;
                let src = &x as *const T as *const __m128i;
                for i in 0..std::mem::size_of::<T>() / 16 {
                    _mm_stream_si128(dst.add(i), _mm_loadu_si128(src.add(i)));
                }
                *self.len.get_unchecked_mut(p) = l + 1;
            }
        } else {
            self.over[p].push(x);
        }
    }
    /// Must be called by the writing thread before others read.
    pub fn fence(&self) {
        unsafe { std::arch::x86_64::_mm_sfence() }
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
