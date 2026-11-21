//! Anonymous memory lives through output. Phase-local regions can be rewound
//! once their borrowers are gone, keeping those physical pages available.
use std::marker::PhantomData;
use std::mem::{align_of, size_of, MaybeUninit};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

const HUGE: usize = 2 << 20;
const CACHE_LINE: usize = 64;
const POPULATE_CHUNK: usize = 64 << 10;
const MADV_POPULATE_WRITE: i32 = 23;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn mallopt(param: i32, value: i32) -> i32;
}

pub fn tune_malloc() {
    const M_MMAP_THRESHOLD: i32 = -3;
    unsafe { mallopt(M_MMAP_THRESHOLD, 32 << 20) };
}

pub struct Arena {
    mapping: NonNull<u8>,
    mapped: usize,
    aligned: NonNull<u8>,
    len: usize,
}

impl Arena {
    pub fn new(sizes: &[usize]) -> std::io::Result<Self> {
        let len = sizes
            .iter()
            .map(|n| n.next_multiple_of(HUGE))
            .sum::<usize>();
        let mapped = len + HUGE;
        // Linux PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS.
        let raw = unsafe { mmap(std::ptr::null_mut(), mapped, 3, 0x22, -1, 0) };
        if raw as isize == -1 {
            return Err(std::io::Error::last_os_error());
        }
        let mapping = NonNull::new(raw.cast::<u8>()).unwrap();
        let aligned = unsafe { mapping.add(mapping.as_ptr().align_offset(HUGE)) };
        const MADV_HUGEPAGE: i32 = 14;
        const MADV_NOHUGEPAGE: i32 = 15;
        let arena = Self {
            mapping,
            mapped,
            aligned,
            len,
        };
        let advice = if std::env::var("PROTO_C_PAGE_MODE").as_deref() == Ok("4k")
            || std::env::var("INGEST_SINK_THP").as_deref() == Ok("0")
        {
            MADV_NOHUGEPAGE
        } else {
            MADV_HUGEPAGE
        };
        if unsafe { madvise(aligned.as_ptr().cast(), len, advice) } != 0 {
            eprintln!(
                "[arena] madvise({advice}): {}",
                std::io::Error::last_os_error()
            );
        }
        Ok(arena)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn regions<const N: usize>(&mut self, sizes: [usize; N]) -> [Region<'_>; N] {
        let mut offset = 0;
        std::array::from_fn(|i| {
            let len = sizes[i].next_multiple_of(HUGE);
            let base = unsafe { self.aligned.add(offset) };
            offset += len;
            assert!(offset <= self.len);
            Region {
                base,
                len,
                cursor: AtomicUsize::new(0),
                populate_from: AtomicUsize::new(usize::MAX),
                populated: AtomicUsize::new(0),
                owner: PhantomData,
            }
        })
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        unsafe { munmap(self.mapping.as_ptr().cast(), self.mapped) };
    }
}

pub struct Region<'a> {
    base: NonNull<u8>,
    len: usize,
    cursor: AtomicUsize,
    populate_from: AtomicUsize,
    populated: AtomicUsize,
    owner: PhantomData<&'a mut [u8]>,
}

// The atomic cursor assigns each allocation a disjoint range, and rewinding
// requires exclusive access after all slices borrowing this region are gone.
unsafe impl Sync for Region<'_> {}

impl Region<'_> {
    pub fn rewind(&mut self) {
        *self.cursor.get_mut() = 0;
    }

    pub fn populate_write(&self, n: usize) -> std::io::Result<()> {
        let n = n.min(self.len);
        if unsafe { madvise(self.base.as_ptr().cast(), n, MADV_POPULATE_WRITE) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn start_cursor_population(&self, warm: usize) {
        self.populate_from
            .store(warm.next_multiple_of(POPULATE_CHUNK), Ordering::Relaxed);
    }

    pub fn stop_cursor_population(&self) -> usize {
        self.populate_from.store(usize::MAX, Ordering::Relaxed);
        self.populated.load(Ordering::Relaxed)
    }

    #[cold]
    fn populate_claim(&self, start: usize, end: usize, from: usize) {
        // The worker whose allocation crosses the boundary advises that
        // 64 KiB span. Warm prefixes are skipped. Each region can have at
        // most one advised but unused tail, bounded by 64 KiB.
        let first = start.next_multiple_of(POPULATE_CHUNK).max(from);
        let last = end.next_multiple_of(POPULATE_CHUNK).min(self.len);
        if first >= last {
            return;
        }
        let len = last - first;
        let ptr = unsafe { self.base.add(first).as_ptr() };
        if unsafe { madvise(ptr.cast(), len, MADV_POPULATE_WRITE) } == 0 {
            self.populated.fetch_add(len, Ordering::Relaxed);
        } else {
            // EINVAL on older kernels and low-memory refusals both fall
            // through to demand paging without stopping the prototype.
            self.populate_from.store(usize::MAX, Ordering::Relaxed);
        }
    }

    pub fn used(&self) -> usize {
        self.cursor.load(Ordering::Relaxed)
    }

    fn alloc<T>(&self, count: usize) -> Option<NonNull<T>> {
        assert!(align_of::<T>() <= CACHE_LINE && size_of::<T>() > 0);
        let bytes = count
            .checked_mul(size_of::<T>())?
            .checked_next_multiple_of(CACHE_LINE)?;
        let start = self.cursor.fetch_add(bytes, Ordering::Relaxed);
        if start > self.len || bytes > self.len - start {
            return None;
        }
        let end = start + bytes;
        let populate = self.populate_from.load(Ordering::Relaxed);
        if end > populate {
            self.populate_claim(start, end, populate);
        }
        Some(unsafe { self.base.add(start).cast() })
    }

    pub fn uninit<T>(&self, count: usize) -> Option<&mut [std::mem::MaybeUninit<T>]> {
        let ptr = self.alloc::<std::mem::MaybeUninit<T>>(count)?;
        Some(unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), count) })
    }

    pub fn slice<T: Copy>(&self, count: usize, value: T) -> Option<&mut [T]> {
        self.init(count, |_| value)
    }

    pub fn init<T>(&self, count: usize, mut value: impl FnMut(usize) -> T) -> Option<&mut [T]> {
        // This region never runs destructors. Its callers use numeric records
        // and atomics, or Copy wrappers around them.
        assert!(!std::mem::needs_drop::<T>());
        let ptr = self.alloc::<T>(count)?.as_ptr();
        for i in 0..count {
            unsafe { ptr.add(i).write(value(i)) };
        }
        Some(unsafe { std::slice::from_raw_parts_mut(ptr, count) })
    }
}

pub const BLOCK_BITS: u32 = 8;
pub const BLOCK: usize = 1 << BLOCK_BITS;

pub struct Bucket<'a, T: Copy, const SIZE: usize = BLOCK> {
    blocks: Vec<NonNull<T>>,
    len: usize,
    initialized: usize,
    owner: PhantomData<&'a mut [T]>,
}

// Each bucket has unique ownership of its blocks and can only append through
// &mut self. Reading a bucket exposes only its initialized prefix.
unsafe impl<T: Copy + Send, const SIZE: usize> Send for Bucket<'_, T, SIZE> {}
unsafe impl<T: Copy + Sync, const SIZE: usize> Sync for Bucket<'_, T, SIZE> {}

impl<T: Copy, const SIZE: usize> Default for Bucket<'_, T, SIZE> {
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            len: 0,
            initialized: 0,
            owner: PhantomData,
        }
    }
}

impl<'a, T: Copy, const SIZE: usize> Bucket<'a, T, SIZE> {
    #[inline]
    pub fn push(&mut self, region: &'a Region<'_>, value: T) -> Option<usize> {
        debug_assert_eq!(self.initialized, self.len);
        let (index, slot) = self.next_slot(region)?;
        unsafe { slot.write(value) };
        self.initialized = self.len;
        Some(index)
    }

    #[inline(always)]
    fn next_slot(&mut self, region: &'a Region<'_>) -> Option<(usize, *mut T)> {
        debug_assert!(SIZE.is_power_of_two());
        let index = self.len;
        let offset = index & (SIZE - 1);
        if offset == 0 && !self.add_block(region) {
            return None;
        }
        let block = unsafe { *self.blocks.last().unwrap_unchecked() };
        self.len += 1;
        Some((index, unsafe { block.as_ptr().add(offset) }))
    }

    #[cold]
    fn add_block(&mut self, region: &'a Region<'_>) -> bool {
        let Some(block) = region.alloc(SIZE) else {
            return false;
        };
        self.blocks.push(block);
        true
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn get(&self, i: usize) -> Option<&T> {
        if i >= self.initialized {
            return None;
        }
        let block = unsafe { self.blocks.get_unchecked(i / SIZE) };
        Some(unsafe { &*block.as_ptr().add(i & (SIZE - 1)) })
    }

    pub fn blocks(&self) -> impl Iterator<Item = &[T]> {
        assert_eq!(self.initialized, self.len);
        self.blocks.iter().enumerate().map(|(i, b)| {
            let count = (self.len - i * SIZE).min(SIZE);
            unsafe { std::slice::from_raw_parts(b.as_ptr(), count) }
        })
    }

    pub fn rows(&self) -> impl Iterator<Item = &T> {
        self.blocks().flatten()
    }
}

#[repr(align(64))]
pub struct WriteLine([MaybeUninit<u8>; 320]);

impl Default for WriteLine {
    fn default() -> Self {
        Self([MaybeUninit::uninit(); 320])
    }
}

/// # Safety
/// T's representation must have no padding. FLUSH_ROWS must divide a
/// bucket's block size and cover a multiple of 64 bytes, at most 320 bytes.
pub unsafe trait StreamRecord: Copy {
    const FLUSH_ROWS: usize;
}

impl<'a, T: StreamRecord, const SIZE: usize> Bucket<'a, T, SIZE> {
    /// # Safety
    /// Each active bucket must have a line of its own from the first push
    /// through its final flush. The caller fences before handing it to readers.
    #[inline(always)]
    pub unsafe fn push_staged(
        &mut self,
        region: &'a Region<'_>,
        value: T,
        line: &mut WriteLine,
    ) -> Option<usize> {
        if !cfg!(feature = "swcb") {
            return self.push(region, value);
        }
        debug_assert_eq!(SIZE % T::FLUSH_ROWS, 0);
        let index = self.len;
        let in_block = index & (SIZE - 1);
        if in_block == 0 && !self.add_block(region) {
            return None;
        }
        self.len += 1;
        let entry = index & (T::FLUSH_ROWS - 1);
        let data = line.0.as_mut_ptr().cast::<T>();
        unsafe { data.add(entry).write(value) };
        if entry + 1 == T::FLUSH_ROWS {
            use std::arch::x86_64::*;
            let block = unsafe { self.blocks.last().unwrap_unchecked() };
            let to = unsafe { block.as_ptr().add(in_block - entry) }.cast::<__m256i>();
            let from = data.cast::<__m256i>();
            for lane in 0..T::FLUSH_ROWS * size_of::<T>() / 32 {
                unsafe { _mm256_stream_si256(to.add(lane), _mm256_load_si256(from.add(lane))) };
            }
            self.initialized = self.len;
        }
        Some(index)
    }

    /// # Safety
    /// line must be the same staging line used for every pending row, which
    /// has been filled with initialized T values.
    pub unsafe fn finish_staged(&mut self, line: &WriteLine) {
        let remaining = self.len - self.initialized;
        if remaining == 0 {
            return;
        }
        let first = self.initialized & (SIZE - 1);
        let dst = unsafe { self.blocks.last().unwrap_unchecked().as_ptr().add(first) };
        unsafe { std::ptr::copy_nonoverlapping(line.0.as_ptr().cast::<T>(), dst, remaining) };
        self.initialized = self.len;
    }
}

/// Fixed-capacity scratch initialized on demand within an arena region.
pub struct Scratch<'a, T: Copy> {
    base: NonNull<T>,
    cap: usize,
    len: usize,
    owner: PhantomData<&'a mut [T]>,
}

unsafe impl<T: Copy + Send> Send for Scratch<'_, T> {}

impl<'a, T: Copy> Scratch<'a, T> {
    pub fn new(region: &'a Region<'_>, cap: usize) -> Option<Self> {
        Some(Self {
            base: region.alloc(cap)?,
            cap,
            len: 0,
            owner: PhantomData,
        })
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn reserve(&self, n: usize) {
        assert!(n <= self.cap - self.len);
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.base.as_ptr()
    }

    /// # Safety
    /// Elements through the new length must already be initialized.
    pub unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.cap);
        self.len = len;
    }

    #[inline(always)]
    pub fn extend_from_slice(&mut self, values: &[T]) {
        self.reserve(values.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                values.as_ptr(),
                self.base.as_ptr().add(self.len),
                values.len(),
            )
        };
        self.len += values.len();
    }

    #[inline(always)]
    pub fn push(&mut self, value: T) {
        self.reserve(1);
        unsafe { self.base.as_ptr().add(self.len).write(value) };
        self.len += 1;
    }
}

impl<T: Copy> std::ops::Deref for Scratch<'_, T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.base.as_ptr(), self.len) }
    }
}

impl<T: Copy> std::ops::DerefMut for Scratch<'_, T> {
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.base.as_ptr(), self.len) }
    }
}
