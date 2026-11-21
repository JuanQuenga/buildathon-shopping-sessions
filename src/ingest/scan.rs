//! LF-aligned input chunks and an L1/L2-tiled SIMD structural scanner that
//! yields well-formed rows' unquoted field ranges.
use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::OnceLock;

pub const PAD: usize = 64;
const TILE: usize = 24 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Events,
    Clicks,
    Links,
}

pub struct FileInfo {
    pub handle: File,
    pub len: u64,
    pub kind: Kind,
}

pub fn open_csv_files(data_dir: &Path) -> Vec<FileInfo> {
    let mut files = Vec::new();
    for (sub, kind) in [("events", Kind::Events), ("clicks", Kind::Clicks), ("links", Kind::Links)] {
        for path in super::small::list_csv(data_dir, sub) {
            let handle = File::open(&path).expect("open csv");
            let len = handle.metadata().map(|m| m.len()).unwrap_or(0);
            files.push(FileInfo { handle, len, kind });
        }
    }
    files
}

#[derive(Clone, Copy, Debug)]
pub struct Chunk {
    pub file: u32,
    pub start: u64,
    pub end: u64,
}

/// Split files into chunks of about `size` bytes (by nominal offsets; rows are
/// owned by the chunk containing their first byte).
pub fn plan(files: &[FileInfo], size: u64) -> Vec<Chunk> {
    let mut v = Vec::new();
    for (i, f) in files.iter().enumerate() {
        let mut s = 0;
        while s < f.len {
            let e = (s + size).min(f.len);
            v.push(Chunk { file: i as u32, start: s, end: e });
            s = e;
        }
    }
    v
}

/// Reads chunk bytes into `buf` such that `buf[lo..hi]` holds exactly the rows
/// owned by the chunk, each terminated by LF (a final unterminated row at EOF
/// gets a synthetic LF), followed by >= PAD zero bytes. Skips the header for the
/// chunk at offset 0. Returns (lo, hi).
pub fn read_chunk(file: &File, flen: u64, c: Chunk, buf: &mut Vec<u8>) -> (usize, usize) {
    const EXTRA: u64 = 4096;
    let rs = if c.start == 0 { 0 } else { c.start - 1 };
    let mut want = (c.end + EXTRA).min(flen) - rs;
    loop {
        let w = want as usize;
        let need = w + PAD + 1;
        if buf.capacity() < need {
            buf.reserve(need - buf.len());
        }
        // SAFETY: u8 has no drop glue; we overwrite [0..w] via read_exact_at and zero [w..need].
        unsafe { buf.set_len(need) };
        read_exact_at(file, &mut buf[..w], rs);
        buf[w..need].fill(0);
        let data = &buf[..w];
        let lo = if c.start == 0 {
            match memchr(b'\n', data) {
                Some(p) => p + 1,
                None => return (0, 0),
            }
        } else {
            match memchr(b'\n', data) {
                Some(p) => p + 1,
                None => {
                    if rs + want >= flen {
                        return (0, 0);
                    }
                    want = (want * 2).min(flen - rs);
                    continue;
                }
            }
        };
        let own_end = (c.end - rs) as usize;
        if lo >= own_end {
            return (0, 0);
        }
        match memchr(b'\n', &data[own_end - 1..]) {
            Some(p) => return (lo, own_end - 1 + p + 1),
            None => {
                if rs + want >= flen {
                    if w > lo {
                        buf[w] = b'\n';
                        return (lo, w + 1);
                    }
                    return (lo, w);
                }
                want = (want * 2).min(flen - rs);
            }
        }
    }
}

fn read_exact_at(f: &File, mut buf: &mut [u8], mut off: u64) {
    while !buf.is_empty() {
        match f.read_at(buf, off) {
            Ok(0) => panic!("unexpected EOF"),
            Ok(n) => {
                buf = &mut buf[n..];
                off += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read error: {e}"),
        }
    }
}

#[inline]
pub fn memchr(c: u8, s: &[u8]) -> Option<usize> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let n = s.len();
        let ptr = s.as_ptr();
        let needle = _mm256_set1_epi8(c as i8);
        let mut i = 0;
        while i + 32 <= n {
            let v = _mm256_loadu_si256(ptr.add(i) as *const __m256i);
            let m = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, needle)) as u32;
            if m != 0 {
                return Some(i + m.trailing_zeros() as usize);
            }
            i += 32;
        }
        while i < n {
            if *ptr.add(i) == c {
                return Some(i);
            }
            i += 1;
        }
        return None;
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    s.iter().position(|&b| b == c)
}

#[inline(always)]
fn masks(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        if cfg!(target_feature = "avx2") {
            let a = _mm256_loadu_si256(p as *const __m256i);
            let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
            let comma = _mm256_set1_epi8(b',' as i8);
            let nl = _mm256_set1_epi8(b'\n' as i8);
            let qt = _mm256_set1_epi8(b'"' as i8);
            let c_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, comma)) as u32 as u64;
            let c_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, comma)) as u32 as u64;
            let n_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, nl)) as u32 as u64;
            let n_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, nl)) as u32 as u64;
            let q_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, qt)) as u32 as u64;
            let q_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, qt)) as u32 as u64;
            let c = c_lo | (c_hi << 32);
            let n = n_lo | (n_hi << 32);
            let q = q_lo | (q_hi << 32);
            return (c | n, q, n);
        }
    }
    let s = unsafe { std::slice::from_raw_parts(p, 64) };
    let (mut sep, mut q, mut n) = (0u64, 0u64, 0u64);
    for (i, &b) in s.iter().enumerate() {
        sep |= ((b == b',' || b == b'\n') as u64) << i;
        n |= ((b == b'\n') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (sep, q, n)
}

#[inline(always)]
fn below(i: u32) -> u64 {
    if i >= 64 { u64::MAX } else { (1u64 << i) - 1 }
}

/// Bits [0, i) when i < 64 (compiles to BMI2 BZHI / single shift).
#[inline(always)]
fn below_lt64(i: u32) -> u64 {
    (1u64 << (i & 63)) - 1
}

#[cfg(test)]
#[inline(always)]
pub fn scan_ref<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(buf: &[u8], lo: usize, hi: usize, mut f: F) {
    if lo >= hi {
        return;
    }
    assert!(buf.len() >= hi + PAD && N <= 15 && hi < u32::MAX as usize);
    let mut seps = [0u32; 16];
    let mut k = 0usize;
    let mut row_start = lo as u32;
    let mut q_block = 0u32;
    let mut q_row = 0u32;
    let mut base = lo;
    let ptr = buf.as_ptr();
    while base < hi {
        let (mut sep, q, nlm) = masks(unsafe { ptr.add(base) });
        let valid = below((hi - base) as u32);
        sep &= valid;
        let q = q & valid;
        while sep != 0 {
            let i = sep.trailing_zeros();
            let pos = (base + i as usize) as u32;
            seps[k & 15] = pos;
            k += 1;
            if (nlm >> i) & 1 != 0 {
                let q_end = q_block + (q & below_lt64(i)).count_ones();
                if k == N {
                    let mut fields = [(0u32, 0u32); N];
                    let mut wrapped = 0u32;
                    let mut s = row_start;
                    for j in 0..N {
                        let e = seps[j];
                        let w = e >= s + 2 && unsafe { *ptr.add(s as usize) == b'"' && *ptr.add(e as usize - 1) == b'"' };
                        wrapped += w as u32;
                        fields[j] = (s + w as u32, e - w as u32);
                        s = e + 1;
                    }
                    if q_end - q_row == 2 * wrapped {
                        f(buf, &fields);
                    }
                }
                k = 0;
                q_row = q_end;
                row_start = pos + 1;
            }
            sep &= sep - 1;
        }
        q_block += q.count_ones();
        base += 64;
    }
}

#[derive(Default)]
pub struct Scratch {
    seps: Vec<u32>,
    lfs: Vec<(u32, u32)>,
    seps16: Vec<u16>,
    lfs16: Vec<(u16, u16)>,
}

#[derive(Clone, Copy)]
pub enum Scanner {
    Avx2,
    #[cfg(target_arch = "x86_64")]
    Avx512Bw512,
    #[cfg(target_arch = "x86_64")]
    Avx512Vl256,
}

impl Scanner {
    pub fn name(self) -> &'static str {
        match self {
            Self::Avx2 => "avx2",
            #[cfg(target_arch = "x86_64")]
            Self::Avx512Bw512 => "avx512bw512",
            #[cfg(target_arch = "x86_64")]
            Self::Avx512Vl256 => "avx512vl256",
        }
    }
}

#[inline(always)]
pub fn scanner() -> Scanner {
    static SCANNER: std::sync::OnceLock<Scanner> = std::sync::OnceLock::new();
    *SCANNER.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        if std::env::var_os("INGEST_NO_AVX512").is_none()
            && std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512bw")
        {
            match std::env::var("INGEST_SCANNER").as_deref() {
                Ok("avx512") => return Scanner::Avx512Bw512,
                Ok("avx512vl") if std::arch::is_x86_feature_detected!("avx512vl") => return Scanner::Avx512Vl256,
                _ => return Scanner::Avx2,
            }
        }
        Scanner::Avx2
    })
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::new(Scratch::default());
}

/// Scan `buf[lo..hi]` (rows terminated by LF; PAD readable bytes after hi) in
/// L1/L2-resident tiles. Field ranges are relative to the slice passed to the callback.
#[inline(always)]
pub fn scan<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(buf: &[u8], lo: usize, hi: usize, f: F) {
    SCRATCH.with(|s| scan_with::<N, F>(buf, lo, hi, &mut s.borrow_mut(), f))
}

#[inline(always)]
pub fn scan_with<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(
    buf: &[u8],
    lo: usize,
    hi: usize,
    sc: &mut Scratch,
    mut f: F,
) {
    if lo >= hi {
        return;
    }
    assert!(buf.len() >= hi + PAD && N <= 15 && hi < u32::MAX as usize);
    let scanner = scanner();
    let mut cur = lo;
    while cur < hi {
        let tile_end = if cur + TILE >= hi {
            hi
        } else {
            match memchr(b'\n', &buf[cur + TILE..hi]) {
                Some(p) => cur + TILE + p + 1,
                None => hi,
            }
        };
        if tile_end - cur <= 65_000 {
            match scanner {
                Scanner::Avx2 => scan_tile_stage1_u16(buf, cur, tile_end, sc, masks),
                #[cfg(target_arch = "x86_64")]
                Scanner::Avx512Bw512 => unsafe { scan_tile_stage1_u16_avx512(buf, cur, tile_end, sc) },
                #[cfg(target_arch = "x86_64")]
                Scanner::Avx512Vl256 => unsafe { scan_tile_stage1_u16_avx512vl(buf, cur, tile_end, sc) },
            }
            scan_tile_stage2_u16::<N, F>(buf, cur, sc, &mut f);
        } else {
            scan_tile_stage1(buf, cur, tile_end, sc);
            scan_tile_stage2::<N, F>(buf, cur, sc, &mut f);
        }
        cur = tile_end;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn scan_tile_stage1_u16_avx512(buf: &[u8], lo: usize, hi: usize, sc: &mut Scratch) {
    use std::arch::x86_64::*;
    let comma = _mm512_set1_epi8(b',' as i8);
    let nl = _mm512_set1_epi8(b'\n' as i8);
    let qt = _mm512_set1_epi8(b'"' as i8);
    scan_tile_stage1_u16(buf, lo, hi, sc, |ptr| {
        let v = _mm512_loadu_si512(ptr.cast());
        let c = _mm512_cmpeq_epi8_mask(v, comma);
        let n = _mm512_cmpeq_epi8_mask(v, nl);
        let q = _mm512_cmpeq_epi8_mask(v, qt);
        (c | n, q, n)
    });
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vl")]
unsafe fn scan_tile_stage1_u16_avx512vl(buf: &[u8], lo: usize, hi: usize, sc: &mut Scratch) {
    use std::arch::x86_64::*;
    let comma = _mm256_set1_epi8(b',' as i8);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let masks32 = |v: __m256i| {
        let (c, n, q): (u32, u32, u32);
        // Keep the byte compares in YMM registers. LLVM can otherwise widen
        // two VL intrinsics into one ZMM compare when their masks are joined.
        std::arch::asm!(
            "vpcmpeqb {c}, {v}, {comma}",
            "vpcmpeqb {n}, {v}, {nl}",
            "vpcmpeqb {q}, {v}, {qt}",
            v = in(ymm_reg) v,
            comma = in(ymm_reg) comma,
            nl = in(ymm_reg) nl,
            qt = in(ymm_reg) qt,
            c = lateout(kreg) c,
            n = lateout(kreg) n,
            q = lateout(kreg) q,
            options(pure, nomem, nostack, preserves_flags),
        );
        (c, n, q)
    };
    scan_tile_stage1_u16(buf, lo, hi, sc, |ptr| {
        let (c0, n0, q0) = masks32(_mm256_loadu_si256(ptr.cast()));
        let (c1, n1, q1) = masks32(_mm256_loadu_si256(ptr.add(32).cast()));
        let combine = |lo: u32, hi: u32| u64::from(lo) | (u64::from(hi) << 32);
        let n = combine(n0, n1);
        (combine(c0, c1) | n, combine(q0, q1), n)
    });
}

#[inline(always)]
fn scan_tile_stage1_u16(
    buf: &[u8],
    lo: usize,
    hi: usize,
    sc: &mut Scratch,
    block_masks: impl Fn(*const u8) -> (u64, u64, u64),
) {
    let ptr = unsafe { buf.as_ptr().add(lo) };
    let tile_len = hi - lo;
    let seps = &mut sc.seps16;
    let lfs = &mut sc.lfs16;
    let need_seps = tile_len + 72;
    if seps.capacity() < need_seps {
        seps.clear();
        seps.reserve(need_seps);
    }
    let need_lfs = tile_len + 64;
    if lfs.capacity() < need_lfs {
        lfs.clear();
        lfs.reserve(need_lfs);
    }
    let sep_base = seps.as_mut_ptr();
    let lf_base = lfs.as_mut_ptr();
    let mut sep_len = 0usize;
    let mut lf_len = 0usize;
    let mut qb = 0u32;
    let mut rel = 0usize;
    while rel < tile_len {
        let (sep, q, nl) = block_masks(unsafe { ptr.add(rel) });
        let rem = tile_len - rel;
        let (m, q, n) = if rem >= 64 {
            (sep, q, nl)
        } else {
            let valid = below(rem as u32);
            (sep & valid, q & valid, nl & valid)
        };
        let mut m0 = m as u32;
        let mut m1 = (m >> 32) as u32;
        let c0 = m0.count_ones() as usize;
        let c1 = m1.count_ones() as usize;
        let b0 = rel as u16;
        let b1 = b0.wrapping_add(32);
        unsafe {
            let o0 = sep_base.add(sep_len);
            let o1 = o0.add(c0);
            for k in 0..4 {
                *o0.add(k) = b0.wrapping_add(m0.trailing_zeros() as u16);
                m0 &= m0.wrapping_sub(1);
            }
            if m0 != 0 {
                let mut o = o0.add(4);
                while m0 != 0 {
                    *o = b0.wrapping_add(m0.trailing_zeros() as u16);
                    o = o.add(1);
                    m0 &= m0.wrapping_sub(1);
                }
            }
            for k in 0..4 {
                *o1.add(k) = b1.wrapping_add(m1.trailing_zeros() as u16);
                m1 &= m1.wrapping_sub(1);
            }
            if m1 != 0 {
                let mut o = o1.add(4);
                while m1 != 0 {
                    *o = b1.wrapping_add(m1.trailing_zeros() as u16);
                    o = o.add(1);
                    m1 &= m1.wrapping_sub(1);
                }
            }
        }
        sep_len += c0 + c1;

        let mut nm = n;
        while nm != 0 {
            let i = nm.trailing_zeros();
            unsafe {
                *lf_base.add(lf_len) = (b0 + i as u16, (qb + (q & below_lt64(i)).count_ones()) as u16);
            }
            lf_len += 1;
            nm &= nm - 1;
        }
        qb += q.count_ones();
        rel += 64;
    }
    unsafe {
        seps.set_len(sep_len);
        lfs.set_len(lf_len);
    }
}

#[inline(always)]
fn scan_tile_stage2_u16<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(
    buf: &[u8],
    lo: usize,
    sc: &Scratch,
    f: &mut F,
) {
    // Keep the tail padding readable for the callback. Derive the pointer from
    // the original buffer so the preceding byte can also be read when lo > 0.
    let tile_buf = &buf[lo..];
    let tile_ptr = unsafe { buf.as_ptr().add(lo) };
    let seps_sl = &sc.seps16[..];
    let lfs_sl = &sc.lfs16[..];
    let sep_len = seps_sl.len();
    let mut k = 0usize;
    let mut row_start = 0u32;
    let mut q_prev = 0u32;
    if lo > 0 {
        for &(lf16, qc16) in lfs_sl {
            let lf = lf16 as u32;
            let qc = qc16 as u32;
            if k + N <= sep_len && unsafe { *seps_sl.get_unchecked(k + N - 1) } as u32 == lf {
                let mut fields = [(0u32, 0u32); N];
                let mut wrapped = 0u32;
                let mut s = row_start;
                for j in 0..N {
                    let e = unsafe { *seps_sl.get_unchecked(k + j) } as u32;
                    let b0 = unsafe { *tile_ptr.add(s as usize) };
                    let b1 = unsafe { *tile_ptr.offset(e as isize - 1) };
                    let w = ((e >= s + 2) & (b0 == b'"') & (b1 == b'"')) as u32;
                    wrapped += w;
                    fields[j] = (s + w, e - w);
                    s = e + 1;
                }
                if qc - q_prev == 2 * wrapped {
                    f(tile_buf, &fields);
                }
                k += N;
            } else {
                while unsafe { *seps_sl.get_unchecked(k) } as u32 != lf {
                    k += 1;
                }
                k += 1;
            }
            row_start = lf + 1;
            q_prev = qc;
        }
    } else {
        for &(lf16, qc16) in lfs_sl {
            let lf = lf16 as u32;
            let qc = qc16 as u32;
            if k + N <= sep_len && unsafe { *seps_sl.get_unchecked(k + N - 1) } as u32 == lf {
                let mut fields = [(0u32, 0u32); N];
                let mut wrapped = 0u32;
                let mut s = row_start;
                for j in 0..N {
                    let e = unsafe { *seps_sl.get_unchecked(k + j) } as u32;
                    let b0 = unsafe { *tile_ptr.add(s as usize) };
                    let b1 = unsafe { *tile_ptr.add(e.saturating_sub(1) as usize) };
                    let w = ((e >= s + 2) & (b0 == b'"') & (b1 == b'"')) as u32;
                    wrapped += w;
                    fields[j] = (s + w, e - w);
                    s = e + 1;
                }
                if qc - q_prev == 2 * wrapped {
                    f(tile_buf, &fields);
                }
                k += N;
            } else {
                while unsafe { *seps_sl.get_unchecked(k) } as u32 != lf {
                    k += 1;
                }
                k += 1;
            }
            row_start = lf + 1;
            q_prev = qc;
        }
    }
}

#[inline(always)]
fn scan_tile_stage1(buf: &[u8], lo: usize, hi: usize, sc: &mut Scratch) {
    let ptr = buf.as_ptr();
    let tile_len = hi - lo;
    let seps = &mut sc.seps;
    let lfs = &mut sc.lfs;
    let need_seps = tile_len + 72;
    if seps.capacity() < need_seps {
        seps.clear();
        seps.reserve(need_seps);
    }
    let need_lfs = tile_len + 64;
    if lfs.capacity() < need_lfs {
        lfs.clear();
        lfs.reserve(need_lfs);
    }
    let sep_base = seps.as_mut_ptr();
    let lf_base = lfs.as_mut_ptr();
    let mut sep_len = 0usize;
    let mut lf_len = 0usize;
    let mut qb = 0u32;
    let mut base = lo;
    while base < hi {
        let (sep, q, nl) = masks(unsafe { ptr.add(base) });
        let rem = hi - base;
        let (m, q, n) = if rem >= 64 {
            (sep, q, nl)
        } else {
            let valid = below(rem as u32);
            (sep & valid, q & valid, nl & valid)
        };
        let mut m0 = m as u32;
        let mut m1 = (m >> 32) as u32;
        let c0 = m0.count_ones() as usize;
        let c1 = m1.count_ones() as usize;
        let b0 = base as u32;
        let b1 = b0 + 32;
        unsafe {
            let o0 = sep_base.add(sep_len);
            let o1 = o0.add(c0);
            for k in 0..4 {
                *o0.add(k) = b0.wrapping_add(m0.trailing_zeros());
                m0 &= m0.wrapping_sub(1);
            }
            if m0 != 0 {
                let mut o = o0.add(4);
                while m0 != 0 {
                    *o = b0.wrapping_add(m0.trailing_zeros());
                    o = o.add(1);
                    m0 &= m0.wrapping_sub(1);
                }
            }
            for k in 0..4 {
                *o1.add(k) = b1.wrapping_add(m1.trailing_zeros());
                m1 &= m1.wrapping_sub(1);
            }
            if m1 != 0 {
                let mut o = o1.add(4);
                while m1 != 0 {
                    *o = b1.wrapping_add(m1.trailing_zeros());
                    o = o.add(1);
                    m1 &= m1.wrapping_sub(1);
                }
            }
        }
        sep_len += c0 + c1;

        let mut nm = n;
        while nm != 0 {
            let i = nm.trailing_zeros();
            unsafe {
                *lf_base.add(lf_len) = (b0 + i, qb + (q & below_lt64(i)).count_ones());
            }
            lf_len += 1;
            nm &= nm - 1;
        }
        qb += q.count_ones();
        base += 64;
    }
    unsafe {
        seps.set_len(sep_len);
        lfs.set_len(lf_len);
    }
}

#[inline(always)]
fn scan_tile_stage2<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(
    buf: &[u8],
    lo: usize,
    sc: &Scratch,
    f: &mut F,
) {
    let ptr = buf.as_ptr();
    let seps_sl = &sc.seps[..];
    let lfs_sl = &sc.lfs[..];
    let sep_len = seps_sl.len();
    let mut k = 0usize;
    let mut row_start = lo as u32;
    let mut q_prev = 0u32;
    for &(lf, qc) in lfs_sl {
        if k + N <= sep_len && unsafe { *seps_sl.get_unchecked(k + N - 1) } == lf {
            // 100% branchless quote unwrapping: avoids ~1.4 branch mispredicts/row on 20%-quoted fields!
            let mut fields = [(0u32, 0u32); N];
            let mut wrapped = 0u32;
            let mut s = row_start;
            if lo > 0 {
                // Every separator e >= lo >= 1, so e - 1 is valid without saturating_sub cmov!
                for j in 0..N {
                    let e = unsafe { *seps_sl.get_unchecked(k + j) };
                    let b0 = unsafe { *ptr.add(s as usize) };
                    let b1 = unsafe { *ptr.add((e - 1) as usize) };
                    let w = ((e >= s + 2) & (b0 == b'"') & (b1 == b'"')) as u32;
                    wrapped += w;
                    fields[j] = (s + w, e - w);
                    s = e + 1;
                }
            } else {
                for j in 0..N {
                    let e = unsafe { *seps_sl.get_unchecked(k + j) };
                    let b0 = unsafe { *ptr.add(s as usize) };
                    let b1 = unsafe { *ptr.add(e.saturating_sub(1) as usize) };
                    let w = ((e >= s + 2) & (b0 == b'"') & (b1 == b'"')) as u32;
                    wrapped += w;
                    fields[j] = (s + w, e - w);
                    s = e + 1;
                }
            }
            if qc - q_prev == 2 * wrapped {
                f(buf, &fields);
            }
            k += N;
        } else {
            while unsafe { *seps_sl.get_unchecked(k) } != lf {
                k += 1;
            }
            k += 1;
        }
        row_start = lf + 1;
        q_prev = qc;
    }
}

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
}

/// Lazily map input files and release each one on its last scanning worker.
pub(super) struct InputMappings<'a> {
    files: &'a [FileInfo],
    mappings: Vec<FileMapping>,
}

impl<'a> InputMappings<'a> {
    pub(super) fn new(files: &'a [FileInfo], chunks: &[Chunk]) -> Self {
        let mut counts = vec![0usize; files.len()];
        for c in chunks {
            counts[c.file as usize] += 1;
        }
        let mappings = files.iter().zip(counts).map(|(file, count)| {
            FileMapping {
                base: OnceLock::new(),
                remaining: AtomicUsize::new(count),
                len: file.len,
            }
        }).collect();
        Self { files, mappings }
    }

    /// # Safety
    /// The chunk must come from this plan and may be leased at most once.
    /// The file must remain unchanged until all its leases finish.
    pub(super) unsafe fn lease(&self, chunk: Chunk) -> ChunkMapping<'_> {
        let index = chunk.file as usize;
        ChunkMapping {
            file: &self.files[index],
            mapping: &self.mappings[index],
            chunk,
        }
    }
}

struct FileMapping {
    base: OnceLock<AtomicPtr<u8>>,
    remaining: AtomicUsize,
    len: u64,
}

impl FileMapping {
    fn unmap(&self) {
        if let Some(base) = self.base.get() {
            let ptr = base.swap(std::ptr::null_mut(), Ordering::Relaxed);
            if !ptr.is_null() {
                // This mapping is dedicated to this file and has no remaining readers.
                unsafe { munmap(ptr, self.len as usize) };
            }
        }
    }
}

impl Drop for FileMapping {
    fn drop(&mut self) {
        self.unmap();
    }
}

pub(super) struct ChunkMapping<'a> {
    file: &'a FileInfo,
    mapping: &'a FileMapping,
    chunk: Chunk,
}

impl Drop for ChunkMapping<'_> {
    fn drop(&mut self) {
        // Each task holds a lease until its callback finishes. The last task
        // acquires the preceding lease releases before unmapping the file.
        if self.mapping.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.mapping.unmap();
        }
    }
}

impl ChunkMapping<'_> {
    #[inline(always)]
    pub(super) fn scan<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(
        &self,
        buf: &mut Vec<u8>,
        f: F,
    ) {
        // The buffered reader provides padding and handles an unterminated
        // final row. Mapped chunks use only readable bytes within the file.
        let file = self.file;
        let c = self.chunk;
        let start = c.start.saturating_sub(1) as usize;
        if file.len <= isize::MAX as u64 && c.end + (PAD as u64) < file.len {
            let base = self.mapping.base.get_or_init(|| {
                const PROT_READ: i32 = 1;
                const MAP_PRIVATE: i32 = 2;
                let ptr = unsafe {
                    mmap(std::ptr::null_mut(), file.len as usize, PROT_READ, MAP_PRIVATE, file.handle.as_raw_fd(), 0)
                };
                AtomicPtr::new(if ptr as isize == -1 { std::ptr::null_mut() } else { ptr })
            }).load(Ordering::Relaxed);
            if !base.is_null() {
                // Input is immutable for the run. The chunk lease prevents concurrent unmapping.
                let data = unsafe { std::slice::from_raw_parts(base.add(start), file.len as usize - start) };
                let own_end = c.end as usize - start;
                let Some(lo) = memchr(b'\n', data).map(|i| i + 1) else {
                    return;
                };
                if lo >= own_end {
                    return;
                }
                if let Some(end) = memchr(b'\n', &data[own_end - 1..]).map(|i| i + own_end) {
                    if end < u32::MAX as usize && end + PAD <= data.len() {
                        scan::<N, F>(&data[..end + PAD], lo, end, f);
                        return;
                    }
                }
            }
        }
        let (lo, hi) = read_chunk(&file.handle, file.len, c, buf);
        scan::<N, F>(buf, lo, hi, f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rows<const N: usize>(text: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut buf = text.to_vec();
        let hi = buf.len();
        buf.resize(hi + PAD, 0);
        let mut out = Vec::new();
        scan::<N, _>(&buf, 0, hi, |b, f| out.push(f.iter().map(|&(s, e)| b[s as usize..e as usize].to_vec()).collect()));
        out
    }
    #[test]
    fn mapped_chunks_match_pread_and_release_the_file() {
        struct TestFile(std::path::PathBuf);
        impl Drop for TestFile {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }

        let path = TestFile(std::env::temp_dir().join(format!(
            "shopping-sessions-mapped-{}.csv",
            std::process::id()
        )));
        let mut input = b"a,b,c,d,e,f,g\n".to_vec();
        for i in 0..80 {
            if i > 0 {
                input.push(b'\n');
            }
            let width = if i % 17 == 0 { 170 } else { i };
            input.resize(input.len() + width, b'x');
            input.extend_from_slice(b",\"y\",z,,a,b,c");
        }
        std::fs::write(&path.0, &input).unwrap();
        let files = [FileInfo {
            handle: File::open(&path.0).unwrap(),
            len: input.len() as u64,
            kind: Kind::Events,
        }];
        let chunks = plan(&files, 64);
        let mappings = InputMappings::new(&files, &chunks);
        let mut want = Vec::new();
        let mut got = Vec::new();
        let mut buf = Vec::new();
        for chunk in chunks {
            let (lo, hi) = read_chunk(&files[0].handle, files[0].len, chunk, &mut buf);
            scan::<7, _>(&buf, lo, hi, |buf, fields| {
                want.push(fields.map(|(lo, hi)| buf[lo as usize..hi as usize].to_vec()));
            });
            // The loop visits each planned chunk once.
            let mapping = unsafe { mappings.lease(chunk) };
            mapping.scan::<7, _>(&mut buf, |buf, fields| {
                got.push(fields.map(|(lo, hi)| buf[lo as usize..hi as usize].to_vec()));
            });
        }
        assert_eq!(got.len(), 80);
        assert_eq!(got, want);
        assert!(mappings.mappings[0].base.get().unwrap().load(Ordering::Relaxed).is_null());
    }

    #[test]
    fn two_stage_matches_reference_random() {
        let mut rng = 0x2545F4914F6CDD1Du64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let alpha = b"ab,\"\n \r";
        for round in 0..300 {
            let n = if round < 5 { 80_000 } else { (next() % 3000) as usize };
            let mut text: Vec<u8> = (0..n).map(|_| {
                let r = next() % 100;
                if r < 70 { b'x' } else { alpha[(next() % alpha.len() as u64) as usize] }
            }).collect();
            if round % 2 == 0 { text.push(b'\n'); }
            let hi = match text.iter().rposition(|&b| b == b'\n') { Some(p) => p + 1, None => continue };
            let mut buf = text.clone();
            buf.resize(text.len() + PAD + 1, 0);
            let lo = (next() % 3) as usize;
            let lo = lo.min(hi);
            let (mut a, mut b) = (Vec::new(), Vec::new());
            scan_ref::<3, _>(&buf, lo, hi, |buf, f| {
                a.push(f.map(|(s, e)| buf[s as usize..e as usize].to_vec()));
            });
            scan::<3, _>(&buf, lo, hi, |buf, f| {
                b.push(f.map(|(s, e)| buf[s as usize..e as usize].to_vec()));
            });
            assert_eq!(a, b);
            let (mut a, mut b) = (Vec::new(), Vec::new());
            scan_ref::<1, _>(&buf, lo, hi, |buf, f| {
                a.push(f.map(|(s, e)| buf[s as usize..e as usize].to_vec()));
            });
            scan::<1, _>(&buf, lo, hi, |buf, f| {
                b.push(f.map(|(s, e)| buf[s as usize..e as usize].to_vec()));
            });
            assert_eq!(a, b);
        }
    }

    #[test]
    fn scanner_matches_csv_split() {
        let mut text = Vec::new();
        let samples: Vec<&[u8]> = vec![
            b"a,b", b"\"a\",\"b\"", b"\"\",", b"a,b,c", b"a", b"", b"\"a,b", b"a\"x\",b", b"\"a\"b\",c", b"\",b",
            b"x\r,y\r", b"\"\"\"\",b", b"\"a\"\"\",b", b" u1,\" u1\"",
        ];
        let long = [b"\"".as_slice(), &[b'x'; 70], b"\",", &[b'y'; 80]].concat();
        let very_long = [b"\"".as_slice(), &[b'x'; 40_000], b"\",", &[b'y'; 40_000]].concat();
        let mut all: Vec<Vec<u8>> = samples.iter().map(|s| s.to_vec()).collect();
        all.push(long);
        all.push(very_long);
        for s in &all {
            text.extend_from_slice(s);
            text.push(b'\n');
        }
        let got = rows::<2>(&text);
        let mut want = Vec::new();
        for s in &all {
            let mut f = [&b""[..]; 2];
            if crate::ingest::csv::split(s, &mut f) {
                want.push(f.iter().map(|x| x.to_vec()).collect::<Vec<_>>());
            }
        }
        assert_eq!(got, want);
    }
}
