use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub const PAD: usize = 64;

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
    for (sub, kind) in [
        ("events", Kind::Events),
        ("clicks", Kind::Clicks),
        ("links", Kind::Links),
    ] {
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
            v.push(Chunk {
                file: i as u32,
                start: s,
                end: e,
            });
            s = e;
        }
    }
    v
}

/// Reads chunk bytes into `buf` such that `buf[lo..hi]` holds exactly the rows
/// owned by the chunk, each terminated by LF (a final unterminated row at EOF
/// gets a synthetic LF), followed by >= PAD zero bytes. Skips the header for the
/// chunk at offset 0. Returns (lo, hi).
pub fn read_chunk(file: &File, flen: u64, c: Chunk, buf: &mut [u8]) -> Option<(usize, usize)> {
    const EXTRA: u64 = 4096;
    let rs = if c.start == 0 { 0 } else { c.start - 1 };
    let mut want = (c.end + EXTRA).min(flen) - rs;
    loop {
        let w = want as usize;
        let need = w + PAD + 1;
        if need > buf.len() {
            return None;
        }
        read_exact_at(file, &mut buf[..w], rs);
        buf[w..need].fill(0);
        let data = &buf[..w];
        let lo = if c.start == 0 {
            match memchr(b'\n', data) {
                Some(p) => p + 1,
                None => return Some((0, 0)),
            }
        } else {
            match memchr(b'\n', data) {
                Some(p) => p + 1,
                None => {
                    if rs + want >= flen {
                        return Some((0, 0));
                    }
                    want = (want * 2).min(flen - rs);
                    continue;
                }
            }
        };
        let own_end = (c.end - rs) as usize;
        if lo >= own_end {
            return Some((0, 0));
        }
        match memchr(b'\n', &data[own_end - 1..]) {
            Some(p) => return Some((lo, own_end + p)),
            None => {
                if rs + want >= flen {
                    if w > lo {
                        buf[w] = b'\n';
                        return Some((lo, w + 1));
                    }
                    return Some((lo, w));
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
    if i >= 64 {
        u64::MAX
    } else {
        (1u64 << i) - 1
    }
}

/// Validate the columns of one LF-delimited row for direct-parser fallback.
pub(super) fn row_fields<const N: usize>(
    buf: &[u8],
    lo: usize,
    lf: usize,
) -> Option<[(u32, u32); N]> {
    assert!(lo <= lf && lf < u32::MAX as usize && buf.len() >= lf + PAD);
    let mut ends = [lf; N];
    let mut seen = 0;
    let mut quotes = 0;
    for base in (lo..lf).step_by(64) {
        let (seps, q, _) = masks(unsafe { buf.as_ptr().add(base) });
        let valid = below((lf - base) as u32);
        let mut seps = seps & valid;
        quotes += (q & valid).count_ones();
        while seps != 0 {
            if seen + 1 == N {
                return None;
            }
            ends[seen] = base + seps.trailing_zeros() as usize;
            seen += 1;
            seps &= seps - 1;
        }
    }
    if seen + 1 != N {
        return None;
    }
    let mut start = lo;
    let mut wrapped = 0;
    let fields = ends.map(|end| {
        let q = usize::from(end - start >= 2 && buf[start] == b'"' && buf[end - 1] == b'"');
        let field = ((start + q) as u32, (end - q) as u32);
        start = end + 1;
        wrapped += q as u32;
        field
    });
    (quotes == 2 * wrapped).then_some(fields)
}
