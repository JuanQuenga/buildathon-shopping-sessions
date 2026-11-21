//! LF-aligned chunked reading (pread into a thread-local padded buffer) and a
//! SIMD structural scanner that yields well-formed rows' unquoted field ranges.
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

pub const PAD: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Events,
    Clicks,
    Links,
}

pub struct FileInfo {
    pub path: PathBuf,
    pub len: u64,
    pub kind: Kind,
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
    // Read from start-1 so we can tell whether start is a row start.
    let rs = if c.start == 0 { 0 } else { c.start - 1 };
    let mut want = (c.end + EXTRA).min(flen) - rs;
    loop {
        buf.clear();
        buf.resize(want as usize + PAD + 1, 0);
        read_exact_at(file, &mut buf[..want as usize], rs);
        let data = &buf[..want as usize];
        // Row start: first byte after an LF at index >= 0 (offset rs), or 0 for start==0.
        let lo = if c.start == 0 {
            // skip header line
            match memchr(b'\n', data) {
                Some(p) => p + 1,
                None => {
                    return (0, 0);
                }
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
        let own_end = (c.end - rs) as usize; // rows must start before this
        if lo >= own_end {
            return (0, 0);
        }
        // Find the end of the last row starting before own_end: first LF at index >= own_end - 1.
        match memchr(b'\n', &data[own_end - 1..]) {
            Some(p) => return (lo, own_end - 1 + p + 1),
            None => {
                if rs + want >= flen {
                    // EOF without trailing LF: synthesize one.
                    let n = want as usize;
                    if n > lo {
                        buf[n] = b'\n';
                        return (lo, n + 1);
                    }
                    return (lo, n);
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
            let m = |x: __m256i, y: __m256i| -> u64 {
                let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, x)) as u32 as u64;
                let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, y)) as u32 as u64;
                lo | (hi << 32)
            };
            let c = m(comma, comma);
            let n = m(nl, nl);
            let q = m(qt, qt);
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
    // bits [0, i)
    if i >= 64 {
        u64::MAX
    } else {
        (1u64 << i) - 1
    }
}

/// Scan `buf[lo..hi]` (rows terminated by LF; PAD readable bytes after hi).
/// Calls `f(buf, fields)` for each well-formed row with exactly N fields; each
/// field is `(start, end)` after removing one outer quote pair.
///
/// Quote rule check per row: a field is "wrapped" if it has length >= 2 and
/// starts and ends with `"`. The row is well-formed iff the number of quote
/// bytes in the row equals 2 x (number of wrapped fields): then every quote is
/// one of the wrapping pairs and no other quote exists anywhere.
#[inline(always)]
pub fn scan_ref<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(buf: &[u8], lo: usize, hi: usize, mut f: F) {
    if lo >= hi {
        return;
    }
    assert!(buf.len() >= hi + PAD && N <= 15 && hi < u32::MAX as usize);
    let mut seps = [0u32; 16];
    let mut k = 0usize;
    let mut row_start = lo as u32;
    let mut q_block = 0u32; // quotes before current block
    let mut q_row = 0u32; // quotes before row start
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
                let q_end = q_block + (q & below(i)).count_ones();
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
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::new(Scratch::default());
}

/// Same contract as `scan_ref`, in two stages: (1) flatten all separator
/// positions (commas and LFs) of the range with unconditional writes, and record
/// for every LF the running quote count; (2) walk rows: a row is well-formed in
/// count iff its N-th separator is its own LF.
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
    let ptr = buf.as_ptr();
    let seps = &mut sc.seps;
    let lfs = &mut sc.lfs;
    seps.clear();
    lfs.clear();
    let mut qb = 0u32;
    let mut base = lo;
    let mut len = 0usize;
    while base < hi {
        if seps.capacity() < len + 72 {
            unsafe { seps.set_len(len) };
            seps.reserve(len + 72 + (hi - base) / 4);
        }
        let (sep, q, nl) = masks(unsafe { ptr.add(base) });
        let valid = below((hi - base) as u32);
        let mut m = sep & valid;
        let q = q & valid;
        let cnt = m.count_ones() as usize;
        let out = unsafe { seps.as_mut_ptr().add(len) };
        let b32 = base as u32;
        let mut i = 0;
        while i < cnt {
            for k in 0..8 {
                unsafe { *out.add(i + k) = b32.wrapping_add(m.trailing_zeros()) };
                m &= m.wrapping_sub(1);
            }
            i += 8;
        }
        len += cnt;
        let mut n = nl & valid;
        while n != 0 {
            let i = n.trailing_zeros();
            lfs.push((b32 + i, qb + (q & below(i)).count_ones()));
            n &= n - 1;
        }
        qb += q.count_ones();
        base += 64;
    }
    unsafe { seps.set_len(len) };
    let seps = &seps[..];
    let mut k = 0usize;
    let mut row_start = lo as u32;
    let mut q_prev = 0u32;
    for &(lf, qc) in lfs.iter() {
        if k + N <= len && unsafe { *seps.get_unchecked(k + N - 1) } == lf {
            let mut fields = [(0u32, 0u32); N];
            let mut wrapped = 0u32;
            let mut s = row_start;
            for j in 0..N {
                let e = unsafe { *seps.get_unchecked(k + j) };
                let w = e >= s + 2 && unsafe { *ptr.add(s as usize) == b'"' && *ptr.add(e as usize - 1) == b'"' };
                wrapped += w as u32;
                fields[j] = (s + w as u32, e - w as u32);
                s = e + 1;
            }
            if qc - q_prev == 2 * wrapped {
                f(buf, &fields);
            }
            k += N;
        } else {
            while seps[k] != lf {
                k += 1;
            }
            k += 1;
        }
        row_start = lf + 1;
        q_prev = qc;
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
            let n = (next() % 3000) as usize;
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
            scan_ref::<3, _>(&buf, lo, hi, |_, f| a.push(*f));
            scan::<3, _>(&buf, lo, hi, |_, f| b.push(*f));
            assert_eq!(a, b);
            let (mut a, mut b) = (Vec::new(), Vec::new());
            scan_ref::<1, _>(&buf, lo, hi, |_, f| a.push(*f));
            scan::<1, _>(&buf, lo, hi, |_, f| b.push(*f));
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
        // long rows crossing 64-byte blocks
        let long = [b"\"".as_slice(), &[b'x'; 70], b"\",", &[b'y'; 80]].concat();
        let mut all: Vec<Vec<u8>> = samples.iter().map(|s| s.to_vec()).collect();
        all.push(long);
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
