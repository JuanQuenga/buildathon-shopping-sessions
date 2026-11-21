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
#[inline(always)]
pub fn scan<const N: usize, F: FnMut(&[u8], &[(u32, u32); N])>(buf: &[u8], lo: usize, hi: usize, mut f: F) {
    if lo >= hi {
        return;
    }
    assert!(buf.len() >= hi + PAD);
    let mut fields = [(0u32, 0u32); N];
    let mut k = 0usize;
    let mut bad = false;
    let mut qc: u32 = 0;
    let mut fs = lo;
    let mut base = lo;
    let ptr = buf.as_ptr();
    while base < hi {
        let (mut sep, q, nlm) = masks(unsafe { ptr.add(base) });
        let valid = below((hi - base) as u32);
        sep &= valid;
        let q = q & valid;
        let mut prev = 0u32;
        while sep != 0 {
            let i = sep.trailing_zeros();
            let pos = base + i as usize;
            qc += (q & below(i) & !below(prev)).count_ones();
            if !bad {
                if k == N {
                    bad = true;
                } else if qc == 0 {
                    fields[k] = (fs as u32, pos as u32);
                    k += 1;
                } else if qc == 2 && pos - fs >= 2 && buf[fs] == b'"' && buf[pos - 1] == b'"' {
                    fields[k] = (fs as u32 + 1, pos as u32 - 1);
                    k += 1;
                } else {
                    bad = true;
                }
            }
            qc = 0;
            prev = i + 1;
            fs = pos + 1;
            if nlm & (1u64 << i) != 0 {
                if !bad && k == N {
                    f(buf, &fields);
                }
                k = 0;
                bad = false;
            }
            sep &= sep - 1;
        }
        qc += (q & !below(prev)).count_ones();
        base += 64;
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
