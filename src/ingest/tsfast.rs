//! SWAR timestamp parsing with identical semantics to `ts::parse_ts`.
//! Callers guarantee at least 32 readable bytes after the field start beyond its
//! length (scan buffers have PAD = 64 bytes after every row).
use super::ts::days_from_civil;

#[inline(always)]
unsafe fn ld8(p: *const u8) -> u64 {
    u64::from_le_bytes(std::ptr::read_unaligned(p as *const [u8; 8]))
}

/// All lanes selected by `m` (0xFF per lane) hold ASCII digits.
#[inline(always)]
fn digits_ok(x: u64, m: u64) -> bool {
    let hi = 0xF0F0_F0F0_F0F0_F0F0u64 & m;
    let t = 0x3030_3030_3030_3030u64 & m;
    ((x & hi) == t) & (((x & m).wrapping_add(0x0606_0606_0606_0606u64 & m) & hi) == t)
}

#[inline(always)]
fn dig(x: u64, lane: u32) -> i64 {
    ((x >> (8 * lane)) & 0x0F) as i64
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// `YYYY-MM-DDTHH:MM:SS` at p (19 bytes) -> UTC ms (before zone), or None.
#[inline(always)]
unsafe fn civil19(p: *const u8) -> Option<i64> {
    let a = ld8(p); // YYYY-MM-
    let b = ld8(p.add(8)); // DDTHH:MM
    let c = ld8(p.add(11)); // HH:MM:SS
    // a: digits lanes 0-3,5,6; '-' at 4,7
    const MA: u64 = 0x00FF_FF00_FFFF_FFFF;
    const SA: u64 = 0xFF00_00FF_0000_0000;
    const VA: u64 = (b'-' as u64) << 56 | (b'-' as u64) << 32;
    // b: digits lanes 0,1,3,4,6,7; 'T' at 2, ':' at 5
    const MB: u64 = 0xFFFF_00FF_FF00_FFFF;
    const SB: u64 = 0x0000_FF00_00FF_0000;
    const VB: u64 = (b':' as u64) << 40 | (b'T' as u64) << 16;
    // c (from 11): lanes 0,1 HH (checked), 2 ':', 3,4 MM (checked), 5 ':', 6,7 SS
    const MC: u64 = 0xFFFF_0000_0000_0000;
    const SC: u64 = 0x0000_FF00_0000_0000;
    const VC: u64 = (b':' as u64) << 40;
    let ok = digits_ok(a, MA) & digits_ok(b, MB) & digits_ok(c, MC) & (a & SA == VA) & (b & SB == VB) & (c & SC == VC);
    if !ok {
        return None;
    }
    let year = dig(a, 0) * 1000 + dig(a, 1) * 100 + dig(a, 2) * 10 + dig(a, 3);
    let month = dig(a, 5) * 10 + dig(a, 6);
    let day = dig(b, 0) * 10 + dig(b, 1);
    let hour = dig(b, 3) * 10 + dig(b, 4);
    let minute = dig(b, 6) * 10 + dig(b, 7);
    let second = dig(c, 6) * 10 + dig(c, 7);
    const DIM: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 || day < 1 {
        return None;
    }
    let dim = if month == 2 && is_leap(year) { 29 } else { DIM[month as usize] };
    if day > dim {
        return None;
    }
    Some((days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second) * 1000)
}

/// Parse up to 8 ASCII digits (k = 1..=8 lanes from p). Returns None if any is not a digit.
#[inline(always)]
pub unsafe fn digits8(p: *const u8, k: usize) -> Option<u64> {
    debug_assert!((1..=8).contains(&k));
    let raw = ld8(p);
    let sh = 8 * (8 - k) as u32;
    // keep k low lanes, move them to the top lanes, fill low lanes with '0'
    let m = u64::MAX >> sh;
    if !digits_ok(raw, m) {
        return None;
    }
    let x = ((raw & m) << sh) | (0x3030_3030_3030_3030u64 & !(u64::MAX << sh));
    let mut v = x.wrapping_sub(0x3030_3030_3030_3030);
    v = v.wrapping_mul(10).wrapping_add(v >> 8);
    v = (((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000u64 << 32)))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32;
    Some(v)
}

/// F3: digits only. Same results as `ts::parse_f3`.
#[inline(always)]
pub unsafe fn f3(p: *const u8, len: usize) -> Option<i64> {
    if len <= 8 {
        return digits8(p, len).map(|v| v as i64);
    }
    if len <= 16 {
        let hi = digits8(p, len - 8)?;
        let lo = digits8(p.add(len - 8), 8)?;
        return Some((hi * 100_000_000 + lo) as i64);
    }
    super::ts::parse_f3(std::slice::from_raw_parts(p, len))
}

/// Same semantics as `ts::parse_ts`.
#[inline(always)]
pub unsafe fn parse(p: *const u8, len: usize) -> Option<i64> {
    if len == 20 {
        if *p.add(19) == b'Z' {
            return civil19(p);
        }
    } else if len == 25 {
        let s = *p.add(19);
        if s == b'+' || s == b'-' {
            let base = civil19(p)?;
            let o = ld8(p.add(17)); // "SS+HH:MM" lanes: 0,1 SS, 2 sign, 3,4 HH, 5 ':', 6,7 MM
            const MO: u64 = 0xFFFF_00FF_FF00_0000;
            if !digits_ok(o, MO) || (o >> 40) & 0xFF != b':' as u64 {
                return None;
            }
            let oh = dig(o, 3) * 10 + dig(o, 4);
            let om = dig(o, 6) * 10 + dig(o, 7);
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 3600 + om * 60) * 1000;
            return Some(if s == b'+' { base - off } else { base + off });
        }
    } else if len == 0 {
        return None;
    }
    if len == 20 || len == 25 {
        // not ISO-shaped at the zone position: only F3 digits can be valid
        return f3(p, len);
    }
    f3(p, len)
}

#[cfg(test)]
mod tests {
    use super::super::ts;
    use super::*;

    fn check(s: &[u8]) {
        let mut buf = s.to_vec();
        buf.extend_from_slice(&[b'7'; 64]);
        let got = unsafe { parse(buf.as_ptr(), s.len()) };
        assert_eq!(got, ts::parse_ts(s), "{:?}", String::from_utf8_lossy(s));
    }

    #[test]
    fn fuzz_against_reference() {
        let seeds: Vec<&[u8]> = vec![
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"2028-02-29T00:00:00Z", b"2027-02-29T00:00:00Z",
            b"1791071890000", b"0001791071890000", b"86400000", b"", b"0", b"99999999999999999999",
            b"9223372036854775807", b"9223372036854775808", b"2026-10-03T23:58:10+23:59", b"2026-10-03T23:58:10+24:00",
            b"12345678901234567", b"00000000000000000000000001", b"2026-12-31T23:59:59Z", b"0000-01-01T00:00:00Z",
            b"9999-12-31T23:59:59-00:00", b"2026-10-03T23:58:10.000Z", b"20261003T235810Z00000", b"1234567890123456789012345",
        ];
        let alphabet = b"0123456789-T:Z+z t.e\xd9\xa2\x00";
        let mut rng: u64 = 0x1234_5678_9abc_def1;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for s in &seeds {
            check(s);
        }
        for _ in 0..2_000_000 {
            let base = seeds[(next() % seeds.len() as u64) as usize];
            let mut s = base.to_vec();
            let muts = next() % 4;
            for _ in 0..muts {
                if s.is_empty() {
                    break;
                }
                let i = (next() % s.len() as u64) as usize;
                match next() % 4 {
                    0 => s[i] = alphabet[(next() % alphabet.len() as u64) as usize],
                    1 => s[i] = b'0' + (next() % 10) as u8,
                    2 => {
                        s.remove(i);
                    }
                    _ => s.insert(i, b'0' + (next() % 10) as u8),
                }
            }
            check(&s);
        }
        // exhaustive-ish calendar sweep
        for y in [1970i64, 2000, 2024, 2025, 2028, 2100] {
            for m in 0..=13 {
                for d in 0..=32 {
                    for (h, mi, se) in [(0, 0, 0), (23, 59, 59), (24, 0, 0), (12, 60, 0), (12, 0, 60)] {
                        check(format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z").as_bytes());
                        check(format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{se:02}+05:30").as_bytes());
                    }
                }
            }
        }
    }
}
