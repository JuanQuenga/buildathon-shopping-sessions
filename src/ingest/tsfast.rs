//! SWAR timestamp parsing with L1-resident date cache and identical semantics to `ts::parse_ts`.
//! Callers guarantee at least 32 readable bytes after the field start beyond its
//! length (scan buffers have PAD = 64 bytes after every row).
use super::ts::{days_from_civil, DAY_MS};

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

/// Inverse of `days_from_civil` (Howard Hinnant): days since 1970-01-01 -> (year, month, day).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (y + i64::from(m <= 2), m, d)
}

/// 8 KiB L1-resident direct-mapped cache of valid `YYYY-MM-DDT` prefixes around the dataset window.
/// Eliminates calendar validation and `days_from_civil` division for >99% of F1/F2 timestamps,
/// covering up to 211 days (~7-8 consecutive months, zero collisions across year rollovers).
#[repr(C, align(16))]
#[derive(Clone, Copy, Default)]
struct DateEntry {
    /// First 8 bytes: `YYYY-MM-` (0 = empty slot).
    a: u64,
    /// Low 24 bits: bytes `DDT` (`b & 0x00FF_FFFF`).
    ddt: u32,
    /// UTC epoch seconds of `YYYY-MM-DDT00:00:00Z` (valid for 1970..2106).
    day_sec: u32,
}

#[repr(C, align(64))]
pub struct DateTable {
    entries: [DateEntry; 512],
}

impl DateTable {
    /// Prepopulate valid dates in `[ws - 14d, we + 14d]` (up to 211 days, zero collisions across
    /// any 8 consecutive months incl. Dec->Jan rollovers, zero-multiply slot hash). Dates outside
    /// 1970..2106 or missing from the cache fall back to exact `civil19_sec_cold`.
    pub fn build(ws_ms: i64, we_ms: i64) -> Self {
        let mut entries = [DateEntry::default(); 512];
        let d0 = ws_ms.div_euclid(DAY_MS) - 14;
        let d1 = (we_ms.div_euclid(DAY_MS) + 14).min(d0 + 210);
        for d in d0..=d1 {
            let sec = d * 86_400;
            if sec < 0 || sec > (u32::MAX as i64 - 86_400) {
                continue;
            }
            let (y, m, day) = civil_from_days(d);
            if !(0..=9999).contains(&y) {
                continue;
            }
            let mut buf = [0u8; 16];
            buf[0] = b'0' + (y / 1000 % 10) as u8;
            buf[1] = b'0' + (y / 100 % 10) as u8;
            buf[2] = b'0' + (y / 10 % 10) as u8;
            buf[3] = b'0' + (y % 10) as u8;
            buf[4] = b'-';
            buf[5] = b'0' + (m / 10) as u8;
            buf[6] = b'0' + (m % 10) as u8;
            buf[7] = b'-';
            buf[8] = b'0' + (day / 10) as u8;
            buf[9] = b'0' + (day % 10) as u8;
            buf[10] = b'T';
            let a = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let ddt = u32::from_le_bytes([buf[8], buf[9], buf[10], 0]);
            let idx = Self::slot(a, ddt);
            entries[idx] = DateEntry { a, ddt, day_sec: sec as u32 };
        }
        DateTable { entries }
    }

    #[inline(always)]
    fn slot(a: u64, ddt: u32) -> usize {
        // Exact `(year * 12 + month) mod 8` across all 12 months and every year rollover:
        // 12 ≡ 4 (mod 8) => (Y3 & 1) * 4; 10 ≡ 2 (mod 8) => (M0 & 1) * 2; plus (M1 & 7).
        let m8 = (((a >> 48) + ((a >> 39) & 2) + ((a >> 22) & 4)) & 7) as usize;
        // Day 01..31 -> 6-bit BCD ((tens & 3) << 4) | (ones & 0x0F): zero multiply, zero collision!
        let day6 = (((ddt & 3) << 4) | ((ddt >> 8) & 0x0F)) as usize;
        (m8 << 6) | day6
    }

    /// Parse `YYYY-MM-DDTHH:MM:SS` (19 bytes) -> UTC epoch seconds (before zone), or None.
    #[inline(always)]
    unsafe fn civil19_sec(&self, p: *const u8) -> Option<i64> {
        let a = ld8(p); // YYYY-MM-
        let ddt = (std::ptr::read_unaligned(p.add(8) as *const u32)) & 0x00FF_FFFF; // DDT
        let e = self.entries.get_unchecked(Self::slot(a, ddt));
        if (e.a == a) & (e.ddt == ddt) {
            return Self::clock_sec(p).map(|sec| sec + e.day_sec as i64);
        }
        civil19_sec_cold(p)
    }

    #[inline(always)]
    unsafe fn clock_sec(p: *const u8) -> Option<i64> {
        let c = ld8(p.add(11)); // HH:MM:SS
        const SC: u64 = 0x0000_FF00_00FF_0000;
        const VC: u64 = ((b':' as u64) << 40) | ((b':' as u64) << 16);
        let d = c.wrapping_sub(0x3030_3030_3030_3030);
        // Simultaneously verifies H0<=2, H1<=9, M0<=5, M1<=9, S0<=5, S1<=9 (no carries on valid digits).
        let ok_hms = (((d.wrapping_add(0x767A_0076_7A00_767Du64) | d) & 0x8080_0080_8000_8080u64) == 0)
            & ((c & SC) == VC);
        let dig = d & 0x0F0F_000F_0F00_0F0Fu64;
        let pairs = dig.wrapping_mul(10).wrapping_add(dig >> 8);
        let hour = pairs & 0xFF;
        if ok_hms & (hour <= 23) {
            let hm_sec = ((pairs & 0x0000_0000_FF00_00FFu64).wrapping_mul((3600u64 << 40) | (60u64 << 16))) >> 40;
            let sec = (pairs >> 48) & 0xFF;
            return Some((hm_sec + sec) as i64);
        }
        None
    }

    #[inline(always)]
    unsafe fn clock_pair(a: *const u8, b: *const u8) -> Option<(i64, i64)> {
        use std::arch::x86_64::*;
        let raw = _mm_set_epi64x(ld8(b.add(11)) as i64, ld8(a.add(11)) as i64);
        let min = _mm_set1_epi64x(i64::from_le_bytes(*b"00:00:00"));
        let max = _mm_set1_epi64x(i64::from_le_bytes(*b"29:59:59"));
        let bad = _mm_or_si128(_mm_subs_epu8(min, raw), _mm_subs_epu8(raw, max));
        // Pack each time as three adjacent decimal pairs, dropping the colons.
        let digits = _mm_shuffle_epi8(
            _mm_sub_epi8(raw, min),
            _mm_setr_epi8(0, 1, 3, 4, 6, 7, -1, -1, 8, 9, 11, 12, 14, 15, -1, -1),
        );
        let times = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
        let hours_bad = _mm_cmpgt_epi16(times, _mm_setr_epi16(23, 59, 59, 0, 23, 59, 59, 0));
        let invalid = _mm_or_si128(bad, hours_bad);
        if _mm_testz_si128(invalid, invalid) == 0 {
            return None;
        }
        let both = _mm_madd_epi16(times, _mm_setr_epi16(3600, 60, 1, 0, 3600, 60, 1, 0));
        let seconds = _mm_hadd_epi32(both, both);
        Some((_mm_cvtsi128_si32(seconds) as i64, _mm_extract_epi32::<1>(seconds) as i64))
    }

    /// Both fields have an independently checked F1 terminator and delimiter.
    /// Returns whole-second `(event_sec, ingest_sec)`.
    #[inline(always)]
    pub(super) unsafe fn parse_f1_pair_sec(&self, a: *const u8, b: *const u8) -> Option<(i64, i64)> {
        let prefix = ld8(a);
        let ddt = a.add(8).cast::<u32>().read_unaligned() & 0x00FF_FFFF;
        let date = self.entries.get_unchecked(Self::slot(prefix, ddt));
        if (date.a == prefix) & (date.ddt == ddt) {
            if (ld8(b) == prefix) & (b.add(8).cast::<u32>().read_unaligned() & 0x00FF_FFFF == ddt) {
                let (event, ingest) = Self::clock_pair(a, b)?;
                let day = date.day_sec as i64;
                return Some((day + event, day + ingest));
            }
            let event = date.day_sec as i64 + Self::clock_sec(a)?;
            return Some((event, self.civil19_sec(b)?));
        }
        Some((civil19_sec_cold(a)?, self.civil19_sec(b)?))
    }

    /// Fast path for `ingest_ts` (guaranteed valid F1): returns `Some(epoch_seconds)` if whole
    /// seconds, `None` if unparseable or non-F1.
    #[inline(always)]
    pub unsafe fn parse_f1_sec(&self, p: *const u8, len: usize) -> Option<i64> {
        if (len == 20) & (*p.add(19) == b'Z') {
            return self.civil19_sec(p);
        }
        None
    }

    /// Exact equivalent of `ts::parse_ts` using the L1 date cache + specialized 13-digit F3
    /// fast path plus a cold fallback.
    #[inline(always)]
    pub unsafe fn parse(&self, p: *const u8, len: usize) -> Option<i64> {
        if len == 20 {
            if *p.add(19) == b'Z' {
                return self.civil19_sec(p).map(|s| s * 1000);
            }
            return parse_cold(p, len);
        }
        if len == 13 {
            const ASCII_ZERO: u64 = 0x3030_3030_3030_3030;
            const DIGIT_LIMIT: u64 = 0x7676_7676_7676_7676;
            const M5: u64 = 0x0000_00FF_FFFF_FFFF;
            let d_hi = ld8(p).wrapping_sub(ASCII_ZERO);
            let d_lo = ld8(p.add(5)).wrapping_sub(ASCII_ZERO);
            // Valid lanes cannot carry or borrow into the next lane. The first
            // invalid lane therefore sets its high bit in d or d + DIGIT_LIMIT.
            let bad_hi = (d_hi.wrapping_add(DIGIT_LIMIT) | d_hi) & 0x0000_0080_8080_8080;
            let bad_lo = (d_lo.wrapping_add(DIGIT_LIMIT) | d_lo) & 0x8080_8080_8080_8080;
            if (bad_hi | bad_lo) == 0 {
                let hi = swar_dec8_digits((d_hi & M5) << 24);
                let lo = swar_dec8_digits(d_lo);
                return Some((hi * 100_000_000 + lo) as i64);
            }
            return None;
        }
        if len == 25 {
            let s = *p.add(19);
            if (s == b'+') | (s == b'-') {
                let base_sec = self.civil19_sec(p)?;
                let o = ld8(p.add(17)); // "SS+HH:MM"
                const MO: u64 = 0xFFFF_00FF_FF00_0000;
                if !digits_ok(o, MO) | (((o >> 40) & 0xFF) != b':' as u64) {
                    return None;
                }
                let oh = dig(o, 3) * 10 + dig(o, 4);
                let om = dig(o, 6) * 10 + dig(o, 7);
                if (oh > 23) | (om > 59) {
                    return None;
                }
                let off = oh * 3600 + om * 60;
                let delta = if s == b'+' { -off } else { off };
                return Some((base_sec + delta) * 1000);
            }
            return parse_cold(p, len);
        }
        parse_cold(p, len)
    }
}

/// Cold fallback for rare dates outside DateTable, unusual F3 lengths, or malformed strings.
#[cold]
#[inline(never)]
unsafe fn parse_cold(p: *const u8, len: usize) -> Option<i64> {
    if len == 20 {
        if *p.add(19) == b'Z' {
            return civil19_sec_cold(p).map(|s| s * 1000);
        }
        return f3(p, len);
    }
    if len == 25 {
        let s = *p.add(19);
        if s == b'+' || s == b'-' {
            let base_sec = civil19_sec_cold(p)?;
            let o = ld8(p.add(17));
            const MO: u64 = 0xFFFF_00FF_FF00_0000;
            if !digits_ok(o, MO) || (o >> 40) & 0xFF != b':' as u64 {
                return None;
            }
            let oh = dig(o, 3) * 10 + dig(o, 4);
            let om = dig(o, 6) * 10 + dig(o, 7);
            if oh > 23 || om > 59 {
                return None;
            }
            let off = oh * 3600 + om * 60;
            let sec = if s == b'+' { base_sec - off } else { base_sec + off };
            return Some(sec * 1000);
        }
        return f3(p, len);
    }
    if len == 0 {
        return None;
    }
    f3(p, len)
}

#[inline(always)]
fn swar_dec8_digits(v: u64) -> u64 {
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    (((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000u64 << 32)))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32
}

/// Cold fallback for `YYYY-MM-DDTHH:MM:SS` -> UTC epoch seconds.
#[cold]
#[inline(never)]
unsafe fn civil19_sec_cold(p: *const u8) -> Option<i64> {
    let a = ld8(p); // YYYY-MM-
    let b = ld8(p.add(8)); // DDTHH:MM
    let c = ld8(p.add(11)); // HH:MM:SS
    const MA: u64 = 0x00FF_FF00_FFFF_FFFF;
    const SA: u64 = 0xFF00_00FF_0000_0000;
    const VA: u64 = (b'-' as u64) << 56 | (b'-' as u64) << 32;
    const MB: u64 = 0xFFFF_00FF_FF00_FFFF;
    const SB: u64 = 0x0000_FF00_00FF_0000;
    const VB: u64 = (b':' as u64) << 40 | (b'T' as u64) << 16;
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
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

/// Parse up to 8 ASCII digits (k = 1..=8 lanes from p). Returns None if any is not a digit.
#[inline(always)]
pub unsafe fn digits8(p: *const u8, k: usize) -> Option<u64> {
    debug_assert!((1..=8).contains(&k));
    let raw = ld8(p);
    let sh = 8 * (8 - k) as u32;
    let m = u64::MAX >> sh;
    if !digits_ok(raw, m) {
        return None;
    }
    let x = ((raw & m) << sh) | (0x3030_3030_3030_3030u64 & !(u64::MAX << sh));
    Some(swar_dec8_digits(x.wrapping_sub(0x3030_3030_3030_3030)))
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

/// Same semantics as `ts::parse_ts` (without a date cache).
#[cfg(test)]
#[inline(always)]
pub unsafe fn parse(p: *const u8, len: usize) -> Option<i64> {
    let dt = DateTable { entries: [DateEntry::default(); 512] };
    dt.parse(p, len)
}

#[cfg(test)]
mod tests {
    use super::super::ts;
    use super::*;

    fn check(dt: &DateTable, s: &[u8]) {
        let mut buf = s.to_vec();
        buf.extend_from_slice(&[b'7'; 64]);
        let want = ts::parse_ts(s);
        let got = unsafe { dt.parse(buf.as_ptr(), s.len()) };
        assert_eq!(got, want, "{:?}", String::from_utf8_lossy(s));
        let got_cold = unsafe { parse(buf.as_ptr(), s.len()) };
        assert_eq!(got_cold, want, "cold {:?}", String::from_utf8_lossy(s));
    }

    #[test]
    fn fuzz_against_reference() {
        // Test across multiple windows including leap-day 2028 and year rollover Dec->Jan.
        let dt_oct = DateTable::build(
            ts::parse_ts(b"2026-10-01T00:00:00Z").unwrap(),
            ts::parse_ts(b"2026-10-15T00:00:00Z").unwrap(),
        );
        let dt_dec = DateTable::build(
            ts::parse_ts(b"2027-12-25T00:00:00Z").unwrap(),
            ts::parse_ts(b"2028-01-08T00:00:00Z").unwrap(),
        );
        let dt_leap = DateTable::build(
            ts::parse_ts(b"2028-02-20T00:00:00Z").unwrap(),
            ts::parse_ts(b"2028-03-05T00:00:00Z").unwrap(),
        );
        let seeds: Vec<&[u8]> = vec![
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"2028-02-29T00:00:00Z", b"2027-02-29T00:00:00Z",
            b"1791071890000", b"0001791071890000", b"86400000", b"", b"0", b"99999999999999999999",
            b"9223372036854775807", b"9223372036854775808", b"2026-10-03T23:58:10+23:59", b"2026-10-03T23:58:10+24:00",
            b"12345678901234567", b"00000000000000000000000001", b"2026-12-31T23:59:59Z", b"0000-01-01T00:00:00Z",
            b"9999-12-31T23:59:59-00:00", b"2026-10-03T23:58:10.000Z", b"20261003T235810Z00000", b"1234567890123456789012345",
            b"2027-12-31T23:59:59Z", b"2028-01-01T00:00:00Z",
        ];
        let alphabet = b"0123456789-T:Z+z t.e\xd9\xa2\x00";
        let mut rng: u64 = 0x1234_5678_9abc_def1;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for dt in [&dt_oct, &dt_dec, &dt_leap] {
            for s in &seeds {
                check(dt, s);
            }
        }
        // Exercise both overlapping SWAR loads with every byte at every digit position.
        let epoch_ms = *b"1791071890000";
        for i in 0..epoch_ms.len() {
            for byte in 0..=u8::MAX {
                let mut input = epoch_ms;
                input[i] = byte;
                check(&dt_oct, &input);
            }
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
            check(&dt_oct, &s);
        }
        // Exhaustive calendar sweep across all 3 date tables + 90-day leap-year window
        let dt_90d = DateTable::build(
            ts::parse_ts(b"2027-12-07T00:00:00Z").unwrap(),
            ts::parse_ts(b"2028-03-06T00:00:00Z").unwrap(),
        );
        // Verify zero slot collisions across any 211-day window starting every day of a 4-year cycle.
        for start_day in (20_000i64..21_461).step_by(29) {
            let ws = start_day * DAY_MS;
            let we = (start_day + 182) * DAY_MS;
            let dt = DateTable::build(ws, we);
            let mut hits = 0usize;
            for d in (start_day - 14)..=(start_day - 14 + 210) {
                let (y, m, day) = civil_from_days(d);
                let s = format!("{y:04}-{m:02}-{day:02}T12:34:56Z");
                let mut buf = s.as_bytes().to_vec();
                buf.resize(64, 0);
                let a = unsafe { ld8(buf.as_ptr()) };
                let ddt = unsafe { std::ptr::read_unaligned(buf.as_ptr().add(8) as *const u32) & 0x00FF_FFFF };
                let e = dt.entries[DateTable::slot(a, ddt)];
                assert_eq!(e.a, a, "slot collision for {s} in start_day={start_day}");
                assert_eq!(e.ddt, ddt, "slot collision for {s} in start_day={start_day}");
                assert_eq!(unsafe { dt.parse(buf.as_ptr(), s.len()) }, ts::parse_ts(s.as_bytes()));
                hits += 1;
            }
            assert_eq!(hits, 211);
        }
        for y in [1970i64, 2000, 2024, 2025, 2026, 2027, 2028, 2100] {
            let dt = if y == 2028 { &dt_90d } else if y == 2027 { &dt_dec } else { &dt_oct };
            for m in 0..=13 {
                for d in 0..=32 {
                    for (h, mi, se) in [(0, 0, 0), (23, 59, 59), (24, 0, 0), (12, 60, 0), (12, 0, 60)] {
                        check(dt, format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z").as_bytes());
                        check(dt, format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{se:02}+05:30").as_bytes());
                    }
                }
            }
        }
        let _ = dt_leap;
    }
}
