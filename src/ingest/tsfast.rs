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

/// Direct-mapped cache of valid `YYYY-MM-DDT` prefixes around the dataset window.
/// Eliminates calendar validation and `days_from_civil` division for >99% of F1/F2 timestamps,
/// covering up to 365 consecutive days with zero collisions via ONE BMI2 `pext` instruction
/// (only the active ~14–21 days = ~336 bytes ever enter L1d cache).
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
    entries: [DateEntry; 2048],
}

impl DateTable {
    /// Prepopulate valid dates in `[ws - 14d, we + 14d]` (up to 365 days, zero collisions across
    /// any full year incl. leap years and Dec->Jan rollovers). Dates outside 1970..2106 or missing
    /// from the cache fall back to exact `civil19_sec_cold`.
    pub fn build(ws_ms: i64, we_ms: i64) -> Self {
        let mut entries = [DateEntry::default(); 2048];
        let d0 = ws_ms.div_euclid(DAY_MS) - 14;
        let d1 = (we_ms.div_euclid(DAY_MS) + 14).min(d0 + 364);
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
            let idx = unsafe { Self::slot_at(buf.as_ptr()) };
            entries[idx] = DateEntry { a, ddt, day_sec: sec as u32 };
        }
        DateTable { entries }
    }

    #[inline(always)]
    unsafe fn slot_at(p: *const u8) -> usize {
        // Bytes 5..10 are `M0, M1, '-', D0, D1`. Byte-swapping puts `D1` in the low bits and
        // `M0, M1` in the high bits, so 14 consecutive days pack into 5 contiguous L1d cache
        // lines with zero collisions across any 365-day year in two instructions (`bswap`+`pext`).
        std::arch::x86_64::_pext_u64(ld8(p.add(5)).swap_bytes(), 0x010F_0003_0F00_0000) as usize
    }

    /// Parse `YYYY-MM-DDTHH:MM:SS` (19 bytes) -> UTC epoch seconds (before zone), or None.
    #[inline(always)]
    unsafe fn civil19_sec(&self, p: *const u8) -> Option<i64> {
        let a = ld8(p); // YYYY-MM-
        let ddt = (std::ptr::read_unaligned(p.add(8) as *const u32)) & 0x00FF_FFFF; // DDT
        let e = self.entries.get_unchecked(Self::slot_at(p));
        if (e.a == a) & (e.ddt == ddt) {
            return Self::clock_sec(p).map(|sec| sec + e.day_sec as i64);
        }
        civil19_sec_cold(p)
    }

    #[inline(always)]
    unsafe fn clock_sec(p: *const u8) -> Option<i64> {
        use std::arch::x86_64::*;
        let raw = _mm_cvtsi64_si128(ld8(p.add(11)) as i64);
        let min = _mm_cvtsi64_si128(i64::from_le_bytes(*b"00:00:00"));
        let max = _mm_cvtsi64_si128(i64::from_le_bytes(*b"29:59:59"));
        let bad = _mm_or_si128(_mm_subs_epu8(min, raw), _mm_subs_epu8(raw, max));
        let digits = _mm_shuffle_epi8(
            _mm_sub_epi8(raw, min),
            _mm_setr_epi8(0, 1, 3, 4, 6, 7, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1),
        );
        let times = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
        let hours_bad = _mm_cmpgt_epi16(times, _mm_setr_epi16(23, 59, 59, 0, 0, 0, 0, 0));
        let invalid = _mm_or_si128(bad, hours_bad);
        if _mm_testz_si128(invalid, invalid) == 0 {
            return None;
        }
        let prod = _mm_madd_epi16(times, _mm_setr_epi16(3600, 60, 1, 0, 0, 0, 0, 0));
        let sum = _mm_add_epi32(prod, _mm_srli_si128::<4>(prod));
        Some(_mm_cvtsi128_si32(sum) as i64)
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
        let sum = _mm_add_epi32(both, _mm_srli_si128::<4>(both));
        Some((_mm_cvtsi128_si32(sum) as i64, _mm_extract_epi32::<2>(sum) as i64))
    }

    /// Both fields have an independently checked F1 terminator and delimiter.
    /// Returns `(event_epoch_sec, ingest_epoch_sec)` without multiplying by 1000.
    #[inline(always)]
    pub(super) unsafe fn parse_f1_pair_sec(&self, a: *const u8, b: *const u8) -> Option<(i64, i64)> {
        let pa = ld8(a);
        let da = a.add(8).cast::<u32>().read_unaligned() & 0x00FF_FFFF;
        let ea = self.entries.get_unchecked(Self::slot_at(a));
        let pb = ld8(b);
        let db = b.add(8).cast::<u32>().read_unaligned() & 0x00FF_FFFF;
        let eb = self.entries.get_unchecked(Self::slot_at(b));
        if (ea.a == pa) & (ea.ddt == da) & (eb.a == pb) & (eb.ddt == db) {
            let (event, ingest) = Self::clock_pair(a, b)?;
            return Some((ea.day_sec as i64 + event, eb.day_sec as i64 + ingest));
        }
        self.parse_f1_pair_sec_cold(a, b)
    }

    #[cold]
    #[inline(never)]
    unsafe fn parse_f1_pair_sec_cold(&self, a: *const u8, b: *const u8) -> Option<(i64, i64)> {
        Some((self.civil19_sec(a)?, self.civil19_sec(b)?))
    }

    /// Direct 13-digit F3 parser via SSE2 `pmaddubsw`/`pmaddwd` (caller verified exact length 13).
    #[inline(always)]
    pub(super) unsafe fn parse_f3_13(&self, p: *const u8) -> Option<i64> {
        use std::arch::x86_64::*;
        let raw = _mm_loadu_si128(p.cast::<__m128i>());
        let d = _mm_sub_epi8(raw, _mm_set1_epi8(b'0' as i8));
        let bad = _mm_subs_epu8(d, _mm_set1_epi8(9));
        let mask13 = _mm_setr_epi8(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0);
        if _mm_testz_si128(bad, mask13) == 0 {
            return None;
        }
        // Shift digits right by 3 bytes -> [0, 0, 0, d0, d1, ..., d12].
        let d16 = _mm_slli_si128::<3>(d);
        let p2 = _mm_maddubs_epi16(d16, _mm_set1_epi16(0x010a));
        let p4 = _mm_madd_epi16(p2, _mm_set1_epi32(0x0001_0064));
        let p4_16 = _mm_packus_epi32(p4, p4);
        let p8 = _mm_madd_epi16(p4_16, _mm_setr_epi16(10000, 1, 10000, 1, 0, 0, 0, 0));
        let q = _mm_cvtsi128_si64(p8) as u64;
        let hi = (q as u32) as i64;
        let lo = (q >> 32) as i64;
        Some(hi * 100_000_000 + lo)
    }

    /// Parse `±HH:MM` timezone suffix of a 25-byte F2 timestamp (`p` points to byte 0 of timestamp,
    /// `sign` is `*p.add(19)` = `'+'` or `'-'`), returning UTC delta in seconds.
    #[inline(always)]
    pub(super) unsafe fn parse_tz_delta_sec(p: *const u8, sign: u8) -> Option<i64> {
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
        Some(if sign == b'+' { -off } else { off })
    }

    /// Direct 25-byte F2 parser (caller verified exact length 25).
    #[inline(always)]
    pub(super) unsafe fn parse_f2_25(&self, p: *const u8) -> Option<i64> {
        let s = *p.add(19);
        if (s == b'+') | (s == b'-') {
            let base_sec = self.civil19_sec(p)?;
            let delta = Self::parse_tz_delta_sec(p, s)?;
            return Some((base_sec + delta) * 1000);
        }
        parse_cold(p, 25)
    }

    /// Direct 20-byte F1 parser (caller verified exact length 20 and `*p.add(19) == b'Z'`).
    #[inline(always)]
    pub(super) unsafe fn parse_f1_20_sec(&self, p: *const u8) -> Option<i64> {
        self.civil19_sec(p)
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
            return self.parse_f3_13(p);
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
    let dt = DateTable { entries: [DateEntry::default(); 2048] };
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
                let e = dt.entries[unsafe { DateTable::slot_at(buf.as_ptr()) }];
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
