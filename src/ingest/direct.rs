//! Row parsers for the compact single-pass sinks. Fixed-width loads may see
//! bytes beyond the row, so each accepted field proves where its delimiter is.

use super::{
    fkey, flen, key_need, parse_click, parse_event, scan, ts, tsfast, Flag, ParseRows, RecC,
    INVALID_CAMP, INVALID_CC,
};
use std::arch::x86_64::*;

#[inline(always)]
unsafe fn ld8(p: *const u8) -> u64 {
    p.cast::<u64>().read_unaligned()
}

/// Locate the value and single-load `u16` closing delimiter mask/expected value.
/// For unquoted fields: `(w & 0x00FF) == DELIM`.
/// For quoted fields:   `(w & 0xFFFF) == [b'"', DELIM]`.
#[inline(always)]
unsafe fn open_delim<const DELIM: u8>(p: *const u8) -> (*const u8, u16, u16, usize) {
    let quoted = (*p == b'"') as usize;
    let mask = if quoted == 0 { 0x00FFu16 } else { 0xFFFFu16 };
    let expect = if quoted == 0 { DELIM as u16 } else { u16::from_le_bytes([b'"', DELIM]) };
    (p.add(quoted), mask, expect, quoted)
}

#[inline(always)]
unsafe fn ends_delim(p: *const u8, len: usize, mask: u16, expect: u16) -> bool {
    (p.add(len).cast::<u16>().read_unaligned() & mask) == expect
}

#[inline(always)]
unsafe fn open(p: *const u8) -> (*const u8, u16, u16, usize) {
    open_delim::<b','>(p)
}

#[inline(always)]
unsafe fn ends(p: *const u8, len: usize, mask: u16, expect: u16) -> bool {
    ends_delim(p, len, mask, expect)
}

/// Branchless 8/9-byte ASCII ID parser terminated by `DELIM` (`,` or `\n`).
/// Eliminates the 93%/7% length branch mispredict via a single `u32` load at `p + 8`.
#[inline(always)]
unsafe fn id_delim<const DELIM: u8>(p: *const u8) -> Option<(u64, *const u8)> {
    let (p, mask, expect, quoted) = open_delim::<DELIM>(p);
    let first = *p;
    let raw_tail = ld8(p.add(1));
    let end4 = p.add(8).cast::<u32>().read_unaligned();
    let is8 = ((end4 as u16) & mask) == expect;
    let is9 = (((end4 >> 8) as u16) & mask) == expect;
    if (u8::from(is8) | u8::from(is9)) == 0 {
        return None;
    }
    let tail = if is8 { raw_tail & 0x00FF_FFFF_FFFF_FFFF } else { raw_tail };
    // Accept lanes in 0x30..=0x7f in one test. The first smaller lane
    // sets its high bit on subtraction, even with cross-lane borrows.
    // This rejects ',', '"', '\n', and NUL inside the field.
    let filled = if is8 { tail | 0x3000_0000_0000_0000 } else { tail };
    if (first.wrapping_sub(0x30) >= 0x50)
        | (((filled.wrapping_sub(0x3030_3030_3030_3030) | filled) & 0x8080_8080_8080_8080) != 0)
    {
        return None;
    }
    let len = if is8 { 8 } else { 9 };
    let key = (u64::from(first) << 56) | _pext_u64(tail.swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F);
    Some((key, p.add(len + 1 + quoted)))
}

#[inline(always)]
unsafe fn id(p: *const u8) -> Option<(u64, *const u8)> {
    id_delim::<b','>(p)
}

/// Fast direct parser for two-column link CSVs (`user_a,user_b\n`).
/// Falls back per row to `scan::row_fields::<2>` + `fkey`.
#[inline]
pub(super) fn links(
    buf: &[u8],
    lo: usize,
    hi: usize,
    flag: &Flag,
    mut emit: impl FnMut(u64, u64),
) {
    if lo >= hi {
        return;
    }
    assert!(hi < u32::MAX as usize && buf.len() >= hi + scan::PAD && buf[hi - 1] == b'\n');
    let base = buf.as_ptr();
    let mut p = unsafe { base.add(lo) };
    let end = unsafe { base.add(hi) };
    while p < end {
        let fast = unsafe {
            id_delim::<b','>(p).and_then(|(a, next)| {
                id_delim::<b'\n'>(next).map(|(b, eol)| (a, b, eol))
            })
        };
        if let Some((a, b, next)) = fast {
            if a != b {
                emit(a, b);
            }
            p = next;
        } else {
            let start = unsafe { p.offset_from(base) as usize };
            let next = row_fields::<2>(buf, start, hi, |f| {
                if flen(f[0]) == 0 || flen(f[1]) == 0 {
                    return;
                }
                match (fkey::<u64>(buf, f[0]), fkey::<u64>(buf, f[1])) {
                    (Some(a), Some(b)) => {
                        if a != b {
                            emit(a, b);
                        }
                    }
                    (None, _) => flag.set(key_need(f[0])),
                    (_, None) => flag.set(key_need(f[1])),
                }
            });
            p = unsafe { base.add(next) };
        }
    }
}

struct Tail {
    comma: usize,
    lf: usize,
    quotes: u32,
}

#[inline(always)]
unsafe fn tail(p: *const u8) -> Option<Tail> {
    let bytes = _mm256_loadu_si256(p.cast::<__m256i>());
    let lf = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'\n' as i8))) as u32)
        .trailing_zeros();
    if lf == 32 {
        return None;
    }
    let mask = _bzhi_u32(u32::MAX, lf);
    let commas =
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b',' as i8))) as u32 & mask;
    if !commas.is_power_of_two() {
        return None;
    }
    let quotes =
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8))) as u32 & mask;
    Some(Tail {
        comma: commas.trailing_zeros() as usize,
        lf: lf as usize,
        quotes,
    })
}

impl Tail {
    #[inline(always)]
    fn wrapping(&self) -> Option<(usize, usize)> {
        let first = self.quotes & 1;
        let last = (self.quotes >> (self.comma + 1)) & 1;
        let expected = first
            | (first << (self.comma.wrapping_sub(1) & 31))
            | (last << (self.comma + 1))
            | (last << (self.lf - 1));
        if (self.quotes != expected)
            | (self.comma < 2 * first as usize)
            | (self.lf - self.comma - 1 < 2 * last as usize)
        {
            return None;
        }
        Some((first as usize, last as usize))
    }
}

pub(super) trait EventOutput {
    fn valid(&mut self, eid: u64, ingest: u32, user: u64, ts: u32, cc: u32);
    fn invalid(&mut self, eid: u64, ingest: u32);
}

/// Single-load check for `Z,` (unquoted) or `Z",` (quoted) at `p + 19`.
#[inline(always)]
unsafe fn open_ingest_f1(raw: *const u8) -> Option<(*const u8, *const u8)> {
    let iq = usize::from(*raw == b'"');
    let p = raw.add(iq);
    let w = p.add(19).cast::<u32>().read_unaligned();
    let imask = if iq == 0 { 0x0000_FFFFu32 } else { 0x00FF_FFFFu32 };
    let iexp = if iq == 0 {
        u32::from_le_bytes(*b"Z,\0\0")
    } else {
        u32::from_le_bytes(*b"Z\",\0")
    };
    if (w & imask) != iexp {
        return None;
    }
    Some((p, p.add(21 + iq)))
}

/// On success, every field and the terminating LF have been checked before
/// emitting. At the start of each field at least PAD bytes remain readable.
#[inline(always)]
unsafe fn event(
    rows: &ParseRows<'_, u64>,
    p: *const u8,
    emit: &mut impl EventOutput,
) -> Option<*const u8> {
    let (eid, p) = id(p)?;
    let (user, p) = id(p)?;
    let (p, mask, expect, q) = open(p);
    let (ts_rel, ingest, valid_time, after_ingest) = if ends(p, 13, mask, expect) {
        let (ingest_p, after) = open_ingest_f1(p.add(14 + q))?;
        let t = rows.dt.parse_f3_13(p)?;
        let ing = rows.dt.parse_f1_20_sec(ingest_p)?;
        if ing as u64 >= u32::MAX as u64 - 1 {
            return None;
        }
        let ok = (t >= rows.ws) & (t < rows.we) & (ing * 1000 - t <= ts::DAY_MS);
        ((t - rows.ws) as u32, ing, ok, after)
    } else {
        let c19 = *p.add(19);
        let (ts_len, delta_sec) = if c19 == b'Z' {
            if !ends(p, 20, mask, expect) {
                return None;
            }
            (20usize, 0i64)
        } else if ((c19 == b'+') | (c19 == b'-')) & ends(p, 25, mask, expect) {
            (25usize, tsfast::DateTable::parse_tz_delta_sec(p, c19)?)
        } else {
            return None;
        };
        let (ingest_p, after) = open_ingest_f1(p.add(ts_len + 1 + q))?;
        let (raw_ts_sec, ing) = rows.dt.parse_f1_pair_sec(p, ingest_p)?;
        let ts_sec = raw_ts_sec + delta_sec;
        if ing as u64 >= u32::MAX as u64 - 1 {
            return None;
        }
        let (ts_rel, ok) = if rows.ws_whole_sec {
            let ok = (ts_sec >= rows.ws_sec) & (ts_sec < rows.we_sec) & (ing - ts_sec <= 86_400);
            (((ts_sec - rows.ws_sec) as u32).wrapping_mul(1000), ok)
        } else {
            let t = ts_sec * 1000;
            let ok = (t >= rows.ws) & (t < rows.we) & (ing * 1000 - t <= ts::DAY_MS);
            ((t - rows.ws) as u32, ok)
        };
        (ts_rel, ing, ok, after)
    };
    let (p, emask, eexpect, eq) = open(after_ingest);
    let ew = ld8(p);
    let word = ew as u32;
    if ((word == u32::from_le_bytes(*b"view")) | (word == u32::from_le_bytes(*b"cart")))
        & ((((ew >> 32) as u16) & emask) == eexpect)
    {
        let p = p.add(5 + eq);
        let tail = tail(p)?;
        let (qp, _) = tail.wrapping()?;
        if valid_time & (tail.comma != 2 * qp) {
            emit.valid(eid, (ingest + 1) as u32, user, ts_rel, 0);
        } else {
            emit.invalid(eid, (ingest + 1) as u32);
        }
        return Some(p.add(tail.lf + 1));
    }
    if (ew == u64::from_le_bytes(*b"purchase")) & ends(p, 8, emask, eexpect) {
        let p = p.add(9 + eq);
        let tail = tail(p)?;
        let (qp, qc) = tail.wrapping()?;
        let product_len = tail.comma - 2 * qp;
        let mut valid = valid_time & (product_len != 0);
        let cc = if valid {
            let len = tail.lf - tail.comma - 1 - 2 * qc;
            if (1..=8).contains(&len) {
                if let Some(cents) = tsfast::digits8(p.add(tail.comma + 1 + qc), len).map(|v| v as u32) {
                    if (1..=10_000_000).contains(&cents) {
                        (rows.lookups.prod.get(p.add(qp), product_len) << 24) | cents
                    } else {
                        valid = false;
                        0
                    }
                } else {
                    valid = false;
                    0
                }
            } else if len == 0 {
                valid = false;
                0
            } else {
                // > 8 digits could be leading-zero valid price like "000000000001"; let slow path handle it.
                return None;
            }
        } else {
            0
        };
        if valid {
            emit.valid(eid, (ingest + 1) as u32, user, ts_rel, cc);
        } else {
            emit.invalid(eid, (ingest + 1) as u32);
        }
        return Some(p.add(tail.lf + 1));
    }
    None
}

#[inline(always)]
unsafe fn click(
    rows: &ParseRows<'_, u64>,
    p: *const u8,
    emit: &mut impl FnMut(RecC<u64>),
) -> Option<*const u8> {
    let (cid, p) = id(p)?;
    let (person, p) = id(p)?;
    // Parse campaign_id up to ',' within 16 bytes, then parse ts directly up to '\n'.
    // Avoids overflowing a 32-byte tail window on F2 / quoted F1 timestamps.
    let (cp, _, _, cq) = open(p);
    let cv = _mm_loadu_si128(cp.cast::<__m128i>());
    let c_commas = _mm_movemask_epi8(_mm_cmpeq_epi8(cv, _mm_set1_epi8(b',' as i8))) as u32;
    let c_quotes = _mm_movemask_epi8(_mm_cmpeq_epi8(cv, _mm_set1_epi8(b'"' as i8))) as u32;
    let c_nls = _mm_movemask_epi8(_mm_cmpeq_epi8(cv, _mm_set1_epi8(b'\n' as i8))) as u32;
    let cq_mask = (cq as u32).wrapping_neg();
    let delim_mask = (c_quotes & cq_mask) | (c_commas & !cq_mask);
    if delim_mask == 0 {
        return None;
    }
    let clen = delim_mask.trailing_zeros() as usize;
    let inside = _bzhi_u32(u32::MAX, clen as u32);
    if (((c_commas | c_quotes | c_nls) & inside) != 0)
        | ((cq != 0) & (*cp.add(clen + 1) != b','))
    {
        return None;
    }
    let after_camp = cp.add(clen + 1 + cq);
    let campaign = rows.lookups.camp.get(cp, clen);
    if campaign == INVALID_CAMP {
        let tv = _mm256_loadu_si256(after_camp.cast::<__m256i>());
        let lf = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(tv, _mm256_set1_epi8(b'\n' as i8))) as u32)
            .trailing_zeros() as usize;
        if lf < 32 {
            // Ensure no extra comma inside the dropped timestamp field (could be malformed quotes).
            let m = _bzhi_u32(u32::MAX, lf as u32);
            let qc = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(tv, _mm256_set1_epi8(b'"' as i8))) as u32) & m;
            let cc = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(tv, _mm256_set1_epi8(b',' as i8))) as u32) & m;
            if (qc == 0) & (cc == 0) {
                return Some(after_camp.add(lf + 1));
            }
        }
        return None;
    }
    let (tp, tmask, texpect, tq) = open_delim::<b'\n'>(after_camp);
    let (t, end) = if (*tp.add(19) == b'Z') & ends_delim(tp, 20, tmask, texpect) {
        (rows.dt.parse_f1_20_sec(tp)? * 1000, tp.add(21 + tq))
    } else if ends_delim(tp, 13, tmask, texpect) {
        (rows.dt.parse_f3_13(tp)?, tp.add(14 + tq))
    } else if ends_delim(tp, 25, tmask, texpect) {
        (rows.dt.parse_f2_25(tp)?, tp.add(26 + tq))
    } else {
        return None;
    };
    let base = rows.clicks_base();
    if (t >= base) & (t < rows.we) {
        emit(RecC {
            person,
            cid,
            ts: (t - base) as u32,
            campaign,
        });
    }
    Some(end)
}

fn row_fields<const N: usize>(
    buf: &[u8],
    lo: usize,
    hi: usize,
    f: impl FnOnce(&[(u32, u32); N]),
) -> usize {
    let lf = lo + scan::memchr(b'\n', &buf[lo..hi]).expect("LF-terminated chunk");
    if let Some(fields) = scan::row_fields(buf, lo, lf) {
        f(&fields);
    }
    lf + 1
}

#[cold]
#[inline(never)]
fn event_slow(
    rows: &ParseRows<'_, u64>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    emit: &mut impl EventOutput,
) -> usize {
    row_fields::<7>(buf, lo, hi, |f| {
        if let Err(need) = parse_event::<false, u64>(
            rows.dt,
            buf,
            f,
            rows.ws,
            rows.we,
            &rows.lookups.prod,
            |eid, _, ingest, user, ts, cc| {
                if cc == INVALID_CC {
                    emit.invalid(eid, ingest);
                } else {
                    emit.valid(eid, ingest, user, ts, cc);
                }
            },
        ) {
            rows.flag.set(need);
        }
    })
}

#[cold]
#[inline(never)]
fn click_slow(
    rows: &ParseRows<'_, u64>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    emit: &mut impl FnMut(RecC<u64>),
) -> usize {
    row_fields::<4>(buf, lo, hi, |f| match parse_click(rows, buf, f) {
        Ok(Some(rec)) => emit(rec),
        Ok(None) => {}
        Err(need) => rows.flag.set(need),
    })
}

#[inline]
pub(super) fn events(
    rows: &ParseRows<'_, u64>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    emit: &mut impl EventOutput,
) {
    if lo >= hi {
        return;
    }
    assert!(hi < u32::MAX as usize && buf.len() >= hi + scan::PAD && buf[hi - 1] == b'\n');
    let base = buf.as_ptr();
    let mut p = unsafe { base.add(lo) };
    let end = unsafe { base.add(hi) };
    while p < end {
        // SAFETY: p starts at a row boundary, hi follows an LF, and the
        // buffer has PAD bytes beyond hi for the fixed-width field probes.
        p = unsafe {
            match event(rows, p, emit) {
                Some(next) => next,
                None => base.add(event_slow(
                    rows,
                    buf,
                    p.offset_from(base) as usize,
                    hi,
                    emit,
                )),
            }
        };
    }
}

#[inline]
pub(super) fn clicks(
    rows: &ParseRows<'_, u64>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    mut emit: impl FnMut(RecC<u64>),
) {
    if lo >= hi {
        return;
    }
    assert!(hi < u32::MAX as usize && buf.len() >= hi + scan::PAD && buf[hi - 1] == b'\n');
    let base = buf.as_ptr();
    let mut p = unsafe { base.add(lo) };
    let end = unsafe { base.add(hi) };
    while p < end {
        // SAFETY: see events. The tail mask stops at the first LF.
        p = unsafe {
            match click(rows, p, &mut emit) {
                Some(next) => next,
                None => base.add(click_slow(
                    rows,
                    buf,
                    p.offset_from(base) as usize,
                    hi,
                    &mut emit,
                )),
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::fast::{Flag, Lookups, PartitionPlan, INVALID_CC};
    use crate::ingest::key::IdLookup;
    use crate::ingest::keys::Key;
    use crate::ingest::tsfast::DateTable;
    use std::sync::atomic::AtomicU8;

    #[test]
    fn direct_rows_match_reference() {
        let ws = ts::parse_ts(b"2026-10-01T00:00:00Z").unwrap();
        let we = ws + 14 * ts::DAY_MS;
        let dt = DateTable::build(ws, we);
        let lookups = Lookups {
            categories: Vec::new(),
            campaigns: Vec::new(),
            prod: IdLookup::build([(b"p1".as_slice(), 3)].into_iter(), 7),
            camp: IdLookup::build([(b"c_42".as_slice(), 11)].into_iter(), INVALID_CAMP),
        };
        let plan = PartitionPlan {
            parse_threads: 1,
            p1_bits: 8,
            p2_bits: 8,
            p1: 256,
            p2: 256,
            cap_ev: 24,
            cap_ck: 24,
        };
        let flag = Flag(AtomicU8::new(0));
        let rows = ParseRows {
            dt: &dt,
            ws,
            we,
            ws_sec: ws / 1000,
            we_sec: we / 1000,
            ws_whole_sec: ws % 1000 == 0 && we % 1000 == 0,
            lookups: &lookups,
            plan: &plan,
            flag: &flag,
            links: None,
        };
        let mut seed = 0xa689_ab51_1f59_032d_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as usize
        };
        let valid_event =
            b"event0011,person01,2026-10-03T23:58:10Z,2026-10-03T23:59:10Z,purchase,p1,300\n";
        let valid_click = b"click0011,person01,c_42,2026-10-03T23:58:10Z\n";
        let mut buf = Vec::new();
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        let mut valid = [0, 0];
        for is_event in [true, false] {
            let nfields = if is_event { 7 } else { 4 };
            // Enumerate every combination of unquoted, wrapped, stray opening,
            // stray closing, and interior quotes, then mutate ordinary rows.
            for i in 0..(5usize.pow(nfields as u32) + 60_000) {
                let combinatorial = i < 5usize.pow(nfields as u32);
                let fields = if is_event {
                    vec![
                        b"event0011".as_slice(),
                        b"person01",
                        [
                            b"2026-10-03T23:58:10Z".as_slice(),
                            b"1791071890000",
                            b"2026-10-03T19:58:10-04:00",
                            b"2026-10-03T23:58:10+99:00",
                            b"0",
                            b"9223372036854775808",
                        ][i % 6],
                        [
                            b"2026-10-03T23:59:10Z".as_slice(),
                            b"2026-10-03T23:59:10Z",
                            b"bad",
                            b"1791071890001",
                        ][i % 4],
                        [b"purchase".as_slice(), b"view", b"cart", b"other"][i % 4],
                        b"p1",
                        [
                            b"300".as_slice(),
                            b"000000000001",
                            b"0",
                            b"10000001",
                            b"10000000",
                            b"-1",
                            b"",
                        ][i % 7],
                    ]
                } else {
                    vec![
                        b"click0011".as_slice(),
                        b"person01",
                        b"c_42",
                        [
                            b"2026-10-03T23:58:10Z".as_slice(),
                            b"1791071890000",
                            b"2026-10-03T19:58:10-04:00",
                            b"9999999999999999999999999999",
                        ][i % 4],
                    ]
                };
                buf.clear();
                buf.extend_from_slice(b"header\n");
                let lo = buf.len();
                let mut quote_mask = i;
                for (index, field) in fields.into_iter().enumerate() {
                    if index != 0 {
                        let separator = if combinatorial || next() % 12 != 0 {
                            b','
                        } else {
                            b'\n'
                        };
                        buf.push(separator);
                    }
                    let mut value = field.to_vec();
                    if !combinatorial && i % 8 != 0 && next() % 3 == 0 {
                        match next() % 6 {
                            0 => value = vec![b'a'; next() % 17],
                            1 => value.clear(),
                            2 => value = "café".as_bytes().to_vec(),
                            3 => value = b" abc.-_\0".to_vec(),
                            4 => value.insert(next() % (value.len() + 1), next() as u8),
                            _ => value = b"\"".to_vec(),
                        }
                    }
                    let quote = if combinatorial {
                        quote_mask % 5
                    } else if i % 8 == 0 {
                        next() % 2
                    } else {
                        [0, 0, 0, 0, 1, 1, 1, 2, 3, 4][next() % 10]
                    };
                    quote_mask /= 5;
                    if matches!(quote, 1 | 2) {
                        buf.push(b'"');
                    }
                    if quote == 4 {
                        value.insert(value.len() / 2, b'"');
                    }
                    buf.extend_from_slice(&value);
                    if matches!(quote, 1 | 3) {
                        buf.push(b'"');
                    }
                }
                if !combinatorial && next() % 16 == 0 {
                    buf.extend_from_slice(b",extra");
                }
                buf.push(b'\n');
                // A valid following row must still be emitted after any fallback.
                buf.extend_from_slice(if is_event { valid_event } else { valid_click });
                let hi = buf.len();
                buf.resize(hi + scan::PAD, 0);
                expected.clear();
                let reference_flag = Flag(AtomicU8::new(0));
                let reference = ParseRows {
                    flag: &reference_flag,
                    ..rows
                };
                if is_event {
                    scan::scan_ref(&buf, lo, hi, |b, f| {
                        let result = parse_event::<false, u64>(
                            &dt,
                            b,
                            f,
                            ws,
                            we,
                            &lookups.prod,
                            |id, hash, ingest, user, ts, cc| {
                                expected.push((id, hash, ingest, user, ts, cc))
                            },
                        );
                        if let Err(need) = result {
                            reference_flag.set(need);
                        }
                    });
                    valid[0] += expected.iter().filter(|r| r.5 != INVALID_CC).count() - 1;
                } else {
                    scan::scan_ref(&buf, lo, hi, |b, f| match parse_click(&reference, b, f) {
                        Ok(Some(r)) => expected.push((r.cid, 0, 0, r.person, r.ts, r.campaign)),
                        Ok(None) => {}
                        Err(need) => reference_flag.set(need),
                    });
                    valid[1] += expected.len() - 1;
                }
                flag.0.store(0, std::sync::atomic::Ordering::Relaxed);
                actual.clear();
                if is_event {
                    struct TestOutput<'a>(&'a mut Vec<(u64, u64, u32, u64, u32, u32)>);
                    impl EventOutput for TestOutput<'_> {
                        fn valid(&mut self, eid: u64, ingest: u32, user: u64, ts: u32, cc: u32) {
                            self.0.push((eid, eid.h(), ingest, user, ts, cc));
                        }
                        fn invalid(&mut self, eid: u64, ingest: u32) {
                            self.0.push((eid, eid.h(), ingest, 0, 0, INVALID_CC));
                        }
                    }
                    events(&rows, &buf, lo, hi, &mut TestOutput(&mut actual));
                } else {
                    clicks(&rows, &buf, lo, hi, |r| {
                        actual.push((r.cid, 0, 0, r.person, r.ts, r.campaign))
                    });
                }
                assert_eq!(actual, expected, "row {i}: {:?}", &buf[lo..hi]);
                assert_eq!(
                    flag.get(),
                    reference_flag.get(),
                    "row {i}: {:?}",
                    &buf[lo..hi]
                );
            }
        }
        assert!(
            valid.into_iter().all(|n| n > 1_000),
            "valid rows: {valid:?}"
        );
    }
}
