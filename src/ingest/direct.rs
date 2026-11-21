//! Row parsers for the compact single-pass sinks. Fixed-width loads may see
//! bytes beyond the row, so each accepted field proves where its delimiter is.

use super::{
    parse_click, parse_event, scan, ts, tsfast, ParseRows, RecC, INVALID_CAMP, INVALID_CC,
};
use std::arch::x86_64::*;

#[inline(always)]
unsafe fn ld8(p: *const u8) -> u64 {
    p.cast::<u64>().read_unaligned()
}

/// Locate the value and the expected closing delimiter of a comma-ended field.
#[inline(always)]
unsafe fn open(p: *const u8) -> (*const u8, u8, usize) {
    let quoted = (*p == b'"') as usize;
    (p.add(quoted), if quoted == 0 { b',' } else { b'"' }, quoted)
}

#[inline(always)]
unsafe fn ends(p: *const u8, len: usize, delim: u8, quoted: usize) -> bool {
    (*p.add(len) == delim) & ((quoted == 0) | (*p.add(len + 1) == b','))
}

#[inline(always)]
unsafe fn id(p: *const u8) -> Option<(u64, *const u8)> {
    let (p, delim, quoted) = open(p);
    let first = *p;
    let mut tail = ld8(p.add(1));
    let len = if ends(p, 9, delim, quoted) {
        9
    } else if ends(p, 8, delim, quoted) {
        tail &= 0x00FF_FFFF_FFFF_FFFF;
        8
    } else {
        return None;
    };
    // Accept lanes in 0x30..=0x7f in one test. The first smaller lane
    // sets its high bit on subtraction, even with cross-lane borrows.
    let filled = tail | if len == 8 { 0x3000_0000_0000_0000 } else { 0 };
    if (first.wrapping_sub(0x30) >= 0x50)
        | (((filled.wrapping_sub(0x3030_3030_3030_3030) | filled) & 0x8080_8080_8080_8080) != 0)
    {
        return None;
    }
    let key = (u64::from(first) << 56) | _pext_u64(tail.swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F);
    Some((key, p.add(len + 1 + quoted)))
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
    let mask = (1u32 << lf) - 1;
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
    let (p, delim, q) = open(p);
    let len = if *p.add(19) == b'Z' && ends(p, 20, delim, q) {
        20
    } else if ends(p, 13, delim, q) {
        13
    } else if ends(p, 25, delim, q) {
        25
    } else {
        return None;
    };
    let (ingest_p, delim, q) = open(p.add(len + 1 + q));
    if *ingest_p.add(19) != b'Z' || !ends(ingest_p, 20, delim, q) {
        return None;
    }
    let (t, ingest) = if len == 20 {
        rows.dt.parse_f1_pair(p, ingest_p)?
    } else {
        (rows.dt.parse(p, len)?, rows.dt.parse_f1_sec(ingest_p, 20)?)
    };
    if (t < rows.ws) | (t >= rows.we) {
        return None;
    }
    if (ingest as u64 >= u32::MAX as u64 - 1) || ingest * 1000 - t > ts::DAY_MS {
        return None;
    }
    let (p, delim, q) = open(ingest_p.add(21 + q));
    let word = p.cast::<u32>().read_unaligned();
    let (purchase, p) = if ((word == u32::from_le_bytes(*b"view"))
        | (word == u32::from_le_bytes(*b"cart")))
        && ends(p, 4, delim, q)
    {
        (false, p.add(5 + q))
    } else if ld8(p) == u64::from_le_bytes(*b"purchase") && ends(p, 8, delim, q) {
        (true, p.add(9 + q))
    } else {
        return None;
    };
    let tail = tail(p)?;
    let (qp, qc) = tail.wrapping()?;
    let product_len = tail.comma - 2 * qp;
    if product_len == 0 {
        return None;
    }
    let cc = if purchase {
        let len = tail.lf - tail.comma - 1 - 2 * qc;
        if !(1..=8).contains(&len) {
            return None;
        }
        let cents = tsfast::digits8(p.add(tail.comma + 1 + qc), len)? as u32;
        if !(1..=10_000_000).contains(&cents) {
            return None;
        }
        (rows.lookups.prod.get(p.add(qp), product_len) << 24) | cents
    } else {
        0
    };
    emit.valid(eid, (ingest + 1) as u32, user, (t - rows.ws) as u32, cc);
    Some(p.add(tail.lf + 1))
}

#[inline(always)]
unsafe fn click(
    rows: &ParseRows<'_, u64>,
    p: *const u8,
    emit: &mut impl FnMut(RecC<u64>),
) -> Option<*const u8> {
    let (cid, p) = id(p)?;
    let (person, p) = id(p)?;
    let tail = tail(p)?;
    let (qc, qt) = tail.wrapping()?;
    let end = p.add(tail.lf + 1);
    let campaign = rows.lookups.camp.get(p.add(qc), tail.comma - 2 * qc);
    if campaign == INVALID_CAMP {
        return Some(end);
    }
    let len = tail.lf - tail.comma - 1 - 2 * qt;
    let base = rows.clicks_base();
    if let Some(t) = rows.dt.parse(p.add(tail.comma + 1 + qt), len) {
        if (t >= base) & (t < rows.we) {
            emit(RecC {
                person,
                cid,
                ts: (t - base) as u32,
                campaign,
            });
        }
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
