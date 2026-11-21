//! Row parsers for the compact single-pass sinks. Fixed-width loads may see
//! bytes beyond the row, so each accepted field proves where its delimiter is.

use super::{scan, ts, tsfast, ParseRows, Result};
use crate::proto_c::packed;

const INVALID_CAMP: u32 = u32::MAX;

#[derive(Clone, Copy, Default)]
pub(super) struct RawClick {
    pub person: u64,
    pub cid: u64,
    pub ts: u32,
    pub campaign: u32,
}
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
unsafe fn id(p: *const u8, prefix: u8) -> Option<(u64, *const u8)> {
    let (p, delim, quoted) = open(p);
    let len = if ends(p, 9, delim, quoted) {
        9
    } else if ends(p, 8, delim, quoted) {
        8
    } else {
        return None;
    };
    let key = packed::encode_short(*p, ld8(p.add(1)), len - 1, prefix)?;
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
    fn unsupported(&mut self, limit: &'static str);
}

/// On success, every field and the terminating LF have been checked before
/// emitting. At the start of each field at least PAD bytes remain readable.
#[inline(always)]
unsafe fn event(rows: &ParseRows, p: *const u8, emit: &mut impl EventOutput) -> Option<*const u8> {
    let (eid, p) = id(p, b'e')?;
    let (user, p) = id(p, b'u')?;
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
    let relative_ingest = rows.ingest_rel(ingest)?;
    if ingest * 1000 - t > ts::DAY_MS {
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
    emit.valid(eid, relative_ingest, user, (t - rows.ws) as u32, cc);
    Some(p.add(tail.lf + 1))
}

#[inline(always)]
unsafe fn click(
    rows: &ParseRows,
    p: *const u8,
    emit: &mut impl FnMut(RawClick),
) -> Option<*const u8> {
    let (cid, p) = id(p, b'k')?;
    let (person, p) = id(p, b'u')?;
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
            emit(RawClick {
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

#[inline]
fn field(b: &[u8], range: (u32, u32)) -> &[u8] {
    &b[range.0 as usize..range.1 as usize]
}

#[inline]
fn timestamp(rows: &ParseRows, b: &[u8]) -> Option<i64> {
    // A parsed field always points into the padded input buffer.
    unsafe { rows.dt.parse(b.as_ptr(), b.len()) }
}

fn parse_event(
    rows: &ParseRows,
    b: &[u8],
    f: &[(u32, u32); 7],
    emit: &mut impl EventOutput,
) -> Result<()> {
    let [eid, usr, time, ingest, kind, prod, price] = f.map(|f| field(b, f));
    if eid.is_empty() {
        return Ok(());
    }
    let eid = packed::encode(eid, b'e').ok_or("event ID exceeds the 43-bit alphabet")?;
    let Some(ims) = timestamp(rows, ingest) else {
        return Err("unparseable ingest timestamp");
    };
    let ingest = rows
        .ingest_rel(ims.div_euclid(1000))
        .filter(|_| ims % 1000 == 0)
        .ok_or("ingest timestamp exceeds the 23-bit whole-second window")?;
    if let Some(t) = timestamp(rows, time).filter(|&t| t >= rows.ws && t < rows.we) {
        if !usr.is_empty() && !prod.is_empty() && ims - t <= ts::DAY_MS {
            let cc = match kind {
                b"view" | b"cart" => Some(0),
                b"purchase" => parse_price(price)
                    .filter(|n| (1..=10_000_000).contains(n))
                    .map(|n| {
                        (unsafe { rows.lookups.prod.get(prod.as_ptr(), prod.len()) } << 24) | n
                    }),
                _ => None,
            };
            if let Some(cc) = cc {
                let usr =
                    packed::encode(usr, b'u').ok_or("event user ID exceeds the 43-bit alphabet")?;
                emit.valid(eid, ingest, usr, (t - rows.ws) as u32, cc);
                return Ok(());
            }
        }
    }
    emit.invalid(eid, ingest);
    Ok(())
}

fn parse_price(bytes: &[u8]) -> Option<u32> {
    let mut price = 0u32;
    for &b in bytes {
        let digit = b.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        price = price.checked_mul(10)?.checked_add(u32::from(digit))?;
    }
    Some(price)
}

fn parse_click(rows: &ParseRows, b: &[u8], f: &[(u32, u32); 4]) -> Result<Option<RawClick>> {
    let [cid, person, campaign, time] = f.map(|f| field(b, f));
    if cid.is_empty() || person.is_empty() {
        return Ok(None);
    }
    let campaign = unsafe { rows.lookups.camp.get(campaign.as_ptr(), campaign.len()) };
    if campaign == INVALID_CAMP {
        return Ok(None);
    }
    let base = rows.clicks_base();
    let Some(t) = timestamp(rows, time).filter(|&t| t >= base && t < rows.we) else {
        return Ok(None);
    };
    let cid = packed::encode(cid, b'k').ok_or("click ID exceeds the 43-bit alphabet")?;
    let person = packed::encode(person, b'u').ok_or("click user ID exceeds the 43-bit alphabet")?;
    Ok(Some(RawClick {
        person,
        cid,
        ts: (t - base) as u32,
        campaign,
    }))
}

#[cold]
#[inline(never)]
fn event_slow(
    rows: &ParseRows,
    buf: &[u8],
    lo: usize,
    hi: usize,
    emit: &mut impl EventOutput,
) -> usize {
    row_fields::<7>(buf, lo, hi, |fields| {
        if let Err(limit) = parse_event(rows, buf, fields, emit) {
            emit.unsupported(limit);
        }
    })
}

#[cold]
#[inline(never)]
fn click_slow(
    rows: &ParseRows,
    buf: &[u8],
    lo: usize,
    hi: usize,
    emit: &mut impl FnMut(RawClick),
    error: &mut Result<()>,
) -> usize {
    row_fields::<4>(buf, lo, hi, |f| match parse_click(rows, buf, f) {
        Ok(Some(rec)) => emit(rec),
        Ok(None) => {}
        Err(limit) => *error = Err(limit),
    })
}

#[inline]
pub(super) fn events(
    rows: &ParseRows,
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
    rows: &ParseRows,
    buf: &[u8],
    lo: usize,
    hi: usize,
    mut emit: impl FnMut(RawClick),
) -> Result<()> {
    if lo >= hi {
        return Ok(());
    }
    assert!(hi < u32::MAX as usize && buf.len() >= hi + scan::PAD && buf[hi - 1] == b'\n');
    let mut error = Ok(());
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
                    &mut error,
                )),
            }
        };
    }
    error
}
