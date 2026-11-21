use std::arch::x86_64::*;

pub const ID_BITS: u32 = 43;
pub const ID_MASK: u64 = (1 << ID_BITS) - 1;
const BASE37_CARDINALITY: u64 = 37u64.pow(8);
const _: () = assert!(BASE37_CARDINALITY < 1 << 42);

const fn subtree(alphabet: u64) -> [u64; 9] {
    let mut counts = [1; 9];
    let mut i = 1;
    while i < counts.len() {
        counts[i] = 1 + alphabet * counts[i - 1];
        i += 1;
    }
    counts
}

const SUB36: [u64; 9] = subtree(36);
const SUB10: [u64; 9] = subtree(10);
const BASE_36_4: u64 = 36u64.pow(4);
const UPPER_EXTENT: u64 = 2 * SUB36[8] - SUB10[8];
const _: () = assert!(UPPER_EXTENT + SUB36[8] - 1 <= ID_MASK);

// Odd multiplication modulo 2^43 and XOR right shifts are reversible.
const H1: u64 = 0xd6e8_feb8_6659_fd93;
const H2: u64 = 0xa5a3_564e_27f8_862d;
pub const LOW_HASH_BITS: u32 = 64 - ID_BITS;
pub const PERSON_BITS: u32 = ID_BITS - 8;
pub const PERSON_MASK: u64 = (1 << PERSON_BITS) - 1;
#[cfg(any(test, feature = "narrow"))]
const TOKEN_BITS: u32 = ID_BITS - 9;
#[cfg(any(test, feature = "narrow"))]
const TOKEN_MASK: u64 = (1 << TOKEN_BITS) - 1;

#[inline(always)]
pub fn hash(key: u64) -> u64 {
    let x = key.wrapping_mul(H1) & ID_MASK;
    let y = (x ^ (x >> 21)).wrapping_mul(H2) & ID_MASK;
    let permuted = y ^ (y >> 22);
    permuted << LOW_HASH_BITS
}

#[inline(always)]
pub fn record_person(raw: u64, h: u64) -> u64 {
    if cfg!(feature = "narrow") {
        (h >> LOW_HASH_BITS) & PERSON_MASK
    } else {
        raw
    }
}

const fn inverse_odd(x: u64) -> u64 {
    let mut inv = 1u64;
    let mut i = 0;
    while i < 6 {
        inv = inv.wrapping_mul(2u64.wrapping_sub(x.wrapping_mul(inv)));
        i += 1;
    }
    inv
}

#[inline]
pub fn restore_person(q: usize, person: u64) -> u64 {
    if cfg!(feature = "narrow") {
        let z = ((q as u64 & 255) << PERSON_BITS) | person;
        let y = z ^ (z >> 22);
        let x = y.wrapping_mul(inverse_odd(H2)) & ID_MASK;
        let original = x ^ (x >> 21) ^ (x >> 42);
        original.wrapping_mul(inverse_odd(H1)) & ID_MASK
    } else {
        person
    }
}

// Dense ranks preserve the order of the padded base-37 encoding while
// reclaiming the unused ranks. The extra space admits uppercase-prefix IDs
// with either consistently uppercase or consistently lowercase suffixes.
#[inline]
pub fn encode(id: &[u8], lowercase_prefix: u8) -> Option<u64> {
    let (&prefix, tail) = id.split_first()?;
    if !(1..=8).contains(&tail.len()) {
        return None;
    }
    if prefix == lowercase_prefix {
        let mut rank = UPPER_EXTENT;
        for (i, &byte) in tail.iter().enumerate() {
            let digit = symbol(byte, b'a')?;
            rank += 1 + digit * SUB36[7 - i];
        }
        Some(rank)
    } else if prefix == lowercase_prefix.to_ascii_uppercase() {
        encode_upper(tail)
    } else {
        None
    }
}

#[inline]
fn symbol(byte: u8, first_letter: u8) -> Option<u64> {
    if byte.is_ascii_digit() {
        Some(u64::from(byte - b'0'))
    } else if byte.wrapping_sub(first_letter) < 26 {
        Some(u64::from(byte - first_letter + 10))
    } else {
        None
    }
}

#[cold]
fn encode_upper(tail: &[u8]) -> Option<u64> {
    let mut rank = 0;
    let mut alphabet = 0;
    for (i, &byte) in tail.iter().enumerate() {
        let rem = 7 - i;
        rank += 1;
        if alphabet != 0 {
            rank += symbol(byte, alphabet)? * SUB36[rem];
            continue;
        }
        // Both alphabets share every digit-only suffix. At the first letter
        // the two sets diverge, and byte order places uppercase first.
        let combined = 2 * SUB36[rem] - SUB10[rem];
        if byte.is_ascii_digit() {
            rank += u64::from(byte - b'0') * combined;
        } else if byte.is_ascii_uppercase() {
            alphabet = b'A';
            rank += 10 * combined + u64::from(byte - b'A') * SUB36[rem];
        } else if byte.is_ascii_lowercase() {
            alphabet = b'a';
            rank += 10 * combined + u64::from(byte - b'a' + 26) * SUB36[rem];
        } else {
            return None;
        }
    }
    Some(rank)
}

const BASE_36_8: u64 = 36u64.pow(8);
const BASE_36_7: u64 = 36u64.pow(7);
const EID_SHORT_BASE: u64 = 3 * BASE_36_8;
const _: () = assert!(EID_SHORT_BASE + 3 * SUB36[7] <= ID_MASK);

/// Fast injective 43-bit encoder for `event_id` (equality-only: no order needed,
/// skips `_mm_sad_epu8` and `/ 35` on the hot 8-char and 7-char lowercase paths).
#[inline(always)]
pub fn encode_eid_short(prefix: u8, tail: u64, suffix_len: usize) -> Option<u64> {
    debug_assert!(matches!(suffix_len, 7 | 8));
    if prefix != b'e' {
        return encode_eid(&[
            prefix,
            tail as u8,
            (tail >> 8) as u8,
            (tail >> 16) as u8,
            (tail >> 24) as u8,
            (tail >> 32) as u8,
            (tail >> 40) as u8,
            (tail >> 48) as u8,
            (tail >> 56) as u8,
        ][..=suffix_len]);
    }
    unsafe {
        let src = _mm_cvtsi64_si128(tail as i64);
        let digit_range = _mm_and_si128(
            _mm_cmpgt_epi8(src, _mm_set1_epi8(b'0' as i8 - 1)),
            _mm_cmpgt_epi8(_mm_set1_epi8(b'9' as i8 + 1), src),
        );
        let letter_range = _mm_and_si128(
            _mm_cmpgt_epi8(src, _mm_set1_epi8(b'a' as i8 - 1)),
            _mm_cmpgt_epi8(_mm_set1_epi8(b'z' as i8 + 1), src),
        );
        let valid = _mm_movemask_epi8(_mm_or_si128(digit_range, letter_range)) as u32;
        let keep = (1 << suffix_len) - 1;
        if valid & keep != keep {
            return None;
        }
        let numbers = _mm_sub_epi8(src, _mm_set1_epi8(b'0' as i8));
        let alpha = _mm_sub_epi8(src, _mm_set1_epi8(b'a' as i8 - 10));
        let digits = _mm_blendv_epi8(alpha, numbers, digit_range);
        if suffix_len == 8 {
            let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x0124));
            let quads = _mm_madd_epi16(pairs, _mm_set1_epi32((1 << 16) | 1296));
            let packed = _mm_cvtsi128_si64(quads) as u64;
            Some((packed as u32 as u64) * BASE_36_4 + (packed >> 32))
        } else {
            let masked = _mm_and_si128(digits, _mm_cvtsi64_si128(0x00ff_ffff_ffff_ffff));
            let pairs = _mm_maddubs_epi16(masked, _mm_set1_epi16(0x0124));
            let quads = _mm_madd_epi16(pairs, _mm_set1_epi32((1 << 16) | 1296));
            let packed = _mm_cvtsi128_si64(quads) as u64;
            // Last lane is 0, so packed_hi = d4*36^3 + d5*36^2 + d6*36 + 0 -> divide by 36 via
            // pre-shifted sum: n7 = (packed_lo * 36^3) + (packed_hi / 36); simpler: shift digits right 1?
            // Actually packed_lo * BASE_36_4 + packed_hi = 36 * n7, and we don't even need to divide by 36
            // unless we match encode_eid! Just compute exact n7 in encode_eid_short:
            let n7 = (packed as u32 as u64) * (36 * 36 * 36) + ((packed >> 32) / 36);
            Some(EID_SHORT_BASE + n7)
        }
    }
}

/// Scalar counterpart to `encode_eid_short` (must return identical 43-bit codes for all valid IDs).
#[inline]
pub fn encode_eid(id: &[u8]) -> Option<u64> {
    let (&prefix, tail) = id.split_first()?;
    if !(1..=8).contains(&tail.len()) {
        return None;
    }
    if tail.len() == 8 {
        if prefix == b'e' {
            let mut n = 0u64;
            for &b in tail {
                n = n * 36 + symbol(b, b'a')?;
            }
            return Some(n);
        }
        if prefix == b'E' {
            let mut n = 0u64;
            let mut alpha = 0u8;
            for &b in tail {
                let d = if b.is_ascii_digit() {
                    u64::from(b - b'0')
                } else if b.is_ascii_uppercase() {
                    if alpha == b'a' {
                        return None;
                    }
                    alpha = b'A';
                    u64::from(b - b'A' + 10)
                } else if b.is_ascii_lowercase() {
                    if alpha == b'A' {
                        return None;
                    }
                    alpha = b'a';
                    u64::from(b - b'a' + 10)
                } else {
                    return None;
                };
                n = n * 36 + d;
            }
            let base = if alpha == b'a' { 2 * BASE_36_8 } else { BASE_36_8 };
            return Some(base + n);
        }
        return None;
    }
    if tail.len() == 7 && prefix == b'e' {
        let mut n = 0u64;
        for &b in tail {
            n = n * 36 + symbol(b, b'a')?;
        }
        return Some(EID_SHORT_BASE + n);
    }
    // Lengths 1..=6 (any valid e/E) or length 7 with prefix 'E': pack into [EID_SHORT_BASE + BASE_36_7, ID_MASK].
    // Map via prefix/case tag (0='e', 1='E' upper/digit, 2='E' lower) + dense length<=7 rank.
    let mut n = 0u64;
    let mut alpha = if prefix == b'e' { b'a' } else if prefix == b'E' { 0 } else { return None };
    let mut mult = 1u64;
    for &b in tail {
        let d = if b.is_ascii_digit() {
            u64::from(b - b'0')
        } else if alpha == 0 {
            if b.is_ascii_uppercase() {
                alpha = b'A';
                u64::from(b - b'A' + 10)
            } else if b.is_ascii_lowercase() {
                alpha = b'a';
                u64::from(b - b'a' + 10)
            } else {
                return None;
            }
        } else {
            symbol(b, alpha)?
        };
        n = n * 36 + d;
        mult *= 36;
    }
    let tag: u64 = if prefix == b'e' {
        0
    } else if alpha == b'a' {
        2
    } else {
        1
    };
    // Distinct per (len 1..=7, tag): offset by sum_{k=1}^{len-1} 36^k = (mult - 36)/35 + 1?
    // Simpler: use SUB36[7] per tag (3 * SUB36[7] fits comfortably after EID_SHORT_BASE + BASE_36_7? Wait:
    // EID_SHORT_BASE + BASE_36_7 + 2 * SUB36[7] = 8.463e12 + 78.36e9 + 161.19e9 = 8.703e12 < 8.796e12 = 2^43!)
    // Since tag==0 never has len==7 here, tag==0 uses [0, SUB36[6]), tag==1 uses [SUB36[6], SUB36[6]+SUB36[7]),
    // tag==2 uses [SUB36[6]+SUB36[7], SUB36[6]+2*SUB36[7]).
    let rank_within_tag = (mult - 1) / 35 + n;
    let tag_off = match tag {
        0 => 0,
        1 => SUB36[6],
        _ => SUB36[6] + SUB36[7],
    };
    let code = EID_SHORT_BASE + BASE_36_7 + tag_off + rank_within_tag;
    debug_assert!(code <= ID_MASK);
    Some(code)
}

/// SIMD equivalent for the usual seven- and eight-byte suffixes.
/// The input is an unaligned little-endian load of eight readable bytes.
#[inline(always)]
pub fn encode_short(prefix: u8, tail: u64, suffix_len: usize, expected: u8) -> Option<u64> {
    debug_assert!(matches!(suffix_len, 7 | 8));
    if prefix != expected {
        if prefix == expected.to_ascii_uppercase() {
            return encode_upper(&tail.to_le_bytes()[..suffix_len]);
        }
        return None;
    }
    unsafe {
        let src = _mm_cvtsi64_si128(tail as i64);
        let digit_range = _mm_and_si128(
            _mm_cmpgt_epi8(src, _mm_set1_epi8(b'0' as i8 - 1)),
            _mm_cmpgt_epi8(_mm_set1_epi8(b'9' as i8 + 1), src),
        );
        let letter_range = _mm_and_si128(
            _mm_cmpgt_epi8(src, _mm_set1_epi8(b'a' as i8 - 1)),
            _mm_cmpgt_epi8(_mm_set1_epi8(b'z' as i8 + 1), src),
        );
        let valid = _mm_movemask_epi8(_mm_or_si128(digit_range, letter_range)) as u32;
        let keep = (1 << suffix_len) - 1;
        if valid & keep != keep {
            return None;
        }
        let numbers = _mm_sub_epi8(src, _mm_set1_epi8(b'0' as i8));
        let alpha = _mm_sub_epi8(src, _mm_set1_epi8(b'a' as i8 - 10));
        let digits = _mm_and_si128(
            _mm_blendv_epi8(alpha, numbers, digit_range),
            _mm_cvtsi64_si128(if suffix_len == 8 {
                -1
            } else {
                0x00ff_ffff_ffff_ffff
            }),
        );
        let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x0124));
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32((1 << 16) | 1296));
        let packed = _mm_cvtsi128_si64(quads) as u64;
        let n = (packed as u32 as u64) * BASE_36_4 + (packed >> 32);
        let sum = _mm_cvtsi128_si64(_mm_sad_epu8(digits, _mm_setzero_si128())) as u64;
        // Sum each digit times SUB36[remaining]. This computes the same
        // sum using its geometric-series identity and a constant divisor.
        Some(UPPER_EXTENT + suffix_len as u64 + (36 * n - sum) / 35)
    }
}

pub fn decode(mut code: u64, lowercase_prefix: u8, out: &mut [u8; 16]) -> usize {
    let lower = code >= UPPER_EXTENT;
    out[0] = if lower {
        lowercase_prefix
    } else {
        lowercase_prefix.to_ascii_uppercase()
    };
    let mut alphabet = if lower { b'a' } else { 0 };
    if lower {
        code -= UPPER_EXTENT;
    }
    let mut len = 1;
    for rem in (0..8).rev() {
        if code == 0 {
            break;
        }
        code -= 1;
        let digit = if alphabet == 0 {
            let combined = 2 * SUB36[rem] - SUB10[rem];
            let digits_extent = 10 * combined;
            if code < digits_extent {
                let digit = code / combined;
                code %= combined;
                out[len] = b'0' + digit as u8;
                len += 1;
                continue;
            }
            code -= digits_extent;
            let letter = code / SUB36[rem];
            alphabet = if letter < 26 { b'A' } else { b'a' };
            code %= SUB36[rem];
            10 + letter % 26
        } else {
            let digit = code / SUB36[rem];
            code %= SUB36[rem];
            digit
        };
        out[len] = if digit < 10 {
            b'0' + digit as u8
        } else {
            alphabet + digit as u8 - 10
        };
        len += 1;
    }
    len
}

pub const INVALID_LOC: u32 = (1 << 30) - 1;
pub const INVALID_INGEST: u32 = (1 << 23) - 1;
#[cfg(any(test, not(feature = "narrow")))]
pub const PURCHASE_LIMIT: u32 = 1 << 21;
pub const CLICK_TS_LIMIT: u32 = 1 << 31;
pub const CAMPAIGN_LIMIT: usize = 1 << 11;

#[cfg(any(test, not(feature = "narrow")))]
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Tok([u32; 3]);

#[cfg(any(test, not(feature = "narrow")))]
impl Tok {
    #[inline(always)]
    pub fn new(eid: u64, ingest: u32, loc: u32) -> Self {
        debug_assert!(eid <= ID_MASK && ingest <= INVALID_INGEST && loc <= INVALID_LOC);
        Self([
            eid as u32,
            (eid >> 32) as u32 | (ingest << 11),
            (ingest >> 21) | (loc << 2),
        ])
    }

    #[inline(always)]
    pub fn eid(self) -> u64 {
        u64::from(self.0[0]) | (u64::from(self.0[1] & 0x7ff) << 32)
    }

    #[inline(always)]
    pub fn ingest(self) -> u32 {
        (self.0[1] >> 11) | ((self.0[2] & 3) << 21)
    }

    #[inline(always)]
    pub fn loc(self) -> u32 {
        self.0[2] >> 2
    }
}

#[cfg(any(test, not(feature = "narrow")))]
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RecB([u32; 3]);

#[cfg(any(test, not(feature = "narrow")))]
impl RecB {
    #[inline(always)]
    pub fn new(person: u64, ts: u32, purchase_idx: u32) -> Self {
        debug_assert!(person <= ID_MASK && purchase_idx < PURCHASE_LIMIT);
        Self([
            person as u32,
            (person >> 32) as u32 | (purchase_idx << 11),
            ts,
        ])
    }

    #[inline(always)]
    pub fn person(self) -> u64 {
        u64::from(self.0[0]) | (u64::from(self.0[1] & 0x7ff) << 32)
    }

    #[inline(always)]
    pub fn ts(self) -> u32 {
        self.0[2]
    }

    #[inline(always)]
    pub fn purchase_idx(self) -> usize {
        (self.0[1] >> 11) as usize
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct RecC([u64; 2]);

impl RecC {
    #[inline(always)]
    pub fn new(person: u64, cid: u64, ts: u32, campaign: u32) -> Self {
        debug_assert!(person <= ID_MASK && cid <= ID_MASK);
        debug_assert!(ts < CLICK_TS_LIMIT && (campaign as usize) < CAMPAIGN_LIMIT);
        Self([
            person | (cid << 43),
            (cid >> 21) | (u64::from(ts) << 22) | (u64::from(campaign) << 53),
        ])
    }

    #[inline(always)]
    pub fn person(self) -> u64 {
        self.0[0] & ID_MASK
    }

    #[inline(always)]
    pub fn cid(self) -> u64 {
        (self.0[0] >> 43) | ((self.0[1] & ((1 << 22) - 1)) << 21)
    }

    #[inline(always)]
    pub fn ts(self) -> u32 {
        ((self.0[1] >> 22) as u32) & (CLICK_TS_LIMIT - 1)
    }

    #[inline(always)]
    pub fn campaign(self) -> u32 {
        (self.0[1] >> 53) as u32
    }
}

#[cfg(feature = "narrow")]
pub type StoredTok = Tok10;
#[cfg(not(feature = "narrow"))]
pub type StoredTok = Tok;
#[cfg(feature = "narrow")]
pub type StoredEvent = RecB8;
#[cfg(not(feature = "narrow"))]
pub type StoredEvent = RecB;

#[cfg(not(feature = "narrow"))]
impl Tok {
    #[inline(always)]
    pub fn pack(eid: u64, _: u64, ingest: u32, loc: u32) -> Option<Self> {
        Some(Self::new(eid, ingest, loc))
    }

    #[inline(always)]
    pub fn key(self) -> u64 {
        self.eid()
    }

    #[inline(always)]
    pub fn location(self, _: u32) -> u32 {
        self.loc()
    }
}

#[cfg(any(test, feature = "narrow"))]
pub const NARROW_INGEST_LIMIT: u32 = (1 << 21) - 1;
const NARROW_ROW_MASK: u32 = (1 << 16) - 1;
const FULL_ROW_MASK: u32 = (1 << 18) - 1;
pub const OVERFLOW_LOC: u32 = u32::MAX;

#[inline(always)]
pub fn needs_loc_side(loc: u32) -> bool {
    cfg!(feature = "narrow") && loc != INVALID_LOC && (loc & FULL_ROW_MASK) >= NARROW_ROW_MASK - 1
}

#[cfg(any(test, feature = "narrow"))]
/// 34-bit exact event key within its 512-way partition, 21-bit ingest seconds and
/// a 25-bit worker-local location. The token block identifies the worker.
#[derive(Clone, Copy)]
#[repr(C, packed(2))]
pub struct Tok10 {
    head: u64,
    tail: u16,
}

#[cfg(any(test, feature = "narrow"))]
impl Tok10 {
    #[inline(always)]
    pub fn pack(_: u64, h: u64, ingest: u32, loc: u32) -> Option<Self> {
        let ingest = if ingest == INVALID_INGEST {
            NARROW_INGEST_LIMIT
        } else if ingest < NARROW_INGEST_LIMIT {
            ingest
        } else {
            return None;
        };
        let location = if loc == INVALID_LOC {
            (1 << 25) - 1
        } else if loc == OVERFLOW_LOC || (loc & FULL_ROW_MASK) >= NARROW_ROW_MASK - 1 {
            (1 << 25) - 2
        } else {
            (((loc >> 18) & 511) << 16) | (loc & NARROW_ROW_MASK)
        };
        Some(Self {
            head: ((h >> LOW_HASH_BITS) & TOKEN_MASK)
                | (u64::from(ingest) << TOKEN_BITS)
                | (u64::from(location) << (TOKEN_BITS + 21)),
            tail: (location >> 9) as u16,
        })
    }

    #[inline(always)]
    pub fn pack_valid_fast(h: u64, ingest: u32, q: usize, idx: usize) -> Option<Self> {
        if ingest >= NARROW_INGEST_LIMIT {
            return None;
        }
        let location = if idx >= (NARROW_ROW_MASK as usize - 1) {
            (1u32 << 25) - 2
        } else {
            ((q as u32) << 17) | (idx as u32)
        };
        Some(Self {
            head: ((h >> LOW_HASH_BITS) & TOKEN_MASK)
                | (u64::from(ingest) << TOKEN_BITS)
                | (u64::from(location) << (TOKEN_BITS + 21)),
            tail: (location >> 9) as u16,
        })
    }

    #[inline(always)]
    pub fn key(self) -> u64 {
        self.head & TOKEN_MASK
    }

    #[inline(always)]
    pub fn ingest(self) -> u32 {
        ((self.head >> TOKEN_BITS) as u32) & NARROW_INGEST_LIMIT
    }

    #[inline(always)]
    pub fn location(self, tid: u32) -> u32 {
        let loc = (u32::from(self.tail) << 9) | (self.head >> (TOKEN_BITS + 21)) as u32;
        match loc {
            n if n == (1 << 25) - 1 => INVALID_LOC,
            n if n == (1 << 25) - 2 => OVERFLOW_LOC,
            _ => (tid << 27) | ((loc >> 16) << 18) | (loc & NARROW_ROW_MASK),
        }
    }
}

#[cfg(any(test, feature = "narrow"))]
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct RecB8(u64);

#[cfg(any(test, feature = "narrow"))]
impl RecB8 {
    #[inline(always)]
    pub fn new(person: u64, sec: u32, side_index: u8) -> Self {
        debug_assert!(person <= PERSON_MASK && sec < 1 << 21);
        Self(person | (u64::from(sec) << PERSON_BITS) | (u64::from(side_index) << 56))
    }

    #[inline(always)]
    pub fn person(self) -> u64 {
        self.0 & PERSON_MASK
    }

    #[inline(always)]
    pub fn seconds(self) -> u32 {
        (self.0 >> PERSON_BITS) as u32 & ((1 << 21) - 1)
    }

    #[inline(always)]
    pub fn side(self) -> usize {
        (self.0 >> 56) as usize
    }
}

// These sizes and padding guarantees are covered by the layout tests.
unsafe impl crate::proto_c::arena::StreamRecord for StoredTok {
    const FLUSH_ROWS: usize = if cfg!(feature = "narrow") { 32 } else { 16 };
}
unsafe impl crate::proto_c::arena::StreamRecord for StoredEvent {
    const FLUSH_ROWS: usize = if cfg!(feature = "narrow") { 8 } else { 16 };
}
unsafe impl crate::proto_c::arena::StreamRecord for RecC {
    const FLUSH_ROWS: usize = 4;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn layouts_and_maxima() {
        assert_eq!(size_of::<Tok>(), 12);
        assert_eq!(size_of::<RecB>(), 12);
        assert_eq!(size_of::<RecC>(), 16);
        assert_eq!(size_of::<Tok10>(), 10);
        assert_eq!(size_of::<RecB8>(), 8);
        let compact =
            Tok10::pack(0, u64::MAX, 1_234_567, (7 << 27) | (250 << 19) | 65_533).unwrap();
        assert_eq!(compact.key(), TOKEN_MASK);
        assert_eq!(compact.ingest(), 1_234_567);
        assert_eq!(compact.location(7), (7 << 27) | (250 << 19) | 65_533);
        let fast = Tok10::pack_valid_fast(u64::MAX, 1_234_567, 250, 65_533).unwrap();
        assert_eq!(fast.key(), compact.key());
        assert_eq!(fast.ingest(), compact.ingest());
        assert_eq!(fast.location(7), compact.location(7));
        let overflow = Tok10::pack(0, 0, 0, 110_000).unwrap();
        assert_eq!(overflow.location(0), OVERFLOW_LOC);
        let invalid = Tok10::pack(0, 0, INVALID_INGEST, INVALID_LOC).unwrap();
        assert_eq!(
            (invalid.ingest(), invalid.location(0)),
            (NARROW_INGEST_LIMIT, INVALID_LOC)
        );
        let narrow = RecB8::new(PERSON_MASK, (1 << 21) - 1, u8::MAX);
        assert_eq!(
            (narrow.person(), narrow.seconds(), narrow.side()),
            (PERSON_MASK, (1 << 21) - 1, 255)
        );
        let tok = Tok::new(ID_MASK, INVALID_INGEST, INVALID_LOC);
        assert_eq!(
            (tok.eid(), tok.ingest(), tok.loc()),
            (ID_MASK, INVALID_INGEST, INVALID_LOC)
        );
        let event = RecB::new(ID_MASK, u32::MAX, PURCHASE_LIMIT - 1);
        assert_eq!(
            (event.person(), event.ts(), event.purchase_idx()),
            (ID_MASK, u32::MAX, PURCHASE_LIMIT as usize - 1)
        );
        let click = RecC::new(
            ID_MASK,
            ID_MASK,
            CLICK_TS_LIMIT - 1,
            CAMPAIGN_LIMIT as u32 - 1,
        );
        assert_eq!(
            (click.person(), click.cid(), click.ts(), click.campaign()),
            (
                ID_MASK,
                ID_MASK,
                CLICK_TS_LIMIT - 1,
                CAMPAIGN_LIMIT as u32 - 1
            )
        );
    }

    #[test]
    fn ids_round_trip_and_sort() {
        assert_eq!(BASE37_CARDINALITY, 3_512_479_453_921);
        let mut ids: Vec<_> = [b"u0".as_slice(), b"u00", b"u0a", b"Uzzzzzzzz", b"UZZZZZZZZ"]
            .into_iter()
            .map(<[u8]>::to_vec)
            .collect();
        let mut rng = 91u64;
        for i in 0..100_000 {
            rng = hash(rng);
            let upper = i % 3 == 0;
            let mut id = vec![if i % 3 != 2 { b'U' } else { b'u' }];
            let mut n = rng;
            for _ in 0..1 + i % 8 {
                let digit = (n % 36) as u8;
                id.push(if digit < 10 {
                    b'0' + digit
                } else if upper {
                    b'A' + digit - 10
                } else {
                    b'a' + digit - 10
                });
                n /= 36;
            }
            ids.push(id);
        }
        ids.sort_unstable();
        ids.dedup();
        let mut prev = 0;
        for id in ids {
            let code = encode(&id, b'u').unwrap();
            assert!(code > prev);
            prev = code;
            let h = hash(code);
            assert_eq!(
                restore_person((h >> 56) as usize, record_person(code, h)),
                code
            );
            let mut out = [0; 16];
            let len = decode(code, b'u', &mut out);
            assert_eq!(&out[..len], id);
            if id.len() >= 8 {
                let mut tail = [0; 8];
                tail[..id.len() - 1].copy_from_slice(&id[1..]);
                assert_eq!(
                    encode_short(id[0], u64::from_le_bytes(tail), id.len() - 1, b'u'),
                    Some(code)
                );
            }
        }
        assert!(encode(b"u123456789", b'u').is_none());
        assert!(encode(b"U00Zz", b'u').is_none());
    }

    #[test]
    fn eid_fast_matches_scalar_and_injective() {
        let mut seen = std::collections::HashSet::new();
        let mut samples: Vec<Vec<u8>> = [
            b"e0".as_slice(),
            b"e00",
            b"e0000000",
            b"e00000000",
            b"ezzzzzzz",
            b"ezzzzzzzz",
            b"E0",
            b"E0000000",
            b"E00000000",
            b"EZZZZZZZZ",
            b"Ezzzzzzzz",
            b"E00a",
            b"E00A",
        ]
        .into_iter()
        .map(<[u8]>::to_vec)
        .collect();
        let mut rng = 77u64;
        for i in 0..50_000 {
            rng = rng.wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(1);
            let upper_prefix = i % 5 == 0;
            let upper_body = i % 3 == 0;
            let mut id = vec![if upper_prefix { b'E' } else { b'e' }];
            let len = if i % 4 == 0 { 1 + i % 7 } else { 7 + (i & 1) };
            let mut n = rng;
            for _ in 0..len {
                let d = (n % 36) as u8;
                id.push(if d < 10 {
                    b'0' + d
                } else if upper_prefix && upper_body {
                    b'A' + d - 10
                } else {
                    b'a' + d - 10
                });
                n /= 36;
            }
            samples.push(id);
        }
        samples.sort_unstable();
        samples.dedup();
        for id in samples {
            let code = encode_eid(&id).expect("valid eid");
            assert!(code <= ID_MASK);
            assert!(seen.insert(code), "collision for {:?}", String::from_utf8_lossy(&id));
            if id.len() >= 8 {
                let mut tail = [0u8; 8];
                tail[..id.len() - 1].copy_from_slice(&id[1..]);
                assert_eq!(
                    encode_eid_short(id[0], u64::from_le_bytes(tail), id.len() - 1),
                    Some(code),
                    "mismatch for {:?}",
                    String::from_utf8_lossy(&id)
                );
            }
        }
        assert!(encode_eid(b"e123456789").is_none());
        assert!(encode_eid(b"E00Zz").is_none());
        assert!(encode_eid(b"u00000001").is_none());
    }
}
