//! Hybrid inline-64 + side-arena single-pass exact ingest & fused aggregation engine.
//!
//! Treats EVERY ID (`event_id`, `user_id`, `click_id`) as a 100% opaque variable-width
//! byte string of arbitrary length (`1..∞`, arbitrary bytes/UTF-8/NUL):
//! - Short IDs (`1..=9` non-NUL ASCII bytes, ~87.5% on v5) are stored inline in `u64`
//!   with bit 63 == 0 (exact lossless, byte-wise order-preserving, zero arena traffic).
//! - Long/other IDs (`> 9` bytes or containing NUL / non-ASCII, ~12.5% on v5 including
//!   33-byte hex IDs, or 100% on `seed60-longids`) are stored verbatim in unified 2 MiB THP
//!   `ArenaBytes` slices (with automatic heap fallback on extreme skew) and referenced via
//!   tagged `u64` handles (bit 63 == 1) with embedded hash fingerprints.
//!
//! Disjoint short/long partition spaces inside unified `Sinks` (SWCB=256/256/192):
//! - `tok_sinks` holds `p1_short` pure-short partitions (`0..p1_short`) followed by
//!   `p1_long` pure-long partitions (`p1_short..p1_total`).
//! - `b_sinks`, `ck_sinks`, `reloc_b`, `reloc_c` hold `p2_short` pure-short partitions
//!   (`0..p2_short`) followed by `p2_long` pure-long partitions (`p2_short..p2_total`).
//! Fusing ID encoding directly into `parse_*_delim` eliminates ALL short-vs-long enum
//! branches in `parse`, `relocate`, and `group+agg`.

use super::{
    add_bin_purchases, emit_session_c, env_usize, field, flen, flush_user_day, for_each_live, fptr,
    fts, partition_slices, sort_ev_slice, CompactClick, CompactEv, Ctx, DeadEvents, DedupBucket32,
    DedupTable, DenseBin, DenseSessions, Flag, FusedAssists, GroupPhase, GroupProfile,
    GroupTimings, Need, PartitionPlan, RecC, Tok, DAY_MS_U32, GAP_MS_U32, INVALID_CAMP,
    LOC_IDX_BITS, MAX_LOC_IDX, MAX_THREADS, NO_REC, WEEK_MS,
};
use crate::aggregate::{self, output, CampStore};
use crate::ingest::keys::Key;
use crate::ingest::par::{self, ArenaVec, Scratch, SinkArena, Sinks, SyncPtr, Timer};
use crate::ingest::scan::{self, FileInfo, Kind};
use crate::ingest::tsfast::{self, DateTable};
use crate::ingest::{slow, ts};
use std::arch::x86_64::*;
use std::cmp::Ordering as CmpOrd;
use std::mem::MaybeUninit;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::Instant;

const HLP_BITS: u32 = 9;
const HLP: usize = 1 << HLP_BITS;
const MAX_HLP_LOCAL_ID: usize = (1 << 23) - 1;

const LONG_BIT: u64 = 1u64 << 63;
const LINK_LONG_BIT: u64 = 1u64 << 62;
const MASK63: u64 = (1u64 << 63) - 1;
const MIX63_M: u64 = 0x5E37_79B9_7F4A_7C15; // odd, < 2^63, bit 62 = 1

// With split `parse_events -> dedup -> parse_clicks`, active SWCB during Events is ONLY
// `tok` + `b` = 576 * (256 + 192) = 252 KiB/worker (504 KiB/SMT pair + 128 KiB LongUserCache =
// 632 KiB/SMT pair, leaving >380 KiB L2 headroom on Cascade Lake!), while during Clicks ONLY
// `ck` is live = 576 * 384 = 216 KiB/worker (432 KiB/SMT pair, halving click flush mispredicts!).
const SWCB_TOK: usize = 256;
const SWCB_B: usize = 192;
const SWCB_CK: usize = 384;

#[target_feature(enable = "avx512bw,avx512vl")]
#[inline]
unsafe fn vl_cmpeq_epi8_mask16(a: __m128i, b: __m128i) -> u32 {
    _mm_cmpeq_epi8_mask(a, b) as u32
}

#[target_feature(enable = "avx512bw,avx512vl")]
#[inline]
unsafe fn vl_mask_cmpeq_epi8_mask16(k: u16, a: __m128i, b: __m128i) -> u32 {
    _mm_mask_cmpeq_epi8_mask(k, a, b) as u32
}

#[target_feature(enable = "avx512bw,avx512vl")]
#[inline]
unsafe fn vl_cmpeq_epi8_mask32(a: __m256i, b: __m256i) -> u32 {
    _mm256_cmpeq_epi8_mask(a, b)
}

#[target_feature(enable = "avx512bw,avx512vl")]
#[inline]
unsafe fn vl_mask_cmpeq_epi8_mask32(k: u32, a: __m256i, b: __m256i) -> u32 {
    _mm256_mask_cmpeq_epi8_mask(k, a, b)
}

/// 12-byte packed surviving event (-25% parse/reloc arena & DRAM bandwidth vs 16B `RecB`):
/// bit 31 == 0 => non-purchase event with inline `ts = ts_or_purch` (`cc = 0`);
/// bit 31 == 1 => purchase (or `ts >= 2^31`) referencing `worker_purch[tid][idx]`.
#[derive(Clone, Copy)]
#[repr(C, align(4))]
struct RecB12 {
    person_lo: u32,
    person_hi: u32,
    ts_or_purch: u32,
}

impl RecB12 {
    #[inline(always)]
    fn new(person: u64, ts_or_purch: u32) -> Self {
        Self { person_lo: person as u32, person_hi: (person >> 32) as u32, ts_or_purch }
    }

    #[inline(always)]
    fn person(self) -> u64 {
        (self.person_lo as u64) | ((self.person_hi as u64) << 32)
    }
}

const fn inv_odd_63(x: u64) -> u64 {
    let mut inv = 1u64;
    let mut i = 0;
    while i < 6 {
        inv = inv.wrapping_mul(2u64.wrapping_sub(x.wrapping_mul(inv))) & MASK63;
        i += 1;
    }
    inv
}

const MIX63_INV: u64 = inv_odd_63(MIX63_M);

/// 1-multiply exact bijection on `[0, 2^63)` with `mix63(0) == 0`.
/// Keeps bit 63 == 0 for all short inline IDs both raw and mixed!
#[inline(always)]
fn mix63(x: u64) -> u64 {
    debug_assert!(x <= MASK63);
    let y = (x ^ (x >> 32)).wrapping_mul(MIX63_M) & MASK63;
    y ^ (y >> 32)
}

/// Exact inverse of `mix63`: `unmix63(mix63(x)) == x` for all `x < 2^63`.
#[inline(always)]
fn unmix63(x: u64) -> u64 {
    debug_assert!(x <= MASK63);
    let y = x ^ (x >> 32);
    let z = y.wrapping_mul(MIX63_INV) & MASK63;
    z ^ (z >> 32)
}

#[inline(always)]
fn is_short(tag: u64) -> bool {
    (tag as i64) >= 0
}

#[inline(always)]
unsafe fn ld8(p: *const u8) -> u64 {
    p.cast::<u64>().read_unaligned()
}

/// Lossless AVX2 packer for verified 33-byte hex IDs (`[ascii_prefix < 128][32 uniform-case hex]`).
/// Returns `(tag, hi, lo)` where `tag = prefix` for `[0-9a-f]` (including all-digits) and
/// `tag = prefix | 0x80` for `[0-9A-F]` containing at least one `A-F`.
/// Returns `None` for any mixed-case, non-hex, or non-ASCII-prefix 33B string (stored verbatim).
#[inline(always)]
unsafe fn try_pack_hex33(p: *const u8) -> Option<(u8, u64, u64)> {
    let b0 = *p;
    if b0.wrapping_sub(1) >= 127 {
        return None;
    }
    let v = _mm256_loadu_si256(p.add(1).cast::<__m256i>());
    let zero = _mm256_set1_epi8(b'0' as i8);
    let d = _mm256_sub_epi8(v, zero);
    let is_dig = _mm256_cmpeq_epi8(_mm256_subs_epu8(d, _mm256_set1_epi8(9)), _mm256_setzero_si256());

    let a_lo = _mm256_sub_epi8(v, _mm256_set1_epi8(b'a' as i8));
    let is_lo =
        _mm256_cmpeq_epi8(_mm256_subs_epu8(a_lo, _mm256_set1_epi8(5)), _mm256_setzero_si256());
    let ok_lo = _mm256_or_si256(is_dig, is_lo);

    let (nib, tag) = if _mm256_movemask_epi8(ok_lo) == -1i32 {
        let lo_val = _mm256_add_epi8(a_lo, _mm256_set1_epi8(10));
        (_mm256_blendv_epi8(lo_val, d, is_dig), b0)
    } else {
        let a_hi = _mm256_sub_epi8(v, _mm256_set1_epi8(b'A' as i8));
        let is_hi = _mm256_cmpeq_epi8(
            _mm256_subs_epu8(a_hi, _mm256_set1_epi8(5)),
            _mm256_setzero_si256(),
        );
        let ok_hi = _mm256_or_si256(is_dig, is_hi);
        if _mm256_movemask_epi8(ok_hi) != -1i32 {
            return None;
        }
        let hi_val = _mm256_add_epi8(a_hi, _mm256_set1_epi8(10));
        (_mm256_blendv_epi8(hi_val, d, is_dig), b0 | 0x80)
    };
    let pairs16 = _mm256_maddubs_epi16(nib, _mm256_set1_epi16(0x0110));
    let lo128 = _mm256_castsi256_si128(pairs16);
    let hi128 = _mm256_extracti128_si256::<1>(pairs16);
    let packed = _mm_packus_epi16(lo128, hi128);
    let hi = _mm_cvtsi128_si64(packed) as u64;
    let lo = _mm_extract_epi64::<1>(packed) as u64;
    Some((tag, hi, lo))
}

#[inline(always)]
fn unpack_hex33(tag: u8, hi: u64, lo: u64, out: &mut [u8; 33]) {
    const HEX_LO: &[u8; 16] = b"0123456789abcdef";
    const HEX_HI: &[u8; 16] = b"0123456789ABCDEF";
    out[0] = tag & 0x7F;
    let lut = if (tag & 0x80) == 0 { HEX_LO } else { HEX_HI };
    let h = hi.to_le_bytes();
    let l = lo.to_le_bytes();
    for i in 0..8 {
        let v = h[i] as usize;
        out[1 + 2 * i] = lut[v >> 4];
        out[2 + 2 * i] = lut[v & 0xF];
    }
    for i in 0..8 {
        let v = l[i] as usize;
        out[17 + 2 * i] = lut[v >> 4];
        out[18 + 2 * i] = lut[v & 0xF];
    }
}

#[inline(always)]
fn hash_packed_33(tag: u8, hi: u64, lo: u64) -> u64 {
    const K0: u64 = 0xA076_1D64_78BD_642F;
    const K1: u64 = 0xE703_7ED1_A0B4_28DB;
    let m = (hi ^ K0 ^ ((tag as u64) << 56)).wrapping_mul(lo ^ K1);
    let x = m ^ (m >> 32) ^ (tag as u64);
    let y = (x ^ (x >> 29)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    y ^ (y >> 32)
}

/// Fast 64-bit hash over any opaque byte slice. Uses identical `hash_packed_33` whenever
/// `try_pack_hex33` matches so packed `ArenaBytes` records and raw slices always agree!
#[inline(always)]
fn hash_long(s: &[u8]) -> u64 {
    if s.len() == 33 {
        if let Some((tag, hi, lo)) = unsafe { try_pack_hex33(s.as_ptr()) } {
            return hash_packed_33(tag, hi, lo);
        }
    }
    unsafe { hash_long_general(s.as_ptr(), s.len()) }
}

#[cold]
#[inline(never)]
unsafe fn hash_long_general(p: *const u8, len: usize) -> u64 {
    const K0: u64 = 0xA076_1D64_78BD_642F;
    const K1: u64 = 0xE703_7ED1_A0B4_28DB;
    const K2: u64 = 0x8EBC_6AF0_9C88_C6E3;
    const K3: u64 = 0x5899_65CC_7537_4CC3;
    let mut h = (len as u64).wrapping_mul(K0) ^ K1;
    if len >= 8 {
        let mut i = 0usize;
        while i + 16 <= len {
            let a = ld8(p.add(i));
            let b = ld8(p.add(i + 8));
            let m = (a ^ K1).wrapping_mul(b ^ K2);
            h ^= m ^ (m >> 32);
            h = h.rotate_left(27).wrapping_mul(K3);
            i += 16;
        }
        if i + 8 <= len {
            let a = ld8(p.add(i));
            h = (h ^ a).wrapping_mul(K1);
            h ^= h >> 29;
        }
        let tail = ld8(p.add(len - 8));
        h = (h ^ tail).wrapping_mul(K2);
        let x = h ^ (len as u64);
        let y = (x ^ (x >> 29)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        y ^ (y >> 32)
    } else if len >= 4 {
        let a = p.cast::<u32>().read_unaligned() as u64;
        let b = p.add(len - 4).cast::<u32>().read_unaligned() as u64;
        let x = h ^ a ^ (b << 32);
        let y = (x ^ (x >> 29)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        y ^ (y >> 32)
    } else if len > 0 {
        let a = *p as u64;
        let b = *p.add(len >> 1) as u64;
        let c = *p.add(len - 1) as u64;
        let x = h ^ a ^ (b << 8) ^ (c << 16);
        let y = (x ^ (x >> 29)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        y ^ (y >> 32)
    } else {
        let y = (h ^ (h >> 29)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        y ^ (y >> 32)
    }
}

/// Append a length-prefixed byte slice to a `Vec<u8>`, returning its `u32` offset.
#[inline(always)]
fn push_long_bytes(arena: &mut Vec<u8>, s: &[u8]) -> Option<u32> {
    let off = arena.len();
    if off > u32::MAX as usize {
        return None;
    }
    let len = s.len();
    if len < 255 {
        arena.reserve(1 + len);
        unsafe {
            let dst = arena.as_mut_ptr().add(off);
            *dst = len as u8;
            std::ptr::copy_nonoverlapping(s.as_ptr(), dst.add(1), len);
            arena.set_len(off + 1 + len);
        }
    } else {
        if len > u32::MAX as usize {
            return None;
        }
        arena.reserve(5 + len);
        unsafe {
            let dst = arena.as_mut_ptr().add(off);
            *dst = 255;
            dst.add(1).cast::<u32>().write_unaligned(len as u32);
            std::ptr::copy_nonoverlapping(s.as_ptr(), dst.add(5), len);
            arena.set_len(off + 5 + len);
        }
    }
    Some(off as u32)
}

#[inline(always)]
unsafe fn get_verbatim_bytes<'a>(p: *const u8, b: u8) -> &'a [u8] {
    if b < 255 {
        std::slice::from_raw_parts(p.add(1), b as usize)
    } else {
        let len = p.add(1).cast::<u32>().read_unaligned() as usize;
        std::slice::from_raw_parts(p.add(5), len)
    }
}

#[inline(always)]
unsafe fn get_long_bytes(arena: &[u8], off: u32) -> &[u8] {
    let p = arena.as_ptr().add(off as usize);
    get_verbatim_bytes(p, *p)
}

#[inline(always)]
unsafe fn resolve_arena_bytes<'a>(arena: &'a [u8], off: u32, tmp: &'a mut [u8; 33]) -> &'a [u8] {
    let p = arena.as_ptr().add(off as usize);
    let b = *p;
    if b == 0 {
        unpack_hex33(*p.add(1), ld8(p.add(2)), ld8(p.add(10)), tmp);
        &tmp[..]
    } else {
        get_verbatim_bytes(p, b)
    }
}

#[inline(always)]
unsafe fn hash_arena_rec(arena: &[u8], off: u32) -> u64 {
    let p = arena.as_ptr().add(off as usize);
    let b = *p;
    if b == 0 {
        hash_packed_33(*p.add(1), ld8(p.add(2)), ld8(p.add(10)))
    } else {
        let s = get_verbatim_bytes(p, b);
        hash_long_general(s.as_ptr(), s.len())
    }
}

#[inline(always)]
unsafe fn eq_arena_packed(arena: &[u8], off: u32, tag: u8, hi: u64, lo: u64) -> bool {
    let p = arena.as_ptr().add(off as usize);
    (*p == 0) && ((*p.add(1) == tag) & (ld8(p.add(2)) == hi) & (ld8(p.add(10)) == lo))
}

#[inline(always)]
unsafe fn eq_arena_arena(a1: &[u8], off1: u32, a2: &[u8], off2: u32) -> bool {
    let p1 = a1.as_ptr().add(off1 as usize);
    let p2 = a2.as_ptr().add(off2 as usize);
    let b1 = *p1;
    let b2 = *p2;
    if (b1 | b2) == 0 {
        (*p1.add(1) == *p2.add(1))
            & (ld8(p1.add(2)) == ld8(p2.add(2)))
            & (ld8(p1.add(10)) == ld8(p2.add(10)))
    } else if b1 != b2 {
        false
    } else {
        get_verbatim_bytes(p1, b1) == get_verbatim_bytes(p2, b2)
    }
}

#[inline(always)]
unsafe fn eq_arena_raw(arena: &[u8], off: u32, raw: &[u8]) -> bool {
    let p = arena.as_ptr().add(off as usize);
    let b = *p;
    if b == 0 {
        if raw.len() != 33 {
            return false;
        }
        let mut tmp = [0u8; 33];
        unpack_hex33(*p.add(1), ld8(p.add(2)), ld8(p.add(10)), &mut tmp);
        &tmp[..] == raw
    } else {
        get_verbatim_bytes(p, b) == raw
    }
}

/// Append-only byte arena backed by 2 MiB THP `SinkArena` (with seamless heap spillover).
/// Stores verified 33-byte uniform-case hex IDs as 18-byte records `[0, tag, hi: u64, lo: u64]`
/// (-47% bytes, zero `memcpy`/`memcmp`), and every other non-empty byte string verbatim
/// `[len, raw...]` (`len >= 1`, disjoint from escape marker `0`).
struct ArenaBytes {
    base: *mut u8,
    cap: usize,
    len: usize,
    heap: Vec<u8>,
    arena: Option<SinkArena>,
}

unsafe impl Send for ArenaBytes {}
unsafe impl Sync for ArenaBytes {}

impl ArenaBytes {
    fn in_arena(arena: SinkArena) -> Self {
        let cap = arena.bytes().min(u32::MAX as usize);
        let base = arena.base_ptr();
        Self { base, cap, len: 0, heap: Vec::new(), arena: Some(arena) }
    }

    fn new_heap(cap: usize) -> Self {
        Self {
            base: std::ptr::null_mut(),
            cap: 0,
            len: 0,
            heap: Vec::with_capacity(cap),
            arena: None,
        }
    }

    #[inline(always)]
    fn as_slice(&self) -> &[u8] {
        if self.cap != 0 {
            unsafe { std::slice::from_raw_parts(self.base, self.len) }
        } else {
            &self.heap
        }
    }

    #[inline(always)]
    fn push_packed33(&mut self, tag: u8, hi: u64, lo: u64) -> Option<u32> {
        let off = self.len;
        if off + 18 <= self.cap {
            unsafe {
                let dst = self.base.add(off);
                *dst = 0;
                *dst.add(1) = tag;
                dst.add(2).cast::<u64>().write_unaligned(hi);
                dst.add(10).cast::<u64>().write_unaligned(lo);
            }
            self.len = off + 18;
            return Some(off as u32);
        }
        self.push_packed33_slow(tag, hi, lo)
    }

    #[cold]
    #[inline(never)]
    fn push_packed33_slow(&mut self, tag: u8, hi: u64, lo: u64) -> Option<u32> {
        self.spill_to_heap(18);
        let off = self.heap.len();
        if off > u32::MAX as usize {
            return None;
        }
        self.heap.reserve(18);
        unsafe {
            let dst = self.heap.as_mut_ptr().add(off);
            *dst = 0;
            *dst.add(1) = tag;
            dst.add(2).cast::<u64>().write_unaligned(hi);
            dst.add(10).cast::<u64>().write_unaligned(lo);
            self.heap.set_len(off + 18);
        }
        Some(off as u32)
    }

    #[inline(always)]
    fn push_bytes(&mut self, s: &[u8]) -> Option<u32> {
        debug_assert!(!s.is_empty());
        if s.len() == 33 {
            if let Some((tag, hi, lo)) = unsafe { try_pack_hex33(s.as_ptr()) } {
                return self.push_packed33(tag, hi, lo);
            }
        }
        self.push_verbatim(s)
    }

    #[inline(always)]
    fn push_verbatim(&mut self, s: &[u8]) -> Option<u32> {
        let slen = s.len();
        let off = self.len;
        if slen < 255 && off + 1 + slen <= self.cap {
            unsafe {
                let dst = self.base.add(off);
                *dst = slen as u8;
                std::ptr::copy_nonoverlapping(s.as_ptr(), dst.add(1), slen);
            }
            self.len = off + 1 + slen;
            return Some(off as u32);
        }
        self.push_slow(s)
    }

    #[cold]
    #[inline(never)]
    fn spill_to_heap(&mut self, extra: usize) {
        if self.cap != 0 {
            let need = self.len + extra;
            let new_cap = (self.cap * 2).max(need).max(4096);
            let mut heap = Vec::with_capacity(new_cap);
            unsafe {
                std::ptr::copy_nonoverlapping(self.base, heap.as_mut_ptr(), self.len);
                heap.set_len(self.len);
            }
            par::release_pages_par(self.base, self.cap, 1);
            self.heap = heap;
            self.cap = 0;
            self.len = 0;
        }
    }

    #[cold]
    #[inline(never)]
    fn push_slow(&mut self, s: &[u8]) -> Option<u32> {
        self.spill_to_heap(s.len() + 5);
        push_long_bytes(&mut self.heap, s)
    }

    fn into_arena(self) -> Option<SinkArena> {
        self.arena
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct PackedCacheSlot {
    hi: u64,
    lo: u64,
    off: u32,
    tag: u8, // 0 = empty; packed33 tags are always >= 0x30
}

/// Per-worker L2-resident (64 KiB) inline-value cache deduplicating repeated long `user_id`s
/// with ZERO random arena chases on hit while keeping Events SWCB + cache < 1 MiB/SMT pair!
struct LongUserCache {
    slots: Vec<PackedCacheSlot>,
    verb: Vec<(u32, u32)>,
}

impl LongUserCache {
    const CAP: usize = 2048;
    const MASK: usize = Self::CAP - 1;
    const VERB_CAP: usize = 2048;
    const VERB_MASK: usize = Self::VERB_CAP - 1;

    fn new() -> Self {
        Self { slots: vec![PackedCacheSlot::default(); Self::CAP], verb: Vec::new() }
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn intern_vec(&mut self, arena: &mut Vec<u8>, s: &[u8], uh: u64) -> Option<u32> {
        if self.verb.is_empty() {
            self.verb.resize(Self::VERB_CAP, (0, 0));
        }
        let fp = ((uh >> 32) as u32) | 1;
        let idx = (uh as usize) & Self::VERB_MASK;
        let slot = unsafe { self.verb.get_unchecked_mut(idx) };
        if slot.0 == fp && unsafe { get_long_bytes(arena, slot.1) } == s {
            return Some(slot.1);
        }
        let off = push_long_bytes(arena, s)?;
        *slot = (fp, off);
        Some(off)
    }

    #[inline(always)]
    fn intern_arena_packed33(
        &mut self,
        arena: &mut ArenaBytes,
        tag: u8,
        hi: u64,
        lo: u64,
        uh: u64,
    ) -> Option<u32> {
        let idx = (uh as usize) & Self::MASK;
        let slot = unsafe { self.slots.get_unchecked_mut(idx) };
        if (slot.tag == tag) & (slot.hi == hi) & (slot.lo == lo) {
            return Some(slot.off);
        }
        let off = arena.push_packed33(tag, hi, lo)?;
        *slot = PackedCacheSlot { hi, lo, off, tag };
        Some(off)
    }

    #[inline(always)]
    fn intern_arena(&mut self, arena: &mut ArenaBytes, s: &[u8], uh: u64) -> Option<u32> {
        if s.len() == 33 {
            if let Some((tag, hi, lo)) = unsafe { try_pack_hex33(s.as_ptr()) } {
                return self.intern_arena_packed33(arena, tag, hi, lo, uh);
            }
        }
        if self.verb.is_empty() {
            self.verb.resize(Self::VERB_CAP, (0, 0));
        }
        let fp = ((uh >> 32) as u32) | 1;
        let idx = (uh as usize) & Self::VERB_MASK;
        let slot = unsafe { self.verb.get_unchecked_mut(idx) };
        if slot.0 == fp && unsafe { eq_arena_raw(arena.as_slice(), slot.1, s) } {
            return Some(slot.1);
        }
        let off = arena.push_verbatim(s)?;
        *slot = (fp, off);
        Some(off)
    }
}

/// Read-only contiguous store of unique long user IDs that appeared in `links/`.
#[derive(Default)]
struct LinkLongStore {
    bytes: Vec<u8>,
    meta: Vec<(u64, u32)>,       // (offset, len)
    packed: Vec<(u8, u64, u64)>, // (tag != 0 => packed 33B hex; tag == 0 => verbatim)
}

impl LinkLongStore {
    #[inline(always)]
    unsafe fn get(&self, id: u32) -> &[u8] {
        let &(off, len) = self.meta.get_unchecked(id as usize);
        std::slice::from_raw_parts(self.bytes.as_ptr().add(off as usize), len as usize)
    }

    #[inline(always)]
    unsafe fn get_packed(&self, id: u32) -> (u8, u64, u64) {
        *self.packed.get_unchecked(id as usize)
    }
}

#[inline(always)]
fn make_parse_long_user(tid: usize, uh: u64, off: u32) -> u64 {
    LONG_BIT | ((tid as u64) << 57) | ((uh >> 39) << 32) | (off as u64)
}

#[inline(always)]
fn make_link_long_user(q_all: usize, uh: u64, link_id: u32) -> u64 {
    LONG_BIT
        | LINK_LONG_BIT
        | (((q_all as u64) & 0x3FF) << 52)
        | (((uh >> 39) & 0x000F_FFFF) << 32)
        | (link_id as u64)
}

#[inline(always)]
fn make_long_cid(tid: usize, off: u32) -> u64 {
    LONG_BIT | ((tid as u64) << 58) | (off as u64)
}

#[inline(always)]
fn canon_q_of(canon_tagged: u64, p2_short_shift: u32) -> usize {
    let sq = (canon_tagged >> p2_short_shift) as usize;
    let lq = ((canon_tagged >> 52) & 0x3FF) as usize;
    let mask = ((canon_tagged >> 63) as usize).wrapping_neg();
    (lq & mask) | (sq & !mask)
}

#[inline(always)]
fn long_user_hash25(tag: u64) -> u32 {
    ((tag >> 32) as u32) & 0x01FF_FFFF
}

#[inline(always)]
fn long_user_hash20(tag: u64) -> u32 {
    ((tag >> 32) as u32) & 0x000F_FFFF
}

#[inline(always)]
unsafe fn resolve_long_user<'a>(
    tag: u64,
    worker_arenas: &[&'a [u8]],
    link_store: &'a LinkLongStore,
    tmp: &'a mut [u8; 33],
) -> &'a [u8] {
    debug_assert!(!is_short(tag));
    if (tag & LINK_LONG_BIT) != 0 {
        link_store.get(tag as u32)
    } else {
        let tid = ((tag >> 57) & 31) as usize;
        resolve_arena_bytes(worker_arenas.get_unchecked(tid), tag as u32, tmp)
    }
}

#[inline(always)]
unsafe fn hash_long_user_tag(
    tag: u64,
    worker_arenas: &[&[u8]],
    link_store: &LinkLongStore,
) -> u64 {
    if (tag & LINK_LONG_BIT) != 0 {
        let lid = tag as u32;
        let (ptag, hi, lo) = link_store.get_packed(lid);
        if ptag != 0 {
            hash_packed_33(ptag, hi, lo)
        } else {
            let s = link_store.get(lid);
            hash_long_general(s.as_ptr(), s.len())
        }
    } else {
        let tid = ((tag >> 57) & 31) as usize;
        hash_arena_rec(worker_arenas.get_unchecked(tid), tag as u32)
    }
}

#[inline(always)]
unsafe fn eq_long_user_tags(
    a: u64,
    b: u64,
    worker_arenas: &[&[u8]],
    link_store: &LinkLongStore,
) -> bool {
    if a == b {
        return true;
    }
    let a_link = (a & LINK_LONG_BIT) != 0;
    let b_link = (b & LINK_LONG_BIT) != 0;
    if !a_link && !b_link {
        let ta = ((a >> 57) & 31) as usize;
        let tb = ((b >> 57) & 31) as usize;
        eq_arena_arena(
            worker_arenas.get_unchecked(ta),
            a as u32,
            worker_arenas.get_unchecked(tb),
            b as u32,
        )
    } else if a_link && b_link {
        // Unique IDs inside LinkLongStore -> distinct u32 ids are never equal.
        debug_assert_ne!(link_store.get(a as u32), link_store.get(b as u32));
        false
    } else if a_link {
        let tb = ((b >> 57) & 31) as usize;
        let lid = a as u32;
        let (ptag, hi, lo) = link_store.get_packed(lid);
        if ptag != 0 {
            eq_arena_packed(worker_arenas.get_unchecked(tb), b as u32, ptag, hi, lo)
        } else {
            eq_arena_raw(worker_arenas.get_unchecked(tb), b as u32, link_store.get(lid))
        }
    } else {
        let ta = ((a >> 57) & 31) as usize;
        let lid = b as u32;
        let (ptag, hi, lo) = link_store.get_packed(lid);
        if ptag != 0 {
            eq_arena_packed(worker_arenas.get_unchecked(ta), a as u32, ptag, hi, lo)
        } else {
            eq_arena_raw(worker_arenas.get_unchecked(ta), a as u32, link_store.get(lid))
        }
    }
}

#[inline(always)]
unsafe fn resolve_long_cid<'a>(
    tag: u64,
    cid_arenas: &[&'a [u8]],
    tmp: &'a mut [u8; 33],
) -> &'a [u8] {
    debug_assert!(!is_short(tag));
    let tid = ((tag >> 58) & 31) as usize;
    resolve_arena_bytes(cid_arenas.get_unchecked(tid), tag as u32, tmp)
}

#[inline(always)]
fn cmp_cid(a: u64, b: u64, cid_arenas: &[&[u8]]) -> CmpOrd {
    if ((a | b) as i64) >= 0 {
        return a.cmp(&b);
    }
    cmp_cid_slow(a, b, cid_arenas)
}

#[cold]
fn cmp_cid_slow(a: u64, b: u64, cid_arenas: &[&[u8]]) -> CmpOrd {
    let mut ba = [0u8; 33];
    let mut bb = [0u8; 33];
    let sa: &[u8] = if is_short(a) {
        let mut s16 = [0u8; 16];
        let n = a.unpack_buf(&mut s16);
        ba[..n].copy_from_slice(&s16[..n]);
        &ba[..n]
    } else {
        unsafe { resolve_long_cid(a, cid_arenas, &mut ba) }
    };
    let sb: &[u8] = if is_short(b) {
        let mut s16 = [0u8; 16];
        let n = b.unpack_buf(&mut s16);
        bb[..n].copy_from_slice(&s16[..n]);
        &bb[..n]
    } else {
        unsafe { resolve_long_cid(b, cid_arenas, &mut bb) }
    };
    sa.cmp(sb)
}

#[cfg(test)]
#[inline(always)]
fn eq_cid(a: u64, b: u64, cid_arenas: &[&[u8]]) -> bool {
    if a == b {
        return true;
    }
    if ((a & b) as i64) >= 0 {
        return false;
    }
    let ta = ((a >> 58) & 31) as usize;
    let tb = ((b >> 58) & 31) as usize;
    unsafe {
        eq_arena_arena(
            cid_arenas.get_unchecked(ta),
            a as u32,
            cid_arenas.get_unchecked(tb),
            b as u32,
        )
    }
}

// Cache-line-aligned table holding all CSV parser LUTs (2 adjacent L1d lines, 0 D1 misses).
#[repr(C, align(64))]
struct ParseLuts {
    vc_mask: [u64; 2],
    vc_view: [u64; 2],
    vc_cart: [u64; 2],
    delim_comma: [u32; 2],
    delim_lf: [u32; 2],
    ingest_f1: [u64; 2], // low32 = mask, high32 = expect
}

static PARSE_LUTS: ParseLuts = ParseLuts {
    vc_mask: [0x0000_00FF_FFFF_FFFF, 0x00FF_FFFF_FFFF_FFFF],
    vc_view: [
        u64::from_le_bytes(*b"view,\0\0\0"),
        u64::from_le_bytes(*b"\"view\",\0"),
    ],
    vc_cart: [
        u64::from_le_bytes(*b"cart,\0\0\0"),
        u64::from_le_bytes(*b"\"cart\",\0"),
    ],
    delim_comma: [
        ((b',' as u32) << 16) | 0x00FF,
        ((u16::from_le_bytes([b'"', b',']) as u32) << 16) | 0xFFFF,
    ],
    delim_lf: [
        ((b'\n' as u32) << 16) | 0x00FF,
        ((u16::from_le_bytes([b'"', b'\n']) as u32) << 16) | 0xFFFF,
    ],
    ingest_f1: [
        ((u32::from_le_bytes(*b"Z,\0\0") as u64) << 32) | 0x0000_FFFF,
        ((u32::from_le_bytes(*b"Z\",\0") as u64) << 32) | 0x00FF_FFFF,
    ],
};

// ============================================================================
// Direct variable-width CSV field parsers (branchless 8/9B + AVX2 33B opaque)
// ============================================================================

#[inline(always)]
unsafe fn open_delim<const DELIM: u8>(p: *const u8) -> (*const u8, u16, u16, usize) {
    let quoted = usize::from(*p == b'"');
    let me = if DELIM == b',' {
        *PARSE_LUTS.delim_comma.get_unchecked(quoted)
    } else if DELIM == b'\n' {
        *PARSE_LUTS.delim_lf.get_unchecked(quoted)
    } else {
        let lut: [u32; 2] = [
            ((DELIM as u32) << 16) | 0x00FF,
            ((u16::from_le_bytes([b'"', DELIM]) as u32) << 16) | 0xFFFF,
        ];
        *lut.get_unchecked(quoted)
    };
    (p.add(quoted), me as u16, (me >> 16) as u16, quoted)
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

/// Exact order-free 1..=9 non-NUL ASCII byte encoder for equality-only `event_id`s.
/// Matches `try_encode_eid_89` bit-for-bit on all 8/9-byte ASCII IDs while avoiding `bswap`
/// and overlapping `ld8(p+1)` loads on the hot path.
#[inline(always)]
unsafe fn encode_eid_short(p: *const u8, len: usize) -> Option<u64> {
    if !(1..=9).contains(&len) {
        return None;
    }
    let m0 = if len >= 8 { u64::MAX } else { (1u64 << (len * 8)) - 1 };
    let w0 = ld8(p) & m0;
    let filled = w0 | !m0;
    let zero = filled.wrapping_sub(0x0101_0101_0101_0101) & !filled;
    let b8 = if len == 9 { *p.add(8) } else { 0 };
    let bad_b8 = (len == 9) & (b8.wrapping_sub(1) >= 127);
    if bad_b8 | (((w0 | zero) & 0x8080_8080_8080_8080) != 0) {
        return None;
    }
    Some((u64::from(b8) << 56) | _pext_u64(w0, 0x7F7F_7F7F_7F7F_7F7F))
}

/// Branchless single-load 8/9-byte ASCII inline encoder reusing `b8 = end4 as u8` and a
/// precomputed `is9` (`0` for 8B, `1` for 9B) so callers avoid re-testing `d8 == 0`.
#[inline(always)]
unsafe fn try_encode_89(p: *const u8, end4: u32, is9: usize) -> Option<u64> {
    let w0 = ld8(p);
    let is9_mask = (is9 as u64).wrapping_neg();
    let b8_raw = end4 as u8;
    let bad_w0 = w0.wrapping_sub(0x3030_3030_3030_3030) | w0;
    let bad_b8 = (u64::from(b8_raw.wrapping_sub(0x30) | b8_raw)) & is9_mask;
    if ((bad_w0 | bad_b8) & 0x8080_8080_8080_8080) == 0 {
        let b8 = u64::from(b8_raw) & is9_mask;
        Some((_pext_u64(w0.swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F) << 7) | b8)
    } else {
        None
    }
}

/// Lean order-free 8/9-byte `event_id` encoder reusing `b8 = end4 as u8` and `is9` (`0` or `1`).
#[inline(always)]
unsafe fn try_encode_eid_89(p: *const u8, end4: u32, is9: usize) -> Option<u64> {
    let w0 = ld8(p);
    let is9_mask = (is9 as u64).wrapping_neg();
    let b8_raw = end4 as u8;
    let bad_w0 = w0.wrapping_sub(0x3030_3030_3030_3030) | w0;
    let bad_b8 = (u64::from(b8_raw.wrapping_sub(0x30) | b8_raw)) & is9_mask;
    if ((bad_w0 | bad_b8) & 0x8080_8080_8080_8080) == 0 {
        let b8 = u64::from(b8_raw) & is9_mask;
        Some((b8 << 56) | _pext_u64(w0, 0x7F7F_7F7F_7F7F_7F7F))
    } else {
        None
    }
}

/// 5-instruction AVX2 opaque 33-byte field validator (zero charset/hex assumption!).
#[inline(always)]
unsafe fn is_valid_opaque_33(p: *const u8) -> bool {
    let b0 = *p;
    let v = _mm256_loadu_si256(p.add(1).cast::<__m256i>());
    let bad = _mm256_or_si256(
        _mm256_or_si256(
            _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8)),
            _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8)),
        ),
        _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'\n' as i8)),
    );
    (_mm256_movemask_epi8(bad) == 0) & (b0 != b',') & (b0 != b'"') & (b0 != b'\n')
}

/// Guaranteed single-cmov zero test (`a == 0 || b == 0`) that LLVM cannot split into two
/// branches (eliminating ~6.5M random 8B-vs-9B branch mispredictions on 20M!).
#[inline(always)]
fn any_zero_u16(a: u16, b: u16) -> bool {
    let mut r = u32::from(b);
    let z = 0u32;
    unsafe {
        std::arch::asm!(
            "test {a:x}, {a:x}",
            "cmove {r:e}, {z:e}",
            a = in(reg) a,
            z = in(reg) z,
            r = inout(reg) r,
            options(pure, nomem, nostack),
        );
    }
    r == 0
}

#[inline(always)]
fn any_zero_u64(a: u64, b: u64) -> bool {
    let mut r = b;
    let z = 0u64;
    unsafe {
        std::arch::asm!(
            "test {a}, {a}",
            "cmove {r}, {z}",
            a = in(reg) a,
            z = in(reg) z,
            r = inout(reg) r,
            options(pure, nomem, nostack),
        );
    }
    r == 0
}

#[inline(always)]
unsafe fn parse_link_ep_delim<const DELIM: u8>(
    p: *const u8,
    tid: usize,
    long_bytes: &mut ArenaBytes,
    cache: &mut LongUserCache,
    flag: &Flag,
) -> Option<(u64, *const u8)> {
    let (p, mask, expect, quoted) = open_delim::<DELIM>(p);
    let end4 = p.add(8).cast::<u32>().read_unaligned();
    let d8 = ((end4 as u16) & mask) ^ expect;
    let d9 = (((end4 >> 8) as u16) & mask) ^ expect;
    if any_zero_u16(d8, d9) {
        let is9 = usize::from(d8 != 0);
        let key = try_encode_89(p, end4, is9)?;
        return Some((mix63(key), p.add(9 + is9 + quoted)));
    }
    if ends_delim(p, 33, mask, expect) {
        let b0 = *p;
        if b0.wrapping_sub(0x30) < 0x50 {
            if let Some((tag, hi, lo)) = try_pack_hex33(p) {
                let h = hash_packed_33(tag, hi, lo);
                let Some(off) = cache.intern_arena_packed33(long_bytes, tag, hi, lo, h) else {
                    flag.set(Need::Slow);
                    return None;
                };
                return Some((make_parse_long_user(tid, h, off), p.add(34 + quoted)));
            }
        }
        if is_valid_opaque_33(p) {
            let s = std::slice::from_raw_parts(p, 33);
            let h = hash_long(s);
            let Some(off) = cache.intern_arena(long_bytes, s, h) else {
                flag.set(Need::Slow);
                return None;
            };
            return Some((make_parse_long_user(tid, h, off), p.add(34 + quoted)));
        }
    }
    None
}

#[cold]
#[inline(never)]
unsafe fn parse_eid_33_cold(
    p: *const u8,
    quoted: usize,
    st: &mut ParseStateH,
    flag: &Flag,
) -> Option<(u64, usize, *const u8)> {
    if !is_valid_opaque_33(p) {
        return None;
    }
    let s = std::slice::from_raw_parts(p, 33);
    if let Some((tag, hi, lo)) = try_pack_hex33(p) {
        let eh = hash_packed_33(tag, hi, lo);
        let Some(off) = st.long_eids.push_packed33(tag, hi, lo) else {
            flag.set(Need::Slow);
            return None;
        };
        let eid_word = ((eh as u64) << 32) | (off as u64);
        let eid_p = st.p1_short + ((eh >> st.p1_long_shift) as usize);
        return Some((eid_word, eid_p, p.add(34 + quoted)));
    }
    let eh = hash_long_general(p, 33);
    let Some(off) = st.long_eids.push_verbatim(s) else {
        flag.set(Need::Slow);
        return None;
    };
    let eid_word = ((eh as u64) << 32) | (off as u64);
    let eid_p = st.p1_short + ((eh >> st.p1_long_shift) as usize);
    Some((eid_word, eid_p, p.add(34 + quoted)))
}

#[cold]
#[inline(never)]
unsafe fn parse_user_33_cold(
    p: *const u8,
    quoted: usize,
    st: &mut ParseStateH,
    flag: &Flag,
) -> Option<(u64, usize, *const u8)> {
    if !is_valid_opaque_33(p) {
        return None;
    }
    let s = std::slice::from_raw_parts(p, 33);
    let uh = hash_long(s);
    let Some(off) = st.user_cache.intern_arena(&mut st.long_users, s, uh) else {
        flag.set(Need::Slow);
        return None;
    };
    let q = st.p2_short + ((uh >> st.p2_long_shift) as usize);
    Some((make_parse_long_user(st.tid, uh, off), q, p.add(34 + quoted)))
}

#[cold]
#[inline(never)]
unsafe fn parse_cid_33_cold(
    p: *const u8,
    quoted: usize,
    st: &mut ParseStateH,
    flag: &Flag,
) -> Option<(u64, *const u8)> {
    if !is_valid_opaque_33(p) {
        return None;
    }
    let s = std::slice::from_raw_parts(p, 33);
    let Some(off) = st.long_cids.push_bytes(s) else {
        flag.set(Need::Slow);
        return None;
    };
    Some((make_long_cid(st.tid, off), p.add(34 + quoted)))
}

#[inline(always)]
unsafe fn parse_eid_delim<const DELIM: u8>(
    p: *const u8,
    st: &mut ParseStateH,
    flag: &Flag,
) -> Option<(u64, usize, *const u8)> {
    let (p, mask, expect, quoted) = open_delim::<DELIM>(p);
    let end4 = p.add(8).cast::<u32>().read_unaligned();
    let d8 = ((end4 as u16) & mask) ^ expect;
    let d9 = (((end4 >> 8) as u16) & mask) ^ expect;
    if any_zero_u16(d8, d9) {
        let is9 = usize::from(d8 != 0);
        let key = try_encode_eid_89(p, end4, is9)?;
        let eid_mixed = key.h();
        let eid_p = (eid_mixed >> st.p1_short_shift) as usize;
        return Some((eid_mixed, eid_p, p.add(9 + is9 + quoted)));
    }
    if ends_delim(p, 33, mask, expect) {
        let b0 = *p;
        if b0.wrapping_sub(0x30) < 0x50 {
            if let Some((tag, hi, lo)) = try_pack_hex33(p) {
                let eh = hash_packed_33(tag, hi, lo);
                let Some(off) = st.long_eids.push_packed33(tag, hi, lo) else {
                    flag.set(Need::Slow);
                    return None;
                };
                let eid_word = ((eh as u64) << 32) | (off as u64);
                let eid_p = st.p1_short + ((eh >> st.p1_long_shift) as usize);
                return Some((eid_word, eid_p, p.add(34 + quoted)));
            }
        }
        return parse_eid_33_cold(p, quoted, st, flag);
    }
    None
}

#[inline(always)]
unsafe fn parse_user_delim<const DELIM: u8>(
    p: *const u8,
    st: &mut ParseStateH,
    flag: &Flag,
) -> Option<(u64, usize, *const u8)> {
    let (p, mask, expect, quoted) = open_delim::<DELIM>(p);
    let end4 = p.add(8).cast::<u32>().read_unaligned();
    let d8 = ((end4 as u16) & mask) ^ expect;
    let d9 = (((end4 >> 8) as u16) & mask) ^ expect;
    if any_zero_u16(d8, d9) {
        let is9 = usize::from(d8 != 0);
        let key = try_encode_89(p, end4, is9)?;
        let m = mix63(key);
        let q = (m >> st.p2_short_shift) as usize;
        return Some((m, q, p.add(9 + is9 + quoted)));
    }
    if ends_delim(p, 33, mask, expect) {
        let b0 = *p;
        if b0.wrapping_sub(0x30) < 0x50 {
            if let Some((tag, hi, lo)) = try_pack_hex33(p) {
                let uh = hash_packed_33(tag, hi, lo);
                let Some(off) =
                    st.user_cache.intern_arena_packed33(&mut st.long_users, tag, hi, lo, uh)
                else {
                    flag.set(Need::Slow);
                    return None;
                };
                let q = st.p2_short + ((uh >> st.p2_long_shift) as usize);
                return Some((make_parse_long_user(st.tid, uh, off), q, p.add(34 + quoted)));
            }
        }
        return parse_user_33_cold(p, quoted, st, flag);
    }
    None
}

#[inline(always)]
unsafe fn parse_cid_delim<const DELIM: u8>(
    p: *const u8,
    st: &mut ParseStateH,
    flag: &Flag,
) -> Option<(u64, *const u8)> {
    let (p, mask, expect, quoted) = open_delim::<DELIM>(p);
    let end4 = p.add(8).cast::<u32>().read_unaligned();
    let d8 = ((end4 as u16) & mask) ^ expect;
    let d9 = (((end4 >> 8) as u16) & mask) ^ expect;
    if any_zero_u16(d8, d9) {
        let is9 = usize::from(d8 != 0);
        let key = try_encode_89(p, end4, is9)?;
        return Some((key, p.add(9 + is9 + quoted)));
    }
    if ends_delim(p, 33, mask, expect) {
        let b0 = *p;
        if b0.wrapping_sub(0x30) < 0x50 {
            if let Some((tag, hi, lo)) = try_pack_hex33(p) {
                let Some(off) = st.long_cids.push_packed33(tag, hi, lo) else {
                    flag.set(Need::Slow);
                    return None;
                };
                return Some((make_long_cid(st.tid, off), p.add(34 + quoted)));
            }
        }
        return parse_cid_33_cold(p, quoted, st, flag);
    }
    None
}

struct Tail {
    commas: u32,
    lf_bit: u32,
    lf: usize,
    quotes: u32,
}

#[inline(always)]
unsafe fn tail_impl<const VL: bool>(p: *const u8) -> Option<Tail> {
    let bytes = _mm256_loadu_si256(p.cast::<__m256i>());
    let lf_mask = if VL {
        vl_cmpeq_epi8_mask32(bytes, _mm256_set1_epi8(b'\n' as i8))
    } else {
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'\n' as i8))) as u32
    };
    if lf_mask == 0 {
        return None;
    }
    let lf = lf_mask.trailing_zeros();
    let lf_bit = lf_mask & lf_mask.wrapping_neg();
    let mask = lf_bit - 1;
    let commas = if VL {
        vl_mask_cmpeq_epi8_mask32(mask, bytes, _mm256_set1_epi8(b',' as i8))
    } else {
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b',' as i8))) as u32 & mask
    };
    if commas.count_ones() != 1 {
        return None;
    }
    let quotes = if VL {
        vl_mask_cmpeq_epi8_mask32(mask, bytes, _mm256_set1_epi8(b'"' as i8))
    } else {
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8))) as u32 & mask
    };
    Some(Tail { commas, lf_bit, lf: lf as usize, quotes })
}

#[inline(always)]
unsafe fn tail(p: *const u8) -> Option<Tail> {
    tail_impl::<false>(p)
}

/// 128-bit fast-path tail scanner for `view`/`cart` (99.0% of tails are <= 16 bytes!),
/// falling back to 32-byte `tail_impl::<VL>` only for the rare 1% longer tails.
#[inline(always)]
unsafe fn tail_vc<const VL: bool>(p: *const u8) -> Option<Tail> {
    let v16 = _mm_loadu_si128(p.cast::<__m128i>());
    let lf16 = if VL {
        vl_cmpeq_epi8_mask16(v16, _mm_set1_epi8(b'\n' as i8))
    } else {
        _mm_movemask_epi8(_mm_cmpeq_epi8(v16, _mm_set1_epi8(b'\n' as i8))) as u32
    };
    if lf16 != 0 {
        let lf = lf16.trailing_zeros();
        let lf_bit = lf16 & lf16.wrapping_neg();
        let mask = lf_bit - 1;
        let commas = if VL {
            vl_mask_cmpeq_epi8_mask16(mask as u16, v16, _mm_set1_epi8(b',' as i8))
        } else {
            _mm_movemask_epi8(_mm_cmpeq_epi8(v16, _mm_set1_epi8(b',' as i8))) as u32 & mask
        };
        if commas.count_ones() != 1 {
            return None;
        }
        let quotes = if VL {
            vl_mask_cmpeq_epi8_mask16(mask as u16, v16, _mm_set1_epi8(b'"' as i8))
        } else {
            _mm_movemask_epi8(_mm_cmpeq_epi8(v16, _mm_set1_epi8(b'"' as i8))) as u32 & mask
        };
        return Some(Tail { commas, lf_bit, lf: lf as usize, quotes });
    }
    tail_impl::<VL>(p)
}

impl Tail {
    /// Branchlessly validate optional CSV wrapping quotes on `(product_id, price)` for
    /// `view`/`cart` in 10 bit/cmov ops; returns `Some(product_nonempty)`.
    #[inline(always)]
    fn wrapping_view_cart(&self) -> Option<bool> {
        let q = self.quotes;
        let c = self.commas;
        let c_next = c << 1;
        let lf_prev = self.lf_bit >> 1;
        let q0 = (q & 1) != 0;
        let f_exp = std::hint::select_unpredictable(q0, 1 | (c >> 1), 0);
        let l_exp = std::hint::select_unpredictable((q & c_next) != 0, c_next | lf_prev, 0);
        let bad = (q ^ (f_exp | l_exp)) | ((q & 1) & (c >> 1)) | ((q & c_next) & lf_prev);
        if bad != 0 {
            return None;
        }
        Some(c != std::hint::select_unpredictable(q0, 4, 1))
    }

    /// Branchlessly validate optional CSV wrapping quotes on `(product_id, price)` for
    /// `purchase`; returns `Some((comma_pos, qp, qc))`.
    #[inline(always)]
    fn wrapping_purchase(&self) -> Option<(usize, usize, usize)> {
        let q = self.quotes;
        let c = self.commas;
        let c_next = c << 1;
        let lf_prev = self.lf_bit >> 1;
        let first = q & 1;
        let last = u32::from((q & c_next) != 0);
        let first_mask = first.wrapping_neg();
        let last_mask = last.wrapping_neg();
        let exp = ((1 | (c >> 1)) & first_mask) | ((c_next | lf_prev) & last_mask);
        let bad = (q ^ exp) | (first_mask & c & 3) | (last_mask & (c_next & lf_prev));
        if bad != 0 {
            return None;
        }
        Some((c.trailing_zeros() as usize, first as usize, last as usize))
    }
}

#[inline(always)]
unsafe fn open_ingest_f1(raw: *const u8) -> Option<(*const u8, *const u8)> {
    let iq = usize::from(*raw == b'"');
    let p = raw.add(iq);
    let w = p.add(19).cast::<u32>().read_unaligned();
    let me = *PARSE_LUTS.ingest_f1.get_unchecked(iq);
    if (w & (me as u32)) != ((me >> 32) as u32) {
        return None;
    }
    Some((p, p.add(21 + iq)))
}

// ============================================================================
// Stage 1: Hybrid Links (mixed short u64 + arbitrary long byte strings)
// ============================================================================

#[derive(Clone, Copy)]
struct LongLinkPair {
    raw_hash: u64,
    canon_tagged: u64,
    raw_hi: u64,
    raw_lo: u64,
    raw_link_id: u32,
    raw_tag: u8,
}

struct HybridBucketLinks {
    has_links: bool,
    short_pairs: Vec<(u64, u64)>,
    short_offsets: Vec<usize>,
    long_pairs: Vec<LongLinkPair>,
    long_offsets: Vec<usize>,
    link_store: LinkLongStore,
}

impl HybridBucketLinks {
    fn empty() -> Self {
        Self {
            has_links: false,
            short_pairs: Vec::new(),
            short_offsets: Vec::new(),
            long_pairs: Vec::new(),
            long_offsets: Vec::new(),
            link_store: LinkLongStore::default(),
        }
    }

    #[inline(always)]
    fn short_part(&self, q: usize) -> &[(u64, u64)] {
        &self.short_pairs[self.short_offsets[q]..self.short_offsets[q + 1]]
    }

    #[inline(always)]
    fn long_part(&self, lq: usize) -> &[LongLinkPair] {
        &self.long_pairs[self.long_offsets[lq]..self.long_offsets[lq + 1]]
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
struct RelocBucket {
    keys: [u64; 4],
    vals: [u64; 4],
}

#[derive(Default)]
struct ShortRelocMap {
    buckets: Vec<RelocBucket>,
    mask: usize,
}

impl ShortRelocMap {
    fn build(&mut self, pairs: &[(u64, u64)]) {
        if pairs.is_empty() {
            self.buckets.clear();
            self.mask = 0;
            return;
        }
        let cap = pairs.len().next_power_of_two().max(4);
        self.buckets.clear();
        self.buckets.resize(cap, RelocBucket::default());
        self.mask = cap - 1;
        for &(raw_mixed, canon_tagged) in pairs {
            let mut slot = (raw_mixed as usize) & self.mask;
            'ins: loop {
                let b = unsafe { self.buckets.get_unchecked_mut(slot) };
                for lane in 0..4 {
                    if b.keys[lane] == 0 {
                        b.keys[lane] = raw_mixed;
                        b.vals[lane] = canon_tagged;
                        break 'ins;
                    }
                }
                slot = (slot + 1) & self.mask;
            }
        }
    }

    #[cfg(test)]
    #[inline(always)]
    fn get(&self, raw_mixed: u64) -> Option<u64> {
        if self.buckets.is_empty() {
            return None;
        }
        let (hit, v) = self.get_branchless(raw_mixed);
        if hit { Some(v) } else { None }
    }

    /// Branchless bucket-0 lookup (99%+ of lookups at 25% load finish without branching).
    #[inline(always)]
    fn get_branchless(&self, raw_mixed: u64) -> (bool, u64) {
        let slot = (raw_mixed as usize) & self.mask;
        let b = unsafe { self.buckets.get_unchecked(slot) };
        let keys = unsafe { _mm256_load_si256(b.keys.as_ptr().cast()) };
        let needle = unsafe { _mm256_set1_epi64x(raw_mixed as i64) };
        let hits = unsafe {
            _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(keys, needle)))
        } as u32;
        if (hits == 0) & (b.keys[3] != 0) {
            return self.get_overflow((slot + 1) & self.mask, raw_mixed);
        }
        let lane = (hits.trailing_zeros() & 3) as usize;
        (hits != 0, unsafe { *b.vals.get_unchecked(lane) })
    }

    #[cold]
    #[inline(never)]
    fn get_overflow(&self, mut slot: usize, raw_mixed: u64) -> (bool, u64) {
        let needle = unsafe { _mm256_set1_epi64x(raw_mixed as i64) };
        loop {
            let b = unsafe { self.buckets.get_unchecked(slot) };
            let keys = unsafe { _mm256_load_si256(b.keys.as_ptr().cast()) };
            let hits = unsafe {
                _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(keys, needle)))
            } as u32;
            if hits != 0 {
                return (true, unsafe { *b.vals.get_unchecked(hits.trailing_zeros() as usize) });
            }
            if b.keys[3] == 0 {
                return (false, 0);
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct LongRelocBucket {
    fps: [u32; 4],
    idxs: [u32; 4],
}

#[derive(Default)]
struct LongRelocMap {
    buckets: Vec<LongRelocBucket>,
    mask: usize,
    wide_hash: bool,
}

impl LongRelocMap {
    fn build(&mut self, pairs: &[LongLinkPair]) {
        if pairs.is_empty() {
            self.buckets.clear();
            self.mask = 0;
            self.wide_hash = false;
            return;
        }
        let cap = pairs.len().next_power_of_two().max(4);
        self.buckets.clear();
        self.buckets.resize(cap, LongRelocBucket::default());
        self.mask = cap - 1;
        self.wide_hash = cap > (1 << 15);
        for (idx, p) in pairs.iter().enumerate() {
            let h25 = (p.raw_hash >> 39) as u32;
            let fp = h25 | 0x8000_0000;
            let mut slot = if self.wide_hash {
                (p.raw_hash as usize) & self.mask
            } else {
                (h25 as usize) & self.mask
            };
            'ins: loop {
                let b = unsafe { self.buckets.get_unchecked_mut(slot) };
                for lane in 0..4 {
                    if b.fps[lane] == 0 {
                        b.fps[lane] = fp;
                        b.idxs[lane] = idx as u32;
                        break 'ins;
                    }
                }
                slot = (slot + 1) & self.mask;
            }
        }
    }

    #[inline(always)]
    fn get(
        &self,
        tag: u64,
        pairs: &[LongLinkPair],
        worker_arenas: &[&[u8]],
        link_store: &LinkLongStore,
    ) -> Option<u64> {
        if self.buckets.is_empty() {
            return None;
        }
        let h25 = long_user_hash25(tag);
        let fp = h25 | 0x8000_0000;
        let tid = ((tag >> 57) & 31) as usize;
        let off = tag as u32;
        let mut slot = if self.wide_hash {
            (unsafe { hash_arena_rec(worker_arenas.get_unchecked(tid), off) } as usize) & self.mask
        } else {
            (h25 as usize) & self.mask
        };
        let needle = unsafe { _mm_set1_epi32(fp as i32) };
        loop {
            let b = unsafe { self.buckets.get_unchecked(slot) };
            let fps = unsafe { _mm_load_si128(b.fps.as_ptr().cast()) };
            let mut hits = unsafe {
                _mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(fps, needle)))
            } as u32;
            while hits != 0 {
                let lane = hits.trailing_zeros() as usize;
                hits &= hits - 1;
                let entry = unsafe { *pairs.get_unchecked(*b.idxs.get_unchecked(lane) as usize) };
                let matched = if entry.raw_tag != 0 {
                    unsafe {
                        eq_arena_packed(
                            worker_arenas.get_unchecked(tid),
                            off,
                            entry.raw_tag,
                            entry.raw_hi,
                            entry.raw_lo,
                        )
                    }
                } else {
                    unsafe {
                        eq_arena_raw(
                            worker_arenas.get_unchecked(tid),
                            off,
                            link_store.get(entry.raw_link_id),
                        )
                    }
                };
                if matched {
                    return Some(entry.canon_tagged);
                }
            }
            if b.fps[3] == 0 {
                return None;
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

#[inline(always)]
fn uf_find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = unsafe { p.get_unchecked(x as usize) }.load(Ordering::Relaxed);
        if px == x {
            return x;
        }
        let gp = unsafe { p.get_unchecked(px as usize) }.load(Ordering::Relaxed);
        if gp != px {
            let _ = unsafe { p.get_unchecked(x as usize) }.compare_exchange_weak(
                px,
                gp,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        x = gp;
    }
}

/// Read-only root lookup after union-find completes: zero `lock cmpxchg` / cache-line bouncing!
#[inline(always)]
fn uf_find_ro(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = unsafe { p.get_unchecked(x as usize) }.load(Ordering::Relaxed);
        if px == x {
            return x;
        }
        x = px;
    }
}

#[inline(always)]
fn cmp_node_key(ka: u64, kb: u64, link_store: &LinkLongStore) -> CmpOrd {
    if ((ka | kb) as i64) >= 0 {
        return unmix63(ka).cmp(&unmix63(kb));
    }
    cmp_node_key_slow(ka, kb, link_store)
}

#[cold]
fn cmp_node_key_slow(ka: u64, kb: u64, link_store: &LinkLongStore) -> CmpOrd {
    let mut ba = [0u8; 16];
    let mut bb = [0u8; 16];
    let sa: &[u8] = if is_short(ka) {
        let n = unmix63(ka).unpack_buf(&mut ba);
        &ba[..n]
    } else {
        unsafe { link_store.get(ka as u32) }
    };
    let sb: &[u8] = if is_short(kb) {
        let n = unmix63(kb).unpack_buf(&mut bb);
        &bb[..n]
    } else {
        unsafe { link_store.get(kb as u32) }
    };
    sa.cmp(sb)
}

fn uf_union_min_hybrid(p: &[AtomicU32], keys: &[u64], link_store: &LinkLongStore, mut a: u32, mut b: u32) {
    loop {
        a = uf_find(p, a);
        b = uf_find(p, b);
        if a == b {
            return;
        }
        let ka = unsafe { *keys.get_unchecked(a as usize) };
        let kb = unsafe { *keys.get_unchecked(b as usize) };
        let (hi, lo) = if cmp_node_key(ka, kb, link_store) == CmpOrd::Greater {
            (a, b)
        } else {
            (b, a)
        };
        if unsafe { p.get_unchecked(hi as usize) }
            .compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

struct LinkParseWorker {
    tid: usize,
    pairs: Vec<(u64, u64)>,
    long_bytes: ArenaBytes,
    cache: LongUserCache,
    buf: Vec<u8>,
}

fn encode_link_slice(
    s: &[u8],
    tid: usize,
    long_bytes: &mut ArenaBytes,
    cache: &mut LongUserCache,
    flag: &Flag,
) -> Option<u64> {
    if let Some(k) = unsafe { u64::enc(s.as_ptr(), s.len()) } {
        Some(mix63(k))
    } else {
        let h = hash_long(s);
        let Some(off) = cache.intern_arena(long_bytes, s, h) else {
            flag.set(Need::Slow);
            return None;
        };
        Some(make_parse_long_user(tid, h, off))
    }
}

fn links_direct_hybrid(
    buf: &[u8],
    lo: usize,
    hi: usize,
    tid: usize,
    pairs: &mut Vec<(u64, u64)>,
    long_bytes: &mut ArenaBytes,
    cache: &mut LongUserCache,
    flag: &Flag,
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
            parse_link_ep_delim::<b','>(p, tid, long_bytes, cache, flag).and_then(|(a, next)| {
                parse_link_ep_delim::<b'\n'>(next, tid, long_bytes, cache, flag)
                    .map(|(b, eol)| (a, b, eol))
            })
        };
        if let Some((a, b, next)) = fast {
            if a != b {
                pairs.push((a, b));
            }
            p = next;
        } else {
            let start = unsafe { p.offset_from(base) as usize };
            let lf = start + scan::memchr(b'\n', &buf[start..hi]).expect("LF-terminated chunk");
            if let Some(f) = scan::row_fields::<2>(buf, start, lf) {
                if flen(f[0]) > 0 && flen(f[1]) > 0 {
                    if let (Some(a), Some(b)) = (
                        encode_link_slice(field(buf, f[0]), tid, long_bytes, cache, flag),
                        encode_link_slice(field(buf, f[1]), tid, long_bytes, cache, flag),
                    ) {
                        if a != b {
                            pairs.push((a, b));
                        }
                    }
                }
            }
            p = unsafe { base.add(lf + 1) };
        }
    }
}

/// 4-way AVX2 L1/L2-resident interning map for pre-mixed short link keys (48 KiB at 1024 buckets
/// vs 256 KiB scalar `Map63`, zero `mix63` recomputation!).
#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct LinkKey4([u64; 4]);

struct LinkShortMap4 {
    keys: Vec<LinkKey4>,
    vals: Vec<[u32; 4]>,
    mask: usize,
    len: usize,
    grow_at: usize,
}

impl LinkShortMap4 {
    fn with_capacity(n: usize) -> Self {
        let nb = ((n / 4) + 4).next_power_of_two().max(4);
        Self {
            keys: vec![LinkKey4::default(); nb],
            vals: vec![[0u32; 4]; nb],
            mask: nb - 1,
            len: 0,
            grow_at: nb * 11 / 4, // 68.75% load
        }
    }

    fn reset(&mut self, n: usize) {
        let nb = ((n / 4) + 4).next_power_of_two().max(4);
        if nb > self.keys.len() {
            self.keys.resize(nb, LinkKey4::default());
            self.vals.resize(nb, [0u32; 4]);
        }
        self.keys[..nb].fill(LinkKey4::default());
        self.mask = nb - 1;
        self.len = 0;
        self.grow_at = nb * 11 / 4;
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let old_nb = self.mask + 1;
        let new_nb = old_nb * 2;
        let old_keys = std::mem::replace(&mut self.keys, vec![LinkKey4::default(); new_nb]);
        let old_vals = std::mem::replace(&mut self.vals, vec![[0u32; 4]; new_nb]);
        self.mask = new_nb - 1;
        self.grow_at = new_nb * 11 / 4;
        for bi in 0..old_nb {
            let kb = old_keys[bi].0;
            let vb = old_vals[bi];
            for lane in 0..4 {
                let k = kb[lane];
                if k == 0 {
                    break;
                }
                let mut slot = (k as usize) & self.mask;
                'ins: loop {
                    let dst_k = &mut self.keys[slot].0;
                    for dl in 0..4 {
                        if dst_k[dl] == 0 {
                            dst_k[dl] = k;
                            self.vals[slot][dl] = vb[lane];
                            break 'ins;
                        }
                    }
                    slot = (slot + 1) & self.mask;
                }
            }
        }
    }

    #[inline(always)]
    fn insert(&mut self, k: u64, v: u32) -> (u32, bool) {
        let mut bi = (k as usize) & self.mask;
        let needle = unsafe { _mm256_set1_epi64x(k as i64) };
        let zero = unsafe { _mm256_setzero_si256() };
        loop {
            let kb = unsafe { self.keys.get_unchecked_mut(bi) };
            let kv = unsafe { _mm256_load_si256(kb.0.as_ptr().cast()) };
            let hits = unsafe {
                _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(kv, needle)))
            } as u32;
            if hits != 0 {
                let lane = hits.trailing_zeros() as usize;
                return (unsafe { *self.vals.get_unchecked(bi).get_unchecked(lane) }, false);
            }
            let empties = unsafe {
                _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(kv, zero)))
            } as u32;
            if empties != 0 {
                let lane = empties.trailing_zeros() as usize;
                unsafe {
                    *kb.0.get_unchecked_mut(lane) = k;
                    *self.vals.get_unchecked_mut(bi).get_unchecked_mut(lane) = v;
                }
                self.len += 1;
                if self.len >= self.grow_at {
                    self.grow();
                }
                return (v, true);
            }
            bi = (bi + 1) & self.mask;
        }
    }
}

fn read_links_hybrid(
    ctx: &Ctx<'_>,
    threads: usize,
    p2_short_bits: u32,
    p2_long_bits: u32,
    long_user_pct: usize,
    tm: &mut Timer,
    scratch_buf: &mut [MaybeUninit<u8>],
) -> Result<HybridBucketLinks, Need> {
    if ctx.link_chunks.is_empty() {
        tm.lap("links-parse");
        tm.lap("links-uf");
        return Ok(HybridBucketLinks::empty());
    }
    let flag = Flag(AtomicU8::new(0));
    let files = &ctx.files;
    let link_chunks = &ctx.link_chunks;
    let link_bytes: u64 = files.iter().filter(|f| f.kind == Kind::Links).map(|f| f.len).sum();
    let est_per_thr = ((link_bytes / 24) as usize / threads.max(1)).max(1024);
    let est_long_bytes = if long_user_pct > 0 {
        ((link_bytes as usize) * long_user_pct / 100 / threads.max(1)).min(32 << 20)
    } else {
        0
    };

    let mut workers: Vec<LinkParseWorker> = par::run(
        threads,
        link_chunks.len(),
        |tid| LinkParseWorker {
            tid,
            pairs: Vec::with_capacity(est_per_thr),
            long_bytes: ArenaBytes::new_heap(est_long_bytes),
            cache: LongUserCache::new(),
            buf: std::mem::take(&mut *ctx.readers[tid].lock().unwrap()),
        },
        |w, i| {
            if flag.get().is_some() {
                return;
            }
            let c = link_chunks[i];
            let fi = &files[c.file as usize];
            let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, &mut w.buf);
            if lo < hi {
                links_direct_hybrid(
                    &w.buf,
                    lo,
                    hi,
                    w.tid,
                    &mut w.pairs,
                    &mut w.long_bytes,
                    &mut w.cache,
                    &flag,
                );
            }
        },
    );
    for w in &mut workers {
        *ctx.readers[w.tid].lock().unwrap() = std::mem::take(&mut w.buf);
    }
    if let Some(n) = flag.get() {
        return Err(n);
    }
    tm.lap("links-parse");

    let mut pairs_by_w = Vec::with_capacity(workers.len());
    let mut raw_long_by_w = Vec::with_capacity(workers.len());
    for w in workers {
        pairs_by_w.push(w.pairs);
        raw_long_by_w.push(w.long_bytes);
    }
    let raw_long_slices: Vec<&[u8]> = raw_long_by_w.iter().map(ArenaBytes::as_slice).collect();
    let links = build_bucket_links_hybrid(
        &pairs_by_w,
        &raw_long_slices,
        p2_short_bits,
        p2_long_bits,
        threads,
        scratch_buf,
    )?;
    let to_drop: Vec<_> = pairs_by_w.into_iter().zip(raw_long_by_w).collect();
    par::drop_par(threads, to_drop);
    tm.lap("links-uf");
    Ok(links)
}

fn build_bucket_links_hybrid(
    pairs_by_w: &[Vec<(u64, u64)>],
    raw_long_by_w: &[&[u8]],
    p2_short_bits: u32,
    p2_long_bits: u32,
    threads: usize,
    scratch_buf: &mut [MaybeUninit<u8>],
) -> Result<HybridBucketLinks, Need> {
    let mut off = vec![0usize; pairs_by_w.len() + 1];
    for (i, v) in pairs_by_w.iter().enumerate() {
        off[i + 1] = off[i] + v.len();
    }
    let nl = off[pairs_by_w.len()];
    if nl == 0 {
        eprintln!("[ingest] links=0 linked_ids=0");
        return Ok(HybridBucketLinks::empty());
    }
    let off_ref = &off;

    // Split scratch into reusable regions so later stages reuse ALREADY-FAULTED pages.
    let slot_a_need = ((2 * nl) * 16 + pairs_by_w.len() * 64 + 4096).min(scratch_buf.len());
    let (slot_a, slot_bc) = scratch_buf.split_at_mut(slot_a_need);
    let slot_b_need = ((2 * nl) * 4 + 4096).min(slot_bc.len());

    #[inline(always)]
    fn ep_part(x: u64) -> usize {
        if is_short(x) {
            (x >> (63 - HLP_BITS)) as usize
        } else {
            ((x >> 48) & ((HLP - 1) as u64)) as usize
        }
    }

    let overflow_flag = AtomicU8::new(0);
    let (
        link_store,
        link_hashes,
        keys,
        parent,
        base,
        long_id_base,
        nn,
        total_long_ids,
    ) = {
        let (slot_b, slot_c) = slot_bc.split_at_mut(slot_b_need);

        struct Scattered<'a> {
            flat: ArenaVec<'a, (u64, u32)>,
            pos: [usize; HLP + 1],
        }
        let mut sc_a = Scratch::from_slice(slot_a);
        let scatter_space: Vec<_> = pairs_by_w
            .iter()
            .map(|p| Mutex::new(Some(sc_a.buffer::<(u64, u32)>(2 * p.len()))))
            .collect();
        let scattered: Vec<Scattered<'_>> =
            par::run(threads, pairs_by_w.len(), |_| Vec::new(), |res, t| {
                let mut cnt = [0usize; HLP];
                for &(a, b) in &pairs_by_w[t] {
                    cnt[ep_part(a)] += 1;
                    cnt[ep_part(b)] += 1;
                }
                let mut pos = [0usize; HLP + 1];
                for p in 0..HLP {
                    pos[p + 1] = pos[p] + cnt[p];
                }
                let mut cur = [0usize; HLP];
                cur.copy_from_slice(&pos[..HLP]);
                let mut flat = scatter_space[t].lock().unwrap().take().unwrap();
                let ptr = flat.as_mut_ptr();
                for (i, &(a, b)) in pairs_by_w[t].iter().enumerate() {
                    let e = 2 * (off_ref[t] + i) as u32;
                    let pa = ep_part(a);
                    let pb = ep_part(b);
                    unsafe {
                        ptr.add(cur[pa]).write((a, e));
                        cur[pa] += 1;
                        ptr.add(cur[pb]).write((b, e + 1));
                        cur[pb] += 1;
                    }
                }
                unsafe { flat.set_len(pos[HLP]) };
                res.push(Scattered { flat, pos });
            })
            .into_iter()
            .flatten()
            .collect();

        // Precompute exact per-partition upper bounds and disjoint offsets in `slot_c`
        // (ZERO mutex locks and ZERO per-partition heap allocations during interning!).
        let mut key_off = [0usize; HLP + 1];
        for p in 0..HLP {
            let n: usize = scattered.iter().map(|s| s.pos[p + 1] - s.pos[p]).sum();
            key_off[p + 1] = key_off[p] + n;
        }
        let mut sc_c = Scratch::from_slice(slot_c);
        let mut keys_staging = sc_c.buffer::<u64>(key_off[HLP]);
        let ks_ptr = SyncPtr(keys_staging.as_mut_ptr());

        struct InternPart {
            p: usize,
            wid: u16,
            key_start: usize,
            key_len: u32,
            lb_start: usize,
            lb_len: usize,
            lm_start: usize,
            lm_len: usize,
        }
        let mut sc_b = Scratch::from_slice(slot_b);
        let mut ep = sc_b.buffer::<u32>(2 * nl);
        let epp = SyncPtr(ep.as_mut_ptr());
        let scattered_ref = &scattered;
        let key_off_ref = &key_off;
        let ov_ref = &overflow_flag;
        let worker_outs = par::run(
            threads,
            HLP,
            |tid| {
                (
                    tid as u16,
                    Vec::<InternPart>::with_capacity(HLP / threads.max(1) + 8),
                    LinkShortMap4::with_capacity(2048),
                    Vec::<u64>::new(),
                    Vec::<u32>::new(),
                    Vec::<u8>::with_capacity(64 << 10),
                    Vec::<(u64, u32, u64, (u8, u64, u64))>::with_capacity(2048),
                )
            },
            move |(wid, res, short_m, long_tbl, long_node_ids, w_lb, w_lm), p| {
                let k_start = key_off_ref[p];
                let n = key_off_ref[p + 1] - k_start;
                short_m.reset(n);
                let mut lcap = ((n / 4).max(16) * 2).next_power_of_two();
                if long_tbl.len() < lcap {
                    long_tbl.resize(lcap, 0);
                }
                long_tbl[..lcap].fill(0);
                let mut lmask = lcap - 1;
                long_node_ids.clear();
                let mut k_len = 0u32;
                let lb_start = w_lb.len();
                let lm_start = w_lm.len();
                let p_hi = (p as u32) << 23;
                let mut tmp33 = [0u8; 33];

                let nw = scattered_ref.len();
                for step in 0..nw {
                    let s = &scattered_ref[(step + *wid as usize) % nw];
                    for &(k, e) in &s.flat[s.pos[p]..s.pos[p + 1]] {
                        let id = if is_short(k) {
                            let (id, ins) = short_m.insert(k, k_len);
                            if ins {
                                unsafe { ks_ptr.write(k_start + k_len as usize, k) };
                                k_len += 1;
                            }
                            id
                        } else {
                            let tid = ((k >> 57) & 31) as usize;
                            let arena = unsafe { *raw_long_by_w.get_unchecked(tid) };
                            let raw_ptr = unsafe { arena.as_ptr().add(k as u32 as usize) };
                            let b0 = unsafe { *raw_ptr };
                            let h25 = long_user_hash25(k);
                            let packed_opt = if b0 == 0 {
                                Some(unsafe {
                                    (*raw_ptr.add(1), ld8(raw_ptr.add(2)), ld8(raw_ptr.add(10)))
                                })
                            } else {
                                None
                            };
                            let mut slot = if lcap > (1 << 16) {
                                let fh = match packed_opt {
                                    Some((pt, hi, lo)) => hash_packed_33(pt, hi, lo),
                                    None => {
                                        let bytes = unsafe { get_verbatim_bytes(raw_ptr, b0) };
                                        unsafe { hash_long_general(bytes.as_ptr(), bytes.len()) }
                                    }
                                };
                                (fh as usize) & lmask
                            } else {
                                (h25 as usize) & lmask
                            };
                            loop {
                                let v = unsafe { *long_tbl.get_unchecked(slot) };
                                if v == 0 {
                                    let lidx = (w_lm.len() - lm_start) as u32;
                                    let nid = k_len;
                                    let aoff = w_lb.len() as u64;
                                    let (blen, full_h, pk) = match packed_opt {
                                        Some((pt, hi, lo)) => {
                                            unpack_hex33(pt, hi, lo, &mut tmp33);
                                            w_lb.extend_from_slice(&tmp33);
                                            (33u32, hash_packed_33(pt, hi, lo), (pt, hi, lo))
                                        }
                                        None => {
                                            let bytes = unsafe { get_verbatim_bytes(raw_ptr, b0) };
                                            w_lb.extend_from_slice(bytes);
                                            (
                                                bytes.len() as u32,
                                                unsafe {
                                                    hash_long_general(bytes.as_ptr(), bytes.len())
                                                },
                                                (0, 0, 0),
                                            )
                                        }
                                    };
                                    w_lm.push((aoff, blen, full_h, pk));
                                    long_node_ids.push(nid);
                                    unsafe {
                                        ks_ptr.write(
                                            k_start + k_len as usize,
                                            LONG_BIT | (lidx as u64),
                                        );
                                    }
                                    k_len += 1;
                                    unsafe {
                                        *long_tbl.get_unchecked_mut(slot) =
                                            ((h25 as u64) << 32) | (lidx as u64 + 1);
                                    }
                                    let lm_count = w_lm.len() - lm_start;
                                    if lm_count * 2 > lcap {
                                        lcap *= 2;
                                        lmask = lcap - 1;
                                        if long_tbl.len() < lcap {
                                            long_tbl.resize(lcap, 0);
                                        }
                                        long_tbl[..lcap].fill(0);
                                        let wide = lcap > (1 << 16);
                                        for (li, &(_, _, mh, _)) in
                                            w_lm[lm_start..].iter().enumerate()
                                        {
                                            let mh25 = (mh >> 39) as u32;
                                            let mut ms = if wide {
                                                (mh as usize) & lmask
                                            } else {
                                                (mh25 as usize) & lmask
                                            };
                                            while unsafe { *long_tbl.get_unchecked(ms) } != 0 {
                                                ms = (ms + 1) & lmask;
                                            }
                                            unsafe {
                                                *long_tbl.get_unchecked_mut(ms) =
                                                    ((mh25 as u64) << 32) | (li as u64 + 1);
                                            }
                                        }
                                    }
                                    break nid;
                                }
                                if (v >> 32) as u32 == h25 {
                                    let li = (v as u32 - 1) as usize;
                                    let (moff, mlen, _, mpk) =
                                        unsafe { *w_lm.get_unchecked(lm_start + li) };
                                    let matched = match packed_opt {
                                        Some(pk) => mpk == pk,
                                        None => {
                                            let bytes = unsafe { get_verbatim_bytes(raw_ptr, b0) };
                                            mpk.0 == 0
                                                && mlen as usize == bytes.len()
                                                && unsafe {
                                                    w_lb.get_unchecked(
                                                        moff as usize
                                                            ..moff as usize + mlen as usize,
                                                    )
                                                } == bytes
                                        }
                                    };
                                    if matched {
                                        break unsafe { *long_node_ids.get_unchecked(li) };
                                    }
                                }
                                slot = (slot + 1) & lmask;
                            }
                        };
                        unsafe { epp.write(e as usize, p_hi | id) };
                    }
                }
                if (k_len as usize) > MAX_HLP_LOCAL_ID {
                    ov_ref.store(1, Ordering::Relaxed);
                }
                res.push(InternPart {
                    p,
                    wid: *wid,
                    key_start: k_start,
                    key_len: k_len,
                    lb_start,
                    lb_len: w_lb.len() - lb_start,
                    lm_start,
                    lm_len: w_lm.len() - lm_start,
                });
            },
        );
        if overflow_flag.load(Ordering::Relaxed) != 0 {
            return Err(Need::Slow);
        }
        unsafe { ep.set_len(2 * nl) };
        drop(scattered);
        drop(scatter_space);
        drop(sc_a);

        let mut parts: Vec<InternPart> = Vec::with_capacity(HLP);
        let mut w_long_bytes: Vec<Vec<u8>> = Vec::with_capacity(worker_outs.len());
        let mut w_long_meta: Vec<Vec<(u64, u32, u64, (u8, u64, u64))>> =
            Vec::with_capacity(worker_outs.len());
        for (_, res, _, _, _, lb, lm) in worker_outs {
            parts.extend(res);
            w_long_bytes.push(lb);
            w_long_meta.push(lm);
        }
        parts.sort_unstable_by_key(|x| x.p);

        let mut base = vec![0u32; HLP + 1];
        let mut long_id_base = vec![0u32; HLP + 1];
        let mut long_byte_base = vec![0usize; HLP + 1];
        for p in 0..HLP {
            base[p + 1] = base[p] + parts[p].key_len;
            long_id_base[p + 1] = long_id_base[p] + parts[p].lm_len as u32;
            long_byte_base[p + 1] = long_byte_base[p] + parts[p].lb_len;
        }
        let nn = base[HLP] as usize;
        let total_long_ids = long_id_base[HLP] as usize;
        let total_long_bytes = long_byte_base[HLP];

        let mut link_bytes: Vec<u8> = Vec::with_capacity(total_long_bytes);
        let mut link_meta = Vec::with_capacity(total_long_ids);
        let mut link_packed = Vec::with_capacity(total_long_ids);
        let mut link_hashes = Vec::with_capacity(total_long_ids);
        if total_long_ids > 0 {
            unsafe {
                link_bytes.set_len(total_long_bytes);
                link_meta.set_len(total_long_ids);
                link_packed.set_len(total_long_ids);
                link_hashes.set_len(total_long_ids);
            }
        }
        // Reuse ALREADY-FAULTED `slot_a` (`scattered`'s pages) for `keys` + `parent`!
        let mut sc_kp = Scratch::from_slice(slot_a);
        let mut keys = sc_kp.buffer::<u64>(nn);
        let mut parent = sc_kp.buffer::<AtomicU32>(nn);
        {
            let (kp, pp) = (SyncPtr(keys.as_mut_ptr()), SyncPtr(parent.as_mut_ptr()));
            let lb_ptr = SyncPtr(link_bytes.as_mut_ptr());
            let lm_ptr = SyncPtr(link_meta.as_mut_ptr());
            let lpk_ptr = SyncPtr(link_packed.as_mut_ptr());
            let lh_ptr = SyncPtr(link_hashes.as_mut_ptr());
            let (parts_ref, base_ref, lib_ref, lbb_ref, wlb_ref, wlm_ref) =
                (&parts, &base, &long_id_base, &long_byte_base, &w_long_bytes, &w_long_meta);
            // Fused single barrier: materialize `link_store` AND `keys` + `parent` together.
            par::run(threads, HLP, |_| (), move |_, p| {
                let ip = &parts_ref[p];
                let s = base_ref[p] as usize;
                let lbase = lib_ref[p] as u64;
                let src = unsafe { ks_ptr.slice(ip.key_start, ip.key_len as usize) };
                for (k, &v) in src.iter().enumerate() {
                    let gv = if is_short(v) { v } else { LONG_BIT | (lbase + (v as u32 as u64)) };
                    unsafe {
                        kp.write(s + k, gv);
                        pp.write(s + k, AtomicU32::new((s + k) as u32));
                    }
                }
                if ip.lm_len > 0 {
                    let wid = ip.wid as usize;
                    let b_off = lbb_ref[p];
                    let id_off = lib_ref[p] as usize;
                    let lb_src = &wlb_ref[wid][ip.lb_start..ip.lb_start + ip.lb_len];
                    let lm_src = &wlm_ref[wid][ip.lm_start..ip.lm_start + ip.lm_len];
                    let lb_base = ip.lb_start as u64;
                    unsafe {
                        lb_ptr.copy_from(b_off, lb_src);
                        for (k, &(moff, mlen, h, pk)) in lm_src.iter().enumerate() {
                            lm_ptr.write(id_off + k, (b_off as u64 + (moff - lb_base), mlen));
                            lpk_ptr.write(id_off + k, pk);
                            lh_ptr.write(id_off + k, h);
                        }
                    }
                }
            });
        }
        unsafe {
            keys.set_len(nn);
            parent.set_len(nn);
        }
        drop(parts);
        drop(keys_staging);
        drop(w_long_bytes);
        drop(w_long_meta);
        let link_store = LinkLongStore { bytes: link_bytes, meta: link_meta, packed: link_packed };

        let node = |code: u32| base[(code >> 23) as usize] + (code & (MAX_HLP_LOCAL_ID as u32));
        let step = 1 << 16;
        let (parent_ref, ep_ref, keys_ref, ls_ref) = (&parent, &ep, &keys, &link_store);
        par::run(threads, nl.div_ceil(step), |_| (), |_, t| {
            for i in t * step..((t + 1) * step).min(nl) {
                let (a, b) = (node(ep_ref[2 * i]), node(ep_ref[2 * i + 1]));
                if a != b {
                    uf_union_min_hybrid(parent_ref, keys_ref, ls_ref, a, b);
                }
            }
        });
        drop(ep);
        (link_store, link_hashes, keys, parent, base, long_id_base, nn, total_long_ids)
    };

    let p2_short = 1usize << p2_short_bits;
    let p2_long = 1usize << p2_long_bits;
    let short_shift = 63 - p2_short_bits;
    let long_shift = 64 - p2_long_bits;
    let short_split = p2_short_bits > HLP_BITS;
    let short_sub_bits = p2_short_bits.saturating_sub(HLP_BITS);
    let short_sub = 1usize << short_sub_bits;
    let long_split = p2_long_bits > HLP_BITS;
    let long_sub_bits = p2_long_bits.saturating_sub(HLP_BITS);
    let long_sub = 1usize << long_sub_bits;
    debug_assert!(short_sub <= 16 && long_sub <= 16);

    // Reuse ALREADY-FAULTED `slot_bc` (`ep` + `keys_staging` pages) for `short_tmp` + `long_tmp`!
    let mut sc_tmp = Scratch::from_slice(slot_bc);
    let mut short_tmp = sc_tmp.buffer::<(u64, u64)>(nn);
    let mut long_tmp = sc_tmp.buffer::<LongLinkPair>(total_long_ids.max(1));
    let stmp_ptr = SyncPtr(short_tmp.as_mut_ptr());
    let ltmp_ptr = SyncPtr(long_tmp.as_mut_ptr());

    let mut short_sub_counts = if short_split { vec![0usize; p2_short] } else { Vec::new() };
    let mut long_sub_counts = if long_split { vec![0usize; p2_long] } else { Vec::new() };
    let mut short_plen = [0usize; HLP];
    let mut long_plen = [0usize; HLP];

    {
        let (base_ref, lib_ref, parent_ref, keys_ref, lh_ref, ls_ref) =
            (&base, &long_id_base, &parent, &keys, &link_hashes, &link_store);
        let sc_ptr = SyncPtr(short_sub_counts.as_mut_ptr());
        let lc_ptr = SyncPtr(long_sub_counts.as_mut_ptr());
        let sp_ptr = SyncPtr(short_plen.as_mut_ptr());
        let lp_ptr = SyncPtr(long_plen.as_mut_ptr());

        par::run(threads, HLP, |_| (), move |_, p| {
            let (s, e) = (base_ref[p] as usize, base_ref[p + 1] as usize);
            let ls = lib_ref[p] as usize;
            let sbase_q = p << short_sub_bits;
            let lbase_q = p << long_sub_bits;
            let mut slocal = [0usize; 16];
            let mut llocal = [0usize; 16];
            let mut sw = s;
            let mut lw = ls;

            for i in s..e {
                let r = uf_find_ro(parent_ref, i as u32) as usize;
                let c = unsafe { *keys_ref.get_unchecked(r) };
                let k = unsafe { *keys_ref.get_unchecked(i) };
                if c != k {
                    let canon_tagged = if is_short(c) {
                        c
                    } else {
                        let cid = c as u32;
                        let ch = unsafe { *lh_ref.get_unchecked(cid as usize) };
                        let clq = (ch >> long_shift) as usize;
                        make_link_long_user(p2_short + clq, ch, cid)
                    };
                    if is_short(k) {
                        if short_split {
                            let sub_i = ((k >> short_shift) as usize) - sbase_q;
                            slocal[sub_i] += 1;
                        }
                        unsafe { stmp_ptr.write(sw, (k, canon_tagged)) };
                        sw += 1;
                    } else {
                        let kid = k as u32;
                        let rh = unsafe { *lh_ref.get_unchecked(kid as usize) };
                        let (raw_tag, raw_hi, raw_lo) = unsafe { ls_ref.get_packed(kid) };
                        if long_split {
                            let sub_i = ((rh >> long_shift) as usize) - lbase_q;
                            llocal[sub_i] += 1;
                        }
                        unsafe {
                            ltmp_ptr.write(
                                lw,
                                LongLinkPair {
                                    raw_hash: rh,
                                    canon_tagged,
                                    raw_hi,
                                    raw_lo,
                                    raw_link_id: kid,
                                    raw_tag,
                                },
                            );
                        }
                        lw += 1;
                    }
                }
            }
            unsafe {
                sp_ptr.write(p, sw - s);
                lp_ptr.write(p, lw - ls);
                if short_split {
                    for sub_i in 0..short_sub {
                        sc_ptr.write(sbase_q + sub_i, slocal[sub_i]);
                    }
                }
                if long_split {
                    for sub_i in 0..long_sub {
                        lc_ptr.write(lbase_q + sub_i, llocal[sub_i]);
                    }
                }
            }
        });
    }
    drop(parent);
    drop(keys);
    drop(link_hashes);

    let mut short_lp_off = [0usize; HLP + 1];
    let mut short_offsets = Vec::with_capacity(p2_short + 1);
    let sacc: usize;
    if short_split {
        let mut acc = 0usize;
        for &c in &short_sub_counts {
            short_offsets.push(acc);
            acc += c;
        }
        short_offsets.push(acc);
        sacc = acc;
    } else {
        let mut acc = 0usize;
        for p in 0..HLP {
            short_lp_off[p] = acc;
            acc += short_plen[p];
        }
        short_lp_off[HLP] = acc;
        let merge_shift = HLP_BITS - p2_short_bits;
        for q in 0..=p2_short {
            short_offsets.push(short_lp_off[q << merge_shift]);
        }
        sacc = acc;
    }

    let mut long_lp_off = [0usize; HLP + 1];
    let mut long_offsets = Vec::with_capacity(p2_long + 1);
    let lacc: usize;
    if long_split {
        let mut acc = 0usize;
        for &c in &long_sub_counts {
            long_offsets.push(acc);
            acc += c;
        }
        long_offsets.push(acc);
        lacc = acc;
    } else {
        let mut acc = 0usize;
        for p in 0..HLP {
            long_lp_off[p] = acc;
            acc += long_plen[p];
        }
        long_lp_off[HLP] = acc;
        let merge_shift = HLP_BITS - p2_long_bits;
        for lq in 0..=p2_long {
            long_offsets.push(long_lp_off[lq << merge_shift]);
        }
        lacc = acc;
    }

    eprintln!(
        "[ingest] links={nl} linked_ids={nn} mapped={} (short={sacc} long={lacc} long_uniq={total_long_ids})",
        sacc + lacc
    );
    if sacc + lacc == 0 {
        return Ok(HybridBucketLinks::empty());
    }

    let mut short_pairs = Vec::with_capacity(sacc);
    let mut long_pairs = Vec::with_capacity(lacc);
    let sp_out = SyncPtr(short_pairs.as_mut_ptr());
    let lp_out = SyncPtr(long_pairs.as_mut_ptr());
    let (base_ref, lib_ref, soff_ref, loff_ref, splen_ref, lplen_ref, slp_ref, llp_ref) = (
        &base,
        &long_id_base,
        &short_offsets,
        &long_offsets,
        &short_plen,
        &long_plen,
        &short_lp_off,
        &long_lp_off,
    );

    par::run(threads, HLP, |_| (), move |_, p| {
        let slen = splen_ref[p];
        if slen > 0 {
            let src = unsafe { stmp_ptr.slice(base_ref[p] as usize, slen) };
            if !short_split {
                unsafe { sp_out.copy_from(slp_ref[p], src) };
            } else {
                let base_q = p << short_sub_bits;
                let mut cur = [0usize; 16];
                for sub_i in 0..short_sub {
                    cur[sub_i] = soff_ref[base_q + sub_i];
                }
                for &(rh, ch) in src.iter() {
                    let sub_i = ((rh >> short_shift) as usize) - base_q;
                    unsafe { sp_out.write(cur[sub_i], (rh, ch)) };
                    cur[sub_i] += 1;
                }
            }
        }
        let llen = lplen_ref[p];
        if llen > 0 {
            let src = unsafe { ltmp_ptr.slice(lib_ref[p] as usize, llen) };
            if !long_split {
                unsafe { lp_out.copy_from(llp_ref[p], src) };
            } else {
                let base_q = p << long_sub_bits;
                let mut cur = [0usize; 16];
                for sub_i in 0..long_sub {
                    cur[sub_i] = loff_ref[base_q + sub_i];
                }
                for &entry in src.iter() {
                    let sub_i = ((entry.raw_hash >> long_shift) as usize) - base_q;
                    unsafe { lp_out.write(cur[sub_i], entry) };
                    cur[sub_i] += 1;
                }
            }
        }
    });
    unsafe {
        short_pairs.set_len(sacc);
        long_pairs.set_len(lacc);
    }

    Ok(HybridBucketLinks {
        has_links: true,
        short_pairs,
        short_offsets,
        long_pairs,
        long_offsets,
        link_store,
    })
}

// ============================================================================
// Stage 2: Hybrid Direct Single-Pass Parse (Events + Clicks, Unified Disjoint Sinks)
// ============================================================================

struct ParseRowsH<'a> {
    dt: &'a DateTable,
    ws: i64,
    we: i64,
    win_len_ms: u64,
    ws_sec: i64,
    win_len_sec: u64,
    ws_whole_sec: bool,
    lookups: &'a super::Lookups,
    flag: &'a Flag,
}

impl ParseRowsH<'_> {
    #[inline(always)]
    fn clicks_base(&self) -> i64 {
        self.ws - WEEK_MS
    }
}

struct ParseStateH {
    tid: usize,
    tok: Sinks<Tok, SWCB_TOK>,
    b: Sinks<RecB12, SWCB_B>,
    ck: Sinks<RecC<u64>, SWCB_CK>,
    purch: Vec<CompactEv>,
    long_eids: ArenaBytes,
    long_users: ArenaBytes,
    long_cids: ArenaBytes,
    user_cache: LongUserCache,
    p1_short: usize,
    p1_short_shift: u32,
    p1_long_shift: u32,
    p2_short: usize,
    p2_short_shift: u32,
    p2_long_shift: u32,
}

impl ParseStateH {
    #[inline(always)]
    fn encode_eid_slice(&mut self, s: &[u8], flag: &Flag) -> Option<(u64, usize)> {
        if let Some(k) = unsafe { encode_eid_short(s.as_ptr(), s.len()) } {
            let m = k.h();
            Some((m, (m >> self.p1_short_shift) as usize))
        } else {
            let eh = hash_long(s);
            let Some(off) = self.long_eids.push_bytes(s) else {
                flag.set(Need::Slow);
                return None;
            };
            let word = ((eh as u64) << 32) | (off as u64);
            Some((word, self.p1_short + ((eh >> self.p1_long_shift) as usize)))
        }
    }

    #[inline(always)]
    fn encode_user_slice(&mut self, s: &[u8], flag: &Flag) -> Option<(u64, usize)> {
        if let Some(k) = unsafe { u64::enc(s.as_ptr(), s.len()) } {
            let m = mix63(k);
            Some((m, (m >> self.p2_short_shift) as usize))
        } else {
            let uh = hash_long(s);
            let Some(off) = self.user_cache.intern_arena(&mut self.long_users, s, uh) else {
                flag.set(Need::Slow);
                return None;
            };
            let q = self.p2_short + ((uh >> self.p2_long_shift) as usize);
            Some((make_parse_long_user(self.tid, uh, off), q))
        }
    }

    #[inline(always)]
    fn encode_cid_slice(&mut self, s: &[u8], flag: &Flag) -> Option<u64> {
        if let Some(k) = unsafe { u64::enc(s.as_ptr(), s.len()) } {
            Some(k)
        } else {
            let Some(off) = self.long_cids.push_bytes(s) else {
                flag.set(Need::Slow);
                return None;
            };
            Some(make_long_cid(self.tid, off))
        }
    }

    #[inline(always)]
    fn push_purch(&mut self, ts: u32, cc: u32, flag: &Flag) -> u32 {
        let idx = self.purch.len();
        if idx >= (1 << 26) {
            flag.set(Need::Slow);
            return 0;
        }
        self.purch.push(CompactEv::new(ts, cc));
        0x8000_0000u32 | ((self.tid as u32) << 26) | (idx as u32)
    }

    #[inline(always)]
    fn emit_valid_rec(
        &mut self,
        eid_word: u64,
        eid_p: usize,
        ingest: u32,
        person_tag: u64,
        user_q: usize,
        ts_or_purch: u32,
        flag: &Flag,
    ) {
        let idx = self.b.push_idx(user_q, RecB12::new(person_tag, ts_or_purch));
        if idx >= MAX_LOC_IDX {
            flag.set(Need::Slow);
            return;
        }
        let loc = ((user_q as u32) << LOC_IDX_BITS) | (idx as u32);
        self.tok.push(eid_p, Tok { eid: eid_word, ingest, loc });
    }

    #[inline(always)]
    fn emit_valid_nonpurch(
        &mut self,
        eid_word: u64,
        eid_p: usize,
        ingest: u32,
        person_tag: u64,
        user_q: usize,
        ts: u32,
        flag: &Flag,
    ) {
        let ts_or_purch = if (ts as i32) >= 0 { ts } else { self.push_purch(ts, 0, flag) };
        self.emit_valid_rec(eid_word, eid_p, ingest, person_tag, user_q, ts_or_purch, flag);
    }

    #[inline(always)]
    fn emit_valid_purch(
        &mut self,
        eid_word: u64,
        eid_p: usize,
        ingest: u32,
        person_tag: u64,
        user_q: usize,
        ts: u32,
        cc: u32,
        flag: &Flag,
    ) {
        let ts_or_purch = self.push_purch(ts, cc, flag);
        self.emit_valid_rec(eid_word, eid_p, ingest, person_tag, user_q, ts_or_purch, flag);
    }

    #[inline(always)]
    fn emit_valid(
        &mut self,
        eid_word: u64,
        eid_p: usize,
        ingest: u32,
        person_tag: u64,
        user_q: usize,
        ts: u32,
        cc: u32,
        flag: &Flag,
    ) {
        if cc == 0 {
            self.emit_valid_nonpurch(eid_word, eid_p, ingest, person_tag, user_q, ts, flag);
        } else {
            self.emit_valid_purch(eid_word, eid_p, ingest, person_tag, user_q, ts, cc, flag);
        }
    }

    #[inline(always)]
    fn emit_invalid(&mut self, eid_word: u64, eid_p: usize, ingest: u32) {
        self.tok.push(eid_p, Tok { eid: eid_word, ingest, loc: NO_REC });
    }
}

#[cold]
#[inline(never)]
unsafe fn parse_purchase_tail(
    rows: &ParseRowsH<'_>,
    p: *const u8,
    valid_time: bool,
    ts_rel: u32,
    st: &mut ParseStateH,
) -> Option<(bool, u32, *const u8)> {
    let tail = tail(p)?;
    let (comma, qp, qc) = tail.wrapping_purchase()?;
    let product_len = comma - 2 * qp;
    let mut valid = valid_time & (product_len != 0);
    let ts_or_purch = if valid {
        let len = tail.lf - comma - 1 - 2 * qc;
        if (1..=8).contains(&len) {
            if let Some(cents) = tsfast::digits8(p.add(comma + 1 + qc), len).map(|v| v as u32) {
                if (1..=10_000_000).contains(&cents) {
                    let cc = (rows.lookups.prod.get(p.add(qp), product_len) << 24) | cents;
                    st.push_purch(ts_rel, cc, rows.flag)
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
            return None;
        }
    } else {
        0
    };
    Some((valid, ts_or_purch, p.add(tail.lf + 1)))
}

#[inline(always)]
unsafe fn event_hybrid<const WS_WHOLE_SEC: bool, const SHORT_WIN: bool, const VL: bool>(
    rows: &ParseRowsH<'_>,
    p: *const u8,
    st: &mut ParseStateH,
) -> Option<*const u8> {
    let (eid_word, eid_p, p) = parse_eid_delim::<b','>(p, st, rows.flag)?;
    let (person_tag, user_q, p) = parse_user_delim::<b','>(p, st, rows.flag)?;
    let (p, mask, expect, q) = open(p);
    let (ts_rel, ingest, valid_time, after_ingest) = if ends(p, 13, mask, expect) {
        let (ingest_p, after) = open_ingest_f1(p.add(14 + q))?;
        let t = rows.dt.parse_f3_13(p)?;
        let ing = rows.dt.parse_f1_20_sec(ingest_p)?;
        if ing as u64 >= u32::MAX as u64 - 1 {
            return None;
        }
        let dt = t.wrapping_sub(rows.ws);
        let ok = ((dt as u64) < rows.win_len_ms) & (ing * 1000 - t <= ts::DAY_MS);
        (dt as u32, ing, ok, after)
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
        let (ts_rel, ok) = if WS_WHOLE_SEC || rows.ws_whole_sec {
            let dsec = ts_sec.wrapping_sub(rows.ws_sec);
            let ok = ((dsec as u64) < rows.win_len_sec) & (ing - ts_sec <= 86_400);
            ((dsec as u32).wrapping_mul(1000), ok)
        } else {
            let t = ts_sec * 1000;
            let dt = t.wrapping_sub(rows.ws);
            let ok = ((dt as u64) < rows.win_len_ms) & (ing * 1000 - t <= ts::DAY_MS);
            (dt as u32, ok)
        };
        (ts_rel, ing, ok, after)
    };
    // Direct 2-entry L1d-shared LUT for `view,` / `"view",` / `cart,` / `"cart",`.
    let eq = usize::from(*after_ingest == b'"');
    let ew_raw = ld8(after_ingest);
    let masked = ew_raw & *PARSE_LUTS.vc_mask.get_unchecked(eq);
    let (valid, ts_or_purch, next_p) = if any_zero_u64(
        masked ^ *PARSE_LUTS.vc_view.get_unchecked(eq),
        masked ^ *PARSE_LUTS.vc_cart.get_unchecked(eq),
    ) {
        let p = after_ingest.add(5 + eq + eq);
        let tail = tail_vc::<VL>(p)?;
        let prod_nonempty = tail.wrapping_view_cart()?;
        let valid = valid_time & prod_nonempty;
        let ts_or_purch = if SHORT_WIN || !valid || (ts_rel as i32) >= 0 {
            ts_rel
        } else {
            st.push_purch(ts_rel, 0, rows.flag)
        };
        (valid, ts_or_purch, p.add(tail.lf + 1))
    } else {
        let p = after_ingest.add(eq);
        let ew = ld8(p);
        let emask = if eq != 0 { 0xFFFFu16 } else { 0x00FFu16 };
        let eexpect = if eq != 0 { u16::from_le_bytes(*b"\",") } else { b',' as u16 };
        if (ew == u64::from_le_bytes(*b"purchase")) & ends(p, 8, emask, eexpect) {
            parse_purchase_tail(rows, p.add(9 + eq), valid_time, ts_rel, st)?
        } else {
            return None;
        }
    };
    // Single shared emit site (1 b.push_idx + 1 tok.push instead of 4 duplicated copies!).
    let loc = if valid {
        let idx = st.b.push_idx(user_q, RecB12::new(person_tag, ts_or_purch));
        if idx >= MAX_LOC_IDX {
            rows.flag.set(Need::Slow);
            return Some(next_p);
        }
        ((user_q as u32) << LOC_IDX_BITS) | (idx as u32)
    } else {
        NO_REC
    };
    st.tok.push(eid_p, Tok { eid: eid_word, ingest: (ingest + 1) as u32, loc });
    Some(next_p)
}

#[cold]
#[inline(never)]
fn event_slow_hybrid(
    rows: &ParseRowsH<'_>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    st: &mut ParseStateH,
) -> usize {
    let lf = lo + scan::memchr(b'\n', &buf[lo..hi]).expect("LF-terminated chunk");
    if let Some(f) = scan::row_fields::<7>(buf, lo, lf) {
        if flen(f[0]) == 0 {
            return lf + 1;
        }
        let Some((eid_word, eid_p)) = st.encode_eid_slice(field(buf, f[0]), rows.flag) else {
            return lf + 1;
        };
        let (ingest, ingest_ms): (u32, Option<i64>) =
            match unsafe { rows.dt.parse_f1_sec(fptr(buf, f[3]), flen(f[3])) } {
                Some(sec) if sec >= 0 && sec < (u32::MAX as i64 - 1) => {
                    ((sec + 1) as u32, Some(sec * 1000))
                }
                Some(_) => {
                    rows.flag.set(Need::Slow);
                    return lf + 1;
                }
                None => match fts(rows.dt, buf, f[3]) {
                    Some(ms) if ms >= 0 && ms % 1000 == 0 && ms / 1000 < (u32::MAX as i64 - 1) => {
                        ((ms / 1000 + 1) as u32, Some(ms))
                    }
                    Some(_) => {
                        rows.flag.set(Need::Slow);
                        return lf + 1;
                    }
                    None => (u32::MAX, None),
                },
            };
        let mut valid_out: Option<(u64, usize, u32, u32)> = None;
        'v: {
            let Some(ims) = ingest_ms else {
                break 'v;
            };
            if flen(f[1]) == 0 || flen(f[5]) == 0 {
                break 'v;
            }
            let t = match fts(rows.dt, buf, f[2]) {
                Some(t) if t >= rows.ws && t < rows.we => t,
                _ => break 'v,
            };
            if ims - t > ts::DAY_MS {
                break 'v;
            }
            let ty = f[4];
            let cents = if flen(ty) == 4 {
                let w = unsafe { std::ptr::read_unaligned(fptr(buf, ty) as *const u32) };
                if (w == u32::from_le_bytes(*b"view")) | (w == u32::from_le_bytes(*b"cart")) {
                    0
                } else {
                    break 'v;
                }
            } else if flen(ty) == 8
                && unsafe { std::ptr::read_unaligned(fptr(buf, ty) as *const u64) }
                    == u64::from_le_bytes(*b"purchase")
            {
                let pr = f[6];
                let lp = flen(pr);
                let v = if (1..=8).contains(&lp) {
                    unsafe { tsfast::digits8(fptr(buf, pr), lp) }.map(|v| v as u32)
                } else {
                    slow::parse_price(field(buf, pr))
                };
                match v {
                    Some(c) if (1..=10_000_000).contains(&c) => c,
                    _ => break 'v,
                }
            } else {
                break 'v;
            };
            let Some((person_tag, user_q)) = st.encode_user_slice(field(buf, f[1]), rows.flag) else {
                return lf + 1;
            };
            let ts_rel = (t - rows.ws) as u32;
            let cc = if cents != 0 {
                let cat = unsafe { rows.lookups.prod.get(fptr(buf, f[5]), flen(f[5])) };
                (cat << 24) | cents
            } else {
                0
            };
            valid_out = Some((person_tag, user_q, ts_rel, cc));
        }
        match valid_out {
            Some((person_tag, user_q, ts_rel, cc)) => {
                st.emit_valid(eid_word, eid_p, ingest, person_tag, user_q, ts_rel, cc, rows.flag)
            }
            None => st.emit_invalid(eid_word, eid_p, ingest),
        }
    }
    lf + 1
}

#[inline(always)]
unsafe fn click_hybrid<const VL: bool>(
    rows: &ParseRowsH<'_>,
    p: *const u8,
    st: &mut ParseStateH,
) -> Option<*const u8> {
    let (cid_tag, p) = parse_cid_delim::<b','>(p, st, rows.flag)?;
    let (person_tag, user_q, p) = parse_user_delim::<b','>(p, st, rows.flag)?;
    let (cp, _, _, cq) = open(p);
    let cv = _mm_loadu_si128(cp.cast::<__m128i>());
    const CAMP_DELIM: [i8; 2] = [b',' as i8, b'"' as i8];
    let (clen, bad_inside) = if VL {
        let delim_mask = vl_cmpeq_epi8_mask16(cv, _mm_set1_epi8(*CAMP_DELIM.get_unchecked(cq)));
        if delim_mask == 0 {
            return None;
        }
        let clen = delim_mask.trailing_zeros() as usize;
        let inside = _bzhi_u32(u32::MAX, clen as u32) as u16;
        let other_m = vl_mask_cmpeq_epi8_mask16(
            inside,
            cv,
            _mm_set1_epi8(*CAMP_DELIM.get_unchecked(cq ^ 1)),
        );
        let nl_m = vl_mask_cmpeq_epi8_mask16(inside, cv, _mm_set1_epi8(b'\n' as i8));
        (clen, (other_m | nl_m) != 0)
    } else {
        let v_delim = _mm_cmpeq_epi8(cv, _mm_set1_epi8(*CAMP_DELIM.get_unchecked(cq)));
        let delim_mask = _mm_movemask_epi8(v_delim) as u32;
        if delim_mask == 0 {
            return None;
        }
        let clen = delim_mask.trailing_zeros() as usize;
        let inside = _bzhi_u32(u32::MAX, clen as u32);
        let v_other = _mm_cmpeq_epi8(cv, _mm_set1_epi8(*CAMP_DELIM.get_unchecked(cq ^ 1)));
        let v_nls = _mm_cmpeq_epi8(cv, _mm_set1_epi8(b'\n' as i8));
        let all_special =
            _mm_movemask_epi8(_mm_or_si128(_mm_or_si128(v_delim, v_other), v_nls)) as u32;
        (clen, (all_special & inside) != 0)
    };
    if bad_inside | ((cq != 0) & (*cp.add(clen + 1) != b',')) {
        return None;
    }
    let after_camp = cp.add(clen + 1 + cq);
    let campaign = rows.lookups.camp.get(cp, clen);
    if campaign == INVALID_CAMP {
        let tv = _mm256_loadu_si256(after_camp.cast::<__m256i>());
        let lf = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(tv, _mm256_set1_epi8(b'\n' as i8)))
            as u32)
            .trailing_zeros() as usize;
        if lf < 32 {
            let m = _bzhi_u32(u32::MAX, lf as u32);
            let qc = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(tv, _mm256_set1_epi8(b'"' as i8)))
                as u32)
                & m;
            let cc = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(tv, _mm256_set1_epi8(b',' as i8)))
                as u32)
                & m;
            if (qc == 0) & (cc == 0) {
                return Some(after_camp.add(lf + 1));
            }
        }
        return None;
    }
    let (tp, tmask, texpect, tq) = open_delim::<b'\n'>(after_camp);
    let (t, end) = if ends_delim(tp, 13, tmask, texpect) {
        (rows.dt.parse_f3_13(tp)?, tp.add(14 + tq))
    } else if (*tp.add(19) == b'Z') & ends_delim(tp, 20, tmask, texpect) {
        (rows.dt.parse_f1_20_sec(tp)? * 1000, tp.add(21 + tq))
    } else if ends_delim(tp, 25, tmask, texpect) {
        (rows.dt.parse_f2_25(tp)?, tp.add(26 + tq))
    } else {
        return None;
    };
    let base = rows.clicks_base();
    if (t >= base) & (t < rows.we) {
        st.ck.push(user_q, RecC { person: person_tag, cid: cid_tag, ts: (t - base) as u32, campaign });
    }
    Some(end)
}

#[cold]
#[inline(never)]
fn click_slow_hybrid(
    rows: &ParseRowsH<'_>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    st: &mut ParseStateH,
) -> usize {
    let lf = lo + scan::memchr(b'\n', &buf[lo..hi]).expect("LF-terminated chunk");
    if let Some(f) = scan::row_fields::<4>(buf, lo, lf) {
        if flen(f[0]) == 0 || flen(f[1]) == 0 {
            return lf + 1;
        }
        let campaign = unsafe { rows.lookups.camp.get(fptr(buf, f[2]), flen(f[2])) };
        if campaign == INVALID_CAMP {
            return lf + 1;
        }
        let base = rows.clicks_base();
        let t = match fts(rows.dt, buf, f[3]) {
            Some(t) if t >= base && t < rows.we => t,
            _ => return lf + 1,
        };
        let Some(cid_tag) = st.encode_cid_slice(field(buf, f[0]), rows.flag) else {
            return lf + 1;
        };
        let Some((person_tag, user_q)) = st.encode_user_slice(field(buf, f[1]), rows.flag) else {
            return lf + 1;
        };
        st.ck.push(user_q, RecC { person: person_tag, cid: cid_tag, ts: (t - base) as u32, campaign });
    }
    lf + 1
}

#[inline(always)]
unsafe fn parse_events_loop<const WS_WHOLE_SEC: bool, const SHORT_WIN: bool, const VL: bool>(
    rows: &ParseRowsH<'_>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    st: &mut ParseStateH,
) {
    if lo >= hi {
        return;
    }
    assert!(hi < u32::MAX as usize && buf.len() >= hi + scan::PAD && buf[hi - 1] == b'\n');
    let base = buf.as_ptr();
    let mut p = base.add(lo);
    let end = base.add(hi);
    while p < end {
        p = match event_hybrid::<WS_WHOLE_SEC, SHORT_WIN, VL>(rows, p, st) {
            Some(next) => next,
            None => base.add(event_slow_hybrid(rows, buf, p.offset_from(base) as usize, hi, st)),
        };
    }
}

#[inline(never)]
fn parse_events_buf_hybrid_impl<const WS_WHOLE_SEC: bool, const SHORT_WIN: bool>(
    rows: &ParseRowsH<'_>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    st: &mut ParseStateH,
) {
    unsafe { parse_events_loop::<WS_WHOLE_SEC, SHORT_WIN, false>(rows, buf, lo, hi, st) };
}

#[target_feature(enable = "avx512bw,avx512vl")]
#[inline(never)]
unsafe fn parse_events_buf_hybrid_impl_vl<const WS_WHOLE_SEC: bool, const SHORT_WIN: bool>(
    rows: &ParseRowsH<'_>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    st: &mut ParseStateH,
) {
    parse_events_loop::<WS_WHOLE_SEC, SHORT_WIN, true>(rows, buf, lo, hi, st);
}

#[inline(always)]
fn parse_events_buf_hybrid(rows: &ParseRowsH<'_>, buf: &[u8], lo: usize, hi: usize, st: &mut ParseStateH) {
    if super::use_avx512vl() {
        unsafe {
            if rows.ws_whole_sec && rows.win_len_ms <= i32::MAX as u64 {
                parse_events_buf_hybrid_impl_vl::<true, true>(rows, buf, lo, hi, st);
            } else {
                parse_events_buf_hybrid_impl_vl::<false, false>(rows, buf, lo, hi, st);
            }
        }
    } else if rows.ws_whole_sec && rows.win_len_ms <= i32::MAX as u64 {
        parse_events_buf_hybrid_impl::<true, true>(rows, buf, lo, hi, st);
    } else {
        parse_events_buf_hybrid_impl::<false, false>(rows, buf, lo, hi, st);
    }
}

#[inline(always)]
unsafe fn parse_clicks_loop<const VL: bool>(
    rows: &ParseRowsH<'_>,
    buf: &[u8],
    lo: usize,
    hi: usize,
    st: &mut ParseStateH,
) {
    if lo >= hi {
        return;
    }
    assert!(hi < u32::MAX as usize && buf.len() >= hi + scan::PAD && buf[hi - 1] == b'\n');
    let base = buf.as_ptr();
    let mut p = base.add(lo);
    let end = base.add(hi);
    while p < end {
        p = match click_hybrid::<VL>(rows, p, st) {
            Some(next) => next,
            None => base.add(click_slow_hybrid(rows, buf, p.offset_from(base) as usize, hi, st)),
        };
    }
}

#[inline(never)]
fn parse_clicks_buf_hybrid_avx2(rows: &ParseRowsH<'_>, buf: &[u8], lo: usize, hi: usize, st: &mut ParseStateH) {
    unsafe { parse_clicks_loop::<false>(rows, buf, lo, hi, st) };
}

#[target_feature(enable = "avx512bw,avx512vl")]
#[inline(never)]
unsafe fn parse_clicks_buf_hybrid_vl(rows: &ParseRowsH<'_>, buf: &[u8], lo: usize, hi: usize, st: &mut ParseStateH) {
    parse_clicks_loop::<true>(rows, buf, lo, hi, st);
}

#[inline(always)]
fn parse_clicks_buf_hybrid(rows: &ParseRowsH<'_>, buf: &[u8], lo: usize, hi: usize, st: &mut ParseStateH) {
    if super::use_avx512vl() {
        unsafe { parse_clicks_buf_hybrid_vl(rows, buf, lo, hi, st) };
    } else {
        parse_clicks_buf_hybrid_avx2(rows, buf, lo, hi, st);
    }
}

struct InputSample {
    avg_ev_bytes: u64,
    avg_ck_bytes: u64,
    long_eid_pct: usize,
    long_user_pct: usize,
    long_cid_pct: usize,
    avg_long_eid_len: usize,
    avg_long_user_len: usize,
    avg_long_cid_len: usize,
}

/// Sample across up to 8 events files + 8 clicks files (head + middle windows, ~2,000 rows,
/// < 200 us) for accurate split-cap `Sinks` & `ArenaBytes` sizing with zero sampling skew.
fn sample_input_stats(files: &[FileInfo]) -> InputSample {
    const SAMPLE_BYTES: usize = 12288;
    let mut buf = vec![0u8; SAMPLE_BYTES + scan::PAD + 1];

    let read_window = |fi: &FileInfo, off: u64, buf: &mut [u8]| -> Option<(usize, usize)> {
        if off >= fi.len {
            return None;
        }
        let n = ((fi.len - off) as usize).min(SAMPLE_BYTES);
        let mut got = 0usize;
        while got < n {
            match fi.handle.read_at(&mut buf[got..n], off + got as u64) {
                Ok(0) => break,
                Ok(k) => got += k,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
        if got == 0 {
            return None;
        }
        buf[got..got + scan::PAD + 1].fill(0);
        // Always skip first partial/header line cleanly.
        let first_nl = scan::memchr(b'\n', &buf[..got])?;
        let pos = first_nl + 1;
        if pos >= got {
            return None;
        }
        let hi = if (off + got as u64) < fi.len {
            let p = buf[pos..got].iter().rposition(|&b| b == b'\n')?;
            pos + p + 1
        } else if buf[got - 1] != b'\n' {
            buf[got] = b'\n';
            got + 1
        } else {
            got
        };
        Some((pos, hi))
    };

    let (mut ev_rows, mut ev_bytes) = (0usize, 0usize);
    let (mut long_eid, mut long_eid_bytes) = (0usize, 0usize);
    let (mut long_user, mut long_user_bytes) = (0usize, 0usize);
    let mut ev_files_sampled = 0usize;
    for fi in files.iter().filter(|f| f.kind == Kind::Events && f.len > 64) {
        let mut offsets = [0u64, 0u64];
        let n_win = if fi.len > (SAMPLE_BYTES as u64) * 3 {
            offsets[1] = fi.len / 2;
            2
        } else {
            1
        };
        let mut f_rows = 0usize;
        for &off in &offsets[..n_win] {
            let Some((mut pos, hi)) = read_window(fi, off, &mut buf) else {
                continue;
            };
            let mut w_rows = 0usize;
            while pos < hi && w_rows < 125 && ev_rows < 2000 {
                let Some(rel_lf) = scan::memchr(b'\n', &buf[pos..hi]) else {
                    break;
                };
                let lf = pos + rel_lf;
                if let Some(f) = scan::row_fields::<7>(&buf, pos, lf) {
                    if flen(f[0]) > 0 {
                        ev_rows += 1;
                        f_rows += 1;
                        w_rows += 1;
                        ev_bytes += lf + 1 - pos;
                        let s0 = field(&buf, f[0]);
                        if unsafe { u64::enc(s0.as_ptr(), s0.len()) }.is_none() {
                            long_eid += 1;
                            long_eid_bytes += if s0.len() == 33 && unsafe { try_pack_hex33(s0.as_ptr()) }.is_some() {
                                17
                            } else {
                                s0.len()
                            };
                        }
                        let s1 = field(&buf, f[1]);
                        if !s1.is_empty() && unsafe { u64::enc(s1.as_ptr(), s1.len()) }.is_none() {
                            long_user += 1;
                            long_user_bytes += if s1.len() == 33 && unsafe { try_pack_hex33(s1.as_ptr()) }.is_some() {
                                17
                            } else {
                                s1.len()
                            };
                        }
                    }
                }
                pos = lf + 1;
            }
        }
        if f_rows > 0 {
            ev_files_sampled += 1;
            if ev_files_sampled >= 8 || ev_rows >= 2000 {
                break;
            }
        }
    }

    let (mut ck_rows, mut ck_bytes, mut long_cid, mut long_cid_bytes) = (0usize, 0usize, 0usize, 0usize);
    let mut ck_files_sampled = 0usize;
    for fi in files.iter().filter(|f| f.kind == Kind::Clicks && f.len > 64) {
        let mut offsets = [0u64, 0u64];
        let n_win = if fi.len > (SAMPLE_BYTES as u64) * 3 {
            offsets[1] = fi.len / 2;
            2
        } else {
            1
        };
        let mut f_rows = 0usize;
        for &off in &offsets[..n_win] {
            let Some((mut pos, hi)) = read_window(fi, off, &mut buf) else {
                continue;
            };
            let mut w_rows = 0usize;
            while pos < hi && w_rows < 125 && ck_rows < 2000 {
                let Some(rel_lf) = scan::memchr(b'\n', &buf[pos..hi]) else {
                    break;
                };
                let lf = pos + rel_lf;
                if let Some(f) = scan::row_fields::<4>(&buf, pos, lf) {
                    if flen(f[0]) > 0 {
                        ck_rows += 1;
                        f_rows += 1;
                        w_rows += 1;
                        ck_bytes += lf + 1 - pos;
                        let s0 = field(&buf, f[0]);
                        if unsafe { u64::enc(s0.as_ptr(), s0.len()) }.is_none() {
                            long_cid += 1;
                            long_cid_bytes += if s0.len() == 33 && unsafe { try_pack_hex33(s0.as_ptr()) }.is_some() {
                                17
                            } else {
                                s0.len()
                            };
                        }
                    }
                }
                pos = lf + 1;
            }
        }
        if f_rows > 0 {
            ck_files_sampled += 1;
            if ck_files_sampled >= 8 || ck_rows >= 2000 {
                break;
            }
        }
    }

    let long_eid_pct = if ev_rows == 0 { 0 } else { (long_eid * 100) / ev_rows };
    let long_user_pct = if ev_rows == 0 { 0 } else { (long_user * 100) / ev_rows };
    let long_cid_pct = if ck_rows == 0 { 0 } else { (long_cid * 100) / ck_rows };
    let avg_long_eid_len = if long_eid == 0 { 33 } else { (long_eid_bytes / long_eid).clamp(16, 254) };
    let avg_long_user_len = if long_user == 0 { 33 } else { (long_user_bytes / long_user).clamp(16, 254) };
    let avg_long_cid_len = if long_cid == 0 { 33 } else { (long_cid_bytes / long_cid).clamp(16, 254) };
    let avg_ev_bytes = if ev_rows >= 8 {
        ((ev_bytes / ev_rows) as u64).clamp(40, 512)
    } else {
        75 + ((long_eid_pct as u64) * 52) / 100
    };
    let avg_ck_bytes = if ck_rows >= 8 {
        ((ck_bytes / ck_rows) as u64).clamp(24, 512)
    } else {
        45 + ((long_eid_pct as u64) * 52) / 100
    };
    InputSample {
        avg_ev_bytes,
        avg_ck_bytes,
        long_eid_pct,
        long_user_pct,
        long_cid_pct,
        avg_long_eid_len,
        avg_long_user_len,
        avg_long_cid_len,
    }
}

// ============================================================================
// Stage 3: Unified Single-Barrier Dedup (0..p1_short = short, p1_short..p1_total = long)
// ============================================================================

fn dedup_long_part(
    active: &[(&[Tok], u32, u32)],
    total: usize,
    long_eids: &[&[u8]],
    dead: &DeadEvents,
    t32: &mut Vec<DedupBucket32>,
    t64: &mut Vec<u64>,
) {
    if total == 0 {
        return;
    }
    let nsl = active.len();
    let get_flat = |idx: u32| -> (&Tok, u32) {
        let mut i = nsl - 1;
        while i > 0 && idx < active[i].1 {
            i -= 1;
        }
        let (sl, base, tid) = active[i];
        (unsafe { sl.get_unchecked((idx - base) as usize) }, tid)
    };

    if total < (1 << 18) - 1 {
        let nb = ((total * 5) / 14 + 1).next_power_of_two().max(4);
        let bmask = nb - 1;
        if t32.len() < nb {
            t32.resize(nb, DedupBucket32::default());
        }
        let table = &mut t32[..nb];
        table.fill(DedupBucket32::default());
        let fp_mask = unsafe { _mm_set1_epi32(0xFFFC_0000u32 as i32) };
        let zero = unsafe { _mm_setzero_si128() };
        let mut flat = 0u32;
        for &(sl, _, cur_tid) in active {
            let cur_arena = unsafe { *long_eids.get_unchecked(cur_tid as usize) };
            for r in sl {
                let h = (r.eid >> 32) as u32;
                let r_off = r.eid as u32;
                let fp_hi = (h & 0x7FFC_0000) | 0x8000_0000;
                let fp_vec = unsafe { _mm_set1_epi32(fp_hi as i32) };
                let mut bi = (h as usize) & bmask;
                'probe: loop {
                    let b = unsafe { table.get_unchecked_mut(bi) };
                    let vals = unsafe { _mm_load_si128(b.0.as_ptr().cast()) };
                    let mut hits = unsafe {
                        _mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(
                            _mm_and_si128(vals, fp_mask),
                            fp_vec,
                        )))
                    } as u32;
                    while hits != 0 {
                        let lane = hits.trailing_zeros() as usize;
                        hits &= hits - 1;
                        let prev = (unsafe { *b.0.get_unchecked(lane) } & 0x3FFFF) - 1;
                        let (w, prev_tid) = get_flat(prev);
                        if unsafe {
                            eq_arena_arena(
                                *long_eids.get_unchecked(prev_tid as usize),
                                w.eid as u32,
                                cur_arena,
                                r_off,
                            )
                        } {
                            if r.ingest < w.ingest {
                                dead.mark(prev_tid as usize, w.loc);
                                unsafe { *b.0.get_unchecked_mut(lane) = fp_hi | (flat + 1) };
                            } else {
                                dead.mark(cur_tid as usize, r.loc);
                            }
                            break 'probe;
                        }
                    }
                    let empties = unsafe {
                        _mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(vals, zero)))
                    } as u32;
                    if empties != 0 {
                        let lane = empties.trailing_zeros() as usize;
                        unsafe { *b.0.get_unchecked_mut(lane) = fp_hi | (flat + 1) };
                        break 'probe;
                    }
                    bi = (bi + 1) & bmask;
                }
                flat += 1;
            }
        }
    } else {
        let cap = (total * 2).next_power_of_two();
        let mask = cap - 1;
        if t64.len() < cap {
            t64.resize(cap, 0);
        }
        let table = &mut t64[..cap];
        table.fill(0);
        let mut flat = 0u32;
        for &(sl, _, cur_tid) in active {
            let cur_arena = unsafe { *long_eids.get_unchecked(cur_tid as usize) };
            for r in sl {
                let r_off = r.eid as u32;
                let h = unsafe { hash_arena_rec(cur_arena, r_off) };
                let fp = (h >> 32) as u32;
                let mut slot = (h as usize) & mask;
                loop {
                    let v = unsafe { *table.get_unchecked(slot) };
                    if v == 0 {
                        unsafe {
                            *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (flat as u64 + 1);
                        }
                        break;
                    }
                    if (v >> 32) as u32 == fp {
                        let prev = (v as u32).wrapping_sub(1);
                        let (w, prev_tid) = get_flat(prev);
                        if unsafe {
                            eq_arena_arena(
                                *long_eids.get_unchecked(prev_tid as usize),
                                w.eid as u32,
                                cur_arena,
                                r_off,
                            )
                        } {
                            if r.ingest < w.ingest {
                                dead.mark(prev_tid as usize, w.loc);
                                unsafe {
                                    *table.get_unchecked_mut(slot) =
                                        ((fp as u64) << 32) | (flat as u64 + 1);
                                }
                            } else {
                                dead.mark(cur_tid as usize, r.loc);
                            }
                            break;
                        }
                    }
                    slot = (slot + 1) & mask;
                }
                flat += 1;
            }
        }
    }
}

// ============================================================================
// Stage 4: Disjoint Pure-Short / Pure-Long Relocation (ZERO `is_short` branches!)
// ============================================================================

#[allow(clippy::too_many_arguments)]
fn relocate_hybrid(
    b_sinks: &[Sinks<RecB12, SWCB_B>],
    ck_sinks: &[Sinks<RecC<u64>, SWCB_CK>],
    worker_long_users: &[&[u8]],
    dead: &DeadEvents,
    links: &HybridBucketLinks,
    free_arenas: Vec<SinkArena>,
    p2_short: usize,
    p2_long: usize,
    p2_short_shift: u32,
    threads: usize,
) -> (Vec<Sinks<RecB12, SWCB_B>>, Vec<Sinks<RecC<u64>, SWCB_CK>>, Vec<SinkArena>) {
    let p2_total = p2_short + p2_long;
    let short_ev: usize = b_sinks
        .iter()
        .map(|s| s.len[..p2_short].iter().map(|&x| x as usize).sum::<usize>())
        .sum();
    let long_ev: usize = b_sinks
        .iter()
        .map(|s| s.len[p2_short..].iter().map(|&x| x as usize).sum::<usize>())
        .sum();
    let short_ck: usize = ck_sinks
        .iter()
        .map(|s| s.len[..p2_short].iter().map(|&x| x as usize).sum::<usize>())
        .sum();
    let long_ck: usize = ck_sinks
        .iter()
        .map(|s| s.len[p2_short..].iter().map(|&x| x as usize).sum::<usize>())
        .sum();
    let cap_rb_s = (short_ev * 52 / 100 / threads / p2_short).max(16) + 16;
    let cap_rb_l = if long_ev > 0 {
        (long_ev * 58 / 100 / threads / p2_long).max(16) + 16
    } else {
        16
    };
    let cap_rc_s = (short_ck * 52 / 100 / threads / p2_short).max(16) + 16;
    let cap_rc_l = if long_ck > 0 {
        (long_ck * 58 / 100 / threads / p2_long).max(16) + 16
    } else {
        16
    };
    let rb_bytes = Sinks::<RecB12, SWCB_B>::arena_bytes_split(p2_short, p2_long, cap_rb_s, cap_rb_l);
    let rc_bytes =
        Sinks::<RecC<u64>, SWCB_CK>::arena_bytes_split(p2_short, p2_long, cap_rc_s, cap_rc_l);

    let mut carved: Vec<Mutex<Option<(SinkArena, SinkArena)>>> = Vec::with_capacity(threads);
    let mut rem_arenas = Vec::with_capacity(free_arenas.len());
    let mut iter = free_arenas.into_iter();
    for _ in 0..threads {
        if let Some(orig) = iter.next() {
            let min_scratch = (orig.bytes() / 6).min(4 << 20);
            if rb_bytes + rc_bytes + min_scratch <= orig.bytes() {
                let (rb_a, rest) = orig.split_at(rb_bytes);
                let (rc_a, rem) = rest.split_at(rc_bytes);
                rem_arenas.push(rem);
                carved.push(Mutex::new(Some((rb_a, rc_a))));
                continue;
            }
            rem_arenas.push(orig);
        }
        carved.push(Mutex::new(None));
    }
    rem_arenas.extend(iter);

    struct RelocStateH {
        short_map: ShortRelocMap,
        long_map: LongRelocMap,
        reloc_b: Sinks<RecB12, SWCB_B>,
        reloc_c: Sinks<RecC<u64>, SWCB_CK>,
        move_buf_b: Vec<RecB12>,
        move_buf_c: Vec<RecC<u64>>,
    }

    let carved_ref = &carved;
    let out: Vec<(Sinks<RecB12, SWCB_B>, Sinks<RecC<u64>, SWCB_CK>)> = par::run_fini(
        threads,
        p2_total,
        |tid| {
            let (reloc_b, reloc_c) =
                if let Some((rb_a, rc_a)) = carved_ref[tid].lock().unwrap().take() {
                    (
                        Sinks::in_arena_split(p2_short, p2_long, cap_rb_s, cap_rb_l, rb_a),
                        Sinks::in_arena_split(p2_short, p2_long, cap_rc_s, cap_rc_l, rc_a),
                    )
                } else {
                    (
                        Sinks::new_split(p2_short, p2_long, cap_rb_s, cap_rb_l),
                        Sinks::new_split(p2_short, p2_long, cap_rc_s, cap_rc_l),
                    )
                };
            RelocStateH {
                short_map: ShortRelocMap::default(),
                long_map: LongRelocMap::default(),
                reloc_b,
                reloc_c,
                move_buf_b: vec![RecB12::new(0, 0); 1024],
                move_buf_c: vec![RecC { person: 0, cid: 0, ts: 0, campaign: 0 }; 512],
            }
        },
        |st, q| {
            if q < p2_short {
                let sp = links.short_part(q);
                if sp.is_empty() {
                    for (tid, sink) in b_sinks.iter().enumerate() {
                        unsafe {
                            sink.compact_part_with_dead_disjoint(q, dead.words(tid, q), |_| true);
                        }
                    }
                    return;
                }
                st.short_map.build(sp);
                let sm = &st.short_map;
                let reloc_b = &mut st.reloc_b;
                let mb_b = &mut st.move_buf_b;
                for (tid, sink) in b_sinks.iter().enumerate() {
                    unsafe {
                        sink.compact_and_buffer_with_dead_disjoint(
                            q,
                            dead.words(tid, q),
                            mb_b,
                            |r| {
                                let (hit, canon_tagged) = sm.get_branchless(r.person());
                                (hit, RecB12::new(canon_tagged, r.ts_or_purch))
                            },
                            |moved| {
                                for &r in moved {
                                    let cq = canon_q_of(r.person(), p2_short_shift);
                                    reloc_b.push(cq, r);
                                }
                            },
                        );
                    }
                }
                let reloc_c = &mut st.reloc_c;
                let mb_c = &mut st.move_buf_c;
                for sink in ck_sinks.iter() {
                    unsafe {
                        sink.compact_and_buffer_disjoint(
                            q,
                            mb_c,
                            |r| {
                                let (hit, canon_tagged) = sm.get_branchless(r.person);
                                (
                                    hit,
                                    RecC {
                                        person: canon_tagged,
                                        cid: r.cid,
                                        ts: r.ts,
                                        campaign: r.campaign,
                                    },
                                )
                            },
                            |moved| {
                                for &r in moved {
                                    let cq = canon_q_of(r.person, p2_short_shift);
                                    reloc_c.push(cq, r);
                                }
                            },
                        );
                    }
                }
            } else {
                let lq = q - p2_short;
                let lp = links.long_part(lq);
                if lp.is_empty() {
                    for (tid, sink) in b_sinks.iter().enumerate() {
                        unsafe {
                            sink.compact_part_with_dead_disjoint(q, dead.words(tid, q), |_| true);
                        }
                    }
                    return;
                }
                st.long_map.build(lp);
                let lm = &st.long_map;
                let reloc_b = &mut st.reloc_b;
                for (tid, sink) in b_sinks.iter().enumerate() {
                    unsafe {
                        sink.compact_part_with_dead_disjoint(q, dead.words(tid, q), |r| {
                            if let Some(canon_tagged) =
                                lm.get(r.person(), lp, worker_long_users, &links.link_store)
                            {
                                let cq = canon_q_of(canon_tagged, p2_short_shift);
                                reloc_b.push(cq, RecB12::new(canon_tagged, r.ts_or_purch));
                                false
                            } else {
                                true
                            }
                        });
                    }
                }
                let reloc_c = &mut st.reloc_c;
                for sink in ck_sinks.iter() {
                    unsafe {
                        sink.compact_part_disjoint(q, |_, r| {
                            if let Some(canon_tagged) =
                                lm.get(r.person, lp, worker_long_users, &links.link_store)
                            {
                                let cq = canon_q_of(canon_tagged, p2_short_shift);
                                reloc_c.push(
                                    cq,
                                    RecC {
                                        person: canon_tagged,
                                        cid: r.cid,
                                        ts: r.ts,
                                        campaign: r.campaign,
                                    },
                                );
                                false
                            } else {
                                true
                            }
                        });
                    }
                }
            }
        },
        |st| {
            st.reloc_b.finish();
            st.reloc_c.finish();
        },
    )
    .into_iter()
    .map(|st| (st.reloc_b, st.reloc_c))
    .collect();

    let (rb, rc) = out.into_iter().unzip();
    (rb, rc, rem_arenas)
}

// ============================================================================
// Stage 5: Disjoint Pure-Short / Pure-Long Direct Block-Chain Group + Aggregate
// ============================================================================

const EV_BLOCK_CAP: usize = 31;

/// 256-byte (4-cache-line) linked block of up to 31 `CompactEv`s per person.
/// Eliminates the 3-pass `ev_flat + pid -> fill -> ev_buf` CSR copy completely:
/// ~92% of persons fit in 1 block (sorted IN PLACE for `<= 16` events, or scattered in 1
/// `dense.scatter` call for `17..=31` events with ZERO `ev_buf` copy!).
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct EvBlock {
    next: u32,
    len: u32,
    evs: [MaybeUninit<CompactEv>; EV_BLOCK_CAP],
}

const _: () = assert!(std::mem::size_of::<EvBlock>() == 256);

impl EvBlock {
    #[inline(always)]
    unsafe fn init_first(dst: *mut Self, ev: CompactEv) {
        std::ptr::addr_of_mut!((*dst).next).write(u32::MAX);
        std::ptr::addr_of_mut!((*dst).len).write(1);
        (*dst).evs.as_mut_ptr().write(MaybeUninit::new(ev));
    }

    #[inline(always)]
    fn slice(&self) -> &[CompactEv] {
        unsafe { std::slice::from_raw_parts(self.evs.as_ptr().cast(), self.len as usize) }
    }

    #[inline(always)]
    fn slice_mut(&mut self) -> &mut [CompactEv] {
        unsafe { std::slice::from_raw_parts_mut(self.evs.as_mut_ptr().cast(), self.len as usize) }
    }
}

/// Register-cached cursor over `ArenaVec<'a, EvBlock>` during partition ingestion:
/// avoids `ArenaVec::Borrowed`/`Heap` enum dispatch on the 92%-hit `append` hot path.
struct EvBlockPool<'a, 'b> {
    ptr: *mut EvBlock,
    len: usize,
    cap: usize,
    vec: &'b mut ArenaVec<'a, EvBlock>,
}

impl<'a, 'b> EvBlockPool<'a, 'b> {
    #[inline(always)]
    fn new(vec: &'b mut ArenaVec<'a, EvBlock>) -> Self {
        vec.clear();
        let ptr = vec.as_mut_ptr();
        let cap = vec.capacity();
        Self { ptr, len: 0, cap, vec }
    }

    #[inline(always)]
    unsafe fn get_mut(&mut self, id: u32) -> &mut EvBlock {
        &mut *self.ptr.add(id as usize)
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        unsafe { self.vec.set_len(self.len) };
        self.vec.reserve(self.cap.max(4096));
        self.ptr = self.vec.as_mut_ptr();
        self.cap = self.vec.capacity();
    }

    #[inline(always)]
    fn alloc_first(&mut self, ev: CompactEv) -> u32 {
        if self.len == self.cap {
            self.grow();
        }
        let id = self.len;
        unsafe { EvBlock::init_first(self.ptr.add(id), ev) };
        self.len = id + 1;
        id as u32
    }
}

impl Drop for EvBlockPool<'_, '_> {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { self.vec.set_len(self.len) };
    }
}

/// 4-way AVX2 cache-line bucketed group map for pre-mixed short `u64` keys (50% max load,
/// 32 KiB L1d-resident per partition). Storing `tails` directly in the 64B bucket keeps the
/// bucket READ-ONLY in L1d on the 92%-hit path (`blk.len < 15`) without touching `persons`.
#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
struct ShortGroupBucket {
    keys: [u64; 4],
    ids: [u32; 4],
    tails: [u32; 4],
}

#[derive(Clone, Copy)]
struct ShortPersonHead {
    slot: u32,
    head: u32,
    full_blocks: u32,
}

struct ShortGroupMap4 {
    buckets: Vec<ShortGroupBucket>,
    persons: Vec<ShortPersonHead>,
    mask: usize,
    grow_at: usize,
}

impl ShortGroupMap4 {
    fn new() -> Self {
        Self { buckets: Vec::new(), persons: Vec::with_capacity(2048), mask: 0, grow_at: 0 }
    }

    fn reset(&mut self, est_keys: usize) {
        let nb = (est_keys / 2 + 4).next_power_of_two().max(4);
        self.buckets.clear();
        self.buckets.resize(nb, ShortGroupBucket::default());
        self.persons.clear();
        self.mask = nb - 1;
        self.grow_at = nb * 2;
    }

    #[cold]
    fn grow(&mut self) {
        let nb = self.buckets.len() * 2;
        let old = std::mem::replace(&mut self.buckets, vec![ShortGroupBucket::default(); nb]);
        self.mask = nb - 1;
        self.grow_at = nb * 2;
        for p in &mut self.persons {
            let obi = (p.slot >> 2) as usize;
            let olane = (p.slot & 3) as usize;
            let ob = unsafe { old.get_unchecked(obi) };
            let key = ob.keys[olane];
            let id = ob.ids[olane];
            let tail = ob.tails[olane];
            let mut bi = (key as usize) & self.mask;
            'ins: loop {
                let b = unsafe { self.buckets.get_unchecked_mut(bi) };
                for lane in 0..4 {
                    if b.keys[lane] == 0 {
                        b.keys[lane] = key;
                        b.ids[lane] = id;
                        b.tails[lane] = tail;
                        p.slot = ((bi as u32) << 2) | (lane as u32);
                        break 'ins;
                    }
                }
                bi = (bi + 1) & self.mask;
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn append_full(
        &mut self,
        bi: usize,
        lane: usize,
        tail: u32,
        ev: CompactEv,
        blocks: &mut EvBlockPool<'_, '_>,
    ) {
        let b = unsafe { self.buckets.get_unchecked_mut(bi) };
        let id = unsafe { *b.ids.get_unchecked(lane) } as usize;
        let new_tail = blocks.alloc_first(ev);
        unsafe {
            blocks.get_mut(tail).next = new_tail;
            *b.tails.get_unchecked_mut(lane) = new_tail;
            self.persons.get_unchecked_mut(id).full_blocks += 1;
        }
    }

    #[cold]
    #[inline(never)]
    fn append_new(
        &mut self,
        bi: usize,
        lane: usize,
        key: u64,
        ev: CompactEv,
        blocks: &mut EvBlockPool<'_, '_>,
    ) {
        let id = self.persons.len() as u32;
        let blk_id = blocks.alloc_first(ev);
        let b = unsafe { self.buckets.get_unchecked_mut(bi) };
        unsafe {
            *b.keys.get_unchecked_mut(lane) = key;
            *b.ids.get_unchecked_mut(lane) = id;
            *b.tails.get_unchecked_mut(lane) = blk_id;
        }
        self.persons.push(ShortPersonHead {
            slot: ((bi as u32) << 2) | (lane as u32),
            head: blk_id,
            full_blocks: 0,
        });
        if self.persons.len() >= self.grow_at {
            self.grow();
        }
    }

    #[inline(always)]
    fn append(&mut self, key: u64, ev: CompactEv, blocks: &mut EvBlockPool<'_, '_>) {
        debug_assert!(key != 0);
        let mut bi = (key as usize) & self.mask;
        let needle = unsafe { _mm256_set1_epi64x(key as i64) };
        let zero = unsafe { _mm256_setzero_si256() };
        loop {
            let b = unsafe { self.buckets.get_unchecked(bi) };
            let kv = unsafe { _mm256_load_si256(b.keys.as_ptr().cast()) };
            let hits = unsafe {
                _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(kv, needle)))
            } as u32;
            if hits != 0 {
                let lane = hits.trailing_zeros() as usize;
                let tail = unsafe { *b.tails.get_unchecked(lane) };
                let blk = unsafe { blocks.get_mut(tail) };
                let len = blk.len as usize;
                if len < EV_BLOCK_CAP {
                    unsafe { blk.evs.get_unchecked_mut(len).write(ev) };
                    blk.len = (len + 1) as u32;
                } else {
                    self.append_full(bi, lane, tail, ev, blocks);
                }
                return;
            }
            let empties = unsafe {
                _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(kv, zero)))
            } as u32;
            if empties != 0 {
                let lane = empties.trailing_zeros() as usize;
                self.append_new(bi, lane, key, ev, blocks);
                return;
            }
            bi = (bi + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, key: u64) -> Option<u32> {
        if self.persons.is_empty() {
            return None;
        }
        let mut bi = (key as usize) & self.mask;
        let needle = unsafe { _mm256_set1_epi64x(key as i64) };
        loop {
            let b = unsafe { self.buckets.get_unchecked(bi) };
            let kv = unsafe { _mm256_load_si256(b.keys.as_ptr().cast()) };
            let hits = unsafe {
                _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(kv, needle)))
            } as u32;
            if hits != 0 {
                return Some(unsafe { *b.ids.get_unchecked(hits.trailing_zeros() as usize) });
            }
            if b.keys[3] == 0 {
                return None;
            }
            bi = (bi + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.persons.len()
    }

    #[inline(always)]
    fn person(&self, id: usize) -> (u64, u32, u32, u32) {
        let p = unsafe { *self.persons.get_unchecked(id) };
        let b = unsafe { self.buckets.get_unchecked((p.slot >> 2) as usize) };
        let lane = (p.slot & 3) as usize;
        unsafe { (*b.keys.get_unchecked(lane), p.head, *b.tails.get_unchecked(lane), p.full_blocks) }
    }
}

#[derive(Clone, Copy)]
struct LongGroupEntry {
    tag: u64,
    head: u32,
    tail: u32,
    full_blocks: u32,
    ptag: u8,
    hi: u64,
    lo: u64,
}

#[inline(always)]
unsafe fn extract_packed_long(
    tag: u64,
    worker_arenas: &[&[u8]],
    link_store: &LinkLongStore,
) -> (u8, u64, u64) {
    if (tag & LINK_LONG_BIT) != 0 {
        link_store.get_packed(tag as u32)
    } else {
        let tid = ((tag >> 57) & 31) as usize;
        let arena = worker_arenas.get_unchecked(tid);
        let p = arena.as_ptr().add(tag as u32 as usize);
        if *p == 0 {
            (*p.add(1), ld8(p.add(2)), ld8(p.add(10)))
        } else {
            (0, 0, 0)
        }
    }
}

#[cold]
#[inline(never)]
unsafe fn eq_long_entry_slow(
    entry: &LongGroupEntry,
    tag: u64,
    worker_arenas: &[&[u8]],
    link_store: &LinkLongStore,
) -> bool {
    if entry.ptag != 0 && (tag & LINK_LONG_BIT) != 0 {
        if (entry.tag & LINK_LONG_BIT) != 0 {
            return false;
        }
        let (pt, h, l) = link_store.get_packed(tag as u32);
        return (pt == entry.ptag) & (h == entry.hi) & (l == entry.lo);
    }
    eq_long_user_tags(entry.tag, tag, worker_arenas, link_store)
}

#[inline(always)]
unsafe fn eq_long_entry(
    entry: &LongGroupEntry,
    tag: u64,
    worker_arenas: &[&[u8]],
    link_store: &LinkLongStore,
) -> bool {
    if entry.tag == tag {
        return true;
    }
    if (entry.ptag != 0) & ((tag & LINK_LONG_BIT) == 0) {
        let tb = ((tag >> 57) & 31) as usize;
        return eq_arena_packed(
            worker_arenas.get_unchecked(tb),
            tag as u32,
            entry.ptag,
            entry.hi,
            entry.lo,
        );
    }
    eq_long_entry_slow(entry, tag, worker_arenas, link_store)
}

struct LongGroupMap {
    table: Vec<u64>,
    entries: Vec<LongGroupEntry>,
    mask: usize,
    wide_hash: bool,
}

impl LongGroupMap {
    fn new() -> Self {
        Self { table: Vec::new(), entries: Vec::with_capacity(512), mask: 0, wide_hash: false }
    }

    fn reset(&mut self, est_keys: usize) {
        let cap = (est_keys * 2).next_power_of_two().max(16);
        self.table.clear();
        self.table.resize(cap, 0);
        self.entries.clear();
        self.mask = cap - 1;
        self.wide_hash = cap > (1 << 15);
    }

    #[cold]
    fn grow(&mut self, worker_arenas: &[&[u8]], link_store: &LinkLongStore) {
        let cap = self.table.len() * 2;
        self.table.clear();
        self.table.resize(cap, 0);
        self.mask = cap - 1;
        self.wide_hash = cap > (1 << 15);
        for (pid, e) in self.entries.iter().enumerate() {
            let h20 = long_user_hash20(e.tag);
            let mut slot = if self.wide_hash {
                let h = if e.ptag != 0 {
                    hash_packed_33(e.ptag, e.hi, e.lo)
                } else {
                    unsafe { hash_long_user_tag(e.tag, worker_arenas, link_store) }
                };
                (h as usize) & self.mask
            } else {
                (h20 as usize) & self.mask
            };
            while self.table[slot] != 0 {
                slot = (slot + 1) & self.mask;
            }
            self.table[slot] = ((h20 as u64) << 32) | (pid as u64 + 1);
        }
    }

    #[cold]
    #[inline(never)]
    fn append_new(
        &mut self,
        slot: usize,
        h20: u32,
        tag: u64,
        ev: CompactEv,
        blocks: &mut EvBlockPool<'_, '_>,
        worker_arenas: &[&[u8]],
        link_store: &LinkLongStore,
    ) {
        let pid = self.entries.len() as u32;
        let blk_id = blocks.alloc_first(ev);
        let (ptag, hi, lo) = unsafe { extract_packed_long(tag, worker_arenas, link_store) };
        self.entries.push(LongGroupEntry {
            tag,
            head: blk_id,
            tail: blk_id,
            full_blocks: 0,
            ptag,
            hi,
            lo,
        });
        unsafe { *self.table.get_unchecked_mut(slot) = ((h20 as u64) << 32) | (pid as u64 + 1) };
        if self.entries.len() * 2 > self.table.len() {
            self.grow(worker_arenas, link_store);
        }
    }

    #[cold]
    #[inline(never)]
    fn append_full(&mut self, pid: usize, tail: u32, ev: CompactEv, blocks: &mut EvBlockPool<'_, '_>) {
        let new_tail = blocks.alloc_first(ev);
        unsafe { blocks.get_mut(tail).next = new_tail };
        let entry = unsafe { self.entries.get_unchecked_mut(pid) };
        entry.tail = new_tail;
        entry.full_blocks += 1;
    }

    #[inline(always)]
    fn append(
        &mut self,
        tag: u64,
        ev: CompactEv,
        blocks: &mut EvBlockPool<'_, '_>,
        worker_arenas: &[&[u8]],
        link_store: &LinkLongStore,
    ) {
        let h20 = long_user_hash20(tag);
        let mut slot = if self.wide_hash {
            (unsafe { hash_long_user_tag(tag, worker_arenas, link_store) } as usize) & self.mask
        } else {
            (h20 as usize) & self.mask
        };
        loop {
            let v = unsafe { *self.table.get_unchecked(slot) };
            if v == 0 {
                self.append_new(slot, h20, tag, ev, blocks, worker_arenas, link_store);
                return;
            }
            if (v >> 32) as u32 == h20 {
                let pid = (v as u32 - 1) as usize;
                let entry = unsafe { self.entries.get_unchecked(pid) };
                if unsafe { eq_long_entry(entry, tag, worker_arenas, link_store) } {
                    let tail = entry.tail;
                    let blk = unsafe { blocks.get_mut(tail) };
                    let len = blk.len as usize;
                    if len < EV_BLOCK_CAP {
                        unsafe { blk.evs.get_unchecked_mut(len).write(ev) };
                        blk.len = (len + 1) as u32;
                    } else {
                        self.append_full(pid, tail, ev, blocks);
                    }
                    return;
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, tag: u64, worker_arenas: &[&[u8]], link_store: &LinkLongStore) -> Option<u32> {
        if self.entries.is_empty() {
            return None;
        }
        let h20 = long_user_hash20(tag);
        let mut slot = if self.wide_hash {
            (unsafe { hash_long_user_tag(tag, worker_arenas, link_store) } as usize) & self.mask
        } else {
            (h20 as usize) & self.mask
        };
        loop {
            let v = unsafe { *self.table.get_unchecked(slot) };
            if v == 0 {
                return None;
            }
            if (v >> 32) as u32 == h20 {
                let pid = (v as u32 - 1) as usize;
                let entry = unsafe { self.entries.get_unchecked(pid) };
                if unsafe { eq_long_entry(entry, tag, worker_arenas, link_store) } {
                    return Some(pid as u32);
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

#[inline(always)]
fn cswap_click_ts(a: &mut [CompactClick<u64>], i: usize, j: usize) {
    unsafe {
        let x = *a.get_unchecked(i);
        let y = *a.get_unchecked(j);
        let swap = x.ts > y.ts;
        let x0 = ((x.campaign as u64) << 32) | (x.ts as u64);
        let y0 = ((y.campaign as u64) << 32) | (y.ts as u64);
        let lo0 = std::hint::select_unpredictable(swap, y0, x0);
        let hi0 = std::hint::select_unpredictable(swap, x0, y0);
        let lo1 = std::hint::select_unpredictable(swap, y.cid, x.cid);
        let hi1 = std::hint::select_unpredictable(swap, x.cid, y.cid);
        *a.get_unchecked_mut(i) =
            CompactClick { ts: lo0 as u32, campaign: (lo0 >> 32) as u32, cid: lo1 };
        *a.get_unchecked_mut(j) =
            CompactClick { ts: hi0 as u32, campaign: (hi0 >> 32) as u32, cid: hi1 };
    }
}

#[cold]
#[inline(never)]
fn tiebreak_clicks_var(cl: &mut [CompactClick<u64>], cid_arenas: &[&[u8]]) {
    let n = cl.len();
    let mut i = 0usize;
    while i + 1 < n {
        let ti = unsafe { cl.get_unchecked(i).ts };
        if unsafe { cl.get_unchecked(i + 1).ts } == ti {
            let mut j = i + 2;
            while j < n && unsafe { cl.get_unchecked(j).ts } == ti {
                j += 1;
            }
            cl[i..j].sort_unstable_by(|a, b| cmp_cid(a.cid, b.cid, cid_arenas));
            i = j;
        } else {
            i += 1;
        }
    }
}

#[cold]
#[inline(never)]
fn radix_sort_clicks_var(
    cl: &mut [CompactClick<u64>],
    tmp: &mut ArenaVec<'_, CompactClick<u64>>,
    cid_arenas: &[&[u8]],
) {
    let n = cl.len();
    if tmp.len() < n {
        tmp.resize(n, CompactClick { ts: 0, campaign: 0, cid: 0 });
    }
    let buf = &mut tmp[..n];
    let mut c0 = [0u32; 2048];
    let mut c1 = [0u32; 2048];
    let mut c2 = [0u32; 1024];
    for c in cl.iter() {
        let t = c.ts;
        c0[(t & 2047) as usize] += 1;
        c1[((t >> 11) & 2047) as usize] += 1;
        c2[(t >> 22) as usize] += 1;
    }
    let t0 = cl[0].ts;
    let skip0 = c0[(t0 & 2047) as usize] == n as u32;
    let skip1 = c1[((t0 >> 11) & 2047) as usize] == n as u32;
    let skip2 = c2[(t0 >> 22) as usize] == n as u32;
    let mut in_tmp = false;

    if !skip0 {
        let mut acc = 0u32;
        for c in c0.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        for &c in cl.iter() {
            let b = (c.ts & 2047) as usize;
            let dst = unsafe { c0.get_unchecked_mut(b) };
            unsafe { *buf.get_unchecked_mut(*dst as usize) = c };
            *dst += 1;
        }
        in_tmp = true;
    }
    if !skip1 {
        let mut acc = 0u32;
        for c in c1.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        if in_tmp {
            for &c in buf.iter() {
                let b = ((c.ts >> 11) & 2047) as usize;
                let dst = unsafe { c1.get_unchecked_mut(b) };
                unsafe { *cl.get_unchecked_mut(*dst as usize) = c };
                *dst += 1;
            }
        } else {
            for &c in cl.iter() {
                let b = ((c.ts >> 11) & 2047) as usize;
                let dst = unsafe { c1.get_unchecked_mut(b) };
                unsafe { *buf.get_unchecked_mut(*dst as usize) = c };
                *dst += 1;
            }
        }
        in_tmp = !in_tmp;
    }
    if !skip2 {
        let mut acc = 0u32;
        for c in c2.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        if in_tmp {
            for &c in buf.iter() {
                let b = (c.ts >> 22) as usize;
                let dst = unsafe { c2.get_unchecked_mut(b) };
                unsafe { *cl.get_unchecked_mut(*dst as usize) = c };
                *dst += 1;
            }
        } else {
            for &c in cl.iter() {
                let b = (c.ts >> 22) as usize;
                let dst = unsafe { c2.get_unchecked_mut(b) };
                unsafe { *buf.get_unchecked_mut(*dst as usize) = c };
                *dst += 1;
            }
        }
        in_tmp = !in_tmp;
    }
    if in_tmp {
        cl.copy_from_slice(buf);
    }
    tiebreak_clicks_var(cl, cid_arenas);
}

#[inline(never)]
fn radix_sort_clicks_var_small(cl: &mut [CompactClick<u64>], cid_arenas: &[&[u8]]) {
    let n = cl.len();
    debug_assert!(n <= 256);
    let mut buf = [MaybeUninit::<CompactClick<u64>>::uninit(); 256];
    let mut c0 = [0u16; 256];
    let mut c1 = [0u16; 256];
    let mut c2 = [0u16; 256];
    let mut c3 = [0u16; 256];
    for c in cl.iter() {
        let t = c.ts;
        c0[(t & 0xFF) as usize] += 1;
        c1[((t >> 8) & 0xFF) as usize] += 1;
        c2[((t >> 16) & 0xFF) as usize] += 1;
        c3[(t >> 24) as usize] += 1;
    }
    let t0 = unsafe { cl.get_unchecked(0).ts };
    let n16 = n as u16;
    let skip0 = c0[(t0 & 0xFF) as usize] == n16;
    let skip1 = c1[((t0 >> 8) & 0xFF) as usize] == n16;
    let skip2 = c2[((t0 >> 16) & 0xFF) as usize] == n16;
    let skip3 = c3[(t0 >> 24) as usize] == n16;
    let mut in_buf = false;

    macro_rules! pass {
        ($counts:ident, $shift:expr, $skip:ident) => {
            if !$skip {
                let mut acc = 0u16;
                for c in $counts.iter_mut() {
                    let v = *c;
                    *c = acc;
                    acc += v;
                }
                if in_buf {
                    let src = unsafe {
                        std::slice::from_raw_parts(buf.as_ptr().cast::<CompactClick<u64>>(), n)
                    };
                    for &c in src {
                        let b = ((c.ts >> $shift) & 0xFF) as usize;
                        let dst = unsafe { $counts.get_unchecked_mut(b) };
                        let pos = *dst as usize;
                        *dst = (pos + 1) as u16;
                        unsafe { *cl.get_unchecked_mut(pos) = c };
                    }
                } else {
                    for &c in cl.iter() {
                        let b = ((c.ts >> $shift) & 0xFF) as usize;
                        let dst = unsafe { $counts.get_unchecked_mut(b) };
                        let pos = *dst as usize;
                        *dst = (pos + 1) as u16;
                        unsafe { buf.get_unchecked_mut(pos).write(c) };
                    }
                }
                in_buf = !in_buf;
            }
        };
    }

    pass!(c0, 0, skip0);
    pass!(c1, 8, skip1);
    pass!(c2, 16, skip2);
    pass!(c3, 24, skip3);

    if in_buf {
        unsafe {
            std::ptr::copy_nonoverlapping(
                buf.as_ptr().cast::<CompactClick<u64>>(),
                cl.as_mut_ptr(),
                n,
            );
        }
    }
    let mut eq_ts = false;
    for i in 1..n {
        eq_ts |= unsafe { cl.get_unchecked(i).ts == cl.get_unchecked(i - 1).ts };
    }
    if eq_ts {
        tiebreak_clicks_var(cl, cid_arenas);
    }
}

/// Unified L1i-lean click sorter for BOTH short and long click IDs:
/// sorts by `ts` only (~1.5 KiB code vs 11.5 KiB duplicated networks) and calls
/// `#[cold] tiebreak_clicks_var` only on rare (<0.1%) equal-millisecond timestamps.
#[inline(never)]
fn sort_clicks_var_only(
    cl: &mut [CompactClick<u64>],
    tmp: &mut ArenaVec<'_, CompactClick<u64>>,
    cid_arenas: &[&[u8]],
) {
    let n = cl.len();
    match n {
        0 | 1 => return,
        2 => {
            cswap_click_ts(cl, 0, 1);
            if unsafe { cl.get_unchecked(0).ts == cl.get_unchecked(1).ts } {
                tiebreak_clicks_var(cl, cid_arenas);
            }
            return;
        }
        3 => {
            cswap_click_ts(cl, 0, 2);
            cswap_click_ts(cl, 0, 1);
            cswap_click_ts(cl, 1, 2);
        }
        4 => {
            cswap_click_ts(cl, 0, 2);
            cswap_click_ts(cl, 1, 3);
            cswap_click_ts(cl, 0, 1);
            cswap_click_ts(cl, 2, 3);
            cswap_click_ts(cl, 1, 2);
        }
        5 => {
            for &(i, j) in &[(0, 1), (3, 4), (2, 4), (2, 3), (0, 3), (0, 2), (1, 4), (1, 3), (1, 2)] {
                cswap_click_ts(cl, i, j);
            }
        }
        6 => {
            for &(i, j) in &[
                (0, 5), (1, 3), (2, 4), (1, 2), (3, 4), (0, 3),
                (2, 5), (0, 1), (2, 3), (4, 5), (1, 2), (3, 4),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        7 => {
            for &(i, j) in &[
                (0, 6), (2, 3), (4, 5), (0, 2), (1, 4), (3, 6), (0, 1), (2, 5),
                (3, 4), (1, 2), (4, 6), (2, 3), (4, 5), (1, 2), (3, 4), (5, 6),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        8 => {
            for &(i, j) in &[
                (0, 2), (1, 3), (4, 6), (5, 7), (0, 4), (1, 5), (2, 6), (3, 7),
                (0, 1), (2, 3), (4, 5), (6, 7), (2, 4), (3, 5), (1, 4), (3, 6),
                (1, 2), (3, 4), (5, 6),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        9 => {
            for &(i, j) in &[
                (0, 3), (1, 7), (2, 5), (4, 8),
                (0, 7), (2, 4), (3, 8), (5, 6),
                (0, 2), (1, 3), (4, 5), (7, 8),
                (1, 4), (3, 6), (5, 7),
                (0, 1), (2, 4), (3, 5), (6, 8),
                (2, 3), (4, 5), (6, 7),
                (1, 2), (3, 4), (5, 6),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        10 => {
            for &(i, j) in &[
                (0, 8), (1, 9), (2, 7), (3, 5), (4, 6),
                (0, 2), (1, 4), (5, 8), (7, 9),
                (0, 3), (2, 4), (5, 7), (6, 9),
                (0, 1), (3, 6), (8, 9),
                (1, 5), (2, 3), (4, 8), (6, 7),
                (1, 2), (3, 5), (4, 6), (7, 8),
                (2, 3), (4, 5), (6, 7),
                (3, 4), (5, 6),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        11 => {
            for &(i, j) in &[
                (0, 1), (2, 3), (0, 2), (1, 3), (1, 2),
                (4, 5), (6, 7), (4, 6), (5, 7), (5, 6),
                (0, 4), (2, 6), (2, 4), (1, 5), (3, 7), (3, 5), (1, 2), (3, 4), (5, 6),
                (8, 9), (8, 10), (9, 10),
                (0, 8), (4, 8), (2, 10), (6, 10), (2, 4), (6, 8),
                (1, 9), (5, 9), (3, 5), (7, 9), (1, 2), (3, 4), (5, 6), (7, 8), (9, 10),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        12 => {
            for &(i, j) in &[
                (0, 1), (2, 3), (0, 2), (1, 3), (1, 2),
                (4, 5), (6, 7), (4, 6), (5, 7), (5, 6),
                (0, 4), (2, 6), (2, 4), (1, 5), (3, 7), (3, 5), (1, 2), (3, 4), (5, 6),
                (8, 9), (10, 11), (8, 10), (9, 11), (9, 10),
                (0, 8), (4, 8), (2, 10), (6, 10), (2, 4), (6, 8),
                (1, 9), (5, 9), (3, 11), (7, 11), (3, 5), (7, 9),
                (1, 2), (3, 4), (5, 6), (7, 8), (9, 10),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        13 => {
            for &(i, j) in &[
                (0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11),
                (0, 2), (1, 3), (4, 6), (5, 7), (8, 10), (9, 11),
                (0, 4), (1, 5), (2, 6), (3, 7), (8, 12),
                (0, 8), (1, 9), (2, 10), (3, 11), (4, 12),
                (5, 10), (6, 9), (3, 12), (7, 11), (1, 2), (4, 8),
                (1, 4), (2, 8), (2, 4), (5, 6), (9, 10), (3, 8), (7, 12),
                (6, 8), (10, 12), (3, 5), (7, 9),
                (3, 4), (5, 6), (7, 8), (9, 10), (11, 12), (6, 7), (8, 9),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        14 => {
            for &(i, j) in &[
                (0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13),
                (0, 2), (1, 3), (4, 6), (5, 7), (8, 10), (9, 11),
                (0, 4), (1, 5), (2, 6), (3, 7), (8, 12), (9, 13),
                (0, 8), (1, 9), (2, 10), (3, 11), (4, 12), (5, 13),
                (5, 10), (6, 9), (3, 12), (7, 11), (1, 2), (4, 8),
                (1, 4), (7, 13), (2, 8), (2, 4), (5, 6), (9, 10), (11, 13), (3, 8), (7, 12),
                (6, 8), (10, 12), (3, 5), (7, 9),
                (3, 4), (5, 6), (7, 8), (9, 10), (11, 12), (6, 7), (8, 9),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        15 => {
            for &(i, j) in &[
                (0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13),
                (0, 2), (1, 3), (4, 6), (5, 7), (8, 10), (9, 11), (12, 14),
                (0, 4), (1, 5), (2, 6), (3, 7), (8, 12), (9, 13), (10, 14),
                (0, 8), (1, 9), (2, 10), (3, 11), (4, 12), (5, 13), (6, 14),
                (5, 10), (6, 9), (3, 12), (13, 14), (7, 11), (1, 2), (4, 8),
                (1, 4), (7, 13), (2, 8), (11, 14),
                (2, 4), (5, 6), (9, 10), (11, 13), (3, 8), (7, 12),
                (6, 8), (10, 12), (3, 5), (7, 9),
                (3, 4), (5, 6), (7, 8), (9, 10), (11, 12), (6, 7), (8, 9),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        16 => {
            for &(i, j) in &[
                (0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13), (14, 15),
                (0, 2), (1, 3), (4, 6), (5, 7), (8, 10), (9, 11), (12, 14), (13, 15),
                (0, 4), (1, 5), (2, 6), (3, 7), (8, 12), (9, 13), (10, 14), (11, 15),
                (0, 8), (1, 9), (2, 10), (3, 11), (4, 12), (5, 13), (6, 14), (7, 15),
                (5, 10), (6, 9), (3, 12), (13, 14), (7, 11), (1, 2), (4, 8),
                (1, 4), (7, 13), (2, 8), (11, 14),
                (2, 4), (5, 6), (9, 10), (11, 13), (3, 8), (7, 12),
                (6, 8), (10, 12), (3, 5), (7, 9),
                (3, 4), (5, 6), (7, 8), (9, 10), (11, 12), (6, 7), (8, 9),
            ] {
                cswap_click_ts(cl, i, j);
            }
        }
        17..=20 => {
            for i in 1..n {
                let v = unsafe { *cl.get_unchecked(i) };
                let mut j = i;
                while j > 0 && unsafe { cl.get_unchecked(j - 1).ts } > v.ts {
                    unsafe { *cl.get_unchecked_mut(j) = *cl.get_unchecked(j - 1) };
                    j -= 1;
                }
                unsafe { *cl.get_unchecked_mut(j) = v };
            }
        }
        21..=79 => {
            cl.sort_unstable_by_key(|a| a.ts);
        }
        80..=256 => {
            radix_sort_clicks_var_small(cl, cid_arenas);
            return;
        }
        _ => {
            radix_sort_clicks_var(cl, tmp, cid_arenas);
            return;
        }
    }
    let mut eq_ts = false;
    for i in 1..n {
        eq_ts |= unsafe { cl.get_unchecked(i).ts == cl.get_unchecked(i - 1).ts };
    }
    if eq_ts {
        tiebreak_clicks_var(cl, cid_arenas);
    }
}

#[cfg(test)]
#[inline]
fn sort_and_dedup_clicks_var(
    cl: &mut [CompactClick<u64>],
    tmp: &mut ArenaVec<'_, CompactClick<u64>>,
    cid_arenas: &[&[u8]],
) -> usize {
    let n = cl.len();
    if n <= 1 {
        return n;
    }
    sort_clicks_var_only(cl, tmp, cid_arenas);
    let mut w = 1usize;
    let mut prev_cid = unsafe { cl.get_unchecked(0).cid };
    for r in 1..n {
        let cur = unsafe { *cl.get_unchecked(r) };
        if !eq_cid(cur.cid, prev_cid, cid_arenas) {
            unsafe { *cl.get_unchecked_mut(w) = cur };
            prev_cid = cur.cid;
            w += 1;
        }
    }
    w
}

#[inline(always)]
fn unpack_ev(ts_or_purch: u32, worker_purch: &[&[CompactEv]]) -> CompactEv {
    if (ts_or_purch as i32) >= 0 {
        CompactEv::new(ts_or_purch, 0)
    } else {
        let tid = ((ts_or_purch >> 26) & 31) as usize;
        let idx = (ts_or_purch & 0x03FF_FFFF) as usize;
        unsafe { *worker_purch.get_unchecked(tid).get_unchecked(idx) }
    }
}

struct DedupedHybrid {
    b_sinks: Vec<Sinks<RecB12, SWCB_B>>,
    ck_sinks: Vec<Sinks<RecC<u64>, SWCB_CK>>,
    reloc_b: Vec<Sinks<RecB12, SWCB_B>>,
    reloc_c: Vec<Sinks<RecC<u64>, SWCB_CK>>,
    worker_purch: Vec<Vec<CompactEv>>,
    worker_long_users: Vec<ArenaBytes>,
    worker_long_cids: Vec<ArenaBytes>,
    link_store: LinkLongStore,
    p2_short: usize,
    p2_total: usize,
    ne_raw: usize,
    dead: Option<DeadEvents>,
    parse_mapping: SinkArena,
    group_scratch: Vec<SinkArena>,
}

struct FusedWorkerH<'a, P> {
    short_map: ShortGroupMap4,
    long_map: LongGroupMap,
    fill: Vec<u32>,
    ev_blocks: ArenaVec<'a, EvBlock>,
    ev_buf: ArenaVec<'a, CompactEv>,
    ev_tmp: ArenaVec<'a, CompactEv>,
    cl_pid: ArenaVec<'a, u32>,
    cl_flat: ArenaVec<'a, CompactClick<u64>>,
    ck_cnt: Vec<u32>,
    ck_off: Vec<u32>,
    cl_buf: ArenaVec<'a, CompactClick<u64>>,
    cl_tmp: ArenaVec<'a, CompactClick<u64>>,
    dense: DenseSessions,
    camp: CampStore,
    writer: output::UserWriter<'a>,
    cat_sum: Vec<u64>,
    cat_touched: Vec<u32>,
    assists: FusedAssists,
    np: usize,
    ne: usize,
    nc: usize,
    written: usize,
    profile: P,
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn sessionize_person<P: GroupProfile, const HAS_CLICKS: bool>(
    w: &mut FusedWorkerH<'_, P>,
    dense_person: bool,
    single_blk: u32,
    ev_cnt: usize,
    cl_st: usize,
    cl_end: usize,
    id_bytes: &[u8],
    day0: i32,
    ws_rem: u32,
    organic: u32,
    categories: &[Vec<u8>],
) {
    let mut cj = cl_st;
    let mut grp_first = cl_end;
    let mut last_ts = u32::MAX;
    let mut rel_day = 0u32;
    let mut next_day_s = DAY_MS_U32 - ws_rem;
    let (mut ud_sessions, mut ud_events, mut ud_revenue) = (0u32, 0u32, 0i64);
    let cl_slice_end = cl_end + usize::from(HAS_CLICKS);

    if dense_person {
        let mut wm = w.dense.word_mask;
        w.dense.word_mask = 0;
        if wm != 0 {
            let wi0 = wm.trailing_zeros() as usize;
            wm &= wm - 1;
            let mut word = unsafe { *w.dense.active_mask.get_unchecked(wi0) };
            unsafe { *w.dense.active_mask.get_unchecked_mut(wi0) = 0 };
            let mut base_bin = wi0 << 6;
            let tz0 = word.trailing_zeros() as usize;
            word &= word - 1;
            let bi0 = base_bin + tz0;
            let bin0 = unsafe { *w.dense.bins.get_unchecked(bi0) };
            unsafe { *w.dense.bins.get_unchecked_mut(bi0) = DenseBin::EMPTY };
            let s0 = bin0.min_ts;
            if s0 >= next_day_s {
                rel_day = (ws_rem + s0) / DAY_MS_U32;
                next_day_s = (rel_day + 1) * DAY_MS_U32 - ws_rem;
            }
            let mut sess_start = s0;
            let mut sess_max = bin0.max_ts;
            let mut sess_last_bin = bi0;
            let mut sess_nev = bin0.count;
            let mut sess_rev = if bin0.purch_head != u32::MAX {
                add_bin_purchases(
                    &w.dense.purch_nodes,
                    bin0.purch_head,
                    &mut w.cat_sum,
                    &mut w.cat_touched,
                )
            } else {
                0
            };
            loop {
                while word != 0 {
                    let tz = word.trailing_zeros() as usize;
                    word &= word - 1;
                    let bi = base_bin + tz;
                    let bin = unsafe { *w.dense.bins.get_unchecked(bi) };
                    unsafe { *w.dense.bins.get_unchecked_mut(bi) = DenseBin::EMPTY };
                    if bi == sess_last_bin + 1 && bin.min_ts - sess_max <= GAP_MS_U32 {
                        sess_max = bin.max_ts;
                        sess_last_bin = bi;
                        sess_nev += bin.count;
                        if bin.purch_head != u32::MAX {
                            sess_rev += add_bin_purchases(
                                &w.dense.purch_nodes,
                                bin.purch_head,
                                &mut w.cat_sum,
                                &mut w.cat_touched,
                            );
                        }
                    } else {
                        emit_session_c::<u64, HAS_CLICKS>(
                            &mut w.camp,
                            &mut w.assists,
                            &w.cl_buf[..cl_slice_end],
                            organic,
                            &mut cj,
                            &mut grp_first,
                            &mut last_ts,
                            sess_start,
                            rel_day,
                            sess_nev,
                            sess_rev,
                            &mut ud_sessions,
                            &mut ud_events,
                            &mut ud_revenue,
                        );
                        let s = bin.min_ts;
                        if s >= next_day_s {
                            flush_user_day(
                                &mut w.writer,
                                &mut w.cat_sum,
                                &mut w.cat_touched,
                                &mut w.profile,
                                id_bytes,
                                categories,
                                day0.wrapping_add(rel_day as i32),
                                ud_sessions,
                                ud_events,
                                ud_revenue,
                            );
                            ud_sessions = 0;
                            ud_events = 0;
                            ud_revenue = 0;
                            rel_day = (ws_rem + s) / DAY_MS_U32;
                            next_day_s = (rel_day + 1) * DAY_MS_U32 - ws_rem;
                        }
                        sess_start = s;
                        sess_max = bin.max_ts;
                        sess_last_bin = bi;
                        sess_nev = bin.count;
                        sess_rev = if bin.purch_head != u32::MAX {
                            add_bin_purchases(
                                &w.dense.purch_nodes,
                                bin.purch_head,
                                &mut w.cat_sum,
                                &mut w.cat_touched,
                            )
                        } else {
                            0
                        };
                    }
                }
                if wm == 0 {
                    break;
                }
                let wi = wm.trailing_zeros() as usize;
                wm &= wm - 1;
                word = unsafe { *w.dense.active_mask.get_unchecked(wi) };
                unsafe { *w.dense.active_mask.get_unchecked_mut(wi) = 0 };
                base_bin = wi << 6;
            }
            emit_session_c::<u64, HAS_CLICKS>(
                &mut w.camp,
                &mut w.assists,
                &w.cl_buf[..cl_slice_end],
                organic,
                &mut cj,
                &mut grp_first,
                &mut last_ts,
                sess_start,
                rel_day,
                sess_nev,
                sess_rev,
                &mut ud_sessions,
                &mut ud_events,
                &mut ud_revenue,
            );
        }
    } else {
        // SAFETY: `sessionize_person` never mutates `w.ev_blocks` or `w.ev_buf`.
        let evs_ptr: *const CompactEv = if single_blk != u32::MAX {
            unsafe { w.ev_blocks.get_unchecked(single_blk as usize).evs.as_ptr().cast() }
        } else {
            w.ev_buf.as_ptr()
        };
        let mut i = 0usize;
        while i < ev_cnt {
            let first = unsafe { *evs_ptr.add(i) };
            let s = first.ts();
            if s >= next_day_s {
                if ud_sessions != 0 {
                    flush_user_day(
                        &mut w.writer,
                        &mut w.cat_sum,
                        &mut w.cat_touched,
                        &mut w.profile,
                        id_bytes,
                        categories,
                        day0.wrapping_add(rel_day as i32),
                        ud_sessions,
                        ud_events,
                        ud_revenue,
                    );
                    ud_sessions = 0;
                    ud_events = 0;
                    ud_revenue = 0;
                }
                rel_day = (ws_rem + s) / DAY_MS_U32;
                next_day_s = (rel_day + 1) * DAY_MS_U32 - ws_rem;
            }
            let mut prev = s;
            let mut k = i + 1;
            let mut cc = first.cc();
            let mut rev = 0u64;
            loop {
                if cc != 0 {
                    let cents = (cc & 0x00FF_FFFF) as u64;
                    rev += cents;
                    let cat = cc >> 24;
                    let slot = unsafe { w.cat_sum.get_unchecked_mut(cat as usize) };
                    if *slot == 0 {
                        w.cat_touched.push(cat);
                    }
                    *slot += cents;
                }
                if k == ev_cnt {
                    break;
                }
                let e = unsafe { *evs_ptr.add(k) };
                let ets = e.ts();
                if ets - prev > GAP_MS_U32 {
                    break;
                }
                prev = ets;
                cc = e.cc();
                k += 1;
            }
            let nev = (k - i) as u32;
            i = k;

            emit_session_c::<u64, HAS_CLICKS>(
                &mut w.camp,
                &mut w.assists,
                &w.cl_buf[..cl_slice_end],
                organic,
                &mut cj,
                &mut grp_first,
                &mut last_ts,
                s,
                rel_day,
                nev,
                rev,
                &mut ud_sessions,
                &mut ud_events,
                &mut ud_revenue,
            );
        }
    }
    flush_user_day(
        &mut w.writer,
        &mut w.cat_sum,
        &mut w.cat_touched,
        &mut w.profile,
        id_bytes,
        categories,
        day0.wrapping_add(rel_day as i32),
        ud_sessions,
        ud_events,
        ud_revenue,
    );
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn aggregate_person<P: GroupProfile>(
    w: &mut FusedWorkerH<'_, P>,
    pi: usize,
    head: u32,
    tail: u32,
    full_blocks: u32,
    ncl_raw: usize,
    id_bytes: &[u8],
    use_dense: bool,
    dense_min: usize,
    day0: i32,
    ws_rem: u32,
    organic: u32,
    categories: &[Vec<u8>],
    wlc_ref: &[&[u8]],
) {
    let ev_cnt = (full_blocks as usize) * EV_BLOCK_CAP
        + unsafe { w.ev_blocks.get_unchecked(tail as usize).len as usize };
    let dense_person = use_dense && ev_cnt >= dense_min;
    let single_blk = if dense_person {
        u32::MAX
    } else if full_blocks == 0 {
        let blk = unsafe { w.ev_blocks.get_unchecked_mut(head as usize) };
        sort_ev_slice(blk.slice_mut(), &mut w.ev_tmp);
        head
    } else {
        w.ev_buf.clear();
        w.ev_buf.reserve(ev_cnt);
        let dst = w.ev_buf.as_mut_ptr();
        let mut off = 0usize;
        let mut b = head;
        while b != u32::MAX {
            let blk = unsafe { w.ev_blocks.get_unchecked(b as usize) };
            let n = blk.len as usize;
            unsafe {
                std::ptr::copy_nonoverlapping(blk.evs.as_ptr().cast::<CompactEv>(), dst.add(off), n);
            }
            off += n;
            b = blk.next;
        }
        debug_assert_eq!(off, ev_cnt);
        unsafe { w.ev_buf.set_len(ev_cnt) };
        sort_ev_slice(&mut w.ev_buf[..ev_cnt], &mut w.ev_tmp);
        u32::MAX
    };
    let (cl_st, cl_len) = if ncl_raw > 0 {
        let cs = unsafe { *w.ck_off.get_unchecked(pi) } as usize;
        let ce = unsafe { *w.ck_off.get_unchecked(pi + 1) } as usize;
        let raw_len = ce - cs;
        if raw_len > 1 {
            sort_clicks_var_only(&mut w.cl_buf[cs..ce], &mut w.cl_tmp, wlc_ref);
        }
        w.nc += raw_len;
        (cs, raw_len)
    } else {
        (0, 0)
    };
    w.profile.end(GroupPhase::Sort);

    if dense_person {
        if full_blocks == 0 {
            let blk = unsafe { w.ev_blocks.get_unchecked(head as usize) };
            w.dense.scatter(blk.slice());
        } else {
            w.dense.reset_purch();
            let mut b = head;
            while b != u32::MAX {
                let blk = unsafe { w.ev_blocks.get_unchecked(b as usize) };
                w.dense.scatter_slice(blk.slice());
                b = blk.next;
            }
        }
    }

    if cl_len > 0 {
        w.assists.clear_sessions();
        let cl_end = cl_st + cl_len;
        let saved_cl = unsafe { *w.cl_buf.get_unchecked(cl_end) };
        unsafe { w.cl_buf.get_unchecked_mut(cl_end).ts = u32::MAX };
        sessionize_person::<P, true>(
            w, dense_person, single_blk, ev_cnt, cl_st, cl_end, id_bytes, day0, ws_rem, organic,
            categories,
        );
        unsafe { *w.cl_buf.get_unchecked_mut(cl_end) = saved_cl };
        w.assists.count(&mut w.camp, &w.cl_buf[cl_st..cl_end]);
    } else {
        sessionize_person::<P, false>(
            w, dense_person, single_blk, ev_cnt, 0, 0, id_bytes, day0, ws_rem, organic, categories,
        );
    }
    w.profile.end(GroupPhase::Aggregate);
}

#[inline(always)]
fn finalize_click_csr<P>(w: &mut FusedWorkerH<'_, P>, np: usize, ncl_raw: usize) -> usize {
    w.ck_off.clear();
    if ncl_raw > 0 {
        unsafe {
            w.cl_pid.set_len(ncl_raw);
            w.cl_flat.set_len(ncl_raw);
        }
        w.ck_off.reserve(np + 1);
        let mut cacc = 0u32;
        for i in 0..np {
            w.ck_off.push(cacc);
            cacc += unsafe { *w.ck_cnt.get_unchecked(i) };
        }
        w.ck_off.push(cacc);
        w.fill.clear();
        w.fill.extend_from_slice(&w.ck_off[..np]);
        w.cl_buf.clear();
        w.cl_buf.reserve(ncl_raw + 1);
        let cl_ptr = w.cl_buf.as_mut_ptr();
        for (&id, &ck) in w.cl_pid.iter().zip(w.cl_flat.iter()) {
            let pos = unsafe { *w.fill.get_unchecked(id as usize) } as usize;
            unsafe {
                cl_ptr.add(pos).write(ck);
                *w.fill.get_unchecked_mut(id as usize) = (pos + 1) as u32;
            }
        }
        unsafe {
            cl_ptr.add(ncl_raw).write(CompactClick { ts: u32::MAX, campaign: 0, cid: 0 });
            w.cl_buf.set_len(ncl_raw + 1);
        }
    }
    ncl_raw
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn group_short_part<P: GroupProfile>(
    w: &mut FusedWorkerH<'_, P>,
    q: usize,
    raw_n: usize,
    raw_ck: usize,
    b_sinks: &[Sinks<RecB12, SWCB_B>],
    reloc_b: &[Sinks<RecB12, SWCB_B>],
    ck_sinks: &[Sinks<RecC<u64>, SWCB_CK>],
    reloc_c: &[Sinks<RecC<u64>, SWCB_CK>],
    dead: Option<&DeadEvents>,
    wp_ref: &[&[CompactEv]],
    wlc_ref: &[&[u8]],
    dense_min: usize,
    day0: i32,
    ws_rem: u32,
    organic: u32,
    categories: &[Vec<u8>],
) {
    w.profile.start();
    let mut live = 0usize;
    w.short_map.reset(raw_n / 25 + 16);
    {
        let mut pool = EvBlockPool::new(&mut w.ev_blocks);
        let short_map = &mut w.short_map;
        for_each_live(b_sinks, dead, q, |r| {
            let ev = unpack_ev(r.ts_or_purch, wp_ref);
            short_map.append(r.person(), ev, &mut pool);
            live += 1;
        });
        for_each_live(reloc_b, None, q, |r| {
            let ev = unpack_ev(r.ts_or_purch, wp_ref);
            short_map.append(r.person(), ev, &mut pool);
            live += 1;
        });
    }
    if live == 0 {
        return;
    }
    w.ne += live;
    let np = w.short_map.len();
    w.np += np;

    let ncl_raw = if raw_ck > 0 {
        w.cl_pid.clear();
        w.cl_pid.reserve(raw_ck);
        w.cl_flat.clear();
        w.cl_flat.reserve(raw_ck);
        let cl_pid = w.cl_pid.as_mut_ptr();
        let cl_flat = w.cl_flat.as_mut_ptr();
        let mut ncl = 0usize;
        w.ck_cnt.clear();
        w.ck_cnt.resize(np, 0);
        for_each_live(ck_sinks, None, q, |r| {
            if let Some(id) = w.short_map.get(r.person) {
                unsafe {
                    cl_pid.add(ncl).write(id);
                    cl_flat.add(ncl).write(CompactClick {
                        ts: r.ts,
                        campaign: r.campaign,
                        cid: r.cid,
                    });
                    *w.ck_cnt.get_unchecked_mut(id as usize) += 1;
                }
                ncl += 1;
            }
        });
        for_each_live(reloc_c, None, q, |r| {
            if let Some(id) = w.short_map.get(r.person) {
                unsafe {
                    cl_pid.add(ncl).write(id);
                    cl_flat.add(ncl).write(CompactClick {
                        ts: r.ts,
                        campaign: r.campaign,
                        cid: r.cid,
                    });
                    *w.ck_cnt.get_unchecked_mut(id as usize) += 1;
                }
                ncl += 1;
            }
        });
        finalize_click_csr(w, np, ncl)
    } else {
        0
    };
    w.profile.end(GroupPhase::Csr);

    let use_dense = w.dense.enabled();
    let mut id_buf = [0u8; 16];
    for pi in 0..np {
        let (key, head, tail, full_blocks) = w.short_map.person(pi);
        let n = unmix63(key).unpack_buf(&mut id_buf);
        aggregate_person(
            w,
            pi,
            head,
            tail,
            full_blocks,
            ncl_raw,
            &id_buf[..n],
            use_dense,
            dense_min,
            day0,
            ws_rem,
            organic,
            categories,
            wlc_ref,
        );
    }
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn group_long_part<P: GroupProfile>(
    w: &mut FusedWorkerH<'_, P>,
    q: usize,
    raw_n: usize,
    raw_ck: usize,
    b_sinks: &[Sinks<RecB12, SWCB_B>],
    reloc_b: &[Sinks<RecB12, SWCB_B>],
    ck_sinks: &[Sinks<RecC<u64>, SWCB_CK>],
    reloc_c: &[Sinks<RecC<u64>, SWCB_CK>],
    dead: Option<&DeadEvents>,
    wp_ref: &[&[CompactEv]],
    wlu_ref: &[&[u8]],
    wlc_ref: &[&[u8]],
    ls_ref: &LinkLongStore,
    dense_min: usize,
    day0: i32,
    ws_rem: u32,
    organic: u32,
    categories: &[Vec<u8>],
) {
    w.profile.start();
    let mut live = 0usize;
    w.long_map.reset(raw_n / 25 + 16);
    {
        let mut pool = EvBlockPool::new(&mut w.ev_blocks);
        let long_map = &mut w.long_map;
        for_each_live(b_sinks, dead, q, |r| {
            let ev = unpack_ev(r.ts_or_purch, wp_ref);
            long_map.append(r.person(), ev, &mut pool, wlu_ref, ls_ref);
            live += 1;
        });
        for_each_live(reloc_b, None, q, |r| {
            let ev = unpack_ev(r.ts_or_purch, wp_ref);
            long_map.append(r.person(), ev, &mut pool, wlu_ref, ls_ref);
            live += 1;
        });
    }
    if live == 0 {
        return;
    }
    w.ne += live;
    let np = w.long_map.entries.len();
    w.np += np;

    let ncl_raw = if raw_ck > 0 {
        w.cl_pid.clear();
        w.cl_pid.reserve(raw_ck);
        w.cl_flat.clear();
        w.cl_flat.reserve(raw_ck);
        let cl_pid = w.cl_pid.as_mut_ptr();
        let cl_flat = w.cl_flat.as_mut_ptr();
        let mut ncl = 0usize;
        w.ck_cnt.clear();
        w.ck_cnt.resize(np, 0);
        for_each_live(ck_sinks, None, q, |r| {
            if let Some(id) = w.long_map.get(r.person, wlu_ref, ls_ref) {
                unsafe {
                    cl_pid.add(ncl).write(id);
                    cl_flat.add(ncl).write(CompactClick {
                        ts: r.ts,
                        campaign: r.campaign,
                        cid: r.cid,
                    });
                    *w.ck_cnt.get_unchecked_mut(id as usize) += 1;
                }
                ncl += 1;
            }
        });
        for_each_live(reloc_c, None, q, |r| {
            if let Some(id) = w.long_map.get(r.person, wlu_ref, ls_ref) {
                unsafe {
                    cl_pid.add(ncl).write(id);
                    cl_flat.add(ncl).write(CompactClick {
                        ts: r.ts,
                        campaign: r.campaign,
                        cid: r.cid,
                    });
                    *w.ck_cnt.get_unchecked_mut(id as usize) += 1;
                }
                ncl += 1;
            }
        });
        finalize_click_csr(w, np, ncl)
    } else {
        0
    };
    w.profile.end(GroupPhase::Csr);

    let use_dense = w.dense.enabled();
    let mut id_tmp = [0u8; 33];
    for pi in 0..np {
        let entry = unsafe { *w.long_map.entries.get_unchecked(pi) };
        let id_bytes = if entry.ptag != 0 {
            unpack_hex33(entry.ptag, entry.hi, entry.lo, &mut id_tmp);
            &id_tmp[..]
        } else {
            unsafe { resolve_long_user(entry.tag, wlu_ref, ls_ref, &mut id_tmp) }
        };
        aggregate_person(
            w,
            pi,
            entry.head,
            entry.tail,
            entry.full_blocks,
            ncl_raw,
            id_bytes,
            use_dense,
            dense_min,
            day0,
            ws_rem,
            organic,
            categories,
            wlc_ref,
        );
    }
}

fn group_fused_hybrid<P: GroupProfile>(
    ctx: &Ctx<'_>,
    deduped: DedupedHybrid,
    output_dir: &Path,
    threads: usize,
    tm: &mut Timer,
) -> Option<SinkArena> {
    let DedupedHybrid {
        b_sinks,
        ck_sinks,
        reloc_b,
        reloc_c,
        worker_purch,
        worker_long_users,
        worker_long_cids,
        link_store,
        p2_short,
        p2_total,
        ne_raw,
        dead,
        parse_mapping,
        mut group_scratch,
    } = deduped;

    let ud = output_dir.join("user_daily");
    let cd = output_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");

    let mut raw_cnt_part = vec![0usize; p2_total];
    let mut raw_ck_part = vec![0usize; p2_total];
    for q in 0..p2_total {
        raw_cnt_part[q] = b_sinks.iter().map(|s| s.count(q)).sum::<usize>()
            + reloc_b.iter().map(|s| s.count(q)).sum::<usize>();
        raw_ck_part[q] = ck_sinks.iter().map(|s| s.count(q)).sum::<usize>()
            + reloc_c.iter().map(|s| s.count(q)).sum::<usize>();
    }
    let mut q_order: Vec<usize> = (0..p2_total).collect();
    q_order.sort_unstable_by_key(|&q| std::cmp::Reverse(raw_cnt_part[q]));

    let ws = ctx.ws;
    let we = ctx.we;
    let day0 = aggregate::day_of(ws);
    let ws_rem = ws.rem_euclid(ts::DAY_MS) as u32;
    let ndays = if we > ws {
        (aggregate::day_of(we - 1) as i64 - day0 as i64 + 1).min(u32::MAX as i64) as u32
    } else {
        0
    };
    let max_bins = if we > ws {
        ((we - ws - 1) as u32 / GAP_MS_U32) as usize + 1
    } else {
        0
    };
    let dense_min = env_usize("INGEST_DENSE_MIN").unwrap_or(8);
    let lookups = ctx.lookups.get();
    let ncamp = lookups.campaigns.len();
    let organic = ncamp as u32;
    let ncat = lookups.categories.len();
    let categories = &lookups.categories;

    let wp_slices: Vec<&[CompactEv]> = worker_purch.iter().map(Vec::as_slice).collect();
    let wlu_slices: Vec<&[u8]> = worker_long_users.iter().map(ArenaBytes::as_slice).collect();
    let wlc_slices: Vec<&[u8]> = worker_long_cids.iter().map(ArenaBytes::as_slice).collect();
    let q_order = &q_order;
    let raw_cnt_part = &raw_cnt_part;
    let raw_ck_part = &raw_ck_part;
    let ud_ref = &ud;
    let wp_ref: &[&[CompactEv]] = &wp_slices;
    let wlu_ref: &[&[u8]] = &wlu_slices;
    let wlc_ref: &[&[u8]] = &wlc_slices;
    let ls_ref = &link_store;
    let est_user_rows = ne_raw / threads / 8 + 1024;
    let scratch: Vec<_> = group_scratch.iter_mut().map(|a| Mutex::new(a.scratch())).collect();
    let group_scratch_available = !scratch.is_empty();
    let event_cap = if group_scratch_available {
        raw_cnt_part.iter().copied().max().unwrap_or(0).max(65536)
    } else {
        65536
    };
    let block_cap = (event_cap / 10 + 4096).min(event_cap).max(4096);
    let click_cap = if group_scratch_available {
        raw_ck_part.iter().copied().max().unwrap_or(0).max(16384)
    } else {
        16384
    };

    let workers: Vec<FusedWorkerH<'_, P>> = par::run_fini(
        threads,
        p2_total,
        |tid| {
            let mut scratch = scratch
                .get(tid)
                .map(|slot| std::mem::take(&mut *slot.lock().unwrap()))
                .unwrap_or_default();
            FusedWorkerH {
                short_map: ShortGroupMap4::new(),
                long_map: LongGroupMap::new(),
                fill: Vec::with_capacity(2048),
                ev_blocks: scratch.buffer(block_cap),
                ev_buf: ArenaVec::default(),
                ev_tmp: if group_scratch_available {
                    scratch.buffer(8192)
                } else {
                    ArenaVec::default()
                },
                cl_pid: scratch.buffer(click_cap),
                cl_flat: scratch.buffer(click_cap),
                ck_cnt: Vec::with_capacity(2048),
                ck_off: Vec::with_capacity(2048),
                cl_buf: scratch.buffer(click_cap + 1),
                cl_tmp: if group_scratch_available {
                    scratch.buffer(2048)
                } else {
                    ArenaVec::default()
                },
                dense: DenseSessions::new(max_bins),
                camp: CampStore::new(ncamp + 1, day0, ndays),
                writer: output::UserWriter::new(ud_ref, tid, est_user_rows),
                cat_sum: vec![0u64; ncat],
                cat_touched: Vec::with_capacity(32),
                assists: FusedAssists::new(ncamp),
                np: 0,
                ne: 0,
                nc: 0,
                written: 0,
                profile: P::default(),
            }
        },
        |w, idx| {
            let q = q_order[idx];
            let raw_n = raw_cnt_part[q];
            if raw_n == 0 {
                return;
            }
            let raw_ck = raw_ck_part[q];
            if q < p2_short {
                group_short_part(
                    w,
                    q,
                    raw_n,
                    raw_ck,
                    &b_sinks,
                    &reloc_b,
                    &ck_sinks,
                    &reloc_c,
                    dead.as_ref(),
                    wp_ref,
                    wlc_ref,
                    dense_min,
                    day0,
                    ws_rem,
                    organic,
                    categories,
                );
            } else {
                group_long_part(
                    w,
                    q,
                    raw_n,
                    raw_ck,
                    &b_sinks,
                    &reloc_b,
                    &ck_sinks,
                    &reloc_c,
                    dead.as_ref(),
                    wp_ref,
                    wlu_ref,
                    wlc_ref,
                    ls_ref,
                    dense_min,
                    day0,
                    ws_rem,
                    organic,
                    categories,
                );
            }
        },
        |w| {
            w.profile.start();
            w.written = w.writer.finish();
            w.profile.end(GroupPhase::Write);
            w.writer.release_buffers();
            drop(std::mem::take(&mut w.ev_blocks));
            drop(std::mem::take(&mut w.ev_buf));
            drop(std::mem::take(&mut w.ev_tmp));
            drop(std::mem::take(&mut w.cl_buf));
            drop(std::mem::take(&mut w.cl_tmp));
            drop(std::mem::take(&mut w.cl_pid));
            drop(std::mem::take(&mut w.cl_flat));
        },
    );

    drop((b_sinks, ck_sinks, reloc_b, reloc_c, worker_long_users, worker_long_cids, link_store));

    let np: usize = workers.iter().map(|w| w.np).sum();
    let ne: usize = workers.iter().map(|w| w.ne).sum();
    let nc: usize = workers.iter().map(|w| w.nc).sum();
    let user_rows_written: usize = workers.iter().map(|w| w.written).sum();
    if user_rows_written == 0 {
        output::write_empty_user(&ud).expect("writing empty user_daily");
    }
    if P::ENABLED {
        let mut totals = [0.0; 4];
        for w in &workers {
            for (total, cpu) in totals.iter_mut().zip(w.profile.totals()) {
                *total += cpu;
            }
        }
        let [csr, sort, agg, write] = totals;
        eprintln!(
            "[ingest-prof] cpu csr {csr:.6}s sort {sort:.6}s sessionize+agg {agg:.6}s user-finish {write:.6}s"
        );
    }
    let camp_rows = aggregate::merge_camps(workers.into_iter().map(|w| w.camp));
    tm.lap("group+agg");
    let t_cw = Instant::now();
    output::write_campaign_raw(&cd, &camp_rows, &lookups.campaigns).expect("writing campaign_daily");
    tm.lap("campaign-write");
    eprintln!(
        "[stage] campaign_write {:.3}s (user rows {user_rows_written}, campaign rows {})",
        t_cw.elapsed().as_secs_f64(),
        camp_rows.len()
    );
    eprintln!("[ingest] key=hybrid persons={np} events={ne} (raw {ne_raw}) clicks={nc}");
    Some(parse_mapping)
}

// ============================================================================
// Top-level entry point
// ============================================================================

pub(super) fn run_to_output(
    ctx: &mut Ctx<'_>,
    threads: usize,
    plan: &PartitionPlan,
    output_dir: &Path,
    tm: &mut Timer,
) -> Result<Option<SinkArena>, Need> {
    if plan.p2_bits > 10 {
        return Err(Need::Slow);
    }
    let workers = plan.parse_threads;
    let sample = sample_input_stats(&ctx.files);
    let est_ev = (ctx.event_bytes / sample.avg_ev_bytes) as usize + 1024;
    let est_ck = (ctx.click_bytes / sample.avg_ck_bytes) as usize + 1024;

    // Keep p1_total = p2_total = 576 (512 short + 64 long on v5) so SWCB=256/256 fits Cascade
    // Lake's 1 MiB L2 per core (288 KiB/worker = 576 KiB/SMT pair) while halving flush mispredicts.
    let base_p1_bits = plan.p1_bits.min(9);
    let (p1_short_bits, p1_long_bits) = if sample.long_eid_pct == 0 {
        (base_p1_bits, 4u32)
    } else if sample.long_eid_pct <= 22 {
        (base_p1_bits, (base_p1_bits.saturating_sub(3)).max(4))
    } else if sample.long_eid_pct <= 55 {
        (base_p1_bits, (base_p1_bits.saturating_sub(1)).max(4))
    } else {
        (4u32, base_p1_bits)
    };
    let base_p2_bits = plan.p2_bits.min(9);
    let (p2_short_bits, p2_long_bits) = if sample.long_user_pct == 0 {
        (base_p2_bits, 3u32)
    } else if sample.long_user_pct <= 22 {
        (base_p2_bits, (base_p2_bits.saturating_sub(3)).max(4))
    } else if sample.long_user_pct <= 55 {
        (base_p2_bits, (base_p2_bits.saturating_sub(1)).max(4))
    } else {
        (4u32, base_p2_bits)
    };

    let p1_short = 1usize << p1_short_bits;
    let p1_long = 1usize << p1_long_bits;
    let p1_total = p1_short + p1_long;
    let p2_short = 1usize << p2_short_bits;
    let p2_long = 1usize << p2_long_bits;
    let p2_total = p2_short + p2_long;
    debug_assert!(p2_total <= 1024);

    // Split-cap zero-overflow capacity sizing: size short and long tiers independently to their
    // sampled densities (eliminates ~120+ MB of dead pre-faulted slack on 512 short buckets while
    // eliminating long-bucket glibc heap overflows!).
    let short_eid_pct = 100usize.saturating_sub(sample.long_eid_pct);
    let short_user_pct = 100usize.saturating_sub(sample.long_user_pct);
    let cap_tok_s = if short_eid_pct > 0 {
        (est_ev * short_eid_pct / 100 / p1_short * 108 / 100 / workers).max(16) + 16
    } else {
        16
    };
    let cap_tok_l = if sample.long_eid_pct > 0 {
        (est_ev * sample.long_eid_pct / 100 / p1_long * 112 / 100 / workers).max(16) + 16
    } else {
        16
    };
    let cap_b_s = if short_user_pct > 0 {
        (est_ev * short_user_pct / 100 / p2_short * 96 / 100 * 108 / 100 / workers).max(16) + 32
    } else {
        16
    };
    let cap_b_l = if sample.long_user_pct > 0 {
        (est_ev * sample.long_user_pct / 100 / p2_long * 96 / 100 * 114 / 100 / workers).max(16) + 32
    } else {
        16
    };
    let cap_ck_s = if short_user_pct > 0 {
        (est_ck * short_user_pct / 100 / p2_short * 110 / 100 / workers).max(16) + 32
    } else {
        16
    };
    let cap_ck_l = if sample.long_user_pct > 0 {
        (est_ck * sample.long_user_pct / 100 / p2_long * 118 / 100 / workers).max(16) + 32
    } else {
        16
    };

    let tok_bytes = Sinks::<Tok, SWCB_TOK>::arena_bytes_split(p1_short, p1_long, cap_tok_s, cap_tok_l);
    let b_bytes = Sinks::<RecB12, SWCB_B>::arena_bytes_split(p2_short, p2_long, cap_b_s, cap_b_l);
    let ck_bytes = Sinks::<RecC<u64>, SWCB_CK>::arena_bytes_split(p2_short, p2_long, cap_ck_s, cap_ck_l);

    // Pre-size 2 MiB THP `ArenaBytes` for long IDs (never populated upfront -> zero unused faults!).
    // Cap per-kind arena reservation at 512 MiB total so extreme 100%-long-ID 60M datasets
    // (`seed60-longids`) drop `long_eids` back to OS right after dedup and stay well under 10 GiB RSS.
    const MAX_LONG_ARENA_TOTAL: usize = 512 << 20;
    let le_est = if sample.long_eid_pct > 0 {
        let pct = (sample.long_eid_pct + 3).min(105);
        ((est_ev * pct / 100 / workers) * (sample.avg_long_eid_len + 1))
            .max(64 << 10)
            .next_multiple_of(64)
    } else {
        0
    };
    let le_bytes = if le_est.saturating_mul(workers) <= MAX_LONG_ARENA_TOTAL { le_est } else { 0 };
    let lu_est = if sample.long_user_pct > 0 {
        let pct = (sample.long_user_pct + 4).min(105);
        let cache_keep = if sample.long_user_pct > 50 { 95 } else { 80 };
        (((est_ev + est_ck) * pct / 100 / workers * cache_keep / 100) * (sample.avg_long_user_len + 1))
            .max(64 << 10)
            .next_multiple_of(64)
    } else {
        0
    };
    let lu_bytes = if lu_est.saturating_mul(workers) <= MAX_LONG_ARENA_TOTAL { lu_est } else { 0 };
    let lc_est = if sample.long_cid_pct > 0 {
        let pct = (sample.long_cid_pct + 4).min(105);
        ((est_ck * pct / 100 / workers) * (sample.avg_long_cid_len + 1))
            .max(64 << 10)
            .next_multiple_of(64)
    } else {
        0
    };
    let lc_bytes = if lc_est.saturating_mul(workers) <= MAX_LONG_ARENA_TOTAL { lc_est } else { 0 };

    let do_populate = env_usize("INGEST_POPULATE").unwrap_or(1) != 0;
    // Split `parse_events -> dedup -> parse_clicks` so `ck_sinks` + `long_cids` recycle the
    // already-faulted `tok + long_eids` arena after dedup (cuts peak RSS ~150 MB on 20M / ~450 MB
    // on 60M, removes ~38k page faults, and eliminates `ck.swcb` L2 pressure during event parsing!).
    let parse_bytes = (tok_bytes + le_bytes + b_bytes + lu_bytes) * workers;

    let (arena, parse_mapping) = SinkArena::new(parse_bytes).split_at(parse_bytes);
    let (mut tokens, mut arena) = arena.split_at((tok_bytes + le_bytes) * workers);

    let mut links = read_links_hybrid(
        ctx,
        threads,
        p2_short_bits,
        p2_long_bits,
        sample.long_user_pct,
        tm,
        tokens.as_mut_uninit(),
    )?;

    let lookups = ctx.lookups.wait();
    if lookups.categories.len() > 256 {
        return Err(Need::Slow);
    }
    tm.lap("small-wait");

    let mut slices = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tok, rest) = tokens.split_at(tok_bytes);
        let (le, rest) = if le_bytes > 0 {
            let (a, r) = rest.split_at(le_bytes);
            (Some(a), r)
        } else {
            (None, rest)
        };
        tokens = rest;
        let (b, rest) = arena.split_at(b_bytes);
        let (lu, rest) = if lu_bytes > 0 {
            let (a, r) = rest.split_at(lu_bytes);
            (Some(a), r)
        } else {
            (None, rest)
        };
        slices.push(Mutex::new(Some((tok, le, b, lu))));
        arena = rest;
    }

    let flag = Flag(AtomicU8::new(0));
    let ws_sec = ctx.ws.div_euclid(1000);
    let we_sec = ctx.we.div_euclid(1000);
    let rows = ParseRowsH {
        dt: &ctx.dt,
        ws: ctx.ws,
        we: ctx.we,
        win_len_ms: (ctx.we - ctx.ws).max(0) as u64,
        ws_sec,
        win_len_sec: (we_sec - ws_sec).max(0) as u64,
        ws_whole_sec: ctx.ws % 1000 == 0 && ctx.we % 1000 == 0,
        lookups,
        flag: &flag,
    };
    let p1_short_shift = 64 - p1_short_bits;
    let p1_long_shift = 64 - p1_long_bits;
    let p2_short_shift = 63 - p2_short_bits;
    let p2_long_shift = 64 - p2_long_bits;
    // Default to reusable thread-local pread buffers (mmap_mode=0): avoids ~520k 4 KiB
    // minor page faults + MADV_DONTNEED TLB shootdowns inside virtualized cloud VMs.
    let mmap_mode = env_usize("INGEST_MMAP_INPUT").unwrap_or(0);
    let files = &ctx.files;
    let main_chunks = &ctx.main_chunks;
    let mut ev_chunks = Vec::with_capacity(main_chunks.len());
    let mut ck_chunks = Vec::new();
    for &c in main_chunks.iter() {
        match files[c.file as usize].kind {
            Kind::Events => ev_chunks.push(c),
            Kind::Clicks => ck_chunks.push(c),
            Kind::Links => unreachable!(),
        }
    }

    let mut mapped_slots: Vec<Mutex<Option<scan::MappedFile>>> = Vec::new();
    let mut mapped_files: Vec<Option<scan::MappedFile>> = Vec::new();
    let mut chunk_rem: Vec<AtomicU32> = Vec::new();
    if mmap_mode != 0 {
        let advice = match mmap_mode {
            2 => 2, // MADV_SEQUENTIAL
            _ => 0,
        };
        if mmap_mode == 4 {
            let mut counts = vec![0u32; files.len()];
            for c in main_chunks.iter() {
                counts[c.file as usize] += 1;
            }
            chunk_rem = counts.into_iter().map(AtomicU32::new).collect();
            mapped_slots = files
                .iter()
                .map(|fi| {
                    let m = if fi.kind != Kind::Links {
                        scan::MappedFile::open(&fi.handle, fi.len, advice)
                    } else {
                        None
                    };
                    Mutex::new(m)
                })
                .collect();
        } else {
            mapped_files = files
                .iter()
                .map(|fi| {
                    if fi.kind != Kind::Links {
                        scan::MappedFile::open(&fi.handle, fi.len, advice)
                    } else {
                        None
                    }
                })
                .collect();
            if mmap_mode == 3 || mmap_mode == 7 {
                let mf_ref = &mapped_files;
                par::run(workers, mf_ref.len(), |_| (), |_, fi| {
                    if let Some(ref m) = mf_ref[fi] {
                        m.populate_read();
                    }
                });
            }
        }
    }
    let mf_ref = &mapped_files;
    let ms_ref = &mapped_slots;
    let crem_ref = &chunk_rem;
    let est_purch_cap = (est_ev / 25 / workers).max(1024);

    let ev_chunks_ref = &ev_chunks;
    let states: Vec<(ParseStateH, Vec<u8>)> = par::run_fini(
        workers,
        ev_chunks_ref.len(),
        |tid| {
            let (tok, le, b, lu) = slices[tid].lock().unwrap().take().unwrap();
            if do_populate {
                tok.populate_write();
                b.populate_write();
            }
            (
                ParseStateH {
                    tid,
                    tok: Sinks::in_arena_split(p1_short, p1_long, cap_tok_s, cap_tok_l, tok),
                    b: Sinks::in_arena_split(p2_short, p2_long, cap_b_s, cap_b_l, b),
                    ck: Sinks::empty(),
                    purch: Vec::with_capacity(est_purch_cap),
                    long_eids: le.map_or_else(|| ArenaBytes::new_heap(0), ArenaBytes::in_arena),
                    long_users: lu.map_or_else(|| ArenaBytes::new_heap(0), ArenaBytes::in_arena),
                    long_cids: ArenaBytes::new_heap(0),
                    user_cache: LongUserCache::new(),
                    p1_short,
                    p1_short_shift,
                    p1_long_shift,
                    p2_short,
                    p2_short_shift,
                    p2_long_shift,
                },
                std::mem::take(&mut *ctx.readers[tid].lock().unwrap()),
            )
        },
        |(st, buf), i| {
            if flag.get().is_some() {
                return;
            }
            let c = ev_chunks_ref[i];
            let fidx = c.file as usize;
            let fi = &files[fidx];
            if mmap_mode == 4 {
                let used_map = {
                    let guard = ms_ref[fidx].lock().unwrap();
                    let ptr_opt = guard.as_ref().map(|m| m as *const scan::MappedFile);
                    drop(guard);
                    if let Some(mptr) = ptr_opt {
                        let m = unsafe { &*mptr };
                        if let Some((slice, lo, hi)) = scan::chunk_in_mapped(m, c) {
                            parse_events_buf_hybrid(&rows, slice, lo, hi, st);
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };
                if !used_map {
                    let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, buf);
                    parse_events_buf_hybrid(&rows, buf, lo, hi, st);
                }
                if crem_ref[fidx].fetch_sub(1, Ordering::AcqRel) == 1 {
                    let _ = ms_ref[fidx].lock().unwrap().take();
                }
                return;
            }
            if let Some(Some(m)) = mf_ref.get(fidx) {
                if let Some((slice, lo, hi)) = scan::chunk_in_mapped(m, c) {
                    parse_events_buf_hybrid(&rows, slice, lo, hi, st);
                    return;
                }
            }
            let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, buf);
            parse_events_buf_hybrid(&rows, buf, lo, hi, st);
        },
        |(st, _)| {
            st.tok.finish();
            st.b.finish();
        },
    );
    if let Some(n) = flag.get() {
        ctx.prefetch.stop();
        let mut states = states;
        for (tid, (_, buf)) in states.iter_mut().enumerate() {
            *ctx.readers[tid].lock().unwrap() = std::mem::take(buf);
        }
        par::drop_par(workers, states);
        drop(parse_mapping);
        return Err(n);
    }
    tm.lap("parse-ev");

    let mut tok_sinks = Vec::with_capacity(states.len());
    let mut b_sinks = Vec::with_capacity(states.len());
    let mut worker_purch = Vec::with_capacity(states.len());
    let mut long_eids = Vec::with_capacity(states.len());
    let mut ck_carry: Vec<Mutex<Option<(ArenaBytes, LongUserCache, Vec<u8>)>>> =
        Vec::with_capacity(states.len());
    let mut ne_raw = 0usize;
    for (st, buf) in states {
        ne_raw += st.tok.len.iter().map(|&x| x as usize).sum::<usize>();
        tok_sinks.push(st.tok);
        b_sinks.push(st.b);
        worker_purch.push(st.purch);
        long_eids.push(st.long_eids);
        ck_carry.push(Mutex::new(Some((st.long_users, st.user_cache, buf))));
    }

    let dead = DeadEvents::new(&b_sinks, p2_total, threads);
    {
        let long_eid_slices: Vec<&[u8]> = long_eids.iter().map(ArenaBytes::as_slice).collect();
        let le_ref: &[&[u8]] = &long_eid_slices;
        par::run(
            threads,
            p1_total,
            |_| (DedupTable::default(), Vec::<DedupBucket32>::new(), Vec::<u64>::new()),
            |(table, t32, t64), p| {
                let mut slices = [(&[][..], 0, 0); MAX_THREADS * 2];
                let (active, total) = partition_slices(&tok_sinks, p, &mut slices);
                if total == 0 {
                    return;
                }
                if p < p1_short {
                    table.mark_losers(active, total, |_, tid, loser| dead.mark(tid as usize, loser.loc));
                } else {
                    dedup_long_part(active, total, le_ref, &dead, t32, t64);
                }
            },
        );
    }
    // Rejoin `tok` + `long_eids` back into ONE contiguous already-faulted arena per worker!
    let free_arenas: Vec<SinkArena> = tok_sinks
        .into_iter()
        .zip(long_eids)
        .map(|(tok_s, le)| {
            let tok_a = tok_s.into_arena();
            match le.into_arena() {
                Some(le_a) => SinkArena::merge(tok_a, le_a),
                None => tok_a,
            }
        })
        .collect();
    tm.lap("dedup");

    // Stage 2B: Parse clicks directly into recycled already-faulted `tok + long_eids` pages!
    let (ck_sinks, worker_long_users, worker_long_cids, group_scratch) = if ck_chunks.is_empty() {
        let mut wlu = Vec::with_capacity(workers);
        let mut wlc = Vec::with_capacity(workers);
        for (tid, slot) in ck_carry.into_iter().enumerate() {
            let (lu, _uc, buf) = slot.into_inner().unwrap().take().unwrap();
            *ctx.readers[tid].lock().unwrap() = buf;
            wlu.push(lu);
            wlc.push(ArenaBytes::new_heap(0));
        }
        (Vec::new(), wlu, wlc, free_arenas)
    } else {
        let mut ck_slices = Vec::with_capacity(workers);
        let mut rem_arenas = Vec::with_capacity(free_arenas.len());
        let mut fa_iter = free_arenas.into_iter();
        for _ in 0..workers {
            let (ck_a, lc_a, needs_pop) = if let Some(orig) = fa_iter.next() {
                if ck_bytes + lc_bytes <= orig.bytes() {
                    let (ck_a, rest) = orig.split_at(ck_bytes);
                    let (lc_a, rem) = if lc_bytes > 0 {
                        let (a, r) = rest.split_at(lc_bytes);
                        (Some(a), r)
                    } else {
                        (None, rest)
                    };
                    rem_arenas.push(rem);
                    (ck_a, lc_a, false)
                } else {
                    rem_arenas.push(orig);
                    let ck_a = SinkArena::new(ck_bytes);
                    let lc_a = if lc_bytes > 0 { Some(SinkArena::new(lc_bytes)) } else { None };
                    (ck_a, lc_a, do_populate)
                }
            } else {
                let ck_a = SinkArena::new(ck_bytes);
                let lc_a = if lc_bytes > 0 { Some(SinkArena::new(lc_bytes)) } else { None };
                (ck_a, lc_a, do_populate)
            };
            ck_slices.push(Mutex::new(Some((ck_a, lc_a, needs_pop))));
        }
        rem_arenas.extend(fa_iter);

        let ck_chunks_ref = &ck_chunks;
        let ck_carry_ref = &ck_carry;
        let ck_slices_ref = &ck_slices;
        let mut ck_states: Vec<(ParseStateH, Vec<u8>)> = par::run_fini(
            workers,
            ck_chunks_ref.len(),
            |tid| {
                let (ck_a, lc_a, needs_pop) = ck_slices_ref[tid].lock().unwrap().take().unwrap();
                let (lu, uc, buf) = ck_carry_ref[tid].lock().unwrap().take().unwrap();
                if needs_pop {
                    ck_a.populate_write();
                }
                (
                    ParseStateH {
                        tid,
                        tok: Sinks::empty(),
                        b: Sinks::empty(),
                        ck: Sinks::in_arena_split(p2_short, p2_long, cap_ck_s, cap_ck_l, ck_a),
                        purch: Vec::new(),
                        long_eids: ArenaBytes::new_heap(0),
                        long_users: lu,
                        long_cids: lc_a.map_or_else(|| ArenaBytes::new_heap(0), ArenaBytes::in_arena),
                        user_cache: uc,
                        p1_short,
                        p1_short_shift,
                        p1_long_shift,
                        p2_short,
                        p2_short_shift,
                        p2_long_shift,
                    },
                    buf,
                )
            },
            |(st, buf), i| {
                if flag.get().is_some() {
                    return;
                }
                let c = ck_chunks_ref[i];
                let fidx = c.file as usize;
                let fi = &files[fidx];
                if mmap_mode == 4 {
                    let used_map = {
                        let guard = ms_ref[fidx].lock().unwrap();
                        let ptr_opt = guard.as_ref().map(|m| m as *const scan::MappedFile);
                        drop(guard);
                        if let Some(mptr) = ptr_opt {
                            let m = unsafe { &*mptr };
                            if let Some((slice, lo, hi)) = scan::chunk_in_mapped(m, c) {
                                parse_clicks_buf_hybrid(&rows, slice, lo, hi, st);
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    };
                    if !used_map {
                        let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, buf);
                        parse_clicks_buf_hybrid(&rows, buf, lo, hi, st);
                    }
                    if crem_ref[fidx].fetch_sub(1, Ordering::AcqRel) == 1 {
                        let _ = ms_ref[fidx].lock().unwrap().take();
                    }
                    return;
                }
                if let Some(Some(m)) = mf_ref.get(fidx) {
                    if let Some((slice, lo, hi)) = scan::chunk_in_mapped(m, c) {
                        parse_clicks_buf_hybrid(&rows, slice, lo, hi, st);
                        return;
                    }
                }
                let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, buf);
                parse_clicks_buf_hybrid(&rows, buf, lo, hi, st);
            },
            |(st, _)| {
                st.ck.finish();
            },
        );
        for (tid, (_, buf)) in ck_states.iter_mut().enumerate() {
            *ctx.readers[tid].lock().unwrap() = std::mem::take(buf);
        }
        if let Some(n) = flag.get() {
            ctx.prefetch.stop();
            par::drop_par(workers, ck_states);
            drop(b_sinks);
            drop(rem_arenas);
            drop(parse_mapping);
            return Err(n);
        }
        let mut cks = Vec::with_capacity(workers);
        let mut wlu = Vec::with_capacity(workers);
        let mut wlc = Vec::with_capacity(workers);
        for (st, _) in ck_states {
            cks.push(st.ck);
            wlu.push(st.long_users);
            wlc.push(st.long_cids);
        }
        (cks, wlu, wlc, rem_arenas)
    };
    if mmap_mode == 6 || mmap_mode == 7 {
        let mf_ref = &mapped_files;
        par::run(workers, mf_ref.len(), |_| (), |_, fi| {
            if let Some(ref m) = mf_ref[fi] {
                m.release_pages();
            }
        });
        drop(mapped_files);
    } else if mmap_mode == 1 || mmap_mode == 2 || mmap_mode == 3 {
        drop(mapped_files);
    }
    ctx.prefetch.stop();
    tm.lap("parse-ck");

    let (reloc_b, reloc_c, dead, group_scratch) = if links.has_links {
        let wlu_slices: Vec<&[u8]> = worker_long_users.iter().map(ArenaBytes::as_slice).collect();
        let (rb, rc, rem) = relocate_hybrid(
            &b_sinks,
            &ck_sinks,
            &wlu_slices,
            &dead,
            &links,
            group_scratch,
            p2_short,
            p2_long,
            p2_short_shift,
            threads,
        );
        links.short_pairs = Vec::new();
        links.short_offsets = Vec::new();
        links.long_pairs = Vec::new();
        links.long_offsets = Vec::new();
        drop(dead);
        tm.lap("relocate");
        (rb, rc, None, rem)
    } else {
        (Vec::new(), Vec::new(), Some(dead), group_scratch)
    };

    let deduped = DedupedHybrid {
        b_sinks,
        ck_sinks,
        reloc_b,
        reloc_c,
        worker_purch,
        worker_long_users,
        worker_long_cids,
        link_store: links.link_store,
        p2_short,
        p2_total,
        ne_raw,
        dead,
        parse_mapping,
        group_scratch,
    };

    let prof = std::env::var("INGEST_PROF").as_deref() == Ok("1");
    if prof {
        Ok(group_fused_hybrid::<GroupTimings>(ctx, deduped, output_dir, threads, tm))
    } else {
        Ok(group_fused_hybrid::<()>(ctx, deduped, output_dir, threads, tm))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mix63_bijection_and_bit63_zero() {
        assert_eq!(mix63(0), 0);
        assert_eq!(unmix63(0), 0);
        for &v in &[1u64, 2, 0x5555_5555_5555_5555, MASK63, MASK63 - 1, 1 << 62] {
            let m = mix63(v);
            assert!(is_short(m) && m > 0);
            assert_eq!(unmix63(m), v);
            assert_eq!(mix63(unmix63(v)), v);
        }
        let mut x = 0x1234_5678_9ABC_DEF1u64;
        for _ in 0..500_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let v = (x & MASK63).max(1);
            let m = mix63(v);
            assert!(is_short(m) && m > 0);
            assert_eq!(unmix63(m), v);
        }
    }

    #[test]
    fn hash_long_33_and_general_distinct() {
        let a = b"u0123456789abcdef0123456789abcdef";
        let b = b"u0123456789abcdef0123456789abcdee";
        let c = b"u0123456789abcdef0123456789abcdff";
        assert_eq!(a.len(), 33);
        assert_ne!(hash_long(a), hash_long(b));
        assert_ne!(hash_long(a), hash_long(c));
        assert_ne!(hash_long(b), hash_long(c));
        let mut d = *a;
        d[24] ^= 1;
        assert_ne!(hash_long(a), hash_long(&d));
        let mut e = *a;
        e[32] ^= 1;
        assert_ne!(hash_long(a), hash_long(&e));
    }

    #[test]
    fn arena_bytes_overflow_to_heap_exact() {
        let sa = SinkArena::new(128);
        let mut ab = ArenaBytes::in_arena(sa);
        let mut offs = Vec::new();
        for i in 0..20usize {
            let s = format!("u{i:032x}");
            offs.push((ab.push_bytes(s.as_bytes()).unwrap(), s));
        }
        // Also test uppercase hex, all-digit hex, mixed-case verbatim, and non-hex verbatim!
        for raw in [
            "U0123456789ABCDEF0123456789ABCDEF",
            "u01234567890123456789012345678901",
            "u0123456789abcdef0123456789ABCDEf",
            "u_not_hex_at_all_33_bytes_long___",
            "arbitrary_utf8_é_long_user_identifier_44B",
        ] {
            offs.push((ab.push_bytes(raw.as_bytes()).unwrap(), raw.to_string()));
        }
        let sl = ab.as_slice();
        let mut tmp = [0u8; 33];
        for (off, s) in &offs {
            let got = unsafe { resolve_arena_bytes(sl, *off, &mut tmp) };
            assert_eq!(got, s.as_bytes());
            assert!(unsafe { eq_arena_raw(sl, *off, s.as_bytes()) });
            assert_eq!(unsafe { hash_arena_rec(sl, *off) }, hash_long(s.as_bytes()));
        }
        // Verify all-digits has canonical tag bit7 == 0 and mixed-case is NOT packed.
        assert!(unsafe { try_pack_hex33(b"u01234567890123456789012345678901".as_ptr()) }.unwrap().0 == b'u');
        assert!(unsafe { try_pack_hex33(b"u0123456789ABCDEF0123456789ABCDEF".as_ptr()) }.unwrap().0 == (b'u' | 0x80));
        assert!(unsafe { try_pack_hex33(b"u0123456789abcdef0123456789ABCDEf".as_ptr()) }.is_none());
        assert!(ab.into_arena().is_some());
    }

    #[test]
    fn branchless_try_encode_89_and_canon_q() {
        let mut b8 = [0u8; 64];
        b8[..9].copy_from_slice(b"u8765432,");
        let e8 = u32::from_le_bytes(b8[8..12].try_into().unwrap());
        let k8 = unsafe { try_encode_89(b8.as_ptr(), e8, 0).unwrap() };
        assert_eq!(k8, u64::enc_slice(b"u8765432").unwrap());

        let mut b9 = [0u8; 64];
        b9[..10].copy_from_slice(b"e12345678,");
        let e9 = u32::from_le_bytes(b9[8..12].try_into().unwrap());
        let k9 = unsafe { try_encode_89(b9.as_ptr(), e9, 1).unwrap() };
        assert_eq!(k9, u64::enc_slice(b"e12345678").unwrap());

        let s_tag = mix63(k8);
        assert_eq!(canon_q_of(s_tag, 63 - 9), (s_tag >> (63 - 9)) as usize);
        let l_tag = make_link_long_user(575, 0xDEAD_BEEF_CAFE_BABEu64, 42);
        assert_eq!(canon_q_of(l_tag, 63 - 9), 575);
    }

    #[test]
    fn hybrid_links_split_and_merge_p2_exact() {
        let mut arena = ArenaBytes::new_heap(256);
        let o1 = arena.push_bytes(b"u00000000000000000000000000000001").unwrap();
        let o2 = arena.push_bytes(b"uzz000000000000000000000000000002").unwrap();
        let h1 = hash_long(b"u00000000000000000000000000000001");
        let h2 = hash_long(b"uzz000000000000000000000000000002");
        let t1 = make_parse_long_user(0, h1, o1);
        let t2 = make_parse_long_user(0, h2, o2);
        let s_mid = mix63(u64::enc_slice(b"u5abcdef").unwrap());
        let pairs = vec![vec![(t1, s_mid), (s_mid, t2)]];
        let raw_slices: Vec<&[u8]> = vec![arena.as_slice()];

        for &(ps_bits, pl_bits) in &[(9u32, 6u32), (4u32, 9u32), (8u32, 5u32)] {
            let bl = build_bucket_links_hybrid(&pairs, &raw_slices, ps_bits, pl_bits, 2, &mut []).unwrap();
            assert!(bl.has_links);
            let p2_short = 1usize << ps_bits;
            let q_mid = (s_mid >> (63 - ps_bits)) as usize;
            let mut sm = ShortRelocMap::default();
            sm.build(bl.short_part(q_mid));
            let canon = sm.get(s_mid).unwrap();
            assert!(!is_short(canon));
            assert_eq!(
                canon_q_of(canon, 63 - ps_bits),
                p2_short + (h1 >> (64 - pl_bits)) as usize
            );
            let mut tmp = [0u8; 33];
            assert_eq!(
                unsafe { resolve_long_user(canon, &[], &bl.link_store, &mut tmp) },
                b"u00000000000000000000000000000001"
            );
            let lq_t2 = (h2 >> (64 - pl_bits)) as usize;
            let mut lm = LongRelocMap::default();
            let lp = bl.long_part(lq_t2);
            lm.build(lp);
            let canon2 = lm.get(t2, lp, &raw_slices, &bl.link_store).unwrap();
            assert_eq!(canon2, canon);
        }
    }

    #[test]
    fn sort_and_dedup_clicks_var_matches_std() {
        let mut arena = Vec::new();
        let mut cids = Vec::new();
        for i in 0..40usize {
            if i % 3 == 0 {
                let s = format!("k{i:032x}");
                let off = push_long_bytes(&mut arena, s.as_bytes()).unwrap();
                cids.push(make_long_cid(0, off));
            } else {
                let s = format!("k{i:07}");
                cids.push(u64::enc_slice(s.as_bytes()).unwrap());
            }
        }
        let arenas: Vec<&[u8]> = vec![arena.as_slice()];
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut tmp = ArenaVec::default();
        for &n in &[0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 25, 300] {
            let mut a = Vec::with_capacity(n);
            for _ in 0..n {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                let ts = ((x >> 32) % 20) as u32;
                let cid = cids[((x >> 16) as usize) % cids.len()];
                a.push(CompactClick { ts, campaign: (x & 7) as u32, cid });
            }
            let mut b = a.clone();
            let k = sort_and_dedup_clicks_var(&mut a, &mut tmp, &arenas);
            b.sort_unstable_by(|u, v| u.ts.cmp(&v.ts).then_with(|| cmp_cid(u.cid, v.cid, &arenas)));
            let mut w = usize::from(!b.is_empty());
            for r in 1..b.len() {
                if !eq_cid(b[r].cid, b[w - 1].cid, &arenas) {
                    b[w] = b[r];
                    w += 1;
                }
            }
            assert_eq!(k, w, "len mismatch for n={n}");
            for i in 0..k {
                assert_eq!(a[i].ts, b[i].ts, "ts mismatch at i={i}, n={n}");
                assert!(eq_cid(a[i].cid, b[i].cid, &arenas), "cid mismatch at i={i}, n={n}");
            }
        }
    }

    #[test]
    fn encode_eid_short_and_short_group_map_exact() {
        let mut buf = [0u8; 64];
        for s in [
            &b"e1234567"[..],
            &b"e12345678"[..],
            &b"e0000000"[..],
            &b"ezzzzzzzz"[..],
            &b"a"[..],
            &b"ab"[..],
            &b"abcdefg"[..],
        ] {
            buf.fill(b',');
            buf[..s.len()].copy_from_slice(s);
            let k_slow = unsafe { encode_eid_short(buf.as_ptr(), s.len()).unwrap() };
            assert_ne!(k_slow, 0);
            if s.len() == 8 || s.len() == 9 {
                let end4 = unsafe { buf.as_ptr().add(8).cast::<u32>().read_unaligned() };
                let k_fast = unsafe { try_encode_eid_89(buf.as_ptr(), end4, usize::from(s.len() == 9)).unwrap() };
                assert_eq!(k_fast, k_slow);
            }
        }
        // Injectivity check across distinct 1..=9 byte ASCII strings
        let mut seen = std::collections::HashSet::new();
        for len in 1..=9 {
            for c in [b'0', b'a', b'z', b'A', b'~'] {
                let mut s = vec![b'e'; len];
                s[len - 1] = c;
                buf.fill(0);
                buf[..len].copy_from_slice(&s);
                let k = unsafe { encode_eid_short(buf.as_ptr(), len).unwrap() };
                assert!(seen.insert(k), "collision on {s:?}");
            }
        }
    }
}
