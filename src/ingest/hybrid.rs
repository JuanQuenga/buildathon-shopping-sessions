//! Hybrid inline-64 + side-arena single-pass exact ingest & fused aggregation engine.
//!
//! Treats EVERY ID (`event_id`, `user_id`, `click_id`) as a 100% opaque variable-width
//! byte string of arbitrary length (`1..∞`, arbitrary bytes/UTF-8/NUL):
//! - Short IDs (`1..=9` non-NUL ASCII bytes, ~87.5% on v5) are stored inline in `u64`
//!   with bit 63 == 0 (exact lossless, byte-wise order-preserving, zero arena traffic).
//! - Long/other IDs (`> 9` bytes or containing NUL / non-ASCII, ~12.5% on v5 including
//!   33-byte hex IDs, or 100% on `seed60-longids`) are stored verbatim in compact
//!   append-only byte arenas and referenced via tagged `u64` handles (bit 63 == 1)
//!   with embedded hash fingerprints so misses/inequalities never dereference the arena.
//!
//! Runs the full 2 MiB THP single-pass `SinkArena` + L2-resident `RelocationMap` +
//! L2-resident `GroupMap` + `DenseSessions` + sorting networks + `FusedAssists` pipeline
//! whenever compact window/category/ingest limits hold, falling back to `generic` otherwise.

use super::{
    add_bin_purchases, emit_session, env_usize, field, flen, flush_user_day, for_each_live, fptr,
    fts, partition_slices, sort_and_dedup_clicks, sort_ev_slice, CompactClick,
    CompactEv, Ctx, DeadEvents, DedupTable, DenseBin, DenseSessions, Flag, FusedAssists,
    GroupEntry, GroupPhase, GroupProfile, GroupTimings, Need, PartitionPlan, RecB, RecC, Tok,
    DAY_MS_U32, GAP_MS_U32, INVALID_CAMP, LOC_IDX_BITS, LP, LP_BITS, MAX_LOC_IDX, MAX_THREADS,
    NO_REC, WEEK_MS,
};
use crate::aggregate::{self, output, CampStore};
use crate::ingest::keys::Key;
use crate::ingest::par::{self, ArenaVec, Scratch, SinkArena, Sinks, SyncPtr, Timer};
use crate::ingest::scan::{self, FileInfo, Kind};
use crate::ingest::tsfast::{self, DateTable};
use crate::ingest::{slow, ts};
use std::arch::x86_64::*;
use std::cmp::Ordering as CmpOrd;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::Instant;

const LONG_BIT: u64 = 1u64 << 63;
const LINK_LONG_BIT: u64 = 1u64 << 62;
const MASK63: u64 = (1u64 << 63) - 1;
const MIX63_M: u64 = 0x5E37_79B9_7F4A_7C15; // odd, < 2^63, bit 62 = 1

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

/// Fast 64-bit hash over any opaque byte slice (specialized unrolled path for 33-byte IDs).
#[inline(always)]
fn hash_long(s: &[u8]) -> u64 {
    const K0: u64 = 0xA076_1D64_78BD_642F;
    const K1: u64 = 0xE703_7ED1_A0B4_28DB;
    const K2: u64 = 0x8EBC_6AF0_9C88_C6E3;
    const K3: u64 = 0x5899_65CC_7537_4CC3;
    let len = s.len();
    let p = s.as_ptr();
    unsafe {
        if len == 33 {
            let a = ld8(p);
            let b = ld8(p.add(8));
            let c = ld8(p.add(16));
            let d = ld8(p.add(24));
            let e = *p.add(32) as u64;
            let m0 = (a ^ K0 ^ (e << 56)).wrapping_mul(b ^ K1);
            let m1 = (c ^ K2).wrapping_mul(d ^ K3);
            let x = m0 ^ m1 ^ (m0 >> 32) ^ (m1 >> 32);
            let y = (x ^ (x >> 29)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            return y ^ (y >> 32);
        }
        hash_long_general(p, len)
    }
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

/// Append a length-prefixed byte slice to an append-only byte arena, returning its `u32` offset.
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
unsafe fn get_long_bytes(arena: &[u8], off: u32) -> &[u8] {
    let p = arena.as_ptr().add(off as usize);
    let b = *p;
    if b < 255 {
        std::slice::from_raw_parts(p.add(1), b as usize)
    } else {
        let len = p.add(1).cast::<u32>().read_unaligned() as usize;
        std::slice::from_raw_parts(p.add(5), len)
    }
}

/// Per-worker direct-mapped cache deduplicating repeated long `user_id`s in `long_users`.
/// Cuts long-user arena RAM by ~65% (keeps whales & active users hot in L3).
struct LongUserCache {
    slots: Vec<(u32, u32)>, // (hash_hi32 | 1, arena_off)
}

impl LongUserCache {
    const CAP: usize = 4096;
    const MASK: usize = Self::CAP - 1;

    fn new() -> Self {
        Self { slots: vec![(0, 0); Self::CAP] }
    }

    #[inline(always)]
    fn intern(&mut self, arena: &mut Vec<u8>, s: &[u8], uh: u64) -> Option<u32> {
        let fp = ((uh >> 32) as u32) | 1;
        let idx = (uh as usize) & Self::MASK;
        let slot = unsafe { self.slots.get_unchecked_mut(idx) };
        if slot.0 == fp && unsafe { get_long_bytes(arena, slot.1) } == s {
            return Some(slot.1);
        }
        let off = push_long_bytes(arena, s)?;
        *slot = (fp, off);
        Some(off)
    }
}

/// Read-only contiguous store of unique long user IDs that appeared in `links/`.
#[derive(Default)]
struct LinkLongStore {
    bytes: Vec<u8>,
    meta: Vec<(u64, u32)>, // (offset, len)
}

impl LinkLongStore {
    #[inline(always)]
    unsafe fn get(&self, id: u32) -> &[u8] {
        let &(off, len) = self.meta.get_unchecked(id as usize);
        std::slice::from_raw_parts(self.bytes.as_ptr().add(off as usize), len as usize)
    }
}

#[inline(always)]
fn make_parse_long_user(tid: usize, uh: u64, off: u32) -> u64 {
    LONG_BIT | ((tid as u64) << 57) | ((uh & 0x01FF_FFFF) << 32) | (off as u64)
}

#[inline(always)]
fn make_link_long_user(q: usize, uh: u64, link_id: u32) -> u64 {
    LONG_BIT
        | LINK_LONG_BIT
        | (((q as u64) & 0x3FF) << 52)
        | ((uh & 0x000F_FFFF) << 32)
        | (link_id as u64)
}

#[inline(always)]
fn make_long_cid(tid: usize, ch: u64, off: u32) -> u64 {
    LONG_BIT | ((tid as u64) << 58) | ((ch & 0x03FF_FFFF) << 32) | (off as u64)
}

#[inline(always)]
fn canon_q_of(canon_tagged: u64, p2_bits: u32) -> usize {
    if is_short(canon_tagged) {
        (canon_tagged >> (63 - p2_bits)) as usize
    } else {
        debug_assert!((canon_tagged & LINK_LONG_BIT) != 0);
        ((canon_tagged >> 52) & 0x3FF) as usize
    }
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
    worker_arenas: &'a [Vec<u8>],
    link_store: &'a LinkLongStore,
) -> &'a [u8] {
    debug_assert!(!is_short(tag));
    if (tag & LINK_LONG_BIT) != 0 {
        link_store.get(tag as u32)
    } else {
        let tid = ((tag >> 57) & 31) as usize;
        get_long_bytes(worker_arenas.get_unchecked(tid), tag as u32)
    }
}

#[inline(always)]
unsafe fn resolve_long_cid<'a>(tag: u64, cid_arenas: &'a [Vec<u8>]) -> &'a [u8] {
    debug_assert!(!is_short(tag));
    let tid = ((tag >> 58) & 31) as usize;
    get_long_bytes(cid_arenas.get_unchecked(tid), tag as u32)
}

#[inline(always)]
fn cmp_cid(a: u64, b: u64, cid_arenas: &[Vec<u8>]) -> CmpOrd {
    if ((a | b) as i64) >= 0 {
        return a.cmp(&b);
    }
    cmp_cid_slow(a, b, cid_arenas)
}

#[cold]
fn cmp_cid_slow(a: u64, b: u64, cid_arenas: &[Vec<u8>]) -> CmpOrd {
    let mut ba = [0u8; 16];
    let mut bb = [0u8; 16];
    let sa: &[u8] = if is_short(a) {
        let n = a.unpack_buf(&mut ba);
        &ba[..n]
    } else {
        unsafe { resolve_long_cid(a, cid_arenas) }
    };
    let sb: &[u8] = if is_short(b) {
        let n = b.unpack_buf(&mut bb);
        &bb[..n]
    } else {
        unsafe { resolve_long_cid(b, cid_arenas) }
    };
    sa.cmp(sb)
}

#[inline(always)]
fn eq_cid(a: u64, b: u64, cid_arenas: &[Vec<u8>]) -> bool {
    if a == b {
        return true;
    }
    if ((a & b) as i64) >= 0 {
        return false;
    }
    // Both long: check embedded 26-bit hash fingerprint first!
    if ((a ^ b) & 0x03FF_FFFF_0000_0000) != 0 {
        return false;
    }
    unsafe { resolve_long_cid(a, cid_arenas) == resolve_long_cid(b, cid_arenas) }
}

/// 16-byte event dedup token for long `event_id`s (bytes live in `long_eids[tid]`).
#[derive(Clone, Copy)]
#[repr(C, align(16))]
struct LongTok {
    hash: u32,
    off: u32,
    ingest: u32,
    loc: u32,
}

const P1_LONG_BITS: u32 = 7;

// ============================================================================
// Direct variable-width CSV field parser (fast 8/9B inline + 33B AVX2 + fallback)
// ============================================================================

#[derive(Clone, Copy)]
enum RawId {
    Short(u64),
    Long(*const u8, usize),
}

#[inline(always)]
unsafe fn open_delim<const DELIM: u8>(p: *const u8) -> (*const u8, u16, u16, usize) {
    let quoted = (*p == b'"') as usize;
    let mask = if quoted == 0 { 0x00FFu16 } else { 0xFFFFu16 };
    let expect = if quoted == 0 {
        DELIM as u16
    } else {
        u16::from_le_bytes([b'"', DELIM])
    };
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

/// Branchless fast path for 8/9-byte ASCII IDs (~87.5%) PLUS 5-instruction AVX2 fast path
/// for 33-byte opaque IDs (~12.5%, zero hex/charset assumption!). Rare other lengths fall
/// back cleanly to `scan::row_fields` per row.
#[inline(always)]
unsafe fn id_delim_var<const DELIM: u8>(p: *const u8) -> Option<(RawId, *const u8)> {
    let (p, mask, expect, quoted) = open_delim::<DELIM>(p);
    let end4 = p.add(8).cast::<u32>().read_unaligned();
    let is8 = ((end4 as u16) & mask) == expect;
    let is9 = (((end4 >> 8) as u16) & mask) == expect;
    if (u8::from(is8) | u8::from(is9)) != 0 {
        let first = *p;
        let raw_tail = ld8(p.add(1));
        let tail = if is8 { raw_tail & 0x00FF_FFFF_FFFF_FFFF } else { raw_tail };
        let filled = if is8 { tail | 0x3000_0000_0000_0000 } else { tail };
        if (first.wrapping_sub(0x30) < 0x50)
            & (((filled.wrapping_sub(0x3030_3030_3030_3030) | filled) & 0x8080_8080_8080_8080) == 0)
        {
            let len = if is8 { 8 } else { 9 };
            let key = (u64::from(first) << 56) | _pext_u64(tail.swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F);
            return Some((RawId::Short(key), p.add(len + 1 + quoted)));
        }
        return None;
    }
    if ends_delim(p, 33, mask, expect) {
        let b0 = *p;
        let v = _mm256_loadu_si256(p.add(1).cast::<__m256i>());
        let bad = _mm256_or_si256(
            _mm256_or_si256(
                _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8)),
                _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8)),
            ),
            _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'\n' as i8)),
        );
        if (_mm256_movemask_epi8(bad) == 0) & (b0 != b',') & (b0 != b'"') & (b0 != b'\n') {
            return Some((RawId::Long(p, 33), p.add(34 + quoted)));
        }
    }
    None
}

#[inline(always)]
unsafe fn encode_slice_raw(s: &[u8]) -> RawId {
    if let Some(k) = u64::enc(s.as_ptr(), s.len()) {
        RawId::Short(k)
    } else {
        RawId::Long(s.as_ptr(), s.len())
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
    Some(Tail { comma: commas.trailing_zeros() as usize, lf: lf as usize, quotes })
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

// ============================================================================
// Stage 1: Hybrid Links (mixed short u64 + arbitrary long byte strings)
// ============================================================================

#[derive(Clone, Copy)]
struct LongLinkPair {
    raw_hash: u64,
    raw_link_id: u32,
    canon_tagged: u64,
}

struct HybridBucketLinks {
    has_links: bool,
    short_pairs: Vec<(u64, u64)>, // (raw_short_mixed63, canon_tagged)
    short_offsets: Vec<usize>,
    long_pairs: Vec<LongLinkPair>, // sorted into P2 buckets by raw_hash >> (64 - p2_bits)
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
    fn long_part(&self, q: usize) -> &[LongLinkPair] {
        &self.long_pairs[self.long_offsets[q]..self.long_offsets[q + 1]]
    }
}

/// 4-way AVX2 open-addressed relocation table for short keys (`raw_mixed -> canon_tagged`).
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
        let cap = ((pairs.len() + 1) / 2).next_power_of_two().max(4);
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

    #[inline(always)]
    fn get(&self, raw_mixed: u64) -> Option<u64> {
        if self.buckets.is_empty() {
            return None;
        }
        let mut slot = (raw_mixed as usize) & self.mask;
        let needle = unsafe { _mm256_set1_epi64x(raw_mixed as i64) };
        loop {
            let b = unsafe { self.buckets.get_unchecked(slot) };
            let keys = unsafe { _mm256_load_si256(b.keys.as_ptr().cast()) };
            let hits = unsafe {
                _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(keys, needle)))
            } as u32;
            if hits != 0 {
                return Some(unsafe { *b.vals.get_unchecked(hits.trailing_zeros() as usize) });
            }
            if b.keys[3] == 0 {
                return None;
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

/// Tiny L1d-resident relocation table for long keys in partition `q` (~175 entries -> 4 KiB).
#[derive(Default)]
struct LongRelocMap {
    table: Vec<u64>, // ((h25 as u64) << 32) | (entry_idx + 1)
    mask: usize,
}

impl LongRelocMap {
    fn build(&mut self, pairs: &[LongLinkPair]) {
        if pairs.is_empty() {
            self.table.clear();
            self.mask = 0;
            return;
        }
        let cap = (pairs.len() * 2).next_power_of_two().max(16);
        self.table.clear();
        self.table.resize(cap, 0);
        self.mask = cap - 1;
        for (idx, p) in pairs.iter().enumerate() {
            let h25 = (p.raw_hash as u32) & 0x01FF_FFFF;
            let mut slot = (h25 as usize) & self.mask;
            while unsafe { *self.table.get_unchecked(slot) } != 0 {
                slot = (slot + 1) & self.mask;
            }
            unsafe { *self.table.get_unchecked_mut(slot) = ((h25 as u64) << 32) | (idx as u64 + 1) };
        }
    }

    #[inline(always)]
    fn get(
        &self,
        tag: u64,
        pairs: &[LongLinkPair],
        worker_arenas: &[Vec<u8>],
        link_store: &LinkLongStore,
    ) -> Option<u64> {
        if self.table.is_empty() {
            return None;
        }
        let h25 = long_user_hash25(tag);
        let mut slot = (h25 as usize) & self.mask;
        let tid = ((tag >> 57) & 31) as usize;
        let off = tag as u32;
        loop {
            let v = unsafe { *self.table.get_unchecked(slot) };
            if v == 0 {
                return None;
            }
            if (v >> 32) as u32 == h25 {
                let entry = unsafe { *pairs.get_unchecked((v as u32 - 1) as usize) };
                let s = unsafe { get_long_bytes(worker_arenas.get_unchecked(tid), off) };
                if s == unsafe { link_store.get(entry.raw_link_id) } {
                    return Some(entry.canon_tagged);
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

fn uf_find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = p[x as usize].load(Ordering::Relaxed);
        if px == x {
            return x;
        }
        let gp = p[px as usize].load(Ordering::Relaxed);
        if gp != px {
            let _ = p[x as usize].compare_exchange_weak(px, gp, Ordering::Relaxed, Ordering::Relaxed);
        }
        x = gp;
    }
}

#[inline(always)]
fn cmp_node_key(ka: u64, kb: u64, link_store: &LinkLongStore) -> CmpOrd {
    if ((ka | kb) as i64) >= 0 {
        return ka.cmp(&kb);
    }
    cmp_node_key_slow(ka, kb, link_store)
}

#[cold]
fn cmp_node_key_slow(ka: u64, kb: u64, link_store: &LinkLongStore) -> CmpOrd {
    let mut ba = [0u8; 16];
    let mut bb = [0u8; 16];
    let sa: &[u8] = if is_short(ka) {
        let n = ka.unpack_buf(&mut ba);
        &ba[..n]
    } else {
        unsafe { link_store.get(ka as u32) }
    };
    let sb: &[u8] = if is_short(kb) {
        let n = kb.unpack_buf(&mut bb);
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
        if p[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

struct LinkParseWorker {
    tid: usize,
    pairs: Vec<(u64, u64)>,
    long_bytes: Vec<u8>,
    cache: LongUserCache,
    buf: Vec<u8>,
}

fn links_direct_hybrid(
    buf: &[u8],
    lo: usize,
    hi: usize,
    tid: usize,
    pairs: &mut Vec<(u64, u64)>,
    long_bytes: &mut Vec<u8>,
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
    let encode_ep = |raw: RawId, long_bytes: &mut Vec<u8>, cache: &mut LongUserCache| -> Option<u64> {
        match raw {
            RawId::Short(k) => Some(k),
            RawId::Long(ptr, len) => {
                let s = unsafe { std::slice::from_raw_parts(ptr, len) };
                let h = hash_long(s);
                let Some(off) = cache.intern(long_bytes, s, h) else {
                    flag.set(Need::Slow);
                    return None;
                };
                Some(make_parse_long_user(tid, h, off))
            }
        }
    };
    while p < end {
        let fast = unsafe {
            id_delim_var::<b','>(p).and_then(|(a, next)| {
                id_delim_var::<b'\n'>(next).map(|(b, eol)| (a, b, eol))
            })
        };
        if let Some((ra, rb, next)) = fast {
            if let (Some(a), Some(b)) = (
                encode_ep(ra, long_bytes, cache),
                encode_ep(rb, long_bytes, cache),
            ) {
                if a != b {
                    pairs.push((a, b));
                }
            }
            p = next;
        } else {
            let start = unsafe { p.offset_from(base) as usize };
            let lf = start + scan::memchr(b'\n', &buf[start..hi]).expect("LF-terminated chunk");
            if let Some(f) = scan::row_fields::<2>(buf, start, lf) {
                if flen(f[0]) > 0 && flen(f[1]) > 0 {
                    let ra = unsafe { encode_slice_raw(field(buf, f[0])) };
                    let rb = unsafe { encode_slice_raw(field(buf, f[1])) };
                    if let (Some(a), Some(b)) = (
                        encode_ep(ra, long_bytes, cache),
                        encode_ep(rb, long_bytes, cache),
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

struct Map63 {
    e: Vec<(u64, u32)>,
    mask: usize,
}

impl Map63 {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        Self { e: vec![(0, 0); cap], mask: cap - 1 }
    }

    fn reset(&mut self, n: usize) {
        let cap = (n * 2).next_power_of_two().max(16);
        if cap > self.e.len() {
            *self = Self::with_capacity(n);
        } else {
            self.mask = cap - 1;
            self.e[..cap].fill((0, 0));
        }
    }

    #[inline(always)]
    fn insert(&mut self, k: u64, v: u32) -> (u32, bool) {
        let mut s = (mix63(k) as usize) & self.mask;
        loop {
            let slot = unsafe { self.e.get_unchecked_mut(s) };
            if slot.0 == k {
                return (slot.1, false);
            }
            if slot.0 == 0 {
                *slot = (k, v);
                return (v, true);
            }
            s = (s + 1) & self.mask;
        }
    }
}

fn read_links_hybrid(
    ctx: &Ctx<'_>,
    threads: usize,
    p2_bits: u32,
    tm: &mut Timer,
    scratch: &mut Scratch<'_>,
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

    let mut workers: Vec<LinkParseWorker> = par::run(
        threads,
        link_chunks.len(),
        |tid| LinkParseWorker {
            tid,
            pairs: Vec::with_capacity(est_per_thr),
            long_bytes: Vec::new(),
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
    let links = build_bucket_links_hybrid(&pairs_by_w, &raw_long_by_w, p2_bits, threads, scratch);
    drop(pairs_by_w);
    drop(raw_long_by_w);
    tm.lap("links-uf");
    Ok(links)
}

fn build_bucket_links_hybrid(
    pairs_by_w: &[Vec<(u64, u64)>],
    raw_long_by_w: &[Vec<u8>],
    p2_bits: u32,
    threads: usize,
    scratch: &mut Scratch<'_>,
) -> HybridBucketLinks {
    let mut off = vec![0usize; pairs_by_w.len() + 1];
    for (i, v) in pairs_by_w.iter().enumerate() {
        off[i + 1] = off[i] + v.len();
    }
    let nl = off[pairs_by_w.len()];
    if nl == 0 {
        eprintln!("[ingest] links=0 linked_ids=0");
        return HybridBucketLinks::empty();
    }
    let off_ref = &off;

    // 1. Flat 2-pass count + scatter endpoints into LP = 256 partitions.
    let ep_part = |x: u64| -> usize {
        if is_short(x) {
            (mix63(x) >> (63 - LP_BITS)) as usize
        } else {
            let tid = ((x >> 57) & 31) as usize;
            let s = unsafe { get_long_bytes(raw_long_by_w.get_unchecked(tid), x as u32) };
            (hash_long(s) >> (64 - LP_BITS)) as usize
        }
    };
    struct Scattered<'a> {
        flat: ArenaVec<'a, (u64, u32)>,
        pos: [usize; LP + 1],
    }
    let scatter_space: Vec<_> = pairs_by_w
        .iter()
        .map(|p| Mutex::new(Some(scratch.buffer::<(u64, u32)>(2 * p.len()))))
        .collect();
    let scattered: Vec<Scattered<'_>> = par::run(threads, pairs_by_w.len(), |_| Vec::new(), |res, t| {
        let mut cnt = [0usize; LP];
        for &(a, b) in &pairs_by_w[t] {
            cnt[ep_part(a)] += 1;
            cnt[ep_part(b)] += 1;
        }
        let mut pos = [0usize; LP + 1];
        for p in 0..LP {
            pos[p + 1] = pos[p] + cnt[p];
        }
        let mut cur = [0usize; LP];
        cur.copy_from_slice(&pos[..LP]);
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
        unsafe { flat.set_len(pos[LP]) };
        res.push(Scattered { flat, pos });
    })
    .into_iter()
    .flatten()
    .collect();

    // 2. Per LP partition: intern short u64s and long byte strings into partition-local node IDs.
    struct InternPart<'a> {
        p: usize,
        keys: ArenaVec<'a, u64>, // raw short u64 OR LONG_BIT | local_long_idx
        long_bytes: Vec<u8>,
        long_meta: Vec<(u64, u32, u64)>, // (local_off, len, full_hash)
    }
    let mut ep = scratch.buffer::<u64>(2 * nl);
    let epp = SyncPtr(ep.as_mut_ptr());
    let scattered_ref = &scattered;
    let key_space = Mutex::new(&mut *scratch);
    let key_space_ref = &key_space;
    let mut parts: Vec<InternPart<'_>> = par::run(
        threads,
        LP,
        |_| (Vec::new(), Map63::with_capacity(1024), Vec::<u64>::new()),
        move |(res, short_m, long_tbl), p| {
            let n: usize = scattered_ref.iter().map(|s| s.pos[p + 1] - s.pos[p]).sum();
            short_m.reset(n);
            let mut lcap = ((n / 4).max(16) * 2).next_power_of_two();
            if long_tbl.len() < lcap {
                long_tbl.resize(lcap, 0);
            }
            long_tbl[..lcap].fill(0);
            let mut lmask = lcap - 1;
            let mut keys = key_space_ref.lock().unwrap().buffer::<u64>(n / 2 + 8);
            let mut long_bytes = Vec::<u8>::new();
            let mut long_meta = Vec::<(u64, u32, u64)>::new(); // (off, len, full_hash)
            let mut long_node_ids = Vec::<u32>::new();

            for s in scattered_ref.iter() {
                for &(k, e) in &s.flat[s.pos[p]..s.pos[p + 1]] {
                    let id = if is_short(k) {
                        let (id, ins) = short_m.insert(k, keys.len() as u32);
                        if ins {
                            keys.push(k);
                        }
                        id
                    } else {
                        let tid = ((k >> 57) & 31) as usize;
                        let bytes = unsafe { get_long_bytes(raw_long_by_w.get_unchecked(tid), k as u32) };
                        let h25 = long_user_hash25(k);
                        let mut slot = (h25 as usize) & lmask;
                        loop {
                            let v = unsafe { *long_tbl.get_unchecked(slot) };
                            if v == 0 {
                                let lidx = long_meta.len() as u32;
                                let nid = keys.len() as u32;
                                let aoff = long_bytes.len() as u64;
                                long_bytes.extend_from_slice(bytes);
                                let full_h = hash_long(bytes);
                                long_meta.push((aoff, bytes.len() as u32, full_h));
                                long_node_ids.push(nid);
                                keys.push(LONG_BIT | (lidx as u64));
                                unsafe {
                                    *long_tbl.get_unchecked_mut(slot) = ((h25 as u64) << 32) | (lidx as u64 + 1);
                                }
                                if long_meta.len() * 2 > lcap {
                                    lcap *= 2;
                                    lmask = lcap - 1;
                                    if long_tbl.len() < lcap {
                                        long_tbl.resize(lcap, 0);
                                    }
                                    long_tbl[..lcap].fill(0);
                                    for (li, &(_, _, mh)) in long_meta.iter().enumerate() {
                                        let mh25 = (mh as u32) & 0x01FF_FFFF;
                                        let mut ms = (mh25 as usize) & lmask;
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
                                let (moff, mlen, _) = unsafe { *long_meta.get_unchecked(li) };
                                if mlen as usize == bytes.len()
                                    && unsafe {
                                        long_bytes.get_unchecked(moff as usize..moff as usize + mlen as usize)
                                    } == bytes
                                {
                                    break unsafe { *long_node_ids.get_unchecked(li) };
                                }
                            }
                            slot = (slot + 1) & lmask;
                        }
                    };
                    unsafe { epp.write(e as usize, ((p as u64) << 32) | id as u64) };
                }
            }
            res.push(InternPart { p, keys, long_bytes, long_meta });
        },
    )
    .into_iter()
    .flat_map(|x| x.0)
    .collect();
    unsafe { ep.set_len(2 * nl) };
    drop(scattered);
    parts.sort_unstable_by_key(|x| x.p);

    let mut base = vec![0u32; LP + 1];
    let mut long_id_base = vec![0u32; LP + 1];
    let mut long_byte_base = vec![0usize; LP + 1];
    for p in 0..LP {
        base[p + 1] = base[p] + parts[p].keys.len() as u32;
        long_id_base[p + 1] = long_id_base[p] + parts[p].long_meta.len() as u32;
        long_byte_base[p + 1] = long_byte_base[p] + parts[p].long_bytes.len();
    }
    let nn = base[LP] as usize;
    let total_long_ids = long_id_base[LP] as usize;
    let total_long_bytes = long_byte_base[LP];

    // Assemble contiguous LinkLongStore + long_hashes in parallel!
    let mut link_bytes: Vec<u8> = Vec::with_capacity(total_long_bytes);
    let mut link_meta = Vec::with_capacity(total_long_ids);
    let mut link_hashes = Vec::with_capacity(total_long_ids);
    if total_long_ids > 0 {
        unsafe {
            link_bytes.set_len(total_long_bytes);
            link_meta.set_len(total_long_ids);
            link_hashes.set_len(total_long_ids);
        }
        let lb_ptr = SyncPtr(link_bytes.as_mut_ptr());
        let lm_ptr = SyncPtr(link_meta.as_mut_ptr());
        let lh_ptr = SyncPtr(link_hashes.as_mut_ptr());
        let (parts_ref, lib_ref, lbb_ref) = (&parts, &long_id_base, &long_byte_base);
        par::run(threads, LP, |_| (), move |_, p| {
            let ip = &parts_ref[p];
            if ip.long_meta.is_empty() {
                return;
            }
            let b_off = lbb_ref[p];
            let id_off = lib_ref[p] as usize;
            unsafe {
                lb_ptr.copy_from(b_off, &ip.long_bytes);
                for (k, &(off, len, h)) in ip.long_meta.iter().enumerate() {
                    lm_ptr.write(id_off + k, (b_off as u64 + off, len));
                    lh_ptr.write(id_off + k, h);
                }
            }
        });
    }
    let link_store = LinkLongStore { bytes: link_bytes, meta: link_meta };

    // 3. Parallel copy of `keys` (fixing up global long IDs) + parallel init of `parent`.
    let mut keys = scratch.buffer::<u64>(nn);
    let mut parent = scratch.buffer::<AtomicU32>(nn);
    {
        let (kp, pp) = (SyncPtr(keys.as_mut_ptr()), SyncPtr(parent.as_mut_ptr()));
        let (parts_ref, base_ref, lib_ref) = (&parts, &base, &long_id_base);
        par::run(threads, LP, |_| (), move |_, p| {
            let s = base_ref[p] as usize;
            let lbase = lib_ref[p] as u64;
            let src = &parts_ref[p].keys;
            for (k, &v) in src.iter().enumerate() {
                let gv = if is_short(v) { v } else { LONG_BIT | (lbase + (v as u32 as u64)) };
                unsafe {
                    kp.write(s + k, gv);
                    pp.write(s + k, AtomicU32::new((s + k) as u32));
                }
            }
        });
    }
    unsafe {
        keys.set_len(nn);
        parent.set_len(nn);
    }
    drop(parts);

    // 4. Parallel lock-free union-find by minimum byte-wise key.
    let node = |code: u64| base[(code >> 32) as usize] + code as u32;
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

    // 5. Fused root collection into P2-bucketed short_pairs & long_pairs.
    let p2 = 1usize << p2_bits;
    debug_assert!(p2_bits >= LP_BITS);
    let sub_bits = p2_bits - LP_BITS;
    let sub = 1usize << sub_bits;
    let short_shift = 63 - p2_bits;
    let long_shift = 64 - p2_bits;

    let mut short_tmp = scratch.buffer::<(u64, u64)>(nn);
    let mut long_tmp = scratch.buffer::<LongLinkPair>(total_long_ids.max(1));
    let stmp_ptr = SyncPtr(short_tmp.as_mut_ptr());
    let ltmp_ptr = SyncPtr(long_tmp.as_mut_ptr());

    let mut short_counts = vec![0usize; p2];
    let mut long_counts = vec![0usize; p2];
    let mut short_plen = [0usize; LP];
    let mut long_plen = [0usize; LP];

    {
        let (base_ref, lib_ref, parent_ref, keys_ref, lh_ref) =
            (&base, &long_id_base, &parent, &keys, &link_hashes);
        let sc_ptr = SyncPtr(short_counts.as_mut_ptr());
        let lc_ptr = SyncPtr(long_counts.as_mut_ptr());
        let sp_ptr = SyncPtr(short_plen.as_mut_ptr());
        let lp_ptr = SyncPtr(long_plen.as_mut_ptr());

        par::run(threads, LP, |_| (), move |_, p| {
            let (s, e) = (base_ref[p] as usize, base_ref[p + 1] as usize);
            let ls = lib_ref[p] as usize;
            let base_q = p << sub_bits;
            let mut slocal = [0usize; 16];
            let mut llocal = [0usize; 16];
            let mut sw = s;
            let mut lw = ls;

            for i in s..e {
                let r = uf_find(parent_ref, i as u32) as usize;
                let c = unsafe { *keys_ref.get_unchecked(r) };
                let k = unsafe { *keys_ref.get_unchecked(i) };
                if c != k {
                    let canon_tagged = if is_short(c) {
                        mix63(c)
                    } else {
                        let cid = c as u32;
                        let ch = unsafe { *lh_ref.get_unchecked(cid as usize) };
                        let cq = (ch >> long_shift) as usize;
                        make_link_long_user(cq, ch, cid)
                    };
                    if is_short(k) {
                        let rh = mix63(k);
                        let sub_i = ((rh >> short_shift) as usize) - base_q;
                        slocal[sub_i] += 1;
                        unsafe { stmp_ptr.write(sw, (rh, canon_tagged)) };
                        sw += 1;
                    } else {
                        let kid = k as u32;
                        let rh = unsafe { *lh_ref.get_unchecked(kid as usize) };
                        let sub_i = ((rh >> long_shift) as usize) - base_q;
                        llocal[sub_i] += 1;
                        unsafe {
                            ltmp_ptr.write(
                                lw,
                                LongLinkPair { raw_hash: rh, raw_link_id: kid, canon_tagged },
                            );
                        }
                        lw += 1;
                    }
                }
            }
            unsafe {
                sp_ptr.write(p, sw - s);
                lp_ptr.write(p, lw - ls);
                for sub_i in 0..sub {
                    sc_ptr.write(base_q + sub_i, slocal[sub_i]);
                    lc_ptr.write(base_q + sub_i, llocal[sub_i]);
                }
            }
        });
    }
    drop(parent);
    drop(keys);
    drop(link_hashes);

    let mut short_offsets = Vec::with_capacity(p2 + 1);
    let mut sacc = 0usize;
    for &c in &short_counts {
        short_offsets.push(sacc);
        sacc += c;
    }
    short_offsets.push(sacc);

    let mut long_offsets = Vec::with_capacity(p2 + 1);
    let mut lacc = 0usize;
    for &c in &long_counts {
        long_offsets.push(lacc);
        lacc += c;
    }
    long_offsets.push(lacc);

    eprintln!(
        "[ingest] links={nl} linked_ids={nn} mapped={} (short={sacc} long={lacc} long_uniq={total_long_ids})",
        sacc + lacc
    );
    if sacc + lacc == 0 {
        return HybridBucketLinks::empty();
    }

    let mut short_pairs = Vec::with_capacity(sacc);
    let mut long_pairs = Vec::with_capacity(lacc);
    let sp_out = SyncPtr(short_pairs.as_mut_ptr());
    let lp_out = SyncPtr(long_pairs.as_mut_ptr());
    let (base_ref, lib_ref, soff_ref, loff_ref, splen_ref, lplen_ref) = (
        &base,
        &long_id_base,
        &short_offsets,
        &long_offsets,
        &short_plen,
        &long_plen,
    );

    par::run(threads, LP, |_| (), move |_, p| {
        let slen = splen_ref[p];
        if slen > 0 {
            let src = unsafe { stmp_ptr.slice(base_ref[p] as usize, slen) };
            if sub == 1 {
                unsafe { sp_out.copy_from(soff_ref[p], src) };
            } else {
                let base_q = p << sub_bits;
                let mut cur = [0usize; 16];
                for sub_i in 0..sub {
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
            if sub == 1 {
                unsafe { lp_out.copy_from(loff_ref[p], src) };
            } else {
                let base_q = p << sub_bits;
                let mut cur = [0usize; 16];
                for sub_i in 0..sub {
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

    HybridBucketLinks {
        has_links: true,
        short_pairs,
        short_offsets,
        long_pairs,
        long_offsets,
        link_store,
    }
}

// ============================================================================
// Stage 2: Hybrid Direct Single-Pass Parse (Events + Clicks)
// ============================================================================

struct ParseRowsH<'a> {
    dt: &'a DateTable,
    ws: i64,
    we: i64,
    ws_sec: i64,
    we_sec: i64,
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
    tok: Sinks<Tok, 128>,
    ltok: Sinks<LongTok, 128>,
    b: Sinks<RecB<u64>, 128>,
    ck: Sinks<RecC<u64>, 192>,
    long_eids: Vec<u8>,
    long_users: Vec<u8>,
    long_cids: Vec<u8>,
    user_cache: LongUserCache,
    p1_shift: u32,
    p1_long_shift: u32,
    p2_short_shift: u32,
    p2_long_shift: u32,
}

impl ParseStateH {
    #[inline(always)]
    fn push_eid_token(&mut self, eid: RawId, ingest: u32, loc: u32, flag: &Flag) {
        match eid {
            RawId::Short(k) => {
                let eid_mixed = k.h();
                let p = (eid_mixed >> self.p1_shift) as usize;
                self.tok.push(p, Tok { eid: eid_mixed, ingest, loc });
            }
            RawId::Long(ptr, len) => {
                let s = unsafe { std::slice::from_raw_parts(ptr, len) };
                let eh = hash_long(s);
                let Some(off) = push_long_bytes(&mut self.long_eids, s) else {
                    flag.set(Need::Slow);
                    return;
                };
                let lp = (eh >> self.p1_long_shift) as usize;
                self.ltok.push(lp, LongTok { hash: eh as u32, off, ingest, loc });
            }
        }
    }

    #[inline(always)]
    fn encode_user(&mut self, user: RawId, flag: &Flag) -> Option<(u64, usize)> {
        match user {
            RawId::Short(k) => {
                let m = mix63(k);
                let q = (m >> self.p2_short_shift) as usize;
                Some((m, q))
            }
            RawId::Long(ptr, len) => {
                let s = unsafe { std::slice::from_raw_parts(ptr, len) };
                let uh = hash_long(s);
                let Some(off) = self.user_cache.intern(&mut self.long_users, s, uh) else {
                    flag.set(Need::Slow);
                    return None;
                };
                let q = (uh >> self.p2_long_shift) as usize;
                Some((make_parse_long_user(self.tid, uh, off), q))
            }
        }
    }

    #[inline(always)]
    fn emit_valid(&mut self, eid: RawId, ingest: u32, user: RawId, ts: u32, cc: u32, flag: &Flag) {
        let Some((person_tag, q)) = self.encode_user(user, flag) else {
            return;
        };
        let idx = self.b.push_idx(q, RecB { person: person_tag, ts, cc });
        if idx >= MAX_LOC_IDX {
            flag.set(Need::Slow);
            return;
        }
        let loc = ((q as u32) << LOC_IDX_BITS) | (idx as u32);
        self.push_eid_token(eid, ingest, loc, flag);
    }

    #[inline(always)]
    fn emit_invalid(&mut self, eid: RawId, ingest: u32, flag: &Flag) {
        self.push_eid_token(eid, ingest, NO_REC, flag);
    }

    #[inline(always)]
    fn emit_click(&mut self, cid: RawId, user: RawId, ts: u32, campaign: u32, flag: &Flag) {
        let Some((person_tag, q)) = self.encode_user(user, flag) else {
            return;
        };
        let cid_tag = match cid {
            RawId::Short(k) => k,
            RawId::Long(ptr, len) => {
                let s = unsafe { std::slice::from_raw_parts(ptr, len) };
                let ch = hash_long(s);
                let Some(off) = push_long_bytes(&mut self.long_cids, s) else {
                    flag.set(Need::Slow);
                    return;
                };
                make_long_cid(self.tid, ch, off)
            }
        };
        self.ck.push(q, RecC { person: person_tag, cid: cid_tag, ts, campaign });
    }
}

#[inline(always)]
unsafe fn event_hybrid(
    rows: &ParseRowsH<'_>,
    p: *const u8,
    st: &mut ParseStateH,
) -> Option<*const u8> {
    let (eid, p) = id_delim_var::<b','>(p)?;
    let (user, p) = id_delim_var::<b','>(p)?;
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
            st.emit_valid(eid, (ingest + 1) as u32, user, ts_rel, 0, rows.flag);
        } else {
            st.emit_invalid(eid, (ingest + 1) as u32, rows.flag);
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
                if let Some(cents) =
                    tsfast::digits8(p.add(tail.comma + 1 + qc), len).map(|v| v as u32)
                {
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
                return None;
            }
        } else {
            0
        };
        if valid {
            st.emit_valid(eid, (ingest + 1) as u32, user, ts_rel, cc, rows.flag);
        } else {
            st.emit_invalid(eid, (ingest + 1) as u32, rows.flag);
        }
        return Some(p.add(tail.lf + 1));
    }
    None
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
        let eid = unsafe { encode_slice_raw(field(buf, f[0])) };
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
        let mut valid_out: Option<(RawId, u32, u32)> = None;
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
            let user = unsafe { encode_slice_raw(field(buf, f[1])) };
            let ts_rel = (t - rows.ws) as u32;
            let cc = if cents != 0 {
                let cat = unsafe { rows.lookups.prod.get(fptr(buf, f[5]), flen(f[5])) };
                (cat << 24) | cents
            } else {
                0
            };
            valid_out = Some((user, ts_rel, cc));
        }
        match valid_out {
            Some((user, ts_rel, cc)) => st.emit_valid(eid, ingest, user, ts_rel, cc, rows.flag),
            None => st.emit_invalid(eid, ingest, rows.flag),
        }
    }
    lf + 1
}

#[inline(always)]
unsafe fn click_hybrid(
    rows: &ParseRowsH<'_>,
    p: *const u8,
    st: &mut ParseStateH,
) -> Option<*const u8> {
    let (cid, p) = id_delim_var::<b','>(p)?;
    let (user, p) = id_delim_var::<b','>(p)?;
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
        st.emit_click(cid, user, (t - base) as u32, campaign, rows.flag);
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
        let cid = unsafe { encode_slice_raw(field(buf, f[0])) };
        let user = unsafe { encode_slice_raw(field(buf, f[1])) };
        st.emit_click(cid, user, (t - base) as u32, campaign, rows.flag);
    }
    lf + 1
}

fn parse_events_buf_hybrid(rows: &ParseRowsH<'_>, buf: &[u8], lo: usize, hi: usize, st: &mut ParseStateH) {
    if lo >= hi {
        return;
    }
    assert!(hi < u32::MAX as usize && buf.len() >= hi + scan::PAD && buf[hi - 1] == b'\n');
    let base = buf.as_ptr();
    let mut p = unsafe { base.add(lo) };
    let end = unsafe { base.add(hi) };
    while p < end {
        p = unsafe {
            match event_hybrid(rows, p, st) {
                Some(next) => next,
                None => base.add(event_slow_hybrid(rows, buf, p.offset_from(base) as usize, hi, st)),
            }
        };
    }
}

fn parse_clicks_buf_hybrid(rows: &ParseRowsH<'_>, buf: &[u8], lo: usize, hi: usize, st: &mut ParseStateH) {
    if lo >= hi {
        return;
    }
    assert!(hi < u32::MAX as usize && buf.len() >= hi + scan::PAD && buf[hi - 1] == b'\n');
    let base = buf.as_ptr();
    let mut p = unsafe { base.add(lo) };
    let end = unsafe { base.add(hi) };
    while p < end {
        p = unsafe {
            match click_hybrid(rows, p, st) {
                Some(next) => next,
                None => base.add(click_slow_hybrid(rows, buf, p.offset_from(base) as usize, hi, st)),
            }
        };
    }
}

/// Sample the first ~32 data rows of events (< 20 us) to split arena capacity cleanly
/// between short `Tok` and `LongTok` (handles 0%, 12.5%, and 100% long-ID datasets!).
fn sample_long_eid_pct(files: &[FileInfo]) -> usize {
    let mut buf = vec![0u8; 4096 + scan::PAD + 1];
    let mut total = 0usize;
    let mut long_n = 0usize;
    for fi in files.iter().filter(|f| f.kind == Kind::Events && f.len > 0) {
        let n = (fi.len as usize).min(4096);
        let mut got = 0usize;
        while got < n {
            match fi.handle.read_at(&mut buf[got..n], got as u64) {
                Ok(0) => break,
                Ok(k) => got += k,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
        if got == 0 {
            continue;
        }
        buf[got..got + scan::PAD + 1].fill(0);
        let Some(first_nl) = scan::memchr(b'\n', &buf[..got]) else {
            continue;
        };
        let mut pos = first_nl + 1;
        if pos >= got {
            continue;
        }
        let hi = if fi.len > got as u64 {
            match buf[pos..got].iter().rposition(|&b| b == b'\n') {
                Some(p) => pos + p + 1,
                None => continue,
            }
        } else {
            if buf[got - 1] != b'\n' {
                buf[got] = b'\n';
                got + 1
            } else {
                got
            }
        };
        while pos < hi && total < 48 {
            let Some(rel_lf) = scan::memchr(b'\n', &buf[pos..hi]) else {
                break;
            };
            let lf = pos + rel_lf;
            if let Some(f) = scan::row_fields::<7>(&buf, pos, lf) {
                if flen(f[0]) > 0 {
                    total += 1;
                    let s = field(&buf, f[0]);
                    if unsafe { u64::enc(s.as_ptr(), s.len()) }.is_none() {
                        long_n += 1;
                    }
                }
            }
            pos = lf + 1;
        }
        if total >= 16 {
            break;
        }
    }
    if total == 0 {
        0
    } else {
        (long_n * 100) / total
    }
}

// ============================================================================
// Stage 3: Dedup (short Tok via DedupTable + long LongTok via exact byte check)
// ============================================================================

fn dedup_long_tokens(
    sinks: &[Sinks<LongTok, 128>],
    long_eids: &[Vec<u8>],
    dead: &DeadEvents,
    p1_long: usize,
    threads: usize,
) {
    par::run(
        threads,
        p1_long,
        |_| (Vec::<u32>::new(), Vec::<u64>::new()),
        |(t32, t64), p| {
            let mut slices = [(&[][..], 0u32, 0u32); MAX_THREADS * 2];
            let (active, total) = partition_slices(sinks, p, &mut slices);
            if total == 0 {
                return;
            }
            let nsl = active.len();
            let get_flat = |idx: u32| -> (&LongTok, u32) {
                let mut i = nsl - 1;
                while i > 0 && idx < active[i].1 {
                    i -= 1;
                }
                let (sl, base, tid) = active[i];
                (unsafe { sl.get_unchecked((idx - base) as usize) }, tid)
            };
            let cap = (total * 2).next_power_of_two();
            let mask = cap - 1;

            if total < (1 << 18) - 1 {
                if t32.len() < cap {
                    t32.resize(cap, 0);
                }
                let table = &mut t32[..cap];
                table.fill(0);
                let mut flat = 0u32;
                for &(sl, _, cur_tid) in active {
                    let cur_arena = unsafe { long_eids.get_unchecked(cur_tid as usize) };
                    for r in sl {
                        let h = r.hash;
                        let fp = h >> 18;
                        let mut slot = (h as usize) & mask;
                        loop {
                            let v = unsafe { *table.get_unchecked(slot) };
                            if v == 0 {
                                unsafe { *table.get_unchecked_mut(slot) = (fp << 18) | (flat + 1) };
                                break;
                            }
                            if (v >> 18) == fp {
                                let prev = (v & ((1 << 18) - 1)) - 1;
                                let (w, prev_tid) = get_flat(prev);
                                let w_bytes = unsafe {
                                    get_long_bytes(long_eids.get_unchecked(prev_tid as usize), w.off)
                                };
                                let r_bytes = unsafe { get_long_bytes(cur_arena, r.off) };
                                if w_bytes == r_bytes {
                                    if r.ingest < w.ingest {
                                        dead.mark(prev_tid as usize, w.loc);
                                        unsafe {
                                            *table.get_unchecked_mut(slot) = (fp << 18) | (flat + 1);
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
            } else {
                if t64.len() < cap {
                    t64.resize(cap, 0);
                }
                let table = &mut t64[..cap];
                table.fill(0);
                let mut flat = 0u32;
                for &(sl, _, cur_tid) in active {
                    let cur_arena = unsafe { long_eids.get_unchecked(cur_tid as usize) };
                    for r in sl {
                        let r_bytes = unsafe { get_long_bytes(cur_arena, r.off) };
                        let h = hash_long(r_bytes);
                        let fp = (h >> 32) as u32;
                        let mut slot = (h as usize) & mask;
                        loop {
                            let v = unsafe { *table.get_unchecked(slot) };
                            if v == 0 {
                                unsafe {
                                    *table.get_unchecked_mut(slot) =
                                        ((fp as u64) << 32) | (flat as u64 + 1);
                                }
                                break;
                            }
                            if (v >> 32) as u32 == fp {
                                let prev = (v as u32).wrapping_sub(1);
                                let (w, prev_tid) = get_flat(prev);
                                let w_bytes = unsafe {
                                    get_long_bytes(long_eids.get_unchecked(prev_tid as usize), w.off)
                                };
                                if w_bytes == r_bytes {
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
        },
    );
}

// ============================================================================
// Stage 4: Relocation (L2-resident ShortRelocMap + L1d-resident LongRelocMap)
// ============================================================================

#[allow(clippy::too_many_arguments)]
fn relocate_hybrid(
    b_sinks: &[Sinks<RecB<u64>, 128>],
    ck_sinks: &[Sinks<RecC<u64>, 192>],
    worker_long_users: &[Vec<u8>],
    dead: &DeadEvents,
    links: &HybridBucketLinks,
    free_arenas: Vec<SinkArena>,
    p2: usize,
    p2_bits: u32,
    threads: usize,
) -> (Vec<Sinks<RecB<u64>, 128>>, Vec<Sinks<RecC<u64>, 192>>, Vec<SinkArena>) {
    let total_ev: usize = b_sinks
        .iter()
        .map(|s| s.len.iter().map(|&x| x as usize).sum::<usize>())
        .sum();
    let total_ck: usize = ck_sinks
        .iter()
        .map(|s| s.len.iter().map(|&x| x as usize).sum::<usize>())
        .sum();
    let cap_rb = (total_ev * 58 / 100 / threads / p2).max(16) + 16;
    let cap_rc = (total_ck * 58 / 100 / threads / p2).max(16) + 16;
    let rb_bytes = Sinks::<RecB<u64>, 128>::arena_bytes(p2, cap_rb);
    let rc_bytes = Sinks::<RecC<u64>, 192>::arena_bytes(p2, cap_rc);

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
        reloc_b: Sinks<RecB<u64>, 128>,
        reloc_c: Sinks<RecC<u64>, 192>,
    }

    let carved_ref = &carved;
    let out: Vec<(Sinks<RecB<u64>, 128>, Sinks<RecC<u64>, 192>)> = par::run_fini(
        threads,
        p2,
        |tid| {
            let (reloc_b, reloc_c) =
                if let Some((rb_a, rc_a)) = carved_ref[tid].lock().unwrap().take() {
                    (Sinks::in_arena(p2, cap_rb, rb_a), Sinks::in_arena(p2, cap_rc, rc_a))
                } else {
                    (Sinks::new(p2, cap_rb), Sinks::new(p2, cap_rc))
                };
            RelocStateH {
                short_map: ShortRelocMap::default(),
                long_map: LongRelocMap::default(),
                reloc_b,
                reloc_c,
            }
        },
        |st, q| {
            let sp = links.short_part(q);
            let lp = links.long_part(q);
            if sp.is_empty() && lp.is_empty() {
                for (tid, sink) in b_sinks.iter().enumerate() {
                    unsafe {
                        sink.compact_part_with_dead_disjoint(q, dead.words(tid, q), |_| true);
                    }
                }
                return;
            }
            st.short_map.build(sp);
            st.long_map.build(lp);
            let sm = &st.short_map;
            let lm = &st.long_map;
            let reloc_b = &mut st.reloc_b;
            for (tid, sink) in b_sinks.iter().enumerate() {
                unsafe {
                    sink.compact_part_with_dead_disjoint(q, dead.words(tid, q), |r| {
                        let mapped = if is_short(r.person) {
                            sm.get(r.person)
                        } else {
                            lm.get(r.person, lp, worker_long_users, &links.link_store)
                        };
                        if let Some(canon_tagged) = mapped {
                            let cq = canon_q_of(canon_tagged, p2_bits);
                            reloc_b.push(cq, RecB { person: canon_tagged, ts: r.ts, cc: r.cc });
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
                        let mapped = if is_short(r.person) {
                            sm.get(r.person)
                        } else {
                            lm.get(r.person, lp, worker_long_users, &links.link_store)
                        };
                        if let Some(canon_tagged) = mapped {
                            let cq = canon_q_of(canon_tagged, p2_bits);
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
// Stage 5: Hybrid Group + Aggregate (ShortGroupMap + LongGroupMap)
// ============================================================================

struct ShortGroupMap {
    entries: Vec<GroupEntry<u64>>, // key != 0 (short mix63); id in unified 0..np; count unused
    used: usize,
    mask: usize,
}

impl ShortGroupMap {
    fn new() -> Self {
        Self { entries: Vec::new(), used: 0, mask: 0 }
    }

    fn reset(&mut self, keys: usize) {
        let cap = (keys * 2).next_power_of_two().max(16);
        self.entries.clear();
        self.entries.resize(cap, GroupEntry::default());
        self.used = 0;
        self.mask = cap - 1;
    }

    #[cold]
    fn grow(&mut self) {
        let cap = self.entries.len() * 2;
        let old = std::mem::replace(&mut self.entries, vec![GroupEntry::default(); cap]);
        self.mask = cap - 1;
        for entry in old {
            if entry.key != 0 {
                let mut i = (entry.key as usize) & self.mask;
                while self.entries[i].key != 0 {
                    i = (i + 1) & self.mask;
                }
                self.entries[i] = entry;
            }
        }
    }

    #[inline(always)]
    fn count(&mut self, key: u64, person_tags: &mut Vec<u64>, person_counts: &mut Vec<u32>) -> u32 {
        debug_assert!(key != 0 && is_short(key));
        let mut i = (key as usize) & self.mask;
        loop {
            let entry = unsafe { self.entries.get_unchecked_mut(i) };
            if entry.key == key {
                let id = entry.id;
                unsafe { *person_counts.get_unchecked_mut(id as usize) += 1 };
                return id;
            }
            if entry.key == 0 {
                let id = person_tags.len() as u32;
                *entry = GroupEntry { key, id, count: 0 };
                person_tags.push(key);
                person_counts.push(1);
                self.used += 1;
                if self.used * 2 > self.entries.len() {
                    self.grow();
                }
                return id;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, key: u64) -> Option<u32> {
        let mut i = (key as usize) & self.mask;
        loop {
            let entry = unsafe { self.entries.get_unchecked(i) };
            if entry.key == key {
                return Some(entry.id);
            }
            if entry.key == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// L1d-resident per-partition grouping table for long person IDs (both parse-arena and LinkLongStore).
struct LongGroupMap {
    table: Vec<u64>,        // ((h20 as u64) << 32) | (local_idx + 1)
    entries: Vec<(u32, u64)>, // (unified_pid, canonical_tag)
    mask: usize,
}

impl LongGroupMap {
    fn new() -> Self {
        Self { table: Vec::new(), entries: Vec::with_capacity(256), mask: 0 }
    }

    fn reset(&mut self, est_keys: usize) {
        let cap = (est_keys * 2).next_power_of_two().max(16);
        self.table.clear();
        self.table.resize(cap, 0);
        self.entries.clear();
        self.mask = cap - 1;
    }

    #[cold]
    fn grow(&mut self, worker_arenas: &[Vec<u8>], link_store: &LinkLongStore) {
        let cap = self.table.len() * 2;
        self.table.clear();
        self.table.resize(cap, 0);
        self.mask = cap - 1;
        let wide_hash = cap > (1 << 20);
        for (li, &(_, tag)) in self.entries.iter().enumerate() {
            let h20 = long_user_hash20(tag);
            let mut slot = if wide_hash {
                let s = unsafe { resolve_long_user(tag, worker_arenas, link_store) };
                (hash_long(s) as usize) & self.mask
            } else {
                (h20 as usize) & self.mask
            };
            while self.table[slot] != 0 {
                slot = (slot + 1) & self.mask;
            }
            self.table[slot] = ((h20 as u64) << 32) | (li as u64 + 1);
        }
    }

    #[inline(always)]
    fn count(
        &mut self,
        tag: u64,
        worker_arenas: &[Vec<u8>],
        link_store: &LinkLongStore,
        person_tags: &mut Vec<u64>,
        person_counts: &mut Vec<u32>,
    ) -> u32 {
        let h20 = long_user_hash20(tag);
        let mut slot = if self.table.len() > (1 << 20) {
            let s = unsafe { resolve_long_user(tag, worker_arenas, link_store) };
            (hash_long(s) as usize) & self.mask
        } else {
            (h20 as usize) & self.mask
        };
        loop {
            let v = unsafe { *self.table.get_unchecked(slot) };
            if v == 0 {
                let li = self.entries.len();
                let id = person_tags.len() as u32;
                self.entries.push((id, tag));
                person_tags.push(tag);
                person_counts.push(1);
                unsafe { *self.table.get_unchecked_mut(slot) = ((h20 as u64) << 32) | (li as u64 + 1) };
                if self.entries.len() * 2 > self.table.len() {
                    self.grow(worker_arenas, link_store);
                }
                return id;
            }
            if (v >> 32) as u32 == h20 {
                let (pid, stored_tag) = unsafe { *self.entries.get_unchecked((v as u32 - 1) as usize) };
                if stored_tag == tag
                    || unsafe {
                        resolve_long_user(stored_tag, worker_arenas, link_store)
                            == resolve_long_user(tag, worker_arenas, link_store)
                    }
                {
                    unsafe { *person_counts.get_unchecked_mut(pid as usize) += 1 };
                    return pid;
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, tag: u64, worker_arenas: &[Vec<u8>], link_store: &LinkLongStore) -> Option<u32> {
        if self.entries.is_empty() {
            return None;
        }
        let h20 = long_user_hash20(tag);
        let mut slot = if self.table.len() > (1 << 20) {
            let s = unsafe { resolve_long_user(tag, worker_arenas, link_store) };
            (hash_long(s) as usize) & self.mask
        } else {
            (h20 as usize) & self.mask
        };
        loop {
            let v = unsafe { *self.table.get_unchecked(slot) };
            if v == 0 {
                return None;
            }
            if (v >> 32) as u32 == h20 {
                let (pid, stored_tag) = unsafe { *self.entries.get_unchecked((v as u32 - 1) as usize) };
                if stored_tag == tag
                    || unsafe {
                        resolve_long_user(stored_tag, worker_arenas, link_store)
                            == resolve_long_user(tag, worker_arenas, link_store)
                    }
                {
                    return Some(pid);
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

#[inline]
fn sort_and_dedup_clicks_var(cl: &mut [CompactClick<u64>], cid_arenas: &[Vec<u8>]) -> usize {
    let n = cl.len();
    if n <= 1 {
        return n;
    }
    cl.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| cmp_cid(a.cid, b.cid, cid_arenas)));
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

struct DedupedHybrid {
    b_sinks: Vec<Sinks<RecB<u64>, 128>>,
    ck_sinks: Vec<Sinks<RecC<u64>, 192>>,
    reloc_b: Vec<Sinks<RecB<u64>, 128>>,
    reloc_c: Vec<Sinks<RecC<u64>, 192>>,
    worker_long_users: Vec<Vec<u8>>,
    worker_long_cids: Vec<Vec<u8>>,
    link_store: LinkLongStore,
    p2: usize,
    ne_raw: usize,
    dead: Option<DeadEvents>,
    parse_mapping: SinkArena,
    group_scratch: Vec<SinkArena>,
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
        worker_long_users,
        worker_long_cids,
        link_store,
        p2,
        ne_raw,
        dead,
        parse_mapping,
        mut group_scratch,
    } = deduped;

    let ud = output_dir.join("user_daily");
    let cd = output_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");

    let mut raw_cnt_part = vec![0usize; p2];
    for q in 0..p2 {
        raw_cnt_part[q] = b_sinks.iter().map(|s| s.count(q)).sum::<usize>()
            + reloc_b.iter().map(|s| s.count(q)).sum::<usize>();
    }
    let mut q_order: Vec<usize> = (0..p2).collect();
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
    let dense_min = env_usize("INGEST_DENSE_MIN").unwrap_or(32);
    let lookups = ctx.lookups.get();
    let ncamp = lookups.campaigns.len();
    let organic = ncamp as u32;
    let ncat = lookups.categories.len();
    let categories = &lookups.categories;

    struct FusedWorkerH<'a, P> {
        short_map: ShortGroupMap,
        long_map: LongGroupMap,
        person_tags: Vec<u64>,
        person_counts: Vec<u32>,
        fill: Vec<u32>,
        pid: ArenaVec<'a, u32>,
        ev_flat: ArenaVec<'a, CompactEv>,
        ev_buf: ArenaVec<'a, CompactEv>,
        ev_tmp: ArenaVec<'a, CompactEv>,
        cl_raw: ArenaVec<'a, (u32, CompactClick<u64>)>,
        ck_cnt: Vec<u32>,
        ck_has_long: Vec<u8>,
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

    let q_order = &q_order;
    let raw_cnt_part = &raw_cnt_part;
    let ud_ref = &ud;
    let wlu_ref = &worker_long_users;
    let wlc_ref = &worker_long_cids;
    let ls_ref = &link_store;
    let est_user_rows = ne_raw / threads / 8 + 1024;
    let scratch: Vec<_> = group_scratch.iter_mut().map(|a| Mutex::new(a.scratch())).collect();
    let group_scratch_available = !scratch.is_empty();
    let event_cap = if group_scratch_available {
        raw_cnt_part.iter().copied().max().unwrap_or(0).max(65536)
    } else {
        65536
    };
    let click_cap = if group_scratch_available {
        let raw_clicks: usize = ck_sinks
            .iter()
            .map(|s| s.len.iter().map(|&x| x as usize).sum::<usize>())
            .sum::<usize>()
            + reloc_c
                .iter()
                .map(|s| s.len.iter().map(|&x| x as usize).sum::<usize>())
                .sum::<usize>();
        (raw_clicks / p2 * 2).max(16384)
    } else {
        16384
    };

    let workers: Vec<FusedWorkerH<'_, P>> = par::run_fini(
        threads,
        p2,
        |tid| {
            let mut scratch = scratch
                .get(tid)
                .map(|slot| std::mem::take(&mut *slot.lock().unwrap()))
                .unwrap_or_default();
            FusedWorkerH {
                short_map: ShortGroupMap::new(),
                long_map: LongGroupMap::new(),
                person_tags: Vec::with_capacity(2048),
                person_counts: Vec::with_capacity(2048),
                fill: Vec::with_capacity(2048),
                pid: scratch.buffer(event_cap),
                ev_flat: scratch.buffer(event_cap),
                ev_buf: scratch.buffer(event_cap),
                ev_tmp: if group_scratch_available {
                    scratch.buffer(8192)
                } else {
                    ArenaVec::default()
                },
                cl_raw: scratch.buffer(click_cap),
                ck_cnt: Vec::with_capacity(2048),
                ck_has_long: Vec::with_capacity(2048),
                ck_off: Vec::with_capacity(2048),
                cl_buf: scratch.buffer(click_cap),
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
            w.profile.start();
            w.short_map.reset(raw_n / 25 + 16);
            w.long_map.reset(raw_n / 160 + 16);
            w.person_tags.clear();
            w.person_counts.clear();
            w.pid.clear();
            w.pid.reserve(raw_n);
            w.ev_flat.clear();
            w.ev_flat.reserve(raw_n);
            let pid = w.pid.as_mut_ptr();
            let ev_flat = w.ev_flat.as_mut_ptr();
            let mut live = 0usize;

            for_each_live(&b_sinks, dead.as_ref(), q, |r| {
                let id = if is_short(r.person) {
                    w.short_map.count(r.person, &mut w.person_tags, &mut w.person_counts)
                } else {
                    w.long_map.count(
                        r.person,
                        wlu_ref,
                        ls_ref,
                        &mut w.person_tags,
                        &mut w.person_counts,
                    )
                };
                unsafe {
                    pid.add(live).write(id);
                    ev_flat.add(live).write(CompactEv::new(r.ts, r.cc));
                }
                live += 1;
            });
            for_each_live(&reloc_b, None, q, |r| {
                let id = if is_short(r.person) {
                    w.short_map.count(r.person, &mut w.person_tags, &mut w.person_counts)
                } else {
                    w.long_map.count(
                        r.person,
                        wlu_ref,
                        ls_ref,
                        &mut w.person_tags,
                        &mut w.person_counts,
                    )
                };
                unsafe {
                    pid.add(live).write(id);
                    ev_flat.add(live).write(CompactEv::new(r.ts, r.cc));
                }
                live += 1;
            });
            if live == 0 {
                return;
            }
            w.ne += live;
            unsafe {
                w.pid.set_len(live);
                w.ev_flat.set_len(live);
            }
            let np = w.person_tags.len();
            w.np += np;
            w.fill.clear();
            w.fill.reserve(np);
            let mut acc = 0u32;
            for &c in &w.person_counts {
                w.fill.push(acc);
                acc += c;
            }
            w.ev_buf.clear();
            w.ev_buf.reserve(live);
            let ev_ptr = w.ev_buf.as_mut_ptr();
            for (&id, &ev) in w.pid.iter().zip(w.ev_flat.iter()) {
                let pos = unsafe { w.fill.get_unchecked_mut(id as usize) };
                unsafe { ev_ptr.add(*pos as usize).write(ev) };
                *pos += 1;
            }
            unsafe { w.ev_buf.set_len(live) };

            // Collect & count-scatter clicks for persons present in partition `q`.
            w.cl_raw.clear();
            w.ck_cnt.clear();
            w.ck_cnt.resize(np, 0);
            w.ck_has_long.clear();
            w.ck_has_long.resize(np, 0);
            for_each_live(&ck_sinks, None, q, |r| {
                let found = if is_short(r.person) {
                    w.short_map.get(r.person)
                } else {
                    w.long_map.get(r.person, wlu_ref, ls_ref)
                };
                if let Some(id) = found {
                    w.cl_raw.push((id, CompactClick { ts: r.ts, campaign: r.campaign, cid: r.cid }));
                    unsafe {
                        *w.ck_cnt.get_unchecked_mut(id as usize) += 1;
                        *w.ck_has_long.get_unchecked_mut(id as usize) |= u8::from(!is_short(r.cid));
                    }
                }
            });
            for_each_live(&reloc_c, None, q, |r| {
                let found = if is_short(r.person) {
                    w.short_map.get(r.person)
                } else {
                    w.long_map.get(r.person, wlu_ref, ls_ref)
                };
                if let Some(id) = found {
                    w.cl_raw.push((id, CompactClick { ts: r.ts, campaign: r.campaign, cid: r.cid }));
                    unsafe {
                        *w.ck_cnt.get_unchecked_mut(id as usize) += 1;
                        *w.ck_has_long.get_unchecked_mut(id as usize) |= u8::from(!is_short(r.cid));
                    }
                }
            });
            let ncl_raw = w.cl_raw.len();
            w.ck_off.clear();
            if ncl_raw > 0 {
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
                for &(id, ck) in w.cl_raw.iter() {
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

            w.profile.end(GroupPhase::Csr);

            let use_dense = w.dense.enabled();
            let mut ev_st = 0usize;
            let mut id_buf = [0u8; 16];
            for pi in 0..np {
                let ev_cnt = unsafe { *w.person_counts.get_unchecked(pi) } as usize;
                let ev_en = ev_st + ev_cnt;
                let dense_person = use_dense && ev_cnt >= dense_min;
                if !dense_person {
                    sort_ev_slice(&mut w.ev_buf[ev_st..ev_en], &mut w.ev_tmp);
                }
                let (cl_st, cl_len) = if ncl_raw > 0 {
                    let cs = unsafe { *w.ck_off.get_unchecked(pi) } as usize;
                    let ce = unsafe { *w.ck_off.get_unchecked(pi + 1) } as usize;
                    let len = if unsafe { *w.ck_has_long.get_unchecked(pi) } == 0 {
                        sort_and_dedup_clicks(&mut w.cl_buf[cs..ce], &mut w.cl_tmp)
                    } else {
                        sort_and_dedup_clicks_var(&mut w.cl_buf[cs..ce], wlc_ref)
                    };
                    w.nc += len;
                    (cs, len)
                } else {
                    (0, 0)
                };
                w.profile.end(GroupPhase::Sort);

                let ptag = unsafe { *w.person_tags.get_unchecked(pi) };
                let id_bytes: &[u8] = if is_short(ptag) {
                    let n = unmix63(ptag).unpack_buf(&mut id_buf);
                    &id_buf[..n]
                } else {
                    unsafe { resolve_long_user(ptag, wlu_ref, ls_ref) }
                };

                w.assists.s_start.clear();
                w.assists.s_day.clear();
                let mut cj = cl_st;
                let cl_end = cl_st + cl_len;
                let saved_cl = if cl_len > 0 {
                    let prev = unsafe { *w.cl_buf.get_unchecked(cl_end) };
                    unsafe { w.cl_buf.get_unchecked_mut(cl_end).ts = u32::MAX };
                    prev
                } else {
                    CompactClick { ts: u32::MAX, campaign: 0, cid: 0 }
                };
                let mut grp_first = cl_end;
                let mut last_ts = u32::MAX;
                let mut rel_day = 0u32;
                let mut next_day_s = DAY_MS_U32 - ws_rem;
                let (mut ud_sessions, mut ud_events, mut ud_revenue) = (0u32, 0u32, 0i64);

                if dense_person {
                    w.dense.scatter(&w.ev_buf[ev_st..ev_en]);
                    let mut has_sess = false;
                    let mut sess_start = 0u32;
                    let mut sess_max = 0u32;
                    let mut sess_last_bin = 0usize;
                    let mut sess_nev = 0u32;
                    let mut sess_rev = 0u64;
                    let mask_len = w.dense.active_mask.len();
                    for wi in 0..mask_len {
                        let mut word = unsafe { *w.dense.active_mask.get_unchecked(wi) };
                        if word == 0 {
                            continue;
                        }
                        unsafe { *w.dense.active_mask.get_unchecked_mut(wi) = 0 };
                        let base_bin = wi << 6;
                        while word != 0 {
                            let tz = word.trailing_zeros() as usize;
                            word &= word - 1;
                            let bi = base_bin + tz;
                            let bin = unsafe { *w.dense.bins.get_unchecked(bi) };
                            unsafe { *w.dense.bins.get_unchecked_mut(bi) = DenseBin::EMPTY };
                            if has_sess
                                && bi == sess_last_bin + 1
                                && bin.min_ts - sess_max <= GAP_MS_U32
                            {
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
                                if has_sess {
                                    emit_session(
                                        &mut w.camp,
                                        &mut w.assists,
                                        &w.cl_buf[..cl_end + usize::from(cl_len > 0)],
                                        cl_len > 0,
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
                                let s = bin.min_ts;
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
                                has_sess = true;
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
                    }
                    if has_sess {
                        emit_session(
                            &mut w.camp,
                            &mut w.assists,
                            &w.cl_buf[..cl_end + usize::from(cl_len > 0)],
                            cl_len > 0,
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
                    let mut i = ev_st;
                    while i < ev_en {
                        let first = unsafe { *w.ev_buf.get_unchecked(i) };
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
                            let cents = cc & 0x00FF_FFFF;
                            if cents != 0 {
                                rev += cents as u64;
                                let cat = cc >> 24;
                                let slot = unsafe { w.cat_sum.get_unchecked_mut(cat as usize) };
                                if *slot == 0 {
                                    w.cat_touched.push(cat);
                                }
                                *slot += cents as u64;
                            }
                            if k == ev_en {
                                break;
                            }
                            let e = unsafe { *w.ev_buf.get_unchecked(k) };
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

                        emit_session(
                            &mut w.camp,
                            &mut w.assists,
                            &w.cl_buf[..cl_end + usize::from(cl_len > 0)],
                            cl_len > 0,
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

                if cl_len > 0 {
                    unsafe { *w.cl_buf.get_unchecked_mut(cl_end) = saved_cl };
                    w.assists.count(&mut w.camp, &w.cl_buf[cl_st..cl_end]);
                }
                ev_st = ev_en;
                w.profile.end(GroupPhase::Aggregate);
            }
        },
        |w| {
            w.profile.start();
            w.written = w.writer.finish();
            w.profile.end(GroupPhase::Write);
            w.writer.release_buffers();
            drop(std::mem::take(&mut w.ev_buf));
            drop(std::mem::take(&mut w.ev_tmp));
            drop(std::mem::take(&mut w.cl_buf));
            drop(std::mem::take(&mut w.cl_tmp));
            drop(std::mem::take(&mut w.cl_raw));
            drop(std::mem::take(&mut w.pid));
            drop(std::mem::take(&mut w.ev_flat));
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
    let long_pct = sample_long_eid_pct(&ctx.files);
    // Estimate row count using realistic bytes/row based on observed long-ID share.
    let bytes_per_ev = 75u64 + ((long_pct as u64) * 52) / 100;
    let est_ev = (ctx.event_bytes / bytes_per_ev) as usize + 1024;
    let bytes_per_ck = 45u64 + ((long_pct as u64) * 52) / 100;
    let est_ck = (ctx.click_bytes / bytes_per_ck) as usize + 1024;

    let (short_pct, ltok_pct) = if long_pct == 0 {
        (109usize, 2usize)
    } else if long_pct < 50 {
        ((100 - long_pct) + 12, long_pct + 10)
    } else {
        ((100 - long_pct) + 5, long_pct + 10)
    };
    let p1_bits = plan.p1_bits;
    let p1 = plan.p1;
    let p2_bits = plan.p2_bits;
    let p2 = plan.p2;
    let p1_long_bits: u32 = if est_ev > 30_000_000 && long_pct >= 50 { 9 } else { P1_LONG_BITS };
    let p1_long: usize = 1 << p1_long_bits;

    let cap_tok = (est_ev * short_pct / 100 / workers / p1).max(16) + 16;
    let cap_ltok = (est_ev * ltok_pct / 100 / workers / p1_long).max(16) + 16;
    let cap_b = (est_ev * 108 / 100 / workers / p2).max(16) + 32;
    let cap_ck = (est_ck * 109 / 100 / workers / p2).max(16) + 32;

    let tok_bytes = Sinks::<Tok, 128>::arena_bytes(p1, cap_tok);
    let ltok_bytes = Sinks::<LongTok, 128>::arena_bytes(p1_long, cap_ltok);
    let b_bytes = Sinks::<RecB<u64>, 128>::arena_bytes(p2, cap_b);
    let ck_bytes = Sinks::<RecC<u64>, 192>::arena_bytes(p2, cap_ck);
    let parse_bytes = (tok_bytes + ltok_bytes + b_bytes + ck_bytes) * workers;

    let (arena, parse_mapping) = SinkArena::new(parse_bytes).split_at(parse_bytes);
    let (mut tokens, mut arena) = arena.split_at((tok_bytes + ltok_bytes) * workers);

    let mut links = read_links_hybrid(ctx, threads, p2_bits, tm, &mut tokens.scratch())?;

    let lookups = ctx.lookups.wait();
    if lookups.categories.len() > 256 {
        return Err(Need::Slow);
    }
    tm.lap("small-wait");

    let mut slices = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tok_both, rest) = tokens.split_at(tok_bytes + ltok_bytes);
        tokens = rest;
        let (tok, ltok) = tok_both.split_at(tok_bytes);
        let (b, rest) = arena.split_at(b_bytes);
        let (ck, rest) = rest.split_at(ck_bytes);
        slices.push(Mutex::new(Some((tok, ltok, b, ck))));
        arena = rest;
    }

    let flag = Flag(AtomicU8::new(0));
    let rows = ParseRowsH {
        dt: &ctx.dt,
        ws: ctx.ws,
        we: ctx.we,
        ws_sec: ctx.ws.div_euclid(1000),
        we_sec: ctx.we.div_euclid(1000),
        ws_whole_sec: ctx.ws % 1000 == 0 && ctx.we % 1000 == 0,
        lookups,
        flag: &flag,
    };
    let p1_shift = 64 - p1_bits;
    let p1_long_shift = 64 - p1_long_bits;
    let p2_short_shift = 63 - p2_bits;
    let p2_long_shift = 64 - p2_bits;
    let do_populate = env_usize("INGEST_POPULATE").unwrap_or(1) != 0;
    let files = &ctx.files;
    let main_chunks = &ctx.main_chunks;

    let mut states: Vec<(ParseStateH, Vec<u8>)> = par::run_fini(
        workers,
        main_chunks.len(),
        |tid| {
            let (tok, ltok, b, ck) = slices[tid].lock().unwrap().take().unwrap();
            if do_populate {
                tok.populate_write();
                ltok.populate_write();
                b.populate_write();
                ck.populate_write();
            }
            (
                ParseStateH {
                    tid,
                    tok: Sinks::in_arena(p1, cap_tok, tok),
                    ltok: Sinks::in_arena(p1_long, cap_ltok, ltok),
                    b: Sinks::in_arena(p2, cap_b, b),
                    ck: Sinks::in_arena(p2, cap_ck, ck),
                    long_eids: Vec::new(),
                    long_users: Vec::new(),
                    long_cids: Vec::new(),
                    user_cache: LongUserCache::new(),
                    p1_shift,
                    p1_long_shift,
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
            let c = main_chunks[i];
            let fi = &files[c.file as usize];
            let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, buf);
            match fi.kind {
                Kind::Events => parse_events_buf_hybrid(&rows, buf, lo, hi, st),
                Kind::Clicks => parse_clicks_buf_hybrid(&rows, buf, lo, hi, st),
                Kind::Links => unreachable!(),
            }
        },
        |(st, _)| {
            st.tok.finish();
            st.ltok.finish();
            st.b.finish();
            st.ck.finish();
        },
    );
    ctx.prefetch.stop();
    for (tid, (_, buf)) in states.iter_mut().enumerate() {
        *ctx.readers[tid].lock().unwrap() = std::mem::take(buf);
    }
    if let Some(n) = flag.get() {
        par::drop_par(workers, states);
        drop(parse_mapping);
        return Err(n);
    }
    tm.lap("parse");

    let mut tok_sinks = Vec::with_capacity(states.len());
    let mut ltok_sinks = Vec::with_capacity(states.len());
    let mut b_sinks = Vec::with_capacity(states.len());
    let mut ck_sinks = Vec::with_capacity(states.len());
    let mut long_eids = Vec::with_capacity(states.len());
    let mut worker_long_users = Vec::with_capacity(states.len());
    let mut worker_long_cids = Vec::with_capacity(states.len());
    let mut ne_raw = 0usize;
    let mut ne_long = 0usize;
    for (st, _) in states {
        let ns: usize = st.tok.len.iter().map(|&x| x as usize).sum();
        let nl: usize = st.ltok.len.iter().map(|&x| x as usize).sum();
        ne_raw += ns + nl;
        ne_long += nl;
        tok_sinks.push(st.tok);
        ltok_sinks.push(st.ltok);
        b_sinks.push(st.b);
        ck_sinks.push(st.ck);
        long_eids.push(st.long_eids);
        worker_long_users.push(st.long_users);
        worker_long_cids.push(st.long_cids);
    }

    let dead = DeadEvents::new(&b_sinks, p2, threads);
    if ne_raw > ne_long {
        par::run(threads, p1, |_| DedupTable::default(), |table, p| {
            let mut slices = [(&[][..], 0, 0); MAX_THREADS * 2];
            let (active, total) = partition_slices(&tok_sinks, p, &mut slices);
            table.mark_losers(active, total, |_, tid, loser| dead.mark(tid as usize, loser.loc));
        });
    }
    if ne_long > 0 {
        dedup_long_tokens(&ltok_sinks, &long_eids, &dead, p1_long, threads);
    }
    // Immediately free long_eids (~88 MiB) and reclaim tok + ltok huge-page arenas!
    drop(long_eids);
    let tok_arenas: Vec<SinkArena> = tok_sinks.into_iter().map(Sinks::into_arena).collect();
    let ltok_arenas: Vec<SinkArena> = ltok_sinks.into_iter().map(Sinks::into_arena).collect();
    // Rejoin adjacent (tok, ltok) slices? They were split from contiguous `tok_both`, or keep larger slice!
    let group_scratch: Vec<SinkArena> = if tok_bytes >= ltok_bytes {
        drop(ltok_arenas);
        tok_arenas
    } else {
        drop(tok_arenas);
        ltok_arenas
    };
    tm.lap("dedup");

    let (reloc_b, reloc_c, dead, group_scratch) = if links.has_links {
        let (rb, rc, rem) = relocate_hybrid(
            &b_sinks,
            &ck_sinks,
            &worker_long_users,
            &dead,
            &links,
            group_scratch,
            p2,
            p2_bits,
            threads,
        );
        // Drop link pair arrays, keeping ONLY compact `link_store` (~5 MiB) for group+agg!
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
        worker_long_users,
        worker_long_cids,
        link_store: links.link_store,
        p2,
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
        // Byte 24 specifically changes hash!
        let mut d = *a;
        d[24] ^= 1;
        assert_ne!(hash_long(a), hash_long(&d));
        // Byte 32 specifically changes hash!
        let mut e = *a;
        e[32] ^= 1;
        assert_ne!(hash_long(a), hash_long(&e));
    }

    #[test]
    fn id_delim_var_handles_short_and_33b_quoted_and_unquoted() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"e12345678,\"u0123456789abcdef0123456789abcdef\",u8765432,");
        buf.extend_from_slice(b"\"kdeadbeef00112233445566778899aabb\"\n");
        let end = buf.len();
        buf.resize(end + scan::PAD, 0);
        unsafe {
            let p = buf.as_ptr();
            let (r1, p) = id_delim_var::<b','>(p).unwrap();
            assert!(matches!(r1, RawId::Short(k) if k == u64::enc_slice(b"e12345678").unwrap()));
            let (r2, p) = id_delim_var::<b','>(p).unwrap();
            match r2 {
                RawId::Long(ptr, len) => {
                    assert_eq!(len, 33);
                    assert_eq!(std::slice::from_raw_parts(ptr, len), b"u0123456789abcdef0123456789abcdef");
                }
                _ => panic!("expected long"),
            }
            let (r3, p) = id_delim_var::<b','>(p).unwrap();
            assert!(matches!(r3, RawId::Short(k) if k == u64::enc_slice(b"u8765432").unwrap()));
            let (r4, p) = id_delim_var::<b'\n'>(p).unwrap();
            match r4 {
                RawId::Long(ptr, len) => {
                    assert_eq!(len, 33);
                    assert_eq!(std::slice::from_raw_parts(ptr, len), b"kdeadbeef00112233445566778899aabb");
                }
                _ => panic!("expected long"),
            }
            assert_eq!(p.offset_from(buf.as_ptr()) as usize, end);
        }
    }

    #[test]
    fn hybrid_links_and_click_cmp_short_vs_long() {
        let mut arena = Vec::new();
        let o1 = push_long_bytes(&mut arena, b"u00000000000000000000000000000001").unwrap();
        let o2 = push_long_bytes(&mut arena, b"uzz000000000000000000000000000002").unwrap();
        let h1 = hash_long(b"u00000000000000000000000000000001");
        let h2 = hash_long(b"uzz000000000000000000000000000002");
        let t1 = make_parse_long_user(0, h1, o1);
        let t2 = make_parse_long_user(0, h2, o2);
        let s_mid = u64::enc_slice(b"u5abcdef").unwrap();
        let pairs = vec![vec![(t1, s_mid), (s_mid, t2)]];
        let raw_long = vec![arena];
        let mut scratch = Scratch::default();
        let bl = build_bucket_links_hybrid(&pairs, &raw_long, 8, 2, &mut scratch);
        assert!(bl.has_links);
        // Byte-wise smallest is t1 ("u000...") < s_mid ("u5...") < t2 ("uzz...").
        let q_mid = (mix63(s_mid) >> (63 - 8)) as usize;
        let mut sm = ShortRelocMap::default();
        sm.build(bl.short_part(q_mid));
        let canon = sm.get(mix63(s_mid)).unwrap();
        assert!(!is_short(canon));
        assert_eq!(
            unsafe { resolve_long_user(canon, &[], &bl.link_store) },
            b"u00000000000000000000000000000001"
        );
        let q_t2 = (h2 >> (64 - 8)) as usize;
        let mut lm = LongRelocMap::default();
        let lp = bl.long_part(q_t2);
        lm.build(lp);
        let canon2 = lm.get(t2, lp, &raw_long, &bl.link_store).unwrap();
        assert_eq!(canon2, canon);
    }
}

