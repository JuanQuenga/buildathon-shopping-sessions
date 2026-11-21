//! Exact fixed-width id keys, generic over width.
//!
//! * `u128` ("wide"): any id of at most 15 bytes (bytes big-endian + length byte).
//! * `u64` ("compact"): ids of 1..=10 chars from `[0-9A-Za-z]`, 6 bits per char,
//!   first char in the top bits, code 0 = padding. Order-preserving and injective.
//!
//! In both, key equality == byte equality and key order == byte-wise order. The
//! empty id maps to 0, which is used as the empty-slot marker and never stored.
use super::key as wide;

pub trait Key: Copy + Eq + Ord + Default + Send + Sync + std::fmt::Debug + 'static {
    /// Encode `len >= 1` bytes at `p` (64 readable bytes after `p` guaranteed).
    /// `None` = this id cannot be represented in this key type.
    unsafe fn enc(p: *const u8, len: usize) -> Option<Self>;
    #[cfg(test)]
    fn enc_slice(s: &[u8]) -> Option<Self> {
        let mut b = [0u8; 80];
        if s.len() > 16 {
            return None;
        }
        b[..s.len()].copy_from_slice(s);
        if s.is_empty() {
            return Some(Self::default());
        }
        unsafe { Self::enc(b.as_ptr(), s.len()) }
    }
    fn h(self) -> u64;
    fn unpack(self, out: &mut Vec<u8>);
    /// Unpack bytes into a 16-byte stack buffer, returning the byte length.
    fn unpack_buf(self, dst: &mut [u8; 16]) -> usize;
    fn is_zero(self) -> bool;
}

#[inline(always)]
fn mix(x: u64) -> u64 {
    let x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let x = (x ^ (x >> 29)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^ (x >> 32)
}

impl Key for u128 {
    #[inline(always)]
    unsafe fn enc(p: *const u8, len: usize) -> Option<u128> {
        if len > wide::MAX_INLINE {
            return None;
        }
        Some(wide::pack_at(p, len))
    }
    #[inline(always)]
    fn h(self) -> u64 {
        mix((self as u64) ^ ((self >> 64) as u64).rotate_left(23).wrapping_mul(0xD6E8_FEB8_6659_FD93))
    }
    fn unpack(self, out: &mut Vec<u8>) {
        wide::unpack(self, out)
    }
    #[inline(always)]
    fn unpack_buf(self, dst: &mut [u8; 16]) -> usize {
        let len = (self & 0xff) as usize;
        *dst = self.to_be_bytes();
        len
    }
    #[inline(always)]
    fn is_zero(self) -> bool {
        self == 0
    }
}

#[repr(align(16))]
struct LiveMasks([[u8; 16]; 11]);

static LIVE_MASKS: LiveMasks = LiveMasks([
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0],
    [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0],
]);

static DEC_LUT: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut c = 1usize;
    while c <= 10 {
        t[c] = (c as u8) + 0x2F;
        c += 1;
    }
    while c <= 36 {
        t[c] = (c as u8) + 0x36;
        c += 1;
    }
    while c <= 62 {
        t[c] = (c as u8) + 0x3C;
        c += 1;
    }
    t
};

impl Key for u64 {
    #[inline(always)]
    unsafe fn enc(p: *const u8, len: usize) -> Option<u64> {
        if len > 10 {
            return None;
        }
        use std::arch::x86_64::*;
        let v = _mm_loadu_si128(p as *const __m128i);
        let live = _mm_load_si128(LIVE_MASKS.0.as_ptr().add(len) as *const __m128i);
        // Unsigned range checks via signed shift by -128: (x - base - 128) < (-128 + width)
        // Digits '0'..='9' (0x30..=0x39): base = 0x30, width = 10 -> sub 0xB0 (-80), cmpgt -118
        let dig = _mm_cmpgt_epi8(_mm_set1_epi8(-118), _mm_sub_epi8(v, _mm_set1_epi8(0xB0u8 as i8)));
        // Alpha 'A'..='Z' or 'a'..='z': (v | 0x20) in 0x61..=0x7A -> sub 0xE1 (-31), cmpgt -102
        let v_lo = _mm_or_si128(v, _mm_set1_epi8(0x20));
        let alpha = _mm_cmpgt_epi8(_mm_set1_epi8(-102), _mm_sub_epi8(v_lo, _mm_set1_epi8(0xE1u8 as i8)));
        let bad = _mm_andnot_si128(_mm_or_si128(dig, alpha), live);
        if _mm_movemask_epi8(bad) != 0 {
            return None;
        }
        // Single-cycle pshufb offset LUT indexed by high nibble (3->0x2F, 4..5->0x36, 6..7->0x3C).
        let hi = _mm_and_si128(_mm_srli_epi16(v, 4), _mm_set1_epi8(0x0F));
        let off_lut = _mm_setr_epi8(0, 0, 0, 0x2F, 0x36, 0x36, 0x3C, 0x3C, 0, 0, 0, 0, 0, 0, 0, 0);
        let off = _mm_shuffle_epi8(off_lut, hi);
        let code = _mm_and_si128(_mm_sub_epi8(v, off), live);
        // pairs: c0*64 + c1 (12 bits) in 16-bit lanes
        let pairs = _mm_maddubs_epi16(code, _mm_set1_epi16(0x0140));
        // quads: p0*4096 + p1 (24 bits) in 32-bit lanes
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_1000));
        let l0 = _mm_cvtsi128_si32(quads) as u32 as u64;
        let l1 = _mm_extract_epi32::<1>(quads) as u32 as u64;
        let l2 = _mm_extract_epi32::<2>(quads) as u32 as u64;
        Some((l0 << 36) | (l1 << 12) | (l2 >> 12))
    }
    #[inline(always)]
    fn h(self) -> u64 {
        mix(self)
    }
    fn unpack(self, out: &mut Vec<u8>) {
        let mut buf = [0u8; 16];
        let n = self.unpack_buf(&mut buf);
        out.extend_from_slice(&buf[..n]);
    }
    #[inline(always)]
    fn unpack_buf(self, dst: &mut [u8; 16]) -> usize {
        let mut x = self << 4;
        let mut n = 0usize;
        while x != 0 {
            let c = (x >> 58) as usize;
            unsafe { *dst.get_unchecked_mut(n) = *DEC_LUT.get_unchecked(c) };
            n += 1;
            x <<= 6;
        }
        n
    }
    #[inline(always)]
    fn is_zero(self) -> bool {
        self == 0
    }
}

/// Open-addressing map K -> V (exact keys; zero key = empty slot), one cache-friendly entry array.
#[derive(Clone)]
pub struct Map<K: Key, V: Copy + Default> {
    e: Vec<(K, V)>,
    mask: usize,
    len: usize,
}

impl<K: Key, V: Copy + Default> Map<K, V> {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        Map { e: vec![(K::default(), V::default()); cap], mask: cap - 1, len: 0 }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
    /// Reset to empty with room for `n` keys, reusing the allocation when possible.
    pub fn reset(&mut self, n: usize) {
        let cap = (n * 2).next_power_of_two().max(16);
        if cap > self.e.len() {
            *self = Self::with_capacity(n);
        } else {
            self.mask = cap - 1;
            self.e[..cap].fill((K::default(), V::default()));
            self.len = 0;
        }
    }
    /// Insert if absent; returns (value, inserted).
    #[inline(always)]
    pub fn insert(&mut self, k: K, v: V) -> (V, bool) {
        let mut s = k.h() as usize & self.mask;
        loop {
            let slot = unsafe { self.e.get_unchecked_mut(s) };
            if slot.0 == k {
                return (slot.1, false);
            }
            if slot.0.is_zero() {
                *slot = (k, v);
                self.len += 1;
                return (v, true);
            }
            s = (s + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn get(&self, k: K) -> Option<V> {
        let mut s = k.h() as usize & self.mask;
        loop {
            let slot = unsafe { self.e.get_unchecked(s) };
            if slot.0 == k {
                return Some(slot.1);
            }
            if slot.0.is_zero() {
                return None;
            }
            s = (s + 1) & self.mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_order_and_roundtrip() {
        let mut ids: Vec<Vec<u8>> = vec![
            b"0".to_vec(), b"9".to_vec(), b"A".to_vec(), b"Z".to_vec(), b"a".to_vec(), b"z".to_vec(),
            b"u1".to_vec(), b"U1".to_vec(), b"u10".to_vec(), b"u1a".to_vec(), b"zzzzzzzzzz".to_vec(),
            b"0000000000".to_vec(), b"e42huhoug".to_vec(), b"k1q2awe54".to_vec(), b"uduffvac".to_vec(),
        ];
        let mut rng = 0x9e3779b97f4a7c15u64;
        let alpha = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for _ in 0..3000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let len = 1 + (rng % 10) as usize;
            ids.push((0..len).map(|i| alpha[((rng >> (i * 6)) % 62) as usize]).collect());
        }
        for a in &ids {
            let ka = u64::enc_slice(a).unwrap();
            let mut out = Vec::new();
            ka.unpack(&mut out);
            assert_eq!(&out, a);
        }
        for a in ids.iter().take(300) {
            for b in &ids {
                let (ka, kb) = (u64::enc_slice(a).unwrap(), u64::enc_slice(b).unwrap());
                assert_eq!(a.cmp(b), ka.cmp(&kb), "{a:?} {b:?}");
                let (wa, wb) = (u128::enc_slice(a).unwrap(), u128::enc_slice(b).unwrap());
                assert_eq!(a.cmp(b), wa.cmp(&wb));
            }
        }
        for bad in [&b"u 1"[..], b"u-1", b"u_1", b"\xc3\xa9", b"zzzzzzzzzzz", b"u1\r", b"\"u1"] {
            assert!(u64::enc_slice(bad).is_none(), "{bad:?}");
        }
        assert!(u128::enc_slice(b"u 1").is_some());
    }
}
