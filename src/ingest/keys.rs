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
    fn is_zero(self) -> bool {
        self == 0
    }
}

impl Key for u64 {
    #[inline(always)]
    unsafe fn enc(p: *const u8, len: usize) -> Option<u64> {
        if len > 10 {
            return None;
        }
        use std::arch::x86_64::*;
        let v = _mm_loadu_si128(p as *const __m128i);
        // lanes >= len -> treated as padding (code 0)
        let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let live = _mm_cmpgt_epi8(_mm_set1_epi8(len as i8), idx);
        // ranges (signed compares are fine: all bounds < 0x80, bytes >= 0x80 are negative -> out of range)
        let ge = |c: u8| _mm_cmpgt_epi8(v, _mm_set1_epi8(c as i8 - 1));
        let le = |c: u8| _mm_cmpgt_epi8(_mm_set1_epi8(c as i8 + 1), v);
        let dig = _mm_and_si128(ge(b'0'), le(b'9'));
        let up = _mm_and_si128(ge(b'A'), le(b'Z'));
        let lo = _mm_and_si128(ge(b'a'), le(b'z'));
        let ok = _mm_or_si128(_mm_or_si128(dig, up), lo);
        let bad = _mm_andnot_si128(ok, live);
        if _mm_movemask_epi8(bad) != 0 {
            return None;
        }
        // code = v - off: digits 0x2F, upper 0x36, lower 0x3C
        let off = _mm_or_si128(
            _mm_or_si128(_mm_and_si128(dig, _mm_set1_epi8(0x2F)), _mm_and_si128(up, _mm_set1_epi8(0x36))),
            _mm_and_si128(lo, _mm_set1_epi8(0x3C)),
        );
        let code = _mm_and_si128(_mm_sub_epi8(v, off), live);
        // pairs: c0*64 + c1 (12 bits) in 16-bit lanes
        let pairs = _mm_maddubs_epi16(code, _mm_set1_epi16(0x0140)); // bytes (64, 1)
        // quads: p0*4096 + p1 (24 bits) in 32-bit lanes
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_1000)); // words (4096, 1)
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
        for i in 0..10 {
            let c = ((self >> (54 - 6 * i)) & 63) as u8;
            if c == 0 {
                break;
            }
            out.push(match c {
                1..=10 => c + 0x2F,
                11..=36 => c + 0x36,
                _ => c + 0x3C,
            });
        }
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
    #[inline(always)]
    pub fn prefetch(&self, k: K) {
        let s = k.h() as usize & self.mask;
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(self.e.as_ptr().add(s) as *const i8);
        }
    }
    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.e[..=self.mask].iter().filter(|x| !x.0.is_zero())
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
