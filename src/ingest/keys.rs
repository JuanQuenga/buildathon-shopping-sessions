//! Exact fixed-width id keys, generic over width.
//!
//! * `u128` ("wide"): any id of at most 15 bytes (bytes big-endian + length byte).
//! * `u64` ("compact"): ids of 1..=9 non-NUL ASCII bytes, 7 bits per byte,
//!   first byte in the high byte, code 0 = padding. Order-preserving and injective.
//!
//! In both, key equality == byte equality and key order == byte-wise order. The
//! zero key is used as the empty-slot marker and never stored.
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

const ASCII_BITS: u64 = 0x7F7F_7F7F_7F7F_7F7F;
const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const TAIL_MASKS: [u64; 10] = [
    0, 0, 0xFF, 0xFFFF, 0xFF_FFFF, 0xFFFF_FFFF, 0xFF_FFFF_FFFF,
    0xFFFF_FFFF_FFFF, 0xFF_FFFF_FFFF_FFFF, u64::MAX,
];

impl Key for u64 {
    #[inline(always)]
    unsafe fn enc(p: *const u8, len: usize) -> Option<u64> {
        if !(1..=9).contains(&len) {
            return None;
        }
        let first = *p;
        let live = *TAIL_MASKS.get_unchecked(len);
        let tail = p.add(1).cast::<u64>().read_unaligned() & live;
        // Fill inactive lanes before testing for NUL in the active ones.
        let filled = tail | !live;
        let zero = filled.wrapping_sub(0x0101_0101_0101_0101) & !filled;
        if (first.wrapping_sub(1) >= 127) | (((tail | zero) & HIGH_BITS) != 0) {
            return None;
        }
        let packed = std::arch::x86_64::_pext_u64(tail.swap_bytes(), ASCII_BITS);
        Some((u64::from(first) << 56) | packed)
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
        dst[0] = (self >> 56) as u8;
        let tail = unsafe { std::arch::x86_64::_pdep_u64(self, ASCII_BITS) };
        unsafe { dst.as_mut_ptr().add(1).cast::<u64>().write_unaligned(tail.swap_bytes()) };
        usize::from(dst[0] != 0) + (8 - (tail.trailing_zeros() as usize >> 3))
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
        // Exhaust every byte at every position for all supported lengths.
        // Equal prefixes, byte boundaries, and lexicographic ordering are all
        // covered by comparing adjacent entries in the sorted byte strings.
        let mut ids = Vec::new();
        for len in 1..=9 {
            for pos in 0..len {
                for byte in 0..=u8::MAX {
                    let mut id = vec![b'a'; len];
                    id[pos] = byte;
                    let key = u64::enc_slice(&id);
                    if (1..=127).contains(&byte) {
                        let key = key.unwrap();
                        assert_ne!(key, 0);
                        let mut buf = [0; 16];
                        let n = key.unpack_buf(&mut buf);
                        assert_eq!(&buf[..n], id);
                        ids.push((id, key));
                    } else {
                        assert_eq!(key, None, "{id:?}");
                        assert!(u128::enc_slice(&id).is_some());
                    }
                }
            }
        }
        ids.sort_unstable();
        ids.dedup();
        assert!(ids.windows(2).all(|p| p[0].0 < p[1].0 && p[0].1 < p[1].1));
        assert_eq!(unsafe { u64::enc([1; 64].as_ptr(), 0) }, None);
        for len in 10..=15 {
            let id = vec![b'a'; len];
            assert_eq!(u64::enc_slice(&id), None);
            assert!(u128::enc_slice(&id).is_some());
        }
    }
}
