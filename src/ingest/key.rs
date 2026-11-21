//! Exact inline keys for ids of at most 15 bytes: bytes big-endian in the high
//! 15 bytes, length in the low byte. Equality of keys == equality of bytes, and
//! u128 order == byte-wise order (shorter prefix sorts first, NULs handled by len).

pub const MAX_INLINE: usize = 15;

#[inline(always)]
pub fn pack(b: &[u8]) -> Option<u128> {
    if b.len() > MAX_INLINE {
        return None;
    }
    let mut a = [0u8; 16];
    a[..b.len()].copy_from_slice(b);
    a[15] = b.len() as u8;
    Some(u128::from_be_bytes(a))
}

/// Pack `len <= 15` bytes at `p`; caller guarantees 16 readable bytes at `p`.
#[inline(always)]
pub unsafe fn pack_at(p: *const u8, len: usize) -> u128 {
    debug_assert!(len <= MAX_INLINE);
    let raw = u128::from_be_bytes(std::ptr::read_unaligned(p as *const [u8; 16]));
    let mask = !(u128::MAX >> (8 * len as u32));
    (raw & mask) | len as u128
}

#[inline]
pub fn unpack(k: u128, out: &mut Vec<u8>) {
    let len = (k & 0xff) as usize;
    out.extend_from_slice(&k.to_be_bytes()[..len]);
}

#[inline(always)]
pub fn hash(k: u128) -> u64 {
    let lo = k as u64;
    let hi = (k >> 64) as u64;
    let x = (lo ^ hi.rotate_left(29).wrapping_mul(0x9E37_79B9_7F4A_7C15)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    x ^ (x >> 31)
}

/// Read-only open-addressing map u128 -> u32 (exact keys). Key 0 (empty id) unused.
pub struct Map {
    keys: Vec<u128>,
    vals: Vec<u32>,
    mask: usize,
}

impl Map {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        Map { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    /// Insert if absent; returns existing or inserted value.
    #[inline]
    pub fn insert(&mut self, k: u128, v: u32) -> u32 {
        debug_assert!(k != 0);
        let mut s = hash(k) as usize & self.mask;
        loop {
            let kk = self.keys[s];
            if kk == k {
                return self.vals[s];
            }
            if kk == 0 {
                self.keys[s] = k;
                self.vals[s] = v;
                return v;
            }
            s = (s + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn get(&self, k: u128) -> Option<u32> {
        let mut s = hash(k) as usize & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(s) };
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(s) });
            }
            if kk == 0 {
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
    fn order() {
        let ids: Vec<&[u8]> = vec![b"", b"\0", b"a", b"a\0", b"a\0\0", b"ab", b"b", b"U1", b"u1", b" u1", b"u1 ", b"zzzzzzzzzzzzzzz"];
        for a in &ids {
            for b in &ids {
                let (ka, kb) = (pack(a).unwrap(), pack(b).unwrap());
                assert_eq!(a.cmp(b), ka.cmp(&kb), "{a:?} {b:?}");
                let buf = [a.to_vec(), vec![7u8; 16]].concat();
                assert_eq!(unsafe { pack_at(buf.as_ptr(), a.len()) }, ka);
                let mut out = Vec::new();
                unpack(ka, &mut out);
                assert_eq!(&out[..], *a);
            }
        }
        assert!(pack(&[1u8; 16]).is_none());
    }
}
