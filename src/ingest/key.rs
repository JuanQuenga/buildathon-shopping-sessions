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

/// Exact id -> value lookup with a dense fast path for ids shaped
/// `<prefix><decimal without leading zero>` (e.g. `p123`, `c_45`); every other
/// id goes through an exact hash map (inline `u128` for <= 15 bytes, `HashMap`
/// for > 15 bytes). Shaped ids map 1:1 to their number, so the dense array is exact.
pub struct IdLookup {
    prefix: [u8; 4],
    plen: usize,
    dense: Vec<u32>,
    rest: Map,
    rest_len: usize,
    long_rest: std::collections::HashMap<Vec<u8>, u32>,
    default: u32,
}

const NONE: u32 = u32::MAX;

impl IdLookup {
    pub fn build<'a>(entries: impl Iterator<Item = (&'a [u8], u32)> + Clone, default: u32) -> IdLookup {
        let first = entries.clone().next().map(|e| e.0).unwrap_or(b"");
        let plen = first.iter().take(4).take_while(|b| !b.is_ascii_digit()).count();
        let mut prefix = [0u8; 4];
        prefix[..plen].copy_from_slice(&first[..plen]);
        let n_entries = entries.clone().count();
        let limit = (n_entries as u64) * 4 + 1024;
        let mut dense: Vec<u32> = Vec::new();
        let mut rest_entries = Vec::new();
        let mut long_rest = std::collections::HashMap::new();
        for (id, v) in entries {
            match shaped(id, &prefix[..plen]) {
                Some(n) if plen > 0 && n < limit => {
                    if dense.len() <= n as usize {
                        dense.resize(n as usize + 1, NONE);
                    }
                    if dense[n as usize] == NONE {
                        dense[n as usize] = v;
                    }
                }
                _ => {
                    if let Some(k) = pack(id) {
                        rest_entries.push((k, v));
                    } else {
                        long_rest.entry(id.to_vec()).or_insert(v);
                    }
                }
            }
        }
        let mut rest = Map::with_capacity(rest_entries.len());
        for (k, v) in &rest_entries {
            rest.insert(*k, *v);
        }
        IdLookup { prefix, plen, dense, rest, rest_len: rest_entries.len(), long_rest, default }
    }

    /// Lookup of field bytes at `p` with `len` bytes; 16+ readable bytes after `p` required.
    #[inline(always)]
    pub unsafe fn get(&self, p: *const u8, len: usize) -> u32 {
        let pl = self.plen;
        if len > pl && len <= pl + 8 && pl > 0 {
            let s = std::slice::from_raw_parts(p, pl);
            if s == &self.prefix[..pl] && *p.add(pl) != b'0' {
                let nd = len - pl;
                if let Some(n) = super::tsfast::digits8(p.add(pl), nd) {
                    if (n as usize) < self.dense.len() {
                        let v = *self.dense.get_unchecked(n as usize);
                        if v != NONE {
                            return v;
                        }
                    }
                    // shaped (<= 8 digits) but absent from dense: only `rest` could hold it if it was >= limit
                    if self.rest_len == 0 && self.long_rest.is_empty() {
                        return self.default;
                    }
                }
            }
        }
        if len <= MAX_INLINE {
            if self.rest_len == 0 {
                return self.default;
            }
            return self.rest.get(pack_at(p, len)).unwrap_or(self.default);
        }
        if self.long_rest.is_empty() {
            return self.default;
        }
        self.long_rest.get(std::slice::from_raw_parts(p, len)).copied().unwrap_or(self.default)
    }
}

/// `<prefix><digits>` with no leading zero, at most 8 digits -> number.
fn shaped(id: &[u8], prefix: &[u8]) -> Option<u64> {
    let d = id.strip_prefix(prefix)?;
    if d.is_empty() || d.len() > 8 || d[0] == b'0' || !d.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(d.iter().fold(0u64, |a, &b| a * 10 + (b - b'0') as u64))
}

#[cfg(test)]
mod lookup_tests {
    use super::*;
    #[test]
    fn lookup_exact() {
        let long_id = b"p_very_long_product_identifier_12345".to_vec();
        let ids: Vec<(Vec<u8>, u32)> = vec![
            (b"p1".to_vec(), 1), (b"p20".to_vec(), 2), (b"p0".to_vec(), 3), (b"p007".to_vec(), 4),
            (b"x9".to_vec(), 5), (b"p99999999".to_vec(), 6), (b"P1".to_vec(), 7), (b"p".to_vec(), 8),
            (long_id.clone(), 9),
        ];
        let l = IdLookup::build(ids.iter().map(|(a, b)| (a.as_slice(), *b)), 0);
        let q = |s: &[u8]| {
            let mut v = s.to_vec();
            v.resize(s.len() + 32, b'9');
            unsafe { l.get(v.as_ptr(), s.len()) }
        };
        for (id, v) in &ids {
            assert_eq!(q(id), *v, "{:?}", String::from_utf8_lossy(id));
        }
        for miss in [
            &b"p2"[..], b"p01", b"p00", b"p200", b"p1 ", b" p1", b"", b"q1", b"p123456789",
            b"p9999999", b"p_very_long_product_identifier_12346",
        ] {
            assert_eq!(q(miss), 0, "{:?}", String::from_utf8_lossy(miss));
        }
        let only_shaped: Vec<(Vec<u8>, u32)> = (1..100u32).map(|i| (format!("c_{i}").into_bytes(), i)).collect();
        let l = IdLookup::build(only_shaped.iter().map(|(a, b)| (a.as_slice(), *b)), 0);
        let q = |s: &[u8]| {
            let mut v = s.to_vec();
            v.resize(s.len() + 32, b'9');
            unsafe { l.get(v.as_ptr(), s.len()) }
        };
        assert_eq!(q(b"c_5"), 5);
        assert_eq!(q(b"c_500"), 0);
        assert_eq!(q(b"C_5"), 0);
        assert_eq!(q(b"c_05"), 0);
        assert_eq!(q(b"c_"), 0);
    }
}
