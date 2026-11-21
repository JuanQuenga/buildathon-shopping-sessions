use super::{par, read, scan, Chunks, ReadBuffers, Result, PARTITIONS};
use crate::proto_c::arena::{Bucket, Region};
use crate::proto_c::packed::{self, hash, ID_BITS, ID_MASK};
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

const INDEX_LIMIT: usize = 1 << (64 - ID_BITS);
const BLOOM_WORDS: usize = (1536 << 10) / 8;
const MASKS: [u64; 1024] = bloom_masks();

const fn bloom_masks() -> [u64; 1024] {
    let mut masks = [0; 1024];
    let mut i = 0;
    let mut x = 41u64;
    while i < masks.len() {
        x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let z = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        masks[i] = (1 << (z & 15))
            | (1 << (16 + ((z >> 8) & 15)))
            | (1 << (32 + ((z >> 16) & 15)))
            | (1 << (48 + ((z >> 24) & 15)));
        i += 1;
    }
    masks
}

#[inline(always)]
pub fn partition(h: u64) -> usize {
    (h >> 56) as usize
}

#[derive(Clone, Copy)]
struct Shard {
    start: usize,
    cap: usize,
}

impl Shard {
    #[inline(always)]
    fn slot(self, h: u64) -> usize {
        let start = ((h as u32 as u64 * self.cap as u64) >> 32) as usize;
        self.start + start
    }

    #[inline(always)]
    fn next(self, index: usize) -> usize {
        if index + 1 == self.start + self.cap {
            self.start
        } else {
            index + 1
        }
    }
}

pub struct Links<'a> {
    table: &'a [u64],
    by_raw: [Shard; PARTITIONS],
    canonical: &'a [u64],
    bloom: &'a [u64],
}

impl Links<'_> {
    #[inline(always)]
    pub fn possible(&self, h: u64) -> bool {
        if self.bloom.is_empty() {
            return false;
        }
        let (i, bits) = bloom_bits(h, self.bloom.len());
        unsafe { *self.bloom.get_unchecked(i) & bits == bits }
    }

    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        let shard = self.by_raw[partition(h)];
        if shard.cap != 0 {
            unsafe { _mm_prefetch::<_MM_HINT_T0>(self.table.as_ptr().add(shard.slot(h)).cast()) };
        }
    }

    #[inline]
    pub fn resolve(&self, raw: u64, h: u64) -> u64 {
        let shard = self.by_raw[partition(h)];
        if shard.cap == 0 {
            return raw;
        }
        let mut i = shard.slot(h);
        loop {
            let slot = unsafe { *self.table.get_unchecked(i) };
            if slot == 0 {
                return raw;
            }
            if slot & ID_MASK == raw {
                return unsafe { *self.canonical.get_unchecked((slot >> ID_BITS) as usize) };
            }
            i = shard.next(i);
        }
    }
}

#[inline(always)]
fn bloom_bits(h: u64, len: usize) -> (usize, u64) {
    let index = ((h as u32 as u64 * len as u64) >> 32) as usize;
    let bits = MASKS[(h >> 54) as usize];
    (index, bits)
}

fn find(parent: &[AtomicU32], mut i: u32) -> u32 {
    loop {
        let p = parent[i as usize].load(Relaxed);
        if p == i {
            return p;
        }
        let gp = parent[p as usize].load(Relaxed);
        if gp == p {
            return p;
        }
        let _ = parent[i as usize].compare_exchange_weak(p, gp, Relaxed, Relaxed);
        i = gp;
    }
}

fn union(parent: &[AtomicU32], keys: &[u64], mut a: u32, mut b: u32) {
    loop {
        a = find(parent, a);
        b = find(parent, b);
        if a == b {
            return;
        }
        // Every parent points toward a strictly smaller key. Once the
        // unions are done, each root is the canonical key itself.
        if keys[a as usize] > keys[b as usize] {
            std::mem::swap(&mut a, &mut b);
        }
        if parent[b as usize]
            .compare_exchange_weak(b, a, Relaxed, Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

pub fn build<'a>(
    chunks: &Chunks,
    buffers: &ReadBuffers<'_>,
    persistent: &'a Region<'_>,
    scratch: &Region<'_>,
    threads: usize,
    timer: &mut par::Timer,
) -> Result<Links<'a>> {
    let pairs = par::run(
        threads,
        chunks.links.len(),
        |tid| (tid, Bucket::<[u64; 2]>::default(), Ok(())),
        |(tid, pairs, error), i| {
            if error.is_err() {
                return;
            }
            // Each pool worker is alone in its own buffer.
            let (buf, lo, hi) = match unsafe { read(chunks, chunks.links[i], buffers, *tid) } {
                Ok(chunk) => chunk,
                Err(err) => {
                    *error = Err(err);
                    return;
                }
            };
            let mut start = lo;
            while start < hi {
                let lf = start + scan::memchr(b'\n', &buf[start..hi]).unwrap();
                if let Some([a, b]) = scan::row_fields::<2>(buf, start, lf) {
                    let a = &buf[a.0 as usize..a.1 as usize];
                    let b = &buf[b.0 as usize..b.1 as usize];
                    if !a.is_empty() && !b.is_empty() {
                        match (packed::encode(a, b'u'), packed::encode(b, b'u')) {
                            (Some(a), Some(b)) if a == b => {}
                            (Some(a), Some(b)) => {
                                if pairs.push(scratch, [a, b]).is_none() {
                                    *error = Err("link pair arena exhausted");
                                    return;
                                }
                            }
                            _ => {
                                *error = Err("link user ID exceeds the 43-bit alphabet");
                                return;
                            }
                        }
                    }
                }
                start = lf + 1;
            }
        },
    );
    for (_, _, error) in &pairs {
        (*error)?;
    }
    timer.lap("links-parse");
    let edges = pairs.iter().map(|(_, p, _)| p.len()).sum::<usize>();
    if edges == 0 {
        timer.lap("links-uf");
        return Ok(Links {
            table: &[],
            by_raw: [Shard { start: 0, cap: 0 }; PARTITIONS],
            canonical: &[],
            bloom: &[],
        });
    }
    if 2 * edges > INDEX_LIMIT {
        return Err("link endpoints exceed the 21-bit union-find limit");
    }

    // Pack each endpoint into an eight-byte (key, original edge endpoint) word.
    // Each parse worker scatters its endpoints by key hash for parallel interning.
    let mut edge_base = Vec::with_capacity(pairs.len());
    let mut prefix = 0;
    for (_, pairs, _) in &pairs {
        edge_base.push(prefix);
        prefix += pairs.len();
    }
    let endpoints = par::run(
        threads,
        pairs.len(),
        |_| Vec::new(),
        |out, tid| {
            let (_, pairs, _) = &pairs[tid];
            let mut buckets: [Bucket<'_, u64>; PARTITIONS] =
                std::array::from_fn(|_| Bucket::default());
            for (i, &pair) in pairs.rows().enumerate() {
                for (side, key) in pair.into_iter().enumerate() {
                    let p = partition(hash(key));
                    let pos = 2 * (edge_base[tid] + i) + side;
                    if buckets[p]
                        .push(scratch, key | ((pos as u64) << ID_BITS))
                        .is_none()
                    {
                        out.push(Err("link endpoint arena exhausted"));
                        return;
                    }
                }
            }
            out.push(Ok(buckets));
        },
    );
    let endpoints = endpoints
        .into_iter()
        .flatten()
        .collect::<Result<Vec<_>>>()?;
    drop(pairs);

    let mut offsets = [0usize; PARTITIONS + 1];
    for p in 0..PARTITIONS {
        offsets[p + 1] = offsets[p] + endpoints.iter().map(|b| b[p].len()).sum::<usize>();
    }
    let ep_nodes = scratch
        .slice(2 * edges, 0u32)
        .ok_or("link node arena exhausted")?;
    let keys = scratch
        .slice(2 * edges, 0u64)
        .ok_or("link key arena exhausted")?;
    let ep_write = par::SyncPtr(ep_nodes.as_mut_ptr());
    let key_write = par::SyncPtr(keys.as_mut_ptr());
    let counts = par::run(
        threads,
        PARTITIONS,
        |_| (Vec::<u64>::new(), Vec::new()),
        |(table, counts), p| {
            let base = offsets[p];
            let n = offsets[p + 1] - base;
            let cap = (2 * n).next_power_of_two();
            table.resize(cap, 0);
            table.fill(0);
            let mut count = 0;
            for encoded in endpoints.iter().flat_map(|b| b[p].rows()) {
                let key = encoded & ID_MASK;
                let mut slot = hash(key) as usize & (cap - 1);
                loop {
                    let v = table[slot];
                    if v == 0 {
                        table[slot] = key | ((count as u64) << ID_BITS);
                        // Each shard writes to its own key range.
                        unsafe { key_write.write(base + count, key) };
                        count += 1;
                        break;
                    }
                    if v & ID_MASK == key {
                        break;
                    }
                    slot = (slot + 1) & (cap - 1);
                }
                // Each original endpoint occurs exactly once.
                unsafe {
                    ep_write.write(
                        (encoded >> ID_BITS) as usize,
                        (base + (table[slot] >> ID_BITS) as usize) as u32,
                    );
                }
            }
            counts.push((p, count));
        },
    );
    let mut counts_by_part = [0; PARTITIONS];
    for (p, count) in counts.into_iter().flat_map(|(_, counts)| counts) {
        counts_by_part[p] = count;
    }

    let parent = scratch
        .init(2 * edges, |i| AtomicU32::new(i as u32))
        .ok_or("union-find arena exhausted")?;
    const STEP: usize = 16 << 10;
    par::run(
        threads,
        edges.div_ceil(STEP),
        |_| (),
        |_, task| {
            let start = task * STEP;
            let end = (start + STEP).min(edges);
            for edge in ep_nodes[2 * start..2 * end].chunks_exact(2) {
                if edge[0] != edge[1] {
                    union(parent, keys, edge[0], edge[1]);
                }
            }
        },
    );

    let root_counts = par::run(
        threads,
        PARTITIONS,
        |_| Vec::new(),
        |out, p| {
            let start = offsets[p];
            let end = start + counts_by_part[p];
            let mut roots = 0;
            for i in start..end {
                let root = find(parent, i as u32);
                parent[i].store(root, Relaxed);
                roots += usize::from(root as usize == i);
            }
            out.push((p, roots, end - start - roots));
        },
    );
    let mut roots_by_part = [0; PARTITIONS];
    let mut by_raw = [Shard { start: 0, cap: 0 }; PARTITIONS];
    let mut mapped_count = 0;
    for (p, roots, mapped) in root_counts.into_iter().flatten() {
        roots_by_part[p] = roots;
        by_raw[p].cap = (mapped * 5).div_ceil(3).next_multiple_of(8);
        mapped_count += mapped;
    }

    let mut table_len = 0;
    let mut canonical_len = 0;
    let mut root_start = [0; PARTITIONS];
    for p in 0..PARTITIONS {
        by_raw[p].start = table_len;
        table_len += by_raw[p].cap;
        root_start[p] = canonical_len;
        canonical_len += roots_by_part[p];
    }

    let table = persistent
        .slice(table_len, 0u64)
        .ok_or("compact link table arena exhausted")?;
    let canonical = persistent
        .slice(canonical_len, 0u64)
        .ok_or("canonical link ID arena exhausted")?;
    let dict_index = scratch
        .slice(2 * edges, 0u32)
        .ok_or("canonical index arena exhausted")?;
    let canonical_write = par::SyncPtr(canonical.as_mut_ptr());
    let index_write = par::SyncPtr(dict_index.as_mut_ptr());
    par::run(
        threads,
        PARTITIONS,
        |_| (),
        |_, p| {
            let start = offsets[p];
            let end = start + counts_by_part[p];
            let mut idx = root_start[p];
            for i in start..end {
                if parent[i].load(Relaxed) as usize == i {
                    // Each shard owns its source and dictionary ranges.
                    unsafe {
                        canonical_write.write(idx, keys[i]);
                        index_write.write(i, idx as u32);
                    }
                    idx += 1;
                }
            }
        },
    );
    let table_write = par::SyncPtr(table.as_mut_ptr());
    par::run(
        threads,
        PARTITIONS,
        |_| (),
        |_, p| {
            let shard = by_raw[p];
            let start = offsets[p];
            let end = start + counts_by_part[p];
            if shard.cap == 0 {
                return;
            }
            // Tables have disjoint write ranges by raw-key shard.
            let slots = unsafe { table_write.slice(shard.start, shard.cap) };
            for i in start..end {
                let root = parent[i].load(Relaxed) as usize;
                if root == i {
                    continue;
                }
                let h = hash(keys[i]);
                let mut slot = shard.slot(h);
                while slots[slot - shard.start] != 0 {
                    slot = shard.next(slot);
                }
                slots[slot - shard.start] = keys[i] | (u64::from(dict_index[root]) << ID_BITS);
            }
        },
    );

    let bloom_len = if mapped_count == 0 { 0 } else { BLOOM_WORDS };
    let bloom = persistent
        .slice(bloom_len, 0u64)
        .ok_or("blocked Bloom filter arena exhausted")?;
    for &slot in table.iter() {
        if slot == 0 {
            continue;
        }
        let (i, bits) = bloom_bits(hash(slot & ID_MASK), bloom_len);
        bloom[i] |= bits;
    }
    eprintln!(
        "[links] edges={edges} linked_ids={} mapped={mapped_count} table_bytes={} canonical_bytes={} bloom_bytes={}",
        counts_by_part.iter().sum::<usize>(), table.len() * 8, canonical.len() * 8, bloom.len() * 8
    );
    timer.lap("links-uf");
    Ok(Links {
        table,
        by_raw,
        canonical,
        bloom,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_links_and_bloom() {
        let a = packed::encode(b"U1", b'u').unwrap();
        let b = packed::encode(b"u1", b'u').unwrap();
        let c = packed::encode(b"u00", b'u').unwrap();
        let d = packed::encode(b"u1a", b'u').unwrap();
        let parent = std::array::from_fn::<_, 3, _>(|i| AtomicU32::new(i as u32));
        let keys = [b, c, a];
        union(&parent, &keys, 0, 1);
        union(&parent, &keys, 2, 1);
        assert_eq!(find(&parent, 0), 2);
        assert_eq!(find(&parent, 1), 2);

        let mut map = [0; 16];
        let mut shards = [Shard { start: 0, cap: 0 }; PARTITIONS];
        let h = hash(b);
        let shard = Shard {
            start: 0,
            cap: map.len(),
        };
        shards[partition(h)] = shard;
        map[shard.slot(h)] = b | (1 << ID_BITS);
        let mut bloom = [0; 1024];
        let (i, bits) = bloom_bits(h, bloom.len());
        bloom[i] = bits;
        let links = Links {
            table: &map,
            by_raw: shards,
            canonical: &[c, a],
            bloom: &bloom,
        };
        assert!(links.possible(h));
        assert_eq!(links.resolve(b, h), a);
        assert_eq!(links.resolve(d, hash(d)), d);

        let mut seen = Vec::new();
        let mut seed = 12;
        for _ in 0..8192 {
            seed = hash(seed);
            let (block, bits) = bloom_bits(seed, bloom.len());
            bloom[block] |= bits;
            seen.push(seed);
        }
        let links = Links {
            table: &map,
            by_raw: shards,
            canonical: &[c, a],
            bloom: &bloom,
        };
        assert!(seen.into_iter().all(|h| links.possible(h)));
    }
}
