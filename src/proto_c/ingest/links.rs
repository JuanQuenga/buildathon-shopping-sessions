use super::{direct, par, read, Chunks, ReadBuffers, Result, PARTITIONS};
use crate::proto_c::arena::{Bucket, LocalAlloc, Region};
use crate::proto_c::packed::{self, hash, ID_BITS, ID_MASK, LOW_HASH_BITS, PERSON_BITS, PERSON_MASK};
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

const INDEX_LIMIT: usize = 1 << (64 - ID_BITS);

#[inline(always)]
pub fn partition(h: u64) -> usize {
    (h >> 56) as usize
}

#[derive(Clone, Copy)]
struct Shard {
    start: usize,
    len: usize,
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct RelocBucket {
    keys: [u64; 4],
    canon: [u64; 4],
}

/// L2-hot per-worker 4-wide AVX2 hash map built once per raw shard `q`.
/// Keys store `perm35 + 1` so `0` unambiguously marks an empty lane with no mask.
#[derive(Default)]
pub struct RelocMap {
    buckets: Vec<RelocBucket>,
    mask: usize,
}

impl RelocMap {
    pub fn reset(&mut self, pairs: &[(u64, u64)]) {
        let cap = pairs.len().div_ceil(2).next_power_of_two().max(1);
        if self.buckets.len() < cap {
            self.buckets.resize(cap, RelocBucket::default());
        }
        let buckets = &mut self.buckets[..cap];
        buckets.fill(RelocBucket::default());
        let mask = cap - 1;
        self.mask = mask;
        for &(perm35, canon43) in pairs {
            let k = perm35 + 1;
            let mut slot = perm35 as usize & mask;
            'ins: loop {
                let b = unsafe { buckets.get_unchecked_mut(slot) };
                for lane in 0..4 {
                    if b.keys[lane] == 0 {
                        b.keys[lane] = k;
                        b.canon[lane] = canon43;
                        break 'ins;
                    }
                }
                slot = (slot + 1) & mask;
            }
        }
    }

    #[inline(always)]
    pub fn prefetch(&self, perm35: u64) -> usize {
        let slot = perm35 as usize & self.mask;
        unsafe {
            _mm_prefetch::<_MM_HINT_T0>(self.buckets.as_ptr().add(slot).cast());
        }
        slot
    }

    #[inline(always)]
    pub fn resolve_slot(&self, perm35: u64, mut slot: usize) -> Option<(usize, u64)> {
        use std::arch::x86_64::*;
        let needle = unsafe { _mm256_set1_epi64x((perm35 + 1) as i64) };
        let zero = unsafe { _mm256_setzero_si256() };
        let mask = self.mask;
        unsafe {
            loop {
                let b = self.buckets.get_unchecked(slot);
                let keys = _mm256_load_si256(b.keys.as_ptr().cast());
                let hits = _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(keys, needle)))
                    as u32;
                if hits != 0 {
                    let c = *b.canon.get_unchecked(hits.trailing_zeros() as usize);
                    return Some(((c >> PERSON_BITS) as usize, c & PERSON_MASK));
                }
                if _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(keys, zero))) != 0 {
                    return None;
                }
                slot = (slot + 1) & mask;
            }
        }
    }
}

pub struct Links<'a> {
    pairs: &'a [(u64, u64)],
    by_raw: [Shard; PARTITIONS],
}

impl<'a> Links<'a> {
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    #[inline(always)]
    pub fn shard(&self, q: usize) -> &[(u64, u64)] {
        let s = unsafe { *self.by_raw.get_unchecked(q) };
        unsafe { self.pairs.get_unchecked(s.start..s.start + s.len) }
    }
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
    buffers: &ReadBuffers,
    persistent: &'a Region<'_>,
    scratch: &Region<'_>,
    threads: usize,
    timer: &mut par::Timer,
) -> Result<Links<'a>> {
    let pairs = par::run(
        threads,
        chunks.links.len(),
        |tid| (tid, Bucket::<[u64; 2]>::default(), LocalAlloc::new(scratch, 256 << 10, false), Ok(())),
        |(tid, pairs, alloc, error), i| {
            if error.is_err() {
                return;
            }
            let (buf, lo, hi) = match unsafe { read(chunks, chunks.links[i], buffers, *tid) } {
                Ok(chunk) => chunk,
                Err(err) => {
                    *error = Err(err);
                    return;
                }
            };
            if let Err(err) = direct::links(buf, lo, hi, |a, b| {
                pairs
                    .push_local(alloc, [a, b])
                    .map(|_| ())
                    .ok_or("link pair arena exhausted")
            }) {
                *error = Err(err);
            }
        },
    );
    let pairs = pairs
        .into_iter()
        .map(|(_, p, _, err)| err.map(|()| p))
        .collect::<Result<Vec<_>>>()?;
    timer.lap("links-parse");
    let edges = pairs.iter().map(|p| p.len()).sum::<usize>();
    if edges == 0 {
        timer.lap("links-uf");
        return Ok(Links {
            pairs: &[],
            by_raw: [Shard { start: 0, len: 0 }; PARTITIONS],
        });
    }
    if 2 * edges > INDEX_LIMIT {
        return Err("link endpoints exceed the 21-bit union-find limit");
    }

    // Pack each endpoint into an eight-byte (key, original edge endpoint) word.
    // Each parse worker scatters its endpoints by key hash for parallel interning.
    let mut edge_base = Vec::with_capacity(pairs.len());
    let mut prefix = 0;
    for p in &pairs {
        edge_base.push(prefix);
        prefix += p.len();
    }
    let endpoints = par::run(
        threads,
        pairs.len(),
        |_| (LocalAlloc::new(scratch, 256 << 10, false), Vec::new()),
        |(alloc, out), tid| {
            let pairs = &pairs[tid];
            let mut buckets: [Bucket<'_, u64>; PARTITIONS] =
                std::array::from_fn(|_| Bucket::default());
            for (i, &pair) in pairs.rows().enumerate() {
                for (side, key) in pair.into_iter().enumerate() {
                    let p = partition(hash(key));
                    let pos = 2 * (edge_base[tid] + i) + side;
                    if buckets[p]
                        .push_local(alloc, key | ((pos as u64) << ID_BITS))
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
        .flat_map(|(_, out)| out)
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
                let mut slot = (hash(key) >> LOW_HASH_BITS) as usize & (cap - 1);
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
    let mut by_raw = [Shard { start: 0, len: 0 }; PARTITIONS];
    let mut mapped_count = 0;
    for (p, _roots, mapped) in root_counts.into_iter().flatten() {
        by_raw[p].len = mapped;
        mapped_count += mapped;
    }

    let mut pairs_len = 0;
    for p in 0..PARTITIONS {
        by_raw[p].start = pairs_len;
        pairs_len += by_raw[p].len;
    }

    let out_pairs = persistent
        .uninit::<(u64, u64)>(pairs_len)
        .ok_or("compact link pairs arena exhausted")?;
    let pairs_write = par::SyncPtr(out_pairs.as_mut_ptr().cast::<(u64, u64)>());
    par::run(
        threads,
        PARTITIONS,
        |_| (),
        |_, p| {
            let shard = by_raw[p];
            if shard.len == 0 {
                return;
            }
            let start = offsets[p];
            let end = start + counts_by_part[p];
            let dst = unsafe { pairs_write.slice(shard.start, shard.len) };
            let mut w = 0usize;
            for i in start..end {
                let root = parent[i].load(Relaxed) as usize;
                if root == i {
                    continue;
                }
                let perm35 = packed::record_person(keys[i], hash(keys[i]));
                let canon43 = hash(keys[root]) >> LOW_HASH_BITS;
                dst[w] = (perm35, canon43);
                w += 1;
            }
            debug_assert_eq!(w, shard.len);
        },
    );
    let out_pairs = unsafe { std::slice::from_raw_parts(out_pairs.as_ptr().cast::<(u64, u64)>(), pairs_len) };

    eprintln!(
        "[links] edges={edges} linked_ids={} mapped={mapped_count} pair_bytes={}",
        counts_by_part.iter().sum::<usize>(), out_pairs.len() * 16
    );
    timer.lap("links-uf");
    Ok(Links {
        pairs: out_pairs,
        by_raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_links_and_reloc_map() {
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

        let hb = hash(b);
        let ha = hash(a);
        let hd = hash(d);
        let pb = packed::record_person(b, hb);
        let ca = ha >> LOW_HASH_BITS;
        let mut map = RelocMap::default();
        map.reset(&[(pb, ca)]);
        let s1 = map.prefetch(pb);
        assert_eq!(
            map.resolve_slot(pb, s1),
            Some(((ca >> PERSON_BITS) as usize, ca & PERSON_MASK))
        );
        let pd = packed::record_person(d, hd);
        let s2 = map.prefetch(pd);
        assert_eq!(map.resolve_slot(pd, s2), None);
    }
}
