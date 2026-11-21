use super::{direct, par, read, Chunks, ReadBuffers, Result, PARTITIONS};
use crate::proto_c::arena::{Bucket, LocalAlloc, Region};
use crate::proto_c::packed::{hash, unpermute, ID_BITS, LOW_HASH_BITS, PERSON_BITS, PERSON_MASK, PERSON_PART_BITS};
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

const INDEX_LIMIT: usize = 1 << (64 - PERSON_BITS);

#[inline(always)]
pub fn partition(h: u64) -> usize {
    (h >> (64 - PERSON_PART_BITS)) as usize
}

// The top 23 permuted bits select a slot. Its remaining 20 bits and the
// canonical 43-bit person fit in one word, with one flag for slot collisions.
const SLOT_BITS: u32 = 23;
const TAG_BITS: u32 = ID_BITS - SLOT_BITS;
const TAG_MASK: u64 = (1 << TAG_BITS) - 1;
const MORE: u64 = 1 << 63;
const WORDS_PER_PART: usize = (1 << SLOT_BITS) / 64 / PARTITIONS;

pub struct Links<'a> {
    present: &'a [u64],
    offsets: &'a [u32],
    entries: &'a [u64],
}

pub struct Probe {
    pub raw: u64,
    hash: u64,
    index: u32,
}

impl Links<'_> {
    #[inline(always)]
    pub fn prepare(&self, raw: u64) -> Probe {
        self.prepare_hash(raw, hash(raw))
    }

    #[inline(always)]
    fn prepare_hash(&self, raw: u64, hash: u64) -> Probe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

        let mut index = u32::MAX;
        if !self.present.is_empty() {
            let slot = (hash >> (64 - SLOT_BITS)) as usize;
            let word = slot >> 6;
            let bit = 1u64 << (slot & 63);
            let present = unsafe { *self.present.get_unchecked(word) };
            if present & bit != 0 {
                let offset = unsafe { *self.offsets.get_unchecked(word) };
                index = offset + (present & (bit - 1)).count_ones();
                unsafe {
                    _mm_prefetch::<_MM_HINT_T0>(self.entries.as_ptr().add(index as usize).cast());
                }
            }
        }
        Probe { raw, hash, index }
    }

    #[inline(always)]
    pub fn resolve(&self, probe: &Probe) -> u64 {
        if probe.index == u32::MAX {
            return probe.hash;
        }
        let tag = (probe.hash >> LOW_HASH_BITS) & TAG_MASK;
        let entry = unsafe { *self.entries.get_unchecked(probe.index as usize) };
        if entry & TAG_MASK == tag {
            return (entry >> TAG_BITS) << LOW_HASH_BITS;
        }
        if entry & MORE != 0 {
            let word = (probe.hash >> (64 - SLOT_BITS + 6)) as usize;
            return self.resolve_collision(probe.hash, word, tag);
        }
        probe.hash
    }

    #[cold]
    fn resolve_collision(&self, hash: u64, word: usize, tag: u64) -> u64 {
        let slot = (hash >> (64 - SLOT_BITS)) as usize & 63;
        let lo = self.offsets[word] as usize + self.present[word].count_ones() as usize;
        let hi = self.offsets[word + 1] as usize;
        // Collisions have the same 20-bit tag space as the first entries.
        // A separate slot number disambiguates collisions in the same word.
        for entry in self.entries[lo..hi].chunks_exact(2) {
            if entry[0] == slot as u64 && entry[1] & TAG_MASK == tag {
                return (entry[1] >> TAG_BITS) << LOW_HASH_BITS;
            }
        }
        hash
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
        return Ok(Links { present: &[], offsets: &[], entries: &[] });
    }
    if 2 * edges >= INDEX_LIMIT {
        return Err("link endpoints exceed the union-find index limit");
    }

    // The shard carries the high bits of the reversible hash. The remaining
    // bits and the edge position fit in one word, including on 60M datasets.
    // Each parse worker scatters its endpoints for parallel interning.
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
                    let h = hash(key);
                    let p = partition(h);
                    let suffix = (h >> LOW_HASH_BITS) & PERSON_MASK;
                    let pos = 2 * (edge_base[tid] + i) + side;
                    if buckets[p]
                        .push_local(alloc, suffix | ((pos as u64) << PERSON_BITS))
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
                let suffix = encoded & PERSON_MASK;
                let mut slot = suffix as usize & (cap - 1);
                loop {
                    let v = table[slot];
                    if v == 0 {
                        table[slot] = suffix | (((count + 1) as u64) << PERSON_BITS);
                        // Each shard writes to its own key range.
                        let key = unpermute(((p as u64) << PERSON_BITS) | suffix);
                        unsafe { key_write.write(base + count, key) };
                        count += 1;
                        break;
                    }
                    if v & PERSON_MASK == suffix {
                        break;
                    }
                    slot = (slot + 1) & (cap - 1);
                }
                // Each original endpoint occurs exactly once.
                unsafe {
                    ep_write.write(
                        (encoded >> PERSON_BITS) as usize,
                        (base + (table[slot] >> PERSON_BITS) as usize - 1) as u32,
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

    let parts = par::run(
        threads,
        PARTITIONS,
        |_| Vec::new(),
        |parts, p| {
            let mut by_slot = Vec::with_capacity(counts_by_part[p]);
            let start = offsets[p];
            let end = start + counts_by_part[p];
            for i in start..end {
                let root = find(parent, i as u32) as usize;
                if root == i {
                    continue;
                }
                let alias = hash(keys[i]) >> LOW_HASH_BITS;
                let canonical = hash(keys[root]) >> LOW_HASH_BITS;
                by_slot.push((alias, canonical));
            }
            by_slot.sort_unstable_by_key(|&(alias, _)| alias);
            let mut present = vec![0u64; WORDS_PER_PART];
            let mut offsets = vec![0u32; WORDS_PER_PART];
            let mut entries = Vec::with_capacity(by_slot.len());
            let mut pos = 0;
            let mut collided = Vec::new();
            for (word, (present, offset)) in present.iter_mut().zip(&mut offsets).enumerate() {
                *offset = entries.len() as u32;
                collided.clear();
                while let Some(&(alias, canonical)) = by_slot.get(pos) {
                    let within = (alias >> TAG_BITS) as usize & (64 * WORDS_PER_PART - 1);
                    if within >> 6 != word {
                        break;
                    }
                    let slot = within & 63;
                    let bit = 1u64 << slot;
                    let entry = (canonical << TAG_BITS) | (alias & TAG_MASK);
                    if *present & bit == 0 {
                        *present |= bit;
                        entries.push(entry);
                    } else {
                        *entries.last_mut().unwrap() |= MORE;
                        collided.extend_from_slice(&[slot as u64, entry]);
                    }
                    pos += 1;
                }
                entries.extend_from_slice(&collided);
            }
            parts.push((p, present, offsets, entries, by_slot.len()));
        },
    );
    let mut parts: Vec<_> = parts.into_iter().flatten().collect();
    parts.sort_unstable_by_key(|part| part.0);

    let words = WORDS_PER_PART * PARTITIONS;
    let entry_len = parts.iter().map(|part| part.3.len()).sum::<usize>();
    let present = persistent.slice(words, 0u64).ok_or("link bitmap arena exhausted")?;
    let offsets = persistent.slice(words + 1, 0u32).ok_or("link prefix arena exhausted")?;
    let entries = persistent.slice(entry_len, 0u64).ok_or("link entry arena exhausted")?;
    let present_write = par::SyncPtr(present.as_mut_ptr());
    let offsets_write = par::SyncPtr(offsets.as_mut_ptr());
    let entries_write = par::SyncPtr(entries.as_mut_ptr());
    let mut entry_offsets = [0; PARTITIONS + 1];
    for p in 0..PARTITIONS {
        entry_offsets[p + 1] = entry_offsets[p] + parts[p].3.len();
    }
    offsets[words] = entry_len as u32;
    par::run(
        threads,
        PARTITIONS,
        |_| (),
        |_, p| {
            let (_, bits, prefix, records, _) = &parts[p];
            let start = p * WORDS_PER_PART;
            let base = entry_offsets[p];
            // Each task fills the bitmap, prefix and entries for its own shard.
            unsafe {
                present_write.slice(start, WORDS_PER_PART).copy_from_slice(bits);
                for (i, &offset) in prefix.iter().enumerate() {
                    offsets_write.write(start + i, base as u32 + offset);
                }
                entries_write.slice(base, records.len()).copy_from_slice(records);
            }
        },
    );
    eprintln!(
        "[links] edges={edges} linked_ids={} mapped={} table_bytes={}",
        counts_by_part.iter().sum::<usize>(),
        parts.iter().map(|part| part.4).sum::<usize>(),
        present.len() * 8 + offsets.len() * 4 + entries.len() * 8,
    );
    timer.lap("links-uf");
    Ok(Links { present, offsets, entries })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_full_alias_and_retains_unlinked_ids() {
        let slot = 5u64;
        let home = slot << (64 - SLOT_BITS);
        let alias = |tag: u64| home | (tag << LOW_HASH_BITS);
        let links = Links {
            present: &[1 << 5, 0],
            offsets: &[0, 3, 3],
            entries: &[MORE | (17 << TAG_BITS) | 8, slot, (23 << TAG_BITS) | 9],
        };
        assert_eq!(links.resolve(&links.prepare_hash(0, alias(8))), 17 << LOW_HASH_BITS);
        assert_eq!(links.resolve(&links.prepare_hash(0, alias(9))), 23 << LOW_HASH_BITS);
        assert_eq!(links.resolve(&links.prepare_hash(0, alias(10))), alias(10));
        assert_eq!(links.resolve(&links.prepare_hash(0, 0)), 0);
    }
}
