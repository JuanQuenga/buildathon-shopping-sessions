//! Parallel, memory-bounded exact ingest + fused aggregation/Parquet output for arbitrary inputs.
//!
//! Handles inputs outside `fast`'s compact representation without ever dropping to single-threaded
//! `slow::load`:
//!   - arbitrary-length `user_id`, `event_id`, `click_id` (0..∞ bytes, incl. 24-40B+ and multi-MB IDs)
//!   - arbitrary category counts (`u32`, incl. 5,000+ categories)
//!   - arbitrary window spans and timestamp ranges (`i64` ms, incl. 90d+ and pre-1970)
//!   - arbitrary `ingest_ts` (`i64` ms; unparseable or overflow -> `i64::MAX`)
//!
//! Memory lifecycle (keeps peak RSS < 8 GB even on 60M events with all 24-40B IDs):
//!   1. `links`     : parallel chunk parse -> 256-part intern -> parallel lock-free union-find ->
//!                    compact `GenericLinks` (only `id != canonical`).
//!   2. `ev-parse`  : parallel chunk parse -> `P1` mmap-backed `SlabBuf` streams per thread.
//!   3. `ev-dedup`  : per `P1` partition, `u32`/`u64` open-addressing dedup + `dead` bitset +
//!                    ring-32 prefetched canonical resolution -> `P2` `SlabBuf` `b_parts`;
//!                    each `P1` partition's `ev_parts` slabs are `munmap`ped immediately on completion.
//!   4. `ck-parse`  : runs ONLY AFTER `ev_parts` is 100% freed; parallel chunk parse -> ring-32
//!                    prefetched canonical resolution -> `P2` `SlabBuf` `ck_parts`; then drops `links`.
//!   5. `group+agg` : per `P2` partition (largest first), consumes and immediately `munmap`s
//!                    `b_parts[q]` and `ck_parts[q]`, sorts events/clicks, sessionizes/attributes/
//!                    computes interval-union assists in `i64` ms, streams directly to `UserWriter`
//!                    and merges `CampStore`s into `campaign_daily`.
use super::par::{self, SlabBuf, SyncPtr, Timer};
use super::scan::{self, Chunk, FileInfo, Kind};
use super::tsfast::{self, DateTable};
use super::{slow, small, ts};
use crate::aggregate::{self, output, CampStore, GAP_MS, NO_CATEGORY, WIN_MS};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const WEEK_MS: i64 = 7 * ts::DAY_MS;
const CHUNK: u64 = 256 << 10;
const LP_BITS: u32 = 8;
const LP: usize = 1 << LP_BITS;
const INVALID_CAMP: u32 = u32::MAX;
const MAX_THREADS: usize = 32;

#[inline(always)]
fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 29)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^ (x >> 32)
}

#[inline(always)]
unsafe fn rd_u32(p: *const u8) -> u32 {
    u32::from_le_bytes(std::ptr::read_unaligned(p as *const [u8; 4]))
}
#[inline(always)]
unsafe fn rd_u64(p: *const u8) -> u64 {
    u64::from_le_bytes(std::ptr::read_unaligned(p as *const [u8; 8]))
}
#[inline(always)]
unsafe fn rd_i64(p: *const u8) -> i64 {
    i64::from_le_bytes(std::ptr::read_unaligned(p as *const [u8; 8]))
}
#[inline(always)]
unsafe fn wr_u32(p: *mut u8, v: u32) {
    std::ptr::write_unaligned(p as *mut [u8; 4], v.to_le_bytes());
}
#[inline(always)]
unsafe fn wr_u64(p: *mut u8, v: u64) {
    std::ptr::write_unaligned(p as *mut [u8; 8], v.to_le_bytes());
}
#[inline(always)]
unsafe fn wr_i64(p: *mut u8, v: i64) {
    std::ptr::write_unaligned(p as *mut [u8; 8], v.to_le_bytes());
}
#[inline(always)]
fn len_enc_size(len: usize) -> usize {
    if len < 255 { 1 } else { 5 }
}
#[inline(always)]
unsafe fn wr_len(p: *mut u8, len: usize) -> *mut u8 {
    if len < 255 {
        *p = len as u8;
        p.add(1)
    } else {
        *p = 255;
        wr_u32(p.add(1), len as u32);
        p.add(5)
    }
}
#[inline(always)]
unsafe fn rd_len(p: *const u8) -> (usize, *const u8) {
    let b = *p;
    if b < 255 {
        (b as usize, p.add(1))
    } else {
        (rd_u32(p.add(1)) as usize, p.add(5))
    }
}

/// Fast, high-avalanche 64-bit hash over an arbitrary byte slice (100% in-bounds).
#[inline]
pub fn hash_bytes(s: &[u8]) -> u64 {
    const K0: u64 = 0xA076_1D64_78BD_642F;
    const K1: u64 = 0xE703_7ED1_A0B4_28DB;
    const K2: u64 = 0x8EBC_6AF0_9C88_C6E3;
    const K3: u64 = 0x5899_65CC_7537_4CC3;
    let len = s.len();
    let p = s.as_ptr();
    let mut h = (len as u64).wrapping_mul(K0) ^ K1;
    if len >= 8 {
        let mut i = 0usize;
        while i + 16 <= len {
            let a = unsafe { rd_u64(p.add(i)) };
            let b = unsafe { rd_u64(p.add(i + 8)) };
            let m = (a ^ K1).wrapping_mul(b ^ K2);
            h ^= m ^ (m >> 32);
            h = h.rotate_left(27).wrapping_mul(K3);
            i += 16;
        }
        if i + 8 <= len {
            let a = unsafe { rd_u64(p.add(i)) };
            h = (h ^ a).wrapping_mul(K1);
            h ^= h >> 29;
        }
        let tail = unsafe { rd_u64(p.add(len - 8)) };
        h = (h ^ tail).wrapping_mul(K2);
        mix64(h ^ (len as u64))
    } else if len >= 4 {
        let a = unsafe { rd_u32(p) } as u64;
        let b = unsafe { rd_u32(p.add(len - 4)) } as u64;
        mix64(h ^ a ^ (b << 32))
    } else if len > 0 {
        let a = s[0] as u64;
        let b = s[len >> 1] as u64;
        let c = s[len - 1] as u64;
        mix64(h ^ a ^ (b << 8) ^ (c << 16))
    } else {
        mix64(h)
    }
}

#[inline(always)]
fn field<'a>(b: &'a [u8], f: (u32, u32)) -> &'a [u8] {
    unsafe { b.get_unchecked(f.0 as usize..f.1 as usize) }
}
#[inline(always)]
fn fptr(b: &[u8], f: (u32, u32)) -> *const u8 {
    unsafe { b.as_ptr().add(f.0 as usize) }
}
#[inline(always)]
fn flen(f: (u32, u32)) -> usize {
    (f.1 - f.0) as usize
}
#[inline(always)]
fn fts(dt: &DateTable, b: &[u8], f: (u32, u32)) -> Option<i64> {
    unsafe { dt.parse(fptr(b, f), flen(f)) }
}

/// Read-only 256-partition map `user_id_bytes -> (canonical_bytes, canonical_hash)` for mapped IDs only.
struct LinkEntry {
    key_off: u64,
    canon_off: u64,
    key_len: u32,
    canon_len: u32,
    canon_hash: u64,
}

struct LinkPart {
    table: Vec<u64>,
    mask: usize,
    entries: Vec<LinkEntry>,
    arena: Vec<u8>,
}

struct GenericLinks {
    has_links: bool,
    parts: Vec<LinkPart>,
}

impl GenericLinks {
    fn empty() -> Self {
        GenericLinks { has_links: false, parts: Vec::new() }
    }

    fn build(files: &[FileInfo], link_chunks: &[Chunk], threads: usize, tm: &mut Timer) -> Self {
        if link_chunks.is_empty() {
            tm.lap("links");
            return Self::empty();
        }
        // 1. Parallel parse of link CSV chunks into per-worker contiguous byte arena + (off_a, len_a, len_b).
        struct RawLinks {
            bytes: Vec<u8>,
            pairs: Vec<(u64, u32, u32)>,
            buf: Vec<u8>,
        }
        let workers: Vec<RawLinks> = par::run(
            threads,
            link_chunks.len(),
            |_| RawLinks { bytes: Vec::new(), pairs: Vec::new(), buf: Vec::with_capacity(CHUNK as usize + 8192) },
            |st, i| {
                let c = link_chunks[i];
                let fi = &files[c.file as usize];
                let mut buf = std::mem::take(&mut st.buf);
                let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, &mut buf);
                if lo < hi {
                    scan::scan::<2, _>(&buf, lo, hi, |b, f| {
                        let la = flen(f[0]);
                        let lb = flen(f[1]);
                        if la == 0 || lb == 0 {
                            return;
                        }
                        let off = st.bytes.len() as u64;
                        st.bytes.extend_from_slice(field(b, f[0]));
                        st.bytes.extend_from_slice(field(b, f[1]));
                        st.pairs.push((off, la as u32, lb as u32));
                    });
                }
                st.buf = buf;
            },
        );
        let mut off = vec![0usize; workers.len() + 1];
        for (i, w) in workers.iter().enumerate() {
            off[i + 1] = off[i] + w.pairs.len();
        }
        let nl = off[workers.len()];
        tm.lap("links-parse");
        if nl == 0 {
            eprintln!("[generic] links=0 linked_ids=0");
            return Self::empty();
        }

        // 2. Parallel scatter lightweight 24-byte refs to LP=256 partitions (zero string copying).
        #[derive(Clone, Copy)]
        struct LinkRef {
            h: u64,
            off: u64,
            len: u32,
            edge_end: u32,
        }
        struct Scattered {
            worker: usize,
            flat: Vec<LinkRef>,
            pos: [usize; LP + 1],
        }
        let workers_ref = &workers;
        let off_ref = &off;
        let mut scattered: Vec<Scattered> = par::run(threads, workers.len(), |_| Vec::new(), |res, t| {
            let w = &workers_ref[t];
            let np = w.pairs.len();
            if np == 0 {
                res.push(Scattered { worker: t, flat: Vec::new(), pos: [0; LP + 1] });
                return;
            }
            let mut hashes: Vec<(u64, u64)> = Vec::with_capacity(np);
            let mut cnt = [0usize; LP];
            for &(o, la, lb) in &w.pairs {
                let sa = &w.bytes[o as usize..o as usize + la as usize];
                let sb = &w.bytes[o as usize + la as usize..o as usize + la as usize + lb as usize];
                let ha = hash_bytes(sa);
                let hb = hash_bytes(sb);
                cnt[(ha >> (64 - LP_BITS)) as usize] += 1;
                cnt[(hb >> (64 - LP_BITS)) as usize] += 1;
                hashes.push((ha, hb));
            }
            let mut pos = [0usize; LP + 1];
            for p in 0..LP {
                pos[p + 1] = pos[p] + cnt[p];
            }
            let mut cur = [0usize; LP];
            cur.copy_from_slice(&pos[..LP]);
            let mut flat: Vec<LinkRef> = Vec::with_capacity(pos[LP]);
            unsafe { flat.set_len(pos[LP]) };
            let ptr = flat.as_mut_ptr();
            for (i, &(o, la, lb)) in w.pairs.iter().enumerate() {
                let (ha, hb) = hashes[i];
                let e = 2 * (off_ref[t] + i) as u32;
                let pa = (ha >> (64 - LP_BITS)) as usize;
                let pb = (hb >> (64 - LP_BITS)) as usize;
                unsafe {
                    ptr.add(cur[pa]).write(LinkRef { h: ha, off: o, len: la, edge_end: e });
                    cur[pa] += 1;
                    ptr.add(cur[pb]).write(LinkRef { h: hb, off: o + la as u64, len: lb, edge_end: e + 1 });
                    cur[pb] += 1;
                }
            }
            res.push(Scattered { worker: t, flat, pos });
        })
        .into_iter()
        .flatten()
        .collect();
        scattered.sort_unstable_by_key(|s| s.worker);

        // 3. Per partition: intern unique IDs into local byte arena; write endpoint codes into `ep`.
        struct InternedPart {
            part: usize,
            arena: Vec<u8>,
            meta: Vec<(u64, u32, u64)>, // (off, len, hash)
        }
        let mut ep: Vec<u64> = Vec::with_capacity(2 * nl);
        unsafe { ep.set_len(2 * nl) };
        let epp = SyncPtr(ep.as_mut_ptr());
        let scattered_ref = &scattered;
        let mut interned: Vec<InternedPart> = par::run(
            threads,
            LP,
            |_| (Vec::new(), Vec::<u64>::new()),
            move |(res, table), p| {
                let n: usize = scattered_ref.iter().map(|s| s.pos[p + 1] - s.pos[p]).sum();
                let mut cap = (n * 2).next_power_of_two().max(16);
                if table.len() < cap {
                    table.resize(cap, 0);
                }
                table[..cap].fill(0);
                let mut mask = cap - 1;
                let mut arena: Vec<u8> = Vec::with_capacity(n * 16);
                let mut meta: Vec<(u64, u32, u64)> = Vec::with_capacity(n / 2 + 8);
                for s in scattered_ref.iter() {
                    let wb = &workers_ref[s.worker].bytes;
                    for r in &s.flat[s.pos[p]..s.pos[p + 1]] {
                        let key = &wb[r.off as usize..r.off as usize + r.len as usize];
                        let fp = (r.h >> 32) as u32;
                        let mut slot = (r.h as usize) & mask;
                        let id = loop {
                            let v = unsafe { *table.get_unchecked(slot) };
                            if v == 0 {
                                let id = meta.len() as u32;
                                let aoff = arena.len() as u64;
                                arena.extend_from_slice(key);
                                meta.push((aoff, r.len, r.h));
                                unsafe { *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (id as u64 + 1) };
                                if meta.len() * 2 > cap {
                                    cap *= 2;
                                    mask = cap - 1;
                                    if table.len() < cap {
                                        table.resize(cap, 0);
                                    }
                                    table[..cap].fill(0);
                                    for (idx, &(_, _, mh)) in meta.iter().enumerate() {
                                        let mfp = (mh >> 32) as u32;
                                        let mut ms = (mh as usize) & mask;
                                        while unsafe { *table.get_unchecked(ms) } != 0 {
                                            ms = (ms + 1) & mask;
                                        }
                                        unsafe {
                                            *table.get_unchecked_mut(ms) = ((mfp as u64) << 32) | (idx as u64 + 1);
                                        }
                                    }
                                }
                                break id;
                            }
                            if (v >> 32) as u32 == fp {
                                let idx = (v as u32) - 1;
                                let (moff, mlen, _) = unsafe { *meta.get_unchecked(idx as usize) };
                                if mlen == r.len
                                    && &arena[moff as usize..moff as usize + mlen as usize] == key
                                {
                                    break idx;
                                }
                            }
                            slot = (slot + 1) & mask;
                        };
                        unsafe { epp.write(r.edge_end as usize, ((p as u64) << 32) | id as u64) };
                    }
                }
                res.push(InternedPart { part: p, arena, meta });
            },
        )
        .into_iter()
        .flat_map(|x| x.0)
        .collect();
        drop(scattered);
        drop(workers);
        interned.sort_unstable_by_key(|x| x.part);

        let mut base = vec![0u32; LP + 1];
        for p in 0..LP {
            base[p + 1] = base[p] + interned[p].meta.len() as u32;
        }
        let nn = base[LP] as usize;

        // 4. Flat node directory pointing into per-partition arenas (zero arena copy!).
        #[derive(Clone, Copy)]
        struct NodeRef {
            part: u32,
            len: u32,
            off: u64,
            hash: u64,
        }
        let mut nodes: Vec<NodeRef> = Vec::with_capacity(nn);
        let mut parent: Vec<AtomicU32> = Vec::with_capacity(nn);
        let mut best: Vec<AtomicU32> = Vec::with_capacity(nn);
        unsafe {
            nodes.set_len(nn);
            parent.set_len(nn);
            best.set_len(nn);
        }
        {
            let (np, pp, bp) = (SyncPtr(nodes.as_mut_ptr()), SyncPtr(parent.as_mut_ptr()), SyncPtr(best.as_mut_ptr()));
            let (int_ref, base_ref) = (&interned, &base);
            par::run(threads, LP, |_| (), move |_, p| {
                let s = base_ref[p] as usize;
                for (k, &(off, len, hash)) in int_ref[p].meta.iter().enumerate() {
                    let i = s + k;
                    unsafe {
                        np.write(i, NodeRef { part: p as u32, len, off, hash });
                        pp.write(i, AtomicU32::new(i as u32));
                        bp.write(i, AtomicU32::new(i as u32));
                    }
                }
            });
        }
        let arenas: Vec<Vec<u8>> = interned.into_iter().map(|ip| ip.arena).collect();

        // 5. Parallel lock-free union-find + lexicographically smallest byte slice per root.
        let node = |code: u64| base[(code >> 32) as usize] + code as u32;
        let step = 1 << 16;
        let (parent_ref, ep_ref, nodes_ref, arenas_ref, best_ref) = (&parent, &ep, &nodes, &arenas, &best);
        par::run(threads, nl.div_ceil(step), |_| (), |_, t| {
            for i in t * step..((t + 1) * step).min(nl) {
                let (a, b) = (node(ep_ref[2 * i]), node(ep_ref[2 * i + 1]));
                if a != b {
                    uf_union(parent_ref, a, b);
                }
            }
        });
        drop(ep);
        let node_bytes = |idx: usize| -> &[u8] {
            let nr = unsafe { *nodes_ref.get_unchecked(idx) };
            unsafe {
                arenas_ref
                    .get_unchecked(nr.part as usize)
                    .get_unchecked(nr.off as usize..nr.off as usize + nr.len as usize)
            }
        };
        par::run(threads, nn.div_ceil(step), |_| (), |_, t| {
            for i in t * step..((t + 1) * step).min(nn) {
                let r = uf_find(parent_ref, i as u32) as usize;
                let ib = node_bytes(i);
                let mut cur = best_ref[r].load(Ordering::Relaxed);
                while ib < node_bytes(cur as usize) {
                    match best_ref[r].compare_exchange_weak(cur, i as u32, Ordering::Relaxed, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(c) => cur = c,
                    }
                }
            }
        });

        // 6. Build compact per-partition lookup tables containing ONLY `id != canonical`.
        let base_ref = &base;
        let mut built: Vec<(usize, LinkPart)> = par::run(threads, LP, |_| Vec::new(), move |res, p| {
            let (s, e) = (base_ref[p] as usize, base_ref[p + 1] as usize);
            let mut entries: Vec<LinkEntry> = Vec::new();
            let mut arena: Vec<u8> = Vec::new();
            let mut hashes: Vec<u64> = Vec::new();
            for i in s..e {
                let r = uf_find(parent_ref, i as u32) as usize;
                let c = best_ref[r].load(Ordering::Relaxed) as usize;
                if c != i {
                    let kb = node_bytes(i);
                    let cb = node_bytes(c);
                    let key_off = arena.len() as u64;
                    arena.extend_from_slice(kb);
                    let canon_off = arena.len() as u64;
                    arena.extend_from_slice(cb);
                    entries.push(LinkEntry {
                        key_off,
                        canon_off,
                        key_len: kb.len() as u32,
                        canon_len: cb.len() as u32,
                        canon_hash: nodes_ref[c].hash,
                    });
                    hashes.push(nodes_ref[i].hash);
                }
            }
            if entries.is_empty() {
                res.push((p, LinkPart { table: Vec::new(), mask: 0, entries, arena }));
                return;
            }
            let cap = (entries.len() * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let mut table = vec![0u64; cap];
            for (idx, &h) in hashes.iter().enumerate() {
                let fp = (h >> 32) as u32;
                let mut slot = (h as usize) & mask;
                while table[slot] != 0 {
                    slot = (slot + 1) & mask;
                }
                table[slot] = ((fp as u64) << 32) | (idx as u64 + 1);
            }
            res.push((p, LinkPart { table, mask, entries, arena }));
        })
        .into_iter()
        .flatten()
        .collect();
        drop(nodes);
        drop(arenas);
        drop(parent);
        drop(best);

        built.sort_unstable_by_key(|x| x.0);
        let total_mapped: usize = built.iter().map(|x| x.1.entries.len()).sum();
        eprintln!("[generic] links={nl} linked_ids={nn} mapped={total_mapped}");
        tm.lap("links-uf");
        if total_mapped == 0 {
            return Self::empty();
        }
        GenericLinks { has_links: true, parts: built.into_iter().map(|x| x.1).collect() }
    }

    #[inline(always)]
    fn prefetch_h(&self, h: u64) {
        let p = (h >> (64 - LP_BITS)) as usize;
        let part = unsafe { self.parts.get_unchecked(p) };
        if !part.table.is_empty() {
            let slot = (h as usize) & part.mask;
            unsafe {
                std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                    part.table.as_ptr().add(slot) as *const i8,
                );
            }
        }
    }

    #[inline(always)]
    fn canonical_h<'a>(&'a self, uid: &'a [u8], h: u64) -> (&'a [u8], u64) {
        let p = (h >> (64 - LP_BITS)) as usize;
        let part = unsafe { self.parts.get_unchecked(p) };
        if part.table.is_empty() {
            return (uid, h);
        }
        let fp = (h >> 32) as u32;
        let mask = part.mask;
        let mut slot = (h as usize) & mask;
        loop {
            let v = unsafe { *part.table.get_unchecked(slot) };
            if v == 0 {
                return (uid, h);
            }
            if (v >> 32) as u32 == fp {
                let e = unsafe { part.entries.get_unchecked((v as u32 - 1) as usize) };
                if e.key_len as usize == uid.len() {
                    let kb = unsafe {
                        part.arena.get_unchecked(e.key_off as usize..e.key_off as usize + e.key_len as usize)
                    };
                    if kb == uid {
                        let cb = unsafe {
                            part.arena.get_unchecked(e.canon_off as usize..e.canon_off as usize + e.canon_len as usize)
                        };
                        return (cb, e.canon_hash);
                    }
                }
            }
            slot = (slot + 1) & mask;
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

fn uf_union(p: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = uf_find(p, a);
        b = uf_find(p, b);
        if a == b {
            return;
        }
        let (hi, lo) = if a > b { (a, b) } else { (b, a) };
        if p[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

#[inline(always)]
fn push_b_rec(sb: &mut SlabBuf, person: &[u8], ph: u64, ts_ms: i64, cat: u32, cents: u32) {
    let is_purch = cents != 0;
    let plen = person.len();
    let need = 17 + len_enc_size(plen) + plen + if is_purch { 8 } else { 0 };
    let dst = sb.alloc_raw(need);
    unsafe {
        *dst = is_purch as u8;
        wr_u64(dst.add(1), ph);
        wr_i64(dst.add(9), ts_ms);
        let q = wr_len(dst.add(17), plen);
        std::ptr::copy_nonoverlapping(person.as_ptr(), q, plen);
        if is_purch {
            wr_u32(q.add(plen), cat);
            wr_u32(q.add(plen + 4), cents);
        }
    }
}

#[inline(always)]
fn push_ck_rec(sb: &mut SlabBuf, person: &[u8], ph: u64, cid: &[u8], ts_ms: i64, campaign: u32) {
    let plen = person.len();
    let clen = cid.len();
    let need = 20 + len_enc_size(plen) + len_enc_size(clen) + plen + clen;
    let dst = sb.alloc_raw(need);
    unsafe {
        wr_u64(dst, ph);
        wr_i64(dst.add(8), ts_ms);
        wr_u32(dst.add(16), campaign);
        let q = wr_len(dst.add(20), plen);
        let q = wr_len(q, clen);
        std::ptr::copy_nonoverlapping(person.as_ptr(), q, plen);
        std::ptr::copy_nonoverlapping(cid.as_ptr(), q.add(plen), clen);
    }
}

fn transpose_slabs(by_thread: Vec<Vec<SlabBuf>>, nparts: usize) -> Vec<Mutex<Vec<SlabBuf>>> {
    let nthreads = by_thread.len();
    let mut out: Vec<Vec<SlabBuf>> = (0..nparts).map(|_| Vec::with_capacity(nthreads)).collect();
    for tv in by_thread {
        for (p, sb) in tv.into_iter().enumerate() {
            if sb.count > 0 {
                out[p].push(sb);
            }
        }
    }
    out.into_iter().map(Mutex::new).collect()
}

/// Parallel, memory-bounded generic pipeline: CSVs -> Parquet directly.
pub fn run_to_output(data_dir: &Path, output_dir: &Path, threads: usize) {
    let threads = threads.clamp(1, MAX_THREADS);
    let mut tm = Timer::new("generic");

    // ---------------- Stage 0: small metadata + prefetch ----------------
    let (ws, we) = small::read_manifest(data_dir);
    let (categories, _, prod) = small::read_products_lookup(data_dir);
    let (campaigns, camp) = small::read_campaigns_lookup(data_dir);
    let dt = DateTable::build(ws, we);
    let files = Arc::new(scan::open_csv_files(data_dir));
    let mut prefetch = par::Prefetch::start(&files, threads);
    let chunks = scan::plan(&files, CHUNK);
    let link_chunks: Vec<Chunk> = chunks.iter().copied().filter(|c| files[c.file as usize].kind == Kind::Links).collect();
    let event_chunks: Vec<Chunk> = chunks.iter().copied().filter(|c| files[c.file as usize].kind == Kind::Events).collect();
    let click_chunks: Vec<Chunk> = chunks.iter().copied().filter(|c| files[c.file as usize].kind == Kind::Clicks).collect();
    let event_bytes: u64 = files.iter().filter(|f| f.kind == Kind::Events).map(|f| f.len).sum();
    let click_bytes: u64 = files.iter().filter(|f| f.kind == Kind::Clicks).map(|f| f.len).sum();
    tm.lap("small");

    // ---------------- Stage 1: Links ----------------
    let links = GenericLinks::build(&files, &link_chunks, threads, &mut tm);

    // ---------------- Stage 2: Events parse ONLY -> P1 SlabBufs ----------------
    let est_ev = (event_bytes / 75) as usize + 1024;
    let p1_bits: u32 = if est_ev <= 4_000_000 { 6 } else { 8 };
    let p1: usize = 1 << p1_bits;
    let p2_bits: u32 = if est_ev <= 4_000_000 { 6 } else { 8 };
    let p2: usize = 1 << p2_bits;

    let slab_ev = ((event_bytes as usize * 6 / 5) / (threads * p1 * 6).max(1))
        .clamp(16 << 10, 512 << 10)
        .next_power_of_two();
    let ev_by_thread: Vec<Vec<SlabBuf>> = par::run(
        threads,
        event_chunks.len(),
        |_| ((0..p1).map(|_| SlabBuf::new(slab_ev)).collect::<Vec<_>>(), Vec::<u8>::with_capacity(CHUNK as usize + 8192)),
        |(parts, buf), i| {
            let c = event_chunks[i];
            let fi = &files[c.file as usize];
            let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, buf);
            if lo < hi {
                scan::scan::<7, _>(buf, lo, hi, |b, f| {
                    let eid_len = flen(f[0]);
                    if eid_len == 0 {
                        return;
                    }
                    let eid = field(b, f[0]);
                    let eid_h = hash_bytes(eid);
                    let ingest_ms = fts(&dt, b, f[3]).unwrap_or(i64::MAX);

                    let mut valid = false;
                    let mut ts_ms = 0i64;
                    let mut cat = 0u32;
                    let mut cents = 0u32;
                    let mut uid_f = (0u32, 0u32);
                    'v: {
                        if ingest_ms == i64::MAX || flen(f[1]) == 0 || flen(f[5]) == 0 {
                            break 'v;
                        }
                        let t = match fts(&dt, b, f[2]) {
                            Some(t) if t >= ws && t < we => t,
                            _ => break 'v,
                        };
                        if ingest_ms.saturating_sub(t) > ts::DAY_MS {
                            break 'v;
                        }
                        let ty = f[4];
                        let c_val = if flen(ty) == 4 {
                            let w = unsafe { rd_u32(fptr(b, ty)) };
                            if (w == u32::from_le_bytes(*b"view")) | (w == u32::from_le_bytes(*b"cart")) {
                                0
                            } else {
                                break 'v;
                            }
                        } else if flen(ty) == 8 && unsafe { rd_u64(fptr(b, ty)) } == u64::from_le_bytes(*b"purchase") {
                            let pr = f[6];
                            let lp = flen(pr);
                            let v = if (1..=8).contains(&lp) {
                                unsafe { tsfast::digits8(fptr(b, pr), lp) }.map(|v| v as u32)
                            } else {
                                slow::parse_price(field(b, pr))
                            };
                            match v {
                                Some(c) if (1..=10_000_000).contains(&c) => c,
                                _ => break 'v,
                            }
                        } else {
                            break 'v;
                        };
                        valid = true;
                        ts_ms = t;
                        uid_f = f[1];
                        cents = c_val;
                        if c_val != 0 {
                            cat = unsafe { prod.get(fptr(b, f[5]), flen(f[5])) };
                        }
                    }

                    let p = (eid_h >> (64 - p1_bits)) as usize;
                    let sb = unsafe { parts.get_unchecked_mut(p) };
                    if !valid {
                        let need = 13 + len_enc_size(eid_len) + eid_len;
                        let dst = sb.alloc_raw(need);
                        unsafe {
                            *dst = 0;
                            wr_u32(dst.add(1), eid_h as u32);
                            wr_i64(dst.add(5), ingest_ms);
                            let q = wr_len(dst.add(13), eid_len);
                            std::ptr::copy_nonoverlapping(eid.as_ptr(), q, eid_len);
                        }
                    } else {
                        let uid = field(b, uid_f);
                        let ulen = uid.len();
                        let is_purch = cents != 0;
                        let flags = if is_purch { 3u8 } else { 1u8 };
                        let need = 13 + len_enc_size(eid_len) + eid_len + 8 + len_enc_size(ulen) + ulen
                            + if is_purch { 8 } else { 0 };
                        let dst = sb.alloc_raw(need);
                        unsafe {
                            *dst = flags;
                            wr_u32(dst.add(1), eid_h as u32);
                            wr_i64(dst.add(5), ingest_ms);
                            let q = wr_len(dst.add(13), eid_len);
                            std::ptr::copy_nonoverlapping(eid.as_ptr(), q, eid_len);
                            let q = q.add(eid_len);
                            wr_i64(q, ts_ms);
                            let q = wr_len(q.add(8), ulen);
                            std::ptr::copy_nonoverlapping(uid.as_ptr(), q, ulen);
                            if is_purch {
                                wr_u32(q.add(ulen), cat);
                                wr_u32(q.add(ulen + 4), cents);
                            }
                        }
                    }
                });
            }
        },
    )
    .into_iter()
    .map(|x| x.0)
    .collect();
    let ne_raw: usize = ev_by_thread.iter().flat_map(|v| v.iter()).map(|s| s.count).sum();
    let ev_total_bytes: usize = ev_by_thread.iter().flat_map(|v| v.iter()).map(|s| s.total_bytes).sum();
    let ev_by_part = transpose_slabs(ev_by_thread, p1);
    tm.lap("ev-parse");

    // ---------------- Stage 3: Events dedup (frees ev_parts partition-by-partition!) ----------------
    let slab_b = ((ev_total_bytes / 2) / (threads * p2 * 6).max(1))
        .clamp(16 << 10, 256 << 10)
        .next_power_of_two();
    struct DedupState {
        t32: Vec<u32>,
        t64: Vec<u64>,
        dead: Vec<u64>,
        ptrs: Vec<*const u8>,
        out: Vec<SlabBuf>,
    }
    // SAFETY: `ptrs` is always cleared before leaving each partition task and never escapes its worker thread.
    unsafe impl Send for DedupState {}

    let links_ref = &links;
    let ev_by_part_ref = &ev_by_part;
    let b_by_thread: Vec<Vec<SlabBuf>> = par::run(
        threads,
        p1,
        |_| DedupState {
            t32: Vec::new(),
            t64: Vec::new(),
            dead: Vec::new(),
            ptrs: Vec::new(),
            out: (0..p2).map(|_| SlabBuf::new(slab_b)).collect(),
        },
        |st, p| {
            let bufs = std::mem::take(&mut *ev_by_part_ref[p].lock().unwrap());
            let total: usize = bufs.iter().map(|s| s.count).sum();
            if total == 0 {
                return;
            }
            st.ptrs.clear();
            st.ptrs.reserve(total);
            for sb in &bufs {
                for sl in sb.slabs() {
                    let base = sl.as_ptr();
                    let end_ptr = unsafe { base.add(sl.len()) };
                    let mut ptr = base;
                    while ptr < end_ptr {
                        st.ptrs.push(ptr);
                        let flags = unsafe { *ptr };
                        let (eid_len, q) = unsafe { rd_len(ptr.add(13)) };
                        let mut next = unsafe { q.add(eid_len) };
                        if flags & 1 != 0 {
                            let (uid_len, q2) = unsafe { rd_len(next.add(8)) };
                            next = unsafe { q2.add(uid_len) };
                            if flags & 2 != 0 {
                                next = unsafe { next.add(8) };
                            }
                        }
                        ptr = next;
                    }
                }
            }
            debug_assert_eq!(st.ptrs.len(), total);

            let dead_words = total.div_ceil(64);
            if st.dead.len() < dead_words {
                st.dead.resize(dead_words, 0);
            }
            let dead = &mut st.dead[..dead_words];
            dead.fill(0);
            let cap = (total * 2).next_power_of_two();
            let mask = cap - 1;

            // Pass 1: dedup by exact event_id bytes, keeping minimum ingest_ms (first on ties).
            if total < (1 << 18) - 1 {
                if st.t32.len() < cap {
                    st.t32.resize(cap, 0);
                }
                let table = &mut st.t32[..cap];
                table.fill(0);
                for flat in 0..total {
                    let ptr = unsafe { *st.ptrs.get_unchecked(flat) };
                    let h = unsafe { rd_u32(ptr.add(1)) };
                    let fp = h >> 18;
                    let mut slot = (h as usize) & mask;
                    loop {
                        let v = unsafe { *table.get_unchecked(slot) };
                        if v == 0 {
                            unsafe { *table.get_unchecked_mut(slot) = (fp << 18) | (flat as u32 + 1) };
                            break;
                        }
                        if (v >> 18) == fp {
                            let prev = (v & ((1 << 18) - 1)) - 1;
                            let wptr = unsafe { *st.ptrs.get_unchecked(prev as usize) };
                            let (elen, ep) = unsafe { rd_len(ptr.add(13)) };
                            let (wlen, wp) = unsafe { rd_len(wptr.add(13)) };
                            if elen == wlen {
                                let esl = unsafe { std::slice::from_raw_parts(ep, elen) };
                                let wsl = unsafe { std::slice::from_raw_parts(wp, wlen) };
                                if esl == wsl {
                                    let ring = unsafe { rd_i64(ptr.add(5)) };
                                    let wing = unsafe { rd_i64(wptr.add(5)) };
                                    if ring < wing {
                                        unsafe {
                                            *dead.get_unchecked_mut((prev >> 6) as usize) |= 1u64 << (prev & 63);
                                            *table.get_unchecked_mut(slot) = (fp << 18) | (flat as u32 + 1);
                                        }
                                    } else {
                                        unsafe {
                                            *dead.get_unchecked_mut(flat >> 6) |= 1u64 << (flat & 63);
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            } else {
                if st.t64.len() < cap {
                    st.t64.resize(cap, 0);
                }
                let table = &mut st.t64[..cap];
                table.fill(0);
                for flat in 0..total {
                    let ptr = unsafe { *st.ptrs.get_unchecked(flat) };
                    let (elen, ep) = unsafe { rd_len(ptr.add(13)) };
                    let esl = unsafe { std::slice::from_raw_parts(ep, elen) };
                    let h = hash_bytes(esl);
                    let fp = (h >> 32) as u32;
                    let mut slot = (h as usize) & mask;
                    loop {
                        let v = unsafe { *table.get_unchecked(slot) };
                        if v == 0 {
                            unsafe { *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (flat as u64 + 1) };
                            break;
                        }
                        if (v >> 32) as u32 == fp {
                            let prev = (v as u32) - 1;
                            let wptr = unsafe { *st.ptrs.get_unchecked(prev as usize) };
                            let (wlen, wp) = unsafe { rd_len(wptr.add(13)) };
                            if elen == wlen {
                                let wsl = unsafe { std::slice::from_raw_parts(wp, wlen) };
                                if esl == wsl {
                                    let ring = unsafe { rd_i64(ptr.add(5)) };
                                    let wing = unsafe { rd_i64(wptr.add(5)) };
                                    if ring < wing {
                                        unsafe {
                                            *dead.get_unchecked_mut((prev >> 6) as usize) |= 1u64 << (prev & 63);
                                            *table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (flat as u64 + 1);
                                        }
                                    } else {
                                        unsafe {
                                            *dead.get_unchecked_mut(flat >> 6) |= 1u64 << (flat & 63);
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }

            // Pass 2: sequential scan of survivors -> ring-32 prefetched canonical resolution -> P2 `b_parts`.
            #[derive(Clone, Copy)]
            struct PendingEv {
                uid_ptr: *const u8,
                uid_len: u32,
                uh: u64,
                ts_ms: i64,
                cat: u32,
                cents: u32,
            }
            const EV_RING: usize = 32;
            let mut ring = [PendingEv { uid_ptr: std::ptr::null(), uid_len: 0, uh: 0, ts_ms: 0, cat: 0, cents: 0 }; EV_RING];
            let mut cnt = 0usize;
            let has_links = links_ref.has_links;
            for flat in 0..total {
                if unsafe { (*dead.get_unchecked(flat >> 6) >> (flat & 63)) & 1 } != 0 {
                    continue;
                }
                let ptr = unsafe { *st.ptrs.get_unchecked(flat) };
                let flags = unsafe { *ptr };
                if flags & 1 == 0 {
                    continue;
                }
                let (eid_len, ep) = unsafe { rd_len(ptr.add(13)) };
                let qptr = unsafe { ep.add(eid_len) };
                let ts_ms = unsafe { rd_i64(qptr) };
                let (uid_len, uid_ptr) = unsafe { rd_len(qptr.add(8)) };
                let uid = unsafe { std::slice::from_raw_parts(uid_ptr, uid_len) };
                let (cat, cents) = if flags & 2 != 0 {
                    unsafe { (rd_u32(uid_ptr.add(uid_len)), rd_u32(uid_ptr.add(uid_len + 4))) }
                } else {
                    (0, 0)
                };
                let uh = hash_bytes(uid);
                if has_links {
                    links_ref.prefetch_h(uh);
                    let slot = cnt & (EV_RING - 1);
                    if cnt >= EV_RING {
                        let pe = unsafe { *ring.get_unchecked(slot) };
                        let puid = unsafe { std::slice::from_raw_parts(pe.uid_ptr, pe.uid_len as usize) };
                        let (person, ph) = links_ref.canonical_h(puid, pe.uh);
                        let q = (ph >> (64 - p2_bits)) as usize;
                        push_b_rec(unsafe { st.out.get_unchecked_mut(q) }, person, ph, pe.ts_ms, pe.cat, pe.cents);
                    }
                    unsafe {
                        *ring.get_unchecked_mut(slot) = PendingEv { uid_ptr, uid_len: uid_len as u32, uh, ts_ms, cat, cents };
                    }
                    cnt += 1;
                } else {
                    let q = (uh >> (64 - p2_bits)) as usize;
                    push_b_rec(unsafe { st.out.get_unchecked_mut(q) }, uid, uh, ts_ms, cat, cents);
                }
            }
            let n_rem = cnt.min(EV_RING);
            for k in (cnt - n_rem)..cnt {
                let pe = unsafe { *ring.get_unchecked(k & (EV_RING - 1)) };
                let puid = unsafe { std::slice::from_raw_parts(pe.uid_ptr, pe.uid_len as usize) };
                let (person, ph) = links_ref.canonical_h(puid, pe.uh);
                let q = (ph >> (64 - p2_bits)) as usize;
                push_b_rec(unsafe { st.out.get_unchecked_mut(q) }, person, ph, pe.ts_ms, pe.cat, pe.cents);
            }
            // Clear raw pointers and immediately unmap `ev_parts[p]` slabs!
            st.ptrs.clear();
            drop(bufs);
        },
    )
    .into_iter()
    .map(|s| s.out)
    .collect();
    drop(ev_by_part);
    let b_by_part = transpose_slabs(b_by_thread, p2);
    tm.lap("ev-dedup");

    // ---------------- Stage 4: Clicks parse (AFTER ev_parts is 100% freed!) ----------------
    let slab_ck = (click_bytes as usize / (threads * p2 * 4).max(1))
        .clamp(16 << 10, 256 << 10)
        .next_power_of_two();
    let ck_base = ws.saturating_sub(WEEK_MS);
    let ck_by_thread: Vec<Vec<SlabBuf>> = par::run(
        threads,
        click_chunks.len(),
        |_| ((0..p2).map(|_| SlabBuf::new(slab_ck)).collect::<Vec<_>>(), Vec::<u8>::with_capacity(CHUNK as usize + 8192)),
        |(parts, buf), i| {
            let c = click_chunks[i];
            let fi = &files[c.file as usize];
            let (lo, hi) = scan::read_chunk(&fi.handle, fi.len, c, buf);
            if lo < hi {
                #[derive(Clone, Copy)]
                struct PendingCk {
                    uid_f: (u32, u32),
                    cid_f: (u32, u32),
                    uh: u64,
                    ts_ms: i64,
                    campaign: u32,
                }
                const CK_RING: usize = 32;
                let mut ring = [PendingCk { uid_f: (0, 0), cid_f: (0, 0), uh: 0, ts_ms: 0, campaign: 0 }; CK_RING];
                let mut cnt = 0usize;
                let has_links = links_ref.has_links;
                scan::scan::<4, _>(buf, lo, hi, |b, f| {
                    if flen(f[0]) == 0 || flen(f[1]) == 0 {
                        return;
                    }
                    let campaign = unsafe { camp.get(fptr(b, f[2]), flen(f[2])) };
                    if campaign == INVALID_CAMP {
                        return;
                    }
                    let t = match fts(&dt, b, f[3]) {
                        Some(t) if t >= ck_base && t < we => t,
                        _ => return,
                    };
                    let uid = field(b, f[1]);
                    let uh = hash_bytes(uid);
                    if has_links {
                        links_ref.prefetch_h(uh);
                        let slot = cnt & (CK_RING - 1);
                        if cnt >= CK_RING {
                            let pc = unsafe { *ring.get_unchecked(slot) };
                            let (person, ph) = links_ref.canonical_h(field(buf, pc.uid_f), pc.uh);
                            let q = (ph >> (64 - p2_bits)) as usize;
                            push_ck_rec(
                                unsafe { parts.get_unchecked_mut(q) },
                                person,
                                ph,
                                field(buf, pc.cid_f),
                                pc.ts_ms,
                                pc.campaign,
                            );
                        }
                        // Pending clicks can outlive the scanner tile. Store ranges relative
                        // to the chunk so they can be read after the callback or the scan.
                        let base = unsafe { b.as_ptr().offset_from(buf.as_ptr()) } as u32;
                        let in_chunk = |(start, end)| (base + start, base + end);
                        unsafe {
                            *ring.get_unchecked_mut(slot) = PendingCk {
                                uid_f: in_chunk(f[1]),
                                cid_f: in_chunk(f[0]),
                                uh,
                                ts_ms: t,
                                campaign,
                            };
                        }
                        cnt += 1;
                    } else {
                        let q = (uh >> (64 - p2_bits)) as usize;
                        push_ck_rec(unsafe { parts.get_unchecked_mut(q) }, uid, uh, field(b, f[0]), t, campaign);
                    }
                });
                let n_rem = cnt.min(CK_RING);
                for k in (cnt - n_rem)..cnt {
                    let pc = unsafe { *ring.get_unchecked(k & (CK_RING - 1)) };
                    let (person, ph) = links_ref.canonical_h(field(buf, pc.uid_f), pc.uh);
                    let q = (ph >> (64 - p2_bits)) as usize;
                    push_ck_rec(
                        unsafe { parts.get_unchecked_mut(q) },
                        person,
                        ph,
                        field(buf, pc.cid_f),
                        pc.ts_ms,
                        pc.campaign,
                    );
                }
            }
        },
    )
    .into_iter()
    .map(|x| x.0)
    .collect();
    prefetch.stop();
    drop(links);
    let ck_by_part = transpose_slabs(ck_by_thread, p2);
    tm.lap("ck-parse");

    // ---------------- Stage 5: Group + Agg + UserWriter (frees b_parts & ck_parts per partition!) ----------------
    let ud = output_dir.join("user_daily");
    let cd = output_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");

    let mut ev_cnt_part = vec![0usize; p2];
    let mut ne = 0usize;
    for q in 0..p2 {
        let c: usize = b_by_part[q].lock().unwrap().iter().map(|s| s.count).sum();
        ev_cnt_part[q] = c;
        ne += c;
    }
    let mut q_order: Vec<usize> = (0..p2).collect();
    q_order.sort_unstable_by_key(|&q| std::cmp::Reverse(ev_cnt_part[q]));

    let day0 = aggregate::day_of(ws);
    let ndays = if we > ws {
        (aggregate::day_of(we - 1) as i64 - day0 as i64 + 1).min(u32::MAX as i64) as u32
    } else {
        0
    };
    let ncamp = campaigns.len();
    let organic = ncamp as u32;
    let ncat = categories.len();
    let categories = &categories;

    #[derive(Clone, Copy)]
    #[repr(C)]
    struct GenEv {
        ts_ms: i64,
        cat: u32,
        cents: u32,
    }
    #[derive(Clone, Copy)]
    #[repr(C)]
    struct GenClick {
        ts_ms: i64,
        campaign: u32,
        cid_len: u32,
        cid_off: u64,
    }

    struct GenWorker<'a> {
        // Local person table (reused across P2 partitions)
        p_table: Vec<u64>,
        p_arena: Vec<u8>,
        p_meta: Vec<(u64, u32, u64)>, // (off, len, hash)
        counts: Vec<u32>,
        fill: Vec<u32>,
        pid: Vec<u32>,
        ev_buf: Vec<GenEv>,
        cid_arena: Vec<u8>,
        cl_raw: Vec<(u32, GenClick)>,
        ck_cnt: Vec<u32>,
        ck_off: Vec<u32>,
        cl_buf: Vec<GenClick>,
        // Aggregate state
        camp: CampStore,
        writer: output::UserWriter<'a>,
        cat_sum: Vec<u64>,
        cat_touched: Vec<u32>,
        s_start: Vec<i64>,
        day_first: Vec<(i32, u32)>,
        stamp: u32,
        c_stamp: Vec<u32>,
        c_lo: Vec<i64>,
        c_hi: Vec<i64>,
        active: Vec<u32>,
        // Counters
        np: usize,
        nc: usize,
        written: usize,
    }

    impl<'a> GenWorker<'a> {
        #[inline(always)]
        fn flush_user_day(
            &mut self,
            id_bytes: &[u8],
            categories: &[Vec<u8>],
            day: i32,
            sessions: u32,
            events: u32,
            revenue: i64,
        ) {
            let mut top = NO_CATEGORY;
            let mut best = 0u64;
            for &c in &self.cat_touched {
                let v = unsafe { *self.cat_sum.get_unchecked(c as usize) };
                if v > best || (v == best && c < top) {
                    best = v;
                    top = c;
                }
                unsafe { *self.cat_sum.get_unchecked_mut(c as usize) = 0 };
            }
            self.cat_touched.clear();
            let cat = if top == NO_CATEGORY {
                None
            } else {
                Some(unsafe { categories.get_unchecked(top as usize) }.as_slice())
            };
            self.writer.push(id_bytes, day, sessions, events, revenue, cat);
        }

        #[inline(always)]
        fn count_interval(&mut self, camp: u32, lo: i64, hi: i64) {
            let st = &self.s_start;
            let df = &self.day_first;
            if st.len() == 1 {
                let s0 = unsafe { *st.get_unchecked(0) };
                if lo <= s0 && s0 <= hi {
                    self.camp.add_assisted(camp, unsafe { df.get_unchecked(0).0 }, 1);
                }
                return;
            }
            let a = st.partition_point(|&x| x < lo);
            let b = st.partition_point(|&x| x <= hi);
            if a >= b {
                return;
            }
            if df.len() == 1 {
                self.camp.add_assisted(camp, unsafe { df.get_unchecked(0).0 }, (b - a) as u32);
                return;
            }
            let mut di = df.partition_point(|d| (d.1 as usize) <= a) - 1;
            let mut idx = a;
            while idx < b {
                let end = if di + 1 < df.len() {
                    (df[di + 1].1 as usize).min(b)
                } else {
                    b
                };
                let day = df[di].0;
                self.camp.add_assisted(camp, day, (end - idx) as u32);
                idx = end;
                di += 1;
            }
        }

        #[inline]
        fn assists(&mut self, pcl: &[GenClick]) {
            let first = self.s_start[0];
            let last = *unsafe { self.s_start.last().unwrap_unchecked() };
            if pcl.len() == 1 {
                let c = unsafe { *pcl.get_unchecked(0) };
                let hi = c.ts_ms.saturating_add(WIN_MS);
                if c.ts_ms <= last && hi >= first {
                    self.count_interval(c.campaign, c.ts_ms, hi);
                }
                return;
            }
            self.stamp = self.stamp.wrapping_add(1);
            if self.stamp == 0 {
                self.c_stamp.fill(0);
                self.stamp = 1;
            }
            let stamp = self.stamp;
            for c in pcl {
                let t = c.ts_ms;
                if t > last {
                    break;
                }
                let hi = t.saturating_add(WIN_MS);
                if hi < first {
                    continue;
                }
                let ci = c.campaign as usize;
                if unsafe { *self.c_stamp.get_unchecked(ci) } == stamp {
                    let prev_hi = unsafe { *self.c_hi.get_unchecked(ci) };
                    if t <= prev_hi {
                        unsafe { *self.c_hi.get_unchecked_mut(ci) = hi };
                        continue;
                    }
                    let lo0 = unsafe { *self.c_lo.get_unchecked(ci) };
                    self.count_interval(c.campaign, lo0, prev_hi);
                } else {
                    unsafe { *self.c_stamp.get_unchecked_mut(ci) = stamp };
                    self.active.push(c.campaign);
                }
                unsafe {
                    *self.c_lo.get_unchecked_mut(ci) = t;
                    *self.c_hi.get_unchecked_mut(ci) = hi;
                }
            }
            let active = std::mem::take(&mut self.active);
            for &c in &active {
                let lo = unsafe { *self.c_lo.get_unchecked(c as usize) };
                let hi = unsafe { *self.c_hi.get_unchecked(c as usize) };
                self.count_interval(c, lo, hi);
            }
            self.active = active;
            self.active.clear();
        }
    }

    let q_order = &q_order;
    let ev_cnt_part = &ev_cnt_part;
    let b_by_part_ref = &b_by_part;
    let ck_by_part_ref = &ck_by_part;
    let ud_ref = &ud;
    let est_user_rows = ne / threads / 8 + 1024;
    let workers: Vec<GenWorker<'_>> = par::run_fini(
        threads,
        p2,
        |tid| GenWorker {
            p_table: Vec::new(),
            p_arena: Vec::with_capacity(65536),
            p_meta: Vec::with_capacity(4096),
            counts: Vec::with_capacity(4096),
            fill: Vec::with_capacity(4096),
            pid: Vec::with_capacity(65536),
            ev_buf: Vec::with_capacity(65536),
            cid_arena: Vec::with_capacity(65536),
            cl_raw: Vec::with_capacity(16384),
            ck_cnt: Vec::with_capacity(4096),
            ck_off: Vec::with_capacity(4096),
            cl_buf: Vec::with_capacity(16384),
            camp: CampStore::new(ncamp + 1, day0, ndays),
            writer: output::UserWriter::new(ud_ref, tid, est_user_rows),
            cat_sum: vec![0u64; ncat],
            cat_touched: Vec::with_capacity(64),
            s_start: Vec::with_capacity(256),
            day_first: Vec::with_capacity(64),
            stamp: 0,
            c_stamp: vec![0u32; ncamp],
            c_lo: vec![0i64; ncamp],
            c_hi: vec![0i64; ncamp],
            active: Vec::with_capacity(64),
            np: 0,
            nc: 0,
            written: 0,
        },
        |w, idx| {
            let q = q_order[idx];
            let b_bufs = std::mem::take(&mut *b_by_part_ref[q].lock().unwrap());
            let ck_bufs = std::mem::take(&mut *ck_by_part_ref[q].lock().unwrap());
            let n = ev_cnt_part[q];
            if n == 0 {
                drop(b_bufs);
                drop(ck_bufs);
                return;
            }

            // 1. Intern unique persons in partition `q` + record per-event `pid`.
            let mut cap = ((n / 12).max(16) * 2).next_power_of_two();
            if w.p_table.len() < cap {
                w.p_table.resize(cap, 0);
            }
            w.p_table[..cap].fill(0);
            let mut mask = cap - 1;
            w.p_arena.clear();
            w.p_meta.clear();
            w.counts.clear();
            if w.pid.capacity() < n {
                w.pid.reserve(n - w.pid.len());
            }
            unsafe { w.pid.set_len(n) };
            let pid_ptr = w.pid.as_mut_ptr();
            let mut j = 0usize;
            for sb in &b_bufs {
                for sl in sb.slabs() {
                    let base = sl.as_ptr();
                    let end_ptr = unsafe { base.add(sl.len()) };
                    let mut ptr = base;
                    while ptr < end_ptr {
                        let is_purch = unsafe { *ptr };
                        let ph = unsafe { rd_u64(ptr.add(1)) };
                        let (plen_u, pstart) = unsafe { rd_len(ptr.add(17)) };
                        let plen = plen_u as u32;
                        let person = unsafe { std::slice::from_raw_parts(pstart, plen_u) };
                        ptr = unsafe { pstart.add(plen_u + if is_purch != 0 { 8 } else { 0 }) };

                        let fp = (ph >> 32) as u32;
                        let mut slot = (ph as usize) & mask;
                        let id = loop {
                            let v = unsafe { *w.p_table.get_unchecked(slot) };
                            if v == 0 {
                                let id = w.p_meta.len() as u32;
                                let aoff = w.p_arena.len() as u64;
                                w.p_arena.extend_from_slice(person);
                                w.p_meta.push((aoff, plen, ph));
                                w.counts.push(0);
                                unsafe { *w.p_table.get_unchecked_mut(slot) = ((fp as u64) << 32) | (id as u64 + 1) };
                                if w.p_meta.len() * 2 > cap {
                                    cap *= 2;
                                    mask = cap - 1;
                                    if w.p_table.len() < cap {
                                        w.p_table.resize(cap, 0);
                                    }
                                    w.p_table[..cap].fill(0);
                                    for (pi, &(_, _, mh)) in w.p_meta.iter().enumerate() {
                                        let mfp = (mh >> 32) as u32;
                                        let mut ms = (mh as usize) & mask;
                                        while unsafe { *w.p_table.get_unchecked(ms) } != 0 {
                                            ms = (ms + 1) & mask;
                                        }
                                        unsafe {
                                            *w.p_table.get_unchecked_mut(ms) = ((mfp as u64) << 32) | (pi as u64 + 1);
                                        }
                                    }
                                }
                                break id;
                            }
                            if (v >> 32) as u32 == fp {
                                let pi = (v as u32) - 1;
                                let (moff, mlen, _) = unsafe { *w.p_meta.get_unchecked(pi as usize) };
                                if mlen == plen
                                    && unsafe {
                                        w.p_arena.get_unchecked(moff as usize..moff as usize + mlen as usize)
                                    } == person
                                {
                                    break pi;
                                }
                            }
                            slot = (slot + 1) & mask;
                        };
                        unsafe {
                            *w.counts.get_unchecked_mut(id as usize) += 1;
                            pid_ptr.add(j).write(id);
                        }
                        j += 1;
                    }
                }
            }
            let np = w.p_meta.len();
            w.np += np;

            // 2. Scatter events into `ev_buf` grouped by person, then immediately unmap `b_bufs`!
            w.fill.clear();
            w.fill.reserve(np);
            let mut acc = 0u32;
            for i in 0..np {
                w.fill.push(acc);
                acc += w.counts[i];
            }
            if w.ev_buf.capacity() < n {
                w.ev_buf.reserve(n - w.ev_buf.len());
            }
            unsafe { w.ev_buf.set_len(n) };
            let ev_ptr = w.ev_buf.as_mut_ptr();
            let mut j = 0usize;
            for sb in &b_bufs {
                for sl in sb.slabs() {
                    let base = sl.as_ptr();
                    let end_ptr = unsafe { base.add(sl.len()) };
                    let mut ptr = base;
                    while ptr < end_ptr {
                        let is_purch = unsafe { *ptr };
                        let ts_ms = unsafe { rd_i64(ptr.add(9)) };
                        let (plen, pstart) = unsafe { rd_len(ptr.add(17)) };
                        let after_p = unsafe { pstart.add(plen) };
                        let (cat, cents) = if is_purch != 0 {
                            ptr = unsafe { after_p.add(8) };
                            unsafe { (rd_u32(after_p), rd_u32(after_p.add(4))) }
                        } else {
                            ptr = after_p;
                            (0, 0)
                        };
                        let id = unsafe { *pid_ptr.add(j) } as usize;
                        let pos = unsafe { *w.fill.get_unchecked(id) } as usize;
                        unsafe {
                            ev_ptr.add(pos).write(GenEv { ts_ms, cat, cents });
                            *w.fill.get_unchecked_mut(id) = (pos + 1) as u32;
                        }
                        j += 1;
                    }
                }
            }
            drop(b_bufs);

            // 3. Collect clicks for persons present in partition `q`, then immediately unmap `ck_bufs`!
            w.cl_raw.clear();
            w.cid_arena.clear();
            w.ck_cnt.clear();
            w.ck_cnt.resize(np, 0);
            for sb in &ck_bufs {
                for sl in sb.slabs() {
                    let base = sl.as_ptr();
                    let end_ptr = unsafe { base.add(sl.len()) };
                    let mut ptr = base;
                    while ptr < end_ptr {
                        let ph = unsafe { rd_u64(ptr) };
                        let ts_ms = unsafe { rd_i64(ptr.add(8)) };
                        let campaign = unsafe { rd_u32(ptr.add(16)) };
                        let (plen_u, q1) = unsafe { rd_len(ptr.add(20)) };
                        let (clen_u, pstart) = unsafe { rd_len(q1) };
                        let plen = plen_u as u32;
                        let clen = clen_u as u32;
                        let person = unsafe { std::slice::from_raw_parts(pstart, plen_u) };
                        let cid = unsafe { std::slice::from_raw_parts(pstart.add(plen_u), clen_u) };
                        ptr = unsafe { pstart.add(plen_u + clen_u) };

                        let fp = (ph >> 32) as u32;
                        let mut slot = (ph as usize) & mask;
                        let found = loop {
                            let v = unsafe { *w.p_table.get_unchecked(slot) };
                            if v == 0 {
                                break None;
                            }
                            if (v >> 32) as u32 == fp {
                                let pi = (v as u32) - 1;
                                let (moff, mlen, _) = unsafe { *w.p_meta.get_unchecked(pi as usize) };
                                if mlen == plen
                                    && unsafe {
                                        w.p_arena.get_unchecked(moff as usize..moff as usize + mlen as usize)
                                    } == person
                                {
                                    break Some(pi);
                                }
                            }
                            slot = (slot + 1) & mask;
                        };
                        if let Some(pid) = found {
                            let cid_off = w.cid_arena.len() as u64;
                            w.cid_arena.extend_from_slice(cid);
                            w.cl_raw.push((pid, GenClick { ts_ms, campaign, cid_len: clen, cid_off }));
                            unsafe { *w.ck_cnt.get_unchecked_mut(pid as usize) += 1 };
                        }
                    }
                }
            }
            drop(ck_bufs);

            let ncl_raw = w.cl_raw.len();
            w.ck_off.clear();
            if ncl_raw > 0 {
                w.ck_off.reserve(np + 1);
                let mut cacc = 0u32;
                for i in 0..np {
                    w.ck_off.push(cacc);
                    cacc += w.ck_cnt[i];
                }
                w.ck_off.push(cacc);
                w.fill.clear();
                w.fill.extend_from_slice(&w.ck_off[..np]);
                if w.cl_buf.capacity() < ncl_raw {
                    w.cl_buf.reserve(ncl_raw - w.cl_buf.len());
                }
                unsafe { w.cl_buf.set_len(ncl_raw) };
                let cl_ptr = w.cl_buf.as_mut_ptr();
                for &(id, ck) in &w.cl_raw {
                    let pos = unsafe { *w.fill.get_unchecked(id as usize) } as usize;
                    unsafe {
                        cl_ptr.add(pos).write(ck);
                        *w.fill.get_unchecked_mut(id as usize) = (pos + 1) as u32;
                    }
                }
            }

            // 4. Per-person sort + sessionize + credit + assists + streaming Parquet output.
            let mut ev_st = 0usize;
            for pi in 0..np {
                let ec = w.counts[pi] as usize;
                let ev_en = ev_st + ec;
                let ev_sl = &mut w.ev_buf[ev_st..ev_en];
                if ec > 1 {
                    if ec <= 16 {
                        for i in 1..ec {
                            let v = ev_sl[i];
                            let mut j = i;
                            while j > 0 && ev_sl[j - 1].ts_ms > v.ts_ms {
                                ev_sl[j] = ev_sl[j - 1];
                                j -= 1;
                            }
                            ev_sl[j] = v;
                        }
                    } else {
                        ev_sl.sort_unstable_by_key(|e| e.ts_ms);
                    }
                }

                let (cl_st, cl_len) = if ncl_raw > 0 {
                    let cs = w.ck_off[pi] as usize;
                    let ce = w.ck_off[pi + 1] as usize;
                    let cl_n = ce - cs;
                    if cl_n > 1 {
                        let cid_arena = &w.cid_arena;
                        let cl_sl = &mut w.cl_buf[cs..ce];
                        cl_sl.sort_unstable_by(|a, b| {
                            a.ts_ms.cmp(&b.ts_ms).then_with(|| {
                                let ca = unsafe {
                                    cid_arena.get_unchecked(a.cid_off as usize..a.cid_off as usize + a.cid_len as usize)
                                };
                                let cb = unsafe {
                                    cid_arena.get_unchecked(b.cid_off as usize..b.cid_off as usize + b.cid_len as usize)
                                };
                                ca.cmp(cb)
                            })
                        });
                        let mut wr = 1usize;
                        for r in 1..cl_n {
                            let prev = cl_sl[wr - 1];
                            let cur = cl_sl[r];
                            let same = prev.cid_len == cur.cid_len
                                && unsafe {
                                    cid_arena.get_unchecked(
                                        prev.cid_off as usize..prev.cid_off as usize + prev.cid_len as usize,
                                    ) == cid_arena.get_unchecked(
                                        cur.cid_off as usize..cur.cid_off as usize + cur.cid_len as usize,
                                    )
                                };
                            if !same {
                                cl_sl[wr] = cur;
                                wr += 1;
                            }
                        }
                        w.nc += wr;
                        (cs, wr)
                    } else {
                        w.nc += cl_n;
                        (cs, cl_n)
                    }
                } else {
                    (0, 0)
                };

                let (poff, plen, _) = w.p_meta[pi];
                // SAFETY: `p_arena` is immutable for the rest of partition `q`.
                let id_bytes: &[u8] = unsafe {
                    std::slice::from_raw_parts(w.p_arena.as_ptr().add(poff as usize), plen as usize)
                };

                w.s_start.clear();
                w.day_first.clear();
                let mut i = ev_st;
                let mut cj = cl_st;
                let cl_end = cl_st + cl_len;
                let mut grp_first = cl_st;
                let mut last_ts = 0i64;
                let mut have_click = false;
                let mut ud_day = i32::MIN;
                let mut cur_day_lo = i64::MAX;
                let mut cur_day_hi = i64::MIN;
                let (mut ud_sessions, mut ud_events, mut ud_revenue) = (0u32, 0u32, 0i64);

                while i < ev_en {
                    let s = unsafe { w.ev_buf.get_unchecked(i).ts_ms };
                    let day = if s >= cur_day_lo && s < cur_day_hi {
                        ud_day
                    } else {
                        let d = aggregate::day_of(s);
                        cur_day_lo = (d as i64).saturating_mul(ts::DAY_MS);
                        cur_day_hi = cur_day_lo.saturating_add(ts::DAY_MS);
                        d
                    };
                    if day != ud_day {
                        if ud_sessions != 0 {
                            w.flush_user_day(id_bytes, categories, ud_day, ud_sessions, ud_events, ud_revenue);
                        }
                        ud_day = day;
                        ud_sessions = 0;
                        ud_events = 0;
                        ud_revenue = 0;
                        w.day_first.push((day, w.s_start.len() as u32));
                    }
                    let mut prev = s;
                    let mut k = i;
                    let mut rev = 0u64;
                    while k < ev_en {
                        let e = unsafe { *w.ev_buf.get_unchecked(k) };
                        if e.ts_ms.saturating_sub(prev) > GAP_MS {
                            break;
                        }
                        prev = e.ts_ms;
                        if e.cents != 0 {
                            rev += e.cents as u64;
                            let slot = unsafe { w.cat_sum.get_unchecked_mut(e.cat as usize) };
                            if *slot == 0 {
                                w.cat_touched.push(e.cat);
                            }
                            *slot += e.cents as u64;
                        }
                        k += 1;
                    }
                    let nev = (k - i) as u32;
                    i = k;

                    while cj < cl_end {
                        let ct = unsafe { w.cl_buf.get_unchecked(cj).ts_ms };
                        if ct > s {
                            break;
                        }
                        if !have_click || ct != last_ts {
                            last_ts = ct;
                            grp_first = cj;
                            have_click = true;
                        }
                        cj += 1;
                    }
                    let credit = if have_click && s.saturating_sub(last_ts) <= WIN_MS {
                        unsafe { w.cl_buf.get_unchecked(grp_first).campaign }
                    } else {
                        organic
                    };
                    w.camp.add_credit(credit, day, rev as i64);

                    ud_sessions += 1;
                    ud_events += nev;
                    ud_revenue += rev as i64;
                    w.s_start.push(s);
                }
                w.flush_user_day(id_bytes, categories, ud_day, ud_sessions, ud_events, ud_revenue);

                if cl_len > 0 {
                    let cl_ptr = w.cl_buf.as_ptr();
                    let pcl = unsafe { std::slice::from_raw_parts(cl_ptr.add(cl_st), cl_len) };
                    w.assists(pcl);
                }
                ev_st = ev_en;
            }
        },
        |w| {
            w.written = w.writer.finish();
        },
    );
    drop(b_by_part);
    drop(ck_by_part);
    let np: usize = workers.iter().map(|w| w.np).sum();
    let nc: usize = workers.iter().map(|w| w.nc).sum();
    let user_rows_written: usize = workers.iter().map(|w| w.written).sum();
    if user_rows_written == 0 {
        output::write_empty_user(&ud).expect("writing empty user_daily");
    }
    let camp_rows = aggregate::merge_camps(workers.into_iter().map(|w| w.camp));
    tm.lap("group+agg");
    let t_cw = Instant::now();
    output::write_campaign_raw(&cd, &camp_rows, &campaigns).expect("writing campaign_daily");
    tm.lap("campaign-write");
    eprintln!(
        "[stage] campaign_write {:.3}s (user rows {user_rows_written}, campaign rows {})",
        t_cw.elapsed().as_secs_f64(),
        camp_rows.len()
    );
    eprintln!("[generic] persons={np} events={ne} (raw {ne_raw}) clicks={nc}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_bytes_basic_and_lengths() {
        let mut seen = std::collections::HashSet::new();
        for len in 0..=64 {
            let s: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
            let h = hash_bytes(&s);
            assert!(seen.insert(h), "collision at len {len}");
        }
        // Single-bit differences in long IDs produce distinct hashes.
        let base = b"eu0pbkzx-ff0qwpy-w6k3pn6bfr05u_cyv8ztvt".to_vec();
        let hb = hash_bytes(&base);
        for i in 0..base.len() {
            let mut m = base.clone();
            m[i] ^= 1;
            assert_ne!(hash_bytes(&m), hb, "bit flip at {i} collided");
        }
    }

    #[test]
    fn slab_buf_roundtrip_heap_and_mmap() {
        for &cap in &[4096usize, 64 << 10] {
            let mut sb = SlabBuf::new(cap);
            let mut expected = Vec::new();
            for i in 0..2000u32 {
                let len = if i == 500 { (cap * 2) + 17 } else { 37 };
                let p = sb.alloc_raw(len);
                let byte = (i & 0xFF) as u8;
                unsafe { std::ptr::write_bytes(p, byte, len) };
                expected.push((len, byte));
            }
            assert_eq!(sb.count, expected.len());
            let mut idx = 0;
            for sl in sb.slabs() {
                let mut off = 0;
                while off < sl.len() {
                    let (len, byte) = expected[idx];
                    assert!(sl[off..off + len].iter().all(|&b| b == byte));
                    off += len;
                    idx += 1;
                }
            }
            assert_eq!(idx, expected.len());
        }
    }

    #[test]
    fn len_encoding_roundtrip() {
        let mut buf = [0u8; 16];
        for &len in &[0usize, 1, 24, 40, 254, 255, 256, 65_535, 3_000_000] {
            let end = unsafe { wr_len(buf.as_mut_ptr(), len) };
            assert_eq!(unsafe { end.offset_from(buf.as_ptr()) } as usize, len_enc_size(len));
            let (got, end2) = unsafe { rd_len(buf.as_ptr()) };
            assert_eq!(got, len);
            assert_eq!(end2 as usize, end as usize);
        }
    }
}
