# Ingest: bytes on disk -> person-grouped events and clicks

Owner: ingest agent. Code: `src/ingest/`. Contract: `docs/INTERFACE.md`.

## Paths and fallbacks

`ingest::load` runs a chain of exact implementations; each later one handles strictly more inputs:

1. `fast::run::<u64>`: **compact keys**. Every id (event, user, click, link ids) must be
   1..=10 chars of `[0-9A-Za-z]`. It is packed 6 bits per char, first char in the top bits,
   code 0 = padding (`keys.rs`). The encoding is injective and order-preserving (byte-wise),
   so key equality is byte equality and key order is byte order. This is not a hash, so no
   collisions are possible.
2. `fast::run::<u128>`: **wide keys**. Any id of at most 15 bytes (bytes big-endian plus a
   length byte, `key.rs`). It is also exact and order-preserving. Retried automatically when
   (1) meets an id it cannot encode, for example the fixture ids with spaces.
3. `slow::load`: a **generic byte-string** path (std `HashMap<Vec<u8>, _>`, single thread).
   It is used when an id is longer than 15 bytes, or a representation limit of the compact
   records is exceeded. The limits are: the window plus 7 days does not fit in u32 ms; more
   than 256 categories; an `ingest_ts` that is not whole seconds in [1970, 2106). Rules say
   `ingest_ts` is always F1. `INGEST_SLOW=1` forces this path. It is the in-repo
   differential oracle.

Only ids that are actually needed are encoded. These are the event_id of every
well-formed row, the user_id of rows that pass validation, and the click/user ids of valid
clicks. Product and campaign ids go through `IdLookup`, an exact dense array for
`<prefix><decimal without leading zero>` (e.g. `p123`, `c_45`) plus an exact hash map for
anything else. So odd product ids never force a fallback.

`INGEST_DUMP=<file>` writes a canonical text dump of `Loaded`. Fast and slow dumps are
byte-identical on dev.

## Pipeline (fused fast path)

`ingest::run_pipeline` runs the fused fast path (`fast::run_to_output`) directly from CSVs to Parquet without materializing `Loaded`. `INGEST_SLOW=1`, `INGEST_DUMP=<file>`, or any input outside the compact representation (`>15B` IDs, `>256` categories, window `>42d`, non-F1 `ingest_ts` outside `[1970, 2106)`) automatically falls back to `fast::load` / `slow::load` + `aggregate::run`.

| Stage | What | Data |
| --- | --- | --- |
| small | manifest, products (`u8`/`u32` dense `IdLookup` + inline/HashMap fallback), campaigns; file list; 256 KiB chunk plan; up to 4 parallel async `posix_fadvise(WILLNEED)` prefetch threads (16 MiB segments, links first, stopped after `parse`) | |
| links-parse | parallel chunk parse of `links/*.csv` | `(K,K)` pairs per thread |
| links-uf | flat 2-pass count/scatter to 256 hash partitions; per-partition interning (reused map); parallel `keys`/`parent`/`best` init; lock-free parallel union-find (link larger index under smaller, CAS path halving); smallest key per root by CAS; ONE 2 MiB huge-page-backed flat partitioned table `id -> canonical` (zero pointer chasing, ~100% 2M dTLB hit rate) | `Links` |
| parse | events and clicks, 256 KiB LF-aligned chunks read with `pread` into L2-hot thread-local padded buffers (no redundant zeroing), L1/L2-tiled branchless SIMD scanner, L1 `DateTable` + specialized 13-digit F3, `pshufb` `u64::enc`; skips `prod.get` on non-purchases (`view`/`cart`); `RecA` carries `eid_hash: u32` in its 4-byte pad; valid clicks resolve canonical person via continuous ring-32 prefetcher | events: `RecA` 32 B into dynamic `P1` (256/1024/2048) 256B SWCB sinks; valid clicks: `RecC` 32 B into dynamic `P2` (256/512/1024) 256B SWCB sinks |
| dedup | per `P1` partition: compact L2-resident `u32` table (`fp14 | idx18`, `u64` fallback >=262k) using precomputed `RecA::eid_hash` + L1 `dead` bitset; 100% sequential Pass 2 resolving canonical persons via continuous ring-32 prefetch into flat huge-page `Links` | `RecB` 16 B into `P2` 256B SWCB sinks |
| group+agg | per `P2` partition (largest first): L2-resident `CompactEv(u64)` `(ts<<32)|cc` scatter + insertion/driftsort/3-pass LSB radix sort + per-person sessionize/credit/assists + direct streaming `UserWriter` Parquet output + per-worker `CampStore` merged into `campaign_daily` | `user_daily/*.parquet`, `campaign_daily/part-0.parquet` |

Details that matter for exactness:

* **CSV** (`scan.rs`): AVX2 masks for `,` `\n` `"` per 64 bytes. Stage 1 flattens all
  separator positions, and records the running quote count at every LF. Stage 2 walks rows. A
  row is well-formed in count iff its N-th separator is its own LF. A field is *wrapped* if
  its length is at least 2 and it starts and ends with `"`. The row is accepted iff its quote
  count equals 2 x wrapped fields, which means no quote exists outside the wrapping pairs.
  `scan_ref` (the older single-pass scanner) and `csv::split` are the references. Unit tests
  include a randomized equivalence test.
* **Chunks** (`read_chunk`): a chunk owns the rows that *start* inside it. It reads from
  `start-1` to find the first row start, and extends past its end to the LF of its last row.
  The header is skipped only at file offset 0. A final row without LF gets a synthetic LF.
* **Timestamps** (`tsfast.rs`): SWAR validation and parsing of F1/F2. F3 uses an 8-digit SWAR
  parse; longer F3 strings use the reference `ts::parse_f3`, which treats overflow as out of
  window. 2M fuzzed strings plus a calendar sweep are checked against `ts::parse_ts`.
* **Dedup before validation**: every well-formed row with a non-empty event_id becomes a
  candidate carrying its own validity, including the late check. The candidate with min
  `ingest` wins. If several candidates tie on `ingest`, the first one wins; rules say tied
  copies are identical. An invalid winner drops the event.
* **Clicks**: invalid clicks are dropped at parse. Rules say copies of a click_id are
  identical, so validity cannot differ between copies. Dedup is exact after sorting by
  (person, ts, click key): equal click ids are adjacent.
* **Records**: `ts` is u32 ms relative to `window_start` for events, or to `window_start - 7d`
  for clicks. `ingest` is u32 seconds + 1. `cat << 24 | cents` (cents <= 10^7 < 2^24).
* Sinks are per-thread huge-page arenas (`MADV_HUGEPAGE`, 1.25x estimate) fronted by L2-resident
  256-byte software write-combining buffers (SWCBs) that flush 4 full cache lines at a time via
  8x `_mm256_stream_si256` non-temporal stores (`sfence` in parallel `run_fini`), plus heap
  overflow vectors. Unmapped immediately when dropped.

## Stage timings

stderr reports `[ingest] <stage> <wall>s cpu <summed thread cpu>s`.

See `bench/RESULTS.md` for full Docker and native A/B benchmarks on `seed20-20m` (1.87 GB) and
`seed60-60m` (5.72 GB).

Single-thread costs (`INGEST_BENCH2=<events csv>`, CPU time per row, ~77 B/row): scan
23–27 ns (~2.8–3.4 GB/s), plus both timestamps ~18 ns, plus keys ~6 ns, full `parse_event` +
SWCB push ~69 ns/row (>1.1 GB/s single-core).

## Knobs

* `INGEST_SLOW=1` forces the generic path.
* `INGEST_DUMP=<file>` writes a canonical dump.
* `INGEST_NO_PREFETCH=1` disables the fadvise thread.
* `INGEST_POPULATE=1` pre-faults sink arenas with `MADV_POPULATE_WRITE`.
* `INGEST_BENCH=<csv>` and `INGEST_BENCH2=<csv>` run single-thread microbenchmarks.
