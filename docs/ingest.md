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

## Pipeline (fast path)

| Stage | What | Data |
| --- | --- | --- |
| small | manifest, products, campaigns; file list; 1 MiB chunk plan; async `posix_fadvise(WILLNEED)` thread over all CSVs (links first) | |
| links-parse | parallel chunk parse of `links/*.csv` | `(K,K)` pairs per thread |
| links-uf | endpoints scattered to 256 hash partitions; per-partition interning; lock-free parallel union-find (link larger index under smaller, CAS path halving); smallest key per root by CAS; per-partition map `id -> canonical`, holding only ids whose canonical differs | `Links` |
| parse | events and clicks, 1 MiB LF-aligned chunks read with `pread` into a thread-local padded buffer, work-stealing over chunks | events: `RecA` 32 B into 2048 partitions by hash(event_id); valid clicks: `RecC` 32 B into 1024 partitions by hash(canonical person) |
| dedup | per event partition: open-addressing table of `fp32 | idx`, full-key verify, keep min `ingest`; winners that are valid resolve the canonical person (batched prefetch) | `RecB` 16 B into 1024 person partitions |
| group | per person partition: person map, counts, scatter directly into the final `events` array at the partition's base, sort each person by ts; clicks: person lookup (drop persons without events), sort by (person, ts, click key), drop adjacent duplicate click ids | final arrays |
| assemble | offsets, clicks, canonical id bytes (`StringTable`) | `Loaded` |

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
* Sinks are per-thread huge-page arenas (`MADV_HUGEPAGE`) with fixed per-partition regions
  (1.5x the estimate) and a heap overflow vector. They are written with non-temporal
  16-byte stores (`sfence` before hand-off) and unmapped when dropped.

## Stage timings

stderr reports `[ingest] <stage> <wall>s cpu <summed thread cpu>s`.

Measured on the shared dev box: a 16-thread Ryzen 7 8845HS, `taskset -c 0-7`, `NCPU=8`, warm
page cache. The box is shared with about 12 other agents (load average 15–50), so numbers are
noisy. Use the best of several runs.

| Dataset | links | parse | dedup | group | assemble | ingest total | pipeline total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| seed20-20m (1.87 GB, 20.9M raw events) | 0.10 | 0.56 | 0.23 | 0.15 | 0.02 | 1.09 | 1.25 |
| seed60-60m (5.7 GB, 64.4M raw events) | see below | | | | | | |

At 60M on this box, the 5.4 GB input does not stay in page cache (about 7 GB available
memory with the other agents), so parse is I/O-bound (8 s). The scorer has 24 GB.

Single-thread costs (`INGEST_BENCH2=<events csv>`, CPU time per row, about 77 B/row): scan
36 ns, plus both timestamps 28 ns, plus keys 6 ns, plus product lookup 10 ns. A full row with
the sink push is 100–115 ns warm.

## Knobs

* `INGEST_SLOW=1` forces the generic path.
* `INGEST_DUMP=<file>` writes a canonical dump.
* `INGEST_NO_PREFETCH=1` disables the fadvise thread.
* `INGEST_POPULATE=1` pre-faults sink arenas with `MADV_POPULATE_WRITE`.
* `INGEST_BENCH=<csv>` and `INGEST_BENCH2=<csv>` run single-thread microbenchmarks.
