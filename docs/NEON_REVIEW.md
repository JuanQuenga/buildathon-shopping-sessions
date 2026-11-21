# Technical Review & Architecture Decision (`NEON_REVIEW`)

Reviewed commit: `8ab1414` (2026-11-20).

## 1. Full Verification Matrix on `8ab1414`

Everything was re-built from scratch and re-verified (`cargo test --release`, native binary, and `linux/amd64` Docker image `ss-test:8ab1414442` under scorer flags `--network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 --user 1000:1000 --cpuset-cpus 0-7 --memory 24g --memory-swap 24g`):

| Check | Result | Notes |
| --- | --- | --- |
| Rust unit tests (`cargo test --release`) | **11/11 PASS** | Includes 2M-iteration `tsfast` fuzz vs `ts::parse_ts` and randomized `scan` vs `scan_ref` |
| Official fixtures (`run_fixtures.py`) | **36/36 PASS** | Verified both native and Docker (`ss-test:8ab1414442`) |
| Adversarial suite (`run_adversarial.sh`) | **32/32 PASS** | 27 small + 5 big (`big_many_parts`, `big_very_long_ids`, `big_heavy_user`, `big_bursty_user`, `big_dev_injected`) |
| Public dev answer (`seed 1`, 1M) | **MATCH** | Native (0.37s) and Docker (0.33s); all 9 `bench/invariants.py` checks pass |
| 1M seeds 2–7 + 1 random 1M seed vs `ref.py` | **7/7 MATCH** | Exact row-set, multiset, and schema match |
| `seed20-20m` (20.9M raw / 19.55M kept events) vs `oracle2` | **MATCH** | 2,247,046 user_daily rows, 28,014 campaign_daily rows |
| `seed60-60m` (64.4M raw / 58.88M kept events) vs `oracle2` | **MATCH** | 6,629,160 user_daily rows, 28,014 campaign_daily rows, 10,021,637 sessions, 19,706,007,920 revenue cents |
| Cross-reader Parquet validation (`dev` + `all_empty_output`) | **PASS** | Polars 2.1.0, PyArrow 26.0.1 (`read_table` + `ParquetFile.metadata`), DuckDB 2.0.1 agree on row counts & types (`string`, `date32[day]`, `int64`) |

## 2. Independent Rule Audit (`rules.md` vs `ref.py` vs `src/`)

Audited `rules.md`, all 36 fixture specs, `reference/ref.py`, and every module under `src/`:

- **CSV dialect (`scan.rs`, `csv.rs`)**: Exact. The quote-count identity (`qc - q_prev == 2 * wrapped_fields`) is mathematically sound: every field has `>= 2 * wrapped` quotes with equality iff all quotes are outer wrapping pairs. `\r` is preserved as data; empty lines and wrong field counts are dropped; chunk splitting (`read_chunk`) owns rows starting in `[c.start, c.end)` and handles lines > 1 MiB and EOF without trailing LF.
- **Timestamps (`ts.rs`, `tsfast.rs`)**: Exact for F1, F2, F3 (including leading zeros, >64-bit F3 overflow -> out-of-window without wrap, ASCII-only digits, `-00:00`, Gregorian leap years 2028/2100, inclusive/exclusive window bounds).
- **Dedup, links, sessions, attribution, assists (`fast.rs`, `slow.rs`, `aggregate/mod.rs`)**: Exact. Lock-free parallel union-find (`uf_union` linking higher index under lower with CAS + path halving) is acyclic and linearizable; dedup before validation is exact (`RecA` carries `INVALID` sentinel when invalid); click dedup after sorting by `(person, ts, click_id)` is exact under the identical-click-copies rule; assists interval-union + binary-search counting is exact and `O(clicks * log sessions)`.

### Subtle Gaps / Hardening Risks Identified

1. **Unparseable `ingest_ts` mismatch (`fast.rs` / `slow.rs` vs `ref.py`)**: `rules.md` guarantees `ingest_ts` is always valid F1, so real data is unaffected, but `ref.py` maps unparseable `ingest_ts` to `+inf` (dropped as late) whereas `fast.rs`/`slow.rs` map it to `0`/`i64::MIN` (wins dedup and skips late check). Worth aligning with `ref.py` (`u32::MAX` + invalid).
2. **Slow single-threaded fallback risk (`slow::load`)**: Currently, any single `product_id` or `campaign_id` > 15 bytes forces `IdLookup::build` to return `None`, and any single `user_id`/`event_id`/`click_id` > 15 bytes (or > 256 categories or > 42-day window) falls back to single-threaded `slow::load` (`HashMap<Vec<u8>, _>`), which would OOM (> 24 GB) or exceed 180s at 60M. While the published generator emits 9-byte base36 IDs (100% handled by `u64` keys), hardening `IdLookup` to accept arbitrary-length keys in `rest` (and widening `categories` / ensuring fallback resilience) removes an avoidable tail risk.
3. **Parquet writer edge cases (`src/output/mod.rs`, `src/output/parquet.rs`)**:
   - `write_user_cols` writes a single row group / data page per worker without splitting on `MAX_ROWS` or checking `page_size <= i32::MAX`. Pathological long IDs or `NCPU=1` could panic in `page_header`.
   - When `num_rows == 0`, `write_columns` emits 8 empty files (one per worker) and writes unused empty DataPage headers before the footer. Better to skip empty worker files (emitting a single clean 0-row `part-0.parquet` with no orphan pages only if total rows == 0).
4. **File discovery (`small::list_csv`)**: Uses `Path::extension() == Some("csv")`, which ignores a dotfile named `.csv`; checking filename bytes `.ends_with(b".csv")` matches `rules.md` (`*.csv`) and `ref.py`.
5. **CPU feature safety**: `.cargo/config.toml` sets `-C target-cpu=x86-64-v3` (AVX2/BMI2/FMA). This is strictly a subset of GCP N2 Cascade Lake (`x86-64-v4` + VNNI) and avoids SIGILL from host-only Zen 4 extensions (VBMI/VBMI2/VPOPCNTDQ/BF16). Any AVX-512 code, if ever tested, must be restricted strictly to Cascade Lake features (`avx512f,avx512cd,avx512bw,avx512dq,avx512vl`).

## 3. Performance Baseline & Bottleneck Analysis (`8ab1414`)

Measured on the shared dev box (AMD Ryzen 7 8845HS, 16 threads, 28 GB RAM, NVMe/btrfs; load average ~15 during Docker runs, ~18–36 during earlier native runs), pinned to 8 threads (`--cpuset-cpus 0-7`, `NCPU=8`, `MEM=24g`):

### Docker timings (`bench/run.sh`, scorer flags, `ss-test:8ab1414442`)

| Dataset | Mode | Docker Wall (s) | In-process (s) | Peak RSS | `links` | `parse` | `dedup` | `group` | `assemble` | `agg+write` |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `seed20-20m` (1.87 GB) | cold (3 runs) | 1.955 / **2.155** / 2.383 | 1.479 / 1.691 / 1.884 | 1.81 GB | 0.10s | 1.00–1.22s | 0.16–0.25s | 0.12–0.17s | 0.01s | 0.06–0.08s |
| `seed20-20m` (1.87 GB) | warm (3 runs) | 1.627 / **1.579** / 1.313 | 1.165 / 1.154 / 0.930 | 1.81 GB | 0.07s | 0.52–0.62s | 0.15–0.22s | 0.11–0.15s | 0.01s | 0.05–0.07s |
| `seed60-60m` (5.72 GB) | cold (2 runs) | 4.905 / 4.241 | 4.517 / 3.822 | 5.21 GB | 0.20s | 2.40–3.03s | 0.67s | 0.31–0.35s | 0.03s | 0.18s |
| `seed60-60m` (5.72 GB) | warm (2 runs) | 3.011 / **2.886** | 2.581 / 2.475 | 5.25 GB | 0.17s | 1.23–1.27s | 0.54–0.55s | 0.30–0.31s | 0.03s | 0.18s |

Docker container start/stop overhead is ~0.41s locally (`debian:bookworm-slim`, 116 MB).

### Native CPU-time breakdown (`seed60-60m` warm, sum across 8 threads = 22.1s CPU) & `INGEST_BENCH2`

- `parse`: **12.05s CPU (55%)**, **1.23s warm / 2.40–3.03s cold wall**.
  - Single-thread `INGEST_BENCH2` (warm arena, `part-00004.csv`): `scan` = **28.3 ns/row**, `2× ts` = **20.1 ns/row**, `2× keys` = **5.1 ns/row**, `prod.get` = **5.7 ns/row**, full `parse_event` = **68.2 ns/row** (109.7 ns/row on round 0 due to arena page faults).
  - Real `parse` is ~151 ns/row CPU across 79.7M rows because of:
    1. `read_chunk` zero-fills 1 MiB (`buf.resize(..., 0)`) before every `pread` (~5.7 GB wasted memset + 5.7 GB kernel->user copy).
    2. `Sinks::push` issues random 16-byte `_mm_stream_si128` stores across **2048** (`RecA`) and **1024** (`RecC`) partitions directly, thrashing the CPU's ~10–12 write-combining buffers (partial-line DRAM writes!) and 2 MB dTLB.
    3. `civil19` recomputes full calendar arithmetic (`days_from_civil` with 64-bit division) and `ingest_ms / 1000` on every row despite only ~26 distinct `YYYY-MM-DD` dates in a 14-day window.
    4. `scan_with` spills `buf` (1 MiB) + `seps` (400 KB) + `lfs` (100 KB) out of 1 MiB L2 cache, checks quotes on all 7 fields even when `qc == q_prev`, and has an 8-deep serial `blsr` chain.
    5. `parse_click` does unprefetched `links.canonical(user)` lookups against a 58 MB map at 60M (15M DRAM cache misses).
    6. Cold I/O: single-threaded blocking `fadvise_willneed` thread + `pread` copies instead of `mmap`/direct async streaming.
- `dedup`: **5.12s CPU (23%)**, **0.54s warm wall**.
  - `table` (8B/entry = 512 KB) + `ev_sinks[p]` (1 MB) exceeds 1 MiB L2; Pass 2 scans `table` in hash order and calls `get(j)` **twice per record** (128M random accesses at 60M); `links.canonical` hits a 58 MB DRAM map on ~65% negative lookups; `b_sinks` streams random 16-byte quarter-cache-line NT stores to 1024 partitions (4× write amplification).
- `group` + `assemble` + `aggregate+write`: **~4.1s CPU (19%)**, **0.51s warm wall**, and **~1.5 GB unnecessary peak RSS**.
  - Artificial 2-agent boundary (`Loaded`) allocates and writes 960 MB `events: Vec<Event>` + 240 MB `clicks: Vec<Click>` + `StringTable`, then reads them all back from DRAM in `aggregate::run`.
- `links-uf`: **0.78s CPU (4%)**, **0.14–0.18s wall**.
  - Poor 8-thread scaling due to single-threaded `vec![0; 2*nl]`, `parent`/`best` init, `keys` flatten copy, and 2048 Vec reallocations.

## 4. Decision: Continue & Evolve the Existing Rust Pipeline

**Verdict: Continue with the existing zero-dependency Rust codebase (option a), while dismantling the artificial ingest/aggregate boundary and re-engineering the hot stages.**

Why not rewrite from scratch in another language/engine?
- Zero-dependency Rust with LLVM `lto = "fat"` + `x86-64-v3` + explicit SIMD intrinsics is already the ideal performance substrate (identical codegen ceiling to C++), compiles in ~30s, produces a ~200 KB binary, and already passes 100% of correctness checks including 60M vs `oracle2` and 32 adversarial cases.
- The current code is compact (~2,500 LOC) and clean; every bottleneck is localized and surgically replaceable without discarding proven exact parsers, tests, and oracles.

## 5. Execution Plan

1. **Tag `candidate-1`** at `8ab1414` as our verified baseline (20M warm 1.58s / cold 2.16s; 60M warm 2.89s / cold 4.24s Docker wall).
2. **Step 1 — Software Write-Combining Buffers (SWCB) + `dedup` Overhaul**:
   - 64-byte L1/L2-resident SWCBs in `Sinks` with full-line `_mm_stream_si256` flushes + dynamic `P1`/`P2` sizing.
   - Compact `u32` dedup hash table (256 KB, L2-resident) + 4 KB L1 `dead` bitset + sequential Pass 2 (eliminates 128M random `get(j)` lookups).
   - Register-blocked L3 Bloom filter in `Links` (filters ~99% of ~65% negative lookups before DRAM) + batched prefetch in `parse_click` + faster parallel `Links::build`.
3. **Step 2 — Fuse `group` + `assemble` + `aggregate` + `user_daily write`**:
   - Process `P2` partitions largest-first (whale/kiosk partitions first) in L2-resident 8-byte `CompactEv { ts: u32, cc: u32 }` buffers, radix-sorting large person slices, and streaming `user_daily` straight to `UserCols` + `CampStore`. Eliminates ~1.5 GB RAM and `Loaded` round-trips.
   - Fix Parquet edge cases (clean 0-row output, split pages before `i32::MAX` / `MAX_ROWS`).
4. **Step 3 — Turbocharge `parse` & Cold I/O**:
   - L1-resident `YYYY-MM-DD` date lookup table + direct `ingest_sec` F1 parser + specialized 13-digit F3.
   - L1/L2-tiled CSV scanner (`scan.rs`): skip quote checks when `qc == q_prev`, fast odd-quote reject, 2×32-bit `tzcnt`/`blsr` split, unchecked `lfs` writes.
   - Faster `u64::enc` (folded alpha range check).
   - Zero-copy `mmap` vs non-zeroing `pread` + optimal cold-cache readahead benchmarked on both 20M and 60M cold/warm.
5. **Step 4 — Hardening & Container Startup**:
   - Arbitrary-length `IdLookup::rest`, `ingest_ts` consistency, `list_csv` `.csv` suffix, verify `FROM scratch` (static musl) vs `debian:bookworm-slim` for container startup + runtime wall time.
   - Tag new `candidate-N` at every major verified milestone.
