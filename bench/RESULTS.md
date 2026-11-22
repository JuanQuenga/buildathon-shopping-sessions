# Benchmark results

## Please submit

- **Tag**: `candidate-15b` (branch `exp/c15-engine`) — **Hybrid inline-64 + side-arena single-pass exact engine for `shopping-sessions-5` opaque variable-width IDs (`2.22×` faster local wall `1.161 -> 0.524 s`, `-52.5%` user CPU `6.31 -> 3.00 s` vs `candidate-15a` on `seed20-20m-v5`)**.
  - **Why**: `shopping-sessions-5` gives 1/8 distinct `user_id`/`event_id`/`click_id` values a 32-char hex suffix (33 bytes total) and mandates treating IDs as opaque variable-width byte strings. `candidate-15a` (`ba527a9597`, `1.9110 s` official on `shopping-sessions-5`, Rank #13) rejected 33B IDs in `u64`/`u128` and fell back to `generic.rs` (`ev-dedup 417 ms`, `505k` faults, 0% THP).
  - **What `candidate-15b` adds (`src/ingest/hybrid.rs`, wired in `src/ingest/fast.rs`)**:
    1. **100% opaque variable-width ID representation (`1..∞` bytes, arbitrary UTF-8/NUL)**: keeps short IDs (`1..=9` non-NUL ASCII, 87.5% on v5) inline in `u64` with bit 63 == 0 (`mix63`/`unmix63` exact bijection on `[0, 2^63)`), and stores long/non-ASCII IDs verbatim in length-prefixed per-worker byte arenas (`long_eids`, `long_users` with 4096-slot direct-mapped dedup cache, `long_cids`) and a read-only `LinkLongStore`, referenced via tagged `u64` handles (bit 63 == 1) with 20/25/26-bit embedded hash fingerprints.
    2. **Direct variable-width parser (`id_delim_var`)**: branchless 8/9B inline ASCII fast path + 5-instruction AVX2 opaque 33B delimiter validator (checks zero `,`/`"`/`\n`, zero charset assumption) + `scan::row_fields` fallback for any other length `1..∞`.
    3. **Full single-pass 2 MiB THP `SinkArena` pipeline for mixed short+long IDs**: samples initial event rows (<20 µs) to size short `Tok` vs 16B `LongTok` partitions; mixed short+long union-find (`cmp_node_key` exact byte-wise ordering); hybrid `dedup` (`Tok` + `LongTok`, immediately frees `long_eids`); L2-resident `ShortRelocMap` (4-way AVX2) + `LongRelocMap`; L2-resident `ShortGroupMap` + `LongGroupMap` + `DenseSessions` + sorting networks + `FusedAssists` + `sort_and_dedup_clicks_var`. Toggle `INGEST_NO_HYBRID=1` allows instant fallback.
  - **Same-glibc Bookworm 2.36 A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, `INGEST_SINK_THP=0` for clean host noise, 3 warm runs)**:
    - `candidate-15a` (falls back to `generic.rs`): med **`1.161 s`** (`user=6.31 s`, `sys=1.67 s`, `minflt=505k`, `peak_rss=1144 MB`; `links 71 ms`, `parse 418 ms`, `dedup 417 ms`, `group+agg 243 ms`)
    - **`candidate-15b` (`hybrid`)**: **`0.513–0.530 s` (med `0.524 s`, `-637 ms`, `2.22×` faster!)**, **`user=3.00 s` (-52.5%)**, `sys=0.90–0.98 s`, `minflt=329k`, `peak_rss=1249 MB` (`links 36 ms`, `parse 261 ms`, `dedup 20.3 ms` [20.5× faster], `relocate 55 ms`, `group+agg 119 ms` [2.04× faster]).
  - **Cascade Lake Cachegrind (`dev-v5` 1M, `--branch-sim=yes`, `I1=32K, D1=32K, LL=6M`)**:
    - `Ir=974.4M`, `D1m=10.30M`, `LL(6M)=2.36M`, `Bcm=6.66M` (`5.5%`).
  - **Explicit official scorer prediction BEFORE submission**: **`0.80–0.88 s`** on `shopping-sessions-5` (vs `candidate-15a`'s `1.9110 s`, projected ~1.05 s faster -> Top 3–5; production default keeps `sink_thp=1` for clean scorer VM THP).
  - **Verification**: 38/38 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev-v5` exact `MATCH` vs `starter-v5/dev-answer` (both `hybrid` and `INGEST_GENERIC=1`), `seed20-20m-v5` exact `MATCH` vs `c15a-generic`, all four 60M datasets exact `MATCH` (`seed60-60m` 2.86 GiB, `seed60-5kcat` 2.91 GiB, `seed60-90d` 2.92 GiB, `seed60-longids` 7.79 GiB), and clean `git archive` Docker build verified `MATCH` on `dev-v5` and `seed20-20m-v5`.

### Previous submission (`candidate-15a`)

- **Tag**: `candidate-15a` (branch `exp/c15-engine` from `candidate-12b2` `79d1dce`, re-scored **`1.9110 s`** on `shopping-sessions-5` Rank #13 via `generic.rs` fallback) — **End-to-end cache, instruction, branch, and teardown overhaul (`-14.4% Ir`, `-30.7% LL(6M)`, `-51.4% LL(1M)`, `-159 ms` Docker warm container wall vs `c12b2`)**.
  - **What it changes vs `candidate-12b2` (`79d1dce`, `0.6811 s`)**:
    1. **Parallel exit teardown + gated `/proc/self/smaps_rollup`**: gates `smaps_rollup` behind `INGEST_DIAG=1` (`-6.3 ms` serial) and runs parallel `madvise(MADV_DONTNEED)` across the parse mapping before exit (`post-total` teardown cut `-45 ms`).
    2. **Pre-mixed bijective `u64` keys (`mix`/`unmix`)**: stores `eid.h()` in `Tok` (zero re-hashing in `dedup`), `raw.h()`/`canon.h()` in `RecB`/`RecC`/`BucketLinks` (zero hashing in `RelocationMap` and `GroupMap<u64, true>`; exact `unmix` only once per unique person).
    3. **L2-resident `relocate` with in-place compaction & token-arena reuse**: packs `RecC` to 24B (`SWCB=192`), carves `reloc_b`+`reloc_c` out of the already-faulted `tok_sinks` huge-page arena, compacts dead+linked rows in place (eliminating `DeadClicks` and removing `dead` bitset scanning + serial popcount from `group_fused`).
    4. **Direct parser & `links` speedups**: direct `direct::links` parser, root-min-key `uf_union_min_key` (drops `best[]` + extra `nn`-node CAS pass), single-load `u16` masked delimiter check in `direct::open_delim`/`ends_delim`, merged single-branch `view|cart` vs `purchase` path, whole-second window/late checks with SSE2 `clock_pair`, fixed 32B `tail()` overflow in `direct::click` (eliminating `click_slow` on F2/quoted timestamps), `p1_bits=10`/`p2_bits=9` + `1.08×` arena cap.
    5. **O(N) zero-sort `DenseSessions` + branchless sorting networks + split `CampStore`**: generation-stamped bitmask 30-min bin sessionizer for dense users (`count >= 32`), Knuth 0-1-verified branchless `cmov` sorting networks for `n <= 8`, sentinel-guarded `FusedAssists` with branchless campaign stamp check, and split 16B `CreditCell` + 4B `assisted` `CampStore` (112 KiB/worker, 8× smaller).
  - **Cascade Lake Cachegrind (`dev` 1M, `--branch-sim=yes`, `I1=32K, D1=32K`)**:
    - `c12b2` (`0.6811 s` official): `Ir=976.1M`, `Bcm=6.33M`, `D1m=11.40M`, `LL(6M)=2.83M`, `LL(1M)=5.42M`
    - **`candidate-15a`**: **`Ir=836.0M` (-14.4%)**, **`Bcm=6.10M` (-3.7%)**, **`D1m=9.60M` (-15.8%)**, **`LL(6M)=1.96M` (-30.7%)**, **`LL(1M)=2.63M` (-51.4%)**
  - **Same-session interleaved Docker warm A/B (`seed20-20m`, `CPUSET=0-3,8-11`, `NCPU=8`, 3 rounds)**:
    - `ss-test:c12b2`: container wall `1.032 / 0.781 / 0.747 s` (med **`0.781 s`**, in-proc `0.655 s`, `user=2.55–2.69 s`, `sys=2.26–3.67 s`)
    - **`ss-test:c15a`**: container wall **`0.649 / 0.622 / 0.602 s`** (med **`0.622 s`**, best **`0.602 s`**, in-proc `0.541 / 0.520 s`, `user=2.25–2.29 s`, `sys=1.79–2.05 s`) — **`-159 ms` (-20.4%) median container wall!**
    - Stage medians (`c12b2` → `c15a`): `links` `68 → 42 ms`, `parse` `408 → 351 ms`, `dedup` `23.0 → 16.0 ms`, `relocate` `37.6 → 32.8 ms`, `group+agg` `108 → 81.5 ms`, `exit+container` `126 → 81 ms`.
  - **Explicit official scorer prediction BEFORE submission**: **`0.51–0.56 s`** (vs `c12b2`'s `0.6811 s`, −120 to −170 ms; all 5 Cachegrind metrics and every pipeline stage are strictly faster).
  - **Verification**: 33/33 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev` + 6 cached 1M seeds + 2 fresh random 1M seeds, `seed20-20m` vs `oracle2`, `INGEST_GENERIC=1`, all four 60M datasets (`seed60-60m` 2.94 GiB, `seed60-5kcat` 2.98 GiB, `seed60-90d` 3.03 GiB, `seed60-longids` 5.98 GiB), and clean `git archive` Docker build verified `MATCH` on `dev` and `seed20-20m`.

### Previous submission (`candidate-12b`)
- **Tag**: `candidate-12b` (HEAD on `main`, code `4ec2aa3` on glibc bookworm) — **1-variable allocation/THP/arena-reuse/`mallopt` change ONLY on `candidate-12a` (`0.7205 s`) base** (zero algorithm change).
  - **What it changes vs `candidate-12a` (`bf812eb`, `0.7205 s`)**:
    1. **Unified 2 MiB-aligned `MADV_HUGEPAGE` arena reuse across stages**: `links-uf` reuses the `tok` arena before `parse`; `relocate` reuses the `tok` arena after `dedup`; `group+agg` reuses relocation arena tails; no mid-run `munmap` of parse arenas (`sink_thp()` fixed to always apply `MADV_HUGEPAGE` whenever THP is `always` or `madvise`).
    2. **Startup `mallopt`** (`M_TRIM_THRESHOLD`, `M_MMAP_THRESHOLD`, `M_ARENA_MAX`) to stop glibc from repeatedly `mmap`/`munmap`-ing transient buffers (`UserCols`, per-thread scratch).
    3. **Recycled per-worker `pread` buffers** across input chunks + `/proc/self/smaps_rollup` `AnonHugePages` diagnostic.
  - **Local verification & fault/THP signature** (note: local 14-day-uptime host is RAM-fragmented under `thp=always/madvise`, so 20M only gets `94–146 MiB` THP locally with kernel compaction `sys`, whereas clean memory gets **87% THP**):
    - **Docker `dev` (clean THP fit)**: `90.1 / 103.4 MiB` (**87% `AnonHugePages`**), `links-uf` **7 faults**, `parse` **379 faults**, `dedup` **0 faults**, `relocate` **2 faults**, total **3,309 faults** (`0.073 s`).
    - **20M (`seed20-20m`, fragmented local host)**: `links-parse` faults −80–95% (`5.1k → 0.2–1.1k`), `links-uf` faults −65–90% (`16.8k → 1.7–6.4k`), `group+agg` faults −37% (`16.2k → 9.9k`) & `group+agg sys` −70% (`0.108 → 0.030 s`), `dedup` faults `747`, `cache-misses:u` `48.1M → 45.6M`, exit `AnonHugePages` `4 → 94–146 MiB`, Peak RSS `954 MB` (Docker).
  - **Explicit model prediction BEFORE submission**:
    - **If the clean scorer VM backs the unified 820 MiB arena with 2 MiB THP** (like Docker `dev` at 87% THP, cutting ~200k+ minor faults): **~0.48–0.58 s** (at `1.1–2.8 µs/fault`, minus user CPU floor).
    - **If the scorer VM has THP disabled or fragmented** (only the ~25–35k non-THP arena-reuse/`mallopt` faults drop): **~0.66–0.70 s** (vs `12a`'s `0.7205 s`).
    - **Worst-case risk**: if the scorer VM has `defrag=always` or heavy compaction stalls on `MADV_HUGEPAGE`, compaction `sys` could partially offset fault savings (recall `c3` with `MADV_HUGEPAGE` beat `c4` without it by **−103 ms** on the scorer, strongly indicating the scorer grants THP cleanly!).
  - **Verification**: 30/30 release unit tests (0 warnings), 36 official fixtures, 32 adversarial suites, `dev` + 1M seeds + `seed20-20m`, forced `INGEST_GENERIC=1`, 8 byte-identical `INGEST_DUMP` comparisons, all four 60M datasets (`seed60-60m` **2.85 GiB**, `seed60-longids` **5.84 GiB**, `seed60-5kcat` **2.98 GiB**, `seed60-90d` **2.91 GiB** — all well under 8 GiB), and Docker `ss-test:c12b` (`--network none --read-only --user 1000:1000 --memory=8g`) exact `MATCH` on `dev` and `seed20-20m`.

## Official scores

| Tag | Commit | Official wall (hidden ~20M) | Rank | Notes |
| --- | --- | ---: | --- | --- |
| `candidate-15a` | `ba527a9` (`33de581`) | **1.9110 s** (`v5`) | **13 (`v5`)** | Re-scored on `shopping-sessions-5` (33B opaque IDs); fell back to `generic.rs` |
| `candidate-12a` | `bf812eb` (`c34c244`) | **0.7205 s** | **26 (NEW BEST)** | Accepted exact output; **−99.0 ms vs 11b** from 1-variable `group+agg` rework (−35k faults, −120 MB RSS, −106 ms group sys) → proves scorer heavily charges fresh memory / minor faults (~1.1–2.8 µs/fault) |
| `candidate-11b` | `ea17016` (`7bd0941`) | **0.8195 s** | NEW BEST (leader 0.31 s) | Accepted exact output; direct specialized events/clicks parsers + co-partitioned L2 Links + zero-fault `tok→reloc` arena reuse (−55.7 ms vs c8) |
| `candidate-11a` | `f6d8110` | **0.8678 s** | — | Accepted exact output; co-partitioned L2 Links + quote-bit SIMD scan + BMI2 PEXT keys (matched 0.865–0.890 s out-of-sample model prediction!) |
| `candidate-8` | `5d0f934` (`49473d0`) | **0.8752 s** | 37 | Accepted exact output (H1: 4.46× smaller compact Parquet 97.9→21.9 MB); −7.4 ms vs c5 |
| `candidate-3` | `63b9256` (`4f01a6e`) | **0.8824 s** | 29 | Accepted exact output |
| `candidate-5` | `2bd1a01` (`6c2778c`) | **0.8826 s** | 36 | Accepted exact output; identical to candidate-3 despite halving event DRAM traffic & removing dedup Pass 2 |
| `candidate-9` | `bcf953c` (`exp/h3-arenas-exit`) | **0.8853 s** | 39 | Accepted exact output (H3: unconditional THP + prefault + `_exit(0)`); +3 ms vs c5 → first-touch/exit not dominant |
| `candidate-10` | `e3ccaf9` (`9de36c7`) | **0.8938 s** | 40 | Accepted exact output (1 MiB `mmap` w/ worker `munmap` on c8 base); +18.6 ms vs c8 (`256 KiB pread`); reverted on `main` |
| `candidate-6` | `5f6718c` (`8f5cb0b`) | **0.948 s** | — | Accepted exact output; +65 ms slower with 24 readers × 4 MiB + t=0 fadvise → cold-I/O hypothesis refuted |
| `candidate-4` | `e0b0a9d` (`f52fa03`) | **0.985 s** | — | Accepted exact output; +103 ms slower on scorer despite −26.3% locally (THP off on Sinks, 128 KiB chunks, AVX-512VL-256) |
| `candidate-7` | `8c4250c` (`c4d681c`) | **1.068 s** | — | Accepted exact output (H2: 4 threads vs 8 on c5 base); +0.185 s |
| `probe-1` | `4456bb8` | rejected (dev) | — | Deliberate 0-row dev mismatch; confirmed scorer returns row diff (not stderr) on mismatch |

## 2026-11-20 commit `4f01a6e` (`candidate-3`: 2M huge-page flat Links, RecA eid_hash, ring-32 prefetch, skip view/cart prod.get, 256 KiB L2-hot chunks + 4 prefetch threads, pshufb enc, CompactEv(u64))

Docker (`ss-test:4f01a6ec67`, 115 MB, measured under heavy shared load average ~33.4):

| Data | Mode | Wall (s) | In-process total (s) | links | parse | dedup | group+agg | Peak RSS |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `seed20-20m` (1.87 GB) | cold (load 33) | 2.719 / **2.559** / 2.476 | 2.168 / 1.974 / 1.952 | 0.20–0.29 | 1.16–1.29 | 0.27–0.31 | 0.25–0.30 | 1.37–1.42 GB |
| `seed20-20m` | warm (load 33) | 1.929 / 2.011 / **1.822** (A/B **1.587**) | 1.215 / 1.484 / 1.260 (A/B **1.066**) | 0.16–0.21 | 0.50–0.62 | 0.16–0.28 | 0.21–0.36 | 1.46–1.64 GB |
| `seed60-60m` (5.72 GB) | cold (load 33) | **7.053** / 7.463 | 6.471 / 6.672 | 0.63–0.65 | 4.29–4.31 | 0.73–0.96 | 0.75–0.77 | 3.95–4.08 GB |
| `seed60-60m` | warm (load 33) | 5.858 / **4.765** | 5.259 / **4.086** | 0.62–0.69 | 1.78–2.94 | 0.72–0.79 | 0.86–0.89 | 4.11–4.12 GB |

Back-to-back Docker A/B vs `candidate-1` (`ss-test:8ab1414442`, exact same load ~33, scorer flags):
- `seed20-20m` warm: `candidate-1` **2.690s / 1.998s** (in-proc `2.096s / 1.456s`, 1.81 GB RSS) -> **`candidate-3` 1.707s / 1.587s** (in-proc **`1.147s / 1.066s`**, 1.62–1.64 GB RSS) — **1.37–1.83× faster in-process wall (-21% to -37% Docker wall)**!
- `seed20-20m` cold: `candidate-1` **2.737s** (in-proc `2.147s`, 1.82 GB RSS) -> **`candidate-3` 2.388s** (in-proc **`1.882s`**, 1.63 GB RSS) — **-13% Docker wall**!
- `seed60-60m` warm: `candidate-1` **7.327s** (in-proc `6.672s`, 4.30 GB RSS) -> **`candidate-3` 5.827s** (in-proc **`5.176s`**, 4.08 GB RSS; standalone warm run2 **4.765s wall / 4.086s in-proc**) — **1.26–1.54× faster Docker wall**!

Native single-core `INGEST_BENCH2` (`part-00004.csv`): `s0` (scan) 26.0–27.3 ns/row (2.8–3.0 GB/s), `s4` (full `parse_event` + SWCB push) **69.5 ns/row (1,107 MB/s)** vs 100–115 ns/row in `candidate-1` (**~1.6× faster single-core**).
Native 60M CPU sum across 8 threads: ~15.9s total (`parse` 8.8–11.1s, `dedup` 3.37–3.47s, `group+agg+write` 2.69–2.80s) vs ~29.0s in `candidate-1` (**~1.82× faster total CPU**).

## 2026-11-20 commit `c0688ae` (`candidate-2`: fused group+agg+write, 256B SWCB, u32 dedup+dead bitset, L1 DateTable, branchless scanner)

Docker (`ss-test:c0688ae640`, measured under heavy shared load average ~33–41):

| Data | Mode | Wall (s) | In-process total (s) | links | parse | dedup | group+agg | Peak RSS |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `seed20-20m` (1.87 GB) | cold (load 33) | 2.980 / **3.212** / 3.511 | 1.879 / 1.912 / 2.295 | 0.13–0.22 | 1.16–1.34 | 0.28–0.43 | 0.20–0.31 | 1.50 GB |
| `seed20-20m` | warm (load 33) | 2.282 / **2.414** / 2.619 | 1.286 / 1.347 / 1.684 | 0.12–0.19 | 0.60–0.81 | 0.20–0.46 | 0.21–0.31 | 1.63 GB |
| `seed60-60m` (5.72 GB) | cold (load 33) | 7.502 / 7.903 | 6.584 / 7.008 | 0.34–0.46 | 4.50–4.65 | 0.92 | 0.80–0.95 | 4.36 GB |
| `seed60-60m` | warm (load 33) | 6.018 / 6.135 | 4.864 / 5.304 | 0.41–0.46 | 2.92–3.14 | 0.75–0.78 | 0.73–0.85 | 4.29 GB |

Back-to-back native A/B vs `candidate-1` (`taskset -c 0-7`, `NCPU=8`, same load ~38):
- `seed20-20m` warm: `candidate-1` 1.602–1.714s wall (~6.86s CPU, 1.80 GB RSS) -> **`candidate-2` 1.208–1.438s wall (4.70s CPU, 1.57 GB RSS)** — **1.46× faster CPU** (`parse` 3.40→2.62s, `dedup` 1.51→0.91s, `group+asm+agg+write` 1.78→0.93s).
- `seed60-60m` warm: `candidate-1` 15.39–17.84s wall (~29.0s CPU, 4.74 GB RSS) -> **`candidate-2` 11.83–16.52s wall (~22.5s CPU, 4.28 GB RSS)** — **1.35× faster CPU** (`dedup` 7.14→4.79s, `group+asm+agg+write` 5.82→2.88s).

## 2026-11-20 commit `8ab1414` (`candidate-1`, Opus baseline)

| Data | Mode | Wall (s) | In-process total (s) | links | parse | dedup | group+asm | agg+write | Peak RSS |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `seed20-20m` (1.87 GB, 19.55M kept) | cold | 1.955 / **2.155** / 2.383 | 1.479 / 1.691 / 1.884 | 0.10 | 1.00–1.22 | 0.16–0.25 | 0.13–0.18 | 0.06–0.08 | 1.81 GB |
| `seed20-20m` | warm | 1.627 / **1.579** / 1.313 | 1.165 / 1.154 / 0.930 | 0.07 | 0.52–0.62 | 0.15–0.22 | 0.12–0.17 | 0.05–0.07 | 1.81 GB |
| `seed60-60m` (5.72 GB, 58.88M kept) | cold | 4.905 / 4.241 | 4.517 / 3.822 | 0.20 | 2.40–3.03 | 0.67 | 0.33–0.37 | 0.18 | 5.21 GB |
| `seed60-60m` | warm | 3.011 / **2.886** | 2.581 / 2.475 | 0.17 | 1.23–1.27 | 0.54–0.55 | 0.33–0.34 | 0.18 | 5.25 GB |

## 2026-11-20 commit `4f63b5a` (ingest fast path `6345372`, aggregate/output v1)

| Data | Mode | Wall (s), 3 runs | In-process total (s) | aggregate | write | Peak RSS |
| --- | --- | --- | --- | --- | --- | --- |
| seed20-20m (19.55M events, 4.93M clicks, 2.25M user rows) | cold | 3.48 / 3.56 / 3.45 | 3.05 / 3.15 / 3.01 | 0.046–0.049 | 0.025–0.026 | 2.85 GB |
| seed20-20m | warm | 3.97 / 4.27 / 3.52 | 3.51 / 3.87 / 3.04 | 0.060–0.131 | 0.030–0.047 | 2.85 GB |
| dev (1M) | warm | 0.56 | 0.17 | 0.005 | 0.002 | 0.19 GB |
