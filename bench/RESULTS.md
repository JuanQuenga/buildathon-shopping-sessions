# Benchmark results

## Please submit

- **Tag**: `candidate-13c` (HEAD on `main`, glibc bookworm) — **Prototype C VMA-uncontended pread + LocalAlloc slabs + L2-hot 4-wide AVX2 RelocMap + dynamic group+agg + fast `eid` & `links` direct parsers** (beats `candidate-12b2` on same-session interleaved local wall by **−71 ms** at **2.2× lower RSS** and **2.3× fewer faults**!).
  - **Root cause fixed vs `candidate-13b` (`38e9a39`, official `0.8135 s`)**:
    1. Moved per-worker `ReadBuffers` out of the `645 MiB` `Arena` VMA onto the heap (eliminated `copy_to_user` vs concurrent Arena page-fault VMA contention: `parse read sys` dropped **`1.02 s → 0.31 s`**, total `sys` dropped **`1.30–1.42 s → 0.62–0.70 s`**!).
    2. Added per-worker `LocalAlloc` (`256 KiB` slabs) across `parse`, `links`, and `relocate`; added `< 1 ms` 16 KiB up-front `prescan_compatible` and 4-thread cold-file `posix_fadvise` prefetch; fixed unparseable `ingest_ts` to emit `INVALID_INGEST` instead of falling back.
    3. Replaced `relocate`'s unbuffered random writes + 8-slot `Links` probe with per-shard L2-hot 4-wide AVX2 `RelocMap` (`perm35 + 1` keys, zero `canonical[]` indirection) + `WriteLine<64>` SWCB streaming (`relocate user` `0.48 → 0.31–0.33 s`).
    4. Switched `group+agg` from serial worker init + static `jobs[tid]` to parallel init + dynamic work-stealing `par::run_fini` over 256 partitions; added whole-second `parse_f1_pair_sec` + `valid_sec` + `Tok10::pack_valid_fast` (zero `*1000`/`/1000` round-trip); added non-order-preserving `encode_eid_short` (drops `_mm_sad_epu8` and `/ 35` on 21M event IDs) and dropped dead `low` bits from `packed::hash`; added direct `links` parser (`links-parse` `17 → 5.7 ms`).
  - **Measured same-session interleaved A/B vs `candidate-12b2` (`seed20-20m`, `4k` pages, 8T pinned `0-3,8-11`, 7 rounds)**:
    - `candidate-12b2`: **`med = 0.8499 s`** (`sys = 0.83 s`, `minflt = 285k`, `RSS = 1,141–1,259 MiB`)
    - **`candidate-13c`**: **`med = 0.7787 s`** (**−71.2 ms faster than `12b2`**, `sys = 0.62–0.70 s`, **`minflt = 124.9k`**, **`RSS = 526 MiB`**)
  - **Wall-line prediction** (`official ≈ 0.19 + 0.63 × local_wall`, scaled from `0.7787 / 0.8499 × 0.780 = 0.715 s`): **`~0.64 s` official** (vs `12b2` `0.6811 s`).
  - **Verification**: 61/61 release unit tests (**0 warnings**), 36/36 official fixtures, 32/32 adversarial suites, `dev` + 1M + `seed20-20m` (Polars/PyArrow/DuckDB), `INGEST_GENERIC=1`, all four 60M datasets (`< 7.5 GiB` RSS), Docker `ss-test:c13c` `dev` + `seed20-20m` exact `MATCH`.

## Official scores

| Tag | Commit | Official wall (hidden ~20M) | Rank | Notes |
| --- | --- | ---: | --- | --- |
| `candidate-12b2` | `79d1dce` | **0.6811 s** | **CURRENT BEST** | Accepted exact output; 1-var `MADV_POPULATE_WRITE` on 12b (−8.5 ms vs 12b; local 4k wall 0.780 s) |
| `candidate-12b` | `3889b4c` (`4ec2aa3`) | **0.6896 s** | 24 | Accepted exact output; 1-var unified arena reuse + `mallopt` (−30.9 ms vs 12a) |
| `candidate-12a` | `bf812eb` (`c34c244`) | **0.7205 s** | 26 | Accepted exact output; −99.0 ms vs 11b from 1-var `group+agg` rework |
| `candidate-13b` | `38e9a39` | **0.8135 s** | — | Accepted exact output; Prototype C CPU/L2 overhaul (512 MiB RSS, local 4k wall 1.012 s; confirmed `official ≈ 0.19 + 0.63 × local_wall`) |
| `candidate-11b` | `ea17016` (`7bd0941`) | **0.8195 s** | — | Accepted exact output; direct specialized row parsers + co-partitioned L2 Links + `tok→reloc` reuse |
| `candidate-11a` | `f6d8110` | **0.8678 s** | — | Accepted exact output; co-partitioned L2 Links + quote-bit SIMD scan + BMI2 PEXT keys |
| `candidate-8` | `5d0f934` (`49473d0`) | **0.8752 s** | 37 | Accepted exact output (H1: 4.46× smaller compact Parquet 97.9→21.9 MB) |
| `candidate-3` | `63b9256` (`4f01a6e`) | **0.8824 s** | 29 | Accepted exact output |
| `candidate-5` | `2bd1a01` (`6c2778c`) | **0.8826 s** | 36 | Accepted exact output |
| `candidate-9` | `bcf953c` (`exp/h3-arenas-exit`) | **0.8853 s** | 39 | Accepted exact output (H3: unconditional THP + prefault + `_exit(0)`) |
| `candidate-10` | `e3ccaf9` (`9de36c7`) | **0.8938 s** | 40 | Accepted exact output (1 MiB `mmap` w/ worker `munmap`; +18.6 ms vs c8; reverted) |
| `candidate-13a` | `cd34094` | **0.9107 s** | — | Accepted exact output; Prototype C narrow deferred 10/8/16B integration (518 MiB RSS, local 4k wall 1.124 s) |
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
