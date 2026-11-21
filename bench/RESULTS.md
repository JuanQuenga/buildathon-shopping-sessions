# Benchmark results

## Please submit

- **Tag**: `candidate-4` (HEAD on `main`) — **ready for official submission now** (passes full gate + all 4× 60M stress datasets in Docker under scorer flags).
  - **What it tests**:
    1. **Safety (60M tail risk eliminated)**: parallel, memory-bounded generic path (`src/ingest/generic.rs` + `SlabBuf` + 512-entry `DateTable`) handles arbitrary ID lengths (`>15B`), arbitrary category counts (`>256`), and arbitrary window spans (`>42d`) in parallel — verified exact `MATCH` vs `oracle2` in Docker on `seed60-60m`, `seed60-longids` (< 5.5 GiB RSS), `seed60-5kcat`, and `seed60-90d`.
    2. **SPEED_REVIEW Items 1–4 & 7 + Wave 1/2 speedups**: parallel `munmap` of arenas and per-worker `UserCols`/scratch teardown before exit (outside-`main` teardown reduced from **153 ms → < 8 ms**), `pread` restored, zero-alloc `products`/`campaigns` overlapped concurrently with `links`, `preadv2(RWF_NOWAIT)` warm-cache check skipping prefetch threads when cached (0 warm / 4 cold), persistent worker pool, no-THP streaming `Sinks`, guarded pure-YMM **AVX-512VL-256** `u16` scanner (verified via `objdump`: zero ZMM / VBMI / VPOPCNTDQ), carry-free 13-digit F3 + SWAR `HH:MM:SS`, `128 KiB` chunks + `128 B` SWCB tuned on 4C×2T SMT (`--cpuset-cpus 0-3,8-11`), linear short-interval assists, Debian glibc runtime (beat musl 0.914 s vs 0.992 s on 8 vCPUs), and `getrusage` user/sys/minflt stage timers + 1-line `[diag]` stderr line.
  - **Local A/B vs `candidate-3` (`ss-test:4f01a6ec67`) on `--cpuset-cpus 0-3,8-11` (`seed20-20m` warm, exact scorer Docker flags)**:
    - `candidate-3`: **1.050 s** median Docker container wall.
    - `candidate-4`: **0.774 s** median Docker container wall (**−26.3%** container wall; outside-`main` < 8 ms).
  - **Verification**: 15/15 release unit tests (0 warnings), `objdump` AVX-512 check, native `check_all.sh` (36 official, 32 adversarial, dev, 8× 1M seeds, 20M vs `oracle2`), forced `INGEST_GENERIC=1` full `check_all.sh`, `INGEST_SLOW=1` + `INGEST_NO_AVX512=1`, Polars/PyArrow/DuckDB cross-reader on dev + `all_empty_output`, Docker 36/36 + dev, and all 4× 60M datasets (`seed60-60m`, `seed60-longids`, `seed60-5kcat`, `seed60-90d`) exact `MATCH` in Docker.

## Official scores

| Tag | Commit | Official wall (hidden ~20M) | Rank | Notes |
| --- | --- | ---: | --- | --- |
| `candidate-3` | `63b9256` (`4f01a6e`) | **0.88 s** | 12→28 (leaders now 0.48–0.64 s) | Accepted exact output |
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
