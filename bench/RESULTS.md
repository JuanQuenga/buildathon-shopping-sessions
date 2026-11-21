# Benchmark results

Command: `bench/run.sh DATA_DIR IMAGE RUNS` (Docker, scorer flags, `--cpuset-cpus 0-7`, 24g).
Wall = container start to exit, measured on the host. Stage timers come from the binary's stderr.
"Cold" evicts the data files from the page cache with `posix_fadvise(DONTNEED)` (no sudo on the
dev box, so `drop_caches` is not available); other page cache (binaries, image layers) stays warm.

Dev box: AMD Ryzen 7 8845HS (16 threads, shared with ~12 other agents, so timings are noisy),
NVMe, btrfs. The scorer is an 8-vCPU Cascade Lake+ N2; expect different absolute numbers.

## 2026-11-20 commit 4f63b5a (ingest fast path 6345372, aggregate/output v1)

| Data | Mode | Wall (s), 3 runs | In-process total (s) | aggregate | write | Peak RSS |
| --- | --- | --- | --- | --- | --- | --- |
| seed20-20m (19.55M events, 4.93M clicks, 2.25M user rows) | cold | 3.48 / 3.56 / 3.45 | 3.05 / 3.15 / 3.01 | 0.046–0.049 | 0.025–0.026 | 2.85 GB |
| seed20-20m | warm | 3.97 / 4.27 / 3.52 | 3.51 / 3.87 / 3.04 | 0.060–0.131 | 0.030–0.047 | 2.85 GB |
| dev (1M) | warm | 0.56 | 0.17 | 0.005 | 0.002 | 0.19 GB |

Observations: ~0.4 s of every run is Docker container start/stop (dev: 0.56 s wall vs 0.17 s
in-process). Cold and warm are indistinguishable here (fast NVMe, reads overlap with parsing).
Aggregate + write are ~2.5% of the in-process time; ingest parse/dedup dominate.
