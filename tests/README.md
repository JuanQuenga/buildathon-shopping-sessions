# Test data and correctness harness

Owner: test agent. Everything here is about making sure a wrong pipeline never reaches the scoreboard.

## Generated datasets (outside the repo)

Root: `/home/juan/.t3/scratch/2026-11-20-you-are-the-coordinator-for-8d9ce8ed/shopping-sessions/data/`
(`$DATA` below). Built with the official generator (`$DATA/bin/generate`, built from
`starter/generator` with a regenerated lockfile, rustc 1.99.1). Clicks = events / 4.

| Dir | Command | Bytes | event/click/link files | Window |
| --- | --- | ---: | --- | --- |
| `dev` | `--seed 1 --events 1000000 --clicks 250000` (= public dev input) | 96,210,428 | 30/40/14 | 2027-12-09 .. 2027-12-23 |
| `seed2-1m` | `--seed 2 --events 1000000 --clicks 250000` | 95,948,708 | 95/15/4 | 2026-09-07 .. 2026-09-21 |
| `seed3-1m` | seed 3, 1M | 96,874,148 | 126/19/4 | 2025-07-16 .. 2025-07-30 |
| `seed4-1m` | seed 4, 1M | 94,910,086 | 31/24/19 | 2027-02-01 .. 2027-02-15 |
| `seed5-1m` | seed 5, 1M | 94,705,344 | 55/35/14 | 2026-09-20 .. 2026-10-04 |
| `seed6-1m` | seed 6, 1M | 95,384,759 | 110/26/6 | 2025-06-23 .. 2025-07-07 |
| `seed7-1m` | seed 7, 1M | 96,852,050 | 143/32/7 | 2027-12-11 .. 2027-12-25 |
| `seed20-20m` | `--seed 20 --events 20000000 --clicks 5000000` | 1,870,722,770 | 87/24/22 | 2026-04-12 .. 2026-04-26 |
| `seed60-60m` | `--seed 60 --events 60000000 --clicks 15000000` | 5,724,405,298 | 26/35/19 | 2027-03-06 .. 2027-03-20 |

Generation is fast (1M: 0.2 s, 20M: 5 s, 60M: ~20 s, peak RSS 7.8 GB) and byte-identical per seed,
so regenerate rather than copy: `$DATA/bin/generate --seed N --events E --clicks C --output DIR`.

Expected outputs (from the reference oracle) live next to each dataset as `$DATA/expected/<name>/`
once computed; the dev answer CSVs are in `starter/dev-answer/dev-answer/`.
