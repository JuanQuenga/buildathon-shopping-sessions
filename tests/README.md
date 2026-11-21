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

## How to run the checks

Python: `$SS_HOME/work-test/venv` (polars 2.1.0, pyarrow, duckdb; `pip install -r tests/requirements.txt`).
All scripts source `scripts/env.sh` (override `SS_HOME`, `SS_DATA`, `PY`).

| Command | What |
| --- | --- |
| `scripts/check_all.sh head [N]` | Build origin/main in a clean export; run 36 official fixtures, 30 adversarial cases, dev answer, seeds 2-7, N random 1M seeds. `SS_BIG=1` adds 20M vs oracle2. Prints a PASS/FAIL matrix. |
| `scripts/check_all.sh <binary or image>` | Same for a given native binary or Docker image tag. |
| `scripts/diff_check.sh CAND DATA [EXPECTED]` | One dataset: run candidate (timed), compare with reference output (cached) or EXPECTED. |
| `scripts/run_candidate.sh CAND DATA OUT` | Run once with the scorer env. Docker images get the scorer flags: `--read-only --tmpfs /tmp:size=64m --user 1000:1000 --network none --cpus 8 --memory 24g --memory-swap 24g --pids-limit 4096 --cap-drop ALL`. |
| `scripts/run_adversarial.sh CAND` | Adversarial cases only (`tests/adversarial/cases`, plus `$SS_DATA/adversarial-big` if generated with `make_cases.py ... --big`). |
| `scripts/build_head.sh [REV]` / `scripts/build_image.sh [REV]` | Build binary / image from a committed rev (never from the shared dirty tree). |
| `scripts/oracle2.sh DATA` | DuckDB oracle output for large generated data (cached in `$SS_DATA/expected2/`). |
| `scripts/compare.py EXP ACT` | Official compare.py semantics + exact multiset + invariants + stray-file warnings. |

Oracles: `reference/ref.py` (pure-Python literal reference, ~16 s per 1M events; see
`reference/NOTES.md`) and `reference/oracle2.py` (DuckDB SQL, 3.7 s per 1M, 50 s for 20M; not
CR-exact, generated data only). They agree on dev, seeds 2-7, and every adversarial case without CR.

Adversarial cases: `tests/adversarial/cases/<name>/CASE.md` has the rule citation;
`expected/*.txt` are human-readable dumps of the reference output.
