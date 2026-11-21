# Rust pipeline vs oracles: mismatch log

Newest status first. Each mismatch gets a minimal reproducer (under `tests/repro/<name>/` or inline) and the rule it breaks.

## Current status

| Rev (origin/main) | Official fixtures | Adversarial (27 small + 5 big) | Dev answer | 1M seeds 2-7 + random | 20M (seed 20) vs oracle2 | 60M (seed 60) vs oracle2 | Docker (scorer flags) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `4f01a6e` (`candidate-3`) | 36/36 | 32/32 | MATCH (0.35s) | 8/8 MATCH (6 cached + 2 random) | MATCH (3.79s) | MATCH (6.63M user / 28K camp rows, 3.89 GB RSS) | 36/36 fixtures + dev MATCH (0.54s); `INGEST_SLOW=1` MATCH; Polars/PyArrow/DuckDB cross-reader PASS; Docker A/B vs cand-1: 20M warm 1.59–1.71s vs 2.00–2.69s, 60M warm 4.77–5.83s vs 7.33s |
| `c0688ae` (`candidate-2`) | 36/36 | 32/32 | MATCH | 7/7 MATCH | MATCH | MATCH (6.63M user / 28K camp rows) | 36/36 fixtures + dev MATCH; A/B vs cand-1: 1.46x faster CPU @20M, 1.35x @60M, -950 MB RSS |
| `8ab1414` (`candidate-1`) | 36/36 | 32/32 | MATCH | 7/7 MATCH | MATCH | MATCH (6.63M user / 28K camp rows) | 36/36 fixtures + dev MATCH; 20M 1.58s warm / 2.16s cold; 60M 2.89s warm / 4.24s cold |
| `80de97b` | 36/36 | 30/30 | MATCH | 7/7 MATCH | MATCH (4.9 s native) | pending | dev + 36 fixtures + 30 adversarial MATCH |
| `e447835` | 36/36 | 30/30 | MATCH | MATCH | MATCH | - | - |
| `f445a1a` | 36/36 | 30/30 | MATCH | 7/7 + 1 random MATCH | - | - | - |

**No open mismatches.**

## Closed / not-a-bug

* 2026-11-20 `e447835`, seed20-20m: 2 user_daily rows of `u4hy5rp6s` differed vs oracle2 (one
  event in the 04-17 vs 04-18 session). Root cause was **oracle2** (DuckDB): `lag` and the running
  `sum` used two window orderings that could order equal-`ts` rows differently, so a tie row could
  get the previous session id. Rust and the Python reference agreed on the extracted person.
  Fixed in oracle2 (RANGE frame); Rust was right.

## How to reproduce anything here

    scripts/check_all.sh head            # build origin/main, run the whole matrix
    SS_BIG=1 scripts/check_all.sh head   # + 20M vs oracle2
    scripts/diff_check.sh <binary|image> <data-dir> [expected-dir]
