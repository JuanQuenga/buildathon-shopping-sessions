# Rust pipeline vs oracles: mismatch log

Maintained by the test agent. Newest status first. Each mismatch gets a minimal reproducer
(under `tests/repro/<name>/` or inline) and the rule it breaks.

## Current status

| Rev (origin/main) | Official fixtures | Adversarial (26 small + 4 big) | Dev answer | 1M seeds 2-7 + random | 20M (seed 20) vs oracle2 | 60M | Docker (scorer flags) |
| --- | --- | --- | --- | --- | --- | --- | --- |
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
