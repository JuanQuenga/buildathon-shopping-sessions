#!/usr/bin/env python3
"""Strict, order-insensitive comparison of two output folders (or expected CSV folders).

    compare.py EXPECTED ACTUAL

Same semantics as the official starter compare.py (it reuses the official loaders from
tests/official/run_fixtures.py: column set, logical types incl. DATE, integer widths
normalized, null equality, row-set equality both ways, duplicate daily keys), plus:
  * exact multiset equality (duplicate identical rows are caught),
  * the rules.md self-check invariants on ACTUAL,
  * warnings for stray entries in the output folders (nested dirs, non-parquet files).
Exit 0 on MATCH, 1 on DIFFER.
"""
from collections import Counter
from pathlib import Path
import os
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tests" / "official"))
from run_fixtures import SCHEMAS, load_actual, load_expected  # noqa: E402
import polars as pl  # noqa: E402

KEYS = {"user_daily": ["user_id", "day"], "campaign_daily": ["campaign_id", "day"]}


def load(folder, table):
    csv = folder / f"{table}.csv"
    if csv.is_file() and not (folder / table).is_dir():
        return load_expected(csv, SCHEMAS[table])
    return load_actual(folder / table, SCHEMAS[table])


def stray(folder, table):
    d = folder / table
    if not d.is_dir():
        return []
    out = []
    for name in sorted(os.listdir(d)):
        p = d / name
        if name.startswith((".", "_")):
            continue
        if p.is_symlink() or not p.is_file() or not name.endswith(".parquet"):
            out.append(name)
    return out


def main():
    exp_dir, act_dir = map(Path, sys.argv[1:3])
    ok = True
    totals = {}
    for table, schema in SCHEMAS.items():
        try:
            e = load(exp_dir, table)
            a = load(act_dir, table)
        except Exception as error:  # noqa: BLE001
            print(f"{table}: LOAD ERROR {type(error).__name__}: {error}")
            ok = False
            continue
        for name in stray(act_dir, table):
            print(f"{table}: WARNING stray entry in output folder: {name!r}")
        keys = KEYS[table]
        dup_e = e.height - e.n_unique(subset=keys)
        dup_a = a.height - a.n_unique(subset=keys)
        only_e = e.join(a, on=list(schema), how="anti", nulls_equal=True)
        only_a = a.join(e, on=list(schema), how="anti", nulls_equal=True)
        same_multiset = Counter(e.rows()) == Counter(a.rows())
        print(f"{table}: rows expected {e.height} vs actual {a.height}; only in expected {only_e.height}; "
              f"only in actual {only_a.height}; duplicate keys {dup_e} vs {dup_a}")
        if dup_e or dup_a or only_e.height or only_a.height or not same_multiset:
            ok = False
            with pl.Config(tbl_rows=20, tbl_cols=10, fmt_str_lengths=60):
                if only_e.height:
                    print("  missing/differing (expected side):")
                    print(only_e.sort(keys).head(20))
                if only_a.height:
                    print("  extra/differing (actual side):")
                    print(only_a.sort(keys).head(20))
            if not same_multiset and not (only_e.height or only_a.height):
                print("  multiset differs (duplicate identical rows)")
        totals[table] = a
    if "user_daily" in totals and "campaign_daily" in totals:
        u, c = totals["user_daily"], totals["campaign_daily"]
        us, ur = int(u["sessions"].sum() or 0), int(u["revenue_cents"].sum() or 0)
        cs, cr = int(c["sessions"].sum() or 0), int(c["revenue_cents"].sum() or 0)
        assisted = int(c["assisted_sessions"].sum() or 0)
        non_org = int(c.filter(pl.col("campaign_id") != "organic")["sessions"].sum() or 0)
        inv = us == cs and ur == cr and assisted >= non_org
        print(f"invariants: sessions {us}/{cs} revenue {ur}/{cr} assisted {assisted} >= non-organic {non_org}: "
              f"{'ok' if inv else 'VIOLATED'}")
        ok = ok and inv
    print("MATCH" if ok else "DIFFER")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
