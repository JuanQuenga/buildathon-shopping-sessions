#!/usr/bin/env python3
"""Self-checks from rules.md on an output folder (needs duckdb):
unique daily keys, equal session and revenue totals across tables, assists >= credited
non-organic sessions, purchasing <= sessions, null top_category iff revenue 0.
Usage: invariants.py OUTPUT_DIR"""
import sys
import duckdb

o = sys.argv[1]
u = f"read_parquet('{o}/user_daily/*.parquet')"
c = f"read_parquet('{o}/campaign_daily/*.parquet')"
q = lambda s: duckdb.sql(s).fetchall()[0]
checks = {
    "user keys unique": q(f"select count(*) = count(distinct (user_id, day)) from {u}")[0],
    "campaign keys unique": q(f"select count(*) = count(distinct (campaign_id, day)) from {c}")[0],
    "sessions equal": q(f"select (select sum(sessions) from {u}) = (select sum(sessions) from {c})")[0],
    "revenue equal": q(f"select (select sum(revenue_cents) from {u}) = (select sum(revenue_cents) from {c})")[0],
    "assists >= credited non-organic": q(
        f"select sum(assisted_sessions) >= sum(case when campaign_id <> 'organic' then sessions else 0 end) from {c}")[0],
    "organic never assists": q(f"select coalesce(sum(assisted_sessions), 0) = 0 from {c} where campaign_id = 'organic'")[0],
    "purchasing <= sessions": q(f"select coalesce(bool_and(purchasing_sessions <= sessions), true) from {c}")[0],
    "top null iff no revenue": q(f"select coalesce(bool_and((top_category is null) = (revenue_cents = 0)), true) from {u}")[0],
    "no zero rows": q(f"select coalesce(bool_and(sessions > 0 or assisted_sessions > 0), true) from {c}")[0],
}
print(q(f"select count(*), sum(sessions), sum(events), sum(revenue_cents) from {u}"))
ok = all(checks.values())
for k, v in checks.items():
    print(("ok  " if v else "FAIL"), k)
sys.exit(0 if ok else 1)
