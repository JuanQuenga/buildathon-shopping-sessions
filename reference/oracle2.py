#!/usr/bin/env python3
"""Second, independent oracle for LARGE generated datasets (20M/60M events): DuckDB SQL.

    oracle2.py DATA_DIR OUTPUT_DIR [--threads N] [--memory 8GB]

Different code and algorithms from ref.py: SQL regex/arithmetic timestamp parsing, window-function
dedup and sessionization, ASOF-join attribution, interval/rank-range assists, BFS components.

LIMITATION: lines are delivered by DuckDB's CSV reader in one-column mode, which treats CR LF as a
line break (drops the CR) and yields blank lines as NULL. Exact only for input without CR bytes
(the generator never writes CR). Use ref.py for hand-made edge cases.
"""
import os
import sys
import time
from collections import deque

import duckdb

DAY = 86_400_000


def log(msg, t0=[time.time()]):
    print(f"[oracle2 {time.time() - t0[0]:7.1f}s] {msg}", file=sys.stderr, flush=True)


def main():
    data, out = sys.argv[1], sys.argv[2]
    threads = int(sys.argv[sys.argv.index("--threads") + 1]) if "--threads" in sys.argv else 8
    memory = sys.argv[sys.argv.index("--memory") + 1] if "--memory" in sys.argv else "8GB"
    tmp = os.environ.get("SS_SCRATCH_ROOT", "/tmp") + f"/duckdb-tmp-{os.getpid()}"
    con = duckdb.connect()
    con.execute(f"SET threads={threads}; SET memory_limit='{memory}'; SET temp_directory='{tmp}';"
                "SET preserve_insertion_order=false;")

    def lines(sub, name):
        pattern = os.path.join(data, sub, "*.csv") if sub else os.path.join(data, name)
        import glob
        if not glob.glob(pattern):
            con.execute(f"CREATE OR REPLACE TEMP VIEW raw_{name.split('.')[0]} AS SELECT NULL::VARCHAR line WHERE false")
            return
        con.execute(f"""CREATE OR REPLACE TEMP VIEW raw_{name.split('.')[0]} AS
            SELECT line FROM read_csv('{pattern}', columns={{'line': 'VARCHAR'}}, header=true,
                 delim=chr(31), quote='', escape='', auto_detect=false, new_line='\\n',
                 strict_mode=false, max_line_size=67108864)""")

    # Field splitting with the dialect's quote rule, as SQL macros.
    con.execute(r"""
        CREATE MACRO field_ok(f) AS (position('"' IN f) = 0 OR regexp_full_match(f, '"[^"]*"'));
        CREATE MACRO unq(f) AS (CASE WHEN position('"' IN f) = 0 THEN f ELSE substr(f, 2, length(f) - 2) END);
        CREATE MACRO fields_ok(l, n) AS (
            l IS NOT NULL AND len(string_split(l, ',')) = n
            AND list_bool_and(list_transform(string_split(l, ','), lambda x: field_ok(x))));
        CREATE MACRO fld(l, i) AS unq(string_split(l, ',')[i]);
        CREATE MACRO dim(y, m) AS (CASE WHEN m = 2 THEN (CASE WHEN (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0
                                  THEN 29 ELSE 28 END) WHEN m IN (4, 6, 9, 11) THEN 30 ELSE 31 END);
        CREATE MACRO civil_ms(s) AS (
            CASE WHEN CAST(substr(s, 6, 2) AS INT) BETWEEN 1 AND 12
                  AND CAST(substr(s, 9, 2) AS INT) BETWEEN 1 AND dim(CAST(substr(s, 1, 4) AS INT), CAST(substr(s, 6, 2) AS INT))
                  AND CAST(substr(s, 12, 2) AS INT) <= 23 AND CAST(substr(s, 15, 2) AS INT) <= 59
                  AND CAST(substr(s, 18, 2) AS INT) <= 59 AND CAST(substr(s, 1, 4) AS INT) >= 1
            THEN epoch_ms(make_date(CAST(substr(s, 1, 4) AS INT), CAST(substr(s, 6, 2) AS INT), 1)::TIMESTAMP)
                 + (CAST(substr(s, 9, 2) AS BIGINT) - 1) * 86400000
                 + CAST(substr(s, 12, 2) AS BIGINT) * 3600000 + CAST(substr(s, 15, 2) AS BIGINT) * 60000
                 + CAST(substr(s, 18, 2) AS BIGINT) * 1000
            ELSE NULL END);
        CREATE MACRO ts_ms(s) AS (
            CASE
              WHEN s IS NULL OR s = '' THEN NULL
              WHEN regexp_full_match(s, '[0-9]+') THEN
                   (CASE WHEN length(ltrim(s, '0')) > 18 THEN 9000000000000000000
                         WHEN ltrim(s, '0') = '' THEN 0 ELSE CAST(ltrim(s, '0') AS BIGINT) END)
              WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z') THEN civil_ms(s)
              WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}')
                   AND CAST(substr(s, 21, 2) AS INT) <= 23 AND CAST(substr(s, 24, 2) AS INT) <= 59 THEN
                   civil_ms(substr(s, 1, 19)) - (CASE WHEN substr(s, 20, 1) = '+' THEN 1 ELSE -1 END)
                        * (CAST(substr(s, 21, 2) AS BIGINT) * 3600000 + CAST(substr(s, 24, 2) AS BIGINT) * 60000)
              ELSE NULL END);
    """)
    import json
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws, we = (con.execute("SELECT ts_ms(?)", [man[k]]).fetchone()[0] for k in ("window_start", "window_end"))
    log(f"window {ws}..{we}")

    for sub, name in (("events", "events"), ("clicks", "clicks"), ("links", "links"),
                      (None, "products.csv"), (None, "campaigns.csv")):
        lines(sub, name)
    con.execute("""CREATE TEMP TABLE products AS SELECT fld(line,1) pid, fld(line,2) cat FROM raw_products WHERE fields_ok(line,2);
                   CREATE TEMP TABLE campaigns AS SELECT fld(line,1) cid, fld(line,2) ch FROM raw_campaigns WHERE fields_ok(line,2);""")

    # Links: connected components by BFS in Python; name = min byte string (Python bytes order).
    edges = con.execute("SELECT fld(line,1), fld(line,2) FROM raw_links WHERE fields_ok(line,2)").fetchall()
    adj = {}
    for a, b in edges:
        if a == "" or b == "":
            continue
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)
    canon = {}
    for start in adj:
        if start in canon:
            continue
        comp, q = [start], deque([start])
        canon[start] = None
        while q:
            x = q.popleft()
            for y in adj[x]:
                if y not in canon:
                    canon[y] = None
                    comp.append(y)
                    q.append(y)
        name = min(comp, key=lambda s: s.encode("utf-8"))
        for x in comp:
            canon[x] = name
    log(f"links: {len(edges)} lines, {len(canon)} linked ids")
    import pyarrow as pa
    cmap = pa.table({"uid": list(canon.keys()), "person": list(canon.values())})
    con.register("cmap_arrow", cmap)
    con.execute("CREATE TEMP TABLE cmap AS SELECT * FROM cmap_arrow")
    del adj, canon, cmap, edges

    con.execute(f"""
        CREATE TEMP TABLE ev_parsed AS
        SELECT fld(line,1) eid, fld(line,2) uid, fld(line,3) ts_s, ts_ms(fld(line,4)) ing,
               fld(line,5) et, fld(line,6) pid, fld(line,7) price_s
        FROM raw_events WHERE fields_ok(line, 7) AND fld(line,1) <> ''""")
    log("events parsed")
    con.execute(f"""
        CREATE TEMP TABLE ev AS
        WITH kept AS (
            SELECT * EXCLUDE (rn) FROM (
              SELECT *, row_number() OVER (PARTITION BY eid ORDER BY ing) rn FROM ev_parsed) WHERE rn = 1),
        v AS (
            SELECT uid, ts_ms(ts_s) ts, ing, et, pid,
                   CASE WHEN et = 'purchase' THEN
                        (CASE WHEN regexp_full_match(price_s, '[0-9]+') AND length(ltrim(price_s, '0')) BETWEEN 1 AND 8
                              THEN CAST(ltrim(price_s, '0') AS BIGINT) ELSE NULL END)
                        ELSE 0 END AS price
            FROM kept)
        SELECT coalesce(m.person, v.uid) person, v.ts, v.price, (v.et = 'purchase') is_p,
               coalesce(p.cat, 'unknown') cat
        FROM v LEFT JOIN cmap m ON m.uid = v.uid LEFT JOIN products p ON p.pid = v.pid
        WHERE v.uid <> '' AND v.pid <> '' AND v.et IN ('view', 'cart', 'purchase')
          AND v.ts IS NOT NULL AND v.ts >= {ws} AND v.ts < {we}
          AND v.price IS NOT NULL AND (v.et <> 'purchase' OR v.price BETWEEN 1 AND 10000000)
          AND v.ing - v.ts <= {DAY}""")
    con.execute("DROP TABLE ev_parsed")
    log(f"events kept: {con.execute('SELECT count(*) FROM ev').fetchone()[0]}")

    con.execute(f"""
        CREATE TEMP TABLE ev_s AS
        SELECT *, sum(CASE WHEN prev IS NULL OR ts - prev > 1800000 THEN 1 ELSE 0 END)
                  OVER (PARTITION BY person ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sid
        FROM (SELECT *, lag(ts) OVER (PARTITION BY person ORDER BY ts) prev FROM ev)""")
    con.execute("""
        CREATE TEMP TABLE sess AS
        SELECT person, sid, min(ts) start, count(*) n, sum(CASE WHEN is_p THEN price ELSE 0 END) rev,
               bool_or(is_p) has_p, (DATE '1970-01-01' + CAST(min(ts) // 86400000 AS INTEGER)) AS day
        FROM ev_s GROUP BY person, sid""")
    log(f"sessions: {con.execute('SELECT count(*) FROM sess').fetchone()[0]}")

    con.execute("""
        CREATE TEMP TABLE sess_day AS SELECT s.person, s.sid, s.day FROM sess s;
        CREATE TEMP TABLE catsum AS
        SELECT e.person, d.day, e.cat, sum(e.price) cents
        FROM ev_s e JOIN sess_day d ON d.person = e.person AND d.sid = e.sid
        WHERE e.is_p GROUP BY ALL;""")
    con.execute("""
        CREATE TEMP TABLE user_daily AS
        SELECT u.person AS user_id, u.day, u.sessions, u.events, u.revenue_cents, t.cat AS top_category
        FROM (SELECT person, day, count(*) sessions, sum(n) events, sum(rev) revenue_cents
              FROM sess GROUP BY person, day) u
        LEFT JOIN (SELECT person, day, cat FROM (
                     SELECT *, row_number() OVER (PARTITION BY person, day ORDER BY cents DESC, cat ASC) rn
                     FROM catsum) WHERE rn = 1) t
          ON t.person = u.person AND t.day = u.day""")
    con.execute("DROP TABLE ev_s; DROP TABLE catsum;")
    log("user_daily done")

    lo = ws - 7 * DAY
    con.execute(f"""
        CREATE TEMP TABLE ck AS
        WITH p AS (SELECT fld(line,1) cid, fld(line,2) uid, fld(line,3) camp, fld(line,4) ts_s
                   FROM raw_clicks WHERE fields_ok(line, 4) AND fld(line,1) <> ''),
        d AS (SELECT * EXCLUDE (rn) FROM (SELECT *, row_number() OVER (PARTITION BY cid) rn FROM p) WHERE rn = 1),
        v AS (SELECT cid, uid, camp, ts_ms(ts_s) ts FROM d)
        SELECT coalesce(m.person, v.uid) person, v.ts, v.cid, v.camp
        FROM v LEFT JOIN cmap m ON m.uid = v.uid
        WHERE v.uid <> '' AND v.camp IN (SELECT cid FROM campaigns)
          AND v.ts IS NOT NULL AND v.ts >= {lo} AND v.ts < {we}""")
    log(f"clicks kept: {con.execute('SELECT count(*) FROM ck').fetchone()[0]}")

    # Credit: best click per (person, ts) = smallest click_id; ASOF to the latest ts <= start.
    con.execute(f"""
        CREATE TEMP TABLE best AS SELECT person, ts, arg_min(camp, cid) camp FROM ck GROUP BY person, ts;
        CREATE TEMP TABLE credit AS
        SELECT s.person, s.sid, s.day, s.rev, s.has_p,
               CASE WHEN b.ts IS NOT NULL AND s.start - b.ts <= {7 * DAY} THEN b.camp ELSE NULL END camp
        FROM sess s ASOF LEFT JOIN best b ON s.person = b.person AND s.start >= b.ts""")
    # Assists: per (person, campaign) disjoint coverage intervals [ts, min(ts+7d, next_ts-1)];
    # sessions inside an interval form a contiguous rank range found with two ASOF joins.
    con.execute(f"""
        CREATE TEMP TABLE srank AS
        SELECT person, start, day, row_number() OVER (PARTITION BY person ORDER BY start, sid) r FROM sess;
        CREATE TEMP TABLE iv AS
        SELECT person, camp, ts lo, least(ts + {7 * DAY}, coalesce(nxt - 1, ts + {7 * DAY})) hi
        FROM (SELECT person, camp, ts, lead(ts) OVER (PARTITION BY person, camp ORDER BY ts) nxt
              FROM (SELECT DISTINCT person, camp, ts FROM ck));
        CREATE TEMP TABLE ivr AS
        SELECT a.person, a.camp, a.ra, b.r rb FROM
          (SELECT i.person, i.camp, i.hi, s.r ra FROM iv i ASOF JOIN srank s ON i.person = s.person AND i.lo <= s.start) a
          ASOF JOIN srank b ON a.person = b.person AND a.hi >= b.start
        WHERE b.r >= a.ra;
        CREATE TEMP TABLE assists AS
        SELECT s.day, x.camp, count(*) n FROM
          (SELECT person, camp, unnest(range(ra, rb + 1)) r FROM ivr) x
          JOIN srank s ON s.person = x.person AND s.r = x.r
        GROUP BY ALL;""")
    log("attribution + assists done")
    con.execute("""
        CREATE TEMP TABLE campaign_daily AS
        WITH cr AS (SELECT coalesce(camp, 'organic') camp, day, count(*) sessions,
                           count(*) FILTER (WHERE has_p) purchasing_sessions, sum(rev) revenue_cents
                    FROM credit GROUP BY ALL)
        SELECT coalesce(cr.camp, a.camp) AS campaign_id,
               coalesce(c.ch, 'organic') AS channel, coalesce(cr.day, a.day) AS day,
               coalesce(cr.sessions, 0) sessions, coalesce(cr.purchasing_sessions, 0) purchasing_sessions,
               coalesce(cr.revenue_cents, 0) revenue_cents, coalesce(a.n, 0) assisted_sessions
        FROM cr FULL OUTER JOIN assists a ON a.camp = cr.camp AND a.day = cr.day
        LEFT JOIN campaigns c ON c.cid = coalesce(cr.camp, a.camp)""")
    for t in ("user_daily", "campaign_daily"):
        os.makedirs(os.path.join(out, t), exist_ok=True)
        cols = ("user_id, day, sessions::BIGINT sessions, events::BIGINT events, revenue_cents::BIGINT revenue_cents, top_category"
                if t == "user_daily" else
                "campaign_id, channel, day, sessions::BIGINT sessions, purchasing_sessions::BIGINT purchasing_sessions, "
                "revenue_cents::BIGINT revenue_cents, assisted_sessions::BIGINT assisted_sessions")
        con.execute(f"COPY (SELECT {cols} FROM {t}) TO '{os.path.join(out, t, 'part-0.parquet')}' (FORMAT parquet)")
    log("written")


if __name__ == "__main__":
    main()
