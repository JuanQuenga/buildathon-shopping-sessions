#!/usr/bin/env python3
"""Independent, literal reference implementation of the Shopping Sessions rules.

Written from rules.md only (never from the Rust pipeline). Slow but obviously
correct is the goal. Everything external (IDs, categories, campaigns) is handled
as raw bytes and compared byte-wise.

Usage: ref.py [DATA_DIR OUTPUT_DIR]   (or env DATA_DIR / OUTPUT_DIR)
Writes OUTPUT_DIR/user_daily/part-0.parquet and OUTPUT_DIR/campaign_daily/part-0.parquet.
Optional env REF_CSV=1 also writes OUTPUT_DIR/{user_daily,campaign_daily}.csv dumps
(only safe for IDs without commas/quotes/newlines, i.e. human inspection).
"""
import datetime
import json
import os
import sys
from bisect import bisect_left, bisect_right

DAY_MS = 86_400_000
SESSION_GAP_MS = 1_800_000
CLICK_LOOKBACK_MS = 7 * DAY_MS
LATE_MS = DAY_MS
MAX_PRICE = 10_000_000

EVENT_FIELDS = 7
CLICK_FIELDS = 4
LINK_FIELDS = 2

DIGITS = frozenset(b"0123456789")


# ---------------------------------------------------------------- CSV dialect

def parse_line(line, nfields):
    """Return the list of field values (bytes; b'' means null) or None if malformed.

    `line` is one physical line without its terminating LF. A CR is data.
    """
    if line == b"":
        return None  # an empty line is malformed
    fields = line.split(b",")
    if len(fields) != nfields:
        return None
    if b'"' not in line:
        return fields
    out = []
    for f in fields:
        if b'"' in f:
            # Exactly one pair of quotes around the whole field, no other quote.
            if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                out.append(f[1:-1])
            else:
                return None
        else:
            out.append(f)
    return out


def physical_lines(data):
    """Split file bytes into physical lines. Only LF ends a line.

    A final line without a trailing LF is still a line (NOTES.md #1).
    """
    if data == b"":
        return []
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()  # the empty string after the final LF is not a line
    return lines


def csv_rows(path, nfields):
    """Yield parsed rows of one CSV file, header (first physical line) skipped,
    malformed lines dropped."""
    with open(path, "rb") as fh:
        data = fh.read()
    lines = physical_lines(data)
    for line in lines[1:]:
        row = parse_line(line, nfields)
        if row is not None:
            yield row


def csv_files(directory):
    """Every regular file named *.csv directly inside `directory`, in a fixed order."""
    if not os.path.isdir(directory):
        return []
    names = sorted(n for n in os.listdir(directory) if n.endswith(".csv"))
    return [os.path.join(directory, n) for n in names if os.path.isfile(os.path.join(directory, n))]


# ---------------------------------------------------------------- timestamps

def is_leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def days_in_month(y, m):
    if m == 2:
        return 29 if is_leap(y) else 28
    return 30 if m in (4, 6, 9, 11) else 31


def days_from_civil(y, m, d):
    """Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant)."""
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    mp = (m + 9) % 12
    doy = (153 * mp + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def _digits(b):
    return len(b) > 0 and all(c in DIGITS for c in b)


def _civil_ms(b):
    """Parse 'YYYY-MM-DDTHH:MM:SS' (19 bytes) -> UTC-naive ms, or None."""
    if len(b) != 19:
        return None
    if b[4] != 0x2D or b[7] != 0x2D or b[10] != 0x54 or b[13] != 0x3A or b[16] != 0x3A:
        return None  # '-', '-', 'T', ':', ':'
    ys, mos, ds, hs, mis, ss = b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19]
    for part in (ys, mos, ds, hs, mis, ss):
        if not _digits(part):
            return None
    y, mo, d, h, mi, s = int(ys), int(mos), int(ds), int(hs), int(mis), int(ss)
    if not (1 <= mo <= 12):
        return None
    if not (1 <= d <= days_in_month(y, mo)):
        return None
    if h > 23 or mi > 59 or s > 59:
        return None
    return ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000


def parse_ts(b):
    """Parse an event/click ts (bytes) to epoch ms (Python int, unbounded), or None.

    F1 'YYYY-MM-DDTHH:MM:SSZ', F2 'YYYY-MM-DDTHH:MM:SS+HH:MM', F3 ASCII digits.
    Out-of-range F3 values are returned as (possibly huge) ints; the caller's
    window check rejects them.
    """
    n = len(b)
    if n == 0:
        return None
    if _digits(b):
        stripped = b.lstrip(b"0")
        if stripped == b"":
            return 0
        if len(stripped) > 30:
            # Far beyond any window (and beyond Python's int-from-str digit limit
            # for absurd lengths). Return a value certainly outside every window.
            return 10 ** 30
        return int(stripped)
    if n == 20 and b[19] == 0x5A:  # 'Z'
        return _civil_ms(b[:19])
    if n == 25 and b[19] in (0x2B, 0x2D) and b[22] == 0x3A:  # '+'/'-', ':'
        base = _civil_ms(b[:19])
        if base is None:
            return None
        oh, om = b[20:22], b[23:25]
        if not (_digits(oh) and _digits(om)):
            return None
        oh, om = int(oh), int(om)
        if oh > 23 or om > 59:
            return None
        offset = (oh * 60 + om) * 60_000
        return base - offset if b[19] == 0x2B else base + offset
    return None


# ---------------------------------------------------------------- linking

class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def add(self, x):
        if x not in self.parent:
            self.parent[x] = x

    def union(self, a, b):
        self.add(a)
        self.add(b)
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # Keep the byte-wise smaller ID as root so the root is the canonical name.
            if rb < ra:
                ra, rb = rb, ra
            self.parent[rb] = ra


# ---------------------------------------------------------------- pipeline

def load_lookup(path):
    """products.csv / campaigns.csv: 2-column clean files. Parsed with the same
    dialect for safety; the first occurrence of a key wins (keys are unique)."""
    table = {}
    if not os.path.isfile(path):
        return table
    for k, v in csv_rows(path, 2):
        if k not in table:
            table[k] = v
    return table


def run(data_dir, out_dir):
    with open(os.path.join(data_dir, "manifest.json"), "rb") as fh:
        manifest = json.loads(fh.read())
    ws = parse_ts(manifest["window_start"].encode())
    we = parse_ts(manifest["window_end"].encode())
    if ws is None or we is None:
        raise SystemExit("bad manifest window")

    products = load_lookup(os.path.join(data_dir, "products.csv"))
    campaigns = load_lookup(os.path.join(data_dir, "campaigns.csv"))

    # Links -> canonical person names.
    uf = UnionFind()
    for path in csv_files(os.path.join(data_dir, "links")):
        for a, b in csv_rows(path, LINK_FIELDS):
            if a == b"" or b == b"":
                continue
            uf.union(a, b)

    canon_cache = {}

    def canon(uid):
        c = canon_cache.get(uid)
        if c is None:
            c = uf.find(uid) if uid in uf.parent else uid
            canon_cache[uid] = c
        return c

    # Events: parse, global dedup on event_id keeping min ingest_ts.
    best = {}  # event_id -> (ingest_ms, row)
    for path in csv_files(os.path.join(data_dir, "events")):
        for row in csv_rows(path, EVENT_FIELDS):
            eid = row[0]
            if eid == b"":
                continue
            ing = parse_ts(row[3])
            if ing is None:
                # ingest_ts is guaranteed valid F1; treat a violation as +infinity
                # so it can only be kept if it is the sole copy (NOTES.md).
                ing = float("inf")
            cur = best.get(eid)
            if cur is None or ing < cur[0]:
                best[eid] = (ing, row)

    # Validate kept copies.
    person_events = {}  # person -> list of (ts, price_or_0, is_purchase, category)
    for eid, (ing, row) in best.items():
        _, uid, ts_b, _, etype, pid, price_b = row
        if uid == b"" or pid == b"":
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (ws <= ts < we):
            continue
        price = 0
        purchase = etype == b"purchase"
        if purchase:
            if not _digits(price_b):
                continue
            stripped = price_b.lstrip(b"0")
            if stripped == b"" or len(stripped) > 8:
                continue
            price = int(stripped)
            if not (1 <= price <= MAX_PRICE):
                continue
        if ing - ts > LATE_MS:
            continue
        category = products.get(pid, b"unknown")
        person_events.setdefault(canon(uid), []).append((ts, price, purchase, category))

    # Clicks: parse, dedup on click_id (keep first seen), validate.
    seen_clicks = {}
    for path in csv_files(os.path.join(data_dir, "clicks")):
        for row in csv_rows(path, CLICK_FIELDS):
            cid = row[0]
            if cid == b"":
                continue
            if cid not in seen_clicks:
                seen_clicks[cid] = row
    person_clicks = {}  # person -> list of (ts, click_id, campaign)
    for cid, (_, uid, camp, ts_b) in seen_clicks.items():
        if uid == b"" or camp not in campaigns:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (ws - CLICK_LOOKBACK_MS <= ts < we):
            continue
        person_clicks.setdefault(canon(uid), []).append((ts, cid, camp))
    for lst in person_clicks.values():
        lst.sort()

    # Sessionize, attribute, aggregate.
    user_daily = {}      # (person, day) -> [sessions, events, revenue, {cat: cents}]
    camp_daily = {}      # (campaign, day) -> [sessions, purchasing, revenue, assisted]

    def camp_row(key):
        r = camp_daily.get(key)
        if r is None:
            r = camp_daily[key] = [0, 0, 0, 0]
        return r

    for person, evs in person_events.items():
        evs.sort(key=lambda e: e[0])
        sessions = []  # [start_ts, events, revenue, purchases, {cat: cents}]
        prev = None
        for ts, price, purchase, cat in evs:
            if prev is None or ts - prev > SESSION_GAP_MS:
                sessions.append([ts, 0, 0, 0, {}])
            s = sessions[-1]
            s[1] += 1
            if purchase:
                s[2] += price
                s[3] += 1
                s[4][cat] = s[4].get(cat, 0) + price
            prev = ts
        clicks = person_clicks.get(person, [])
        click_ts = [c[0] for c in clicks]
        for start, n_events, revenue, n_purchases, cats in sessions:
            day = start // DAY_MS
            u = user_daily.get((person, day))
            if u is None:
                u = user_daily[(person, day)] = [0, 0, 0, {}]
            u[0] += 1
            u[1] += n_events
            u[2] += revenue
            for cat, cents in cats.items():
                u[3][cat] = u[3].get(cat, 0) + cents

            lo = bisect_left(click_ts, start - CLICK_LOOKBACK_MS)
            hi = bisect_right(click_ts, start)
            eligible = clicks[lo:hi]
            if eligible:
                latest = max(c[0] for c in eligible)
                credited = min(c[1] for c in eligible if c[0] == latest)
                credited_camp = next(c[2] for c in eligible if c[0] == latest and c[1] == credited)
                for camp in {c[2] for c in eligible}:
                    camp_row((camp, day))[3] += 1
            else:
                credited_camp = None  # organic
            r = camp_row((credited_camp, day))
            r[0] += 1
            r[1] += 1 if n_purchases > 0 else 0
            r[2] += revenue

    # Materialize rows.
    def top_category(cats):
        if not cats:
            return None
        best_cat = None
        best_cents = -1
        for cat, cents in cats.items():
            if cents > best_cents or (cents == best_cents and cat < best_cat):
                best_cat, best_cents = cat, cents
        return best_cat

    epoch = datetime.date(1970, 1, 1)
    ud = []
    for (person, day), (n_s, n_e, rev, cats) in user_daily.items():
        tc = top_category(cats)
        ud.append((person.decode("utf-8"), epoch + datetime.timedelta(days=day), n_s, n_e, rev,
                   None if tc is None else tc.decode("utf-8")))
    cd = []
    for (camp, day), (n_s, n_p, rev, n_a) in camp_daily.items():
        if camp is None:
            cid, ch = "organic", "organic"
        else:
            cid, ch = camp.decode("utf-8"), campaigns[camp].decode("utf-8")
        cd.append((cid, ch, epoch + datetime.timedelta(days=day), n_s, n_p, rev, n_a))
    ud.sort(key=lambda r: (r[0].encode(), r[1]))
    cd.sort(key=lambda r: (r[0].encode(), r[2]))
    write_outputs(out_dir, ud, cd)
    return ud, cd


def write_outputs(out_dir, ud, cd):
    import pyarrow as pa
    import pyarrow.parquet as pq

    ud_schema = pa.schema([("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()),
                           ("events", pa.int64()), ("revenue_cents", pa.int64()),
                           ("top_category", pa.string())])
    cd_schema = pa.schema([("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()),
                           ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()),
                           ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64())])
    for name, schema, rows in (("user_daily", ud_schema, ud), ("campaign_daily", cd_schema, cd)):
        d = os.path.join(out_dir, name)
        os.makedirs(d, exist_ok=True)
        cols = list(zip(*rows)) if rows else [[] for _ in schema]
        table = pa.Table.from_arrays([pa.array(list(c), type=f.type) for c, f in zip(cols, schema)],
                                     schema=schema)
        pq.write_table(table, os.path.join(d, "part-0.parquet"))
        if os.environ.get("REF_CSV"):
            with open(os.path.join(out_dir, name + ".csv"), "w", encoding="utf-8", newline="\n") as fh:
                fh.write(",".join(schema.names) + "\n")
                for r in rows:
                    fh.write(",".join("" if v is None else str(v) for v in r) + "\n")


def main():
    args = [a for a in sys.argv[1:]]
    data_dir = os.environ.get("DATA_DIR") or (args[0] if len(args) >= 1 else "/data")
    out_dir = os.environ.get("OUTPUT_DIR") or (args[1] if len(args) >= 2 else "/output")
    run(data_dir, out_dir)


if __name__ == "__main__":
    main()
