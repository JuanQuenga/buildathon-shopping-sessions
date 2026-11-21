#!/usr/bin/env python3
"""Writes the adversarial cases: make_cases.py OUT_DIR [--big]

Each case is OUT_DIR/<name>/input/ (layout of /data) plus OUT_DIR/<name>/CASE.md with the
rule it targets. Expected outputs are produced separately by the reference
(scripts/run_adversarial.sh). Small cases are committed under tests/adversarial/cases/;
--big also writes the large generated ones (heavy user, many parts, very long ids) which
stay outside the repo.
"""
import calendar
import os
import random
import shutil
import sys

DAY = 86_400_000
H = 3_600_000
M = 60_000


def ms(f1):
    """'YYYY-MM-DDTHH:MM:SS[.fff]Z' -> epoch ms (independent of the reference parser)."""
    assert f1.endswith("Z")
    body = f1[:-1]
    frac = 0
    if "." in body:
        body, f = body.split(".")
        frac = int(f.ljust(3, "0"))
    date, time = body.split("T")
    y, mo, d = map(int, date.split("-"))
    h, mi, s = map(int, time.split(":"))
    return calendar.timegm((y, mo, d, h, mi, s, 0, 0, 0)) * 1000 + frac


def f1(t):
    """epoch ms (whole second) -> F1."""
    assert t % 1000 == 0, t
    import time as _t
    return _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime(t // 1000))


def f3(t):
    return str(t)


EV_HDR = b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
CL_HDR = b"click_id,user_id,campaign_id,ts"
LN_HDR = b"user_id_a,user_id_b"


def b(x):
    return x if isinstance(x, bytes) else str(x).encode("utf-8")


def line(*fields):
    return b",".join(b(f) for f in fields)


class Case:
    def __init__(self, name, rule, start="2026-10-01T00:00:00Z", end="2026-10-15T00:00:00Z"):
        self.name, self.rule = name, rule
        self.start, self.end = start, end
        self.files = {}  # relative path -> bytes
        self.products = [("p1", "shoes"), ("p2", "books")]
        self.campaigns = [("c_1", "search"), ("c_2", "social")]
        self.events = {}  # file -> list of lines
        self.clicks = {}
        self.links = {}

    def ev(self, eid, uid, ts, ingest=None, etype="view", pid="p1", price="", file="part-00000.csv"):
        if ingest is None:
            t = ms(ts) if isinstance(ts, str) and ts.endswith("Z") and "T" in ts else None
            ingest = f1((t // 1000) * 1000 + H) if t is not None else "2026-10-05T00:00:00Z"
        self.events.setdefault(file, []).append(line(eid, uid, ts, ingest, etype, pid, price))

    def raw_ev(self, raw, file="part-00000.csv"):
        self.events.setdefault(file, []).append(b(raw))

    def ck(self, cid, uid, camp, ts, file="part-00000.csv"):
        self.clicks.setdefault(file, []).append(line(cid, uid, camp, ts))

    def raw_ck(self, raw, file="part-00000.csv"):
        self.clicks.setdefault(file, []).append(b(raw))

    def ln(self, a, bb, file="part-00000.csv"):
        self.links.setdefault(file, []).append(line(a, bb))

    def raw_ln(self, raw, file="part-00000.csv"):
        self.links.setdefault(file, []).append(b(raw))

    def write(self, root):
        d = os.path.join(root, self.name)
        if os.path.exists(d):
            shutil.rmtree(d)
        inp = os.path.join(d, "input")
        for sub in ("events", "clicks", "links"):
            os.makedirs(os.path.join(inp, sub))
        for sub, hdr, table in (("events", EV_HDR, self.events), ("clicks", CL_HDR, self.clicks),
                                ("links", LN_HDR, self.links)):
            if not table and not any(k.startswith(sub + "/") for k in self.files):
                table = {"part-00000.csv": []}
            for fname, lines in table.items():
                with open(os.path.join(inp, sub, fname), "wb") as fh:
                    fh.write(hdr + b"\n" + b"".join(l + b"\n" for l in lines))
        for rel, data in self.files.items():
            p = os.path.join(inp, rel)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "wb") as fh:
                fh.write(data)
        with open(os.path.join(inp, "products.csv"), "wb") as fh:
            fh.write(b"product_id,category\n" + b"".join(line(p, c) + b"\n" for p, c in self.products))
        with open(os.path.join(inp, "campaigns.csv"), "wb") as fh:
            fh.write(b"campaign_id,channel\n" + b"".join(line(c, ch) + b"\n" for c, ch in self.campaigns))
        with open(os.path.join(inp, "manifest.json"), "w") as fh:
            fh.write('{\n  "window_start": "%s",\n  "window_end": "%s"\n}\n' % (self.start, self.end))
        with open(os.path.join(d, "CASE.md"), "w", encoding="utf-8") as fh:
            fh.write(f"# {self.name}\n\n{self.rule}\n")


T = ms("2026-10-03T10:00:00Z")
WS = ms("2026-10-01T00:00:00Z")
WE = ms("2026-10-15T00:00:00Z")


def small_cases():
    cases = []

    c = Case("f3_huge_and_overflow",
             "rules.md Timestamps: F3 leading zeros are allowed; a value too long for a 64-bit integer is "
             "not malformed, only outside the window (must not wrap into the window).")
    c.ev("e1", "u1", "0" * 50 + f3(T), f1(T + H))
    c.ev("e2", "u2", "0" * 5000 + f3(T), f1(T + H))
    c.ev("e3", "u3", f3(T + 2 ** 64), f1(T + H))            # wraps to T in u64
    c.ev("e4", "u4", f3(T + 5 * 2 ** 64), f1(T + H))
    c.ev("e5", "u5", f3(T + 2 ** 128), f1(T + H))           # wraps to T in u128
    c.ev("e6", "u6", "9" * 25, f1(T + H))
    c.ev("e7", "u7", "0", f1(T + H))
    c.ev("e8", "u8", "86400000", f1(T + H))
    c.ev("e9", "u9", f3(T + 2 ** 63), f1(T + H))            # wraps negative in i64
    c.ev("e10", "u10", "0" * 19 + f3(T)[-0:], f1(T + H))
    c.ck("k1", "u1", "c_1", "000" + f3(T - H))
    c.ck("k2", "u2", "c_2", f3(T - H + 2 ** 64))            # dropped; would credit u2 if wrapped
    c.ck("k3", "u2", "c_1", "0" * 3000 + f3(T - 2 * H))     # credits u2
    cases.append(c)

    c = Case("nonascii_digits",
             "rules.md CSV dialect: digits mean ASCII 0-9 only, in F3, in F1/F2 fields and in purchase prices.")
    fw = str.maketrans("0123456789", "０１２３４５６７８９")
    ar = str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩")
    c.ev("e1", "u1", f1(T), f1(T + H))
    c.ev("e2", "u2", f3(T).translate(fw), f1(T + H))
    c.ev("e3", "u3", f3(T).translate(ar), f1(T + H))
    c.ev("e4", "u4", "2026-10-03T1०:00:00Z", f1(T + H))
    c.ev("e5", "u5", f3(T)[:-1] + "²", f1(T + H))
    c.ev("e6", "u6", "２026-10-03T10:00:00Z", f1(T + H))
    c.ev("e7", "u7", f1(T), f1(T + H), "purchase", "p1", "２５０")
    c.ev("e8", "u8", f1(T), f1(T + H), "purchase", "p1", "٢٥٠")
    c.ev("e9", "u9", f1(T), f1(T + H), "purchase", "p1", "1٠٠")
    c.ev("e10", "u10", f1(T), f1(T + H), "purchase", "p1", "250")
    c.ev("e11", "u11", f1(T), f1(T + H), "purchase", "p1", "²50")
    c.ev("e12", "u12", "2026-10-03T10:00:00+0５:00", f1(T + H))
    c.ck("k1", "u1", "c_1", f3(T - H).translate(fw))       # dropped
    c.ck("k2", "u1", "c_2", f3(T - 2 * H))                 # credits u1
    c.ck("k3", "u10", "c_1", "2026-10-03T0９:00:00Z")      # dropped -> u10 organic
    cases.append(c)

    c = Case("unicode_ids",
             "rules.md Linked IDs / Clicks / Outputs: byte-wise (UTF-8) comparison everywhere: canonical names, "
             "click_id ties, top_category ties. UTF-16 or locale order differs; no Unicode normalization.")
    c.products += [("p3", "éclair"), ("p4", "zebra"), ("p5", "～"), ("p6", "😀"), ("p7", "Zulu")]
    c.ln("é1", "zz")
    c.ln("😀", "～")
    c.ln("a1", "B1")
    c.ev("e1", "é1", f1(T), f1(T + H))
    c.ev("e2", "😀", f1(T), f1(T + H))
    c.ev("e3", "a1", f1(T), f1(T + H), "purchase", "p3", "500")
    c.ev("e4", "a1", f1(T + M), f1(T + H), "purchase", "p4", "500")
    c.ev("e5", "Ä", f1(T), f1(T + H), "purchase", "p5", "700")
    c.ev("e6", "Ä", f1(T + M), f1(T + H), "purchase", "p6", "700")
    c.ev("e7", "A\u0308", f1(T), f1(T + H))   # decomposed: a different user
    c.ev("e8", "w1", f1(T), f1(T + H), "purchase", "p4", "100")
    c.ev("e9", "w1", f1(T), f1(T + H), "purchase", "p7", "100")   # 'Zulu' < 'zebra'
    c.ev("e10", "x y", f1(T), f1(T + H))                     # NBSP id
    c.ev("e11", "tab\tid", f1(T), f1(T + H))
    c.ck("kz", "zz", "c_1", f1(T - H))
    c.ck("ké", "é1", "c_2", f1(T - H))          # tie: 'kz' (0x7A) < 'ké' (0xC3) -> c_1
    c.ck("😀", "～", "c_1", f1(T - H))
    c.ck("～", "😀", "c_2", f1(T - H))          # tie: U+FF5E < U+1F600 in UTF-8 -> c_2
    c.ck("b1", "a1", "c_1", f1(T - H))
    c.ck("B1", "B1", "c_2", f1(T - H))          # tie: 'B1' < 'b1' -> c_2
    cases.append(c)

    c = Case("crlf_everywhere",
             "rules.md CSV dialect: only LF ends a line; CR is data. CRLF files: view kept (price ignored), "
             "purchase price '1000\\r' bad, quoted last field followed by CR is malformed, click ts '...Z\\r' bad, "
             "link endpoint 'u2\\r' is a different ID.")
    crlf = lambda lines, hdr: hdr + b"\r\n" + b"".join(l + b"\r\n" for l in lines)
    c.files["events/part-00000.csv"] = crlf([
        line("e1", "u1", f1(T), f1(T + H), "view", "p1", ""),
        line("e2", "u1", f1(T + M), f1(T + H), "purchase", "p1", "1000"),
        line("e3", "u1", f1(T + 2 * M), f1(T + H), "purchase", "p2", '"700"'),
        line("e4", "u1", f1(T + 3 * M), f1(T + H), "cart", "p2", "5"),
        line("e5", "u2", f1(T), f1(T + H), "view", "p1", ""),
        line("e6", "u2\r", f1(T + 5 * M), f1(T + H), "view", "p1", ""),
        line("e7", "u3", f1(T), f1(T + H), "purchase", "p2", "800"),
    ], EV_HDR)
    c.files["events/part-00001.csv"] = EV_HDR + b"\n" + line("e8", "u3", f1(T + M), f1(T + H), "purchase", "p2", "900") + b"\n"
    c.files["clicks/part-00000.csv"] = crlf([line("k1", "u1", "c_1", f1(T - H)), line("k2", "u3", "c_2", f3(T - H))], CL_HDR)
    c.files["clicks/part-00001.csv"] = CL_HDR + b"\n" + line("k3", "u3", "c_1\r", f1(T - H)) + b"\n" + line("k4", "u2", "c_1", f1(T - H)) + b"\n"
    c.files["links/part-00000.csv"] = crlf([line("u1", "u2"), line("u3", "u4")], LN_HDR)
    c.files["links/part-00001.csv"] = LN_HDR + b"\n" + line("u5\r", "u1") + b"\n"
    c.ev("e9", "u5\r", f1(T + 10 * M), f1(T + H), file="part-00002.csv")
    cases.append(c)

    c = Case("header_rows",
             "rules.md Input/fixture L3: only the first line of each CSV is a header; a later line equal to the "
             "header is data (and must not crash a parser even though its ingest_ts is not a timestamp).")
    c.ev("e1", "u1", f1(T), f1(T + H))
    c.raw_ev(EV_HDR)
    c.ev("e2", "user_id_b", f1(T), f1(T + H))
    c.raw_ev(EV_HDR)
    c.raw_ck(CL_HDR)
    c.ck("k1", "u1", "c_1", f1(T - H))
    c.raw_ln(LN_HDR)
    cases.append(c)

    c = Case("empty_and_header_only_files",
             "rules.md Input: read every *.csv; some hold only the header. Empty (0-byte) files, header without LF, "
             "a links/ folder with no files, a clicks/ folder with only an empty file, non-.csv files ignored.")
    c.files["events/a-empty.csv"] = b""
    c.files["events/b-header-nolf.csv"] = EV_HDR
    c.files["events/c-header.csv"] = EV_HDR + b"\n"
    c.files["events/d-data.csv"] = EV_HDR + b"\n" + line("e1", "u1", f1(T), f1(T + H), "view", "p1", "") + b"\n"
    c.files["events/notes.txt"] = EV_HDR + b"\n" + line("e2", "u2", f1(T), f1(T + H), "view", "p1", "") + b"\n"
    c.files["events/e-data.csv.bak"] = EV_HDR + b"\n" + line("e3", "u3", f1(T), f1(T + H), "view", "p1", "") + b"\n"
    c.files["clicks/only-empty.csv"] = b""
    c.files["links/.keep"] = b""
    cases.append(c)

    c = Case("final_line_no_lf",
             "AMBIGUITY (reference NOTES.md #1): a final line without a trailing LF is treated as a line. The generator "
             "always ends files with LF, so this cannot affect scoring; informational.")
    c.files["events/part-00000.csv"] = EV_HDR + b"\n" + line("e1", "u1", f1(T), f1(T + H), "view", "p1", "") + b"\n" + line("e2", "u2", f1(T), f1(T + H), "purchase", "p1", "100")
    c.files["clicks/part-00000.csv"] = CL_HDR + b"\n" + line("k1", "u2", "c_1", f1(T - H))
    c.files["links/part-00000.csv"] = LN_HDR + b"\n" + line("u3", "u1")
    cases.append(c)

    c = Case("dedup_rules",
             "rules.md Events 1-4: malformed copies are dropped before dedup; dedup is global on event_id keeping the "
             "smallest ingest_ts; validation and the late check apply to the kept copy only (an invalid or late "
             "earliest copy suppresses the event; the earliest copy's user/ts/price win).")
    A, B = "part-00000.csv", "part-00001.csv"
    ts = lambda m: f1(T + m * M)
    ig = lambda hh, mm, ss=0: "2026-10-03T%02d:%02d:%02dZ" % (hh, mm, ss)
    c.raw_ev(line("e1", "u1", ts(0), ig(10, 30), "view", "p1"), A)                    # 6 fields: malformed
    c.ev("e1", "u1", ts(0), ig(10, 40), file=B)                                      # kept
    c.raw_ev(line("e2", '"u1', ts(1), ig(10, 30), "view", "p1", ""), A)               # unbalanced quote
    c.ev("e2", "u1", ts(1), ig(10, 40), file=B)                                      # kept
    c.ev("e3", "u1", "2026-10-03 10:05:00Z", ig(10, 30), file=A)                     # bad ts earliest
    c.ev("e3", "u1", ts(5), ig(10, 40), file=B)                                      # suppressed
    c.ev("e4", "u1", ts(6), ig(10, 30), "View", file=B)                              # bad type earliest
    c.ev("e4", "u1", ts(6), ig(10, 40), file=A)
    c.ev("e5", "u1", "2026-10-01T09:00:00Z", "2026-10-02T09:00:01Z", file=A)          # late earliest
    c.ev("e5", "u1", ts(7), ig(11, 0), file=B)
    c.ev("e6", "u1", ts(8), ig(10, 35), file=A)
    c.ev("e6", "u2", ts(8), ig(10, 30), file=B)                                      # kept for u2
    c.ev("e7", "", ts(9), ig(10, 30), file=A)                                        # empty user earliest
    c.ev("e7", "u1", ts(9), ig(10, 40), file=B)
    c.ev("e8", "u1", ts(10), ig(10, 30), "purchase", "p1", "0", file=B)              # bad price earliest
    c.ev("e8", "u1", ts(10), ig(10, 40), "purchase", "p1", "100", file=A)
    c.ev("e9", "u1", ts(11), ig(10, 30), "view", "", file=A)                         # empty product earliest
    c.ev("e9", "u1", ts(11), ig(10, 40), "view", "p1", file=B)
    c.ev("e10", "u1", ts(12), ig(10, 50), "purchase", "p1", "300", file=A)
    c.raw_ev(line('"e10"', '"u1"', '"' + ts(12) + '"', '"' + ig(10, 45) + '"', '"purchase"', '"p1"', '"300"'), B)
    c.raw_ev(line("e10", "u1", ts(12), ig(10, 55), "purchase", "p1", "300", "x"), A)  # malformed copy
    c.ev("e11", "u4", ts(13), ig(10, 30), file=B)
    c.ev("e11", "u3", ts(13), ig(10, 29, 59), file=A)                                 # kept for u3
    c.ev("e12", "u5", ts(14), ig(10, 50), "purchase", "p2", "999", file=A)
    c.ev("e12", "u5", ts(14), ig(10, 20), "view", "p2", "", file=B)                   # kept: a view
    c.raw_ev(line('""', "u1", ts(15), ig(10, 30), "view", "p1", ""), A)               # empty event_id
    c.ev("e14", "u6", "2026-10-03T10:00:00Z", "2026-10-04T10:00:00Z", file=A)        # exactly 24h: kept
    c.ev("e14", "u6", "2026-10-03T10:20:00Z", "2026-10-04T10:00:01Z", file=B)
    c.ev("e15", "u7", "2026-10-05T12:00:00Z", ig(10, 30), file=B)                    # negative delay, kept
    c.ev("e15", "u7", ts(10), ig(10, 31), file=A)
    c.ev("e16", "u8", ts(0), "2026-10-03T08:00:00Z", file=A)                         # kept copy earlier ingest
    c.ev("e16", "u8", ts(0), "2026-10-03T08:00:00Z", file=B)                         # identical tie copy
    c.ck("k1", "u2", "c_1", f1(T - H))
    cases.append(c)

    c = Case("quote_edges",
             "rules.md CSV dialect: one pair of quotes around a whole field is syntax; any other quote placement "
             "makes the line malformed (even in ignored fields); \"\" is null; spaces inside quotes are kept.")
    ok = lambda eid, uid, **kw: c.ev(eid, uid, f1(T), f1(T + H), **kw)
    ok("e1", "q1")
    c.raw_ev(line("e2", '"q2"x', f1(T), f1(T + H), "view", "p1", ""))
    c.raw_ev(line("e3", 'q3"', f1(T), f1(T + H), "view", "p1", ""))
    c.raw_ev(line("e4", '""', f1(T), f1(T + H), "view", "p1", ""))           # null user -> dropped
    c.raw_ev(line("e5", '"', f1(T), f1(T + H), "view", "p1", ""))            # lone quote
    c.raw_ev(line("e6", '"""', f1(T), f1(T + H), "view", "p1", ""))
    c.raw_ev(line("e7", '""""', f1(T), f1(T + H), "view", "p1", ""))
    c.raw_ev(line("e8", '"q8"', f1(T), f1(T + H), "view", "p1", ""))         # ok -> q8
    c.raw_ev(line("e9", '" q9 "', f1(T), f1(T + H), "view", "p1", ""))       # ' q9 '
    c.raw_ev(line("e10", '"q10', f1(T), f1(T + H), "view", "p1", ""))
    c.raw_ev(line("e11", 'q1"1"', f1(T), f1(T + H), "view", "p1", ""))
    c.raw_ev(line("e12", "q12", f1(T), f1(T + H), "purchase", "p1", '"0250"'))  # 250
    c.raw_ev(line("e13", "q13", '"' + f1(T) + '"', f1(T + H), "view", "p1", ""))
    c.raw_ev(line("e14", "q14", f1(T), f1(T + H), "view", "p1", '1"0'))     # quote in ignored field
    c.raw_ev(line("e15", "q15", f1(T), f1(T + H), "view", "p1", '"abc"'))   # fine
    c.raw_ev(line("e16", "q16", f1(T), f1(T + H), "view", "p1", '""'))      # fine
    c.raw_ev(line("e17", '" "', f1(T), f1(T + H), "view", "p1", ""))         # user ' '
    c.raw_ev(line("e18", "q18", f1(T), f1(T + H), '"view"', '"p2"', ""))
    c.raw_ev(line("e19", "q19", f1(T), f1(T + H), "view", "p1", '"'))       # lone quote in ignored field
    c.raw_ev(line('"e20', "q20", f1(T), f1(T + H), "view", "p1", ""))
    c.raw_ev(line("e21", "q21", f1(T), f1(T + H), "view ", "p1", ""))        # bad type (space)
    c.raw_ev(line("e22", "q22", f1(T), f1(T + H), '" view"', "p1", ""))      # bad type
    c.raw_ck(line('"k1"', '"q1"', '"c_1"', '"' + f1(T - H) + '"'))
    c.raw_ck(line("k1", "q1", "c_1", f1(T - H)))
    c.raw_ck(line("k2", 'q8"', "c_2", f1(T - H)))                           # malformed
    c.raw_ck(line("k3", "q8", '"c_2', f1(T - H)))                           # malformed
    c.raw_ln(line('"q12"', '"q13"'))
    c.raw_ln(line('q14', '"q15'))                                           # malformed
    cases.append(c)

    c = Case("field_counts_blank_lines",
             "rules.md CSV dialect: a line with the wrong number of fields is dropped; an empty line is malformed; "
             "a line of spaces or a lone CR is one field (wrong count).")
    c.ev("e1", "f1", f1(T), f1(T + H))
    c.raw_ev(line("e2", "f2", f1(T), f1(T + H), "view", "p1"))
    c.raw_ev(line("e3", "f3", f1(T), f1(T + H), "view", "p1", "", ""))
    c.raw_ev(b"")
    c.raw_ev(b"   ")
    c.raw_ev(b"\r")
    c.raw_ev(b",,,,,,")
    c.raw_ev(b"")
    c.ev("e4", "f4", f1(T), f1(T + H))
    c.raw_ev(line("e5", "f5", f1(T), f1(T + H), "view", "p1", "", "", "", ""))
    c.raw_ck(line("k1", "f1", "c_1"))
    c.raw_ck(line("k2", "f1", "c_1", f1(T - H), ""))
    c.raw_ck(b"")
    c.ck("k3", "f4", "c_2", f1(T - H))
    c.raw_ln(line("f1"))
    c.raw_ln(line("f1", "f4", "f9"))
    c.raw_ln(b"")
    c.raw_ln(b",")
    cases.append(c)

    c = Case("offsets",
             "rules.md Timestamps F2: offset hours 00-23 and minutes 00-59 with a colon; subtract to get UTC; "
             "-00:00 is UTC; the window and the session day use the UTC instant.")
    for i, ts in enumerate([
        "2026-10-03T10:00:00+23:59", "2026-10-03T10:00:00-23:59", "2026-10-03T10:00:00-00:00",
        "2026-10-03T10:00:00+00:00", "2026-10-03T10:00:00+24:00", "2026-10-03T10:00:00+05:60",
        "2026-10-03T10:00:00+0500", "2026-10-03T10:00:00+5:00", "2026-10-03T10:00:00+05:00:00",
        "2026-10-03T10:00:00+05", "2026-10-03T10:00:00−05:00", "2026-10-03T10:00:00 +05:00",
        "2026-10-01T05:00:00+05:00", "2026-10-01T04:59:59+05:00", "2026-10-14T18:59:59-05:00",
        "2026-10-14T19:00:00-05:00", "2026-09-30T23:30:00-01:00", "2026-10-03T10:00:00+05:00Z",
        "2026-10-03T10:00:00Z+05:00", "2026-10-03T10:00:00+-5:00", "2026-10-03T10:00:00-99:00",
        "2026-10-15T09:00:00+10:00", "2026-10-03T10:00:00+23:60", "2026-10-03T10:00:00+00:59",
    ]):
        c.ev(f"e{i}", f"o{i:02d}", ts, "2026-09-25T00:00:00Z")
    # click at exactly s - 7d expressed with an offset
    c.ev("x1", "oc", "2026-10-09T10:00:00Z", "2026-10-09T10:00:00Z")
    c.ck("k1", "oc", "c_1", "2026-10-02T15:00:00+05:00")   # 10:00Z on Oct 2 = s - 7d
    c.ck("k2", "oc", "c_2", "2026-10-02T09:59:59-00:00")   # 1 s too early
    cases.append(c)

    c = Case("calendar_leap_2028",
             "rules.md Timestamps: February 29 needs a leap year; impossible calendar values (Feb 30, day 00, "
             "month 13, hour 24, minute 60, second 60) are bad even if they would land in the window.",
             "2028-02-24T00:00:00Z", "2028-03-09T00:00:00Z")
    for i, ts in enumerate([
        "2028-02-29T12:00:00Z", "2028-02-30T12:00:00Z", "2028-02-31T12:00:00Z", "2028-03-00T12:00:00Z",
        "2028-13-01T12:00:00Z", "2028-02-29T24:00:00Z", "2028-02-29T23:60:00Z", "2028-02-29T23:59:60Z",
        "2028-03-01T00:30:00+01:00", "2028-02-28T23:50:00Z", "2028-00-29T12:00:00Z", "2028-04-31T12:00:00Z",
    ]):
        c.ev(f"e{i}", f"d{i:02d}", ts, "2028-02-01T00:00:00Z")
    c.ev("e99", "d09", "2028-02-29T00:10:00Z", "2028-02-01T00:00:00Z")   # same session as 02-28 23:50
    c.ck("k1", "d00", "c_1", "2028-02-22T12:00:00Z")    # before window, within 7 days
    c.ck("k2", "d09", "c_2", "2028-02-29T12:00:00Z")    # after session start -> no credit
    cases.append(c)

    c = Case("calendar_nonleap_2027",
             "rules.md Timestamps: 2027-02-29 is impossible (not a leap year).",
             "2027-02-20T00:00:00Z", "2027-03-06T00:00:00Z")
    c.ev("e1", "n1", "2027-02-29T10:00:00Z", "2027-02-01T00:00:00Z")
    c.ev("e2", "n2", "2027-03-01T00:30:00+01:00", "2027-02-01T00:00:00Z")
    c.ev("e3", "n3", "2027-02-28T10:00:00Z", "2027-02-01T00:00:00Z")
    c.ck("k1", "n3", "c_1", "2027-02-29T09:00:00Z")
    cases.append(c)

    c = Case("calendar_century_2100",
             "rules.md Timestamps: Gregorian leap rule (2100 is not a leap year). Beyond the generator's 2025-2028 "
             "windows, but the manifest window is arbitrary.",
             "2100-02-22T00:00:00Z", "2100-03-08T00:00:00Z")
    c.ev("e1", "y1", "2100-02-29T10:00:00Z", "2100-02-01T00:00:00Z")
    c.ev("e2", "y2", "2100-03-01T10:00:00Z", "2100-02-01T00:00:00Z")
    c.ev("e3", "y3", f3(ms("2100-03-01T10:00:00Z")), "2100-03-01T11:00:00Z")
    c.ck("k1", "y2", "c_1", f3(ms("2100-02-22T10:00:00Z")))
    cases.append(c)

    c = Case("window_and_late_edges",
             "rules.md Timestamps/Events 4: event window [start, end); click window [start-7d, end); late means "
             "ingest_ts - ts > 24 h (exactly 24 h kept, 24 h + 1 ms dropped, negative delay kept).")
    I = ms("2026-10-05T00:00:00Z")
    c.ev("e1", "w01", f1(WS), f1(WS + H))
    c.ev("e2", "w02", f3(WS), f1(WS + H))
    c.ev("e3", "w03", f3(WS - 1), f1(WS + H))
    c.ev("e4", "w04", f3(WE - 1), f1(WE - 1000))
    c.ev("e5", "w05", f1(WE), f1(WE))
    c.ev("e6", "w06", f3(I - DAY), f1(I))
    c.ev("e7", "w07", f3(I - DAY - 1), f1(I))
    c.ev("e8", "w08", f3(I + 5 * DAY), f1(I))
    c.ev("e9", "w09", f3(I - DAY + 1), f1(I))
    c.ck("k1", "w01", "c_1", f1(WS - 7 * DAY))
    c.ck("k2", "w02", "c_2", f3(WS - 7 * DAY - 1))
    c.ck("k3", "w04", "c_1", f3(WE - 1))
    c.ck("k4", "w04", "c_2", f1(WE))
    c.ck("k5", "w02", "c_1", f3(WS - 7 * DAY))
    cases.append(c)

    c = Case("click_edges_and_ties",
             "rules.md Clicks 4/7: eligible clicks satisfy s-7d <= ts <= s; latest ts wins; ties go to the "
             "byte-wise smallest click_id (not numeric, not case-insensitive); every eligible campaign assists once.")
    c.campaigns.append(("c_3", "email"))
    S = ms("2026-10-08T12:00:00Z")
    for u in ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"]:
        c.ev("e_" + u, u, f1(S), f1(S + H))
    c.ck("kA", "t1", "c_3", f1(S - 7 * DAY))
    c.ck("kB", "t1", "c_2", f3(S - 7 * DAY - 1))
    c.ck("k10", "t1", "c_1", f1(S))
    c.ck("k9", "t1", "c_2", f3(S))
    c.ck("k", "t2", "c_2", f1(S - 5 * M))
    c.ck("k0", "t2", "c_1", f1(S - 5 * M))
    c.ck("K1", "t3", "c_1", f1(S - 5 * M))
    c.ck("k0x", "t3", "c_2", f1(S - 5 * M))
    c.ck("k_t4", "t4", "c_2", f3(S + 1))
    c.ck("k_t5", "t5", "c_2", f3(S - 7 * DAY))
    c.ck("100", "t6", "c_1", f1(S - M))
    c.ck("99", "t6", "c_2", f1(S - M))                 # '100' < '99'
    c.ck("m1", "t7", "c_1", f1(S - M))
    c.ck("m2", "t7", "c_1", f1(S - M))
    c.ck("m3", "t7", "c_1", f1(S - 2 * M))
    c.ck("k_b", "t8", "c_1", f1(S - M))
    c.ck("k_a", "t8", "c_2", f1(S - M))
    c.ck("k_c", "t8", "c_3", f3(S - M - 1))
    cases.append(c)

    c = Case("click_mid_session",
             "rules.md Clicks 4/6: a click during a session cannot credit that session (start already passed) but "
             "credits later sessions up to exactly 7 days after it; clicks never split sessions.")
    s1 = ms("2026-10-03T10:00:00Z")
    for i, off in enumerate([0, 20 * M, 40 * M]):
        c.ev(f"a{i}", "m1", f1(s1 + off), f1(s1 + H))
    c.ev("b0", "m1", f1(s1 + 2 * H), f1(s1 + 3 * H))                   # session 2
    c.ev("c0", "m1", f1(s1 + 20 * M + 7 * DAY), f1(s1 + 7 * DAY + H))   # exactly 7d after click
    c.ev("d0", "m2", f1(s1), f1(s1 + H))
    c.ev("d1", "m2", f3(s1 + 20 * M + 7 * DAY + 1), f1(s1 + 7 * DAY + H))
    c.ck("k1", "m1", "c_1", f1(s1 + 20 * M))
    c.ck("k2", "m2", "c_2", f1(s1 + 20 * M))
    cases.append(c)

    c = Case("links_weird",
             "rules.md Linked IDs: empty endpoints drop the line; self/reversed/repeated links change nothing; "
             "joins are transitive across files in any order (long chains); the person is named by the byte-wise "
             "smallest ID even if it only appears in links; a CR or space makes a different ID.")
    rnd = random.Random(7)
    c.raw_ln(line("a1", "")); c.raw_ln(line("", "a2")); c.raw_ln(line('""', "a3")); c.raw_ln(line("a4", '""'))
    c.ln("a1", "a1"); c.ln("a5", "a5")
    chain = [f"ch{i:04d}" for i in range(2000)]
    edges = [(chain[i], chain[i + 1]) if rnd.random() < 0.5 else (chain[i + 1], chain[i]) for i in range(1999)]
    edges.append(("ch1500", "ch"))
    rnd.shuffle(edges)
    for i, (x, y) in enumerate(edges):
        c.ln(x, y, file=f"part-0000{i % 3}.csv")
    c.ln("Z9", "a9"); c.ln("a9", "zz9"); c.ln("zz9", "a9")
    c.raw_ln(line('"b1"', '"b2"'))
    c.raw_ln(b"b3,b4\r")
    c.ln(" b5", "b6")
    c.ln("clk", "a9")
    star = [f"dev{i:04d}" for i in range(3000)]
    for x in star[1:]:
        c.ln("hub", x, file="part-00003.csv")
    for i, x in enumerate([chain[1999], chain[1000], "a1", "a2", "a3", "a4", "a5", "a9", "b2", "b4", "b5", "b6"]):
        c.ev(f"l{i}", x, f1(T + i * H), f1(T + i * H + H))
    for i, x in enumerate(star[1:]):
        c.ev(f"s{i}", x, f3(T + i * 7 * 1000), f1(T))         # 7 s apart: one long session
    c.ck("k1", "clk", "c_1", f1(T + 6 * H))
    c.ck("k2", "dev2999", "c_2", f1(T - 7 * DAY))
    cases.append(c)

    c = Case("sessions_midnight_and_gaps",
             "rules.md Events 5/6: a gap of exactly 30 min stays in the session (chains can span days); over 30 min "
             "splits; equal timestamps share a session; the session's day is its start's UTC date.")
    t = ms("2026-10-03T22:00:00Z")
    for i in range(0, 61):                                 # every 30 min for 30 hours
        c.ev(f"g{i}", "s1", f1(t + i * 30 * M), f1(t + i * 30 * M + H), "purchase" if i == 60 else "view", "p1",
             "100" if i == 60 else "")
    t2 = t + 60 * 30 * M + 30 * M + 1
    c.ev("g_split", "s1", f3(t2), f1(t2 - t2 % 1000 + H))
    m0 = ms("2026-10-06T00:00:00Z")
    c.ev("h0", "s2", f3(m0 - 1), f1(m0 + H))
    c.ev("h1", "s2", f3(m0), f1(m0 + H))
    c.ev("h2", "s3", f3(m0), f1(m0 + H))
    c.ev("h3", "s3", f3(m0 + 30 * M), f1(m0 + H))
    c.ev("h4", "s3", f3(m0 + 30 * M), f1(m0 + H), file="part-00001.csv")
    c.ev("h5", "s3", f3(m0 + 60 * M + 1), f1(m0 + 2 * H), file="part-00001.csv")
    c.ev("h6", "s3", f3(m0 + 60 * M + 1), f1(m0 + 2 * H))
    c.ck("k1", "s2", "c_1", f3(m0 - 1))
    cases.append(c)

    c = Case("prices",
             "rules.md Events 3: purchase price must be ASCII digits with value 1..10,000,000 (leading zeros ok); "
             "a bad price drops the whole purchase; view/cart prices are ignored.")
    for i, p in enumerate(["0", "1", "10000000", "10000001", "0250", "000000000000000000000000010000000", "",
                           " 100", "+100", "100.0", "1e3", "-5", "99999999999999999999999", '"100"', "0x10",
                           "100 ", "00", "18446744073709551716", "4294967396"]):
        c.raw_ev(line(f"e{i}", f"r{i:02d}", f1(T), f1(T + H), "purchase", "p1", p))
    c.raw_ev(line("v1", "r99", f1(T), f1(T + H), "view", "p1", "-5"))
    c.raw_ev(line("v2", "r99", f1(T), f1(T + H), "cart", "p1", "abc"))
    c.raw_ev(line("v3", "r99", f1(T), f1(T + H), "view", "p1", "99999999999999999999999"))
    c.files["events/part-00001.csv"] = EV_HDR + b"\r\n" + line("e100", "r100", f1(T), f1(T + H), "purchase", "p1", "1000") + b"\r\n"
    cases.append(c)

    c = Case("top_category_unknown",
             "rules.md Events 7 / Outputs: unknown products get category 'unknown', which competes; ties by byte-wise "
             "smallest category; amounts summed over all of the day's sessions; null without purchases.")
    c.products += [("pa", "unknowa"), ("pz", "unknownz"), ("pS", "Shoes")]
    P = lambda eid, uid, pid, price, off=0: c.ev(eid, uid, f1(T + off), f1(T + off + H), "purchase", pid, price)
    P("a1", "t1", "p9", "500"); P("a2", "t1", "pa", "500")
    P("b1", "t2", "p9", "500"); P("b2", "t2", "pz", "500")
    P("c1", "t3", "P1", "700"); P("c2", "t3", "p1", "300")
    P("d1", "t4", " p1", "100")
    P("e1", "t5", "p1", "600"); P("e2", "t5", "p2", "400", 2 * H); P("e3", "t5", "p2", "300", 3 * H)
    P("f1", "t6", "p1", "100"); P("f2", "t6", "p2", "100", DAY)
    c.ev("g1", "t7", f1(T), f1(T + H)); c.ev("g2", "t7", f1(T + M), f1(T + H), "cart", "p2", "")
    P("h1", "t8", "", "100")
    P("i1", "t9", "pS", "100"); P("i2", "t9", "p1", "100")
    P("j1", "t10", "p1", "200"); P("j2", "t10", "p2", "100", 2 * H); P("j3", "t10", "p2", "100", 3 * H)
    cases.append(c)

    c = Case("assists_rich",
             "rules.md Clicks 7 / Outputs: each eligible campaign assists a session once (including the credited "
             "one); assist-only (campaign, day) rows have 0 credited measures; assists land on the session's day.")
    c.campaigns += [("c_3", "email"), ("c_4", "display")]
    s = ms("2026-10-05T10:00:00Z")
    for i, d in enumerate([0, 1, 2, 6]):
        c.ev(f"e{i}", "x1", f1(s + d * DAY), f1(s + d * DAY + H), "purchase" if d == 1 else "view", "p1",
             "500" if d == 1 else "")
    for j in range(5):
        c.ck(f"kc1_{j}", "x1", "c_1", f1(s - DAY + j * M))
    c.ck("kc3", "x1", "c_3", f1(s - 2 * DAY))
    c.ck("kc2", "x1", "c_2", f1(s - 30 * M))
    c.ck("kc4", "x1", "c_4", f1(s - 8 * DAY))           # too old for every session
    c.ck("kc4b", "x1", "c_4", f1(s + DAY + H))          # mid-day between sessions: credits day 2 and 6
    c.ev("o1", "x2", f1(s), f1(s + H))                   # organic, no assists
    cases.append(c)

    c = Case("all_empty_output",
             "rules.md Outputs: with no surviving events both tables are empty but must still be written as valid "
             "Parquet with the exact schema (DATE day).")
    c.ev("e1", "u1", "2026-10-03T10:00:00", f1(T + H))
    c.ck("k1", "u1", "c_1", f1(T - H))
    cases.append(c)

    c = Case("clicks_without_events",
             "rules.md Outputs: campaign rows exist only for sessions; valid clicks of a person with no surviving "
             "events produce nothing.")
    c.ev("e1", "u1", f1(T), f1(T + H))
    c.ck("k1", "u2", "c_1", f1(T - H))
    c.ck("k2", "u3", "c_2", f1(T - H))
    c.ev("e2", "u3", "bad", f1(T + H))
    cases.append(c)

    c = Case("click_dedup_and_validation",
             "rules.md Clicks 1-3: malformed or empty-click_id lines dropped; dedup on click_id; empty user, unknown "
             "campaign (case/space variants) or out-of-window ts drop the click.")
    c.ev("e1", "v1", f1(T), f1(T + H))
    c.ev("e2", "v2", f1(T), f1(T + H))
    c.ev("e3", "v3", f1(T), f1(T + H))
    c.ev("e4", "v4", f1(T), f1(T + H))
    c.raw_ck(line('"k1"', "v1", "c_1", f1(T - H)))
    c.raw_ck(line("k1", '"v1"', '"c_1"', f1(T - H)), file="part-00001.csv")
    c.raw_ck(line("", "v2", "c_1", f1(T - H)))
    c.raw_ck(line('""', "v2", "c_1", f1(T - H)))
    c.ck("k2", "", "c_1", f1(T - H))
    c.ck("k3", "v3", "C_1", f1(T - H))
    c.ck("k4", "v3", " c_1", f1(T - H))
    c.ck("k5", "v3", "c_1 ", f1(T - H))
    c.ck("k6", "v4", "c_2", f1(WS - 7 * DAY - 1000))
    c.ck("k7", "v4", "c_1", "2026-09-24T00:00:00Z")
    cases.append(c)

    c = Case("revenue_over_32bit",
             "rules.md Outputs: signed 64-bit sums; revenue far above 2^32 in one user-day and one campaign-day.")
    for i in range(450):
        c.ev(f"e{i}", "big", f3(T + i * 1000), f1(T + H), "purchase", "p1" if i % 3 else "p2", "10000000")
    c.ck("k1", "big", "c_1", f1(T - H))
    cases.append(c)

    return cases


def big_cases(seed=11):
    cases = []
    rnd = random.Random(seed)

    c = Case("big_many_parts",
             "rules.md Input: many files of varying size (incl. empty and header-only) must all be read.")
    for i in range(400):
        c.ev(f"e{i}", f"u{i % 37}", f3(T + rnd.randrange(0, 3 * DAY)), f1(T), "view", "p1", "",
             file=f"part-{i:05d}.csv")
    for i in range(400, 410):
        c.files[f"events/part-{i:05d}.csv"] = b"" if i % 2 else EV_HDR + b"\n"
    for i in range(150):
        c.ck(f"k{i}", f"u{i % 37}", "c_1" if i % 2 else "c_2", f3(T + rnd.randrange(-6 * DAY, 3 * DAY)),
             file=f"part-{i:05d}.csv")
    for i in range(60):
        c.ln(f"u{i % 37}", f"u{(i * 7) % 37}", file=f"part-{i:05d}.csv")
    cases.append(c)

    c = Case("big_very_long_ids",
             "rules.md: IDs have no length limit; multi-megabyte lines must not break block splitting or buffers.")
    long_user = "u" + "x" * 3_000_000
    long_eid = "e" + "y" * 1_000_000
    long_click = "k" + "z" * 300_000
    long_prod = "p" + "w" * 100_000
    long_camp = "c_" + "v" * 50_000
    c.products.append((long_prod, "longcat"))
    c.campaigns.append((long_camp, "longchannel"))
    c.ev(long_eid, long_user, f1(T), f1(T + H), "purchase", long_prod, "1234")
    c.ev("e2", long_user, f1(T + M), f1(T + H))
    c.ev("e3", "short", f1(T), f1(T + H))
    for i in range(20000):  # filler around the long lines so chunked readers split near them
        c.ev(f"f{i}", f"filler{i % 100}", f3(T + i * 10_000), f1(T), file="part-00001.csv")
    c.ev(long_eid[:-1], "short", f1(T + M), f1(T + H), file="part-00001.csv")
    c.ck(long_click, long_user, long_camp, f1(T - H))
    c.ck(long_click[:-1], long_user, "c_1", f1(T - H))   # tie: shorter prefix is smaller -> c_1
    c.ln(long_user, "u" + "x" * 2_999_999 + "a")
    cases.append(c)

    c = Case("big_heavy_user",
             "rules.md Events 5 / Clicks: one person with ~1M events over 6 linked IDs, dense ties, duplicates, "
             "mixed timestamp forms, 40 campaigns; stresses per-person sort, sessions and assists.")
    c.campaigns = [(f"c_{i}", f"ch{i % 5}") for i in range(1, 41)]
    ids = [f"heavy{i}" for i in range(6)]
    for i in range(1, 6):
        c.ln(ids[i], ids[rnd.randrange(i)], file="part-00000.csv")
    t = WS
    n = 0
    while n < 1_000_000:
        t += rnd.choice([0, 0, 1, 1000, 7000, 60_000, 30 * M, 30 * M + 1, 31 * M, 4 * H])
        if t >= WE:
            t = WS + rnd.randrange(0, 14 * DAY)
        uid = rnd.choice(ids)
        form = rnd.random()
        ts = f3(t) if (t % 1000 or form < 0.3) else f1(t) if form < 0.85 else \
            (lambda x: f1(x + 2 * H)[:-1] + "+02:00")(t)
        typ = "purchase" if rnd.random() < 0.05 else "view"
        price = str(rnd.randrange(1, 50000)) if typ == "purchase" else ""
        pid = f"p{rnd.randrange(1, 4)}"
        ingest = f1((t // 1000) * 1000 + rnd.choice([0, 1000, H, DAY, DAY + 1000]))
        c.ev(f"h{n}", uid, ts, ingest, typ, pid, price, file=f"part-{n % 7:05d}.csv")
        if rnd.random() < 0.03:
            c.ev(f"h{n}", rnd.choice(ids), ts, f1((t // 1000) * 1000 + DAY + 5000), typ, pid, price,
                 file=f"part-{(n + 3) % 7:05d}.csv")
        n += 1
    c.products.append(("p3", "toys"))
    for k in range(30000):
        ct = WS - 7 * DAY + rnd.randrange(0, 21 * DAY)
        c.ck(f"k{k}", rnd.choice(ids), f"c_{rnd.randrange(1, 41)}", f3(ct), file=f"part-{k % 3:05d}.csv")
        if rnd.random() < 0.05:
            c.ck(f"k{k}x", rnd.choice(ids), f"c_{rnd.randrange(1, 41)}", f3(ct), file=f"part-{k % 3:05d}.csv")
    for j in range(2000):
        c.ev(f"o{j}", f"other{j % 300}", f3(WS + rnd.randrange(0, 14 * DAY)), f1(WS), "view", "p1", "",
             file="part-00007.csv")
    cases.append(c)
    return cases


def main():
    out = sys.argv[1]
    big = "--big" in sys.argv
    os.makedirs(out, exist_ok=True)
    for case in (big_cases() if big else small_cases()):
        case.write(out)
        print(case.name)


if __name__ == "__main__":
    main()
