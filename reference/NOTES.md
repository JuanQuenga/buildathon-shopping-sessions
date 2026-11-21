# Reference oracle: notes and resolved ambiguities

`reference/ref.py` is written from `rules.md` alone, without looking at the Rust pipeline.
It passes all 36 official fixtures and reproduces the public dev answer exactly
(117,038 + 27,400 rows; seed 1, 1M events; ~16 s on one core).

Ambiguities and how the reference resolves them (none are exercised by the 36 fixtures
or the dev answer unless noted; the generator does not produce them either):

1. **Final line without LF.** Treated as a complete line (parsed normally). An empty file
   (0 bytes) has no header and no rows. A file that is only a header with or without LF has no rows.
2. **Empty line / `\r`-only line.** An empty line is malformed. A line that is just `\r` is one
   non-empty field, so it has the wrong field count and is malformed too (same outcome).
3. **Quoted field followed by CR** (`...,"1000"\r` from CRLF files): the quote is not around the
   whole field (the field is `"1000"\r`), so the line is malformed and dropped.
4. **Lone `"`** is malformed (one quote cannot be a pair). `""` is an empty field (null).
   `"a"b`, `a"`, `"a""` and `a"b"` are malformed.
5. **F3 range.** Leading zeros are stripped before comparing; any digit string of any length is a
   valid number and simply falls outside the window when large. `0`/`000` is epoch 0.
6. **Unparseable `ingest_ts`** (contract says it never happens): treated as +infinity during dedup,
   so such a copy only survives if it is the only copy, and then it is late (dropped).
   Ties on equal `ingest_ts` keep the first copy read (files in name order); the contract says tied
   copies are identical.
7. **Click dedup** keeps the first copy read; contract says copies are identical.
8. **products.csv / campaigns.csv** are parsed with the same CSV dialect (quotes stripped,
   malformed lines dropped); first occurrence wins on a duplicate key.
9. **Manifest** timestamps are parsed with the same F1/F2/F3 parser.
10. **File discovery:** every regular file whose name ends in `.csv` directly in `events/`,
    `clicks/`, `links/` (including dot-files; the generator never writes those).
11. **Output strings** are decoded as UTF-8 (input is UTF-8 per contract); invalid UTF-8 in an ID
    would crash the reference rather than guess.
12. **Purchase price** must be 1..=10,000,000 after stripping leading zeros; `0000` is zero (bad).
