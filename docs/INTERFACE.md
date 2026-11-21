# Ingest -> aggregate interface

Authority: ingest agent (owns `Cargo.toml`, `src/main.rs`, `src/types.rs`, `src/ingest/`).
Changes are announced here with a dated changelog entry.

## Entry points

```rust
// src/ingest/mod.rs
pub fn load(data_dir: &Path, threads: usize) -> types::Loaded;
// src/aggregate/mod.rs  (owned by aggregate agent; main.rs calls exactly this)
pub fn run(loaded: types::Loaded, output_dir: &Path, threads: usize);
```

`main.rs` reads `DATA_DIR` (default `/data`), `OUTPUT_DIR` (default `/output`), `NCPU`
(default: available parallelism), ignores argv, calls `load` then `run`, then
`std::process::exit(0)` (no destructors). `run` must have written and closed both tables
before returning. Stage timings go to stderr as `[stage] name 1.234s`.

## Data contract (`src/types.rs`)

* Person-grouped CSR. Person `p` (0..`persons()`) owns
  `events[event_offsets[p]..event_offsets[p+1]]`, sorted by `ts_ms` ascending, and
  `clicks[click_offsets[p]..click_offsets[p+1]]`, sorted by (`ts_ms` asc, click_id byte-wise asc).
  Use `loaded.person_events(p)` / `loaded.person_clicks(p)`.
* Every listed person has >= 1 surviving event. Persons with only clicks are omitted
  (they can't produce output). Person order is arbitrary; `person_ids.get(p)` is the
  canonical id (byte-wise smallest linked id) for the `user_id` column.
* Events are final survivors: deduped, valid, in window, not late. `cents == 0` means not a
  purchase; purchases have 1..=10,000,000.
* Clicks are deduplicated by click_id, valid (non-empty user, known campaign, ts in
  `[window_start - 7d, window_end)`). Attribution tie rule: among clicks with equal `ts_ms`,
  the **first** in the slice has the smallest click_id.
* `categories` sorted byte-wise ascending and unique, includes `unknown`, so for
  `top_category` ties, the smaller category index wins.
* `campaigns` sorted byte-wise by campaign_id: `(campaign_id, channel)`.
* All timestamps are i64 UTC epoch ms. Event/Click are 16-byte `#[repr(C)]`.

## Cargo.toml

The aggregate agent may append dependencies under the `[dependencies]` marker line
(keep the rest as is; ping in this doc).

## Aggregate/output side (owner: aggregate agent)

* `aggregate::run` creates `user_daily/` and `campaign_daily/`, aggregates in parallel over
  person chunks (balanced by event count, largest first), and each worker writes its own
  `user_daily/part-{worker}.parquet` (zero rows allowed) as soon as it runs out of work; the
  merged campaign table goes to `campaign_daily/part-0.parquet`.
* `src/output/` is included as `aggregate::output` via `#[path]`, so `main.rs` needs no change.
* Parquet: hand-written, one row group, PLAIN, uncompressed, INT64 counters, INT32 `DATE`,
  `STRING` (UTF8) byte arrays, `top_category` OPTIONAL with real nulls. Verified with
  Polars, PyArrow, DuckDB and the official `run_fixtures.py` / `compare.py`.
* Relies on: per-person events sorted by ts, clicks by (ts, click_id), categories and campaigns
  sorted byte-wise, every listed person has >= 1 event.

## Changelog

* 2026-11-20 v1: initial contract.
