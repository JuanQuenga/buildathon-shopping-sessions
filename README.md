# Shopping Sessions: native Rust pipeline

Builders Buildathon, Databricks track **Shopping Sessions**. A single native Rust binary reads
the CSV inputs under `$DATA_DIR`, resolves linked IDs, deduplicates and validates events and
clicks, sessionizes per person, attributes and counts assists, and writes the two daily tables
as Parquet under `$OUTPUT_DIR`. The spec is `rules.md` from the track page.

## Layout

| Path | What |
| --- | --- |
| `src/ingest/` | Bytes to person-grouped, deduplicated, validated events and clicks (exact CSV dialect, timestamp forms, union-find over links) |
| `src/aggregate/` | Sessions (30 min gap), UTC start day, attribution (latest click in `[s - 7d, s]`, tie on smallest click_id), assists (interval union per person and campaign), daily rollups |
| `src/output/` | Minimal hand-written Parquet writer (PLAIN, uncompressed, one row group per file, logical `DATE`, `STRING`, real nulls), files written in parallel |
| `reference/`, `tests/`, `scripts/` | Independent Python oracle, official fixtures and validators, differential checks |
| `bench/` | Timing harness and results per commit |
| `docs/INTERFACE.md` | Contract between ingest and aggregate |

## Build and run

```sh
docker build -t shopping-sessions .
scripts/run_candidate.sh shopping-sessions DATA_DIR OUTPUT_DIR   # scorer flags
```

Native: `cargo build --release`, then `DATA_DIR=... OUTPUT_DIR=... target/release/pipeline`.
Stage timings go to stderr.

## License and provenance

MIT, see `LICENSE`. All code in this repository was written during the event weekend
(Nov 20–22, 2026); no pre-weekend code is used.

Open-source software used: the Rust toolchain and standard library (MIT/Apache-2.0), the
`rust` and `debian` Docker base images. The pipeline binary has no third-party crates.
Test tooling (not in the image): Python, Polars, PyArrow, DuckDB, and the organizers'
published fixtures, `run_fixtures.py`, `compare.py` and data generator.
