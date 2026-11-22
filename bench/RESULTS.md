# Benchmark results

## Please submit

- **Tag**: `candidate-15h` (branch `exp/c15-engine`) — **Split `#[inline(never)] group_short_part` / `group_long_part` (`-4.23M/M` Ir, `-2.5k` I1mr), branchless `select_unpredictable` packed `cswap_click_ts` + restored `13..=16` Green click sorting networks (`-1.72M/M` Ir, `-123k/M` Bcm), `INGEST_DENSE_MIN = 17` eliminating `quicksort::<CompactEv>` (`-1.12M/M` net Ir, `-78k/M` Bcm, `-8.3k` I1mr), branchless `select_unpredictable` + direct bit-drain in `FusedAssists::count` (`<= 64`, `-55k/M` Bcm at `+0 Ir`), branchless `click_hybrid` delimiter mask (`select_unpredictable`), and short-circuiting `eq_arena_packed` safety guard (`-2.0 ms` local Bookworm ext wall `0.4321 -> 0.4301 s`, `group+agg -4.0 ms` `92.9 -> 89.0 ms`, `-7.38M/M` Ir, `-4.46M/M` Dr, `-1.21M/M` Dw, `-299k/M` [`-7.4%`, `-6.0M`/20M] Bcm vs `candidate-15g` `0.6657 s` official)**.
  - **What `candidate-15h` changes vs `candidate-15g` (`6e1ec47776`, `0.6657 s` official PB on `shopping-sessions-5`)**:
    1. **Outlined `#[inline(never)] group_short_part` & `group_long_part` (`src/ingest/hybrid.rs`)**: splits the monolithic `21.8 KiB` `group_fused_hybrid` closure into separate short (`7.5 KiB`) and long (`13.0 KiB`) partition functions so the 88% short-partition loop fits cleanly in Cascade Lake's 32 KiB L1i without register spills from long-map code (`-4.23M/M` Ir, `-2.5k` I1mr).
    2. **Branchless packed `cswap_click_ts` + restored `13..=16` Green sorting networks (`src/ingest/hybrid.rs`)**: packs `(campaign << 32) | ts` and swaps via `std::hint::select_unpredictable` (`cmov`, zero branch mispredicts) and restores `13..=16` Green networks on `CompactClick<u64>` (`sort_clicks_var_only`: `-1.72M/M` Ir, `-123k/M` Bcm).
    3. **`INGEST_DENSE_MIN = 17` (`src/ingest/hybrid.rs`)**: routes `17..=23`-event persons into `DenseSessions` instead of `sort_ev_slice` / `quicksort::<CompactEv>`, eliminating `quicksort::<CompactEv>` and `ipnsort` from the hot working set (`-1.12M/M` net Ir, `-78k/M` Bcm, `-8.3k` I1mr).
    4. **Branchless `FusedAssists::count` (`<= 64`) & `click_hybrid` (`src/ingest/fast.rs`, `src/ingest/hybrid.rs`)**: replaces `if seen.0 == stamp` with `std::hint::select_unpredictable` and direct `loop { ... if new_bits == 0 { break; } }` bit-drain in `FusedAssists::count` (`-55k/M` Bcm at `+0.00M` Ir, keeping 16B `campaigns`), uses `select_unpredictable` for `click_hybrid`'s delimiter mask, and short-circuits `(*p == 0)` in `eq_arena_packed`.
  - **Same-glibc Bookworm 2.36 persistent-container interleaved warm A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, `INGEST_SINK_THP=0`)**:
    - `candidate-15g` (`0.6657 s` official): EXT `0.4290 / 0.4334 / 0.4321 s` (med **`0.4321 s`**, in-proc med `0.4225 s`, `group+agg 92.9 ms`, `peak_rss 1059 MB`)
    - **`candidate-15h`**: EXT **`0.4301 / 0.4325 / 0.4295 s` (med `0.4301 s`, `-2.0 ms` vs `15g`; in-proc med `0.4207 s`, `group+agg 89.0 ms` `-4.0 ms` across 3/3 rounds, `peak_rss 1059 MB`)**.
    - **Stage-by-stage `seed20-20m-v5` median ms breakdown (`candidate-15h`, Bookworm `THP=0`, `NCPU=8`)**:
      `small=0.84 ms | links-parse=4.93 ms | links-uf=28.58 ms | parse=218.76 ms | dedup=19.81 ms | relocate=36.18 ms | group+agg=88.96 ms | campaign-write=1.13 ms | total(in-proc)=420.68 ms | ext=430.1 ms`
  - **Cascade Lake Cachegrind (`dev-v5` 1M, `--branch-sim=yes`, `I1=32K, D1=32K, LL=16M`)**:
    - `candidate-15g`: `Ir=931.13M`, `I1mr=59.3k`, `Dr=186.81M`, `Dw=81.10M`, `DLmr=821.1k`, `DLmw=898.1k`, `Bcm=4,029.1k`
    - **`candidate-15h`**: **`Ir=923.75M` (`-7.38M` / `-0.8%`)**, **`I1mr=58.0k` (`-1.3k`)**, **`Dr=182.35M` (`-4.46M`)**, **`Dw=79.89M` (`-1.21M`)**, `DLmr=820.4k` (`-0.7k`), `DLmw=899.6k`, **`Bcm=3,730.0k` (`-299.2k/M` = `-7.4%` = `-6.0M` fewer mispredicts on 20M!)**.
  - **Explicit official scorer prediction BEFORE submission**: **`0.650–0.660 s`** on `shopping-sessions-5` (`-6 to -15 ms` vs `candidate-15g`'s `0.6657 s`, driven by `-4.0 ms` local `group+agg`, `-7.38M/M` Ir, `-5.67M/M` D-refs, and `-6.0M` fewer branch mispredicts on Cascade Lake with zero LLC/RSS regressions).
  - **Verification**: 41/41 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev-v5` exact `MATCH` (`hybrid` + `INGEST_GENERIC=1`), `seed20-20m-v5` exact `MATCH` (`1059 MB` peak RSS), all four 60M datasets clean `< 10 GiB` RSS (`seed60-60m` `2580 MB`, `seed60-5kcat` `2996 MB`, `seed60-90d` `2977 MB`, `seed60-longids` `8436 MB`), and clean `git archive` Docker build verified `MATCH` on `dev-v5` under `--network none --read-only --user 1000:1000`.

### Previous submission (`candidate-15g`)

- **Tag**: `candidate-15g` (branch `exp/c15-engine`, official **`0.6657 s`** on `shopping-sessions-5`, `-14.0 ms` vs `15f`) — **12-byte `RecB12` + `SWCB_B=192` + `purch` sidecar (`-25%` event arena/DRAM traffic, `-102 MB` RSS), branchless L1d-buffered `ShortRelocMap::get_branchless` relocation (`-450k/M` Bcm), `links-uf` scratch page reuse + `keys` cap fix (`n*3/4+32`) + per-worker long interning + fused barrier (`-16.4k` THP faults), SSE2 `parse_f3_13`/`clock_sec` + unconditional `eb` in `parse_f1_pair_sec`, single-load branchless `try_encode_89`/`try_encode_eid_89` + LUT `open_delim`/`open_ingest_f1` + single-exit `event_hybrid` (`44->14` `vmovntdq`), 4-way AVX2 `ShortGroupMap4` (`50%` load), `FusedAssists` AVX2 `<= 64`, `HAS_CLICKS`-specialized `sessionize_person` + `DenseSessions::word_mask`, unified `ts`-only click sorting (`-10 KiB` L1i, delete `ck_has_long`), and pre-zeroed `UserCols::defs` (`-11.2 ms` local Bookworm ext wall `0.4557 -> 0.4445 s`, `-13.5M/M` Ir, `-627k/M` [`-13.5%`, `-12.5M`/20M] Bcm, `-261k/M` [`-24.1%`] DLmr, `-180k/M` [`-16.7%`] DLmw vs `candidate-15f` `0.6797 s` official)**.
  - **What `candidate-15g` changes vs `candidate-15f` (`83a08a87f5`, `0.6797 s` official on `shopping-sessions-5`)**:
    1. **12-byte `RecB12` + `SWCB_B = 192` + `purch: Vec<CompactEv>` sidecar (`src/ingest/hybrid.rs`)**: packs 97% non-purchase events (`ts < 2^31`, `cc == 0`) inline into 12 bytes (`person_lo`, `person_hi`, `ts_or_purch`), indexing the 3% purchases (and any `ts >= 2^31`) into a worker-local `CompactEv` sidecar (`-25%` `b_sinks`/`reloc_b` arena & DRAM traffic, `-102 MB` peak RSS, `6` vs `8` AVX2 NT stores/flush, `252 KiB`/worker SWCB).
    2. **Branchless L1d-buffered `relocate_hybrid` (`src/ingest/hybrid.rs`, `src/ingest/par.rs`)**: adds `ShortRelocMap::get_branchless` (`97%` bucket-0 hit/miss without branching) + `compact_and_buffer_{with_dead_,}disjoint` with 12 KiB L1d move buffers, eliminating the 39%/61% relocation branch mispredicts while preserving in-place compaction (`relocate`: `38.3 -> 34.8 ms`).
    3. **`links-uf` scratch reuse & allocation overhaul (`src/ingest/hybrid.rs`, `src/ingest/par.rs`)**: fixes `parts[p].keys` scratch cap (`n/2+8 -> n*3/4+32`, eliminating all 256 partition heap spills), splits `tokens.as_mut_uninit()` into disjoint reusable slices so `keys`+`parent` reuse `scattered` pages and `short_tmp`+`long_tmp` reuse `ep`+`parts` pages (`-16.4k` faults under THP), replaces 512 per-partition `long_bytes`/`long_meta` heap Vecs with 8 per-worker buffers, packs 33B hex once on insert, fuses the `link_store` and `keys`/`parent` barriers, and drops staging buffers in parallel (`par::drop_par`).
    4. **Direct parser & timestamp `-Ir/-Bcm` cuts (`src/ingest/tsfast.rs`, `src/ingest/hybrid.rs`)**: SSE2 `parse_f3_13` & `clock_sec` (`-26M` scalar insns/M on F3/clicks), unconditional `eb` lookup in `parse_f1_pair_sec` (`-57.5k Bcm/M`), single-load branchless `try_encode_89` (`pext(bswap(ld8(p))) << 7 | b8`) & `try_encode_eid_89` (zero `select_unpredictable`/`movabs`), 2-entry LUT `open_delim`/`open_ingest_f1`, branchless `Tail::wrapping_view_cart` (no `tzcnt(commas)`), single-branch `any_zero_u32` `view|cart`, outlined `<0.01%` 33B verbatim fallbacks & `parse_purchase_tail`, and a single shared emit site in `event_hybrid` (`parse`: `237.4 -> 228.1 ms`).
    5. **`group+agg` L1i/Bcm overhaul (`src/ingest/fast.rs`, `src/ingest/hybrid.rs`, `src/output/mod.rs`)**: 4-way AVX2 `ShortGroupMap4` (`50%` load, 32 KiB L1d-resident), single-loop `for_each_live` (no `.chain()` overhead, stays inlined), `FusedAssists` AVX2 `<= 64` sessions with hoisted `camp.assisted` pointer (`#[inline(never)]`), `HAS_CLICKS`-specialized `sessionize_person` + 2-step branchless `emit_session_c` + `DenseSessions::word_mask` `tzcnt` drain, unified `ts`-only click sorting (`2..=12` networks + cold `tiebreak_clicks_var`, deleting `ck_has_long` and `-10 KiB` L1i), `#[inline(never)] flush_user_day`, and pre-zeroed `UserCols::defs`.
  - **Same-glibc Bookworm 2.36 persistent-container interleaved warm A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, `INGEST_SINK_THP=0`)**:
    - `candidate-15f` (`0.6797 s` official): EXT `0.4557 / 0.4555 / 0.4576 s` (med **`0.4557 s`**, in-proc `0.4478 s`, `parse 237.4 ms`, `relocate 38.3 ms`, `minflt 316k`, `peak_rss 1161 MB`)
    - **`candidate-15g`**: EXT **`0.4445 / 0.4425 / 0.4476 s` (med `0.4445 s`, `-11.2 ms` vs `15f`, 3/3 wins; in-proc `0.4330–0.4382 s`, `parse 228.1–231.2 ms`, `relocate 34.8 ms`, `minflt 289k`, `peak_rss 1059 MB`)**.
  - **Cascade Lake Cachegrind (`dev-v5` 1M, `--branch-sim=yes`, `I1=32K, D1=32K, LL=16M`)**:
    - `candidate-15f`: `Ir=944.6M`, `I1mr=60.0k`, `DLmr=1,082k`, `DLmw=1,078k`, `Bcm=4,656k`
    - **`candidate-15g`**: **`Ir=931.1M` (`-13.5M` / `-1.4%`)**, **`I1mr=59.3k` (`-0.7k`)**, **`DLmr=821k` (`-261k` / `-24.1%`)**, **`DLmw=898k` (`-180k` / `-16.7%`)**, **`Bcm=4,029k` (`-627k/M` = `-13.5%` = `-12.5M` fewer mispredicts on 20M!)**.
  - **Explicit official scorer prediction BEFORE submission**: **`0.62–0.65 s`** on `shopping-sessions-5` (`-30 to -60 ms` vs `candidate-15f`'s `0.6797 s`, amplified on Cascade Lake by `-24.1%` LLC read misses, `-16.7%` LLC write misses, `-13.5%` branch mispredicts, `-102 MB` RSS, and `-16.4k` THP `links-uf` faults).
  - **Verification**: 41/41 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev-v5` exact `MATCH` (`hybrid` + `INGEST_GENERIC=1`), `seed20-20m-v5` exact `MATCH` (`1059 MB` peak RSS), all four 60M datasets clean `< 10 GiB` RSS (`seed60-60m` `2583 MB`, `seed60-5kcat` `2989 MB`, `seed60-90d` `2972 MB`, `seed60-longids` `8158 MB`), and clean `git archive` Docker build verified `MATCH` on `dev-v5` and `seed20-20m-v5` under `--network none --read-only --user 1000:1000`.

### Previous submission (`candidate-15f`)

- **Tag**: `candidate-15f` (branch `exp/c15-engine`, official **`0.6797 s`** on `shopping-sessions-5`, `-9.7 ms` vs `15c` / `-34.0 ms` vs `15e`) — **Eliminate all 4 Cascade Lake regressions from `15d/15d2/15e` (`+17.6 to +24.5 ms` vs `15c`): restore zero-overflow `1.12x + 32` sink caps (`max(short, long)` density) + `ov.reserve(1024)`, restore `15c`'s `sink_thp()` (`MADV_HUGEPAGE` whenever `thp != "never"`), shrink `group_fused_hybrid` from `44.2 KiB -> 19.9 KiB` (`< 32 KiB` Cascade Lake L1i via `#[inline(never)]` on `aggregate_person`/`sort_clicks_var_only`/`sort_clicks_only` + `parse_{events,clicks}_buf_hybrid`), restore `INGEST_DENSE_MIN = 24` & `col_category = Snappy`, drop slower branchless `push_cond`, and pair `base_p1_bits = min(9)` (`p1_total = 576`) with `SWCB=256/256/192` (`576 KiB`/SMT pair `< 1 MiB` L2) while keeping 18B packed verified-33B-hex IDs, 4-way SSE2 `DedupTable`, AVX2 `FusedAssists::count`, lazy `day_runs`, and branchless `emit_session`/`cswap_click` (`-11.2 ms` local Bookworm ext wall `0.4581 -> 0.4469 s`, `-1.017M/M` [`-20.3M`/20M] branch mispredicts vs `candidate-15c` `0.6894 s` official)**.
  - **Root cause of `candidate-15e` (`0.7137 s` official, `+24.3 ms` vs `15c` / `+6.7 ms` vs `15d2`)**:
    1. **`1.06x + 16` sink caps** (`cap_b = 1.007x`, long buckets `0.889x`) spilled skewed & long partitions into non-THP glibc heap `Vec`s (`ov.reserve(128/256)`), whereas `15c` had `1.12x + 16/32` + `ov.reserve(1024)`.
    2. **Cascade Lake 32 KiB L1i overflow**: `group_fused_hybrid::<()>::{closure#7}` had bloated to `44,160 B` (`> 32 KiB` L1i!) from inlining `aggregate_person` twice + `13..=16` click sorting networks.
    3. **Smart `sink_thp()`** skipped `MADV_HUGEPAGE` under `thp=always` and used a `< 400 µs` probe under `thp=madvise` that can false-negative on cold KVM EPT faults (`+6.7 ms` vs `15d2`), while branchless `push_cond` added `+25.4M Ir/M`.
    4. **`INGEST_DENSE_MIN = 17`** and **`col_category = Compression::None`** vs `15c`'s `24` and `Snappy`.
  - **Same-glibc Bookworm 2.36 persistent-container interleaved warm A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, `INGEST_SINK_THP=0`)**:
    - `candidate-15c` (`0.6894 s` official): EXT `0.4553 / 0.4581 / 0.4598 s` (med **`0.4581 s`**, in-proc `0.4491 s`, `user=2.579–2.625 s`, `parse 242.8 ms`, `group+agg 93.9 ms`, `minflt=306k`)
    - **`candidate-15f`**: EXT **`0.4464 / 0.4469 / 0.4475 s` (med `0.4469 s`, `-11.2 ms` vs `15c`, 3/3 warm wins, `< 1.1 ms` jitter; in-proc `0.4388 s`, `user=2.501–2.540 s`, `parse 235.7 ms`, `group+agg 90.3 ms`, `minflt=297k`)**.
  - **Cascade Lake Cachegrind (`dev-v5` 1M, `--branch-sim=yes`, `I1=32K, D1=32K, LL=6M`)**:
    - `candidate-15c`: `Ir=936.9M`, `LLd=2.230M`, `Bcm=5,708k`
    - **`candidate-15f`**: `Ir=944.6M`, **`LLd=2.161M` (`-70k` / `-3.1%` vs `15c`)**, **`Bcm=4,691k` (`-1,017k/M` = `-17.8%` = `-20.3M` fewer mispredicts on 20M vs `15c`)**, **`I1 misses=60k`** (`group_fused_hybrid` `19.9 KiB` L1i-fit).
  - **Explicit official scorer prediction BEFORE submission**: **`0.61–0.65 s`** on `shopping-sessions-5` (cleanly beating `candidate-15c`'s `0.6894 s` by `-40 to -80 ms` once all 4 `15d..15e` Cascade Lake regressions are eliminated while retaining 18B packed IDs, 4-way SSE2 Dedup, AVX2 `FusedAssists`, and `-20.3M` fewer mispredicts).
  - **Verification**: 41/41 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev-v5` exact `MATCH` (`hybrid` + `INGEST_GENERIC=1`), `seed20-20m-v5` exact `MATCH` (`1169 MB` peak RSS), all four 60M datasets exact `MATCH` (`seed60-60m` `2898 MB`, `seed60-5kcat` `2993 MB`, `seed60-90d` `3001 MB`, `seed60-longids` `8492 MB` — all `< 10 GiB`), and clean `git archive` Docker build verified `MATCH` on `dev-v5` and `seed20-20m-v5` under `--network none --read-only --user 1000:1000`.

### Previous submission (`candidate-15e`)

- **Tag**: `candidate-15e` (branch `exp/c15-engine`, official **`0.7137 s`** on `shopping-sessions-5`, `+24.3 ms` vs `15c` / `+6.7 ms` vs `15d2`) — **Remove skewed 2-pass synthetic PGO from `Dockerfile` (`-265k/M` Bcm vs `15d2`), smart `sink_thp()` avoiding `defrag=madvise` compaction stalls, branchless AVX2 bitmask `FusedAssists::count` (`2..=16` sessions) with lazy `day_runs` + branchless `emit_session`, and branchless `ShortRelocMap::get_branchless` + `Sinks::push_cond` + unconditional arena compaction (`-20.3 ms` local Bookworm ext wall `0.4752 -> 0.4549 s`, `-1.19M/M` [`-23.8M`/20M] branch mispredicts, `-40k` minor faults vs `candidate-15c` `0.6894 s` official)**.
  - **Root cause of `candidate-15d2` (`0.7070 s` official, `+17.6 ms` vs `15c`)**: Cascade Lake Cachegrind (`dev-v5` 1M, `I1=32K, D1=32K, LL=6M`) proved that the 2-pass synthetic `pgo_gen.rs` build (`NCPU=4`, wrong F3 epoch `1_774_992_000`, uniform `25` events/person) added **`+265k/M` (`+5.3M` extra mispredicts on 20M!)** (`5.071M -> 5.336M Bcm`) vs the exact same code without PGO (`d2nopgo`), reversing `15d2`'s real `-40k` minor-fault and `-637k/M` `Bcm` wins on Cascade Lake.
  - **What `candidate-15e` changes vs `candidate-15d2` (`297c68c9e0`, `0.7070 s` official)**:
    1. **Clean non-PGO `Dockerfile`**: removes 2-pass synthetic PGO (`cargo build --release --locked -j 4`, `-Ctarget-cpu=x86-64-v3`, ~23 s build, `-265k/M` Bcm vs `15d2`).
    2. **Smart `sink_thp()` (`src/ingest/par.rs`)**: skips redundant `MADV_HUGEPAGE` under `thp=always` (preserving opportunistic 2 MiB huge pages on clean VMs while eliminating `+194 ms` `defrag=madvise` synchronous compaction stalls on fragmented hosts); under `thp=madvise`, probes a single 2 MiB fault (`< 400 µs`).
    3. **AVX2 bitmask `FusedAssists::count` (`2..=16` sessions) + lazy `day_runs` + branchless `emit_session` (`src/ingest/fast.rs`)**: evaluates all `2..=16` session windows per click in 1–2 `_mm256_cmpgt_epi32` ops, skips `day_runs`/`s_run` tracking on `92%+` of persons, and selects attributed vs organic credit via `select_unpredictable` (`group+agg`: `94.2 -> 88.4 ms`, `-47 ms` user vs `15c`; `-165k/M` Bcm).
    4. **Branchless `ShortRelocMap::get_branchless` + `Sinks::push_cond` + unconditional compaction (`src/ingest/hybrid.rs`, `src/ingest/par.rs`)**: eliminates the 57%/43% mapped-vs-unmapped branch on ~98% of short relocation lookups via a dedicated trailing dummy SWCB slot (`relocate_hybrid` `Bcm`: `579.5k -> 174.8k/M`, `-405k/M` = `-8.1M` fewer mispredicts on 20M!).
  - **Same-glibc Bookworm 2.36 persistent-container interleaved 4-round A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, zero env flags for `15e`)**:
    - `candidate-15c` (`T0`, `0.6894 s` official): clean rounds `0.4752 / 0.4739 / 0.4888 s` (med **`0.4752 s`**, `group+agg 94.2 ms`, `minflt 321k`)
    - `d2nopgo` (`T0`): clean rounds `0.4565 / 0.4545 / 0.4556 s` (med **`0.4556 s`**, `group+agg 90.0 ms`, `minflt 282k`)
    - **`candidate-15e` (`auto`, `sink_thp=0`)**: clean rounds **`0.4566 / 0.4551 / 0.4549 s` (med `0.4551 s`, `-20.1 ms` vs `15c`; `group+agg 88.4 ms` `-5.8 ms`, `minflt 282k` `-40k`)**.
  - **Cascade Lake Cachegrind (`dev-v5` 1M, `--branch-sim=yes`, `I1=32K, D1=32K, LL=6M`)**:
    - `candidate-15c`: `Ir=936.9M`, `LLd=2.230M`, `Bcm=5,708k`
    - `candidate-15d2` (PGO): `Ir=914.5M`, `LLd=2.148M`, `Bcm=5,336k`
    - **`candidate-15e`**: **`LLd=2.143M` (`-87k` / `-3.9%` vs `15c`)**, **`Bcm=4,517k` (`-1,191k/M` = `-20.9%` = `-23.8M` fewer mispredicts on 20M vs `15c`; `-819k/M` = `-16.4M` fewer vs `15d2`!)**.
  - **Explicit official scorer prediction BEFORE submission**: **`0.61–0.65 s`** on `shopping-sessions-5` (vs `candidate-15c` `0.6894 s` and `candidate-15d2` `0.7070 s`, recovering `-40 to -80 ms` from removing skewed PGO, `-40k` minor faults, smart `sink_thp()`, and `-23.8M` fewer branch mispredicts).
  - **Verification**: 41/41 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev-v5` exact `MATCH` (`hybrid` + `INGEST_GENERIC=1`), `seed20-20m-v5` exact `MATCH` (`1.01 GiB` peak RSS), all four 60M datasets exact `MATCH` (`seed60-60m` `2.64 GiB`, `seed60-5kcat` `2.96 GiB`, `seed60-90d` `2.90 GiB`, `seed60-longids` `8.13 GiB` — all `< 10 GiB`), and clean `git archive` Docker build verified `MATCH` on `dev-v5` and `seed20-20m-v5` under `--network none --read-only --user 1000:1000`.

### Previous submission (`candidate-15d2`)

- **Tag**: `candidate-15d2` (branch `exp/c15-engine`, official **`0.7070 s`** on `shopping-sessions-5`, `-6.9 ms` vs `15d` but `+17.6 ms` vs `15c` due to skewed synthetic PGO) — **Revert VM-hostile `mmap` input back to `pread` (`CHUNK=256 KiB`) and restore L2-friendly `SWCB=128/128/192`, keeping lossless 18B packed verified-33B-hex IDs, 4-way SSE2 `DedupTable`, `any_zero_u16`, `FusedAssists` Day-Run, `col_category` uncompressed, and 2-pass LLVM PGO (`-24.5 ms` / `-5.2%` local Bookworm ext wall `0.4729 -> 0.4484 s`, `-155 ms` user, `-67 ms` sys, `-40k` minor faults vs `candidate-15c`)**.
  - **Why**: `candidate-15d` (`43aba2cc93`) scored `0.7139 s` (`+24.5 ms` vs `candidate-15c` `0.6894 s`) in the cloud scorer VM despite `-37.7 ms` locally because `INGEST_MMAP_INPUT=6` traded 4,000 `pread` syscalls into reusable thread-local buffers for ~520,000 4 KiB read-only `filemap_fault`s + `MADV_DONTNEED` TLB shootdowns inside the VM, and `SWCB=256/256/192` pushed per-core L2 over 1 MiB.
  - **Same-glibc Bookworm 2.36 interleaved 4-round A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, `INGEST_SINK_THP=0`, zero contention)**:
    - `candidate-15c` (`0.6894 s` official): ext `0.4724 / 0.4906 / 0.4720 / 0.4734 s` (med **`0.4729 s`**, in-proc `0.4592 s`, `user=2.648 s`, `sys=0.958 s`, `minflt=320k`)
    - `candidate-15d2` (non-PGO, 256K): ext `0.4469 / 0.4558 / 0.4513 / 0.4482 s` (med **`0.4498 s`**, in-proc `0.4373 s`, `user=2.541 s`, `sys=0.868 s`, `minflt=280k`)
    - **`candidate-15d2` (Docker PGO, 256K)**: ext **`0.4436 / 0.4529 / 0.4509 / 0.4458 s` (med `0.4484 s`, `-24.5 ms` / `-5.2%` vs `15c`; in-proc `0.4361 s`, `user=2.493 s` `-155 ms`, `sys=0.891 s` `-67 ms`, `minflt=280k` `-40k`)** — beats both `c15c` and non-PGO on 4/4 rounds!
    - `candidate-15d2` (Docker PGO, 512K): ext `0.4518 / 0.4523 / 0.4468 / 0.4515 s` (med `0.4517 s`, slower than 256K).
  - **Explicit official scorer prediction BEFORE submission**: **`0.62–0.65 s`** on `shopping-sessions-5` (vs `candidate-15c`'s `0.6894 s`, `-40 to -70 ms` from `-40k` minor faults, `-155 ms` user CPU, `-670k/M` branch mispredicts, 18B packed IDs, 4-way SSE2 Dedup, and 2-pass PGO with zero VM `mmap` fault penalty).
  - **Verification**: 41/41 release unit tests (0 warnings), clean `git archive` Docker PGO build (`116 MB`) verified exact `MATCH` on `dev-v5` and `seed20-20m-v5` under `--network none --read-only --user 1000:1000`.

### Previous submission (`candidate-15d`)

- **Tag**: `candidate-15d` (branch `exp/c15-engine`, official **`0.7139 s`** on `shopping-sessions-5`, `+24.5 ms` VM regression from `mmap` input + `SWCB=256`) — **Lossless 18B packed verified-33B-hex `ArenaBytes` + `LinkLongStore::packed`, 4-way SSE2 `DedupTable`, zero-copy whole-file `mmap` with parallel `MADV_DONTNEED`, `SWCB=256/256/192`, `any_zero_u16` single-branch 8/9B dispatch, lean order-free `eid` encoding, `FusedAssists` Day-Run aggregation, and self-contained 2-pass LLVM PGO (`-37.7 ms` / `-7.7%` local Bookworm ext wall `0.4901 -> 0.4524 s`, `-222 ms` sys, `-670k/M` branch mispredicts vs `candidate-15c`)**.
  - **What `candidate-15d` changes vs `candidate-15c` (`f227276c4e`, `0.6894 s` official on `shopping-sessions-5`)**:
    1. **Zero-copy whole-file `mmap` input (`src/ingest/scan.rs`, `src/ingest/hybrid.rs`)**: `mmap(PROT_READ, MAP_PRIVATE)` each `Events`/`Clicks` file once (`>= 64 KiB`), passing interior chunks (`hi + PAD <= flen`, `512 KiB` default chunk) zero-copy into the direct parsers to eliminate 2.03 GB of `pread` kernel-to-user copies (`parse`: `256 -> 233 ms`, `sys`: `-222 ms`). Uses parallel `MADV_DONTNEED` + instant `munmap` (`mmap_mode=6`) for `<= 3.5 GiB` inputs and streaming per-file `munmap` (`mmap_mode=4`) for `> 3.5 GiB` (60M+) inputs to cap Peak RSS (`3.69 GiB` on `seed60-60m`).
    2. **Lossless 18B packed verified-33B-hex in `ArenaBytes` + `LinkLongStore::packed` + `LongLinkPair`**: packs verified 33-byte uniform-case hex IDs as `[0, tag, hi: u64, lo: u64]` (`18B` vs `34B`, `-47%` long-ID arena bytes, `-41k` minor faults, `-136 MB` pre-mmap RSS), with 2-mul `hash_packed_33` and 3-register `eq_arena_packed`/`eq_arena_arena` (eliminates `__memcmp_avx2_movbe` and `unpack_hex33` across `parse`, `dedup`, `relocate`, and `group+agg`, while keeping 100% verbatim opaque fallback for any non-hex/mixed-case/arbitrary-length string).
    3. **4-way SSE2 `DedupTable` (`src/ingest/fast.rs`, `src/ingest/hybrid.rs`) + `SWCB=256/256/192`**: halves `DedupTable` L2 footprint (`512 -> 256 KiB` at `<= 70%` load) and cuts dedup branch mispredicts by `-75.5%` (`584k -> 143k Bcm/M`, `dedup`: `20.6 -> 17.8 ms`); widens `SWCB_TOK`/`SWCB_B` to `256B` (still `590 KiB`/SMT pair `<< 1 MiB` L2 on `p1_total=p2_total=576`), halving `Sinks::push_idx` flush mispredicts (`400k -> 214k Bcm/M`).
    4. **Direct parser & `group+agg` micro-optimizations**: guaranteed single-cmov `any_zero_u16` (`test`+`cmove`) eliminating LLVM's `is8`/`is9` double-branch split; lean order-free `try_encode_eid_89` (no `bswap` or overlapping `ld8(p+1)`); F3-first Huffman timestamp dispatch in `click_hybrid`; `DateTable::parse_f1_pair_sec` same-date fast path; `FusedAssists` Day-Run aggregation (`u16` stamps, 8 KiB L1d-hot) + branchless `CampStore::add_credit_in_window`; skip redundant click dedup compaction (`sort_clicks_only` with `13..=16` Green networks + `256` radix cutoff); `INGEST_DENSE_MIN = 17`; `col_category` uncompressed (`Compression::None`); tightened `1.06× + 16` sink caps.
    5. **Self-contained 2-pass LLVM PGO in `Dockerfile` (`src/pgo_gen.rs`)**: generates a deterministic synthetic v5-shaped training corpus (`INGEST_GEN_PGO`) during the build stage, profiles, merges via `llvm-profdata`, and rebuilds with `-Ctarget-cpu=x86-64-v3 -Cprofile-use` (`1.7 MB` stripped binary vs `2.1 MB`, zero data/profile shipped in runtime image, `53 s` build).
  - **Same-glibc Bookworm 2.36 interleaved 4-round A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, `INGEST_SINK_THP=0`)**:
    - `candidate-15c` (`0.6894 s` official): ext `0.4976 / 0.4893 / 0.4908 / 0.4887 s` (med **`0.4901 s`**, in-proc `0.4745 s`, `sys=1.03 s`)
    - `candidate-15d` (non-PGO): ext `0.4680 / 0.4794 / 0.4551 / 0.4567 s` (med **`0.4624 s`**, in-proc `0.4483 s`)
    - **`candidate-15d` (Docker PGO)**: ext **`0.4507 / 0.4663 / 0.4538 / 0.4510 s` (med `0.4524 s`, `-37.7 ms` / `-7.7%` vs `15c`; in-proc `0.4374 s`, `sys=0.833 s` `-222 ms`, `user=2.559 s` `-88 ms`; `parse 232.8 ms` `-23.2 ms`, `dedup 17.8 ms` `-2.8 ms`, `relocate 37.1 ms` `-2.8 ms`, `group+agg 92.6 ms` `-3.1 ms`)**.
  - **Docker warm container wall A/B (`--network none --read-only --user 1000:1000`, `THP=0`, 3 rounds)**:
    - `ss-test:f227276c4e` (`15c`): `0.8332 / 0.8275 / 0.8243 s` (med **`0.8275 s`**, in-proc `0.4698 s`)
    - **`ss-test:c15d-pgo`**: **`0.7797 / 0.7995 / 0.8006 s` (med `0.7995 s`, best `0.7797 s`, in-proc `0.4363 s` `-33.5 ms`)**.
  - **Cascade Lake Cachegrind (`dev-v5` 1M, `--branch-sim=yes`, `I1=32K, D1=32K, LL=6M`)**:
    - `candidate-15c`: `Ir=936.9M`, `D1m=10.27M`, `Bcm=5.71M` (`5.3%`)
    - **`candidate-15d` (pre-PGO `d5`)**: **`Ir=933.6M`**, **`Bcm=5.04M` (`-670k/M` = `-11.8%` = `-13.4M` fewer mispredicts on 20M!)**.
  - **Explicit official scorer prediction BEFORE submission**: **`0.60–0.64 s`** on `shopping-sessions-5` (vs `candidate-15c`'s `0.6894 s`, `-50 to -90 ms` from `-38 ms` local ext wall × `~1.5×` Cascade Lake scaling + PGO branch layout + zero-copy `mmap6` eliminating 2.03 GB `pread` copies).
  - **Verification**: 41/41 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev-v5` exact `MATCH` (`hybrid` + `INGEST_GENERIC=1`), `seed20-20m-v5` exact `MATCH`, all four 60M datasets exact `MATCH` (`seed60-60m` 3.69 GiB, `seed60-5kcat` 2.93 GiB, `seed60-90d` 2.92 GiB, `seed60-longids` 8.38 GiB — all `< 10 GiB`), and clean `git archive` Docker PGO build (`116 MB`) verified `MATCH` on `dev-v5` and `seed20-20m-v5`.

### Previous submission (`candidate-15c`)

- **Tag**: `candidate-15c` (branch `exp/c15-engine`, official **`0.6894 s`** on `shopping-sessions-5`, Rank #7 at submission, `-107.1 ms` vs `candidate-15b`) — **Disjoint short/long unified `Sinks`, unified 2 MiB THP `ArenaBytes` for long IDs, zero-branch fused ID parsers, 4-way SSE2 `LongRelocMap`, branchless click sorting networks, and 37B Snappy user-ID fast path (`-59.6 ms` / `-11.3%` local wall `0.5258 -> 0.4662 s`, best `0.4564 s`, `-985k/M` branch mispredicts vs `candidate-15b`)**.
  - **What `candidate-15c` changes vs `candidate-15b` (`05343d7`, `0.7965 s` official on `shopping-sessions-5`)**:
    1. **Disjoint pure-short / pure-long partition spaces inside unified `Sinks` (`SWCB=128/128/192`)**: `tok_sinks` holds `p1_short` pure-short (`1024`) + `p1_long` pure-long (`128`) = `1152` partitions (single-barrier dedup); `b_sinks`, `ck_sinks`, `reloc_b`, `reloc_c` hold `p2_short` pure-short (`512`) + `p2_long` pure-long (`64`) = `576` partitions. Eliminates ALL `is_short` branches in `relocate` (`56.6 -> 43.3 ms`) and `group+agg` (`112.5 -> 102.4 ms`), and lets pure-short `group+agg` use single-array `GroupMap<u64, true>`.
    2. **Unified 2 MiB THP `ArenaBytes` for `long_eids`, `long_users`, `long_cids`** (`<= 512 MiB` per kind, seamless heap spillover with `MADV_DONTNEED` release on extreme 100%-long-ID 60M datasets): eliminates `Vec` doubling `memcpy`s, moves ~160 MiB of long IDs into 2 MiB THP on the clean scorer VM, and rejoins `tok` + `long_eids` via `SinkArena::merge` right after `dedup` for zero-fault `relocate` + `group_scratch` reuse.
    3. **Fused register-only direct ID parsers + branchless `try_encode_89` + `tsfast` dual-date SSE2 `clock_pair`**: eliminates `match eid`/`match user`/`match cid` and `if is8` branches (`parse`: `270 -> 243 ms`, `-27 ms`), keeps `SWCB=128/128/192` + `LongUserCache::CAP=4096` L2-hot on 2-way SMT, and uses SSE2 `clock_pair` even when F2 timezones cross midnight (`tsfast.rs Bcm`: `60.4k -> 17.7k`).
    4. **Zero-`hash_long` link scatter + 4-way SSE2 `LongRelocMap` + branchless `canon_q_of`**: embeds `fp25 = uh >> 39` directly in parse handles so `ep_part` needs zero arena derefs; 32B-aligned 4-way SSE2 `LongRelocBucket` accelerates pure-long relocation.
    5. **Branchless click sorting networks (`n <= 12`) + 3-pass radix sort (`> 256`) + `n_sess == 1` `FusedAssists` fast path + `DENSE_MIN = 24` + 37B Snappy `user_id` fast path (`parquet.rs` threshold `32 -> 64`)**: cuts `sort_and_dedup_clicks_var` mispredicts by `-80%`, fixes pure-long row-group fallback to slow generic `snappy()`, and packs `cl_pid`/`cl_flat` cleanly.
  - **Same-glibc Bookworm 2.36 interleaved 3-round A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, `INGEST_SINK_THP=0`)**:
    - `candidate-15b` (`0.7965 s` official): `0.5303 / 0.5122 / 0.5258 s` (med **`0.5258 s`**, `user=2.924 s`; `parse 270 ms`, `relocate 56.6 ms`, `group+agg 112.5 ms`)
    - **`candidate-15c`**: **`0.4662 / 0.4760 / 0.4564 s` (med `0.4662 s`, `-59.6 ms` / `-11.3%` faster wall!, `user=2.789 s` `-135 ms`; `parse 243 ms` `-27 ms`, `relocate 43.3 ms` `-13.3 ms`, `group+agg 102.4 ms` `-10.1 ms`)**.
  - **Cascade Lake Cachegrind (`dev-v5` 1M, `--branch-sim=yes`, `I1=32K, D1=32K, LL=6M`)**:
    - `candidate-15b`: `Ir=974.4M`, `D1m=10.30M`, `LL(6M)=2.36M`, `Bcm=6.70M` (`5.5%`)
    - **`candidate-15c`**: **`Ir=936.9M` (-37.5M, -3.9%)**, **`D1m=10.27M`**, **`LL(6M)=2.24M` (-5.1%)**, **`Bcm=5.71M` (-985k = -14.7% = -19.7M fewer mispredicts on 20M!)**.
  - **Explicit official scorer prediction BEFORE submission**: **`0.69–0.73 s`** on `shopping-sessions-5` (vs `candidate-15b`'s `0.7965 s`, `-65 to -105 ms` from `-60 ms` local wall + `-19.7M` branch mispredicts + ~160 MiB moved to 2 MiB THP).
  - **Verification**: 40/40 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev-v5` exact `MATCH` (`hybrid` + `INGEST_GENERIC=1`), `seed20-20m-v5` exact `MATCH`, all four 60M datasets exact `MATCH` (`seed60-60m` 2.82 GiB, `seed60-5kcat` 2.96 GiB, `seed60-90d` 2.96 GiB, `seed60-longids` `< 8.5 GiB` verified), and clean `git archive` Docker build verified `MATCH` on `dev-v5` and `seed20-20m-v5`.

### Previous submission (`candidate-15b`)

- **Tag**: `candidate-15b` (branch `exp/c15-engine`, official **`0.7965 s`** on `shopping-sessions-5`, Rank #4 at submission) — **Hybrid inline-64 + side-arena single-pass exact engine for `shopping-sessions-5` opaque variable-width IDs (`2.22×` faster local wall `1.161 -> 0.524 s`, `-52.5%` user CPU `6.31 -> 3.00 s` vs `candidate-15a` on `seed20-20m-v5`)**.
  - **Why**: `shopping-sessions-5` gives 1/8 distinct `user_id`/`event_id`/`click_id` values a 32-char hex suffix (33 bytes total) and mandates treating IDs as opaque variable-width byte strings. `candidate-15a` (`ba527a9597`, `1.9110 s` official on `shopping-sessions-5`, Rank #13) rejected 33B IDs in `u64`/`u128` and fell back to `generic.rs` (`ev-dedup 417 ms`, `505k` faults, 0% THP).
  - **What `candidate-15b` adds (`src/ingest/hybrid.rs`, wired in `src/ingest/fast.rs`)**:
    1. **100% opaque variable-width ID representation (`1..∞` bytes, arbitrary UTF-8/NUL)**: keeps short IDs (`1..=9` non-NUL ASCII, 87.5% on v5) inline in `u64` with bit 63 == 0 (`mix63`/`unmix63` exact bijection on `[0, 2^63)`), and stores long/non-ASCII IDs verbatim in length-prefixed per-worker byte arenas (`long_eids`, `long_users` with 4096-slot direct-mapped dedup cache, `long_cids`) and a read-only `LinkLongStore`, referenced via tagged `u64` handles (bit 63 == 1) with 20/25/26-bit embedded hash fingerprints.
    2. **Direct variable-width parser (`id_delim_var`)**: branchless 8/9B inline ASCII fast path + 5-instruction AVX2 opaque 33B delimiter validator (checks zero `,`/`"`/`\n`, zero charset assumption) + `scan::row_fields` fallback for any other length `1..∞`.
    3. **Full single-pass 2 MiB THP `SinkArena` pipeline for mixed short+long IDs**: samples initial event rows (<20 µs) to size short `Tok` vs 16B `LongTok` partitions; mixed short+long union-find (`cmp_node_key` exact byte-wise ordering); hybrid `dedup` (`Tok` + `LongTok`, immediately frees `long_eids`); L2-resident `ShortRelocMap` (4-way AVX2) + `LongRelocMap`; L2-resident `ShortGroupMap` + `LongGroupMap` + `DenseSessions` + sorting networks + `FusedAssists` + `sort_and_dedup_clicks_var`. Toggle `INGEST_NO_HYBRID=1` allows instant fallback.
  - **Same-glibc Bookworm 2.36 A/B (`seed20-20m-v5`, `CPUSET=0-3,8-11`, `NCPU=8`, `INGEST_SINK_THP=0` for clean host noise, 3 warm runs)**:
    - `candidate-15a` (falls back to `generic.rs`): med **`1.161 s`** (`user=6.31 s`, `sys=1.67 s`, `minflt=505k`, `peak_rss=1144 MB`; `links 71 ms`, `parse 418 ms`, `dedup 417 ms`, `group+agg 243 ms`)
    - **`candidate-15b` (`hybrid`)**: **`0.513–0.530 s` (med `0.524 s`, `-637 ms`, `2.22×` faster!)**, **`user=3.00 s` (-52.5%)**, `sys=0.90–0.98 s`, `minflt=329k`, `peak_rss=1249 MB` (`links 36 ms`, `parse 261 ms`, `dedup 20.3 ms` [20.5× faster], `relocate 55 ms`, `group+agg 119 ms` [2.04× faster]).
  - **Cascade Lake Cachegrind (`dev-v5` 1M, `--branch-sim=yes`, `I1=32K, D1=32K, LL=6M`)**:
    - `Ir=974.4M`, `D1m=10.30M`, `LL(6M)=2.36M`, `Bcm=6.66M` (`5.5%`).
  - **Explicit official scorer prediction BEFORE submission**: **`0.80–0.88 s`** on `shopping-sessions-5` (vs `candidate-15a`'s `1.9110 s`, projected ~1.05 s faster -> Top 3–5; production default keeps `sink_thp=1` for clean scorer VM THP).
  - **Verification**: 38/38 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev-v5` exact `MATCH` vs `starter-v5/dev-answer` (both `hybrid` and `INGEST_GENERIC=1`), `seed20-20m-v5` exact `MATCH` vs `c15a-generic`, all four 60M datasets exact `MATCH` (`seed60-60m` 2.86 GiB, `seed60-5kcat` 2.91 GiB, `seed60-90d` 2.92 GiB, `seed60-longids` 7.79 GiB), and clean `git archive` Docker build verified `MATCH` on `dev-v5` and `seed20-20m-v5`.

### Previous submission (`candidate-15a`)

- **Tag**: `candidate-15a` (branch `exp/c15-engine` from `candidate-12b2` `79d1dce`, re-scored **`1.9110 s`** on `shopping-sessions-5` Rank #13 via `generic.rs` fallback) — **End-to-end cache, instruction, branch, and teardown overhaul (`-14.4% Ir`, `-30.7% LL(6M)`, `-51.4% LL(1M)`, `-159 ms` Docker warm container wall vs `c12b2`)**.
  - **What it changes vs `candidate-12b2` (`79d1dce`, `0.6811 s`)**:
    1. **Parallel exit teardown + gated `/proc/self/smaps_rollup`**: gates `smaps_rollup` behind `INGEST_DIAG=1` (`-6.3 ms` serial) and runs parallel `madvise(MADV_DONTNEED)` across the parse mapping before exit (`post-total` teardown cut `-45 ms`).
    2. **Pre-mixed bijective `u64` keys (`mix`/`unmix`)**: stores `eid.h()` in `Tok` (zero re-hashing in `dedup`), `raw.h()`/`canon.h()` in `RecB`/`RecC`/`BucketLinks` (zero hashing in `RelocationMap` and `GroupMap<u64, true>`; exact `unmix` only once per unique person).
    3. **L2-resident `relocate` with in-place compaction & token-arena reuse**: packs `RecC` to 24B (`SWCB=192`), carves `reloc_b`+`reloc_c` out of the already-faulted `tok_sinks` huge-page arena, compacts dead+linked rows in place (eliminating `DeadClicks` and removing `dead` bitset scanning + serial popcount from `group_fused`).
    4. **Direct parser & `links` speedups**: direct `direct::links` parser, root-min-key `uf_union_min_key` (drops `best[]` + extra `nn`-node CAS pass), single-load `u16` masked delimiter check in `direct::open_delim`/`ends_delim`, merged single-branch `view|cart` vs `purchase` path, whole-second window/late checks with SSE2 `clock_pair`, fixed 32B `tail()` overflow in `direct::click` (eliminating `click_slow` on F2/quoted timestamps), `p1_bits=10`/`p2_bits=9` + `1.08×` arena cap.
    5. **O(N) zero-sort `DenseSessions` + branchless sorting networks + split `CampStore`**: generation-stamped bitmask 30-min bin sessionizer for dense users (`count >= 32`), Knuth 0-1-verified branchless `cmov` sorting networks for `n <= 8`, sentinel-guarded `FusedAssists` with branchless campaign stamp check, and split 16B `CreditCell` + 4B `assisted` `CampStore` (112 KiB/worker, 8× smaller).
  - **Cascade Lake Cachegrind (`dev` 1M, `--branch-sim=yes`, `I1=32K, D1=32K`)**:
    - `c12b2` (`0.6811 s` official): `Ir=976.1M`, `Bcm=6.33M`, `D1m=11.40M`, `LL(6M)=2.83M`, `LL(1M)=5.42M`
    - **`candidate-15a`**: **`Ir=836.0M` (-14.4%)**, **`Bcm=6.10M` (-3.7%)**, **`D1m=9.60M` (-15.8%)**, **`LL(6M)=1.96M` (-30.7%)**, **`LL(1M)=2.63M` (-51.4%)**
  - **Same-session interleaved Docker warm A/B (`seed20-20m`, `CPUSET=0-3,8-11`, `NCPU=8`, 3 rounds)**:
    - `ss-test:c12b2`: container wall `1.032 / 0.781 / 0.747 s` (med **`0.781 s`**, in-proc `0.655 s`, `user=2.55–2.69 s`, `sys=2.26–3.67 s`)
    - **`ss-test:c15a`**: container wall **`0.649 / 0.622 / 0.602 s`** (med **`0.622 s`**, best **`0.602 s`**, in-proc `0.541 / 0.520 s`, `user=2.25–2.29 s`, `sys=1.79–2.05 s`) — **`-159 ms` (-20.4%) median container wall!**
    - Stage medians (`c12b2` → `c15a`): `links` `68 → 42 ms`, `parse` `408 → 351 ms`, `dedup` `23.0 → 16.0 ms`, `relocate` `37.6 → 32.8 ms`, `group+agg` `108 → 81.5 ms`, `exit+container` `126 → 81 ms`.
  - **Explicit official scorer prediction BEFORE submission**: **`0.51–0.56 s`** (vs `c12b2`'s `0.6811 s`, −120 to −170 ms; all 5 Cachegrind metrics and every pipeline stage are strictly faster).
  - **Verification**: 33/33 release unit tests (0 warnings), 36/36 official fixtures, 32/32 adversarial suites, `dev` + 6 cached 1M seeds + 2 fresh random 1M seeds, `seed20-20m` vs `oracle2`, `INGEST_GENERIC=1`, all four 60M datasets (`seed60-60m` 2.94 GiB, `seed60-5kcat` 2.98 GiB, `seed60-90d` 3.03 GiB, `seed60-longids` 5.98 GiB), and clean `git archive` Docker build verified `MATCH` on `dev` and `seed20-20m`.

### Previous submission (`candidate-12b`)
- **Tag**: `candidate-12b` (HEAD on `main`, code `4ec2aa3` on glibc bookworm) — **1-variable allocation/THP/arena-reuse/`mallopt` change ONLY on `candidate-12a` (`0.7205 s`) base** (zero algorithm change).
  - **What it changes vs `candidate-12a` (`bf812eb`, `0.7205 s`)**:
    1. **Unified 2 MiB-aligned `MADV_HUGEPAGE` arena reuse across stages**: `links-uf` reuses the `tok` arena before `parse`; `relocate` reuses the `tok` arena after `dedup`; `group+agg` reuses relocation arena tails; no mid-run `munmap` of parse arenas (`sink_thp()` fixed to always apply `MADV_HUGEPAGE` whenever THP is `always` or `madvise`).
    2. **Startup `mallopt`** (`M_TRIM_THRESHOLD`, `M_MMAP_THRESHOLD`, `M_ARENA_MAX`) to stop glibc from repeatedly `mmap`/`munmap`-ing transient buffers (`UserCols`, per-thread scratch).
    3. **Recycled per-worker `pread` buffers** across input chunks + `/proc/self/smaps_rollup` `AnonHugePages` diagnostic.
  - **Local verification & fault/THP signature** (note: local 14-day-uptime host is RAM-fragmented under `thp=always/madvise`, so 20M only gets `94–146 MiB` THP locally with kernel compaction `sys`, whereas clean memory gets **87% THP**):
    - **Docker `dev` (clean THP fit)**: `90.1 / 103.4 MiB` (**87% `AnonHugePages`**), `links-uf` **7 faults**, `parse` **379 faults**, `dedup` **0 faults**, `relocate` **2 faults**, total **3,309 faults** (`0.073 s`).
    - **20M (`seed20-20m`, fragmented local host)**: `links-parse` faults −80–95% (`5.1k → 0.2–1.1k`), `links-uf` faults −65–90% (`16.8k → 1.7–6.4k`), `group+agg` faults −37% (`16.2k → 9.9k`) & `group+agg sys` −70% (`0.108 → 0.030 s`), `dedup` faults `747`, `cache-misses:u` `48.1M → 45.6M`, exit `AnonHugePages` `4 → 94–146 MiB`, Peak RSS `954 MB` (Docker).
  - **Explicit model prediction BEFORE submission**:
    - **If the clean scorer VM backs the unified 820 MiB arena with 2 MiB THP** (like Docker `dev` at 87% THP, cutting ~200k+ minor faults): **~0.48–0.58 s** (at `1.1–2.8 µs/fault`, minus user CPU floor).
    - **If the scorer VM has THP disabled or fragmented** (only the ~25–35k non-THP arena-reuse/`mallopt` faults drop): **~0.66–0.70 s** (vs `12a`'s `0.7205 s`).
    - **Worst-case risk**: if the scorer VM has `defrag=always` or heavy compaction stalls on `MADV_HUGEPAGE`, compaction `sys` could partially offset fault savings (recall `c3` with `MADV_HUGEPAGE` beat `c4` without it by **−103 ms** on the scorer, strongly indicating the scorer grants THP cleanly!).
  - **Verification**: 30/30 release unit tests (0 warnings), 36 official fixtures, 32 adversarial suites, `dev` + 1M seeds + `seed20-20m`, forced `INGEST_GENERIC=1`, 8 byte-identical `INGEST_DUMP` comparisons, all four 60M datasets (`seed60-60m` **2.85 GiB**, `seed60-longids` **5.84 GiB**, `seed60-5kcat` **2.98 GiB**, `seed60-90d` **2.91 GiB** — all well under 8 GiB), and Docker `ss-test:c12b` (`--network none --read-only --user 1000:1000 --memory=8g`) exact `MATCH` on `dev` and `seed20-20m`.

## Official scores

| Tag | Commit | Official wall (hidden ~20M) | Rank | Notes |
| --- | --- | ---: | --- | --- |
| `candidate-15c` | `f227276` | **0.6894 s** (`v5`) | **7 at sub (`v5`)** | Disjoint short/long unified `Sinks` + THP `ArenaBytes` + fused ID parsers (`-107.1 ms` vs `15b`, receipt `BB-B927381C`) |
| `candidate-15b` | `05343d7` | **0.7965 s** (`v5`) | **4 at sub (`v5`)** | Hybrid inline-64 + side-arena engine (`2.40×` faster than `15a` on `v5`, receipt `BB-6674A5CC`) |
| `candidate-15a` | `ba527a9` (`33de581`) | **1.9110 s** (`v5`) | **13 (`v5`)** | Re-scored on `shopping-sessions-5` (33B opaque IDs); fell back to `generic.rs` |
| `candidate-12a` | `bf812eb` (`c34c244`) | **0.7205 s** | **26 (NEW BEST)** | Accepted exact output; **−99.0 ms vs 11b** from 1-variable `group+agg` rework (−35k faults, −120 MB RSS, −106 ms group sys) → proves scorer heavily charges fresh memory / minor faults (~1.1–2.8 µs/fault) |
| `candidate-11b` | `ea17016` (`7bd0941`) | **0.8195 s** | NEW BEST (leader 0.31 s) | Accepted exact output; direct specialized events/clicks parsers + co-partitioned L2 Links + zero-fault `tok→reloc` arena reuse (−55.7 ms vs c8) |
| `candidate-11a` | `f6d8110` | **0.8678 s** | — | Accepted exact output; co-partitioned L2 Links + quote-bit SIMD scan + BMI2 PEXT keys (matched 0.865–0.890 s out-of-sample model prediction!) |
| `candidate-8` | `5d0f934` (`49473d0`) | **0.8752 s** | 37 | Accepted exact output (H1: 4.46× smaller compact Parquet 97.9→21.9 MB); −7.4 ms vs c5 |
| `candidate-3` | `63b9256` (`4f01a6e`) | **0.8824 s** | 29 | Accepted exact output |
| `candidate-5` | `2bd1a01` (`6c2778c`) | **0.8826 s** | 36 | Accepted exact output; identical to candidate-3 despite halving event DRAM traffic & removing dedup Pass 2 |
| `candidate-9` | `bcf953c` (`exp/h3-arenas-exit`) | **0.8853 s** | 39 | Accepted exact output (H3: unconditional THP + prefault + `_exit(0)`); +3 ms vs c5 → first-touch/exit not dominant |
| `candidate-10` | `e3ccaf9` (`9de36c7`) | **0.8938 s** | 40 | Accepted exact output (1 MiB `mmap` w/ worker `munmap` on c8 base); +18.6 ms vs c8 (`256 KiB pread`); reverted on `main` |
| `candidate-6` | `5f6718c` (`8f5cb0b`) | **0.948 s** | — | Accepted exact output; +65 ms slower with 24 readers × 4 MiB + t=0 fadvise → cold-I/O hypothesis refuted |
| `candidate-4` | `e0b0a9d` (`f52fa03`) | **0.985 s** | — | Accepted exact output; +103 ms slower on scorer despite −26.3% locally (THP off on Sinks, 128 KiB chunks, AVX-512VL-256) |
| `candidate-7` | `8c4250c` (`c4d681c`) | **1.068 s** | — | Accepted exact output (H2: 4 threads vs 8 on c5 base); +0.185 s |
| `probe-1` | `4456bb8` | rejected (dev) | — | Deliberate 0-row dev mismatch; confirmed scorer returns row diff (not stderr) on mismatch |

## 2026-11-20 commit `4f01a6e` (`candidate-3`: 2M huge-page flat Links, RecA eid_hash, ring-32 prefetch, skip view/cart prod.get, 256 KiB L2-hot chunks + 4 prefetch threads, pshufb enc, CompactEv(u64))

Docker (`ss-test:4f01a6ec67`, 115 MB, measured under heavy shared load average ~33.4):

| Data | Mode | Wall (s) | In-process total (s) | links | parse | dedup | group+agg | Peak RSS |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `seed20-20m` (1.87 GB) | cold (load 33) | 2.719 / **2.559** / 2.476 | 2.168 / 1.974 / 1.952 | 0.20–0.29 | 1.16–1.29 | 0.27–0.31 | 0.25–0.30 | 1.37–1.42 GB |
| `seed20-20m` | warm (load 33) | 1.929 / 2.011 / **1.822** (A/B **1.587**) | 1.215 / 1.484 / 1.260 (A/B **1.066**) | 0.16–0.21 | 0.50–0.62 | 0.16–0.28 | 0.21–0.36 | 1.46–1.64 GB |
| `seed60-60m` (5.72 GB) | cold (load 33) | **7.053** / 7.463 | 6.471 / 6.672 | 0.63–0.65 | 4.29–4.31 | 0.73–0.96 | 0.75–0.77 | 3.95–4.08 GB |
| `seed60-60m` | warm (load 33) | 5.858 / **4.765** | 5.259 / **4.086** | 0.62–0.69 | 1.78–2.94 | 0.72–0.79 | 0.86–0.89 | 4.11–4.12 GB |

Back-to-back Docker A/B vs `candidate-1` (`ss-test:8ab1414442`, exact same load ~33, scorer flags):
- `seed20-20m` warm: `candidate-1` **2.690s / 1.998s** (in-proc `2.096s / 1.456s`, 1.81 GB RSS) -> **`candidate-3` 1.707s / 1.587s** (in-proc **`1.147s / 1.066s`**, 1.62–1.64 GB RSS) — **1.37–1.83× faster in-process wall (-21% to -37% Docker wall)**!
- `seed20-20m` cold: `candidate-1` **2.737s** (in-proc `2.147s`, 1.82 GB RSS) -> **`candidate-3` 2.388s** (in-proc **`1.882s`**, 1.63 GB RSS) — **-13% Docker wall**!
- `seed60-60m` warm: `candidate-1` **7.327s** (in-proc `6.672s`, 4.30 GB RSS) -> **`candidate-3` 5.827s** (in-proc **`5.176s`**, 4.08 GB RSS; standalone warm run2 **4.765s wall / 4.086s in-proc**) — **1.26–1.54× faster Docker wall**!

Native single-core `INGEST_BENCH2` (`part-00004.csv`): `s0` (scan) 26.0–27.3 ns/row (2.8–3.0 GB/s), `s4` (full `parse_event` + SWCB push) **69.5 ns/row (1,107 MB/s)** vs 100–115 ns/row in `candidate-1` (**~1.6× faster single-core**).
Native 60M CPU sum across 8 threads: ~15.9s total (`parse` 8.8–11.1s, `dedup` 3.37–3.47s, `group+agg+write` 2.69–2.80s) vs ~29.0s in `candidate-1` (**~1.82× faster total CPU**).

## 2026-11-20 commit `c0688ae` (`candidate-2`: fused group+agg+write, 256B SWCB, u32 dedup+dead bitset, L1 DateTable, branchless scanner)

Docker (`ss-test:c0688ae640`, measured under heavy shared load average ~33–41):

| Data | Mode | Wall (s) | In-process total (s) | links | parse | dedup | group+agg | Peak RSS |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `seed20-20m` (1.87 GB) | cold (load 33) | 2.980 / **3.212** / 3.511 | 1.879 / 1.912 / 2.295 | 0.13–0.22 | 1.16–1.34 | 0.28–0.43 | 0.20–0.31 | 1.50 GB |
| `seed20-20m` | warm (load 33) | 2.282 / **2.414** / 2.619 | 1.286 / 1.347 / 1.684 | 0.12–0.19 | 0.60–0.81 | 0.20–0.46 | 0.21–0.31 | 1.63 GB |
| `seed60-60m` (5.72 GB) | cold (load 33) | 7.502 / 7.903 | 6.584 / 7.008 | 0.34–0.46 | 4.50–4.65 | 0.92 | 0.80–0.95 | 4.36 GB |
| `seed60-60m` | warm (load 33) | 6.018 / 6.135 | 4.864 / 5.304 | 0.41–0.46 | 2.92–3.14 | 0.75–0.78 | 0.73–0.85 | 4.29 GB |

Back-to-back native A/B vs `candidate-1` (`taskset -c 0-7`, `NCPU=8`, same load ~38):
- `seed20-20m` warm: `candidate-1` 1.602–1.714s wall (~6.86s CPU, 1.80 GB RSS) -> **`candidate-2` 1.208–1.438s wall (4.70s CPU, 1.57 GB RSS)** — **1.46× faster CPU** (`parse` 3.40→2.62s, `dedup` 1.51→0.91s, `group+asm+agg+write` 1.78→0.93s).
- `seed60-60m` warm: `candidate-1` 15.39–17.84s wall (~29.0s CPU, 4.74 GB RSS) -> **`candidate-2` 11.83–16.52s wall (~22.5s CPU, 4.28 GB RSS)** — **1.35× faster CPU** (`dedup` 7.14→4.79s, `group+asm+agg+write` 5.82→2.88s).

## 2026-11-20 commit `8ab1414` (`candidate-1`, Opus baseline)

| Data | Mode | Wall (s) | In-process total (s) | links | parse | dedup | group+asm | agg+write | Peak RSS |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `seed20-20m` (1.87 GB, 19.55M kept) | cold | 1.955 / **2.155** / 2.383 | 1.479 / 1.691 / 1.884 | 0.10 | 1.00–1.22 | 0.16–0.25 | 0.13–0.18 | 0.06–0.08 | 1.81 GB |
| `seed20-20m` | warm | 1.627 / **1.579** / 1.313 | 1.165 / 1.154 / 0.930 | 0.07 | 0.52–0.62 | 0.15–0.22 | 0.12–0.17 | 0.05–0.07 | 1.81 GB |
| `seed60-60m` (5.72 GB, 58.88M kept) | cold | 4.905 / 4.241 | 4.517 / 3.822 | 0.20 | 2.40–3.03 | 0.67 | 0.33–0.37 | 0.18 | 5.21 GB |
| `seed60-60m` | warm | 3.011 / **2.886** | 2.581 / 2.475 | 0.17 | 1.23–1.27 | 0.54–0.55 | 0.33–0.34 | 0.18 | 5.25 GB |

## 2026-11-20 commit `4f63b5a` (ingest fast path `6345372`, aggregate/output v1)

| Data | Mode | Wall (s), 3 runs | In-process total (s) | aggregate | write | Peak RSS |
| --- | --- | --- | --- | --- | --- | --- |
| seed20-20m (19.55M events, 4.93M clicks, 2.25M user rows) | cold | 3.48 / 3.56 / 3.45 | 3.05 / 3.15 / 3.01 | 0.046–0.049 | 0.025–0.026 | 2.85 GB |
| seed20-20m | warm | 3.97 / 4.27 / 3.52 | 3.51 / 3.87 / 3.04 | 0.060–0.131 | 0.030–0.047 | 2.85 GB |
| dev (1M) | warm | 0.56 | 0.17 | 0.005 | 0.002 | 0.19 GB |
