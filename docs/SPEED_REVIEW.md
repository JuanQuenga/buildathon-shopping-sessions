# Speed review: the 0.88 s as a budget on the scorer

Reviewer: independent second pair of eyes (read-only on the code). Date: 2026-11-21, 01:40 ET.
Reviewed: `origin/main` at `63b9256` (candidate-3), read in full. I also built and timed the
work-in-progress commit `2d0237a` on `wip-neon`, because three of its changes (mmap input,
`mem::forget` of the sinks, AVX-512 scan) are exactly what this review was asked to judge.
All measurements were made in a private copy, on at most 2 cores, with the 20M and 1M sets.

**Leaderboard now (fetched 01:30 ET): leader 0.59 s, then 0.62, 0.63, 0.64, 0.65, 0.70. We are
at 0.88 s with about 20 entries ahead. The gap to first is 0.29 s (33%).**

## Headline

1. **The WIP branch hides about 105 ms of serial work behind process exit.** `2d0237a` is faster
   inside its own timer, but it `mem::forget`s the sinks and the file mappings, so the kernel
   tears them down on one thread after `main` returns. The scorer times the container, not `main`.
   Measured at 20M, 2 threads: time outside `main` is **8 to 10 ms on `63b9256` and 111 to 121 ms
   on `2d0237a`**. That cost does not shrink with more threads, so on the scorer it is about 15 to
   20% of the run. Free the memory in parallel before exit instead. This is the first thing to fix.
2. **Measure process wall, not the in-process total.** Every A/B in `bench/RESULTS.md` and in the
   WIP commit message uses the internal timer. Use start-to-exit of the process (or the container)
   for every decision from now on.
3. **About a third of our CPU time is kernel time**, mostly first-touch page faults on 1.2 to
   1.4 GB of fresh arenas, plus the `pread` copy and the frees. Bytes of fresh memory per event is
   the lever that matters most after the exit fix.
4. **The local bench topology is wrong.** `--cpuset-cpus 0-7` on the dev box is 8 physical cores.
   The scorer has 4 cores with 2 hyperthreads each. Use `0-3,8-11`.
5. Realistic total from the list below: **120 to 200 ms at 20M, so about 0.68 to 0.76 s**. Reaching
   0.59 s needs all of it plus a favourable huge-page regime on the scorer, which we cannot see.

## Where the 0.88 s goes

Local numbers are from a busy shared laptop CPU, so only proportions are reliable. One thread,
`seed20-20m`, warm cache, `63b9256`, transparent huge pages disabled for the process so the
figures are not distorted by local compaction stalls (see the THP section):

| Stage | CPU s | of which kernel | Notes |
| --- | ---: | ---: | --- |
| small (manifest, products, campaigns, file list) | 0.02 | | serial, before any bulk byte is read |
| links parse + union-find | 0.18 | 0.05 | |
| parse | 2.17 | 0.68 | events about 1.5, clicks about 0.4, `pread` copy 0.25, page faults 0.43 |
| dedup | 0.85 | 0.23 | pass 1 (the actual dedup) 0.095; pass 2 (links lookup + scatter of `RecB`) 0.67; serial `drop(ev_sinks)` 0.07 |
| group + aggregate + write | 0.71 | 0.14 | map 0.07, scatter 0.03, clicks 0.04, sort events 0.13, sort clicks 0.08, sessionize + emit 0.15, assists 0.13, serial drops 0.07 |
| **total** | **3.95** | **1.10** | user 2.85, kernel 1.10, 346k minor faults |

Scaled to the official 0.88 s, assuming 0.05 to 0.10 s of container fixed cost that everybody
pays (a no-op container lives 85 ms by Docker's own timestamps on the dev box):

| Part | Estimated share of 0.88 s |
| --- | ---: |
| Container start and exit, outside our control | 0.05 to 0.10 |
| Serial in-process work (small stage, frees, merges) | 0.04 to 0.15 |
| parse | about 0.40 |
| dedup | about 0.15 |
| group + aggregate + write | about 0.12 |
| links | about 0.035 |

One local CPU-second of parallel work is worth roughly 0.18 s of scorer wall. One local second of
serial work is worth about one second. That is why the serial items rank first.

## Ranked list

Savings are milliseconds of official wall. 60M figures assume linear scaling of the 20M figure
unless the cost is fixed. "Regime" means the saving depends on whether the scorer hands out huge
pages cheaply, which nobody on the team can currently observe.

| # | Change | 20M | 60M | Confidence | Exactness risk | Effort |
| --- | --- | ---: | ---: | --- | --- | --- |
| 1 | Free arenas and mappings in parallel before exit; never `forget` them | 5 to 70 on `main`, **about 90 on WIP** | 15 to 200, about 270 on WIP | High on WIP (measured). Regime-dependent on `main` | None | 1 h |
| 2 | Run the small stage concurrently with links, or make it cheap | 15 to 25 | 15 to 25 | Medium-high | Low | 1 h |
| 3 | Skip the `fadvise` prefetch threads when the page cache is already warm | 5 to 15 | 15 to 45 | Medium | None | 30 min |
| 4 | Bench on the scorer's topology with process wall and user/kernel split, then retune chunk and write-buffer sizes | 0 to 40 | 0 to 120 | Unknown sign per knob; the method is certain | None | 1 to 2 h |
| 5 | Materialise each event once (16-byte dedup token + 16-byte person record at parse; dead-bit set) | 45 to 90 | 150 to 270 | Medium (55%) | Moderate: exact by design, needs the full oracle matrix | 6 to 10 h |
| 6 | Decide the huge-page policy from evidence, not from the dev box | −60 to +60 | ×3 | Low without scorer evidence | None | 1 h plus a probe |
| 7 | Group-stage micro work (assists, small sorts, unchecked column pushes) | 10 to 20 | 30 to 60 | Medium | Low | 3 h |
| 8 | Fuse scanner stages for the 7-field row | 15 to 30 | 45 to 90 | Low-medium | Medium (scanner is correctness-critical) | 4 to 6 h |
| 9 | Persistent thread pool (already in WIP) | 3 to 5 | 3 to 5 | Medium | Low | done |

**Top five to do first: 1, 4, 2, 3, then 5 if at least eight hours of verified working time
remain before Sunday 11:00 ET.** If not, do 7 instead of 5.

### 1. Free memory in parallel, before exit

What: in `fast.rs`, `drop(ev_sinks)` after dedup and `drop(b_sinks); drop(ck_sinks)` after the
group stage run on the main thread on `63b9256`. On `2d0237a` they are replaced by
`std::mem::forget`, and the read-only file mappings are forgotten too. Neither is right. Replace
both with a parallel teardown: one task per arena on the worker pool calling `munmap`, and for
mapped input a `madvise(MADV_DONTNEED)` by each worker on the chunk it just finished. Also drop
each worker's `UserCols` inside its own `fini` hook rather than in the serial
`workers.into_iter()` on the main thread.

Why: the kernel frees an address space on one thread, and it does so before the parent is told
the process is gone, so the scorer's clock is still running. Measured:

| Build | In-process total, best of 3 (2 threads) | Time outside `main`, median |
| --- | ---: | ---: |
| `63b9256` | 2.22 to 2.28 s | 8 to 10 ms |
| `2d0237a` (WIP) | 1.70 to 1.83 s | **111 to 121 ms** |

On `63b9256` the same cost sits inside the stages: 0.14 s of serial frees with 4 KiB pages,
0.07 to 0.09 s when some huge pages were obtained, near zero if every arena is huge-page backed.
A page-level test left by the previous reviewer on this box gives 0.22 µs per 4 KiB page to
unmap, or 58 ms per GB, single-threaded.

### 2. Small stage off the critical path

What: `build_ctx` reads the manifest, parses 50,000 products into a `HashMap<Vec<u8>, u32>`
(one allocation per key, SipHash, a sort of 50,000 cloned category strings), builds `IdLookup`
with three passes over that map, then lists and opens the files. It takes 19 to 20 ms of serial
CPU before the first bulk read. Links parsing and union-find need none of it. Start the file
listing first, then build products and campaigns on one pool thread while links run. Event
parsing waits for `IdLookup`, which will be ready long before.

Why on the scorer: serial milliseconds cost full wall, and a Cascade Lake core is slower than the
dev box's. This saving is the same at 60M.

### 3. Warm-cache check before prefetching

What: `build_ctx` always starts up to four threads that call `posix_fadvise(WILLNEED)` over every
file. On a warm cache this only walks the page cache. Measured at 20M: median kernel time 1.86 s
with prefetch and 1.62 s without (minimum 1.57 and 1.51; noisy, same sign on the 1M set: 144 ms
and 125 ms median wall). Probe once with `preadv2(..., RWF_NOWAIT)` on the first 256 KiB of the
largest file: data returned means cached, `EAGAIN` means cold. Start the prefetchers only when
cold. A wrong guess costs time, never correctness.

Why on the scorer: practice takes the median of three runs on the same files, so at least two
are warm, and a 0.59 s leader on 1.9 GB of input is only possible from memory.

### 4. Bench method

- Topology: `lscpu -e` on the dev box shows CPUs 0 to 7 are eight different cores, and 8 to 15
  are their siblings. `bench/run.sh` pins to `0-7`. Use `--cpuset-cpus 0-3,8-11` to get 4 cores
  with 2 threads each, which is what "8 vCPUs" means on N2.
- Each Cascade Lake core has 1 MiB of L2 shared by its two threads. The current parse working
  set per thread is a 256 KiB chunk plus 256 KiB of write-combining lines at 20M and 512 KiB at
  60M (`p1 = 2048`). Two threads on a core need 1 to 1.5 MiB. Test a 128 KiB chunk and 128-byte
  lines on the sibling topology at both sizes. My 2-core attempt at chunk sizes was too noisy to
  call (spread 20% at load 10), so this is open, not a finding.
- Timers: print `getrusage` user, kernel and minor-fault counts at every stage lap. The kernel
  share is invisible today.
- Clock: wrap the binary and time start to exit. The internal total missed 105 ms above.

### 5. One materialisation per event

Today every well-formed event is written as a 32-byte `RecA` into partitions by event id, read
back for dedup, and its survivor is written again as a 16-byte `RecB` into partitions by person.
Pass 1, the actual dedup, costs 0.095 s. Pass 2, which only moves survivors, costs 0.67 s. That
is 48 bytes of fresh memory per event and two scatters.

Proposed, exact, no hashing-only equality:

- At parse, resolve the canonical person with the existing ring-32 prefetch (as clicks already
  do) and push the 16-byte person record straight into the person partitions. Its position
  `(thread, partition, index)` is known at push time.
- Also push a 16-byte token `{event id key, ingest, position}` into the event-id partitions.
  Rows that fail validation push a token with a "no record" position, because an invalid copy
  can still win the dedup and must kill the valid copies.
- Dedup reads only tokens: same table, same exact key comparison, same "smallest ingest wins,
  first seen wins a tie" rule. For every loser it sets one bit in a per-sink dead bitset
  (20.9M bits is 2.6 MB at 20M). No pass 2.
- The group stage skips records whose dead bit is set.

Fresh bytes per event fall from 48 to 32, one scatter and the whole second allocation wave
disappear, and the links lookup moves into parse at the same cost. Estimated net 0.2 to 0.45
local CPU-seconds of 3.95.

Fallbacks needed: position must fit its bit budget (a whale's partition can hold over a million
records, so use a 64-bit token layout or fall back to today's path when an index overflows); the
wide-key path needs the same treatment; overflow vectors keep the same logical index. Verify
with the 36 fixtures, the adversarial suite, the seeds, and the 20M and 60M oracle before
tagging.

### 6. Huge pages: get evidence

On the dev box `MADV_HUGEPAGE` is erratic. With it, kernel time was 1.38 to 2.12 s; with huge
pages disabled for the process it was a steady 1.08 to 1.11 s. But when huge pages were actually
obtained, user time was 17% lower (2.33 to 2.42 s against 2.80 to 2.87 s). The dev box has no
free memory and a fragmented free list, so every madvised fault tries direct compaction first
(`defrag = madvise`). A scorer VM that has built and run many images may be in the same state or
may be clean.

WIP drops `MADV_HUGEPAGE` on the sinks. That is the right call for the dev box and a coin flip
for the scorer: with `enabled = always` it keeps opportunistic huge pages without stalls, with
`enabled = madvise` (the Ubuntu default) it gives up huge pages entirely.

Options, in order of preference:
1. Find out. A submission that fails the dev check returns its logs. One deliberately failing
   run that prints `/sys/kernel/mm/transparent_hugepage/{enabled,defrag}`, the CPU model, kernel
   version, the filesystems behind `/data` and `/output`, and the stage timers with the
   user/kernel split would settle this and item 4. It costs one of the two attempts per hour and
   does not touch the board. This is the team owner's decision, not an agent's.
2. Decide at run time: read `enabled`. If `always`, do not madvise. If `madvise`, madvise the
   first 16 MiB, touch it, and time it; keep madvising only if a 2 MiB fault stayed under about
   0.5 ms.

### 7 and 8. Remaining CPU work

- Assists cost 27 ns per click (0.13 s). `count_interval` does two or three binary searches per
  campaign interval; most persons have one or two session days, so a linear walk for short
  session lists should be cheaper.
- Small sorts: 0.13 s for events, 0.08 s for clicks. Try a higher insertion-sort cut-off (24 to
  32) and a branch-free network for 8 or fewer.
- `UserCols::push` does seven capacity-checked pushes per row. Reserve once per partition.
- Scanner: 17.4 ns per row single-threaded (4.4 GB/s), about a third of event parse. Stage 1
  writes every separator position to memory and stage 2 reads it back. A row-at-a-time variant
  that takes the comma mask of the row's first 64 to 128 bytes and peels six commas in registers
  could reach about 10 ns. It must keep the quote-count identity and pass the randomized
  equivalence test.

## Measured and rejected

| Idea | Measurement | Verdict |
| --- | --- | --- |
| `mmap` instead of `pread` (in WIP) | 1.61 GB of events, 2 threads, warm, AVX2 scan as consumer. `pread` into a 256 KiB buffer: 0.31 CPU-s, 0.157 to 0.179 s wall including exit. `mmap` with fault-around: 0.24 CPU-s but 0.194 to 0.233 s wall including exit, because unmapping at exit is serial. `mmap` plus `MADV_DONTNEED` per chunk: 0.29 CPU-s, 0.151 to 0.159 s. `MADV_POPULATE_READ` and `MAP_POPULATE`: worse | No gain over `pread` when torn down properly, a loss when forgotten. Keep it only with per-chunk `MADV_DONTNEED`, and re-measure by process wall |
| `FROM scratch` + static binary | No-op binary, scorer flags, 16 interleaved runs each. Debian + dynamic: `docker start -a` 180 ms, container lifetime 84 ms. Scratch + static: 174 ms and 86 ms | No difference. Do it for hygiene if wanted, not for time. `CMD` is already a direct exec |
| AVX-512 scan (in WIP) | Not measurable here: the dev box is Zen 4, which has no frequency licence. On Cascade Lake, 512-bit integer work puts the core in the L1 licence and lowers its frequency for all code on it, and the scan is about a fifth of parse | Unproven on the scorer and possibly negative. Keep `INGEST_NO_AVX512` and A/B it there, or use the 256-bit mask forms (AVX-512VL/BW), which carry no licence cost |
| Five unconditional separator slots in stage 1 | 10.8% of 32-byte halves hold more than four separators, so the spill branch is data-dependent. Built it: 17.4 ns per row both ways | No gain |
| Narrower or encoded Parquet columns | `user_daily` is 96.3 MB at 20M: `user_id` 29.1, three INT64 columns 18.0 each, `day` 9.0, `top_category` 4.3. Writing costs about 30 ms of CPU spread over 8 workers | Under 5 ms of wall available. Not worth any schema risk. No `fsync`, 8 files, one vectored write each: leave as is |
| `O_DIRECT`, more reader threads | Input is served from memory in the timed runs | No |
| Allocator change | Hot structures are explicit mappings; `malloc` is not on the hot path | No |
| Fewer than 8 threads for some stage | Parse is compute-bound and still gains from the sibling thread; dedup pass 2 and the group stage are latency-bound and gain more | Keep 8 everywhere |

## Risks for the final

Correctness: I found no exactness bug in `63b9256`. The scanner's quote-count identity, chunk
ownership, the dedup tie rule, click dedup after sort, attribution and the assist interval union
all hold. The date cache cannot return a wrong date: a slot collision fails the full comparison
and falls to the exact path.

What could still break or slow the run:

1. **Unverified WIP.** `2d0237a` is marked WIP and the working tree has further uncommitted edits
   on top of it. Its message reports matches on 20M and the three 60M stress sets, but not the
   full matrix at 8 threads in Docker. Anything submitted as final must pass the whole matrix
   from a clean checkout of the exact commit, timed by container wall.
2. **Exit teardown at 60M.** With `forget`, 5.7 GB of mappings and about 4 GB of arenas are
   unmapped serially after `main`: roughly 0.3 to 0.5 s that no internal timer shows.
3. **Huge-page regime.** Fresh memory is 4.1 GB at 60M. With 4 KiB pages that is about a million
   faults. A scorer with fragmented memory and `MADV_HUGEPAGE` could stall as the dev box does.
   This costs time, not correctness, and nowhere near the 180 s cap.
4. **AVX-512 path runs for the first time on the scorer.** The dev box supports AVX-512BW, so
   the code path is exercised, but any instruction beyond F/BW/VL/DQ/CD (VBMI, VBMI2, VPOPCNTDQ)
   would be an illegal instruction on Cascade Lake. Check the disassembly of the final binary
   for those, or run it under an emulator restricted to Cascade Lake.
5. **Leaving the fast path.** More than 256 categories, a window over about 42 days, or an id
   over 15 bytes sends `63b9256` to the single-threaded `slow::load`, which would not finish 60M
   in 180 s. The generator cannot produce these, and WIP's parallel generic path (10 to 13 s on
   the stress sets at 3 threads) closes the hole. Make sure that path is in whatever is submitted.
6. **Wide-key retry.** Any id with a character outside `[0-9A-Za-z]` or longer than 10 restarts
   the whole run with 128-bit keys: about twice the time and twice the record memory (still far
   below 24 GB). Generator ids never trigger it.
7. **Skew.** A shared-device person can own over a million events in one partition. The overflow
   vectors and the 64-bit dedup table handle it exactly; the cost is a slower partition, already
   scheduled largest-first.
8. **Cold first run.** If the first final run reads from disk, it is the slow one of three and
   the median ignores it. Keep the prefetchers for that case (item 3 keeps them when cold).

## How this was measured

`seed20-20m` (1.87 GB) and the 1M dev set, warm cache, `taskset` to one or two CPUs, load
average 6 to 11 from other agents. Phase timers and `getrusage` counters were added to a private
copy only. Huge pages were switched off per process with `prctl(PR_SET_THP_DISABLE)`. Container
overhead used no-op images with the scorer's flags. Nothing was submitted, and no file in the
shared clone was changed apart from this document.
