#!/usr/bin/env bash
# Time the full container (start to exit) the way the scorer runs it.
#   bench/run.sh DATA_DIR [IMAGE] [RUNS]
# Env: CPUSET (default 0-7; the scorer pins 8 vCPUs), MEM (24g), MODE (cold|warm|both, default both),
#      OUT_ROOT (default $SS_HOME/bench-out, on disk like the scorer's /output),
#      CHECK=1 to compare the last output with $SS_HOME/data/expected/<basename DATA_DIR>.
# Cold = page cache for DATA_DIR evicted with posix_fadvise(DONTNEED) before the run (no root needed).
# Prints one line per run: mode, wall seconds, stage timers, peak RSS (VmHWM, from the binary).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
data=$(cd "$1" && pwd); image=${2:-shopping-sessions:bench}; runs=${3:-3}
SS_HOME=${SS_HOME:-/home/juan/.t3/scratch/2026-11-20-you-are-the-coordinator-for-8d9ce8ed/shopping-sessions}
CPUSET=${CPUSET:-0-7}; MEM=${MEM:-24g}; MODE=${MODE:-both}
OUT_ROOT=${OUT_ROOT:-$SS_HOME/bench-out}
mkdir -p "$OUT_ROOT"
modes=(); case $MODE in both) modes=(cold warm);; *) modes=($MODE);; esac
for mode in "${modes[@]}"; do
  for i in $(seq 1 "$runs"); do
    out=$(mktemp -d "$OUT_ROOT/out.XXXXXX"); scratch=$(mktemp -d "$OUT_ROOT/scr.XXXXXX")
    chmod 777 "$out" "$scratch"
    [ "$mode" = cold ] && python3 "$here/evict.py" "$data" 2>/dev/null
    log=$(mktemp); rc=0
    t0=$(date +%s.%N)
    docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
      --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
      --cpuset-cpus "$CPUSET" --memory "$MEM" --memory-swap "$MEM" --user 1000:1000 \
      -v "$data":/data:ro -v "$out":/output -v "$scratch":/scratch \
      -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
      -e NCPU=8 -e MEM_GB="${MEM%g}" "$image" 2>"$log" >/dev/null || rc=$?
    t1=$(date +%s.%N)
    wall=$(python3 -c "print(f'{$t1-$t0:.3f}')")
    stages=$(grep -E '^\[(stage|ingest|generic|main)\]' "$log" | grep -vE 'links=|persons=' | sed -E 's/^\[[a-z]+\] *//; s/ +/=/' | awk '{print $1}' | paste -sd' ')
    rss=$(grep -oE 'peak_rss_mb=[0-9]+' "$log" || true)
    echo "$(basename "$data") $mode run$i rc=$rc wall=${wall}s $rss | $stages"
    if [ "${CHECK:-0}" = 1 ] && [ "$i" = "$runs" ]; then
      exp="$SS_HOME/data/expected/$(basename "$data")"
      (cd "$here/../tests/official" && "${PY:-python3}" compare.py "$out" "$exp" | tail -1)
    fi
    rm -f "$log"
    rm -rf "$out" "$scratch" 2>/dev/null || true
  done
done
