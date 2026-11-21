#!/usr/bin/env bash
# Time the full container (start to exit) the way the scorer runs it.
#   bench/run.sh DATA_DIR [IMAGE] [RUNS]
# Env: CPUSET (default 0-3,8-11, four cores with two SMT threads each),
#      NCPU (8), MEM (24g), MODE (cold|warm|both, default both),
#      OUT_ROOT (default $SS_HOME/bench-out, on disk like the scorer's /output),
#      CHECK=1 to compare the last output with $SS_HOME/data/expected/<basename DATA_DIR>.
# Cold evicts input pages with posix_fadvise(DONTNEED); warm reads them before timing.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
data=$(cd "$1" && pwd); image=${2:-shopping-sessions:bench}; runs=${3:-3}
source "$here/../scripts/env.sh"
CPUSET=${CPUSET:-0-3,8-11}; NCPU=${NCPU:-8}; MEM=${MEM:-24g}; MODE=${MODE:-both}
OUT_ROOT=${OUT_ROOT:-$SS_HOME/bench-out}
mkdir -p "$OUT_ROOT"
out= scratch= log= cid=
cleanup() {
  if [[ -n "$cid" ]]; then docker rm -f "$cid" >/dev/null 2>&1 || true; fi
  rm -rf -- "$out" "$scratch" "$log"
}
trap cleanup EXIT
case $MODE in
  both) modes=(cold warm) ;;
  cold|warm) modes=("$MODE") ;;
  *) echo "Unknown MODE: $MODE" >&2; exit 2 ;;
esac
for mode in "${modes[@]}"; do
  for i in $(seq 1 "$runs"); do
    out=$(mktemp -d "$OUT_ROOT/out.XXXXXX"); scratch=$(mktemp -d "$OUT_ROOT/scr.XXXXXX")
    chmod 777 "$out" "$scratch"
    log=$(mktemp "$OUT_ROOT/run.XXXXXX")
    if [[ "$mode" = cold ]]; then
      python3 "$here/evict.py" "$data" >/dev/null
    else
      find "$data" -type f -name '*.csv' -exec cat {} + >/dev/null
    fi
    cid=$(docker create --platform linux/amd64 \
      --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
      --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
      --cpus "$NCPU" --cpuset-cpus "$CPUSET" --memory "$MEM" --memory-swap "$MEM" --user 1000:1000 \
      -v "$data":/data:ro,z -v "$out":/output:z -v "$scratch":/scratch:z \
      -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
      -e NCPU="$NCPU" -e MEM_GB="${MEM%g}" "$image")
    read -r wall cli_wall rc < <(python3 - "$cid" "$log" <<'PY'
import datetime
import json
import subprocess
import sys
import time

cid, log = sys.argv[1:]
with open(log, "w") as err:
    start = time.monotonic()
    run = subprocess.run(["docker", "start", "-a", cid], stdout=subprocess.DEVNULL, stderr=err)
    cli_wall = time.monotonic() - start
state = json.loads(subprocess.check_output(["docker", "inspect", "--format", "{{json .State}}", cid]))
started, finished = (datetime.datetime.fromisoformat(state[key]) for key in ("StartedAt", "FinishedAt"))
wall = (finished - started).total_seconds()
print(f"{wall:.6f} {cli_wall:.6f} {run.returncode or state['ExitCode']}")
PY
    )
    echo "$(basename "$data") $mode run$i rc=$rc wall=${wall}s docker_start_wall=${cli_wall}s"
    cat "$log"
    [[ "$rc" = 0 ]] || exit "$rc"
    if [[ "${CHECK:-0}" = 1 && "$i" = "$runs" ]]; then
      "$PY" "$here/../scripts/compare.py" "$out" "$SS_DATA/expected/$(basename "$data")"
    fi
    cleanup
    out= scratch= log= cid=
  done
done
