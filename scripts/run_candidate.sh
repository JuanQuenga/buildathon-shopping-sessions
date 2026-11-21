#!/usr/bin/env bash
# Run a candidate pipeline once, the way the scorer would.
#   run_candidate.sh CANDIDATE [DATA_DIR OUTPUT_DIR]
# CANDIDATE is either an executable file (native binary, run with the scorer's env and
# two trailing positional args to prove it tolerates them) or a Docker image tag (run with
# the scorer's flags: read-only root, UID 1000, no network, 8 cpus, 24g, 64 MB /tmp tmpfs).
# DATA_DIR/OUTPUT_DIR may also come from the environment (run_fixtures.py sets both AND
# appends them as trailing args). Exit code is the candidate's.
set -euo pipefail
cand=$1; shift
data=${1:-${DATA_DIR:?}}; out=${2:-${OUTPUT_DIR:?}}
data=$(cd "$data" && pwd); mkdir -p "$out"; out=$(cd "$out" && pwd)
SCRATCH_ROOT=${SS_SCRATCH_ROOT:-/home/juan/.t3/scratch/2026-11-20-you-are-the-coordinator-for-8d9ce8ed/shopping-sessions/scratch-test}
mkdir -p "$SCRATCH_ROOT"
scratch=$(mktemp -d "$SCRATCH_ROOT/run.XXXXXX")
cleanup() { rm -rf "$scratch" 2>/dev/null || true; }
trap cleanup EXIT
CPUS=${CPUS:-8}; MEM=${MEM:-24g}
if [ -f "$cand" ] && [ -x "$cand" ]; then
  env -i PATH=/usr/bin:/bin DATA_DIR="$data" OUTPUT_DIR="$out" SCRATCH_DIR="$scratch" HOME="$scratch" \
      NCPU="$CPUS" MEM_GB="${MEM%g}" ${RUST_BACKTRACE:+RUST_BACKTRACE=$RUST_BACKTRACE} \
      "$cand" "$data" "$out"
else
  chmod 777 "$out" "$scratch"
  docker run --rm --platform linux/amd64 \
    --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges \
    --pids-limit 4096 --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
    --user 1000:1000 \
    -v "$data":/data:ro,z -v "$out":/output:z -v "$scratch":/scratch:z \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU="${CPUS%.*}" -e MEM_GB="${MEM%g}" \
    "$cand"
fi
