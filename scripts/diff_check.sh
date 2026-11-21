#!/usr/bin/env bash
# One-command differential check of a candidate against the reference oracle.
#   scripts/diff_check.sh CANDIDATE DATA_DIR [EXPECTED_DIR]
# CANDIDATE: native binary path or Docker image tag (see run_candidate.sh).
# EXPECTED_DIR: a folder with user_daily/ + campaign_daily/ parquet dirs or user_daily.csv +
# campaign_daily.csv. Default: reference output for DATA_DIR (computed once and cached).
# Prints wall time, compare report; exit 0 only on exact MATCH.
set -euo pipefail
source "$(dirname "$0")/env.sh"
cand=$1; data=$2; exp=${3:-}
[ -z "$exp" ] && exp=$(expected_for "$data")
out=$(mktemp -d "$SS_SCRATCH_ROOT/out.XXXXXX")
trap 'rm -rf "$out" 2>/dev/null || true' EXIT
start=$(date +%s.%N)
set +e
"$ROOT/scripts/run_candidate.sh" "$cand" "$data" "$out" 2> "$out.stderr"
rc=$?
set -e
end=$(date +%s.%N)
printf 'candidate exit %d, wall %.2fs on %s\n' "$rc" "$(echo "$end - $start" | bc)" "$data"
if [ "$rc" -ne 0 ]; then tail -20 "$out.stderr"; rm -f "$out.stderr"; echo DIFFER; exit 1; fi
grep '^\[stage\]' "$out.stderr" | tr '\n' ' ' || true; echo
rm -f "$out.stderr"
"$PY" -I "$ROOT/scripts/compare.py" "$exp" "$out"
