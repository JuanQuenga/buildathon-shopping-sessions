#!/usr/bin/env bash
# Everything that must pass before a submission:
#   scripts/check_all.sh CANDIDATE [N_RANDOM_SEEDS]
# CANDIDATE: native binary, Docker image tag, or "head" (build origin/main via build_head.sh).
#  1. the 36 official fixtures (official run_fixtures.py, trailing positional args included)
#  2. adversarial cases (tests/adversarial/cases + $SS_DATA/adversarial-big if generated)
#  3. the public dev answer (seed 1, 1M events)
#  4. the cached 1M seeds in $SS_DATA (seed*-1m) + N fresh random 1M seeds (default 2), vs the reference
#  5. if $SS_BIG=1: seed20-20m vs the DuckDB oracle (scripts/oracle2.sh)
# Prints a pass/fail matrix; exit 0 only if every line passed.
set -uo pipefail
source "$(dirname "$0")/env.sh"
cand=$1; nrand=${2:-2}
if [ "$cand" = head ]; then cand=$("$ROOT/scripts/build_head.sh") || exit 1; fi
declare -a matrix; rc=0
record() { matrix+=("$(printf '%-6s %s' "$1" "$2")"); [ "$1" = PASS ] || rc=1; }
log=$(mktemp "$SS_SCRATCH_ROOT/check_all.XXXXXX.log")
echo "candidate: $cand   (full log: $log)"

if (cd "$ROOT/tests/official" && "$PY" -I run_fixtures.py fixtures -- "$ROOT/scripts/run_candidate.sh" "$cand") >> "$log" 2>&1; then
  record PASS "official fixtures ($(grep -c '^ok' "$log")/36)"
else record FAIL "official fixtures: $(grep '^FAIL' "$log" | cut -d' ' -f2 | sort -u | tr '\n' ' ')"; fi

if out=$("$ROOT/scripts/run_adversarial.sh" "$cand" 2>&1); then record PASS "adversarial: $(echo "$out" | tail -1)"
else echo "$out" >> "$log"; record FAIL "adversarial: $(echo "$out" | tail -1)"; fi

if out=$("$ROOT/scripts/diff_check.sh" "$cand" "$SS_DATA/dev" "$DEV_ANSWER" 2>&1); then
  record PASS "dev answer ($(echo "$out" | grep -o 'wall [0-9.]*s'))"
else echo "$out" >> "$log"; record FAIL "dev answer: $(echo "$out" | grep -E 'rows|exit' | tr '\n' ' ')"; fi

seeds=("$SS_DATA"/seed*-1m)
for i in $(seq 1 "$nrand"); do
  s=$(( (RANDOM << 15 | RANDOM) + 1000 ))
  d="$SS_SCRATCH_ROOT/rand-seed$s"
  "$GENERATE" --seed "$s" --events 1000000 --clicks 250000 --output "$d" --threads 8 >/dev/null && seeds+=("$d")
done
for d in "${seeds[@]}"; do
  [ -d "$d" ] || continue
  if out=$("$ROOT/scripts/diff_check.sh" "$cand" "$d" 2>&1); then
    record PASS "$(basename "$d") ($(echo "$out" | grep -o 'wall [0-9.]*s'))"
  else echo "$out" >> "$log"; record FAIL "$(basename "$d"): $(echo "$out" | grep -E 'rows|exit' | tr '\n' ' ')"; fi
  case "$d" in "$SS_SCRATCH_ROOT"/rand-seed*) [ "${KEEP_RANDOM:-0}" = 1 ] || rm -rf "$d" "$SS_SCRATCH_ROOT/expected";; esac
done

if [ "${SS_BIG:-0}" = 1 ]; then
  if out=$("$ROOT/scripts/diff_check.sh" "$cand" "$SS_DATA/seed20-20m" "$("$ROOT/scripts/oracle2.sh" "$SS_DATA/seed20-20m")" 2>&1); then
    record PASS "seed20-20m vs oracle2 ($(echo "$out" | grep -o 'wall [0-9.]*s'))"
  else echo "$out" >> "$log"; record FAIL "seed20-20m: $(echo "$out" | grep -E 'rows|exit' | tr '\n' ' ')"; fi
fi

echo; echo "== check_all matrix for $cand"; printf '%s\n' "${matrix[@]}"
[ $rc -eq 0 ] && echo "ALL PASS" || echo "SOME FAILED (details in $log)"
exit $rc
