#!/usr/bin/env bash
# Run a candidate on every adversarial case and compare with the reference's expected output.
#   scripts/run_adversarial.sh CANDIDATE [CASES_DIR ...]
# Default CASES_DIR: tests/adversarial/cases (committed) and, if present, $SS_DATA/adversarial-big
# (make it with: python3 tests/adversarial/make_cases.py $SS_DATA/adversarial-big --big).
# Expected output per case lives in <case>/expected/ (parquet) and is (re)built by the reference
# when missing or when reference/ref.py changed. Prints one line per case; exit 1 if any fail.
set -uo pipefail
source "$(dirname "$0")/env.sh"
cand=$1; shift
dirs=("$@")
if [ ${#dirs[@]} -eq 0 ]; then
  dirs=("$ROOT/tests/adversarial/cases")
  [ -d "$SS_DATA/adversarial-big" ] && dirs+=("$SS_DATA/adversarial-big")
fi
refsha=$(sha256sum "$ROOT/reference/ref.py" | cut -c1-16)
pass=0; fail=0; failed=()
for d in "${dirs[@]}"; do
  for case in "$d"/*/; do
    case=${case%/}; name=$(basename "$case")
    [ -d "$case/input" ] || continue
    if [ "$(cat "$case/expected/.refstamp" 2>/dev/null)" != "$refsha" ]; then
      rm -rf "$case/expected"; mkdir -p "$case/expected"
      if ! "$PY" -I "$ROOT/reference/ref.py" "$case/input" "$case/expected" > /dev/null; then
        echo "REFERENCE FAILED on $name"; fail=$((fail+1)); failed+=("$name"); continue
      fi
      REF_CSV=1 "$PY" -I "$ROOT/reference/ref.py" "$case/input" "$SS_SCRATCH_ROOT/refcsv.$$" > /dev/null
      cp "$SS_SCRATCH_ROOT/refcsv.$$"/*.csv "$case/expected/" 2>/dev/null; rm -rf "$SS_SCRATCH_ROOT/refcsv.$$"
      # keep the human-readable dumps out of compare.py's way
      for t in user_daily campaign_daily; do mv "$case/expected/$t.csv" "$case/expected/$t.txt"; done
      echo "$refsha" > "$case/expected/.refstamp"
    fi
    if [ "$cand" = "--expected-only" ]; then continue; fi
    out=$(mktemp -d "$SS_SCRATCH_ROOT/adv.XXXXXX")
    if ! "$ROOT/scripts/run_candidate.sh" "$cand" "$case/input" "$out" > "$out.log" 2>&1; then
      echo "FAIL $name: candidate exit $(tail -3 "$out.log" | tr '\n' ' ')"; fail=$((fail+1)); failed+=("$name")
    elif "$PY" -I "$ROOT/scripts/compare.py" "$case/expected" "$out" > "$out.cmp" 2>&1; then
      echo "ok   $name"; pass=$((pass+1))
    else
      echo "FAIL $name ($(head -1 "$case/CASE.md" | sed 's/^# //'))"; sed 's/^/    /' "$out.cmp" | head -60
      fail=$((fail+1)); failed+=("$name")
    fi
    rm -rf "$out" "$out.log" "$out.cmp"
  done
done
[ "$cand" = "--expected-only" ] && exit 0
echo "adversarial: $pass passed, $fail failed ${failed[*]:-}"
[ "$fail" -eq 0 ]
