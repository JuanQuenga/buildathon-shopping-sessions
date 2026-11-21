#!/usr/bin/env bash
# Expected output for a large generated dataset from the DuckDB oracle (reference/oracle2.py), cached.
#   scripts/oracle2.sh DATA_DIR   -> prints the folder holding user_daily/ and campaign_daily/
set -euo pipefail
source "$(dirname "$0")/env.sh"
data=$(cd "$1" && pwd)
stamp="$(sha256sum "$ROOT/reference/oracle2.py" | cut -c1-16) $data"
dir="$SS_DATA/expected2/$(basename "$data")"
if [ "$(cat "$dir/.stamp" 2>/dev/null)" != "$stamp" ]; then
  rm -rf "$dir"; mkdir -p "$dir"
  "$PY" -I "$ROOT/reference/oracle2.py" "$data" "$dir" --threads "${O2_THREADS:-8}" --memory "${O2_MEM:-8GB}" >&2
  echo "$stamp" > "$dir/.stamp"
fi
echo "$dir"
