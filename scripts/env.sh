# Shared settings for the test scripts (sourced). Override any of these from the environment.
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SS_HOME=${SS_HOME:-/home/juan/.t3/scratch/2026-11-20-you-are-the-coordinator-for-8d9ce8ed/shopping-sessions}
SS_DATA=${SS_DATA:-$SS_HOME/data}
SS_SCRATCH_ROOT=${SS_SCRATCH_ROOT:-$SS_HOME/scratch-test}
export SS_SCRATCH_ROOT
DEV_ANSWER=${DEV_ANSWER:-$SS_HOME/starter/dev-answer/dev-answer}
GENERATE=${GENERATE:-$SS_DATA/bin/generate}
if [ -z "${PY:-}" ]; then
  if [ -x "$SS_HOME/work-test/venv/bin/python" ]; then PY="$SS_HOME/work-test/venv/bin/python"; else PY=python3; fi
fi
mkdir -p "$SS_SCRATCH_ROOT"
# expected_for DATA_DIR -> prints a folder with the reference output for DATA_DIR (cached).
expected_for() {
  local data; data=$(cd "$1" && pwd)
  local stamp; stamp="$(sha256sum "$ROOT/reference/ref.py" | cut -c1-16) $data"
  local dir="$SS_DATA/expected/$(basename "$data")"
  case "$data" in "$SS_DATA"/*) ;; *) dir="$SS_SCRATCH_ROOT/expected/$(echo "$data" | sha256sum | cut -c1-16)";; esac
  if [ "$(cat "$dir/.refstamp" 2>/dev/null)" != "$stamp" ]; then
    rm -rf "$dir"; mkdir -p "$dir"
    "$PY" -I "$ROOT/reference/ref.py" "$data" "$dir" >&2 || { echo "reference failed on $data" >&2; return 1; }
    echo "$stamp" > "$dir/.refstamp"
  fi
  echo "$dir"
}
