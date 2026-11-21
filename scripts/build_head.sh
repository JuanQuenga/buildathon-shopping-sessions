#!/usr/bin/env bash
# Build the pipeline binary from a committed revision (default: origin/main after fetch),
# in an isolated export, so tests never see another agent's half-edited working tree.
#   scripts/build_head.sh [REV]   -> prints the path of the built binary on stdout
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$ROOT"
if [ -z "${1:-}" ]; then git fetch -q origin main; rev=origin/main; else rev=$1; fi
sha=$(git rev-parse --short=10 "$rev")
dir="$SS_HOME/work-test/builds/$sha"
if [ ! -x "$dir/pipeline" ]; then
  rm -rf "$dir.src"; mkdir -p "$dir.src" "$dir"
  git archive "$sha" | tar -x -C "$dir.src"
  (cd "$dir.src" && CARGO_TARGET_DIR="$SS_HOME/work-test/target-head" "${CARGO:-$HOME/.cargo/bin/cargo}" build --release --quiet >&2)
  cp "$SS_HOME/work-test/target-head/release/pipeline" "$dir/pipeline"
fi
echo "$dir/pipeline"
