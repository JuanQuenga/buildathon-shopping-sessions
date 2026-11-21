#!/usr/bin/env bash
# Build the Docker image from a committed revision (default origin/main) exactly as the scorer
# would (repo root Dockerfile, linux/amd64), from a clean `git archive` export.
#   scripts/build_image.sh [REV]   -> prints the image tag (ss-test:<sha>)
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$ROOT"
if [ -z "${1:-}" ]; then git fetch -q origin main; rev=origin/main; else rev=$1; fi
sha=$(git rev-parse --short=10 "$rev")
tag="ss-test:$sha"
if ! docker image inspect "$tag" >/dev/null 2>&1; then
  src="$SS_HOME/work-test/builds/$sha.img-src"
  rm -rf "$src"; mkdir -p "$src"
  git archive "$sha" | tar -x -C "$src"
  du -sh "$src" >&2
  docker build --platform linux/amd64 -q -t "$tag" "$src" >&2
  rm -rf "$src"
fi
echo "$tag"
