#!/usr/bin/env bash
# Sweep driver for the decision-owner baseline (issue #2248).
#
# Each round runs the harvest postgres arm of assay #14 once per claim cap.
# A round spreads drift over every cell.
#
# Required environment:
#   DOB_BIN     the patched assay #14 harvest binary (see README.md)
# Optional:
#   DOB_ROUNDS  rounds, default 3
#   DOB_CLAIMS  claim caps, default 1,2,4
#   DOB_DEPTHS  default 250,500,1000,2000
#   DOB_OUT     output directory, default results/raw next to this file
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROUNDS="${DOB_ROUNDS:-3}"
CLAIMS="${DOB_CLAIMS:-1,2,4}"
DEPTHS="${DOB_DEPTHS:-250,500,1000,2000}"
OUT="${DOB_OUT:-$HERE/results/raw}"
BIN="${DOB_BIN:?set DOB_BIN to the patched harvest binary}"

if [ -d "$OUT" ] && [ -n "$(ls -A "$OUT")" ]; then
  echo "$OUT is not empty. Move it away before a new sweep." >&2
  exit 1
fi
mkdir -p "$OUT"

for round in $(seq 0 $((ROUNDS - 1))); do
  echo "==> round $round, $(date -u +%FT%TZ)"
  for claims in ${CLAIMS//,/ }; do
    ASSAY14_MAX_CONCURRENT_CLAIMS="$claims" ASSAY14_TREE="claims${claims}" \
      ASSAY14_ROUND="$round" ASSAY14_DEPTHS="$DEPTHS" ASSAY14_ARMS=postgres "$BIN" \
      > "$OUT/r${round}-claims${claims}.txt" 2>&1 ||
      echo "run failed: exit $?" >> "$OUT/r${round}-claims${claims}.txt"
    grep '^cell ' "$OUT/r${round}-claims${claims}.txt" | cut -d' ' -f2-7 || true
  done
done
echo "==> done, $(date -u +%FT%TZ)"
python3 "$HERE/grade.py" "$OUT"
