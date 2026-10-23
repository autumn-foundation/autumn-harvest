#!/usr/bin/env bash
# Sweep driver for the owner decider (issue #2255).
#
# Each round runs the harvest postgres arm of assay #14 once per mode and
# claim cap. Legacy is owner mode off. Owner is owner mode on, so the owner
# decider takes the workflow tasks. A round alternates the modes, so drift
# spreads over both.
#
# Required environment:
#   ODC_BIN     the patched assay #14 harvest binary (see README.md)
# Optional:
#   ODC_ROUNDS  rounds, default 3
#   ODC_CLAIMS  claim caps, default 1,2,4
#   ODC_DEPTHS  default 500,1000,2000
#   ODC_OUT     output directory, default results/raw next to this file
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROUNDS="${ODC_ROUNDS:-3}"
CLAIMS="${ODC_CLAIMS:-1,2,4}"
DEPTHS="${ODC_DEPTHS:-500,1000,2000}"
OUT="${ODC_OUT:-$HERE/results/raw}"
BIN="${ODC_BIN:?set ODC_BIN to the patched harvest binary}"

if [ -d "$OUT" ] && [ -n "$(ls -A "$OUT")" ]; then
  echo "$OUT is not empty. Move it away before a new sweep." >&2
  exit 1
fi
mkdir -p "$OUT"

for round in $(seq 0 $((ROUNDS - 1))); do
  echo "==> round $round, $(date -u +%FT%TZ)"
  # Odd rounds run owner mode first.
  if [ $((round % 2)) -eq 0 ]; then modes="legacy owner"; else modes="owner legacy"; fi
  for claims in ${CLAIMS//,/ }; do
    for mode in $modes; do
      owner=0
      [ "$mode" = owner ] && owner=1
      ASSAY14_OWNER_MODE="$owner" ASSAY14_MAX_CONCURRENT_CLAIMS="$claims" \
        ASSAY14_TREE="${mode}-claims${claims}" ASSAY14_ROUND="$round" \
        ASSAY14_DEPTHS="$DEPTHS" ASSAY14_ARMS=postgres "$BIN" \
        > "$OUT/r${round}-${mode}-claims${claims}.txt" 2>&1 ||
        echo "run failed: exit $?" >> "$OUT/r${round}-${mode}-claims${claims}.txt"
      grep '^cell ' "$OUT/r${round}-${mode}-claims${claims}.txt" | cut -d' ' -f2-7 || true
    done
  done
done
echo "==> done, $(date -u +%FT%TZ)"
python3 "$HERE/grade.py" "$OUT"
