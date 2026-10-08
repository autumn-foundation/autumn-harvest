#!/usr/bin/env bash
# Sweep driver for assay ledger #14. It runs the registered matrix in the
# registered order and keeps every run's verbatim output.
#
# Each round runs every cell once: Temporal at each depth, then each harvest
# tree. A round spreads drift over every cell, not over one tree.
#
# Required environment:
#   ASSAY14_BINS   tree=binary pairs, comma separated, in run order. Example:
#                  0aeb887=/b/base,513b7aa=/b/fix,9f444b7=/b/trunk
# Optional:
#   ASSAY14_ROUNDS   rounds, default 3
#   ASSAY14_DEPTHS   default 250,500,1000,2000
#   ASSAY14_OUT      output directory, default results/raw next to this file
#   ASSAY11_TEMPORAL_IMAGE  passed through to assay #11's runner
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TEMPORAL_DIR="$HERE/../0011-harvest-vs-temporal"
ROUNDS="${ASSAY14_ROUNDS:-3}"
DEPTHS="${ASSAY14_DEPTHS:-250,500,1000,2000}"
OUT="${ASSAY14_OUT:-$HERE/results/raw}"
BINS="${ASSAY14_BINS:?set ASSAY14_BINS to tree=binary pairs}"

[ -x "$TEMPORAL_DIR/assay11" ] || {
  echo "build assay #11's Temporal arm first: (cd $TEMPORAL_DIR && go build -o assay11 .)" >&2
  exit 1
}
# Start from an empty directory, so the grader never mixes two sweeps.
if [ -d "$OUT" ] && [ -n "$(ls -A "$OUT")" ]; then
  echo "$OUT is not empty. Move it away before a new sweep." >&2
  exit 1
fi
mkdir -p "$OUT"

for round in $(seq 0 $((ROUNDS - 1))); do
  echo "==> round $round, $(date -u +%FT%TZ)"
  for depth in ${DEPTHS//,/ }; do
    # A failed run stays in the record and leaves its cell incomplete. It does
    # not stop the sweep.
    ASSAY11_REPS=1 ASSAY11_WORKFLOWS="$depth" "$TEMPORAL_DIR/run.sh" \
      > "$OUT/r${round}-temporal-d${depth}.txt" 2>&1 ||
      echo "run failed: exit $?" >> "$OUT/r${round}-temporal-d${depth}.txt"
    grep '^rep ' "$OUT/r${round}-temporal-d${depth}.txt" || true
  done
  for pair in ${BINS//,/ }; do
    tree="${pair%%=*}"
    bin="${pair#*=}"
    ASSAY14_TREE="$tree" ASSAY14_ROUND="$round" ASSAY14_DEPTHS="$DEPTHS" "$bin" \
      > "$OUT/r${round}-${tree}.txt" 2>&1 ||
      echo "run failed: exit $?" >> "$OUT/r${round}-${tree}.txt"
    grep '^cell ' "$OUT/r${round}-${tree}.txt" | cut -d' ' -f2-7 || true
  done
done
echo "==> done, $(date -u +%FT%TZ)"
python3 "$HERE/grade.py" "$OUT"
