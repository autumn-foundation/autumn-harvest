#!/usr/bin/env bash
# Sweep driver for the claim-concurrency measurement.
#
# Each round runs Temporal at each depth, then the harvest postgres arm once
# per claim cap. A round spreads drift over every cell.
#
# Required environment:
#   CC_BIN   the patched assay #14 harvest binary (see README.md)
# Optional:
#   CC_ROUNDS  rounds, default 3
#   CC_CLAIMS  claim caps, default 1,2,4
#   CC_DEPTHS  default 250,500,1000,2000
#   CC_OUT     output directory, default results/raw next to this file
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TEMPORAL_DIR="$HERE/../../assays/apparatus/0011-harvest-vs-temporal"
ROUNDS="${CC_ROUNDS:-3}"
CLAIMS="${CC_CLAIMS:-1,2,4}"
DEPTHS="${CC_DEPTHS:-250,500,1000,2000}"
OUT="${CC_OUT:-$HERE/results/raw}"
BIN="${CC_BIN:?set CC_BIN to the patched harvest binary}"
CONTAINER="${ASSAY11_CONTAINER:-temporal-bench}"

[ -x "$TEMPORAL_DIR/assay11" ] || {
  echo "build assay #11's Temporal arm first: (cd $TEMPORAL_DIR && go build -o assay11 .)" >&2
  exit 1
}
if [ -d "$OUT" ] && [ -n "$(ls -A "$OUT")" ]; then
  echo "$OUT is not empty. Move it away before a new sweep." >&2
  exit 1
fi
mkdir -p "$OUT"

for round in $(seq 0 $((ROUNDS - 1))); do
  echo "==> round $round, $(date -u +%FT%TZ)"
  for depth in ${DEPTHS//,/ }; do
    ASSAY11_REPS=1 ASSAY11_WORKFLOWS="$depth" ASSAY11_CAP_SECS=900 \
      "$TEMPORAL_DIR/run.sh" > "$OUT/r${round}-temporal-d${depth}.txt" 2>&1 ||
      echo "run failed: exit $?" >> "$OUT/r${round}-temporal-d${depth}.txt"
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    if ! left="$(docker ps -aq --filter "name=^${CONTAINER}\$")" || [ -n "$left" ]; then
      echo "cannot confirm that $CONTAINER is gone; stopping the sweep" >&2
      exit 1
    fi
    grep '^rep ' "$OUT/r${round}-temporal-d${depth}.txt" || true
  done
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
