#!/usr/bin/env bash
# Usage: paired3.sh <base-bin> <new-bin> <outdir> <rounds> [VAR=value ...]
#
# Runs the throughput cell at 1 shard in three arms per round:
#   base  the base binary
#   off   the new binary with HARVEST_BENCH_CLAIM_LEAN=0 (the window form first)
#   on    the new binary with its default (the lean form first)
# The order of the arms rotates over six permutations, so no arm keeps one
# slot of a round. The rate is the middle half of the completions over the
# measured window, as in ../claim-window-split/paired.sh. A run that the
# in-flight gate withholds still yields a rate.
set -u
base=$1; new=$2; out=$3; rounds=$4; shift 4
mkdir -p "$out"
EXTRA=("$@")
run() {
  label=$1; bin=$2; lean=$3
  env HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@127.0.0.1:55432/postgres \
    HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 "${EXTRA[@]}" \
    ${lean:+HARVEST_BENCH_CLAIM_LEAN=$lean} "$bin" --bench > "$out/$label.md" 2>"$out/$label.err"
  window=$(grep '`measured_window_secs`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$5); print $5}')
  done_n=$(grep '`completions`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$5); print $5}')
  inflight=$(grep -o 'against a target of [0-9]*: [0-9.]*' "$out/$label.md" | grep -o '[0-9.]*$')
  sound=$(grep '`workflows_per_sec`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$6); print $6}')
  awk -v l="$label" -v w="$window" -v c="$done_n" -v f="$inflight" -v s="$sound" 'BEGIN {
    r = (c / 2) / w; printf "%s rate=%.2f inflight=%s latency_ms=%.0f gate=%s\n", l, r, f, 1000 * f / r, s }' \
    | tee -a "$out/paired-runs.txt"
}
arm() { case $1 in
  base) run "base_$2" "$base" "" ;;
  off)  run "off_$2" "$new" 0 ;;
  on)   run "on_$2" "$new" "" ;;
esac; }
orders=("base off on" "on base off" "off on base" "on off base" "base on off" "off base on")
for i in $(seq 1 "$rounds"); do
  for a in ${orders[$(( (i - 1) % 6 ))]}; do arm "$a" "$i"; done
done
