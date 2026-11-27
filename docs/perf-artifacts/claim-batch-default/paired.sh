#!/usr/bin/env bash
# Usage: paired.sh <e2e_bench binary> <outdir> <rounds> <in flight> <arm> [<arm> ...]
#
# Runs the throughput cell at 1 shard with one binary, one run per arm per
# round. Each round rotates the order of the arms. An arm is one of:
#   lean     one row per claim, the lean statement first
#   bN       a batch cap of N rows (HARVEST_BENCH_CLAIM_BATCH=N)
#   bNoff    a batch cap of N rows with the lean statement off
# The rate is the middle half of the completions over the measured window,
# as in ../claim-window-split/paired.sh. A run that the in-flight gate
# withholds still yields a rate.
set -u
bin=$1; out=$2; rounds=$3; inflight_target=$4; shift 4
arms=("$@"); n=${#arms[@]}
mkdir -p "$out"
run() {
  label=$1; shift
  env HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@127.0.0.1:55432/postgres \
    HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
    HARVEST_BENCH_INFLIGHT="$inflight_target" "$@" "$bin" --bench > "$out/$label.md" 2>"$out/$label.err"
  window=$(grep '`measured_window_secs`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$5); print $5}')
  done_n=$(grep '`completions`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$5); print $5}')
  inflight=$(grep -o 'against a target of [0-9]*: [0-9.]*' "$out/$label.md" | grep -o '[0-9.]*$')
  sound=$(grep '`workflows_per_sec`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$6); print $6}')
  awk -v l="$label" -v w="$window" -v c="$done_n" -v f="$inflight" -v s="$sound" 'BEGIN {
    r = (c / 2) / w; printf "%s rate=%.2f inflight=%s latency_ms=%.0f gate=%s\n", l, r, f, 1000 * f / r, s }' \
    | tee -a "$out/paired-runs.txt"
}
arm() {
  case $1 in
    lean)   run "lean_$2" HARVEST_BENCH_CLAIM_BATCH=1 ;;
    b*off)  k=${1#b}; k=${k%off}; run "$1_$2" HARVEST_BENCH_CLAIM_BATCH="$k" HARVEST_BENCH_CLAIM_LEAN=0 ;;
    b*)     run "$1_$2" HARVEST_BENCH_CLAIM_BATCH="${1#b}" ;;
    *)      echo "unknown arm: $1" >&2; exit 2 ;;
  esac
}
for i in $(seq 1 "$rounds"); do
  for k in $(seq 0 $((n - 1))); do arm "${arms[$(( (k + i - 1) % n ))]}" "$i"; done
done
