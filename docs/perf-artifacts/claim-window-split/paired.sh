#!/usr/bin/env bash
# Usage: paired.sh <base-bin> <new-bin> <outdir> <pairs>
#
# Runs the throughput cell at 1 shard, alternating the order each pair. The
# rate is the middle half of the completions over the measured window. That
# is the headline of e2e_bench, computed from its raw rows, so a run that the
# in-flight gate withholds still yields a rate. The mean in-flight count and
# the latency by Little's law are printed beside it (issue #2680).
set -u
base=$1; new=$2; out=$3; pairs=$4
mkdir -p "$out"
run() {
  label=$1; bin=$2
  HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@127.0.0.1:55432/postgres \
  HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
    "$bin" --bench > "$out/$label.md" 2>"$out/$label.err"
  window=$(grep '`measured_window_secs`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$5); print $5}')
  done_n=$(grep '`completions`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$5); print $5}')
  inflight=$(grep -o 'against a target of 32: [0-9.]*' "$out/$label.md" | grep -o '[0-9.]*$')
  sound=$(grep '`workflows_per_sec`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$6); print $6}')
  awk -v l="$label" -v w="$window" -v c="$done_n" -v f="$inflight" -v s="$sound" 'BEGIN {
    r = (c / 2) / w; printf "%s rate=%.2f inflight=%s latency_ms=%.0f gate=%s\n", l, r, f, 1000 * f / r, s }' \
    | tee -a "$out/paired-runs.txt"
}
for i in $(seq 1 "$pairs"); do
  if [ $((i % 2)) -eq 1 ]; then run base_$i "$base"; run new_$i "$new"; else run new_$i "$new"; run base_$i "$base"; fi
done
