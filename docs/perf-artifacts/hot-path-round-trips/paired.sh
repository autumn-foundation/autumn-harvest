#!/usr/bin/env bash
# Usage: paired.sh <base-bin> <new-bin> <outdir> <pairs>
set -u
base=$1; new=$2; out=$3; pairs=$4
mkdir -p "$out"
run() {
  label=$1; bin=$2
  HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@127.0.0.1:55432/postgres \
  HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
    "$bin" --bench > "$out/$label.md" 2>"$out/$label.err"
  wps=$(grep '`workflows_per_sec`' "$out/$label.md" | awk -F'|' '{gsub(/ /,"",$5); print $5}')
  echo "$label workflows_per_sec=$wps" | tee -a "$out/paired-runs.txt"
}
for i in $(seq 1 "$pairs"); do
  if [ $((i % 2)) -eq 1 ]; then run base_$i "$base"; run new_$i "$new"; else run new_$i "$new"; run base_$i "$base"; fi
done
