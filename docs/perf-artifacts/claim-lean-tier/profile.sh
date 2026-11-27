#!/usr/bin/env bash
# Usage: profile.sh <label> <e2e_bench binary> <outdir> [VAR=value ...]
#
# Runs the throughput cell at 1 shard and samples the whole box for 10 s,
# from 6 s after the start: 999 Hz, cpu-clock, frame-pointer call graphs.
# Writes the share of all samples of each executor phase to
# <outdir>/<label>-phases.txt. PERF names the perf binary.
set -u
label=$1; bin=$2; out=$3; shift 3
perf=${PERF:-perf}
mkdir -p "$out"
env HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@127.0.0.1:55432/postgres \
  HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 "$@" \
  "$bin" --bench > "$out/$label.md" 2>"$out/$label.err" &
bench=$!
sleep 6
"$perf" record -a -g -F 999 -e cpu-clock -o "$out/$label.data" -- sleep 10 2>"$out/$label-record.log"
wait "$bench"
"$perf" report -i "$out/$label.data" --children --sort symbol --stdio 2>/dev/null \
  | grep -E '\] (standard_ExecutorStart|standard_ExecutorRun|standard_ExecutorEnd|ExecInitNode|ExecInitNestLoop|ExecInitAgg|ExecInitCteScan|ExecSetParamPlan|EvalPlanQualBegin|EvalPlanQualEnd|GetCachedPlan|pg_plan_queries|PostgresMain) ' \
  | awk '{print $1, $4}' > "$out/$label-phases.raw"
"$perf" report -i "$out/$label.data" --no-children --sort comm --stdio 2>/dev/null \
  | grep -E '^ +[0-9.]+% +(postgres|tokio-rt-worker|swapper)' | awk '{print $1, $2}' > "$out/$label-comm.raw"
rate=$(awk -F'|' '/`completions`/ {gsub(/ /,"",$5); c=$5} /`measured_window_secs`/ {gsub(/ /,"",$5); w=$5} END {printf "%.2f", (c / 2) / w}' "$out/$label.md")
{ echo "== $label rate=$rate"; cat "$out/$label-phases.raw" "$out/$label-comm.raw"; } > "$out/$label-phases.txt"
rm -f "$out/$label-phases.raw" "$out/$label-comm.raw"
cat "$out/$label-phases.txt"
