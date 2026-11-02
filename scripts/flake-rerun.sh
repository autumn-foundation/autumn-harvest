#!/usr/bin/env bash
# Same-commit rerun protocol for one test binary (Semaphore, CI health).
# usage: flake-rerun.sh <test-binary> <test-name> <N> [cpu-spinners]
# Run from the crate directory with HARVEST_TEST_DATABASE_URL set.
# Prints the failure count, and the Postgres event order of each failure.
bin=$1; t=$2; n=$3; sp=${4:-0}; pids=()
for ((i=0;i<sp;i++)); do (while :; do :; done) & pids+=($!); done
fail=0
for ((i=1;i<=n;i++)); do
  out=$($bin "$t" --exact 2>&1) || { fail=$((fail+1)); echo "run $i FAIL: $(echo "$out" | grep -E "postgres:.*(Activity|Signal)" | tr -s " " | tr "\n" "|")"; }
done
for p in "${pids[@]}"; do kill $p 2>/dev/null; done
echo "RESULT $t: $fail/$n failed (spinners=$sp)"
