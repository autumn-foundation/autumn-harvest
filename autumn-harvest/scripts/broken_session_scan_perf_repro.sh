#!/usr/bin/env bash
# Reproduction harness for the broken-session scan index fix (Ledger perf
# pass, issue #2069), documented in docs/performance-broken-session-scan.md.
#
# The capture test seeds a throwaway database at three sizes, runs
# `sessions::enforce_broken_sessions` once per size, and writes
# pg_stat_statements, EXPLAIN, index statistics and a state dump to
# docs/perf-artifacts/broken-session-scan/<label>-*.
#
# "before" runs with the new migration moved aside. "after" runs with it in
# place. The script restores the migration on exit.
#
# Usage:
#   HARVEST_TEST_DATABASE_URL=postgres://postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/broken_session_scan_perf_repro.sh
#
# The role must be a superuser, and the server must preload
# pg_stat_statements. Without HARVEST_TEST_DATABASE_URL the test starts a
# Postgres 16 container.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
mig="$(ls -d "$root"/autumn-harvest/migrations/*_harvest_task_queue_session_active_index)"
aside="$(mktemp -d)"
restore() { [ -d "$aside/$(basename "$mig")" ] && mv "$aside/$(basename "$mig")" "$mig"; rm -rf "$aside"; }
trap restore EXIT

run() {
  PERF_LABEL="$1" cargo test -p autumn-harvest --all-features --test integration -- \
    --ignored zz_capture_broken_session_scan_perf_evidence --nocapture
}

mv "$mig" "$aside/"
run before
mv "$aside/$(basename "$mig")" "$mig"
run after

out="$root/docs/perf-artifacts/broken-session-scan"
for n in 100 400 1600; do
  cmp "$out/before-state-n$n.txt" "$out/after-state-n$n.txt"
  echo "n=$n: state identical"
done
cat "$out/before-sweep.txt" "$out/after-sweep.txt"
