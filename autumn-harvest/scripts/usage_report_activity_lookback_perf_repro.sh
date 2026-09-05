#!/usr/bin/env bash
# Reproduction harness for the usage-report activity-lookback index (issue
# #596 / Ledger perf pass), documented in
# `docs/performance-usage-report-activity-lookback.md`.
#
# The evidence-capture test
# (`usage_report_activity_lookback_tests::zz_capture_usage_report_activity_lookback_evidence`)
# drops the candidate index unconditionally before the "before" capture (so
# it reproduces the pre-fix baseline whether or not
# `20260905181020_harvest_usage_activity_lookback_index` has already run
# against the target database), creates it, and captures the "after" form --
# same query text both times, only the schema changes -- against the SAME
# seeded fixture in ONE test invocation.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL`, when set, is treated as an ADMIN URL, exactly
# as `claim_bench_support.rs` treats it elsewhere in this crate: the harness
# creates, migrates, seeds, measures, and drops a fresh uniquely-named
# database per run. When unset, `claim_bench_support::db::setup_bench_db`
# falls back to a testcontainer automatically. If neither an external
# database nor a Docker daemon is reachable, the capture test SKIPs loudly
# instead of producing artifacts.
#
# Writes into `docs/perf-artifacts/usage-report-activity-lookback/`:
#   {before,after}.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
#     `usage::usage_sql()`, against a 40,000-execution / ~450,000-event
#     production-shaped fixture with a skewed 1% "batch" tail
#     (50-300 activities per execution).
#   {before,after}.pg_stat_statements.txt
#     A `pg_stat_statements` snapshot after each form's execution.
#   {before,after}.result-rows.txt
#     The sorted grouped-usage result set each form returned --
#     byte-identical between the two, which is this fix's equivalence proof.
#
# Preconditions: a Rust toolchain that can build this crate, and either Docker
# (for the harness's own testcontainer fallback) or a reachable Postgres named
# by `HARVEST_TEST_DATABASE_URL` with `pg_stat_statements` in
# `shared_preload_libraries` (the harness creates the extension itself, but
# the module must already be preloaded at postmaster start -- `CREATE
# EXTENSION` alone cannot retroactively enable it).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="$REPO_ROOT/docs/perf-artifacts/usage-report-activity-lookback"
TEST_FILTER="usage_report_activity_lookback_tests::zz_capture_usage_report_activity_lookback_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

echo "== capturing before/after evidence via ${TEST_FILTER} =="
cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --nocapture "$TEST_FILTER" 2>&1 | tee /tmp/usage_report_activity_lookback_capture.log

if ! grep -q "^equivalence confirmed:" /tmp/usage_report_activity_lookback_capture.log; then
  echo "FATAL: the capture run did not report equivalence -- either it \
skipped (no database reachable) or the equivalence assertion inside the \
test itself failed. Check /tmp/usage_report_activity_lookback_capture.log." >&2
  exit 1
fi

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
