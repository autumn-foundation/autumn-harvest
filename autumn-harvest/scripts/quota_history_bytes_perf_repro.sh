#!/usr/bin/env bash
# Reproduction harness for the quota `history_bytes` admission-check
# measurement (issue #946 AC7 / Ledger perf pass) documented in
# `docs/performance-quota-history-bytes.md`.
#
# Like `capability_labels_claim_perf_repro.sh`, this does NOT toggle a code
# fix: `quota::quota_usage_query()` is unmodified end to end -- measurement
# found no query-shape rewrite that helps (a `LATERAL` rewrite was tried and
# measured worse; see the doc page). It runs the `#[ignore]`d evidence-capture
# test `quota_history_bytes_perf_tests::zz_capture_quota_history_bytes_evidence`
# ONCE. That single test seeds one target tenant's fixed 1,000-execution /
# 178,000-event footprint against THREE background-table sizes
# (`NOISE_SWEEP = [3, 15, 100]`, ~205k / 313k / 1.08M total `harvest_events`
# rows) and captures all three, plus one negative-result `LATERAL` variant at
# the smallest size, so there is nothing else for this script to toggle.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/quota_history_bytes_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/quota_history_bytes_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL`, when set, is treated as an ADMIN URL, exactly
# as `claim_bench_support.rs` treats it elsewhere in this crate: the harness
# creates, migrates, seeds, measures, and drops a fresh uniquely-named
# database per test run. When it is UNSET, `setup_bench_db()` falls back to a
# testcontainer automatically -- this script does not gate on the variable
# being present, so that fallback is reachable. If neither an external
# database nor a Docker daemon is reachable, the capture test SKIPs loudly
# instead of producing artifacts.
#
# Writes into `docs/perf-artifacts/quota-history-bytes-admission/`:
#   noise_mult-{3,15,100}.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` for the real
#     `quota::quota_usage_query()` text at each background-table size, with
#     the target tenant's own 1,000-execution footprint held fixed.
#   lateral-variant-negative-result.explain.txt
#     The rejected `LATERAL` rewrite's plan at the smallest size.
#   pg_stat_statements.txt
#     A `pg_stat_statements` snapshot after driving the REAL
#     `quota::load_quota_usage()` Rust function (not literal-substituted SQL)
#     20 times at the largest fixture.
#   fixture-summary.txt
#     The fixture's exact `(active_executions, history_bytes, dead_letters)`
#     at each sweep point -- the test asserts these are identical across all
#     three sizes as a correctness sanity check.
#
# Preconditions: a Rust toolchain that can build this crate, and either Docker
# (for the harness's own testcontainer fallback) or a reachable Postgres named
# by `HARVEST_TEST_DATABASE_URL`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Must match the fixed path the Rust test itself writes to (derived from
# CARGO_MANIFEST_DIR at compile time, not overridable from here) -- this is
# display-only, used for the final `ls` below, not passed into the test.
OUT_DIR="$REPO_ROOT/docs/perf-artifacts/quota-history-bytes-admission"
TEST_FILTER="quota_history_bytes_perf_tests::zz_capture_quota_history_bytes_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

LOG="/tmp/quota_history_bytes_capture.log"
echo "== capturing via ${TEST_FILTER} =="
cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --nocapture "$TEST_FILTER" 2>&1 | tee "$LOG"

if ! grep -q "^== capture complete: artifacts in " "$LOG"; then
  echo "FATAL: the test run did not report capture completion -- either the \
capture test failed, or it SKIPped for lack of a reachable database. Check \
the log at ${LOG}." >&2
  exit 1
fi

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
