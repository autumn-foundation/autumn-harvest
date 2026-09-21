#!/usr/bin/env bash
# Reproduction harness for quota_reconcile::CANDIDATE_SQL's residual
# workflow_name filter (issue #1226), documented in
# `docs/performance-quota-reconcile-candidate-scan.md`.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL`, when set, is treated as an ADMIN URL, exactly
# as `claim_bench_support.rs` treats it elsewhere in this crate: the harness
# creates, migrates, seeds, and measures against a fresh uniquely-named
# database per run. It does NOT drop that database when the run ends --
# `setup_bench_db` documents this: an idle lease connection is held open
# instead, so the database stays visible to `pg_stat_activity` and defends
# itself against a concurrent run's stale-database sweep. A FUTURE run's own
# `setup_bench_db` call sweeps and drops databases stale from a prior run,
# not this run's own teardown. On a one-off local run against a real Postgres
# server (not the testcontainer fallback), this leaves a roughly 600,000-row
# `harvest_claim_bench_*` database behind until the next run of this or any
# other suite against the same server; `DROP DATABASE` it by hand if that
# matters. When unset, `claim_bench_support::db::setup_bench_db` falls back
# to a testcontainer automatically, which the daemon reclaims entirely on
# its own.
#
# Writes into `docs/perf-artifacts/quota-reconcile-candidate-scan/`:
#   noise-{20000,100000,500000}.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` for the unmodified
#     candidate scan, first tick, at each noise-population size.
#   alternative-index-explain.txt
#     The SAME unmodified query with the migration comment's rejected
#     (workflow_name, id) partial index also present, at the largest size.
#   pg_stat_statements.txt
#     A pg_stat_statements snapshot after driving a real, complete
#     reconcile_quota_keys_from() pass at the largest size.
#   fixture-summary.txt
#     Per-size row counts and the full-pass tick/backfill totals.
#
# Preconditions: a Rust toolchain that can build this crate, and either Docker
# (for the harness's own testcontainer fallback) or a reachable Postgres named
# by `HARVEST_TEST_DATABASE_URL` with `pg_stat_statements` in
# `shared_preload_libraries`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="$REPO_ROOT/docs/perf-artifacts/quota-reconcile-candidate-scan"
TEST_FILTER="quota_reconcile_candidate_scan_perf_tests::zz_capture_quota_reconcile_candidate_scan_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

LOG="/tmp/quota_reconcile_candidate_scan_capture.log"
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
