#!/usr/bin/env bash
# Reproduction harness for the workflow-history-ceiling scanner query fix in
# `timeout::workflow_history_ceiling_query()` (issue #493 / Ledger perf pass),
# documented in `docs/performance-history-ceiling.md`.
#
# Unlike `concurrency_key_claim_perf_repro.sh` / `queue_pause_claim_perf_repro.sh`,
# this script does NOT toggle git state: the evidence-capture test
# (`history_ceiling_claim_tests::zz_capture_history_ceiling_claim_evidence`)
# carries the exact pre-fix query text as a hardcoded `BEFORE_SQL` constant
# and runs both the "before" and "after" forms against the SAME seeded
# fixture in ONE test invocation -- so there is nothing to stash or check
# out, and no risk of the "before" half silently drifting from what actually
# shipped.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/history_ceiling_claim_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/history_ceiling_claim_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL`, when set, is treated as an ADMIN URL, exactly
# as `claim_bench_support.rs` treats it elsewhere in this crate: the harness
# creates, migrates, seeds, measures, and drops a fresh uniquely-named
# database per run. When unset, `claim_bench_support::db::setup_bench_db`
# falls back to a testcontainer automatically. If neither an external
# database nor a Docker daemon is reachable, the capture test SKIPs loudly
# instead of producing artifacts.
#
# Writes into `docs/perf-artifacts/history-ceiling-scanner/`:
#   {before,after}-history-ceiling.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
#     `workflow_history_ceiling_query()`'s pre-fix and post-fix text, against
#     a 100,000-execution / 3,000-RUNNING / ~4,000,000-event production-shaped
#     fixture, ceiling=5,000.
#   {before,after}-pg_stat_statements.txt
#     A `pg_stat_statements` snapshot after each form's execution (see
#     "Known limitation" in the doc page -- this has come back empty in every
#     observed run).
#   {before,after}-result-rows.txt
#     The sorted `(id, event_count)` result set each form returned --
#     byte-identical between the two, which is this fix's equivalence proof.
#
# Preconditions: a Rust toolchain that can build this crate, and either Docker
# (for the harness's own testcontainer fallback) or a reachable Postgres named
# by `HARVEST_TEST_DATABASE_URL`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="$REPO_ROOT/docs/perf-artifacts/history-ceiling-scanner"
TEST_FILTER="history_ceiling_claim_tests::zz_capture_history_ceiling_claim_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

echo "== capturing before/after evidence via ${TEST_FILTER} =="
cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --nocapture "$TEST_FILTER" 2>&1 | tee /tmp/history_ceiling_claim_capture.log

if ! grep -q "^equivalence confirmed:" /tmp/history_ceiling_claim_capture.log; then
  echo "FATAL: the capture run did not report equivalence -- either it \
skipped (no database reachable) or the equivalence assertion inside the \
test itself failed. Check /tmp/history_ceiling_claim_capture.log." >&2
  exit 1
fi

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
