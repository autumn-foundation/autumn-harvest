#!/usr/bin/env bash
# Reproduction harness for the parent-close-cascade unfinished-update-handler
# check N+1 fix (Ledger perf pass), documented in
# `docs/performance-parent-close-cascade-unfinished-handlers.md`.
#
# Before this fix, every caller of `apply_parent_close_cascade`'s
# `closed_children` (five sites in `worker.rs`, three more in `timeout.rs`,
# plus the admission-path `deferred_checks` shape in `execution.rs` and
# `completion_trigger.rs`) looped over its collected `(exec_id,
# workflow_name)` pairs and called `check_and_report_unfinished_handlers`
# once per pair -- one `SELECT * FROM harvest_events WHERE workflow_exec_id
# = $1` per pair, issued after the writing transaction committed. This
# script drives BOTH the old per-pair loop (reproduced in the test itself,
# calling the still-present single-execution function) and the new
# `check_and_report_unfinished_handlers_batch` against the SAME seeded
# fixture, and captures a `pg_stat_statements` snapshot for each.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/parent_close_cascade_unfinished_handlers_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/parent_close_cascade_unfinished_handlers_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL`, when set, is treated as an ADMIN URL, exactly
# as `claim_bench_support.rs` treats it elsewhere in this crate:
# `claim_bench_support::db::setup_bench_db` creates, migrates, and measures
# against a fresh uniquely-named database, and holds an idle lease
# connection open rather than dropping it (see that function's own doc
# comment). When unset, it falls back to a testcontainer automatically,
# which the daemon reclaims entirely on its own.
#
# Writes into `docs/perf-artifacts/parent-close-cascade-unfinished-handlers/`:
#   before.pg_stat_statements.txt / before.result-rows.txt
#     The looped per-child `check_and_report_unfinished_handlers` strategy:
#     `pg_stat_statements` for the per-exec-id `harvest_events` SELECT, and
#     every reported (workflow_name, count) pair.
#   after.pg_stat_statements.txt / after.result-rows.txt
#     The real, shipped `check_and_report_unfinished_handlers_batch`: one
#     `eq_any` `harvest_events` SELECT, and the same reported pairs.
#
# Preconditions: a Rust toolchain that can build this crate, and either
# Docker (for the harness's own testcontainer fallback) or a reachable
# Postgres named by `HARVEST_TEST_DATABASE_URL` with `pg_stat_statements` in
# `shared_preload_libraries`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="$REPO_ROOT/docs/perf-artifacts/parent-close-cascade-unfinished-handlers"
TEST_FILTER="parent_close_cascade_unfinished_handlers_perf::zz_capture_parent_close_cascade_unfinished_handlers_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

LOG="/tmp/parent_close_cascade_unfinished_handlers_capture.log"
echo "== capturing via ${TEST_FILTER} =="
cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --nocapture "$TEST_FILTER" 2>&1 | tee "$LOG"

if ! grep -q "^== done. Artifacts in " "$LOG"; then
  echo "FATAL: the test run did not report capture completion -- either the \
capture test failed, or it SKIPped for lack of a reachable database. Check \
the log at ${LOG}." >&2
  exit 1
fi

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
