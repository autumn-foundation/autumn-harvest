#!/usr/bin/env bash
# Reproduction harness for the schedule_to_close_at claim-predicate
# measurement (issue #378 / Ledger perf pass) documented in
# `docs/performance-schedule-to-close.md`.
#
# Like `capability_labels_claim_perf_repro.sh`, this does NOT toggle a code
# fix: `queue::claim_task_query()` is unmodified end to end -- this predicate
# is a plain inline column test (`schedule_to_close_at IS NULL OR
# schedule_to_close_at > NOW()`), not a subquery, so there is no query-shape
# fix to try. It runs the `#[ignore]`d evidence-capture test
# `claim_budget_tests::zz_capture_schedule_to_close_claim_evidence` ONCE. That
# single test drives the real `claim_task_query()`/`claim_task()` at TWO
# seeded *data* states of `harvest_task_queue.schedule_to_close_at` -- `NULL`
# (today's default) vs. every row carrying a deadline 100 years in the future
# (far enough out that it never elapses no matter how long the capture takes
# to run, and excludes nothing) -- and captures both, so there is nothing
# else for this script to toggle.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL`, when set, is treated as an ADMIN URL, exactly
# as `claim_bench_support.rs` treats it elsewhere in this crate: the harness
# creates, migrates, seeds, measures, and drops a fresh uniquely-named
# database per test run. When it is UNSET, `setup_bench_db()` falls back to a
# testcontainer automatically -- this script does not gate on the variable
# being present, so that fallback is reachable. If neither an external
# database nor a Docker daemon is reachable, the capture test SKIPs loudly
# instead of producing artifacts -- see `bench_db_or_skip()` in
# `claim_budget_tests.rs`.
#
# Writes into `docs/perf-artifacts/schedule-to-close-claim-predicate/`:
#   {no-schedule-to-close,schedule-to-close}-claim-backlog-{1000,10000,100000}.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
#     `claim_task_query()` at each published `BACKLOG_SWEEP` depth, at both
#     seeded states.
#   {no-schedule-to-close,schedule-to-close}-pg_stat_statements.txt
#     A `pg_stat_statements` snapshot after driving the REAL `claim_task()`
#     Rust function (not literal-substituted SQL) to drain the 10k-row
#     headline backlog, at both seeded states.
#   fixture-summary.txt
#     Seeded/claimable row counts per depth, plus the claimed-row count from
#     each stat-snapshot drain (the test asserts these are equal between the
#     two labels as a correctness sanity check).
#
# Preconditions: a Rust toolchain that can build this crate, and either Docker
# (for the harness's own testcontainer fallback) or a reachable Postgres named
# by `HARVEST_TEST_DATABASE_URL`.
#
# If `HARVEST_TEST_DATABASE_URL` points at an external Postgres instance
# (rather than relying on the testcontainer fallback), that instance must
# ALSO have `pg_stat_statements` in `shared_preload_libraries` (a
# server-level setting that requires a restart to change, so it cannot be
# provisioned by this script or by the harness at connection time) and the
# role in the URL must have permission to call
# `pg_stat_statements_reset(...)` (superuser, or an explicit GRANT). Neither
# is checked up front: a vanilla external Postgres that satisfies every
# other precondition above will still complete the `EXPLAIN` sweep and then
# fail partway through, once the capture reaches its `pg_stat_statements`
# snapshot. The testcontainer fallback (unset `HARVEST_TEST_DATABASE_URL`,
# Docker reachable) does not have this problem -- see the image/setup this
# crate's `db::setup_bench_db()` uses, which provisions the extension
# itself. Codex review on PR #1339 (P2) flagged this gap; the same gap
# exists in the sibling `capability_labels_claim_perf_repro.sh` and
# `concurrency_key_claim_perf_repro.sh` and is not fixed there by this
# change.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Must match the fixed path the Rust test itself writes to (derived from
# CARGO_MANIFEST_DIR at compile time, not overridable from here) -- this is
# display-only, used for the final `ls` below, not passed into the test.
OUT_DIR="$REPO_ROOT/docs/perf-artifacts/schedule-to-close-claim-predicate"
TEST_FILTER="claim_budget_tests::zz_capture_schedule_to_close_claim_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

LOG="/tmp/schedule_to_close_claim_capture.log"
echo "== capturing via ${TEST_FILTER} =="
cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --nocapture "$TEST_FILTER" 2>&1 | tee "$LOG"

if ! grep -q "^== capture complete: schedule-to-close evidence, artifacts in " "$LOG"; then
  echo "FATAL: the test run did not report capture completion -- either the \
capture test failed, or it SKIPped for lack of a reachable database. Check \
the log at ${LOG}." >&2
  exit 1
fi

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
