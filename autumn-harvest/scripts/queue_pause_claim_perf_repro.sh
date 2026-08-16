#!/usr/bin/env bash
# Reproduction harness for the queue-pause anti-join rewrite in
# `queue::claim_task_query()` (issue #619 / Ledger perf pass) documented in
# `docs/performance.md`.
#
# Runs the `#[ignore]`d evidence-capture test
# `claim_budget_tests::zz_capture_queue_pause_claim_evidence` TWICE against the
# SAME harness, fixture generator, and (where the harness falls back to a
# testcontainer) the same freshly-created database within one script
# invocation:
#
#   1. "after"  -- against the tree exactly as checked out (assumed to have
#                  the fix, since that is what this script ships alongside).
#   2. "before" -- against `queue.rs` with the fix temporarily removed via
#                  `git stash`, restored automatically on exit (success,
#                  failure, or interrupt).
#
# The capture test self-detects which shape it measured by inspecting
# `queue::claim_task_query()`'s own text at runtime, so there is no
# hand-maintained "old" copy of the query for either invocation to drift out
# of sync with -- both runs execute the real, compiled function.
#
# Usage:
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/queue_pause_claim_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL` is treated as an ADMIN URL, exactly as
# `claim_bench_support.rs` treats it elsewhere in this crate: the harness
# creates, migrates, seeds, measures, and drops a fresh uniquely-named
# database per test run. Unset it (or run without Docker reachable either) and
# the capture test SKIPs loudly instead of producing artifacts -- see
# `bench_db_or_skip()` in `claim_budget_tests.rs`.
#
# Writes into `docs/perf-artifacts/queue-pause-claim-anti-join/`:
#   {before,after}-claim-backlog-{1000,10000,100000}.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
#     `claim_task_query()` at each published `BACKLOG_SWEEP` depth, with one
#     of the four polled queues paused.
#   {before,after}-pg_stat_statements.txt
#     A `pg_stat_statements` snapshot after driving the REAL `claim_task()`
#     Rust function (not literal-substituted SQL) to drain a 10k-row headline
#     backlog.
#   {before,after}-fixture-summary.txt
#     Seeded/claimable row counts and the paused queue name, per depth.
#
# Only `queue.rs` is toggled -- the test files carrying this script's own
# assertions are left exactly as checked out on both runs, since the "before"
# capture only needs the production query text to revert, not the tests that
# assert its shape.
#
# Preconditions: a Rust toolchain that can build this crate, and either Docker
# (for the harness's own testcontainer fallback) or a reachable Postgres named
# by `HARVEST_TEST_DATABASE_URL`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

QUEUE_RS="autumn-harvest/src/queue.rs"
OUT_DIR="${1:-$REPO_ROOT/docs/perf-artifacts/queue-pause-claim-anti-join}"
TEST_FILTER="claim_budget_tests::zz_capture_queue_pause_claim_evidence"

: "${HARVEST_TEST_DATABASE_URL:?set HARVEST_TEST_DATABASE_URL to an admin connection string, e.g. postgres://postgres:postgres@localhost:5432/postgres (or ensure Docker is reachable for the testcontainer fallback)}"

capture() {
  local label="$1"
  echo "== capturing '${label}' via ${TEST_FILTER} =="
  cargo test -p autumn-harvest --features db,testing --test integration -- \
    --ignored --nocapture "$TEST_FILTER" 2>&1 | tee "/tmp/queue_pause_claim_capture_${label}.log"

  local produced
  produced="$(grep -c "^== capture complete: label=${label}," "/tmp/queue_pause_claim_capture_${label}.log" || true)"
  if [ "$produced" -lt 1 ]; then
    echo "FATAL: the test run did not report 'label=${label}' -- either \
queue::claim_task_query() was not in the expected shape for this half of the \
repro (see the git-stash step above/below), or the capture test itself \
failed/skipped. Check the log at /tmp/queue_pause_claim_capture_${label}.log." >&2
    exit 1
  fi
}

STASHED=0
restore() {
  if [ "$STASHED" -eq 1 ]; then
    echo "== restoring ${QUEUE_RS} (git stash pop) =="
    git stash pop
    STASHED=0
  fi
}
trap restore EXIT

echo "== half one: 'after' -- the tree as checked out =="
capture "after"

echo "== half two: 'before' -- ${QUEUE_RS} with the fix temporarily removed =="
if git diff --quiet -- "$QUEUE_RS"; then
  echo "FATAL: ${QUEUE_RS} has no uncommitted changes relative to HEAD, so \
there is nothing for 'git stash' to remove. This script is meant to be run \
from the PR branch BEFORE the fix commit lands (uncommitted diff present). \
To reproduce after the fix has been committed, check out the commit \
immediately before it, re-run this script's 'after' half by hand against that \
older commit's queue.rs, then check out the fix commit and re-run the 'after' \
half again -- there is no single-command path once both shapes are only \
reachable via git history rather than one working tree." >&2
  exit 1
fi
git stash push --quiet -- "$QUEUE_RS"
STASHED=1
capture "before"
restore
trap - EXIT

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
