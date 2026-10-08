#!/usr/bin/env bash
# Reproduction harness for the pause-array-size finding (issue #1215)
# documented in `docs/performance.md`.
#
# Like `capability_labels_claim_perf_repro.sh`, this does NOT toggle a code
# fix: `queue::claim_task_query()` is unmodified end to end -- issue #1215
# found a cost that scales with pause-array width, not a query-shape defect
# to rewrite. It runs the `#[ignore]`d evidence-capture test
# `claim_budget_tests::zz_capture_pause_array_size_claim_evidence` ONCE, which
# sweeps array size (0/1/20/199 ballast pause rows -- 0% selectivity, so the
# effect cannot be explained by a change in which rows are eligible) across
# three shapes: `paused_activities` (#807, unconditional), and `paused_queues`
# (#619) both bound to a typical worker's own queues and to an atypical
# worker whose own bind is itself wide.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/pause_array_size_claim_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/pause_array_size_claim_perf_repro.sh
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
# Writes into `docs/perf-artifacts/pause-array-size/`:
#   activity-pause-array-{0,1,20,199}.explain.txt
#   queue-pause-bound-array-{0,1,20,199}.explain.txt
#   queue-pause-wide-array-{0,199}.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, SETTINGS, TIMING OFF)` for
#     `claim_task_query()` at a fixed 10,000-row backlog, one file per array
#     size per shape.
#   summary.txt
#     One line per capture: predicate, array size, backlog, and the `Sort
#     Method:` line the plan reported (or "(no Sort node)").
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
OUT_DIR="$REPO_ROOT/docs/perf-artifacts/pause-array-size"
TEST_FILTER="claim_budget_tests::zz_capture_pause_array_size_claim_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

LOG="/tmp/pause_array_size_claim_capture.log"
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
