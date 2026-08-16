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
#   2. "before" -- against `queue.rs` with the fix temporarily removed. Two
#                  ways to get there, auto-detected:
#                    - dev loop (uncommitted diff present, fix not yet
#                      committed): `git stash` the file, as before.
#                    - clean checkout (no uncommitted diff -- the normal state
#                      once this branch has merged): walk `git log` for
#                      `queue.rs` to the most recent commit whose content does
#                      NOT yet contain the fix marker
#                      (`paused_queues AS MATERIALIZED`), and materialize that
#                      commit's content into the working file for the
#                      duration of the "before" capture. Either way the
#                      original working-tree file is restored on exit
#                      (success, failure, or interrupt).
#
# The capture test self-detects which shape it measured by inspecting
# `queue::claim_task_query()`'s own text at runtime, so there is no
# hand-maintained "old" copy of the query for either invocation to drift out
# of sync with -- both runs execute the real, compiled function.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/queue_pause_claim_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/queue_pause_claim_perf_repro.sh
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
FIX_MARKER="paused_queues AS MATERIALIZED"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

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
repro (see the before-half setup above/below), or the capture test itself \
failed/skipped. Check the log at /tmp/queue_pause_claim_capture_${label}.log." >&2
    exit 1
  fi
}

# One of: "none" (nothing to restore yet), "stash" (dev-loop half, restore via
# `git stash pop`), "checkout" (clean-checkout half, restore via
# `git checkout -- $QUEUE_RS`). Set right before the corresponding mutation so
# the trap is always correct regardless of which exit path fires.
RESTORE_MODE="none"
restore() {
  case "$RESTORE_MODE" in
    stash)
      echo "== restoring ${QUEUE_RS} (git stash pop) =="
      git stash pop
      ;;
    checkout)
      echo "== restoring ${QUEUE_RS} (git checkout) =="
      git checkout -- "$QUEUE_RS"
      ;;
    none) ;;
  esac
  RESTORE_MODE="none"
}
trap restore EXIT

echo "== half one: 'after' -- the tree as checked out =="
capture "after"

echo "== half two: 'before' -- ${QUEUE_RS} with the fix temporarily removed =="
if ! git diff --quiet -- "$QUEUE_RS"; then
  echo "== dev-loop checkout: stashing the uncommitted ${QUEUE_RS} diff =="
  git stash push --quiet -- "$QUEUE_RS"
  RESTORE_MODE="stash"
else
  echo "== clean checkout: ${QUEUE_RS} has no uncommitted diff -- looking up \
the last commit predating the fix marker '${FIX_MARKER}' =="
  PRE_FIX_COMMIT=""
  while IFS= read -r sha; do
    # `git show` failing here (a transient object-read error, a shallow clone
    # missing history, ...) must abort loudly rather than be silently treated
    # as "marker not found" -- the two cannot share one `!` on a piped
    # command, or a transient failure on the fix commit itself would
    # misattribute it as the pre-fix baseline and the "before"/"after"
    # capture would silently compare the fixed query against itself. The
    # content is captured only to check for the marker; materialization below
    # re-runs `git show` with a raw file redirect so no shell string
    # round-trip can alter the file's exact bytes (e.g. trailing newlines).
    content="$(git show "${sha}:${QUEUE_RS}" 2>&1)" || {
      echo "FATAL: 'git show ${sha}:${QUEUE_RS}' failed -- cannot walk \
history to find a pre-fix revision:
${content}" >&2
      exit 1
    }
    # A herestring, not `printf ... | grep -q`: `-q` exits the instant it
    # finds a match, closing its read end -- on content this size (~160KB)
    # that can race the still-writing producer side of a real pipe and
    # deliver it SIGPIPE before it finishes, which `pipefail` (set at the top
    # of this script) then reports as pipeline failure even though `grep`
    # itself matched successfully, silently flipping this `if !` backwards.
    # A herestring hands `content` to `grep` directly with no second process
    # in between, so there is nothing to race or receive SIGPIPE.
    if ! grep -qF "$FIX_MARKER" <<<"$content"; then
      PRE_FIX_COMMIT="$sha"
      break
    fi
  done < <(git log --format='%H' -- "$QUEUE_RS")
  if [ -z "$PRE_FIX_COMMIT" ]; then
    echo "FATAL: every commit touching ${QUEUE_RS} in this history already \
contains the fix marker '${FIX_MARKER}' -- could not locate a pre-fix \
revision to reproduce the baseline from." >&2
    exit 1
  fi
  echo "== using ${QUEUE_RS} from ${PRE_FIX_COMMIT} (pre-fix) =="
  # RESTORE_MODE is set BEFORE the mutation below (not after) so a failure in
  # the `git show` redirect itself still leaves the exit trap knowing to
  # `git checkout -- $QUEUE_RS` -- the redirect can create/truncate the file
  # before failing partway through.
  RESTORE_MODE="checkout"
  git show "${PRE_FIX_COMMIT}:${QUEUE_RS}" > "$QUEUE_RS" || {
    echo "FATAL: 'git show ${PRE_FIX_COMMIT}:${QUEUE_RS}' failed on the \
materializing redirect, despite succeeding moments earlier during the \
search above -- transient error; restoring ${QUEUE_RS} and aborting." >&2
    exit 1
  }
fi
capture "before"
restore
trap - EXIT

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
