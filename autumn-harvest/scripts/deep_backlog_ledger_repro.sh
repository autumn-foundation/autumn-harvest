#!/usr/bin/env bash
# Ledger capture against the seeded deep-backlog fixture (issue #1956).
# The findings are in `docs/performance-deep-backlog.md`.
#
# Usage:
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/deep_backlog_ledger_repro.sh
#
# With HARVEST_TEST_DATABASE_URL unset, the harness starts a postgres:16
# container with pg_stat_statements preloaded. Docker must be reachable.
#
# The URL is an admin URL. The harness creates one database per run, seeds
# it, drives the claim workload, snapshots pg_stat_statements and
# pg_stat_user_tables, and then drops the database. An external server needs
# pg_stat_statements in shared_preload_libraries.
#
# Knobs (all optional):
#   HARVEST_DEEP_BACKLOG_SEED   fixture seed (default 1956)
#   HARVEST_DEEP_BACKLOG_ROWS   live task rows of the deep run (default 1000000)
#   HARVEST_DEEP_BACKLOG_SECS   wall-clock bound of each workload (default 600)
#   HARVEST_DEEP_BACKLOG_OUT    artifact directory
#                               (default docs/perf-artifacts/deep-backlog)
#
# Writes, for each of the `shallow` and `deep` runs:
#   <run>-post-seed-pg_stat_user_tables.txt   table stats after the seed
#   <run>-workload-pg_stat_user_tables.txt    table counter deltas of the workload
#   <run>-pg_stat_statements.txt              statements of the workload
# and fixture-summary.txt for both runs.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

TEST_FILTER="deep_backlog_fixture_tests::zz_capture_deep_backlog_ledger_evidence"
LOG="$(mktemp -t deep_backlog_ledger.XXXXXX.log)"

echo "== capturing via ${TEST_FILTER}; log in ${LOG} =="
if ! cargo test -p autumn-harvest --test integration -- \
  --ignored --nocapture --exact "$TEST_FILTER" 2>&1 | tee "$LOG"; then
  echo "FATAL: the capture test failed. See ${LOG}." >&2
  exit 1
fi

if ! grep -q "^== capture complete: artifacts in " "$LOG"; then
  echo "FATAL: the capture did not complete. The test failed, or it skipped" \
    "for lack of a database. See ${LOG}." >&2
  exit 1
fi
echo "== done =="
