#!/usr/bin/env bash
# Reproduction harness for the timeout-scanner query profiling pass
# (Ledger perf pass) documented in `docs/performance-timeout-scanner.md`.
#
# This does NOT toggle a code fix: `timeout.rs`'s scanner query builders are
# unmodified end to end -- measurement found every scanner query already
# cheap at realistic scale, so there is nothing to compare a "before" and
# "after" of. It runs the `#[ignore]`d evidence-capture test
# `timeout_scanner_perf_repro::zz_capture_timeout_scanner_evidence` ONCE
# against a production-shaped fixture (a large terminal `harvest_task_queue`
# bulk dwarfing a small live population, plus a sparse
# `harvest_workflow_executions` deadline population -- see the doc page for
# the exact shape and why it matters).
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/timeout_scanner_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/timeout_scanner_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL`, when set, is treated as an ADMIN URL, exactly
# as `claim_bench_support.rs` treats it elsewhere in this crate: the harness
# creates, migrates, seeds, measures, and leaves in place (for inspection) a
# fresh uniquely-named database per run. When it is UNSET, `setup_bench_db()`
# falls back to a testcontainer automatically. If neither an external
# database nor a Docker daemon is reachable, the capture test SKIPs loudly
# instead of producing artifacts.
#
# Writes into `docs/perf-artifacts/timeout-scanner-queries/`:
#   scanner-queries.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for each of
#     the five scanner query builders in `autumn_harvest::timeout`, driven
#     verbatim from their `pub const fn`s.
#   pg_stat_statements-after-one-tick.txt
#     A `pg_stat_statements` snapshot after driving the REAL
#     `timeout::enforce_timeouts_once()` production entry point once against
#     the same fixture.
#   fixture-summary.txt
#     The exact seeded row counts and cardinality skew.
#
# Preconditions: a Rust toolchain that can build this crate, and either
# Docker (for the harness's own testcontainer fallback) or a reachable
# Postgres named by HARVEST_TEST_DATABASE_URL. `pg_stat_statements` must be
# `CREATE EXTENSION`'d and preloaded via `shared_preload_libraries` on the
# target instance for the second artifact; without it the EXPLAIN bundle is
# still written and the script says so.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="$REPO_ROOT/docs/perf-artifacts/timeout-scanner-queries"
TEST_FILTER="timeout_scanner_perf_repro::zz_capture_timeout_scanner_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

echo "== capturing timeout-scanner evidence via ${TEST_FILTER} =="
cargo test -p autumn-harvest --features db --test integration -- \
  --ignored --nocapture "$TEST_FILTER" 2>&1 | tee /tmp/timeout_scanner_capture.log

if ! grep -q "== capture complete ==" /tmp/timeout_scanner_capture.log; then
  echo "FATAL: the test run did not report capture completion -- either no \
database was reachable (see the log for 'no database reachable; nothing \
captured') or the capture test itself failed. Check \
/tmp/timeout_scanner_capture.log." >&2
  exit 1
fi

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
