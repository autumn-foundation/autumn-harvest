#!/usr/bin/env bash
# Reproduction harness for the build-reachability fan-out fix (Ledger perf
# pass), documented in docs/performance-build-reachability-fanout.md.
#
# The evidence-capture test
# (`build_reachability_fanout_perf::zz_capture_build_reachability_fanout_evidence`)
# seeds a production-shaped fixture into a throwaway database, then captures
# pg_stat_statements + result-set evidence for the "before" reproduction (a
# loop over the unchanged per-build build_reachability() helper, which is
# what the pre-fix all_build_reachability() did internally) and for the real,
# shipped all_build_reachability() ("after"), in one run.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/build_reachability_fanout_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/build_reachability_fanout_perf_repro.sh
#
# HARVEST_TEST_DATABASE_URL, when set, is treated as an ADMIN URL, exactly as
# claim_bench_support.rs treats it elsewhere in this crate: the harness
# creates, migrates, seeds, measures, and drops a fresh uniquely-named
# database per run. When unset, claim_bench_support::db::setup_bench_db falls
# back to a testcontainer automatically. If neither an external database nor
# a Docker daemon is reachable, the capture test skips instead of producing
# artifacts.
#
# Writes into docs/perf-artifacts/build-reachability-fanout/:
#   {before,after}.pg_stat_statements.txt
#     A pg_stat_statements snapshot for each strategy, filtered to
#     build-id-bearing statements, ranked by total buffers.
#   {before,after}.result-rows.txt
#     The per-build reachability counters each strategy computed --
#     byte-identical between the two, which is this fix's equivalence proof.
#
# Preconditions: a Rust toolchain that can build this crate, and either
# Docker (for the harness's own testcontainer fallback) or a reachable
# Postgres named by HARVEST_TEST_DATABASE_URL with pg_stat_statements in
# shared_preload_libraries (the harness creates the extension itself, but the
# module must already be preloaded at postmaster start -- CREATE EXTENSION
# alone cannot retroactively enable it).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="$REPO_ROOT/docs/perf-artifacts/build-reachability-fanout"
TEST_FILTER="build_reachability_fanout_perf::zz_capture_build_reachability_fanout_evidence"

if [ -z "${HARVEST_TEST_DATABASE_URL:-}" ]; then
  echo "== HARVEST_TEST_DATABASE_URL is unset -- relying on the testcontainer \
fallback (Docker must be reachable). Set the variable to an admin connection \
string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to skip \
Docker entirely. =="
fi

echo "== capturing before/after evidence via ${TEST_FILTER} =="
cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --nocapture "$TEST_FILTER" 2>&1 | tee /tmp/build_reachability_fanout_capture.log

if ! grep -q "^equivalence confirmed:" /tmp/build_reachability_fanout_capture.log; then
  echo "FATAL: the capture run did not report equivalence -- either it \
skipped (no database reachable) or the equivalence assertion inside the \
test itself failed. Check /tmp/build_reachability_fanout_capture.log." >&2
  exit 1
fi

echo "== done. Artifacts in ${OUT_DIR} =="
ls -la "$OUT_DIR"
