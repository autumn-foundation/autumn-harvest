#!/usr/bin/env bash
# Runs the e2e_bench binary in BENCH_BIN with batches of up to 8 rows.
HARVEST_BENCH_CLAIM_BATCH=8 exec "$BENCH_BIN" "$@"
