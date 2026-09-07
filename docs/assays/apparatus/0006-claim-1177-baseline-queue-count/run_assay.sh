#!/usr/bin/env bash
set -euo pipefail
: "${PGDATABASE:?set PGDATABASE, e.g. prospect_assay6}"
mkdir -p results
psql -v ON_ERROR_STOP=1 -f schema.sql | tee results/run.log

echo "--- multi-queue control (reproduce ledger #5's shape on this instance) ---" | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -v backlog=10000 -v queues=4 -v keys=256 -v running_rows=0 \
  -f ../0005-claim-batched-seek-and-refine/seed.sql | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -f multi_queue_control.sql | tee results/multi_queue_control.explain.txt | tee -a results/run.log

echo "--- single-queue test (the pre-registered hypothesis) ---" | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -v backlog=10000 -f seed_single_queue.sql | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -f single_queue_diagnostic.sql | tee results/single_queue.explain.txt | tee -a results/run.log

echo "--- done ---" | tee -a results/run.log
