#!/usr/bin/env bash
set -euo pipefail
: "${PGDATABASE:?set PGDATABASE, e.g. prospect_assay6}"
mkdir -p results
psql -v ON_ERROR_STOP=1 -f schema.sql | tee results/run.log

# All three arms reuse ledger #5's own seed.sql verbatim (same
# concurrency_key/concurrency_cap population on every row, same
# generate_series shape) so row/tuple width is identical across arms --
# queues is the only seed parameter that varies (post-review, Codex, P2).

echo "--- multi-queue control (reproduce ledger #5's shape on this instance) ---" | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -v backlog=10000 -v queues=4 -v keys=256 -v running_rows=0 \
  -f ../0005-claim-batched-seek-and-refine/seed.sql | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -f multi_queue_control.sql | tee results/multi_queue_control.explain.txt | tee -a results/run.log

echo "--- single-queue, ANY() over a 1-element array (production-representative shape) ---" | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -v backlog=10000 -v queues=1 -v keys=256 -v running_rows=0 \
  -f ../0005-claim-batched-seek-and-refine/seed.sql | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -f single_queue_any_diagnostic.sql | tee results/single_queue_any.explain.txt | tee -a results/run.log

echo "--- single-queue, scalar equality (secondary arm -- not itself a production shape) ---" | tee -a results/run.log
psql -v ON_ERROR_STOP=1 -f single_queue_diagnostic.sql | tee results/single_queue_scalar.explain.txt | tee -a results/run.log

echo "--- done ---" | tee -a results/run.log
