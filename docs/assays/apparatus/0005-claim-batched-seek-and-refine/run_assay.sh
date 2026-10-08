#!/usr/bin/env bash
# Runs the whole pre-registered assay -- schema, function, every seed,
# every control/candidate measurement -- as ONE continuous psql session
# (driver.sql), so control and candidate genuinely share backend-local
# state. Writes one file per (scenario, measurement) under results/.
#
# `\timing`'s "Time: ... ms" lines are NOT redirected by driver.sql's own
# `\o` commands (verified directly: `\o` only redirects query *result*
# output, `\timing` reports go to psql's own stdout regardless) -- so
# results/run.log is generated here, from this run's own stdout, every
# time this script runs. Post-review (Codex): the archived run.log was
# previously reconstructed by hand from a captured terminal transcript,
# which a later rerun would not have refreshed even if its own numbers
# differed.
set -euo pipefail
cd "$(dirname "$0")"

DB="${PGDATABASE:-prospect_assay5}"
mkdir -p results

psql -X -q -v ON_ERROR_STOP=1 -d "$DB" -f driver.sql | tee results/run.log

echo "done"
