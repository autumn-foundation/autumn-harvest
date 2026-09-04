#!/usr/bin/env bash
# Runs the whole pre-registered assay -- schema, every seed, every
# control/candidate EXPLAIN -- as ONE continuous psql session (driver.sql),
# so control and candidate genuinely share backend-local state, matching
# the pre-registration and report's "same session" description. Writes one
# EXPLAIN file per (scenario, variant) under results/.
set -euo pipefail
cd "$(dirname "$0")"

DB="${PGDATABASE:-prospect_assay3}"
mkdir -p results

psql -X -q -v ON_ERROR_STOP=1 -d "$DB" -f driver.sql

echo "done"
