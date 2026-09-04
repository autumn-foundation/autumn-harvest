#!/usr/bin/env bash
# Runs the pre-registered scenarios: for each, seeds the fixture, captures
# the control plan (committed-fix shape), then -- after the candidate index
# exists -- captures the candidate plan (partial-index + base-table
# correlated subquery). Writes one EXPLAIN file per (scenario, variant).
set -euo pipefail
cd "$(dirname "$0")"

DB="${PGDATABASE:-prospect_assay3}"
OUT=results
mkdir -p "$OUT"

psql -X -q -d "$DB" -f schema.sql

declare -a SCENARIOS=(
  "idle_256:10000:4:256:0"
  "hot_256:10000:4:256:2000"
  "hot_5000:10000:4:5000:2000"
  "idle_5000:10000:4:5000:0"
)

run_variant() {
  local name="$1" file="$2" label="$3"
  psql -X -q -d "$DB" -f "$file" > "$OUT/${name}-${label}.explain.txt"
  echo "wrote $OUT/${name}-${label}.explain.txt"
}

for s in "${SCENARIOS[@]}"; do
  IFS=':' read -r name backlog queues keys running <<< "$s"
  echo "== seeding $name (backlog=$backlog queues=$queues keys=$keys running=$running) =="
  psql -X -q -d "$DB" -v backlog="$backlog" -v queues="$queues" -v keys="$keys" -v running_rows="$running" -f seed.sql
  run_variant "$name" control.sql control
done

echo "== adding candidate index =="
psql -X -q -d "$DB" -f candidate_index.sql

for s in "${SCENARIOS[@]}"; do
  IFS=':' read -r name backlog queues keys running <<< "$s"
  echo "== re-seeding $name for candidate run =="
  psql -X -q -d "$DB" -v backlog="$backlog" -v queues="$queues" -v keys="$keys" -v running_rows="$running" -f seed.sql
  run_variant "$name" candidate.sql candidate
done

echo "done"
