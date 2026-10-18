#!/usr/bin/env bash
# Usage: run.sh "<psql-args>" <n executions> <paused tasks> <outdir>
# Prints the deterministic counters: plan buffers, and a digest of the claimed ids + the next 50 candidates.
set -euo pipefail
PSQL="psql $1 -X -q"; N=$2; PZ=$3; OUT=$4; HERE="$(cd "$(dirname "$0")" && pwd)"; mkdir -p "$HERE/$OUT"
$PSQL -v n="$N" -v paused="$PZ" -f "$HERE/fixture.sql" >/dev/null
Q="$(cat "$HERE/claim.sql")"
{ echo "PREPARE c(text,text[],text,bigint,text[],text[]) AS $Q;"
  echo "\\set ON_ERROR_STOP 1"
  echo "BEGIN;"
  echo "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) EXECUTE c('w0','{default}','',NULL,'{}','{}');"
  echo "ROLLBACK;"
} > "$HERE/$OUT/explain_${PZ}p_$N.sql"
$PSQL -f "$HERE/$OUT/explain_${PZ}p_$N.sql" > "$HERE/$OUT/explain_${PZ}p_$N.txt"
# Equivalence: 120 sequential claims (each marks its row RUNNING), ids in claim order, then ROLLBACK.
# Includes the 50 oldest PENDING tasks that sit on PAUSED runs: they must never appear.
{ echo "PREPARE c(text,text[],text,bigint,text[],text[]) AS $Q;"; echo "BEGIN;"
  for i in $(seq 1 120); do echo "EXECUTE c('w0','{default}','',NULL,'{}','{}');"; done; echo "ROLLBACK;"; } > "$HERE/$OUT/claims_${PZ}p_$N.sql"
$PSQL -At -f "$HERE/$OUT/claims_${PZ}p_$N.sql" | grep -E '^[0-9a-f]{8}-' | cut -d'|' -f1 > "$HERE/$OUT/claimed_ids_${PZ}p_$N.txt"
echo "n=$N paused=$PZ: claims=$(wc -l < "$HERE/$OUT/claimed_ids_${PZ}p_$N.txt") sha=$(sha256sum "$HERE/$OUT/claimed_ids_${PZ}p_$N.txt" | cut -c1-16)"
echo "n=$N paused=$PZ: $(grep -E 'Buffers: shared' "$HERE/$OUT/explain_${PZ}p_$N.txt" | head -1)"
