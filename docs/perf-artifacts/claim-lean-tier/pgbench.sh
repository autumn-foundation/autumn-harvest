#!/usr/bin/env bash
# Usage: pgbench.sh <texts dir> <outdir> <rounds> <seconds> [<variant>]
#
# Times the window text and the lean text of one claim variant alone, with
# executor startup: pgbench, one client, extended protocol, the claim plan
# settings, then a rollback. The two forms run in turn, and the order swaps
# each round. <texts dir> holds <variant>-window.sql and <variant>-lean.sql,
# as lean_claim_db_tests::write_every_windowed_claim_text writes them.
# The fixture is pgbench-seed.sql on a copy of the migrated database that
# TEMPLATE names. The run ends with the plan node count of each form, from
# EXPLAIN of a generic plan.
set -eu
texts=$1; out=$2; rounds=$3; secs=$4; variant=${5:-any}
here=$(cd "$(dirname "$0")" && pwd)
export PGPASSWORD=${PGPASSWORD:-postgres}
conn="-h ${PGHOST:-127.0.0.1} -p ${PGPORT:-55432} -U ${PGUSER:-postgres}"
db=${DB:-scratch_lean_pgbench}
mkdir -p "$out"
psql $conn -Xq -d postgres -c "DROP DATABASE IF EXISTS $db" \
  -c "CREATE DATABASE $db TEMPLATE ${TEMPLATE:-harvest_schema}"
psql $conn -Xq -d "$db" -f "$here/pgbench-seed.sql"
for form in window lean; do
  python3 "$here/pgbench_text.py" "$texts/$variant-$form.sql" > "$out/pgbench-$variant-$form.sql"
done
for r in $(seq 1 "$rounds"); do
  if [ $((r % 2)) -eq 1 ]; then order="window lean"; else order="lean window"; fi
  for form in $order; do
    pgbench $conn -n -M prepared -c 1 -T "$secs" -D w=w1 -D 'q={default}' -D 'r={}' -D 'm={}' \
      -f "$out/pgbench-$variant-$form.sql" -r "$db" > "$out/$form-r$r.out" 2>&1
    lat=$(grep -E '^ +[0-9.]+ +[0-9]+ +WITH' "$out/$form-r$r.out" | awk '{print $1}')
    tps=$(grep -E '^tps' "$out/$form-r$r.out" | awk '{print $3}')
    echo "round=$r form=$form stmt_ms=$lat tps=$tps" | tee -a "$out/pgbench-runs.txt"
  done
done
# The plan nodes that one claim of each form initializes, from a generic plan.
for form in window lean; do
  { echo "BEGIN; SET LOCAL jit = off; SET LOCAL plan_cache_mode = force_generic_plan;"
    echo "SET LOCAL enable_bitmapscan = off; SET LOCAL enable_sort = off;"
    echo "PREPARE c(text, text[], text, bigint, text[], text[]) AS"
    cat "$texts/$variant-$form.sql"
    echo ";"
    echo "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) EXECUTE c('w1', ARRAY['default'], '', NULL, ARRAY[]::text[], ARRAY[]::text[]);"
    echo "ROLLBACK;"
  } | psql $conn -Xq -At -d "$db" > "$out/explain-$variant-$form.txt"
  nodes=$(grep -c -- '->' "$out/explain-$variant-$form.txt")
  never=$(grep -c -- '->.*(never executed)' "$out/explain-$variant-$form.txt")
  echo "form=$form plan_nodes=$((nodes + 1)) never_executed=$never" | tee -a "$out/plan-nodes.txt"
done
psql $conn -Xq -d postgres -c "DROP DATABASE $db"
