#!/usr/bin/env bash
# Usage: run.sh <texts dir> <outdir> <rounds> <claims> <pattern> [<pattern> ...]
#
# Times a claim loop of mixed claim kinds under three skip rules (see gen.py).
# Each run seeds the fixture again. Each round runs the three rules in a
# rotated order. TEMPLATE names a migrated database to copy. One psql
# session sends each statement as its own round trip, and the BEGIN batch
# as one, as a worker does. The time is the wall time of psql.
set -eu
texts=$1; out=$2; rounds=$3; claims=$4; shift 4
here=$(cd "$(dirname "$0")" && pwd)
export PGPASSWORD=${PGPASSWORD:-postgres}
PSQL="psql -h ${PGHOST:-127.0.0.1} -p ${PGPORT:-55432} -U ${PGUSER:-postgres} -Xq"
db=${DB:-scratch_lean_mixed}
mkdir -p "$out"
$PSQL -d postgres -c "DROP DATABASE IF EXISTS $db" -c "CREATE DATABASE $db TEMPLATE ${TEMPLATE:-harvest_schema}"
python3 "$here/load_forms.py" "$texts" > "$out/forms.sql"
$PSQL -d "$db" -f "$here/seed.sql"
orders=("off shared final" "final off shared" "shared final off")
for pattern in "$@"; do
  for rule in off shared final; do
    python3 "$here/gen.py" "$rule" "$pattern" "$claims" "$out/forms.sql" \
      > "$out/run-$pattern-$rule.sql" 2>> "$out/counts.txt"
  done
  for r in $(seq 1 "$rounds"); do
    for rule in ${orders[$(( (r - 1) % 3 ))]}; do
      $PSQL -d "$db" -c "call seed_mixed($(( claims + 400 )))" -c "vacuum analyze harvest_task_queue"
      t0=$(date +%s%N)
      res=$($PSQL -d "$db" -f "$out/run-$pattern-$rule.sql" 2>&1 | grep -v '^PREPARE' | tr '\n' ' ')
      t1=$(date +%s%N)
      echo "pattern=$pattern round=$r rule=$rule ms=$(( (t1 - t0) / 1000000 )) :: $res" | tee -a "$out/timing.txt"
    done
  done
done
$PSQL -d postgres -c "DROP DATABASE $db"
