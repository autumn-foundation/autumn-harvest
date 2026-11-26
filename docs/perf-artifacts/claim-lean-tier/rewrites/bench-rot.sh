#!/usr/bin/env bash
# Usage: bench-rot.sh <out-dir> <rounds> <seconds> <m-bind> <set>...
#
# Runs pgbench once per rewrite set per round, one client, and records the
# mean latency of the claim statement. Round r starts at set (r-1) mod n, so
# no set keeps one slot of a round. TEXTS is the out-dir of mk.py. DB is a
# migrated database with ../pgbench-seed.sql or seed-gates.sql loaded.
set -u
export PGPASSWORD=postgres
OUT=$1; ROUNDS=$2; SECS=$3; M=$4; shift 4
DB=${DB:-scratch_lean_rewrites}; TEXTS=${TEXTS:-texts}; DEFAULT_Q="{default}"; Q=${Q:-$DEFAULT_Q}
mkdir -p "$OUT"
vars=("$@"); n=${#vars[@]}
for r in $(seq 1 "$ROUNDS"); do
  for k in $(seq 0 $((n-1))); do
    v=${vars[$(( (k + r - 1) % n ))]}
    pgbench -h 127.0.0.1 -p 55432 -U postgres -n -M prepared -c 1 \
      -T "$SECS" -D w=w1 -D "q=$Q" -D 'r={}' -D "m=$M" -f "$TEXTS/pgb-$v.sql" -r "$DB" \
      > "$OUT/pgb-$v-r$r.out" 2>&1
    lat=$(grep -E "^ +[0-9.]+ +[0-9]+ +WITH" "$OUT/pgb-$v-r$r.out" | awk '{print $1}')
    tps=$(grep -E "^tps" "$OUT/pgb-$v-r$r.out" | awk '{print $3}')
    echo "$r $v stmt_ms=$lat tps=$tps" >> "$OUT/summary.txt"
  done
done
