#!/usr/bin/env bash
# Usage: interleave.sh <out-dir> <rounds> <seconds> <m-bind> <set>...
#
# One pgbench script holds every rewrite set, each in its own claim
# transaction, so one backend runs the sets back to back. pgbench -r then
# reports the mean latency of each statement over the same seconds. Each
# round rotates the order of the sets. TEXTS is the out-dir of mk.py. DB is
# a migrated database with ../pgbench-seed.sql or seed-gates.sql loaded.
set -u
export PGPASSWORD=postgres
OUT=$1; ROUNDS=$2; SECS=$3; M=$4; shift 4
DB=${DB:-scratch_lean_rewrites}; TEXTS=${TEXTS:-texts}; DEFAULT_Q="{default}"; Q=${Q:-$DEFAULT_Q}
mkdir -p "$OUT"
vars=("$@"); n=${#vars[@]}
for r in $(seq 1 "$ROUNDS"); do
  order=()
  for k in $(seq 0 $((n-1))); do order+=("${vars[$(( (k + r - 1) % n ))]}"); done
  script="$OUT/script-r$r.sql"; : > "$script"
  for v in "${order[@]}"; do cat "$TEXTS/pgb-$v.sql" >> "$script"; done
  pgbench -h 127.0.0.1 -p 55432 -U postgres -n -M prepared -c 1 \
    -T "$SECS" -D w=w1 -D "q=$Q" -D 'r={}' -D "m=$M" -f "$script" -r "$DB" \
    > "$OUT/pgb-r$r.out" 2>&1
  mapfile -t lats < <(grep -E "^ +[0-9.]+ +[0-9]+ +WITH" "$OUT/pgb-r$r.out" | awk '{print $1}')
  tps=$(grep -E "^tps" "$OUT/pgb-r$r.out" | awk '{print $3}')
  for k in $(seq 0 $((n-1))); do
    echo "$r ${order[$k]} stmt_ms=${lats[$k]} tps=$tps" | tee -a "$OUT/summary.txt"
  done
done
