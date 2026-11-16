#!/usr/bin/env bash
# Usage: pgbench.sh <db> <rounds>
# Times the window form and its batch forms, alternating, on one backlog.
# Each script runs the claim plan settings, the claim, then a rollback.
dir=$(dirname "$0")
for r in $(seq 1 "$2"); do
  for k in 1 2 4 8; do
    f="$dir/pgbench-claim-batch-$k.sql"; [ "$k" = 1 ] && f="$dir/pgbench-claim-window.sql"
    ms=$(/usr/lib/postgresql/16/bin/pgbench -h 127.0.0.1 -p 55432 -U postgres -n -M prepared \
      -c 1 -T 8 -D w=w1 -D 'q={default}' -D 'r={}' -D 'm={}' -f "$f" "$1" 2>&1 \
      | awk '/latency average/ {print $4}')
    awk -v r="$r" -v k="$k" -v ms="$ms" \
      'BEGIN { printf "round %s k=%s stmt_ms=%s per_row_ms=%.3f\n", r, k, ms, ms / k }'
  done
done
