#!/usr/bin/env bash
# Usage: pgbench.sh <db> <script.sql>
# Runs one claim form in a loop, the way a claim loop sends it: extended
# protocol, the claim plan settings, then a rollback (issue #2680).
/usr/lib/postgresql/16/bin/pgbench -h 127.0.0.1 -p 55432 -U postgres -n -M prepared \
  -c 1 -T 5 -D w=bench-s0-0 -D 'q={default}' -D 'r={}' -D 'm={}' -f "$2" -r "$1"
