#!/usr/bin/env bash
# NON-PRODUCTION assay apparatus (ledger #15). Applies every up.sql in order.
# usage: run.sh <pg-bin-dir> <port> <outdir>
set -u
BIN=$1; PORT=$2; OUT=$3; MIG=${MIG:-autumn-harvest/migrations}
D=$OUT/data; mkdir -p "$OUT"; rm -rf "$D"
export LD_LIBRARY_PATH=$BIN/../lib
"$BIN/initdb" -D "$D" -U postgres -A trust >/dev/null
"$BIN/pg_ctl" -D "$D" -o "-p $PORT -k /tmp -c listen_addresses=127.0.0.1" -l "$OUT/pg.log" -w start >/dev/null || { echo "server failed to start on port $PORT" >&2; exit 2; }
PSQL=${PSQL:-$BIN/psql}; P="$PSQL -h 127.0.0.1 -p $PORT -U postgres -X -q"
$P -d postgres -Atc "select version()" | tee "$OUT/version.txt"
rm -f "$OUT/summary.txt"; : > "$OUT/summary.txt"
rc=0
for run in 1 2 3; do
  db=assay$run; $P -d postgres -c "create database $db" 
  ok=0; fail=0; : > "$OUT/run$run.fail"
  for d in $(ls "$MIG"); do
    if $P -d $db -1 -v ON_ERROR_STOP=1 -f "$MIG/$d/up.sql" >/dev/null 2>"$OUT/err.tmp"; then ok=$((ok+1)); else fail=$((fail+1)); rc=1; echo "$d: $(head -c 300 $OUT/err.tmp | tr '\n' ' ')" >> "$OUT/run$run.fail"; fi
  done
  echo "run $run: applied=$ok failed=$fail total=$((ok+fail))" | tee -a "$OUT/summary.txt"
  $P -d $db -v ON_ERROR_STOP=1 -Atc "
   select 'col',table_name,column_name,data_type from information_schema.columns where table_schema='public'
   union all select 'idx',tablename,indexname,'' from pg_indexes where schemaname='public'
   union all select 'trg',c.relname,t.tgname,'' from pg_trigger t join pg_class c on c.oid=t.tgrelid where not t.tgisinternal and c.relnamespace='public'::regnamespace
   union all select 'tbl',tablename,'','' from pg_tables where schemaname='public' order by 1,2,3,4" > "$OUT/sig$run.txt" || { echo "signature query failed on run $run" >&2; rc=1; rm -f "$OUT/sig$run.txt"; }
done
"$BIN/pg_ctl" -D "$D" -m fast stop >/dev/null
exit $rc
