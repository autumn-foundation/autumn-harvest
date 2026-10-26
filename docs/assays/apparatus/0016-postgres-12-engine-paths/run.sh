#!/usr/bin/env bash
# NON-PRODUCTION assay apparatus (ledger #16). Never merges as engine code.
# usage: run.sh <pg-bin-dir> <port> <outdir> <label> <runs> [module-regex]
set -u
BIN=$1; PORT=$2; OUT=$3; LABEL=$4; RUNS=$5; ONLY=${6:-.}
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(git rev-parse --show-toplevel)
TESTBIN=${TESTBIN:?path to the integration test binary}
MIG=$ROOT/autumn-harvest/migrations
mkdir -p "$OUT"; D=$OUT/data; rm -rf "$D"; export LD_LIBRARY_PATH=$BIN/../lib
"$BIN/initdb" -D "$D" -U postgres -A trust >/dev/null
"$BIN/pg_ctl" -D "$D" -o "-p $PORT -k /tmp -c listen_addresses=127.0.0.1 -c max_connections=300" -l "$OUT/pg.log" -w start >/dev/null || exit 2
P="psql -h 127.0.0.1 -p $PORT -U postgres -X -q"
$P -d postgres -Atc "select version()" > "$OUT/version.txt"
FILTERS=$(grep -E "$ONLY" "$HERE/modules.txt" | sed 's/$/::/' | tr '\n' ' ')
for run in $(seq 1 "$RUNS"); do
  db=assay$run; $P -d postgres -c "create database $db"
  for d in $(ls "$MIG"); do $P -d $db -1 -v ON_ERROR_STOP=1 -f "$MIG/$d/up.sql" >/dev/null 2>>"$OUT/mig$run.err" || echo "MIGRATION FAIL $d" >> "$OUT/mig$run.err"; done
  start=$(date +%s)
  HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:$PORT/$db timeout 5400 \
    "$TESTBIN" $FILTERS --test-threads=4 > "$OUT/$LABEL-run$run.log" 2>&1
  echo "run $run exit=$? secs=$(( $(date +%s)-start ))" | tee -a "$OUT/summary.txt"
  grep -E '^test .* \.\.\. (ok|FAILED|ignored)' "$OUT/$LABEL-run$run.log" | sort > "$OUT/$LABEL-run$run.results"
done
"$BIN/pg_ctl" -D "$D" -m fast stop >/dev/null
