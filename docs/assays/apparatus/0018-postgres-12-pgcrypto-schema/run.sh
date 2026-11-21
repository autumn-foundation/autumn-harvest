#!/usr/bin/env bash
# NON-PRODUCTION assay apparatus (ledger #18). Never merges into engine code.
# usage: run.sh <pg-bin-dir> <port> <outdir> <label> <runs>
# PGOPTIONS splits on spaces, so a search_path value holds no space.
# Run as a non-root user that owns <outdir>. MIG defaults to the core migrations.
set -u
BIN=$1; PORT=$2; OUT=$3; LABEL=$4; RUNS=${5:-3}
MIG=${MIG:-autumn-harvest/migrations}
D=$OUT/data; mkdir -p "$OUT"; rm -rf "$D"
export LD_LIBRARY_PATH=$BIN/../lib
"$BIN/initdb" -D "$D" -U postgres -A trust >/dev/null
"$BIN/pg_ctl" -D "$D" -o "-p $PORT -k /tmp -c listen_addresses=127.0.0.1" -l "$OUT/pg.log" -w start >/dev/null || { echo "server failed to start" >&2; exit 2; }
PSQL="$BIN/psql -h 127.0.0.1 -p $PORT -U postgres -X -q -v ON_ERROR_STOP=1 -v VERBOSITY=verbose"
$PSQL -d postgres -Atc "select version()" > "$OUT/version.txt"
RES=$OUT/$LABEL.results; : > "$RES"

# migrate <db> <search_path>: prints "pass" or "fail <dir> <first error line>".
migrate() {
  local db=$1 sp=$2 d
  for d in $(ls "$MIG"); do
    if ! PGOPTIONS="-c search_path=$sp" $PSQL -d "$db" -1 -f "$MIG/$d/up.sql" >/dev/null 2>"$OUT/err.tmp"; then
      echo "fail $d $(grep -m1 ERROR "$OUT/err.tmp" | sed 's/^.*ERROR: *//')"; return
    fi
  done
  echo pass
}

# probe <db> <search_path> <schema>: the throttle.rs:634 text, then a default-only id.
probe() {
  local db=$1 sp=$2 sc=$3 s1 s2
  s1="INSERT INTO harvest_start_throttle (id, workflow_name, throttle_key, bucket_key, workflow_id, queue_name, input, start_options, deferred_at, expires_at, shard_id, created_at) VALUES (gen_random_uuid(), 'wf', 'tk', 'bk', 'assay18', 'default', 'null', '{}', NOW(), NULL, 0, NOW())"
  s2="INSERT INTO harvest_audit_log (operation, target_type, route_or_command, status) VALUES ('assay18.probe', 'assay', 'assay18', 'succeeded')"
  for s in "$s1" "$s2"; do
    if ! PGOPTIONS="-c search_path=$sp" $PSQL -d "$db" -c "$s" >/dev/null 2>"$OUT/err.tmp"; then
      echo "fail $(echo "$s" | cut -c1-40) | $(grep -m1 ERROR "$OUT/err.tmp" | sed 's/^.*ERROR: *//')"; return
    fi
  done
  local n
  n=$($PSQL -d "$db" -Atc "select (select count(*) from $sc.harvest_start_throttle where workflow_id='assay18') + (select count(*) from $sc.harvest_audit_log where operation='assay18.probe')")
  [ "$n" = 2 ] && echo pass || echo "fail rows landed outside $sc (count=$n)"
}

for run in $(seq 1 "$RUNS"); do
  for sc in S3 S4 S5 S1 S2; do
    db=$(echo "${sc}_r$run" | tr A-Z a-z)
    $PSQL -d postgres -c "drop database if exists $db" -c "create database $db"
    case $sc in
      S1) $PSQL -d $db -c "create schema harvest_alt"; schema=harvest_alt; msp=harvest_alt; rsp=harvest_alt ;;
      S2) $PSQL -d $db -c "create extension pgcrypto" -c "create schema harvest_alt"; schema=harvest_alt; msp=harvest_alt; rsp=harvest_alt ;;
      S3) $PSQL -d $db -c "create extension pgcrypto" -c "create schema harvest_alt"; schema=harvest_alt; msp=harvest_alt,public; rsp=harvest_alt,public ;;
      S4) $PSQL -d $db -c "create extension pgcrypto" -c "create schema harvest_alt"; schema=harvest_alt; msp=harvest_alt,public; rsp=harvest_alt ;;
      S5) $PSQL -d $db -c "create schema harvest_a" -c "create schema harvest_b"
          first=$(migrate $db harvest_a)
          if [ "$first" != pass ]; then echo "$LABEL run=$run $sc setup=fail($first)" | tee -a "$RES"; continue; fi
          schema=harvest_b; msp=harvest_b; rsp=harvest_b ;;
    esac
    ext=$($PSQL -d $db -Atc "select extnamespace::regnamespace from pg_extension where extname='pgcrypto'")
    m=$(migrate $db "$msp")
    if [ "$m" = pass ]; then p=$(probe $db "$rsp" $schema); else p=skipped; fi
    ext2=$($PSQL -d $db -Atc "select extnamespace::regnamespace from pg_extension where extname='pgcrypto'")
    echo "$LABEL run=$run $sc pgcrypto_before=${ext:-none} pgcrypto_after=${ext2:-none} migrate=[$m] probe=[$p]" | tee -a "$RES"
  done
done
"$BIN/pg_ctl" -D "$D" -m fast stop >/dev/null
