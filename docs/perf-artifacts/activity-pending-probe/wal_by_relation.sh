#!/bin/sh
# Appends 10,000 events (rolled back), then splits the WAL of that range by relation with pg_waldump.
# usage: wal_by_relation.sh <dir with wal.sql>
Q="psql -h /tmp/kspg -p 55440 -U postgres -d ks -At"
$Q -c "CHECKPOINT" >/dev/null
L0=$($Q -c "SELECT pg_current_wal_insert_lsn()")
sed -e '/^CHECKPOINT;/d' -e '/l0 \\gset/d' -e 's/pg_wal_lsn_diff(pg_current_wal_insert_lsn(), :.l0.)/0/' "$1/wal.sql" | $Q >/dev/null
L1=$($Q -c "SELECT pg_current_wal_insert_lsn()")
DB=$($Q -c "SELECT oid FROM pg_database WHERE datname = current_database()")
total=$(( $($Q -c "SELECT pg_wal_lsn_diff('$L1', '$L0')") ))
for rel in harvest_events idx_spike_activity_pending idx_harvest_events_activity_started_lookup idx_harvest_events_history_page harvest_events_workflow_exec_id_event_id_key; do
  node=$($Q -c "SELECT pg_relation_filenode('$rel')")
  [ -z "$node" ] && continue
  bytes=$(su postgres -c "/usr/lib/postgresql/16/bin/pg_waldump -p /tmp/kspg/data/pg_wal -s $L0 -e $L1 --relation=1663/$DB/$node 2>/dev/null" \
    | sed -n 's/.*len (rec\/tot): *[0-9]*\/ *\([0-9]*\).*/\1/p' | awk '{s+=$1} END {print s+0}')
  echo "range_total=$total $rel wal_bytes=$bytes"
done
