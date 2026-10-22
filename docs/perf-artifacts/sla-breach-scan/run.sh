#!/usr/bin/env bash
# Usage: ./run.sh "<psql connection args>" <executions> baseline|after
# A Postgres 16 with every migration applied. Writes EXPLAIN (ANALYZE, BUFFERS,
# VERBOSE, SETTINGS) and the flagged ids of the scan to the output directory.
set -euo pipefail
D="$1"; N="$2"; OUT="$3"
HERE="$(cd "$(dirname "$0")" && pwd)"
BODY="$HERE/scan_body.sql"; [ "$OUT" = after ] && BODY="$HERE/scan_body_after.sql"
mkdir -p "$HERE/$OUT"
psql $D -q -v ON_ERROR_STOP=1 -v n="$N" -f "$HERE/fixture.sql" >/dev/null 2>&1
psql $D -q -c "VACUUM ANALYZE harvest_workflow_executions"
# Plan and buffers on the steady-state table: no breach pending, so 0 rows flagged.
{ echo "BEGIN;"; echo "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)"; cat "$BODY" | sed '$ s/$/\nRETURNING id, workflow_name, queue_name;/'; echo "ROLLBACK;"; } \
  | psql $D -q > "$HERE/$OUT/explain_$N.txt"
# Result set on the table plus the boundary rows: ids flagged, in id order.
{ echo "BEGIN;"; cat "$HERE/edge_rows.sql"; echo "ANALYZE harvest_workflow_executions;";
  echo "CREATE TEMP TABLE got ON COMMIT DROP AS WITH u AS ("; cat "$BODY"; echo "RETURNING id) SELECT id FROM u;";
  echo "\\o $HERE/$OUT/flagged_$N.txt";
  echo "SELECT e.note, e.flag AS expected, (g.id IS NOT NULL) AS flagged FROM edge e LEFT JOIN got g USING (id) ORDER BY e.note;";
  echo "SELECT count(*) AS flagged_total, md5(string_agg(id::text, ',' ORDER BY id)) AS flagged_md5 FROM got;";
  echo "ROLLBACK;"; } | psql $D -q
