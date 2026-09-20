-- Index to support the recent_event_execs CTE in
-- status_summary::count_stalled_candidates (issue #1643).
--
-- The CTE is `SELECT DISTINCT workflow_exec_id FROM harvest_events WHERE
-- timestamp >= NOW() - N minutes`. Neither existing harvest_events index
-- leads with timestamp (idx_harvest_events_exec_last and
-- idx_harvest_events_exec both lead with workflow_exec_id;
-- idx_harvest_events_history_page leads with workflow_exec_id too), so this
-- query would otherwise fall back to a full table scan. The covering
-- (timestamp, workflow_exec_id) key turns that into an index range scan
-- plus an index-only projection for the DISTINCT. Measurements:
-- docs/performance-status-summary-stalled.md.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_events`,
-- which blocks every append, claim and completion touching this table. That
-- is the same trade-off `20260702000000_harvest_usage_report_indexes`,
-- `20260905181020_harvest_usage_activity_lookback_index`,
-- `20260911213344_harvest_external_outbox_scan_indexes` and
-- `20260913010332_harvest_audit_export_seq_idx_covering` documented for
-- their own additions to a large table.
--
-- Unlike those, this index is NOT partial -- it has no WHERE clause, so its
-- build must read and index every row currently in harvest_events, not one
-- rare event type. Size the build window from the table's total row count,
-- not from a filtered subset.
--
-- For a live, already-large deployment, build it out of band first, then
-- run this migration. The guard below accepts an index that already exists
-- with the expected definition and is valid.
--
-- The out-of-band recipe is the one written out in full in
-- 20260905181020_harvest_usage_activity_lookback_index/up.sql. It covers
-- the partitioned-harvest_events variant (`harvest partition enable`, issue
-- #958), the indisvalid cleanup pass, the per-leaf convergence loop and the
-- partition-maintenance freeze it needs. Substitute this index's own name,
-- column list (`timestamp, workflow_exec_id`, no WHERE clause) and
-- fingerprint (below) into that recipe.
--
-- The guard rejects two states rather than reporting success over them: a
-- same-named index with a DIFFERENT definition (this migration never
-- installed the intended index, and down.sql would drop an unrelated one),
-- and a matching but INVALID index (an out-of-band CONCURRENTLY build never
-- finished).
DO $$
DECLARE
    existing_index_oid oid;
    existing_def text;
    existing_valid boolean;
BEGIN
    SELECT pg_class.oid, pg_get_indexdef(pg_class.oid), pg_index.indisvalid
      INTO existing_index_oid, existing_def, existing_valid
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'idx_harvest_events_recent_by_timestamp'
      AND pg_index.indrelid = 'harvest_events'::regclass;

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_events_recent_by_timestamp
            ON harvest_events (timestamp, workflow_exec_id);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree ("timestamp", workflow_exec_id)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_events_recent_by_timestamp already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_events_recent_by_timestamp already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
