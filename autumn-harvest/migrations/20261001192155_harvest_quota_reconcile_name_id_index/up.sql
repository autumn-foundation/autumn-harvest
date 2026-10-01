-- Name-leading candidate index for quota_reconcile (issue #1631).
--
-- The scan now seeks once per registered workflow name. Each seek reads
-- `workflow_name = $name AND id > $cursor ORDER BY id LIMIT $batch`.
-- This index serves that read in `id` order with no sort. The work per
-- tick is bounded by names times batch size. Unrelated candidate rows in
-- other types are never read.
--
-- The old `(id)` index, idx_harvest_we_quota_reconcile_candidates, stays.
-- Migrations run before the new binary rolls out. Old workers keep the
-- old global query until they stop, and that query needs the old index.
-- The new query does not use it. A later migration drops it, once no old
-- binary can run.
--
-- Like the old index, this one self-shrinks. A row leaves it when its
-- `quota_key` is set or its execution turns terminal.
--
-- On a live deployment, use the concurrent form. It cannot run inside
-- Diesel's migration transaction. Build the index first:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_quota_reconcile_name_id
--       ON harvest_workflow_executions (workflow_name, id)
--       WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED');
-- The block below accepts a prebuilt index only if its definition matches
-- and it is valid. It raises an error otherwise.
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
    WHERE pg_class.relname = 'idx_harvest_we_quota_reconcile_name_id'
      AND pg_index.indrelid = 'harvest_workflow_executions'::regclass;

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_we_quota_reconcile_name_id
            ON harvest_workflow_executions (workflow_name, id)
            WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED');
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (workflow_name, id) WHERE ((quota_key IS NULL) AND (state = ANY (ARRAY[''RUNNING''::text, ''PAUSED''::text])))'
    THEN
        RAISE EXCEPTION
            'idx_harvest_we_quota_reconcile_name_id already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_we_quota_reconcile_name_id already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
