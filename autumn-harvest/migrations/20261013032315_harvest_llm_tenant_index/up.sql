-- Tenant index for the LLM budgets (issue #1997).
--
-- A tenant LLM cap sums the agent cost ledger (issue #1996) of one
-- `(workflow_name, quota_key)` in a rolling window. The ledger has no key
-- column, so the check joins each ledger row to its run.
--
-- A ledger row is never newer than the end of its run. Only a run that is
-- active, or that ended in the window, can hold a row in the window. This
-- index finds those runs by a range on the end time. A run that has not
-- ended sorts as `infinity`, so it is always in the range. The check then
-- reads the ledger of each run by its primary key.
--
-- Without the index, the check reads every run of the tenant that retention
-- keeps. It runs before each LLM step.
--
-- The index is partial on `quota_key IS NOT NULL`. A run with no key has no
-- tenant spend.
--
-- On a live deployment, use the concurrent form. It cannot run inside
-- Diesel's migration transaction. Build the index first:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_llm_tenant
--       ON harvest_workflow_executions
--          (workflow_name, quota_key, (COALESCE(completed_at, 'infinity'::timestamptz)))
--       WHERE quota_key IS NOT NULL;
-- The block below accepts a prebuilt index only if its definition matches
-- and it is valid. It raises an error otherwise.
SET LOCAL lock_timeout = '5s';

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
    WHERE pg_class.relname = 'idx_harvest_we_llm_tenant'
      AND pg_index.indrelid = 'harvest_workflow_executions'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #1997 operators prebuild it CONCURRENTLY
        CREATE INDEX idx_harvest_we_llm_tenant
            ON harvest_workflow_executions
               (workflow_name, quota_key, (COALESCE(completed_at, 'infinity'::timestamptz)))
            WHERE quota_key IS NOT NULL;
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (workflow_name, quota_key, COALESCE(completed_at, ''infinity''::timestamp with time zone)) WHERE (quota_key IS NOT NULL)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_we_llm_tenant already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_we_llm_tenant already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
