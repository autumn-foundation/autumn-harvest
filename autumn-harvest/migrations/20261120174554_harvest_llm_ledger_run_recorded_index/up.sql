-- Covering index for the tenant LLM spend read (issue #1997, Ledger pass).
--
-- A tenant LLM cap sums the ledger rows of each run of the tenant that fall
-- in the window. The primary key finds every row of a run, and the read then
-- drops each row outside the window. A heap page holds the rows of many runs,
-- so the read touched about one heap page per ledger row of each run.
--
-- This index keys the rows of a run by `recorded_at`, so the window is a
-- range scan. It includes the three summed columns, so the sum reads the
-- index only. `docs/performance-llm-spend-tenant-window.md` has the numbers.
--
-- The plain build takes `SHARE` on `harvest_llm_ledger` for the build, which
-- blocks the ledger inserts of completing LLM steps. On a large ledger,
-- build the index first:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS harvest_llm_ledger_run_recorded
--       ON harvest_llm_ledger (workflow_exec_id, recorded_at)
--       INCLUDE (input_tokens, output_tokens, cost_usd_micros);
-- The block below accepts a prebuilt index only if its definition matches
-- and it is valid. It raises an error otherwise.
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    existing_def text;
    existing_valid boolean;
BEGIN
    SELECT pg_get_indexdef(i.indexrelid), i.indisvalid
      INTO existing_def, existing_valid
      FROM pg_index i
     WHERE i.indexrelid = to_regclass('harvest_llm_ledger_run_recorded');
    IF existing_def IS NULL THEN
        CREATE INDEX harvest_llm_ledger_run_recorded
            ON harvest_llm_ledger (workflow_exec_id, recorded_at)
            INCLUDE (input_tokens, output_tokens, cost_usd_micros);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (workflow_exec_id, recorded_at) INCLUDE (input_tokens, output_tokens, cost_usd_micros)'
    THEN
        RAISE EXCEPTION
            'harvest_llm_ledger_run_recorded already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'harvest_llm_ledger_run_recorded already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
